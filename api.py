"""
algo.order — Order Execution API
─────────────────────────────────
Dedicated service for broker-order actions used across every domain
(simulator, algo trade, scanner, signal) — place-order, SL/Target
triggers/adjustments, alert-config, and payoff-chart SL markers.

Every route here is copied (not moved) from algo.simulator/api.py and
algo.simulator/simulator/api_server.py — algo.simulator's own copies are left
untouched since its webhook handlers (_simulator_pt_webhook_create_strategy,
_simulator_pt_webhook_fire_live_adjustment) call _simulator_place_manual_order_core
in-process and its background risk-monitor reads the adjustments/triggers
collections directly from Mongo, independent of which process last wrote
them. This mirrors the existing precedent: algo.trade/api.py already keeps
its own separate copy of the exact same place-order function.

Run:
    uvicorn order_main:app --reload --port 8004
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import logging
import os
import threading

import requests
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import pathlib as _pathlib
from dotenv import load_dotenv
load_dotenv(_pathlib.Path(__file__).resolve().parent / ".env")

from bson import ObjectId
from fastapi import Depends, FastAPI, APIRouter, Header, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from features import auth as app_auth
from features.mongo_data import MongoData

log = logging.getLogger(__name__)

app = FastAPI(title="algo.order — Order Execution API", version="1.0.0")
order_router = APIRouter(prefix="/order")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

IST = timezone(timedelta(hours=5, minutes=30))
_shared_mongo = MongoData()
_simulator_strategy_col = _shared_mongo._db["simulator_strategy"]

# Server-to-server auth for the broker execution gateway (see _verify_internal_token
# below) — algo.trade's live_order_manager.py calls those routes with no logged-in
# user/JWT, so they can't use app_auth.get_current_user like every other route here.
INTERNAL_SERVICE_TOKEN = os.getenv("INTERNAL_SERVICE_TOKEN", "")

# Dev bridge: when set, THIS box's place-order route forwards the request as-is to the
# live server and returns whatever it responds with, instead of placing the order itself
# — for a local/dev box that can't reach the broker (no whitelisted IP, no live
# credentials) but still needs a real order_id to develop/test status-tracking, MPP
# pricing logs, etc. against. Every OTHER route (status polling, order book, the
# order-updates socket) still runs fully local — only placement is redirected. Off
# (LIVE_ORDER_PLACEMENT unset/false) behaves exactly as before: places for real, right here.
LIVE_ORDER_PLACEMENT = os.getenv("LIVE_ORDER_PLACEMENT", "false").strip().lower() == "true"
LIVE_ORDER_BASE_URL = os.getenv("LIVE_ORDER_BASE_URL", "https://order.finedgealgo.com/order").rstrip("/")


# ── Order-update push (broker order-status pushed straight to the Order Pad/Orderbook) ──
# _app_loop is captured at startup so a push listener (called from another thread — e.g.
# dhan_order_update.py's per-account WS, or an internal broadcast route for postback-based
# brokers) can hop back onto the FastAPI event loop to actually send on the browser sockets,
# same bridge pattern as algo.websocket/ws_main.py's _InternalTickHub for tick data.
_app_loop: "asyncio.AbstractEventLoop | None" = None
_order_update_sockets: dict[str, list] = {}
_order_update_sockets_lock = threading.Lock()


def _register_order_update_socket(broker_id: str, ws: WebSocket) -> None:
    with _order_update_sockets_lock:
        _order_update_sockets.setdefault(broker_id, []).append(ws)


def _unregister_order_update_socket(broker_id: str, ws: WebSocket) -> None:
    with _order_update_sockets_lock:
        sockets = _order_update_sockets.get(broker_id)
        if sockets and ws in sockets:
            sockets.remove(ws)
            if not sockets:
                _order_update_sockets.pop(broker_id, None)


async def _broadcast_order_update(broker_id: str, message: dict) -> None:
    with _order_update_sockets_lock:
        sockets = list(_order_update_sockets.get(broker_id, []))
    dead = []
    for ws in sockets:
        try:
            await ws.send_json(message)
        except Exception:
            dead.append(ws)
    if dead:
        for ws in dead:
            _unregister_order_update_socket(broker_id, ws)


def _resolve_dhan_account_for_broker_id(broker_id: str) -> tuple[str, str] | None:
    """
    broker_id here is a kite_market_config._id (see _simulator_place_manual_order_core:
    Dhan orders are placed against broker_id == that doc's own _id, not a
    broker_configuration._id like FlatTrade/Kite) — resolve it back to that account's
    client_id/access_token so the WS route can open (or reuse) that SPECIFIC account's
    order-update connection. Returns None for any broker_id that isn't a Dhan
    kite_market_config doc (FlatTrade/Kite broker_ids fall through untouched).
    """
    try:
        doc = _shared_mongo._db["kite_market_config"].find_one({"_id": ObjectId(broker_id), "broker": "dhan"})
    except Exception:
        return None
    if not doc:
        return None
    client_id = str(doc.get("user_id") or doc.get("dhan_client_id") or "").strip()
    access_token = str(doc.get("access_token") or "").strip()
    if not client_id or not access_token:
        return None
    return client_id, access_token


@app.on_event("startup")
async def _capture_app_loop() -> None:
    global _app_loop
    _app_loop = asyncio.get_event_loop()

    # Every blocking broker REST call in this service (place_order, orders(), quote
    # fetches, Mongo lookups run off the loop) goes through asyncio.to_thread, which uses
    # this loop's default executor — Python's own default caps that pool at
    # min(32, cpu_count()+4), meaning as few as 5-6 workers on a small box. That's a real
    # ceiling on how many legs of one basket (e.g. a 10-leg order) can actually place
    # concurrently: legs beyond the pool size queue behind earlier ones instead of firing
    # together, regardless of place_legs_hedge_ordered's own asyncio.gather batching.
    # Raised here since these are short I/O-bound waits (network round trips), not
    # CPU-bound work — a bigger pool costs idle-thread memory, not CPU.
    _app_loop.set_default_executor(concurrent.futures.ThreadPoolExecutor(max_workers=32))


@order_router.websocket("/ws/order-updates")
async def order_updates_socket(websocket: WebSocket, broker_id: str = Query(default="")) -> None:
    """
    Instant order-status push for the Order Pad / Orderbook — Dhan's own Live Order
    Update WS (see features.dhan_order_update) relayed straight through, replacing their
    old poll-GET-/broker/orders-every-4s loop with a true push the moment Dhan emits a
    status change (COMPLETE/REJECTED/CANCELLED/TRIGGER_PENDING/OPEN).

    Dhan connections are per-account (features.dhan_order_update's pool) and started here,
    on demand, for the SPECIFIC account broker_id resolves to — not a single app-wide
    connection — so this scales to one Dhan account per user instead of assuming there's
    only ever one. A broker_id that isn't a Dhan kite_market_config doc (FlatTrade/Kite)
    just registers the socket for whatever other channel pushes to it (e.g. a broker
    postback) without touching Dhan at all.
    """
    await websocket.accept()
    if not broker_id:
        await websocket.close()
        return
    _register_order_update_socket(broker_id, websocket)

    dhan_listener = None
    dhan_conn = None
    try:
        # Any failure here (Mongo hiccup, pool/thread error) must not crash the whole
        # handler before the receive loop even starts — that would skip straight to the
        # outer finally with no chance to log why, and the browser just sees the socket
        # die instantly and reconnect-loop forever with no clue what happened server-side.
        dhan_account = await asyncio.to_thread(_resolve_dhan_account_for_broker_id, broker_id)
        if dhan_account:
            from features.dhan_order_update import dhan_order_update_pool
            client_id, access_token = dhan_account
            dhan_conn = await asyncio.to_thread(dhan_order_update_pool.ensure_started, client_id, access_token)
            if dhan_conn is not None:
                loop = asyncio.get_event_loop()

                def _forward(entry: dict) -> None:
                    # Runs on the Dhan WS's own thread — hop back onto this route's event
                    # loop to actually send. Scoped to this one broker_id/account pair,
                    # unlike the old app-wide broadcast.
                    asyncio.run_coroutine_threadsafe(_broadcast_order_update(broker_id, entry), loop)

                dhan_listener = _forward
                dhan_conn.add_update_listener(dhan_listener)
    except Exception:
        log.exception("[ORDER UPDATE WS] Dhan connection setup failed broker_id=%s — continuing without push for it.", broker_id)

    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        if dhan_conn is not None and dhan_listener is not None:
            dhan_conn.remove_update_listener(dhan_listener)
        _unregister_order_update_socket(broker_id, websocket)


def _verify_internal_token(x_internal_token: str = Header(default="")) -> None:
    if not INTERNAL_SERVICE_TOKEN or x_internal_token != INTERNAL_SERVICE_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid internal service token.")


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}


def _find_owned_strategy(strategy_id: str, current_user: dict) -> Optional[dict]:
    """
    Same ownership rule as algo.simulator's own _find_owned_strategy copies
    (api.py / simulator/api_server.py each keep their own rather than a
    cross-module import) — None for both "doesn't exist" and "belongs to
    someone else" so callers never leak which case it was.
    """
    doc = _simulator_strategy_col.find_one({"_id": ObjectId(strategy_id)})
    if not doc:
        return None
    doc_user_id = doc.get("user_id")
    current_user_id = current_user.get("_id")
    if doc_user_id is not None and current_user_id is not None and str(doc_user_id) != str(current_user_id):
        return None
    return doc


# ════════════════════════════════════════════════════════════════════════════
# Place order — copied from algo.simulator/api.py (_simulator_place_manual_order_core
# and its helpers, lines ~3434-3894 there)
# ════════════════════════════════════════════════════════════════════════════

class ManualOrderLeg(BaseModel):
    underlying: str
    expiry: str            # "YYYY-MM-DD"
    strike: float = 0.0    # 0.0 for a futures leg (option_type "FUT")
    option_type: str       # "CE" / "PE" / "FUT"
    side: str               # "BUY" / "SELL"
    quantity: int
    order_type: str         # "MARKET" / "LIMIT" / "SL"
    product: str             # "NRML" / "MIS"
    price: float = 0.0
    trigger_price: float = 0.0
    leg_id: str = ""        # Order Pad row id (client-generated) — echoed back so the
                             # frontend can match each result to its exact row/leg instead
                             # of relying on array order.
    security_id: str = ""   # Dhan security_id, when the frontend already has it (Order Pad
                             # rows carry it as `token` off the same active_option_tokens/
                             # broker=dhan feed _resolve_dhan_security would otherwise
                             # re-query). Optional — empty/wrong falls back to the Mongo
                             # lookup, so this is a pure speed optimization, never trusted
                             # blindly for correctness.


class ManualOrderRequest(BaseModel):
    broker_id: str
    orders: list[ManualOrderLeg]


_manual_order_kite_cache: dict[tuple, dict] = {}
_manual_order_kite_cache_date: str = ""


def _fetch_manual_order_kite_cache(raw_db, kite_doc: dict | None) -> dict[tuple, dict]:
    """
    Same shape/keying as spot_atm_utils._load_kite_instruments(), fetched directly with a
    specific Kite account's own credentials instead of going through that shared helper —
    which silently skips fetching (returns its empty cache) whenever Dhan is the active
    market-data feed broker, a global/unrelated setting that has nothing to do with whether
    a real Kite account is configured for placing this order.
    """
    global _manual_order_kite_cache, _manual_order_kite_cache_date
    today = datetime.now().strftime("%Y-%m-%d")
    if _manual_order_kite_cache_date == today and _manual_order_kite_cache:
        return _manual_order_kite_cache

    doc = kite_doc
    if doc is None:
        for candidate in raw_db["broker_configuration"].find({"broker_type": "live"}):
            name = str(candidate.get("broker_name") or candidate.get("name") or "").lower()
            if ("kite" in name or "zerodha" in name) and candidate.get("api_key") and candidate.get("access_token"):
                doc = candidate
                break
    if not doc:
        return {}

    try:
        from kiteconnect import KiteConnect  # type: ignore

        kite = KiteConnect(api_key=str(doc.get("api_key") or "").strip())
        kite.set_access_token(str(doc.get("access_token") or "").strip())
        new_cache: dict[tuple, dict] = {}
        for segment in ("NFO", "BFO"):
            for inst in kite.instruments(segment):
                name = str(inst.get("name") or "").strip().upper()
                inst_type = str(inst.get("instrument_type") or "").strip().upper()
                exp = inst.get("expiry")
                stk = inst.get("strike")
                sym = str(inst.get("tradingsymbol") or "").strip()
                if not (name and inst_type in ("CE", "PE") and exp and stk is not None and sym):
                    continue
                try:
                    exp_str = exp.strftime("%Y-%m-%d")
                except AttributeError:
                    exp_str = str(exp)[:10]
                new_cache[(name, exp_str, float(stk), inst_type)] = {
                    "symbol": sym,
                    "exchange": str(inst.get("exchange") or segment),
                }
        _manual_order_kite_cache = new_cache
        _manual_order_kite_cache_date = today
        return new_cache
    except Exception as exc:
        log.debug("manual order kite instrument fetch error: %s", exc)
        return {}


def _resolve_manual_order_symbol(leg: "ManualOrderLeg", raw_db, kite_doc: dict | None = None) -> tuple[str, str] | None:
    """
    Kite-native (underlying, expiry, strike, option_type) → (tradingsymbol, exchange).
    Same instrument metadata _to_flattrade_symbol() already uses for the FlatTrade
    conversion — account-agnostic, so it's safe to resolve this way regardless of
    which broker_id is actually placing the order.
    """
    from features.spot_atm_utils import _load_kite_instruments

    cache = _load_kite_instruments()
    if not cache:
        cache = _fetch_manual_order_kite_cache(raw_db, kite_doc)

    key = (
        leg.underlying.strip().upper(),
        leg.expiry.strip()[:10],
        float(leg.strike),
        leg.option_type.strip().upper(),
    )
    inst = cache.get(key)
    if not inst:
        return None
    return str(inst["symbol"]), str(inst["exchange"])


def _resolve_dhan_security(leg: "ManualOrderLeg", raw_db) -> dict | None:
    """
    (underlying, expiry, strike, option_type) → Dhan's own securityId/symbol/exchangeSegment,
    from the same active_option_tokens collection execution_socket.py already keys positions off
    of. Dhan identifies instruments by numeric securityId, not a tradingsymbol string, so this
    doesn't reuse _resolve_manual_order_symbol (that one resolves the Kite-style symbol).
    """
    doc = raw_db["active_option_tokens"].find_one({
        "broker": "dhan",
        "instrument": leg.underlying.strip().upper(),
        "expiry": leg.expiry.strip()[:10],
        "strike": float(leg.strike),
        "option_type": leg.option_type.strip().upper(),
    })
    if not doc:
        return None
    security_id = str(doc.get("token") or "").strip()
    if not security_id:
        return None
    return {
        "security_id": security_id,
        "symbol": str(doc.get("symbol") or "").strip(),
        "exchange_segment": str(doc.get("ws_segment") or "").strip().upper() or "NSE_FNO",
    }


# f"{segment}:{sec_id}" → last-seen-good market-data dict. Never evicted —
# see the resilience note in _fetch_dhan_market_data()'s docstring below.
_DHAN_MARKET_DATA_LAST_GOOD: dict[str, dict] = {}


def _fetch_dhan_market_data(segment: str, sec_ids: list[int], db) -> dict[str, dict]:
    """
    Fetch LTP + OI + best bid/ask from Dhan /marketfeed/quote for a list of security IDs.
    Returns {str(sec_id): {"ltp": float, "oi": int, "bid": float, "ask": float, "prev_close": float}}.
    Dhan /quote supports up to 1000 per segment — send as few requests as possible.

    WS-first + last-good fallback, same resilience as
    features.broker_gateway.get_broker_rest_quotes: Dhan's REST quote
    endpoint rate-limits to ~1 req/sec per account. A WS ltp_map hit resolves
    a sec_id with zero REST round trip; a 429/failed REST attempt falls
    straight back to the last real value seen for that sec_id.
    """
    if not sec_ids:
        return {}
    raw_db = db._db if hasattr(db, "_db") else db
    cfg = raw_db["kite_market_config"].find_one({"broker": "dhan", "enabled": True}) or {}
    access_token = str(cfg.get("access_token") or "").strip()
    client_id = str(cfg.get("user_id") or cfg.get("dhan_client_id") or "").strip()
    if not access_token or not client_id:
        return {}

    result: dict[str, dict] = {}

    # WS ltp_map/oi_map are keyed by bare numeric security id regardless of
    # segment (index/equity/FNO ticks all land there — see dhan_ticker.py's
    # binary parser), so a hit here is an in-memory read, no REST call at all.
    try:
        from features.dhan_ticker import dhan_ticker_manager as _dtm  # type: ignore
        for sid in sec_ids:
            sid_str = str(sid)
            ws_ltp = float(_dtm.ltp_map.get(sid_str) or 0)
            if ws_ltp > 0:
                cached = _DHAN_MARKET_DATA_LAST_GOOD.get(f"{segment}:{sid_str}") or {}
                ws_bid = float(_dtm.bid_map.get(sid_str) or 0)
                ws_ask = float(_dtm.ask_map.get(sid_str) or 0)
                ws_bid_qty = int(_dtm.bid_qty_map.get(sid_str) or 0)
                ws_ask_qty = int(_dtm.ask_qty_map.get(sid_str) or 0)
                result[sid_str] = {
                    "ltp": ws_ltp,
                    "oi": int(_dtm.oi_map.get(sid_str) or cached.get("oi", 0)),
                    "bid": ws_bid or cached.get("bid", 0.0),
                    "ask": ws_ask or cached.get("ask", 0.0),
                    # Level-0 qty only — see algo.simulator/api.py's _fetch_dhan_market_data
                    # for why the walk still falls back to REST depth when this isn't enough.
                    "bid_depth": [{"price": ws_bid, "quantity": ws_bid_qty}] if ws_bid > 0 else cached.get("bid_depth", []),
                    "ask_depth": [{"price": ws_ask, "quantity": ws_ask_qty}] if ws_ask > 0 else cached.get("ask_depth", []),
                    "prev_close": cached.get("prev_close", 0.0),
                }
    except Exception:
        pass

    missing = [sid for sid in sec_ids if str(sid) not in result]
    if missing:
        from features.broker_gateway import dhan_quote_post_blocking

        _BATCH = 500  # Dhan /quote supports up to 1000 per segment
        batches = [missing[i: i + _BATCH] for i in range(0, len(missing), _BATCH)]

        for batch in batches:
            for _attempt in range(3):
                try:
                    r = dhan_quote_post_blocking({segment: batch}, access_token, client_id, timeout=15.0)
                    if r is None:
                        continue
                    if r.status_code == 200:
                        raw = r.json()
                        data = (raw.get("data") or raw).get(segment) or {}
                        for sid, info in data.items():
                            if not isinstance(info, dict):
                                continue
                            depth = info.get("depth") or {}
                            buy_levels = depth.get("buy") or []
                            sell_levels = depth.get("sell") or []
                            entry = {
                                "ltp": float(info.get("last_price") or 0),
                                "oi":  int(info.get("oi") or 0),
                                "bid": float((buy_levels[0] or {}).get("price") or 0) if buy_levels else 0.0,
                                "ask": float((sell_levels[0] or {}).get("price") or 0) if sell_levels else 0.0,
                                # Full depth (all levels Dhan returns, best→worst) — lets
                                # _resolve_mpp_price walk past level 0 for an order qty bigger
                                # than the top level alone holds instead of pricing as if it
                                # were the whole book.
                                "bid_depth": [
                                    {"price": float(lvl.get("price") or 0), "quantity": int(lvl.get("quantity") or 0)}
                                    for lvl in buy_levels if float(lvl.get("price") or 0) > 0
                                ],
                                "ask_depth": [
                                    {"price": float(lvl.get("price") or 0), "quantity": int(lvl.get("quantity") or 0)}
                                    for lvl in sell_levels if float(lvl.get("price") or 0) > 0
                                ],
                                "prev_close": float((info.get("ohlc") or {}).get("close") or 0),
                            }
                            result[str(sid)] = entry
                            if entry["ltp"] > 0:
                                _DHAN_MARKET_DATA_LAST_GOOD[f"{segment}:{sid}"] = entry
                        break
                    else:
                        log.warning("[DHAN QUOTE] segment=%s status=%d attempt=%d body=%s",
                                    segment, r.status_code, _attempt, r.text[:200])
                except Exception as _e:
                    log.warning("[DHAN QUOTE] error=%s attempt=%d", _e, _attempt)

    for sid in sec_ids:
        sid_str = str(sid)
        if sid_str not in result or not result[sid_str].get("ltp"):
            cached = _DHAN_MARKET_DATA_LAST_GOOD.get(f"{segment}:{sid_str}")
            if cached:
                result[sid_str] = cached

    return result


async def _fetch_dhan_quote_for_leg(leg: "ManualOrderLeg", raw_db, quote_cache: dict[str, dict] | None = None) -> dict | None:
    """
    Resolves this leg's Dhan security_id and returns its live quote {"symbol","ltp","bid","ask"}.
    Returns None if Dhan has no contract match for this leg at all.

    Shared by _resolve_mpp_price and _resolve_ltp_price — every order's price, regardless of
    which broker (FlatTrade/Kite/Dhan) actually executes it, is read from this one feed.

    quote_cache (security_id -> quote dict), when given, is consulted before ever calling Dhan's
    REST /marketfeed/quote — see _batch_prefetch_dhan_quotes: that endpoint is rate-gated to one
    call per ~1.05s per process (wait_for_dhan_slot), so resolving each leg of a multi-leg order
    independently here serializes them behind that gate — 96ms for the first leg, ~1.1s more for
    the second, growing with leg count. The core function prefetches every leg's quote in one
    batched call before placing any leg, so this only ever falls back to a live REST call when
    the cache doesn't have it yet (e.g. instrument missing from the batch resolve).
    """
    resolved = await asyncio.to_thread(_resolve_dhan_security, leg, raw_db)
    if not resolved:
        return None
    sec_id = resolved["security_id"]
    if quote_cache is not None and sec_id in quote_cache:
        quote = quote_cache[sec_id]
    else:
        quote = (await asyncio.to_thread(
            _fetch_dhan_market_data, resolved["exchange_segment"], [int(sec_id)], _shared_mongo,
        )).get(sec_id, {})
    return {
        "symbol": resolved["symbol"],
        "ltp": float(quote.get("ltp") or 0),
        "bid": float(quote.get("bid") or 0),
        "ask": float(quote.get("ask") or 0),
        # Full depth (best→worst) when available — see _fetch_dhan_market_data. WS-sourced
        # quotes only ever have 1 level (Dhan's other 4 WS depth levels aren't parsed); REST
        # quotes carry whatever Dhan's /marketfeed/quote returns (typically 5).
        "bid_depth": quote.get("bid_depth") or ([{"price": float(quote.get("bid") or 0), "quantity": 0}] if quote.get("bid") else []),
        "ask_depth": quote.get("ask_depth") or ([{"price": float(quote.get("ask") or 0), "quantity": 0}] if quote.get("ask") else []),
    }


async def _batch_prefetch_dhan_quotes(orders: list["ManualOrderLeg"], raw_db) -> dict[str, dict]:
    """
    Resolves every MPP/LTP leg's Dhan security in parallel (cheap Mongo lookups, no rate limit),
    then fetches all their quotes in ONE call per exchange segment instead of one REST round trip
    per leg — see _fetch_dhan_quote_for_leg's docstring for why per-leg calls stack up behind
    Dhan's ~1.05s quote rate gate. Returns {security_id: quote_dict}, empty if no leg needs a
    live price (plain LIMIT/MARKET/SL orders use their own typed-in price, no feed lookup at all).
    """
    price_legs = [o for o in orders if o.order_type in ("MPP", "LTP")]
    if not price_legs:
        return {}
    resolved_list = await asyncio.gather(*(asyncio.to_thread(_resolve_dhan_security, leg, raw_db) for leg in price_legs))
    by_segment: dict[str, list[int]] = {}
    for resolved in resolved_list:
        if resolved:
            by_segment.setdefault(resolved["exchange_segment"], []).append(int(resolved["security_id"]))
    quote_cache: dict[str, dict] = {}
    for segment, sec_ids in by_segment.items():
        quote_cache.update(await asyncio.to_thread(_fetch_dhan_market_data, segment, sec_ids, _shared_mongo))
    return quote_cache


def _notify_mpp_ltp_price_unresolved(kind: str, message: str) -> None:
    """
    Shared by _resolve_mpp_price/_resolve_ltp_price — every failure to resolve a real,
    fresh price pages admin via Telegram instead of failing silently, since the only other
    signal is a 0.0 return the caller must already be checking for.
    """
    print(f"[{kind} PRICE] {message}", flush=True)
    try:
        from features.telegram_notifier import notify_admin
        notify_admin(f"{kind.lower()}_price_unresolved", message)
    except Exception as exc:
        log.warning("[%s PRICE] notify_admin failed: %s", kind, exc)


async def _resolve_mpp_price(leg: "ManualOrderLeg", raw_db, quote_cache: dict[str, dict] | None = None) -> float:
    """
    MPP's walked-depth + protection% formula, priced off Dhan's feed regardless of the
    execution broker (see _fetch_dhan_quote_for_leg) — same formula as
    live_order_manager.py's place_live_entry_order/place_live_exit_order and
    algo.simulator/api.py's & algo.trade/api.py's copies of this same function. Previously
    this copy placed MPP orders at bare LTP with no bid/ask protection at all — a limit
    order sitting between the spread doesn't cross and just rests, silently defeating the
    whole point of "Market Price Protection."

    Prices off the side this leg's qty actually has to sweep to fill — a BUY matches
    against resting ASK orders, a SELL matches against resting BID orders — and walks that
    side's depth levels (best→worst) until leg.quantity is covered, instead of assuming the
    top level alone holds the whole order.

    Returns 0.0 — never leg.price as a stand-in — when Dhan has no contract match or no live
    depth on the side this leg needs. Every caller already treats a <= 0 return as
    "unresolved" and aborts the order instead of placing it.
    """
    from features.live_order_manager import _mpp_protection_pct, _clamp_limit_price, _walk_depth_for_qty

    quote = await _fetch_dhan_quote_for_leg(leg, raw_db, quote_cache)
    if not quote:
        _notify_mpp_ltp_price_unresolved(
            "MPP", f"No Dhan contract match for {leg.option_type} {leg.strike} exp={leg.expiry} — order NOT placed.",
        )
        return 0.0

    ltp = quote["ltp"]
    is_buy = leg.side == "BUY"
    consume_side = quote["ask_depth"] if is_buy else quote["bid_depth"]
    walked_price, fully_covered = _walk_depth_for_qty(consume_side, leg.quantity)
    if walked_price <= 0:
        _notify_mpp_ltp_price_unresolved(
            "MPP",
            f"No live depth for {quote.get('symbol')} (side={'ask' if is_buy else 'bid'}, qty={leg.quantity}) — order NOT placed.",
        )
        return 0.0
    if not fully_covered:
        print(
            f"[MPP PRICE][dhan-feed] symbol={quote['symbol']} qty={leg.quantity} not fully covered by visible "
            f"{'ask' if is_buy else 'bid'}-side depth — pricing off deepest visible level {walked_price}, "
            f"remainder may not fill immediately.",
            flush=True,
        )

    pct = _mpp_protection_pct(ltp, is_option=leg.option_type.strip().upper() != "FUT")
    raw_price = walked_price * (1 + pct / 100) if is_buy else walked_price * (1 - pct / 100)
    price = _clamp_limit_price(raw_price, is_buy)
    print(
        f"[MPP PRICE][dhan-feed] symbol={quote['symbol']} ltp={ltp} qty={leg.quantity} "
        f"walked_price={walked_price} fully_covered={fully_covered} pct={pct}% price={price} is_buy={is_buy}",
        flush=True,
    )
    return price


async def _resolve_ltp_price(leg: "ManualOrderLeg", raw_db, quote_cache: dict[str, dict] | None = None) -> float:
    """
    "Execute At LTP" price source — same Dhan-feed-regardless-of-execution-broker principle as
    _resolve_mpp_price (both just return live LTP; kept as separate order types since the UI
    exposes them separately).

    Returns 0.0 — never leg.price — if Dhan has no match/quote yet.
    """
    quote = await _fetch_dhan_quote_for_leg(leg, raw_db, quote_cache)
    if not quote or quote["ltp"] <= 0:
        _notify_mpp_ltp_price_unresolved(
            "LTP", f"No Dhan quote for {leg.option_type} {leg.strike} exp={leg.expiry} — order NOT placed.",
        )
        return 0.0
    print(f"[LTP PRICE][dhan-feed] symbol={quote['symbol']} ltp={quote['ltp']}", flush=True)
    return quote["ltp"]


async def _simulator_place_manual_order_core(body: ManualOrderRequest) -> dict:
    """
    Places real orders with the broker — this is live money, not a simulation.
    FlatTrade/Kite use their own place_order() already proven elsewhere in this
    codebase. Dhan goes straight to https://api.dhan.co/v2/orders (same direct-
    REST pattern already used for Dhan positions/quotes) — UNVERIFIED against a
    live order, unlike the other two: dhanhq SDK isn't installed, and this is
    adapted from an untested reference in the sibling option-algo repo. Test
    with one small/throwaway order before relying on it for size.
    """
    broker_id = str(body.broker_id or "").strip()
    print(f"[PLACE_ORDER] request broker_id={broker_id} legs={len(body.orders)} orders={[o.model_dump() for o in body.orders]}", flush=True)
    try:
        raw_db = _shared_mongo._db

        # One batched quote fetch for every MPP/LTP leg up front — see
        # _batch_prefetch_dhan_quotes's docstring: resolving each leg's price
        # independently (as before) serializes them behind Dhan's ~1.05s quote
        # rate gate, adding roughly 1s per extra leg to a multi-leg order.
        quote_cache = await _batch_prefetch_dhan_quotes(body.orders, raw_db)

        # Looked up by _id (the specific account the user picked in the Order Pad), not just
        # "any doc with broker=dhan" — kite_market_config can hold one Dhan doc per user (see
        # broker_accounts.get_market_broker_accounts_for_user), so matching on broker alone
        # would silently route every user's order through whichever Dhan doc Mongo happened
        # to return first instead of the one this broker_id actually names.
        dhan_cfg = {}
        if broker_id:
            try:
                dhan_cfg = raw_db["kite_market_config"].find_one({"_id": ObjectId(broker_id), "broker": "dhan"}) or {}
            except Exception:
                dhan_cfg = {}
        if dhan_cfg:
            dhan_client_id = str(dhan_cfg.get("user_id") or dhan_cfg.get("dhan_client_id") or "").strip()
            dhan_access_token = str(dhan_cfg.get("access_token") or "").strip()
            if not dhan_access_token or not dhan_client_id:
                print("[PLACE_ORDER][dhan] credentials not configured", flush=True)
                return {"status": "error", "message": "Dhan credentials not configured.", "results": []}

            # Warm this account's order-update connection right away — don't wait for the
            # browser to (re)open the /ws/order-updates socket first. Cheap no-op if it's
            # already up (see dhan_order_update.py's pool docstring).
            try:
                from features.dhan_order_update import dhan_order_update_pool
                dhan_order_update_pool.ensure_started(dhan_client_id, dhan_access_token)
            except Exception:
                log.warning("[PLACE_ORDER][dhan] order-update WS warm-start failed", exc_info=True)

            from features.dhan_broker import get_dhan_instance
            from features.order_execution import place_broker_order

            dhan_order_type_map = {"LIMIT": "LIMIT", "MARKET": "MARKET", "SL": "SL"}
            dhan_adapter = get_dhan_instance(_shared_mongo, dhan_client_id, dhan_access_token)

            async def _place_one_dhan_leg(leg: "ManualOrderLeg") -> dict:
                # Frontend-supplied security_id skips the Mongo round trip entirely — see
                # ManualOrderLeg.security_id's docstring. exchange_segment isn't sent by the
                # client, so this uses the same "NSE_FNO" default _resolve_dhan_security itself
                # falls back to when a contract's ws_segment isn't set.
                if leg.security_id.strip():
                    resolved = {
                        "security_id": leg.security_id.strip(),
                        "symbol": f"{leg.underlying} {leg.strike:g}{leg.option_type}",
                        "exchange_segment": "NSE_FNO",
                    }
                else:
                    resolved = await asyncio.to_thread(_resolve_dhan_security, leg, raw_db)
                if not resolved:
                    print(f"[PLACE_ORDER][dhan] instrument not found for leg={leg.model_dump()}", flush=True)
                    return {"leg": leg.model_dump(), "status": "error", "message": "Instrument not found."}

                price = leg.price
                requested_type = leg.order_type
                if requested_type == "MPP":
                    price = await _resolve_mpp_price(leg, raw_db, quote_cache)
                    if price <= 0:
                        print(f"[PLACE_ORDER][dhan] MPP price unresolved for leg={leg.model_dump()}", flush=True)
                        return {"leg": leg.model_dump(), "status": "error", "message": "MPP price unavailable — no live quote for this contract."}
                    requested_type = "LIMIT"
                elif requested_type == "LTP":
                    price = await _resolve_ltp_price(leg, raw_db, quote_cache)
                    if price <= 0:
                        print(f"[PLACE_ORDER][dhan] LTP price unresolved for leg={leg.model_dump()}", flush=True)
                        return {"leg": leg.model_dump(), "status": "error", "message": "LTP price unavailable — no live quote for this contract."}
                    requested_type = "LIMIT"

                dhan_order_type = dhan_order_type_map.get(requested_type, "LIMIT")
                result = await asyncio.to_thread(
                    place_broker_order,
                    dhan_adapter,
                    tradingsymbol=resolved["symbol"],
                    exchange="NFO",
                    transaction_type="BUY" if leg.side == "BUY" else "SELL",
                    quantity=leg.quantity,
                    order_type=dhan_order_type,
                    product=leg.product,
                    price=price,
                    trigger_price=leg.trigger_price or 0.0,
                    context={"purpose": "manual_order_pad", "broker": "dhan", "symbol": resolved["symbol"]},
                    broker_kwargs={"security_id": resolved["security_id"], "exchange_segment": resolved["exchange_segment"]},
                    check_status=False,
                )
                if result["status"] != "success":
                    return {"leg": leg.model_dump(), "status": "error", "message": result["message"]}
                return {
                    "leg": leg.model_dump(), "status": "success", "order_id": result["order_id"],
                    "broker_status": result.get("broker_status", "UNKNOWN"),
                    "average_price": result.get("average_price"),
                    "filled_quantity": result.get("filled_quantity"),
                }

            from features.order_execution import place_legs_hedge_ordered
            dhan_results: list[dict] = await place_legs_hedge_ordered(body.orders, _place_one_dhan_leg)

            any_ok = any(r["status"] == "success" for r in dhan_results)
            all_ok = bool(dhan_results) and all(r["status"] == "success" for r in dhan_results)
            overall_status = "success" if all_ok else ("partial" if any_ok else "error")
            print(f"[PLACE_ORDER] done status={overall_status} results={dhan_results}", flush=True)
            return {"status": overall_status, "results": dhan_results}

        try:
            doc = raw_db["broker_configuration"].find_one({"_id": ObjectId(broker_id)})
        except Exception:
            doc = None
        if not doc:
            print(f"[PLACE_ORDER] broker account not found for broker_id={broker_id}", flush=True)
            return {"status": "error", "message": "Broker account not found.", "results": []}

        broker_name = str(doc.get("broker_name") or doc.get("name") or "").strip().lower()
        is_flattrade = "flattrade" in broker_name
        is_kite = "zerodha" in broker_name or "kite" in broker_name
        print(f"[PLACE_ORDER] resolved broker_name={broker_name} is_flattrade={is_flattrade} is_kite={is_kite}", flush=True)
        if not is_flattrade and not is_kite:
            print(f"[PLACE_ORDER] rejected — order placement not supported for broker_name={broker_name}", flush=True)
            return {"status": "error", "message": "Order placement isn't available for this broker yet.", "results": []}

        results: list[dict] = []

        if is_flattrade:
            from features.flattrade_broker import get_flattrade_instance

            adapter = get_flattrade_instance(str(doc.get("user_id") or ""), str(doc.get("access_token") or ""))
            if adapter is None:
                print("[PLACE_ORDER][flattrade] session not available", flush=True)
                return {"status": "error", "message": "FlatTrade session not available.", "results": []}

            async def _place_one_flattrade_leg(leg: "ManualOrderLeg") -> dict:
                resolved = await asyncio.to_thread(_resolve_manual_order_symbol, leg, raw_db)
                if not resolved:
                    print(f"[PLACE_ORDER][flattrade] instrument not found for leg={leg.model_dump()}", flush=True)
                    return {"leg": leg.model_dump(), "status": "error", "message": "Instrument not found."}
                symbol, exchange = resolved

                price = leg.price
                order_type = leg.order_type
                if order_type == "MPP":
                    price = await _resolve_mpp_price(leg, raw_db, quote_cache)
                    if price <= 0:
                        print(f"[PLACE_ORDER][flattrade] MPP price unresolved for leg={leg.model_dump()}", flush=True)
                        return {"leg": leg.model_dump(), "status": "error", "message": "MPP price unavailable — no live quote for this contract."}
                    order_type = "LIMIT"
                elif order_type == "LTP":
                    price = await _resolve_ltp_price(leg, raw_db, quote_cache)
                    if price <= 0:
                        print(f"[PLACE_ORDER][flattrade] LTP price unresolved for leg={leg.model_dump()}", flush=True)
                        return {"leg": leg.model_dump(), "status": "error", "message": "LTP price unavailable — no live quote for this contract."}
                    order_type = "LIMIT"

                print(
                    f"[PLACE_ORDER][flattrade] placing tradingsymbol={symbol} exchange={exchange} "
                    f"transaction_type={leg.side} quantity={leg.quantity} order_type={order_type} "
                    f"product={leg.product} price={price} trigger_price={leg.trigger_price}",
                    flush=True,
                )
                from features.order_execution import place_broker_order
                result = await asyncio.to_thread(
                    place_broker_order,
                    adapter,
                    tradingsymbol=symbol,
                    exchange=exchange,
                    transaction_type=leg.side,
                    quantity=leg.quantity,
                    order_type=order_type,
                    product=leg.product,
                    price=price,
                    trigger_price=leg.trigger_price,
                    context={"purpose": "manual_order_pad", "broker": "flattrade", "symbol": symbol},
                    check_status=False,
                )
                print(f"[PLACE_ORDER][flattrade] response={result}", flush=True)
                if result["status"] != "success":
                    return {"leg": leg.model_dump(), "status": "error", "message": result["message"]}
                return {
                    "leg": leg.model_dump(), "status": "success", "order_id": result["order_id"],
                    "broker_status": result.get("broker_status", "UNKNOWN"),
                    "average_price": result.get("average_price"),
                    "filled_quantity": result.get("filled_quantity"),
                }

            from features.order_execution import place_legs_hedge_ordered
            results = await place_legs_hedge_ordered(body.orders, _place_one_flattrade_leg)
        else:
            from kiteconnect import KiteConnect  # type: ignore

            api_key = str(doc.get("api_key") or "").strip()
            access_token = str(doc.get("access_token") or "").strip()
            if not api_key or not access_token:
                print("[PLACE_ORDER][kite] session not available", flush=True)
                return {"status": "error", "message": "Kite session not available.", "results": []}
            kite = KiteConnect(api_key=api_key)
            kite.set_access_token(access_token)

            async def _place_one_kite_leg(leg: "ManualOrderLeg") -> dict:
                resolved = await asyncio.to_thread(_resolve_manual_order_symbol, leg, raw_db, doc)
                if not resolved:
                    print(f"[PLACE_ORDER][kite] instrument not found for leg={leg.model_dump()}", flush=True)
                    return {"leg": leg.model_dump(), "status": "error", "message": "Instrument not found."}
                symbol, exchange = resolved

                price = leg.price
                order_type = leg.order_type
                if order_type == "MPP":
                    price = await _resolve_mpp_price(leg, raw_db, quote_cache)
                    if price <= 0:
                        print(f"[PLACE_ORDER][kite] MPP price unresolved for leg={leg.model_dump()}", flush=True)
                        return {"leg": leg.model_dump(), "status": "error", "message": "MPP price unavailable — no live quote for this contract."}
                    order_type = "LIMIT"
                elif order_type == "LTP":
                    price = await _resolve_ltp_price(leg, raw_db, quote_cache)
                    if price <= 0:
                        print(f"[PLACE_ORDER][kite] LTP price unresolved for leg={leg.model_dump()}", flush=True)
                        return {"leg": leg.model_dump(), "status": "error", "message": "LTP price unavailable — no live quote for this contract."}
                    order_type = "LIMIT"

                print(
                    f"[PLACE_ORDER][kite] placing tradingsymbol={symbol} exchange={exchange} "
                    f"transaction_type={leg.side} quantity={leg.quantity} order_type={order_type} "
                    f"product={leg.product} price={price} trigger_price={leg.trigger_price}",
                    flush=True,
                )
                from features.order_execution import place_broker_order
                result = await asyncio.to_thread(
                    place_broker_order,
                    kite,
                    tradingsymbol=symbol,
                    exchange=exchange,
                    transaction_type=leg.side,
                    quantity=leg.quantity,
                    order_type=order_type,
                    product=leg.product,
                    price=price or 0.0,
                    trigger_price=leg.trigger_price or 0.0,
                    variety=kite.VARIETY_REGULAR,
                    context={"purpose": "manual_order_pad", "broker": "kite", "symbol": symbol},
                    check_status=False,
                )
                print(f"[PLACE_ORDER][kite] response={result}", flush=True)
                if result["status"] != "success":
                    return {"leg": leg.model_dump(), "status": "error", "message": result["message"]}
                return {
                    "leg": leg.model_dump(), "status": "success", "order_id": result["order_id"],
                    "broker_status": result.get("broker_status", "UNKNOWN"),
                    "average_price": result.get("average_price"),
                    "filled_quantity": result.get("filled_quantity"),
                }

            from features.order_execution import place_legs_hedge_ordered
            results = await place_legs_hedge_ordered(body.orders, _place_one_kite_leg)

        any_ok = any(r["status"] == "success" for r in results)
        all_ok = bool(results) and all(r["status"] == "success" for r in results)
        overall_status = "success" if all_ok else ("partial" if any_ok else "error")
        print(f"[PLACE_ORDER] done status={overall_status} results={results}", flush=True)
        return {
            "status": overall_status,
            "results": results,
        }
    except Exception as exc:
        print(f"[PLACE_ORDER] unhandled error={exc}", flush=True)
        return {"status": "error", "message": str(exc), "results": []}


def _persist_manual_order_pad_orders(body: "ManualOrderRequest", result: dict, current_user: dict) -> None:
    """
    Logs every Order Pad leg to `manual_order_pad_orders` — success or error alike,
    since this places real money and a broker rejection is exactly the kind of thing
    that must survive a missed toast/refresh, not just live in the HTTP response.
    Tracked per broker_id + position (underlying/expiry/strike/option_type) + leg_id
    (the Order Pad row id the frontend sent) so a specific order's outcome can always
    be traced back to the specific leg it belongs to.

    Also backfills each entry in result["results"] with db_id/placed_at (and leg_id,
    for entries the core placement function already built) — including synthesizing
    an entry for every ordered leg when the core function bailed out before per-leg
    placement (e.g. "Broker account not found") and returned results=[], so the
    caller always gets one result per requested leg, never a short/empty list.
    """
    col = _shared_mongo._db["manual_order_pad_orders"]
    now_str = datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S")
    user_id = str(current_user.get("_id") or "")
    overall_status = str(result.get("status") or "error")
    overall_message = str(result.get("message") or "")
    leg_results: list = result.get("results") or []
    synced_results: list = []
    docs: list = []
    for i, leg in enumerate(body.orders):
        leg_result = dict(leg_results[i]) if i < len(leg_results) else {
            "leg": leg.model_dump(), "status": overall_status, "message": overall_message,
        }
        docs.append({
            "user_id": user_id,
            "broker_id": body.broker_id,
            "leg_id": leg.leg_id,
            "underlying": leg.underlying,
            "expiry": leg.expiry,
            "strike": leg.strike,
            "option_type": leg.option_type,
            "side": leg.side,
            "quantity": leg.quantity,
            "order_type": leg.order_type,
            "product": leg.product,
            "price": leg.price,
            "trigger_price": leg.trigger_price,
            "status": str(leg_result.get("status") or overall_status),
            "order_id": str(leg_result.get("order_id") or ""),
            "message": str(leg_result.get("message") or ""),
            "placed_at": now_str,
        })
        leg_result["placed_at"] = now_str
        leg_result.setdefault("leg_id", leg.leg_id)
        synced_results.append(leg_result)

    # One batched insert_many instead of N sequential insert_one round trips — this
    # function runs synchronously on the event loop (see _place_manual_order_and_notify's
    # asyncio.to_thread wrapper), so N Mongo round trips here used to mean N * mongo-
    # latency of the whole process being unable to serve any other request, on top of
    # delaying this response — the same "blocking sync pymongo call in an async path"
    # class of bug fixed elsewhere in this codebase (shared/chart_api.py, etc.).
    if docs:
        try:
            inserted = col.insert_many(docs, ordered=True)
            for leg_result, inserted_id in zip(synced_results, inserted.inserted_ids):
                leg_result["db_id"] = str(inserted_id)
        except Exception as exc:
            print(f"[PLACE_ORDER] db persist failed error={exc}", flush=True)
    result["results"] = synced_results


async def _place_manual_order_and_notify(body: ManualOrderRequest, user_id: str) -> dict:
    """
    Shared by the user-facing route (JWT auth, Order Pad) and the internal
    server-to-server route (X-Internal-Token auth, webhook-triggered strategy
    creation + the live SL/TG adjustment monitor) — same DB logging + Telegram
    notify either way, since a live order is a live order regardless of who or
    what triggered it.
    """
    result = await _simulator_place_manual_order_core(body)
    try:
        await asyncio.to_thread(_persist_manual_order_pad_orders, body, result, {"_id": user_id})
    except Exception as exc:
        print(f"[PLACE_ORDER] db persist error={exc}", flush=True)
    try:
        from features.telegram_notifier import notify_user

        status = str(result.get("status") or "")
        leg_summary = ", ".join(
            f"{o.side} {o.underlying} {o.strike}{o.option_type} x{o.quantity}" for o in body.orders
        )
        if status == "success":
            notify_user("PT_ORDER_PLACED", f"Order placed — {leg_summary}", {"broker": body.broker_id})
        elif status in ("error", "partial"):
            notify_user(
                "PT_ORDER_FAILED" if status == "error" else "PT_ORDER_PARTIAL",
                f"Order {status} — {leg_summary} — {result.get('message', '')}",
                {"broker": body.broker_id},
            )
    except Exception as exc:
        print(f"[PLACE_ORDER] telegram notify error={exc}", flush=True)
    return result


@order_router.post("/trade/positions/place-order")
async def simulator_place_manual_order(
    body: ManualOrderRequest, request: Request, current_user: dict = Depends(app_auth.get_current_user_for_order_placement),
) -> dict:
    """
    Same route + wrapper as algo.trade's and algo.simulator's own copies (all
    three call the identical _simulator_place_manual_order_core) — this is
    now the one the Order Pad's "Trade"/Execute button posts to (ORDER_API_BASE).

    LIVE_ORDER_PLACEMENT (see its own comment near INTERNAL_SERVICE_TOKEN): when set,
    forwards straight to the live server instead of placing here — placement only, every
    other route on this box is untouched.
    """
    if LIVE_ORDER_PLACEMENT:
        auth_header = request.headers.get("authorization", "")

        def _forward() -> dict:
            resp = requests.post(
                f"{LIVE_ORDER_BASE_URL}/trade/positions/place-order",
                json=body.model_dump(),
                headers={"Authorization": auth_header, "Content-Type": "application/json"},
                timeout=30,
            )
            return resp.json()

        try:
            return await asyncio.to_thread(_forward)
        except Exception as exc:
            log.error("[LIVE_ORDER_PLACEMENT] forward to %s failed: %s", LIVE_ORDER_BASE_URL, exc)
            return {"status": "error", "message": f"Live order forward failed: {exc}", "results": []}

    return await _place_manual_order_and_notify(body, str(current_user.get("_id") or ""))


@order_router.post("/internal/place-order")
async def internal_place_manual_order(body: ManualOrderRequest, _: None = Depends(_verify_internal_token)) -> dict:
    """
    Same place-order flow as simulator_place_manual_order, for server-to-server
    callers with no logged-in user to hold a JWT — webhook-triggered strategy
    creation (algo.simulator's _simulator_pt_webhook_create_strategy) and the
    live SL/TG adjustment monitor (simulator_risk_monitor.py's
    _fire_broker_adjustment) call this instead of placing the order in-process
    on their own box. This is what makes every live order, regardless of which
    service/box actually initiated it, talk to the broker from THIS box — the
    one whitelisted with Dhan for live order placement.
    """
    return await _place_manual_order_and_notify(body, "internal-service")


# ════════════════════════════════════════════════════════════════════════════
# SL/Target triggers, adjustments, alert-config, sl-marker — copied from
# algo.simulator/api.py (lines ~4102-4363) and simulator/api_server.py
# (lines ~536-643)
# ════════════════════════════════════════════════════════════════════════════

class PTTriggerIn(BaseModel):
    broker_id: str
    leg_id: str
    underlying: Optional[str] = None
    expiry: Optional[str] = None
    strike: Optional[float] = None
    option_type: Optional[str] = None
    side: Optional[str] = None
    sl_mode: str
    sl_value: float
    tp_mode: str
    tp_value: float
    entry_price: float
    quantity: int
    exited: Optional[bool] = False


class PortfolioLegSnapshot(BaseModel):
    leg_id: str
    quantity: int


class PTPortfolioTriggerIn(BaseModel):
    broker_id: str
    underlying: str
    sl_upper: Optional[float] = None
    sl_lower: Optional[float] = None
    legs_snapshot: list[PortfolioLegSnapshot] = []


class PTAlertConfigLegSnapshot(BaseModel):
    leg_id: str
    quantity: int
    entry_price: float
    side: str


class PTAlertConfigToggle(BaseModel):
    enabled: bool = False
    unit: str = "points"
    value: float = 0.0


class PTAlertConfigTrailingStop(BaseModel):
    enabled: bool = False
    unit: str = "points"
    x: float = 0.0
    y: float = 0.0


class PTAlertConfigHedgeStrikeType(BaseModel):
    enabled: bool = False
    mode: str = "delta"
    value: float = 0.0
    strike: str = "ATM"


class PTAlertConfigHedgeTimeControl(BaseModel):
    enabled: bool = False
    entry_time: str = "09:15"
    exit_time: str = "15:30"


class PTAlertConfigIn(BaseModel):
    broker_id: str
    underlying: str
    trading_mode: str = "auto"
    stoploss: PTAlertConfigToggle
    target: PTAlertConfigToggle
    trailing_stop: PTAlertConfigTrailingStop
    hedge_strike_type: PTAlertConfigHedgeStrikeType
    hedge_time_control: PTAlertConfigHedgeTimeControl
    legs_snapshot: list[PTAlertConfigLegSnapshot] = []


class AdjustmentPositionIn(BaseModel):
    side: str
    lots: int
    qty: int
    strike: float
    option_type: str
    expiry: str
    entry_price: float
    tag: str  # "EXIT" | "NEW"


class PTAdjustmentIn(BaseModel):
    broker_id: Optional[str] = None
    underlying: Optional[str] = None
    strategy_id: Optional[str] = None
    trigger_condition: Optional[str] = None
    trigger_price: Optional[float] = None
    positions: list[AdjustmentPositionIn] = []
    status: bool = True


class PTAdjustmentPatchIn(BaseModel):
    positions: list[AdjustmentPositionIn] = []
    trigger_price: Optional[float] = None
    trigger_condition: Optional[str] = None


@order_router.post("/simulator/paper-trade/triggers")
async def simulator_pt_save_trigger(body: PTTriggerIn, current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    """
    Upserts the SL/Target a leg's "Add Alert"/"Update Alert" toggle was set to, keyed by
    (broker_id, leg_id) — always overwrites rather than no-op'ing on an existing doc.
    """
    try:
        col = _shared_mongo._db["simulator_triggers"]
        now_str = datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S")
        col.update_one(
            {"broker_id": body.broker_id, "leg_id": body.leg_id},
            {
                "$set": {
                    "underlying": body.underlying, "expiry": body.expiry, "strike": body.strike,
                    "option_type": body.option_type, "side": body.side,
                    "sl_mode": body.sl_mode, "sl_value": body.sl_value,
                    "tp_mode": body.tp_mode, "tp_value": body.tp_value,
                    "entry_price_at_set": body.entry_price, "quantity_at_set": body.quantity,
                    "exited_at_set": body.exited,
                    "status": "active", "updated_at": now_str,
                },
                "$setOnInsert": {"created_at": now_str},
            },
            upsert=True,
        )
        return {"status": "success"}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@order_router.post("/simulator/paper-trade/portfolio-triggers")
async def simulator_pt_save_portfolio_trigger(body: PTPortfolioTriggerIn, current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    """
    Upserts the payoff chart's upper/lower stoploss marker for a whole basket, keyed by
    (broker_id, underlying).
    """
    try:
        col = _shared_mongo._db["simulator_portfolio_triggers"]
        now_str = datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S")
        snapshot = sorted(
            ({"leg_id": s.leg_id, "quantity": s.quantity} for s in body.legs_snapshot if s.quantity > 0),
            key=lambda s: s["leg_id"],
        )
        col.update_one(
            {"broker_id": body.broker_id, "underlying": body.underlying},
            {
                "$set": {
                    "sl_upper": body.sl_upper, "sl_lower": body.sl_lower,
                    "legs_snapshot": snapshot,
                    "status": "active", "updated_at": now_str,
                },
                "$setOnInsert": {"created_at": now_str},
            },
            upsert=True,
        )
        return {"status": "success"}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@order_router.get("/simulator/paper-trade/alert-config")
async def simulator_pt_get_alert_config(broker_id: str = Query(...), underlying: str = Query(...), current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    try:
        doc = _shared_mongo._db["simulator_portfolio_triggers"].find_one(
            {"broker_id": broker_id, "underlying": underlying},
        ) or {}
        return {
            "status": "success",
            "trading_mode": doc.get("alert_trading_mode") or "auto",
            "stoploss": doc.get("alert_stoploss") or {},
            "target": doc.get("alert_target") or {},
            "trailing_stop": doc.get("alert_trailing_stop") or {},
            "hedge_strike_type": doc.get("alert_hedge_strike_type") or {},
            "hedge_time_control": doc.get("alert_hedge_time_control") or {},
        }
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@order_router.post("/simulator/paper-trade/alert-config")
async def simulator_pt_save_alert_config(body: PTAlertConfigIn, current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    """
    Upserts the "Position Configuration" panel's basket-level Stoploss/Target/
    Trail SL/Hedge settings into the SAME doc as the payoff-chart sl_upper/
    sl_lower marker (simulator_portfolio_triggers), under separately-namespaced
    alert_* fields. The live ratcheting itself happens in
    features/simulator_risk_monitor.py, not here.
    """
    try:
        col = _shared_mongo._db["simulator_portfolio_triggers"]
        now_str = datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S")
        snapshot = [s.model_dump() for s in body.legs_snapshot if s.quantity > 0]
        col.update_one(
            {"broker_id": body.broker_id, "underlying": body.underlying},
            {
                "$set": {
                    "alert_trading_mode": body.trading_mode,
                    "alert_stoploss": body.stoploss.model_dump(),
                    "alert_target": body.target.model_dump(),
                    "alert_trailing_stop": body.trailing_stop.model_dump(),
                    "alert_hedge_strike_type": body.hedge_strike_type.model_dump(),
                    "alert_hedge_time_control": body.hedge_time_control.model_dump(),
                    "alert_legs_snapshot": snapshot,
                    "alert_peak_mtm": 0.0,
                    "alert_status": "active",
                    "alert_updated_at": now_str,
                },
                "$setOnInsert": {"broker_id": body.broker_id, "underlying": body.underlying, "created_at": now_str},
            },
            upsert=True,
        )
        return {"status": "success"}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@order_router.get("/simulator/paper-trade/adjustments")
async def simulator_pt_list_adjustments(
    broker_id: Optional[str] = Query(default=None),
    underlying: Optional[str] = Query(default=None),
    strategy_id: Optional[str] = Query(default=None),
    current_user: dict = Depends(app_auth.get_current_user),
) -> dict:
    """
    The "🔔 Alert" bottom-sheet's saved reverse-order preview — plain CRUD, no drift-check.
    Keyed by (broker_id, underlying) for the live-broker view, or by strategy_id for a
    saved/virtual strategy.
    """
    try:
        query = {"strategy_id": strategy_id} if strategy_id else {"broker_id": broker_id, "underlying": underlying}
        query["status"] = {"$ne": False}
        docs = list(_shared_mongo._db["simulator_adjustments"].find(query).sort("updated_at", -1))
        for d in docs:
            d["_id"] = str(d["_id"])
        return {"status": "success", "adjustments": docs}
    except Exception as exc:
        return {"status": "error", "message": str(exc), "adjustments": []}


@order_router.post("/simulator/paper-trade/adjustments")
async def simulator_pt_create_adjustment(body: PTAdjustmentIn, current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    try:
        now_str = datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S")
        doc = body.model_dump()
        doc["created_at"] = now_str
        doc["updated_at"] = now_str
        result = _shared_mongo._db["simulator_adjustments"].insert_one(doc)
        return {"status": "success", "id": str(result.inserted_id)}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@order_router.patch("/simulator/paper-trade/adjustments/{adjustment_id}")
async def simulator_pt_update_adjustment(adjustment_id: str, body: PTAdjustmentPatchIn, current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    try:
        update: dict = {"updated_at": datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S")}
        update["positions"] = [p.model_dump() for p in body.positions]
        update["status"] = True
        update["webhook_error"] = None
        if body.trigger_price is not None:
            update["trigger_price"] = body.trigger_price
        if body.trigger_condition is not None:
            update["trigger_condition"] = body.trigger_condition
        _shared_mongo._db["simulator_adjustments"].update_one({"_id": ObjectId(adjustment_id)}, {"$set": update})
        return {"status": "success"}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@order_router.delete("/simulator/paper-trade/adjustments")
async def simulator_pt_delete_adjustment(
    trigger_condition: str = Query(...),
    broker_id: Optional[str] = Query(default=None),
    underlying: Optional[str] = Query(default=None),
    strategy_id: Optional[str] = Query(default=None),
    current_user: dict = Depends(app_auth.get_current_user),
) -> dict:
    """
    Removing the payoff chart's Upper/Lower SL marker must also drop that side's saved
    "🔔 Alert" reverse-order basket — otherwise the marker's price is gone but its
    adjustment positions silently linger in simulator_adjustments.
    """
    try:
        query: dict = {"trigger_condition": trigger_condition}
        query.update({"strategy_id": strategy_id} if strategy_id else {"broker_id": broker_id, "underlying": underlying})
        result = _shared_mongo._db["simulator_adjustments"].delete_many(query)
        return {"status": "success", "deleted": result.deleted_count}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


# ─── Saved-strategy variants (alert-config / sl-marker keyed by strategy doc, not broker+underlying) ───

class PTPositionRiskIn(BaseModel):
    index: int
    sl_mode: str = "percent"
    sl_value: float = 0.0
    tp_mode: str = "percent"
    tp_value: float = 0.0


class PTStrategyAlertConfigIn(BaseModel):
    positions: List[PTPositionRiskIn] = []
    trading_mode: str = "auto"
    stoploss: Dict[str, Any] = {}
    target: Dict[str, Any] = {}
    trailing_stop: Dict[str, Any] = {}
    hedge_strike_type: Dict[str, Any] = {}
    hedge_time_control: Dict[str, Any] = {}


class PTStrategySlMarkerIn(BaseModel):
    sl_upper: Optional[float] = None
    sl_lower: Optional[float] = None


@order_router.put("/simulator/paper-trade/strategies/{strategy_id}/alert-config")
async def pt_save_strategy_alert_config(strategy_id: str, body: PTStrategyAlertConfigIn, current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    """
    Persists Stoploss/Target (per-leg + basket) directly onto this saved
    strategy's own doc — a saved/virtual strategy has no broker_id/leg_id, so
    this can't reuse simulator_triggers/simulator_portfolio_triggers.
    features/simulator_risk_monitor.py reads these same fields back to
    check/fire (paper exit only — no real broker order).
    """
    try:
        doc = _find_owned_strategy(strategy_id, current_user)
        if not doc:
            return {"status": "error", "message": "Not found"}

        positions = list(doc.get("positions") or [])
        risk_by_index = {r.index: r for r in body.positions}
        for i, pos in enumerate(positions):
            risk = risk_by_index.get(i)
            if risk:
                pos["sl_mode"] = risk.sl_mode
                pos["sl_value"] = risk.sl_value
                pos["tp_mode"] = risk.tp_mode
                pos["tp_value"] = risk.tp_value

        now_str = datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S")
        _simulator_strategy_col.update_one(
            {"_id": ObjectId(strategy_id)},
            {"$set": {
                "positions": positions,
                "alert_trading_mode": body.trading_mode,
                "alert_stoploss": body.stoploss,
                "alert_target": body.target,
                "alert_trailing_stop": body.trailing_stop,
                "alert_hedge_strike_type": body.hedge_strike_type,
                "alert_hedge_time_control": body.hedge_time_control,
                "alert_peak_mtm": 0.0,
                "alert_status": "active",
                "alert_updated_at": now_str,
            }},
        )
        return {"status": "success"}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@order_router.put("/simulator/paper-trade/strategies/{strategy_id}/sl-marker")
async def pt_save_strategy_sl_marker(strategy_id: str, body: PTStrategySlMarkerIn, current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    """
    Persists the payoff chart's upper/lower stoploss marker directly onto
    this saved strategy's own doc. The live-broker counterpart is keyed by
    (broker_id, underlying) instead (see simulator_pt_save_portfolio_trigger).
    """
    try:
        if not _find_owned_strategy(strategy_id, current_user):
            return {"status": "error", "message": "Not found"}
        now_str = datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S")
        result = _simulator_strategy_col.update_one(
            {"_id": ObjectId(strategy_id)},
            {"$set": {
                "sl_upper": body.sl_upper,
                "sl_lower": body.sl_lower,
                "sl_marker_status": "active",
                "sl_marker_updated_at": now_str,
            }},
        )
        if result.matched_count == 0:
            return {"status": "error", "message": "Not found"}
        return {"status": "success"}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


# ════════════════════════════════════════════════════════════════════════════
# Generic broker execution gateway — used by shared/features/live_order_manager.py
# (algo.trade's live SL/TG/entry/squareoff pipeline) instead of calling the broker
# adapter in-process. algo.trade still owns every decision (tick processing, when
# to fire, price/qty, registry bookkeeping) — only the final "talk to the broker"
# call crosses the process boundary to land here.
# ════════════════════════════════════════════════════════════════════════════

def _resolve_broker_adapter(raw_db, trade_broker_id: str | None):
    """
    Port of live_order_manager.py's get_broker_for_trade — same
    broker_configuration/kite_market_config lookup and Dhan/FlatTrade/Kite
    branch, just keyed by an id string instead of a trade dict (algo.trade
    resolves which trade this is; only the broker identity crosses the
    process boundary).
    """
    broker_id = str(trade_broker_id or "").strip()

    if broker_id:
        try:
            from features.flattrade_broker import _is_flattrade_doc, get_flattrade_instance
            from features.dhan_broker import _is_dhan_doc, get_dhan_instance
            broker_doc = raw_db["broker_configuration"].find_one(
                {"_id": ObjectId(broker_id)},
                {"access_token": 1, "user_id": 1, "name": 1, "broker_icon": 1, "broker_user_id": 1},
            ) or {}
            access_token = str(broker_doc.get("access_token") or "").strip()
            if access_token and _is_flattrade_doc(broker_doc):
                user_id = str(broker_doc.get("user_id") or "").strip()
                ft = get_flattrade_instance(user_id, access_token)
                if ft:
                    return ft
            elif access_token and _is_dhan_doc(broker_doc):
                client_id = str(broker_doc.get("broker_user_id") or broker_doc.get("user_id") or "").strip()
                dhan = get_dhan_instance(_shared_mongo, client_id, access_token)
                if dhan:
                    return dhan
            elif access_token:
                from features.broker_gateway import get_broker_rest_client_with_token as get_kite_instance
                return get_kite_instance(access_token)
        except Exception as exc:
            log.debug("broker lookup error broker=%s: %s", broker_id, exc)

    # Fallback — default broker via kite_market_config (Kite or Dhan, whichever is enabled)
    try:
        market_cfg = raw_db["kite_market_config"].find_one(
            {"enabled": True}, {"broker": 1, "access_token": 1, "user_id": 1, "dhan_client_id": 1},
        ) or {}
        access_token = str(market_cfg.get("access_token") or "").strip()
        if access_token and str(market_cfg.get("broker") or "").strip().lower() == "dhan":
            from features.dhan_broker import get_dhan_instance
            client_id = str(market_cfg.get("user_id") or market_cfg.get("dhan_client_id") or "").strip()
            dhan = get_dhan_instance(_shared_mongo, client_id, access_token)
            if dhan:
                return dhan
        elif access_token:
            from features.broker_gateway import get_broker_rest_client_with_token as get_kite_instance
            return get_kite_instance(access_token)
    except Exception as exc:
        log.debug("market config token lookup error: %s", exc)

    return None


class BrokerPlaceOrderRequest(BaseModel):
    trade_broker_id: str = ""
    tradingsymbol: str
    exchange: str
    transaction_type: str
    quantity: int
    order_type: str
    product: str
    variety: str = "regular"
    price: float = 0.0
    trigger_price: float = 0.0
    validity: str = "DAY"
    context: dict = {}
    broker_kwargs: dict = {}


class BrokerCancelOrderRequest(BaseModel):
    trade_broker_id: str = ""
    variety: str = "regular"
    order_id: str


class BrokerModifyOrderRequest(BaseModel):
    trade_broker_id: str = ""
    order_id: str
    order_type: str
    price: float
    trigger_price: float
    exchange: str
    tradingsymbol: str
    quantity: int


@order_router.get("/broker/orders")
async def get_broker_orders(broker_id: str = Query(...), current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    """
    Returns this broker account's order book straight from Dhan/Kite/FlatTrade's
    own orders() API — not our own DB snapshot — so the Orders tab shows the
    broker's real, current status (COMPLETE/OPEN/REJECTED/CANCELLED/TRIGGER
    PENDING) for every order, live. Every broker's own order-book endpoint already
    only returns the current trading day's orders, so no separate date filter
    needed here — reusing the same _resolve_broker_adapter the internal /broker/
    place gateway uses, just with user-JWT auth instead of the internal token
    (this is a user opening a tab in the UI, not a server-to-server call).
    """
    raw_db = _shared_mongo._db
    adapter = await asyncio.to_thread(_resolve_broker_adapter, raw_db, broker_id)
    if not adapter:
        return {"status": "error", "message": "Broker not resolved.", "orders": []}
    try:
        orders = await asyncio.to_thread(adapter.orders)
    except Exception as exc:
        return {"status": "error", "message": str(exc), "orders": []}
    return {"status": "success", "orders": orders}


class RetryOrderRequest(BaseModel):
    broker_id: str
    tradingsymbol: str
    exchange: str
    transaction_type: str
    quantity: int
    product: str
    price: float = 0.0
    trigger_price: float = 0.0


@order_router.post("/trade/positions/retry-order")
async def retry_broker_order(body: RetryOrderRequest, current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    """
    Re-places a REJECTED/CANCELLED order from the Orders tab (see
    get_broker_orders above) as a brand-new order — once a broker has
    terminated an order there is nothing left to modify, so this is the only
    option for those two statuses (an OPEN/TRIGGER_PENDING order should hit
    modify_broker_order below instead, same order_id, no duplicate). Same
    {"order_id","status","message","raw"} contract as broker_place_order
    below, just reachable with a user JWT (that one's server-to-server only)
    since this is a user clicking "Retry" in the UI.

    order_type isn't available here — neither adapter's orders() (dhan_broker.py
    /flattrade_broker.py) returns the original order type, only price/trigger_price
    — so it's inferred the same way a trader would read the row back: a
    trigger_price means it was a stop-loss order, a price with no trigger means
    LIMIT, and neither means MARKET.
    """
    raw_db = _shared_mongo._db
    adapter = await asyncio.to_thread(_resolve_broker_adapter, raw_db, body.broker_id)
    if not adapter:
        return {"order_id": "", "status": "error", "message": "Broker not resolved.", "raw": None}
    order_type = "SL" if body.trigger_price > 0 else ("LIMIT" if body.price > 0 else "MARKET")
    from features.order_execution import place_broker_order
    result = await asyncio.to_thread(
        place_broker_order,
        adapter,
        tradingsymbol=body.tradingsymbol,
        exchange=body.exchange,
        transaction_type=body.transaction_type,
        quantity=body.quantity,
        order_type=order_type,
        product=body.product,
        price=body.price,
        trigger_price=body.trigger_price,
        context={"user_id": str(current_user.get("_id") or ""), "retry": True},
    )
    return result


class ModifyOrderRequest(BaseModel):
    broker_id: str
    order_id: str
    tradingsymbol: str
    exchange: str
    quantity: int
    price: float = 0.0
    trigger_price: float = 0.0


@order_router.post("/trade/positions/modify-order")
async def modify_broker_order(body: ModifyOrderRequest, current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    """
    Sensibull-style "retry a stuck order" — an order still OPEN/TRIGGER_PENDING
    at the broker is still live and modifiable, so re-send it as a modify on
    the SAME order_id instead of retry_broker_order's place-a-new-order path
    (which would leave two live orders for one leg). User-JWT-reachable twin
    of broker_modify_order below (that one's server-to-server only).

    order_type inferred from price/trigger_price the same way retry_broker_order
    does — the broker's order-book response never echoes back the original type.
    """
    raw_db = _shared_mongo._db
    adapter = await asyncio.to_thread(_resolve_broker_adapter, raw_db, body.broker_id)
    if not adapter:
        return {"status": "error", "message": "Broker not resolved."}
    order_type = "SL" if body.trigger_price > 0 else ("LIMIT" if body.price > 0 else "MARKET")
    try:
        new_order_id = await asyncio.to_thread(
            adapter.modify_order,
            order_id=body.order_id,
            order_type=order_type,
            price=body.price,
            trigger_price=body.trigger_price,
            exchange=body.exchange,
            tradingsymbol=body.tradingsymbol,
            quantity=body.quantity,
        )
    except Exception as exc:
        return {"status": "error", "message": str(exc)}
    return {"status": "success", "order_id": new_order_id}


class RepeatOrderRequest(BaseModel):
    broker_id: str
    security_id: str
    exchange_segment: str
    tradingsymbol: str
    exchange: str
    transaction_type: str
    quantity: int
    product: str
    order_type: str          # "MARKET" / "LIMIT" / "MPP" / "LTP"
    price: float = 0.0
    trigger_price: float = 0.0


@order_router.post("/trade/positions/repeat-order")
async def repeat_broker_order(body: RepeatOrderRequest, current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    """
    Orderbook's "repeat this order" action — always places a brand-new order (unlike
    modify_broker_order, which reuses the same order_id for a still-live one). Dhan-only
    for now (see feedback memory: broker features land per-file, incrementally): MPP/LTP
    resolve Dhan's live feed straight off security_id/exchange_segment (both already sat
    on the Orderbook row from DhanAdapter.orders()) instead of re-parsing underlying/
    expiry/strike out of tradingSymbol — Dhan's own tradingSymbol doesn't reliably encode
    which week's contract it is (see _resolve_dhan_security's docstring above), but
    securityId always identifies the exact instrument the original order was for.
    """
    raw_db = _shared_mongo._db
    dhan_cfg = raw_db["kite_market_config"].find_one({"broker": "dhan"}) or {}
    if body.broker_id != str(dhan_cfg.get("_id") or "").strip():
        return {"status": "error", "message": "Repeat Order currently supports Dhan accounts only.", "results": []}
    dhan_client_id = str(dhan_cfg.get("user_id") or dhan_cfg.get("dhan_client_id") or "").strip()
    dhan_access_token = str(dhan_cfg.get("access_token") or "").strip()
    if not dhan_access_token or not dhan_client_id:
        return {"status": "error", "message": "Dhan credentials not configured."}

    price = body.price
    dhan_order_type = body.order_type.upper()
    if dhan_order_type in ("MPP", "LTP"):
        quote = (await asyncio.to_thread(
            _fetch_dhan_market_data, body.exchange_segment, [int(body.security_id)], _shared_mongo,
        )).get(body.security_id, {})
        ltp = float(quote.get("ltp") or 0)
        if ltp <= 0:
            print(f"[REPEAT_ORDER] no live {dhan_order_type} price for security_id={body.security_id}", flush=True)
            return {"status": "error", "message": "No live price available for this contract — order NOT placed."}
        price = ltp
        dhan_order_type = "LIMIT"

    from features.dhan_broker import get_dhan_instance
    from features.order_execution import place_broker_order
    dhan_adapter = get_dhan_instance(_shared_mongo, dhan_client_id, dhan_access_token)
    result = await asyncio.to_thread(
        place_broker_order,
        dhan_adapter,
        tradingsymbol=body.tradingsymbol,
        exchange=body.exchange,
        transaction_type=body.transaction_type,
        quantity=body.quantity,
        order_type=dhan_order_type,
        product=body.product,
        price=price,
        trigger_price=body.trigger_price,
        context={"purpose": "orderbook_repeat", "broker": "dhan", "symbol": body.tradingsymbol, "user_id": str(current_user.get("_id") or "")},
        broker_kwargs={"security_id": body.security_id, "exchange_segment": body.exchange_segment},
        check_status=False,
    )
    return result


@order_router.post("/broker/place")
async def broker_place_order(body: BrokerPlaceOrderRequest, _: None = Depends(_verify_internal_token)) -> dict:
    """
    Same {"order_id", "status", "message", "raw"} contract as
    features.order_execution.place_broker_order — never raises, any failure
    (adapter not resolved, broker rejection) comes back as status="error" so
    live_order_manager.py's existing result["status"] != "success" checks
    don't need to change.
    """
    raw_db = _shared_mongo._db
    adapter = await asyncio.to_thread(_resolve_broker_adapter, raw_db, body.trade_broker_id)
    if not adapter:
        return {"order_id": "", "status": "error", "message": "Broker adapter not resolved.", "raw": None}
    from features.order_execution import place_broker_order
    result = await asyncio.to_thread(
        place_broker_order,
        adapter,
        tradingsymbol=body.tradingsymbol,
        exchange=body.exchange,
        transaction_type=body.transaction_type,
        quantity=body.quantity,
        order_type=body.order_type,
        product=body.product,
        variety=body.variety,
        price=body.price,
        trigger_price=body.trigger_price,
        validity=body.validity,
        context=body.context,
        broker_kwargs=body.broker_kwargs,
    )
    return result


@order_router.post("/broker/cancel")
async def broker_cancel_order(body: BrokerCancelOrderRequest, _: None = Depends(_verify_internal_token)) -> dict:
    """
    live_order_manager.py's call sites already wrap this in their own
    try/except — a non-2xx here (adapter not resolved, broker rejection)
    surfaces as an exception on the caller's side, same as a raised
    adapter.cancel_order() call used to.
    """
    raw_db = _shared_mongo._db
    adapter = await asyncio.to_thread(_resolve_broker_adapter, raw_db, body.trade_broker_id)
    if not adapter:
        raise HTTPException(status_code=400, detail="Broker adapter not resolved.")
    await asyncio.to_thread(adapter.cancel_order, variety=body.variety, order_id=body.order_id)
    return {"status": "success"}


@order_router.post("/broker/modify")
async def broker_modify_order(body: BrokerModifyOrderRequest, _: None = Depends(_verify_internal_token)) -> dict:
    """Same contract as adapter.modify_order(...) — see broker_cancel_order's note on error propagation."""
    raw_db = _shared_mongo._db
    adapter = await asyncio.to_thread(_resolve_broker_adapter, raw_db, body.trade_broker_id)
    if not adapter:
        raise HTTPException(status_code=400, detail="Broker adapter not resolved.")
    result = await asyncio.to_thread(
        adapter.modify_order,
        order_id=body.order_id,
        order_type=body.order_type,
        price=body.price,
        trigger_price=body.trigger_price,
        exchange=body.exchange,
        tradingsymbol=body.tradingsymbol,
        quantity=body.quantity,
    )
    return result if isinstance(result, dict) else {"status": "success", "raw": result}


app.include_router(order_router)
