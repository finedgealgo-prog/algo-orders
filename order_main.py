"""
algo.order entrypoint.

Run (from /media/ashok-innoppl/7CD60970D6092C48/algo-backend/algo.order):
    uvicorn order_main:app --reload --port 8004
"""

from api import app  # noqa: F401
