"""Slippage estimation API route.

NOTE: This router must be included in app.py. Add the following to create_app():

    from YM_data_collection.api.routes.slippage import router as slippage_router
    v1.include_router(slippage_router)

before `app.include_router(v1)`.
"""

from __future__ import annotations

from decimal import Decimal
from enum import Enum
from typing import Optional

from fastapi import APIRouter, Depends, Query, Request

from YM_data_collection.api.response import error_response, ok_response
from YM_data_collection.services.slippage_service import SlippageService

router = APIRouter(prefix="/marketdata/slippage", tags=["slippage"])


# ── Helpers ──────────────────────────────────────────────────────────────────


class Side(str, Enum):
    buy = "buy"
    sell = "sell"


def _decimal_to_str(value: Decimal | None) -> str | None:
    """Convert a Decimal to its string representation for JSON serialization."""
    if value is None:
        return None
    return str(value)


def _serialize_result(result: dict) -> dict:
    """Convert Decimal fields in a slippage result dict to strings."""
    decimal_keys = (
        "reference_price",
        "estimated_avg_fill_price",
        "slippage_abs",
        "slippage_bps",
        "filled_qty",
        "unfilled_qty",
    )
    serialized: dict = {}
    for key, value in result.items():
        if key in decimal_keys and isinstance(value, Decimal):
            serialized[key] = _decimal_to_str(value)
        elif key in decimal_keys and value is None:
            serialized[key] = None
        else:
            serialized[key] = value
    return serialized


# ── Dependency injection ─────────────────────────────────────────────────────


def get_slippage_service(request: Request) -> SlippageService:
    return request.app.state.slippage_service


# ── Endpoint ─────────────────────────────────────────────────────────────────


@router.get("/estimate")
def estimate_slippage(
    venue: str = Query(..., description="Trading venue, e.g. binance"),
    market_type: str = Query(..., description="Market type, e.g. spot or perp"),
    symbol: str = Query(..., description="Trading symbol, e.g. BTCUSDT"),
    side: Side = Query(..., description="Trade side: buy or sell"),
    quote_asset_amount: Optional[str] = Query(
        None, description="Notional amount in quote currency"
    ),
    base_asset_amount: Optional[str] = Query(
        None, description="Amount in base currency (not yet supported)"
    ),
    service: SlippageService = Depends(get_slippage_service),
):
    """Estimate slippage for a given trade size by walking the order book."""

    # ── Mutual exclusivity / required validation ─────────────────────────
    if quote_asset_amount is not None and base_asset_amount is not None:
        return error_response(
            code="INVALID_PARAMS",
            message="quote_asset_amount and base_asset_amount are mutually exclusive; provide exactly one",
            status_code=400,
        )

    if quote_asset_amount is None and base_asset_amount is None:
        return error_response(
            code="INVALID_PARAMS",
            message="One of quote_asset_amount or base_asset_amount is required",
            status_code=400,
        )

    if base_asset_amount is not None:
        return error_response(
            code="NOT_SUPPORTED",
            message="base_asset_amount not yet supported, use quote_asset_amount",
            status_code=400,
        )

    # ── Parse amount ─────────────────────────────────────────────────────
    try:
        amount_quote = Decimal(quote_asset_amount)
    except Exception:
        return error_response(
            code="INVALID_PARAMS",
            message="quote_asset_amount must be a valid decimal number",
            status_code=400,
        )

    if amount_quote <= 0:
        return error_response(
            code="INVALID_PARAMS",
            message="quote_asset_amount must be positive",
            status_code=400,
        )

    # ── Call service ─────────────────────────────────────────────────────
    result = service.estimate(
        market_type=market_type,
        symbol=symbol,
        side=side.value,
        amount_quote=amount_quote,
    )

    # ── Serialize Decimals and respond ───────────────────────────────────
    serialized = _serialize_result(result)
    return ok_response(data=serialized)
