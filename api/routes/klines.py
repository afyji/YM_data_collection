"""Kline query API routes."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Request

from YM_data_collection.api.response import ok_response
from YM_data_collection.services.query_service import MarketDataQueryService

router = APIRouter(prefix="/marketdata/klines", tags=["klines"])


def get_query_service(request: Request) -> MarketDataQueryService:
    """FastAPI dependency that resolves the query service from app.state."""
    svc = request.app.state.query_service
    if svc is None:
        raise RuntimeError("query_service not configured on app.state")
    return svc


@router.get("/recent")
async def klines_recent(
    venue: str,
    market_type: str,
    symbol: str,
    interval: str,
    count: int = 20,
    svc: MarketDataQueryService = Depends(get_query_service),
):
    """Return the most recent *count* klines for a symbol/interval."""
    result = svc.query_klines_recent(market_type, symbol, interval, count)
    items = result["data"]
    meta = {
        "count": len(items),
        "source": result["meta"]["source"],
    }
    return ok_response(data={"items": items}, meta=meta)


@router.get("/range")
async def klines_range(
    venue: str,
    market_type: str,
    symbol: str,
    interval: str,
    start_ts_ms: int,
    end_ts_ms: int,
    page: int = 1,
    count: int = 20,
    svc: MarketDataQueryService = Depends(get_query_service),
):
    """Return klines within [start_ts_ms, end_ts_ms] with pagination."""
    result = svc.query_klines_range(
        market_type, symbol, interval, start_ts_ms, end_ts_ms
    )
    all_items = result["data"]
    total = len(all_items)

    # Pagination
    start_idx = (page - 1) * count
    end_idx = start_idx + count
    page_items = all_items[start_idx:end_idx]
    has_next = end_idx < total

    meta = {
        "page": page,
        "count": len(page_items),
        "has_next": has_next,
        "total": total,
        "source": result["meta"]["source"],
    }
    return ok_response(data={"items": page_items}, meta=meta)
