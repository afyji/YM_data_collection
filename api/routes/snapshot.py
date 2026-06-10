"""Snapshot and depth query API endpoints.

NOTE: This router must be included in app.py, e.g.:

    from YM_data_collection.api.routes.snapshot import router as snapshot_router
    v1.include_router(snapshot_router)

Do NOT modify app.py from this module — another task handles integration.
"""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, Query, Request

from YM_data_collection.api.response import ok_response
from YM_data_collection.services.query_service import MarketDataQueryService

router = APIRouter(prefix="/marketdata", tags=["marketdata"])


def get_query_service(request: Request) -> MarketDataQueryService:
    """Retrieve the query service from application state."""
    return request.app.state.query_service


@router.get("/snapshot/latest")
async def snapshot_latest(
    venue: str = Query(..., description="Exchange venue identifier"),
    market_type: str = Query(..., description="Market type, e.g. perp"),
    symbol: str = Query(..., description="Trading symbol, e.g. BTCUSDT"),
    query_service: MarketDataQueryService = Depends(get_query_service),
):
    """Return the latest combined snapshot across all data types."""
    data_types = ["mark_price", "index_price", "open_interest", "funding_rate"]

    # Collect individual snapshot results
    combined_data: dict = {}
    combined_meta: dict = {"source": "none", "fallback_used": False, "cache_refreshed": False}
    any_data_found = False

    for dt in data_types:
        result = query_service.query_latest_snapshot(market_type, symbol, dt)
        combined_data[dt] = result["data"]
        meta = result["meta"]
        if result["data"] is not None:
            any_data_found = True
            # If any sub-query found data, promote its source info
            if combined_meta["source"] == "none":
                combined_meta = {
                    "source": meta["source"],
                    "fallback_used": meta["fallback_used"],
                    "cache_refreshed": meta["cache_refreshed"],
                }
            else:
                # If any sub-query used fallback or cache refresh, reflect that
                if meta["fallback_used"]:
                    combined_meta["fallback_used"] = True
                if meta["cache_refreshed"]:
                    combined_meta["cache_refreshed"] = True
                # Prefer cache over mysql, keep most specific source
                if meta["source"] == "cache" and combined_meta["source"] == "mysql":
                    combined_meta["source"] = "cache"

    # Depth snapshot
    depth_result = query_service.query_latest_depth(market_type, symbol)
    combined_data["depth_snapshot"] = depth_result["data"]
    depth_meta = depth_result["meta"]
    if depth_result["data"] is not None:
        any_data_found = True
        if combined_meta["source"] == "none":
            combined_meta = {
                "source": depth_meta["source"],
                "fallback_used": depth_meta["fallback_used"],
                "cache_refreshed": depth_meta["cache_refreshed"],
            }
        else:
            if depth_meta["fallback_used"]:
                combined_meta["fallback_used"] = True
            if depth_meta["cache_refreshed"]:
                combined_meta["cache_refreshed"] = True
            if depth_meta["source"] == "cache" and combined_meta["source"] == "mysql":
                combined_meta["source"] = "cache"

    return ok_response(data=combined_data, meta=combined_meta)


@router.get("/depth/latest")
async def depth_latest(
    venue: str = Query(..., description="Exchange venue identifier"),
    market_type: str = Query(..., description="Market type, e.g. perp"),
    symbol: str = Query(..., description="Trading symbol, e.g. BTCUSDT"),
    levels: Optional[int] = Query(None, description="Number of depth levels to return"),
    query_service: MarketDataQueryService = Depends(get_query_service),
):
    """Return the latest depth snapshot for a symbol."""
    result = query_service.query_latest_depth(market_type, symbol)
    return ok_response(data=result["data"], meta=result["meta"])
