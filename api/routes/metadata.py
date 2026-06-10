"""Metadata, status, quality, and manifest query API routes."""

from __future__ import annotations

import json
from decimal import Decimal
from typing import Any, Optional

from fastapi import APIRouter, Depends, Query, Request

from YM_data_collection.api.response import error_response, ok_response

router = APIRouter(tags=["metadata"])


# ── Dependency injection helpers ─────────────────────────────────────────


def get_instrument_repo(request: Request):
    return request.app.state.instrument_repo


def get_coverage_service(request: Request):
    return request.app.state.coverage_service


def get_checkpoint_repo(request: Request):
    return request.app.state.checkpoint_repo


def get_quality_repo(request: Request):
    return request.app.state.quality_repo


def get_manifest_repo(request: Request):
    return request.app.state.manifest_repo


# ── Serialization helper ─────────────────────────────────────────────────


def _serialize_model(obj: Any) -> dict[str, Any]:
    """Convert a Pydantic model to a JSON-safe dict, turning Decimals to str."""
    data = obj.model_dump()
    return _convert_decimals(data)


def _convert_decimals(obj: Any) -> Any:
    """Recursively convert Decimal values to str for JSON serialization."""
    if isinstance(obj, Decimal):
        return str(obj)
    if isinstance(obj, dict):
        return {k: _convert_decimals(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_convert_decimals(v) for v in obj]
    return obj


# ── Endpoints ────────────────────────────────────────────────────────────


@router.get("/metadata/instruments")
async def list_instruments(
    venue: Optional[str] = Query(None),
    market_type: Optional[str] = Query(None),
    is_active: bool = Query(True),
    repo=Depends(get_instrument_repo),
):
    """Return instrument metadata, optionally filtered."""
    if is_active:
        instruments = repo.list_active()
    else:
        instruments = repo.list_all()

    # Apply optional filters
    if venue is not None:
        instruments = [i for i in instruments if i.venue == venue]
    if market_type is not None:
        instruments = [i for i in instruments if i.market_type == market_type]

    items = [_serialize_model(i) for i in instruments]
    return ok_response(
        data={"items": items},
        meta={"count": len(items), "source": "mysql"},
    )


@router.get("/metadata/coverage")
async def get_coverage(
    venue: str = Query(...),
    market_type: str = Query(...),
    symbol: str = Query(...),
    data_type: str = Query(...),
    interval: Optional[str] = Query(None),
    svc=Depends(get_coverage_service),
):
    """Return data coverage information for a symbol."""
    result = svc.get_coverage(venue, market_type, symbol, data_type, interval)
    if result is None:
        return error_response(
            code="NOT_FOUND",
            message="No coverage found for the specified parameters",
            status_code=404,
        )
    return ok_response(
        data=result,
        meta={"source": "mysql"},
    )


@router.get("/metadata/status")
async def get_status(
    venue: str = Query(...),
    market_type: str = Query(...),
    symbol: str = Query(...),
    data_type: str = Query(...),
    interval: Optional[str] = Query(None),
    repo=Depends(get_checkpoint_repo),
):
    """Return ingestion checkpoint / status for a symbol."""
    checkpoint = repo.get(venue, market_type, symbol, data_type, interval)
    if checkpoint is None:
        return error_response(
            code="NOT_FOUND",
            message="No checkpoint found for the specified parameters",
            status_code=404,
        )
    checkpoint_data = _serialize_model(checkpoint)
    return ok_response(
        data={"checkpoint": checkpoint_data},
        meta={"source": "mysql"},
    )


@router.get("/metadata/quality-issues")
async def list_quality_issues(
    symbol: Optional[str] = Query(None),
    data_type: Optional[str] = Query(None),
    status_filter: str = Query("open"),
    page: int = Query(1, ge=1),
    count: int = Query(20, ge=1),
    repo=Depends(get_quality_repo),
):
    """Return data quality issues, with optional filtering and pagination."""
    if symbol is not None:
        issues = repo.list_by_symbol(symbol, data_type)
    else:
        issues = repo.list_by_status(status_filter)

    # Pagination
    total = len(issues)
    start_idx = (page - 1) * count
    end_idx = start_idx + count
    page_issues = issues[start_idx:end_idx]

    items = [_serialize_model(i) for i in page_issues]
    return ok_response(
        data={"items": items},
        meta={"page": page, "count": len(items), "total": total, "source": "mysql"},
    )


@router.get("/datasets/manifests")
async def list_manifests(
    dataset_name: Optional[str] = Query(None),
    symbol: Optional[str] = Query(None),
    data_type: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    count: int = Query(20, ge=1),
    repo=Depends(get_manifest_repo),
):
    """Return file manifests, with optional filtering and pagination."""
    if dataset_name is not None:
        manifests = repo.list_by_dataset(dataset_name)
    elif symbol is not None:
        manifests = repo.list_by_symbol(symbol, data_type)
    else:
        manifests = []

    # Pagination
    total = len(manifests)
    start_idx = (page - 1) * count
    end_idx = start_idx + count
    page_manifests = manifests[start_idx:end_idx]

    items = [_serialize_model(m) for m in page_manifests]
    return ok_response(
        data={"items": items},
        meta={"page": page, "count": len(items), "total": total, "source": "mysql"},
    )


@router.get("/datasets/manifests/detail")
async def get_manifest_detail(
    dataset_name: str = Query(...),
    symbol: str = Query(...),
    data_type: Optional[str] = Query(None),
    repo=Depends(get_manifest_repo),
):
    """Return a single manifest matched by dataset_name + symbol + data_type."""
    manifests = repo.list_by_symbol(symbol, data_type)
    # Filter by dataset_name
    match = None
    for m in manifests:
        if m.dataset_name == dataset_name:
            match = m
            break

    if match is None:
        return error_response(
            code="NOT_FOUND",
            message="No manifest found for the specified parameters",
            status_code=404,
        )
    return ok_response(
        data=_serialize_model(match),
        meta={"source": "mysql"},
    )
