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
    data = _convert_decimals(data)
    # DC-T063: expose manifest_id (from model.id) as the stable external identifier
    from YM_data_collection.domain.models import FileManifest
    if isinstance(obj, FileManifest):
        if obj.id is not None:
            data["manifest_id"] = obj.id
        data.pop("id", None)
    return data


def _convert_decimals(obj: Any) -> Any:
    """Recursively convert Decimal values to str for JSON serialization."""
    if isinstance(obj, Decimal):
        return str(obj)
    if isinstance(obj, dict):
        return {k: _convert_decimals(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_convert_decimals(v) for v in obj]
    return obj


def _normalize_interval_key(data: dict[str, Any]) -> dict[str, Any]:
    """Rename interval_code -> interval in a dict for API response."""
    if "interval_code" in data:
        data = dict(data)
        data["interval"] = data.pop("interval_code")
    return data


# ── Endpoints ────────────────────────────────────────────────────────────


@router.get("/metadata/instruments")
async def list_instruments(
    venue: Optional[str] = Query(None),
    market_type: Optional[str] = Query(None),
    is_active: bool = Query(True),
    page: int = Query(1, ge=1),
    count: int = Query(20, ge=1),
    repo=Depends(get_instrument_repo),
):
    """Return instrument metadata, optionally filtered, with pagination."""
    if is_active:
        instruments = repo.list_active()
    else:
        instruments = repo.list_all()

    # Apply optional filters
    if venue is not None:
        instruments = [i for i in instruments if i.venue == venue]
    if market_type is not None:
        instruments = [i for i in instruments if i.market_type == market_type]

    # Pagination
    total = len(instruments)
    start_idx = (page - 1) * count
    end_idx = start_idx + count
    page_instruments = instruments[start_idx:end_idx]

    items = [_serialize_model(i) for i in page_instruments]
    return ok_response(
        data={"items": items},
        meta={"page": page, "count": len(items), "total": total, "source": "mysql"},
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
    # Normalize interval_code -> interval for API consumers
    if isinstance(result, dict):
        result = _normalize_interval_key(result)
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
    """Return ingestion status for a symbol with flattened shape."""
    checkpoint = repo.get(venue, market_type, symbol, data_type, interval)
    if checkpoint is None:
        return error_response(
            code="NOT_FOUND",
            message="No checkpoint found for the specified parameters",
            status_code=404,
        )
    checkpoint_data = _serialize_model(checkpoint)

    # Build flattened response: top-level identity/status + checkpoint sub-object
    # Normalize interval_code -> interval in both top-level and checkpoint
    interval_val = checkpoint_data.pop("interval_code", None)
    checkpoint_data["interval"] = interval_val

    # Extract status-related fields to top level
    data: dict[str, Any] = {
        "venue": checkpoint_data.pop("venue"),
        "market_type": checkpoint_data.pop("market_type"),
        "symbol": checkpoint_data.pop("symbol"),
        "data_type": checkpoint_data.pop("data_type"),
        "interval": interval_val,
        "status": checkpoint_data.pop("status"),
        "last_success_at_utc": checkpoint_data.pop("last_success_at_utc", None),
        "last_error_message": checkpoint_data.pop("last_error_message", None),
        "checkpoint": checkpoint_data,
    }
    return ok_response(
        data=data,
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
    manifest_id: int = Query(...),
    repo=Depends(get_manifest_repo),
):
    """Return a single manifest matched by manifest_id."""
    match = repo.get_by_id(manifest_id)
    if match is None:
        return error_response(
            code="NOT_FOUND",
            message="No manifest found for the specified manifest_id",
            status_code=404,
        )
    return ok_response(
        data=_serialize_model(match),
        meta={"source": "mysql"},
    )


@router.get("/datasets/download")
async def download_dataset(
    manifest_id: int = Query(...),
    repo=Depends(get_manifest_repo),
):
    """Download a dataset file by manifest_id."""
    from fastapi.responses import FileResponse
    from pathlib import Path

    manifest = repo.get_by_id(manifest_id)
    if manifest is None:
        return error_response(
            code="NOT_FOUND",
            message="No manifest found for the specified manifest_id",
            status_code=404,
        )
    file_path = Path(manifest.file_path)
    if not file_path.is_file():
        return error_response(
            code="NOT_FOUND",
            message="File not found on disk",
            status_code=404,
        )
    return FileResponse(
        path=str(file_path),
        filename=file_path.name,
        media_type="application/octet-stream",
    )
