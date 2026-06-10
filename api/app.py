"""FastAPI application factory for the YM data-collection service."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, FastAPI

from YM_data_collection.api.auth import create_auth_dependency
from YM_data_collection.api.middleware import RequestIdMiddleware
from YM_data_collection.api.routes.klines import router as klines_router
from YM_data_collection.api.routes.metadata import router as metadata_router
from YM_data_collection.api.routes.slippage import router as slippage_router
from YM_data_collection.api.routes.snapshot import router as snapshot_router
from YM_data_collection.api.routes.system import router as system_router
from YM_data_collection.config.models import DataCollectionConfig
from YM_data_collection.ws.handler import create_ws_endpoint
from YM_data_collection.ws.hub import ConnectionHub


def create_app(
    config: DataCollectionConfig,
    *,
    query_service: Any = None,
    snapshot_service: Any = None,
    slippage_service: Any = None,
    coverage_service: Any = None,
    instrument_repo: Any = None,
    quality_repo: Any = None,
    manifest_repo: Any = None,
    checkpoint_repo: Any = None,
    health_checker: Any = None,
) -> FastAPI:
    """Build and return a fully-configured FastAPI application."""

    app = FastAPI(
        title="YM Data Collection API",
        docs_url="/docs" if config.service.api_docs_enabled else None,
        redoc_url="/redoc" if config.service.api_docs_enabled else None,
        openapi_url="/openapi.json" if config.service.api_docs_enabled else None,
    )

    # ── Store services on app.state ────────────────────────────────────
    app.state.query_service = query_service
    app.state.snapshot_service = snapshot_service
    app.state.slippage_service = slippage_service
    app.state.coverage_service = coverage_service
    app.state.instrument_repo = instrument_repo
    app.state.quality_repo = quality_repo
    app.state.manifest_repo = manifest_repo
    app.state.checkpoint_repo = checkpoint_repo
    app.state.health_checker = health_checker
    app.state.start_time_utc = datetime.now(timezone.utc)

    # ── Middleware ──────────────────────────────────────────────────────
    if config.service.request_id_enabled:
        app.add_middleware(RequestIdMiddleware)

    # ── Auth dependency ────────────────────────────────────────────────
    auth_dep = create_auth_dependency(config.auth)

    # ── Router (prefix /api/v1) ────────────────────────────────────────
    v1 = APIRouter(prefix="/api/v1", dependencies=[Depends(auth_dep)])

    # ── Sub-routers ────────────────────────────────────────────────────
    v1.include_router(system_router)
    v1.include_router(klines_router)
    v1.include_router(snapshot_router)
    v1.include_router(slippage_router)
    v1.include_router(metadata_router)

    app.include_router(v1)

    # ── WebSocket endpoint ─────────────────────────────────────────────
    hub = ConnectionHub(max_connections=config.websocket.max_connections)
    ws_endpoint = create_ws_endpoint(config, hub)
    app.add_api_websocket_route("/ws/v1/marketdata", ws_endpoint)
    app.state.ws_hub = hub

    return app
