"""System health and runtime-status API routes."""

from __future__ import annotations

import time
from dataclasses import asdict
from datetime import datetime, timezone

from fastapi import APIRouter, Request

from YM_data_collection.api.response import ok_response

router = APIRouter(prefix="/system", tags=["system"])


@router.get("/health")
async def health_check(request: Request):
    """Returns overall system health.

    Uses ServiceHealthChecker from app.state (if available).
    If no health_checker on app.state, returns basic {status: ok}.
    """
    health_checker = getattr(request.app.state, "health_checker", None)

    if health_checker is None:
        return ok_response(
            data={
                "overall_healthy": True,
                "components": [],
                "checked_at_utc": datetime.now(timezone.utc).isoformat(),
            }
        )

    system_health = health_checker.run_all()

    components = []
    for status in system_health.statuses:
        component_dict: dict = {
            "component": status.component,
            "healthy": status.healthy,
        }
        if status.latency_ms is not None:
            component_dict["latency_ms"] = status.latency_ms
        component_dict["detail"] = status.detail
        if status.error is not None:
            component_dict["error"] = status.error
        components.append(component_dict)

    return ok_response(
        data={
            "overall_healthy": system_health.overall_healthy,
            "components": components,
            "checked_at_utc": system_health.checked_at_utc,
        }
    )


@router.get("/runtime-status")
async def runtime_status(request: Request):
    """Returns runtime status: active connections, uptime, config info."""
    # ws_connections
    ws_hub = getattr(request.app.state, "ws_hub", None)
    ws_connections = ws_hub.get_connection_count() if ws_hub is not None else 0

    # uptime_seconds
    start_time = getattr(request.app.state, "start_time_utc", None)
    if start_time is not None:
        uptime_seconds = (datetime.now(timezone.utc) - start_time).total_seconds()
    else:
        uptime_seconds = 0.0

    # version
    version = getattr(request.app.state, "version", "1.0.0")

    return ok_response(
        data={
            "ws_connections": ws_connections,
            "uptime_seconds": uptime_seconds,
            "version": version,
        }
    )
