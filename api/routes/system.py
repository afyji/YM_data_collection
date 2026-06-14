"""System health and runtime-status API routes."""

from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Request

from YM_data_collection.api.response import ok_response

router = APIRouter(prefix="/system", tags=["system"])


def _health_status_from(status) -> str:
    """Map a HealthStatus to a formal status string."""
    if status.healthy:
        return "ok"
    return "error"


@router.get("/health")
async def health_check(request: Request):
    """Returns overall system health with formal component shapes.

    Uses ServiceHealthChecker from app.state (if available).
    If no health_checker on app.state, returns formal shape with unknown statuses.
    """
    health_checker = getattr(request.app.state, "health_checker", None)
    ws_hub = getattr(request.app.state, "ws_hub", None)
    now_utc = datetime.now(timezone.utc).isoformat()

    # Base formal shape for when no health_checker is present
    mysql_data = {"status": "unknown"}
    cache_data = {"status": "unknown"}
    http_data = {"status": "ok"}  # route itself is reachable
    ws_status = "disabled"
    ws_active = 0
    if ws_hub is not None:
        ws_status = "ok"
        try:
            ws_active = ws_hub.get_connection_count()
        except Exception:
            ws_active = 0
    ws_data = {"status": ws_status, "active_connections": ws_active}
    streams_data = {"status": "unknown"}
    overall_status = "ok"
    overall_healthy = True
    components = []

    if health_checker is not None:
        system_health = health_checker.run_all()

        overall_healthy = system_health.overall_healthy
        # Determine overall_status
        any_unhealthy = any(not s.healthy for s in system_health.statuses)
        if overall_healthy:
            overall_status = "ok"
        else:
            overall_status = "degraded"

        # Build backward-compat components list
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

        # Map checker component names to formal keys
        for status in system_health.statuses:
            formal = _health_status_from(status)
            mapped: dict = {"status": formal}
            if status.latency_ms is not None:
                mapped["latency_ms"] = status.latency_ms
            if status.detail:
                mapped["detail"] = status.detail
            if status.error is not None:
                mapped["error"] = status.error

            if status.component == "mysql":
                mysql_data = mapped
            elif status.component == "redis":
                cache_data = mapped
            elif status.component == "http_api":
                http_data = mapped
            elif status.component == "data_freshness":
                streams_data = mapped

        # Override overall_status based on severity
        all_unhealthy = all(not s.healthy for s in system_health.statuses)
        if not overall_healthy and all_unhealthy:
            overall_status = "error"
        elif not overall_healthy:
            overall_status = "degraded"

        now_utc = system_health.checked_at_utc

    return ok_response(
        data={
            "overall_status": overall_status,
            "overall_healthy": overall_healthy,
            "mysql": mysql_data,
            "cache": cache_data,
            "http": http_data,
            "ws": ws_data,
            "streams": streams_data,
            "components": components,
            "checked_at_utc": now_utc,
        },
        meta={"source": "health_checker" if health_checker else "builtin"},
    )


@router.get("/runtime-status")
async def runtime_status(request: Request):
    """Returns runtime status: processes, checkpoint summary, connections, uptime."""
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

    now_utc = datetime.now(timezone.utc).isoformat()

    # ── Processes ──────────────────────────────────────────────────────────
    processes = [
        {"name": "run_data_api", "status": "running"},
    ]

    # WebSocket process status
    config = getattr(request.app.state, "config", None)
    if ws_hub is not None:
        ws_enabled = True
        if config is not None:
            ws_enabled = getattr(config.service, "ws_enabled", True)
        processes.append({
            "name": "websocket",
            "status": "running" if ws_enabled else "disabled",
        })
    else:
        processes.append({"name": "websocket", "status": "disabled"})

    # ── Checkpoint summary ─────────────────────────────────────────────────
    checkpoint_repo = getattr(request.app.state, "checkpoint_repo", None)
    if checkpoint_repo is None:
        checkpoint_summary = {
            "tracked_streams": 0,
            "ok_count": 0,
            "error_count": 0,
            "latest_updated_at_utc": None,
            "status": "unknown",
        }
    else:
        try:
            ok_list = checkpoint_repo.list_by_status("ok")
            error_list = checkpoint_repo.list_by_status("error")
            ok_count = len(ok_list) if ok_list else 0
            error_count = len(error_list) if error_list else 0
            tracked = ok_count + error_count

            # Find latest updated_at across all checkpoints
            latest = None
            all_cps = (ok_list or []) + (error_list or [])
            for cp in all_cps:
                ts = getattr(cp, "last_success_at_utc", None)
                if ts is not None:
                    if latest is None or ts > latest:
                        latest = ts

            if latest is not None:
                latest_str = latest.isoformat() if hasattr(latest, "isoformat") else str(latest)
            else:
                latest_str = None

            if error_count > 0 and ok_count > 0:
                cp_status = "degraded"
            elif error_count > 0:
                cp_status = "error"
            else:
                cp_status = "ok"

            checkpoint_summary = {
                "tracked_streams": tracked,
                "ok_count": ok_count,
                "error_count": error_count,
                "latest_updated_at_utc": latest_str,
                "status": cp_status,
            }
        except Exception:
            checkpoint_summary = {
                "tracked_streams": 0,
                "ok_count": 0,
                "error_count": 0,
                "latest_updated_at_utc": None,
                "status": "error",
            }

    return ok_response(
        data={
            "ws_connections": ws_connections,
            "uptime_seconds": uptime_seconds,
            "version": version,
            "processes": processes,
            "checkpoint_summary": checkpoint_summary,
            "checked_at_utc": now_utc,
        },
        meta={"source": "runtime_status"},
    )
