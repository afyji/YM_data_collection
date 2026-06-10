"""Tests for system health and runtime-status API routes."""

from __future__ import annotations

import time
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from YM_data_collection.api.app import create_app
from YM_data_collection.config.models import DataCollectionConfig
from YM_data_collection.quality.health_checker import HealthStatus, SystemHealth


def _make_config() -> DataCollectionConfig:
    """Build a minimal DataCollectionConfig for testing."""
    return DataCollectionConfig(
        app={"app_name": "test", "env": "dev", "timezone": "UTC", "log_level": "INFO"},
        mysql={
            "host": "localhost",
            "port": 3306,
            "database": "test",
            "username": "root",
            "password_secret_ref": "secret",
            "pool_size": 5,
            "max_overflow": 2,
            "connect_timeout_seconds": 5,
            "read_timeout_seconds": 5,
            "write_timeout_seconds": 5,
        },
        cache={
            "enabled": False,
            "backend": "redis",
            "host": "localhost",
            "port": 6379,
            "password_secret_ref": "secret",
            "db": 0,
            "ttl_seconds": 60,
        },
        auth={
            "enabled": False,
            "http_token_secret_ref": "secret",
            "ws_token_secret_ref": "secret",
            "internal_service_token_secret_ref": "secret",
        },
        binance={},
        ingestion={
            "historical_batch_size": 100,
            "realtime_enabled": False,
            "checkpoint_enabled": False,
            "raw_trace_enabled": False,
        },
        validation={
            "kline_boundary_validation_enabled": False,
            "kline_auto_repair_enabled": False,
            "trade_quote_quantity_tolerance_bps": 0,
            "mark_index_deviation_warning_bps": 0,
            "depth_order_validation_enabled": False,
            "quality_record_enabled": False,
        },
        depth={
            "default_depth_levels": 20,
            "freshness_threshold_ms": 1000,
            "max_snapshot_age_ms": 60000,
        },
        slippage={
            "slippage_estimation_enabled": False,
            "default_slippage_depth_levels": 10,
            "max_slippage_depth_levels": 50,
            "reference_price_mode": "mid_price",
            "insufficient_depth_policy": "reject",
            "allow_partial_fill_estimation": False,
        },
        service={
            "http_enabled": True,
            "http_host": "0.0.0.0",
            "http_port": 8000,
            "ws_enabled": False,
            "ws_host": "0.0.0.0",
            "ws_port": 8001,
            "default_page_count": 20,
            "max_page_count": 100,
            "download_enabled": False,
            "http_read_timeout_seconds": 30,
            "http_write_timeout_seconds": 30,
            "http_keepalive_enabled": True,
            "request_id_enabled": False,
            "api_docs_enabled": False,
        },
        websocket={
            "heartbeat_enabled": False,
            "heartbeat_interval_seconds": 30,
            "pong_timeout_seconds": 60,
            "client_idle_timeout_seconds": 300,
            "max_subscriptions_per_connection": 10,
            "max_connections": 5,
            "send_queue_size": 100,
            "message_max_bytes": 1048576,
            "snapshot_push_enabled": False,
            "quality_event_push_enabled": False,
            "stream_status_push_enabled": False,
        },
        query_source={
            "snapshot_cache_first_enabled": False,
            "depth_cache_first_enabled": False,
            "snapshot_mysql_fallback_enabled": False,
            "depth_mysql_fallback_enabled": False,
            "cache_backfill_on_fallback_enabled": False,
            "allow_http_read_from_parquet": False,
        },
        window={
            "default_recent_kline_count": 20,
            "max_recent_kline_count": 1000,
            "http_window_refill_enabled": False,
            "http_window_refill_limit": 100,
        },
        export={
            "enabled": False,
            "base_dir": "/tmp",
            "default_format": "parquet",
            "compression": "snappy",
            "partition_rule": "date",
            "manifest_write_enabled": False,
            "overwrite_same_version_enabled": False,
        },
        download={
            "download_enabled": False,
            "download_token_required": False,
            "download_url_expire_seconds": 300,
            "max_download_file_size_mb": 100,
            "download_audit_enabled": False,
        },
        quality={
            "enabled": False,
            "warning_thresholds": {},
            "error_thresholds": {},
            "email_alert_enabled": False,
            "email_recipients": [],
        },
    )


def _make_healthy_checker() -> MagicMock:
    """Mock health_checker with all components healthy."""
    checker = MagicMock()
    checker.run_all.return_value = SystemHealth(
        overall_healthy=True,
        statuses=[
            HealthStatus(component="mysql", healthy=True, latency_ms=2.3, detail="SELECT 1 ok"),
            HealthStatus(component="redis", healthy=True, latency_ms=1.1, detail="ping()=True"),
            HealthStatus(
                component="data_freshness",
                healthy=True,
                latency_ms=5.0,
                detail="latest kline age=120s, max=600s",
            ),
        ],
        checked_at_utc=datetime.now(timezone.utc).isoformat(),
    )
    return checker


def _make_unhealthy_checker() -> MagicMock:
    """Mock health_checker with one unhealthy component."""
    checker = MagicMock()
    checker.run_all.return_value = SystemHealth(
        overall_healthy=False,
        statuses=[
            HealthStatus(component="mysql", healthy=True, latency_ms=2.3, detail="SELECT 1 ok"),
            HealthStatus(
                component="redis",
                healthy=False,
                latency_ms=100.0,
                detail="ping() raised",
                error="Connection refused",
            ),
            HealthStatus(
                component="data_freshness",
                healthy=True,
                latency_ms=5.0,
                detail="latest kline age=120s, max=600s",
            ),
        ],
        checked_at_utc=datetime.now(timezone.utc).isoformat(),
    )
    return checker


# ── Health endpoint tests ──────────────────────────────────────────────────


class TestHealthCheck:
    """Tests for GET /api/v1/system/health."""

    def test_health_returns_200(self):
        """Health endpoint returns 200 status code."""
        config = _make_config()
        app = create_app(config)
        client = TestClient(app)
        resp = client.get("/api/v1/system/health")
        assert resp.status_code == 200

    def test_health_with_all_healthy(self):
        """Health endpoint returns all healthy components."""
        config = _make_config()
        checker = _make_healthy_checker()
        app = create_app(config, health_checker=checker)
        client = TestClient(app)
        resp = client.get("/api/v1/system/health")
        body = resp.json()

        assert body["success"] is True
        assert body["data"]["overall_healthy"] is True
        assert len(body["data"]["components"]) == 3

        mysql = body["data"]["components"][0]
        assert mysql["component"] == "mysql"
        assert mysql["healthy"] is True
        assert mysql["latency_ms"] == 2.3
        assert mysql["detail"] == "SELECT 1 ok"

        redis = body["data"]["components"][1]
        assert redis["component"] == "redis"
        assert redis["healthy"] is True

        assert "checked_at_utc" in body["data"]

    def test_health_with_unhealthy_component(self):
        """Health endpoint reflects unhealthy component."""
        config = _make_config()
        checker = _make_unhealthy_checker()
        app = create_app(config, health_checker=checker)
        client = TestClient(app)
        resp = client.get("/api/v1/system/health")
        body = resp.json()

        assert body["data"]["overall_healthy"] is False
        redis = body["data"]["components"][1]
        assert redis["component"] == "redis"
        assert redis["healthy"] is False
        assert redis["error"] == "Connection refused"

    def test_health_without_health_checker(self):
        """Without health_checker, returns basic ok response."""
        config = _make_config()
        app = create_app(config)
        client = TestClient(app)
        resp = client.get("/api/v1/system/health")
        body = resp.json()

        assert body["success"] is True
        assert body["data"]["overall_healthy"] is True
        assert body["data"]["components"] == []
        assert "checked_at_utc" in body["data"]


# ── Runtime-status endpoint tests ──────────────────────────────────────────


class TestRuntimeStatus:
    """Tests for GET /api/v1/system/runtime-status."""

    def test_runtime_status_returns_200(self):
        """Runtime-status endpoint returns 200 status code."""
        config = _make_config()
        app = create_app(config)
        client = TestClient(app)
        resp = client.get("/api/v1/system/runtime-status")
        assert resp.status_code == 200

    def test_runtime_status_basic_fields(self):
        """Runtime-status returns ws_connections, uptime_seconds, version."""
        config = _make_config()
        app = create_app(config)
        client = TestClient(app)
        resp = client.get("/api/v1/system/runtime-status")
        body = resp.json()

        assert body["success"] is True
        assert "ws_connections" in body["data"]
        assert "uptime_seconds" in body["data"]
        assert "version" in body["data"]
        assert isinstance(body["data"]["uptime_seconds"], float)
        assert body["data"]["uptime_seconds"] >= 0

    def test_runtime_status_with_ws_hub(self):
        """Runtime-status shows connection count from ws_hub."""
        config = _make_config()
        app = create_app(config)
        client = TestClient(app)

        # The ws_hub is created in create_app; mock its connection count
        app.state.ws_hub.get_connection_count = MagicMock(return_value=5)

        resp = client.get("/api/v1/system/runtime-status")
        body = resp.json()

        assert body["data"]["ws_connections"] == 5

    def test_runtime_status_without_ws_hub(self):
        """Runtime-status shows 0 connections when ws_hub is removed."""
        config = _make_config()
        app = create_app(config)
        client = TestClient(app)

        # Remove ws_hub to simulate its absence
        del app.state.ws_hub

        resp = client.get("/api/v1/system/runtime-status")
        body = resp.json()

        assert body["data"]["ws_connections"] == 0

    def test_runtime_status_version_default(self):
        """Runtime-status returns default version 1.0.0."""
        config = _make_config()
        app = create_app(config)
        client = TestClient(app)
        resp = client.get("/api/v1/system/runtime-status")
        body = resp.json()

        assert body["data"]["version"] == "1.0.0"

    def test_runtime_status_custom_version(self):
        """Runtime-status returns custom version from app.state."""
        config = _make_config()
        app = create_app(config)
        app.state.version = "2.3.4"
        client = TestClient(app)
        resp = client.get("/api/v1/system/runtime-status")
        body = resp.json()

        assert body["data"]["version"] == "2.3.4"
