"""Tests for the YM data-collection FastAPI application."""

from __future__ import annotations

import os
from unittest.mock import patch

import pytest
from starlette.testclient import TestClient

from YM_data_collection.api.app import create_app
from YM_data_collection.config.models import (
    AppConfig,
    AuthConfig,
    DataCollectionConfig,
    ServiceConfig,
)


# ── Fixtures ────────────────────────────────────────────────────────────────

_MOCK_HTTP_TOKEN = "test-secret-token-abc123"


def _make_config(
    *,
    auth_enabled: bool = True,
    request_id_enabled: bool = True,
    api_docs_enabled: bool = False,
) -> DataCollectionConfig:
    """Build a minimal DataCollectionConfig suitable for testing."""
    return DataCollectionConfig(
        app=AppConfig(app_name="ym_data_test", env="dev", timezone="UTC", log_level="WARNING"),
        mysql={"host": "localhost", "port": 3306, "database": "test", "username": "root",
               "password_secret_ref": "TEST_DB_PASS", "pool_size": 1, "max_overflow": 0,
               "connect_timeout_seconds": 5, "read_timeout_seconds": 5, "write_timeout_seconds": 5},
        cache={"enabled": False, "backend": "redis", "host": "localhost", "port": 6379,
               "password_secret_ref": "TEST_CACHE_PASS", "db": 0, "ttl_seconds": 60},
        auth=AuthConfig(
            enabled=auth_enabled,
            http_token_secret_ref="YM_HTTP_API_TOKEN",
            ws_token_secret_ref="YM_WS_TOKEN",
            internal_service_token_secret_ref="YM_INTERNAL_TOKEN",
        ),
        binance={
            "spot": {"rest_base_url": "https://api.binance.com",
                      "ws_base_url": "wss://stream.binance.com:9443/ws"},
            "perp": {"rest_base_url": "https://fapi.binance.com",
                      "ws_base_url": "wss://fstream.binance.com/ws"},
        },
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
        depth={"default_depth_levels": 20, "freshness_threshold_ms": 5000, "max_snapshot_age_ms": 60000},
        slippage={
            "slippage_estimation_enabled": False,
            "default_slippage_depth_levels": 10,
            "max_slippage_depth_levels": 50,
            "reference_price_mode": "mid_price",
            "insufficient_depth_policy": "reject",
            "allow_partial_fill_estimation": False,
        },
        service=ServiceConfig(
            http_enabled=True,
            http_host="127.0.0.1",
            http_port=8000,
            ws_enabled=False,
            ws_host="127.0.0.1",
            ws_port=8001,
            default_page_count=20,
            max_page_count=100,
            download_enabled=False,
            http_read_timeout_seconds=30,
            http_write_timeout_seconds=30,
            http_keepalive_enabled=True,
            request_id_enabled=request_id_enabled,
            api_docs_enabled=api_docs_enabled,
        ),
        websocket={
            "heartbeat_enabled": False,
            "heartbeat_interval_seconds": 30,
            "pong_timeout_seconds": 10,
            "client_idle_timeout_seconds": 300,
            "max_subscriptions_per_connection": 10,
            "max_connections": 100,
            "send_queue_size": 256,
            "message_max_bytes": 65536,
            "snapshot_push_enabled": False,
            "quality_event_push_enabled": False,
            "stream_status_push_enabled": False,
        },
        query_source={
            "snapshot_cache_first_enabled": False,
            "depth_cache_first_enabled": False,
            "snapshot_mysql_fallback_enabled": True,
            "depth_mysql_fallback_enabled": True,
            "cache_backfill_on_fallback_enabled": False,
            "allow_http_read_from_parquet": False,
        },
        window={
            "default_recent_kline_count": 100,
            "max_recent_kline_count": 1000,
            "http_window_refill_enabled": False,
            "http_window_refill_limit": 50,
        },
        export={
            "enabled": False,
            "base_dir": "/tmp/ym_export",
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


@pytest.fixture()
def client_auth_enabled():
    """TestClient with auth enabled and the secret available in env."""
    config = _make_config(auth_enabled=True, request_id_enabled=True)
    app = create_app(config)
    with TestClient(app, raise_server_exceptions=False) as c:
        with patch.dict(os.environ, {"YM_HTTP_API_TOKEN": _MOCK_HTTP_TOKEN}):
            yield c


@pytest.fixture()
def client_auth_disabled():
    """TestClient with auth disabled."""
    config = _make_config(auth_enabled=False, request_id_enabled=True)
    app = create_app(config)
    with TestClient(app, raise_server_exceptions=False) as c:
        yield c


@pytest.fixture()
def client_no_request_id():
    """TestClient with request-id middleware disabled."""
    config = _make_config(auth_enabled=False, request_id_enabled=False)
    app = create_app(config)
    with TestClient(app, raise_server_exceptions=False) as c:
        yield c


# ── Tests ───────────────────────────────────────────────────────────────────


class TestHealthEndpoint:
    """GET /api/v1/system/health"""

    def test_returns_200_ok_with_unified_response(self, client_auth_disabled):
        resp = client_auth_disabled.get("/api/v1/system/health")
        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert body["code"] == "OK"
        assert body["data"]["overall_healthy"] is True


class TestTokenAuth:
    """X-API-Token header validation (tested against kline endpoint)."""

    def test_valid_token_passes(self, client_auth_enabled):
        resp = client_auth_enabled.get(
            "/api/v1/system/health",
            headers={"X-API-Token": _MOCK_HTTP_TOKEN},
        )
        assert resp.status_code == 200

    def test_missing_token_returns_401(self, client_auth_enabled):
        resp = client_auth_enabled.get("/api/v1/marketdata/klines/recent")
        assert resp.status_code == 401

    def test_invalid_token_returns_401(self, client_auth_enabled):
        resp = client_auth_enabled.get(
            "/api/v1/marketdata/klines/recent",
            headers={"X-API-Token": "wrong-token"},
        )
        assert resp.status_code == 401

    def test_auth_disabled_allows_all(self, client_auth_disabled):
        resp = client_auth_disabled.get("/api/v1/system/health")
        assert resp.status_code == 200


class TestRequestIdMiddleware:
    """X-Request-ID response header."""

    def test_request_id_header_present(self, client_auth_disabled):
        resp = client_auth_disabled.get("/api/v1/system/health")
        assert "X-Request-ID" in resp.headers
        # Should be a valid UUID4 format
        request_id = resp.headers["X-Request-ID"]
        assert len(request_id) == 36  # 8-4-4-4-12 with hyphens

    def test_request_id_unique_per_request(self, client_auth_disabled):
        resp1 = client_auth_disabled.get("/api/v1/system/health")
        resp2 = client_auth_disabled.get("/api/v1/system/health")
        assert resp1.headers["X-Request-ID"] != resp2.headers["X-Request-ID"]

    def test_request_id_absent_when_disabled(self, client_no_request_id):
        resp = client_no_request_id.get("/api/v1/system/health")
        assert "X-Request-ID" not in resp.headers
