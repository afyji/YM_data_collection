"""Tests for klines API routes."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from YM_data_collection.api.app import create_app
from YM_data_collection.config.models import DataCollectionConfig


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


def _make_mock_service() -> MagicMock:
    """Return a MagicMock that quacks like MarketDataQueryService."""
    svc = MagicMock()
    svc.query_klines_recent.return_value = {
        "data": [
            {"open_ts_ms": 1000, "close_ts_ms": 2000, "open": "100", "close": "105"},
            {"open_ts_ms": 2000, "close_ts_ms": 3000, "open": "105", "close": "110"},
            {"open_ts_ms": 3000, "close_ts_ms": 4000, "open": "110", "close": "115"},
        ],
        "meta": {"source": "mysql", "fallback_used": False, "cache_refreshed": False},
    }
    svc.query_klines_range.return_value = {
        "data": [
            {"open_ts_ms": i * 1000, "close_ts_ms": (i + 1) * 1000}
            for i in range(50)
        ],
        "meta": {"source": "mysql", "fallback_used": False, "cache_refreshed": False},
    }
    return svc


@pytest.fixture()
def client():
    config = _make_config()
    svc = _make_mock_service()
    app = create_app(config, query_service=svc)
    return TestClient(app), svc


# ── Recent endpoint ─────────────────────────────────────────────────────


class TestKlinesRecent:
    def test_recent_returns_ok(self, client):
        tc, svc = client
        resp = tc.get(
            "/api/v1/marketdata/klines/recent",
            params={
                "venue": "binance",
                "market_type": "spot",
                "symbol": "BTCUSDT",
                "interval": "1h",
                "count": 3,
            },
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert body["code"] == "OK"

    def test_recent_wraps_items(self, client):
        tc, svc = client
        resp = tc.get(
            "/api/v1/marketdata/klines/recent",
            params={
                "venue": "binance",
                "market_type": "spot",
                "symbol": "BTCUSDT",
                "interval": "1h",
                "count": 3,
            },
        )
        body = resp.json()
        assert "items" in body["data"]
        assert len(body["data"]["items"]) == 3

    def test_recent_meta(self, client):
        tc, svc = client
        resp = tc.get(
            "/api/v1/marketdata/klines/recent",
            params={
                "venue": "binance",
                "market_type": "spot",
                "symbol": "BTCUSDT",
                "interval": "1h",
                "count": 3,
            },
        )
        body = resp.json()
        assert body["meta"]["count"] == 3
        assert body["meta"]["source"] == "mysql"

    def test_recent_default_count(self, client):
        tc, svc = client
        tc.get(
            "/api/v1/marketdata/klines/recent",
            params={
                "venue": "binance",
                "market_type": "spot",
                "symbol": "BTCUSDT",
                "interval": "1h",
                # count omitted -> default 20
            },
        )
        svc.query_klines_recent.assert_called_with("spot", "BTCUSDT", "1h", 20)

    def test_recent_service_called_correctly(self, client):
        tc, svc = client
        tc.get(
            "/api/v1/marketdata/klines/recent",
            params={
                "venue": "binance",
                "market_type": "perp",
                "symbol": "ETHUSDT",
                "interval": "5m",
                "count": 10,
            },
        )
        svc.query_klines_recent.assert_called_once_with("perp", "ETHUSDT", "5m", 10)


# ── Range endpoint ──────────────────────────────────────────────────────


class TestKlinesRange:
    def test_range_returns_ok(self, client):
        tc, svc = client
        resp = tc.get(
            "/api/v1/marketdata/klines/range",
            params={
                "venue": "binance",
                "market_type": "spot",
                "symbol": "BTCUSDT",
                "interval": "1h",
                "start_ts_ms": 0,
                "end_ts_ms": 50000,
            },
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True

    def test_range_pagination_default(self, client):
        tc, svc = client
        resp = tc.get(
            "/api/v1/marketdata/klines/range",
            params={
                "venue": "binance",
                "market_type": "spot",
                "symbol": "BTCUSDT",
                "interval": "1h",
                "start_ts_ms": 0,
                "end_ts_ms": 50000,
            },
        )
        body = resp.json()
        # Default page=1, count=20 -> first 20 of 50 items
        assert len(body["data"]["items"]) == 20
        assert body["meta"]["page"] == 1
        assert body["meta"]["count"] == 20
        assert body["meta"]["total"] == 50
        assert body["meta"]["has_next"] is True

    def test_range_pagination_page2(self, client):
        tc, svc = client
        resp = tc.get(
            "/api/v1/marketdata/klines/range",
            params={
                "venue": "binance",
                "market_type": "spot",
                "symbol": "BTCUSDT",
                "interval": "1h",
                "start_ts_ms": 0,
                "end_ts_ms": 50000,
                "page": 2,
                "count": 20,
            },
        )
        body = resp.json()
        # Items 20-39 of 50
        assert len(body["data"]["items"]) == 20
        assert body["meta"]["page"] == 2
        assert body["meta"]["has_next"] is True

    def test_range_pagination_last_page(self, client):
        tc, svc = client
        resp = tc.get(
            "/api/v1/marketdata/klines/range",
            params={
                "venue": "binance",
                "market_type": "spot",
                "symbol": "BTCUSDT",
                "interval": "1h",
                "start_ts_ms": 0,
                "end_ts_ms": 50000,
                "page": 3,
                "count": 20,
            },
        )
        body = resp.json()
        # Items 40-49 of 50 -> 10 items, no next page
        assert len(body["data"]["items"]) == 10
        assert body["meta"]["has_next"] is False
        assert body["meta"]["total"] == 50

    def test_range_meta_source(self, client):
        tc, svc = client
        resp = tc.get(
            "/api/v1/marketdata/klines/range",
            params={
                "venue": "binance",
                "market_type": "spot",
                "symbol": "BTCUSDT",
                "interval": "1h",
                "start_ts_ms": 0,
                "end_ts_ms": 50000,
            },
        )
        body = resp.json()
        assert body["meta"]["source"] == "mysql"

    def test_range_service_called_correctly(self, client):
        tc, svc = client
        tc.get(
            "/api/v1/marketdata/klines/range",
            params={
                "venue": "binance",
                "market_type": "perp",
                "symbol": "ETHUSDT",
                "interval": "15m",
                "start_ts_ms": 100000,
                "end_ts_ms": 200000,
            },
        )
        svc.query_klines_range.assert_called_once_with(
            "perp", "ETHUSDT", "15m", 100000, 200000
        )

    def test_range_empty_result(self, client):
        tc, svc = client
        svc.query_klines_range.return_value = {
            "data": [],
            "meta": {"source": "none", "fallback_used": False, "cache_refreshed": False},
        }
        resp = tc.get(
            "/api/v1/marketdata/klines/range",
            params={
                "venue": "binance",
                "market_type": "spot",
                "symbol": "BTCUSDT",
                "interval": "1h",
                "start_ts_ms": 0,
                "end_ts_ms": 100,
            },
        )
        body = resp.json()
        assert body["data"]["items"] == []
        assert body["meta"]["total"] == 0
        assert body["meta"]["has_next"] is False
        assert body["meta"]["source"] == "none"
