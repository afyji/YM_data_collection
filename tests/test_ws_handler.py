"""Tests for WebSocket connection handler (DC-T034)."""

from __future__ import annotations

import json
import os
from unittest.mock import patch

import pytest
from starlette.testclient import TestClient

from YM_data_collection.api.app import create_app
from YM_data_collection.config.models import (
    AppConfig,
    AuthConfig,
    DataCollectionConfig,
    WebSocketConfig,
)


# ── Minimal config helpers ─────────────────────────────────────────────

def _make_config(*, auth_enabled: bool, ws_token: str = "test-ws-secret") -> DataCollectionConfig:
    """Build a minimal DataCollectionConfig for WS tests."""
    return DataCollectionConfig(
        app=AppConfig(app_name="test", env="dev", timezone="UTC", log_level="WARNING"),
        mysql={"host": "localhost", "port": 3306, "database": "test", "username": "u",
                "password_secret_ref": "TEST_MYSQL_PW", "pool_size": 1, "max_overflow": 0,
                "connect_timeout_seconds": 5, "read_timeout_seconds": 5, "write_timeout_seconds": 5},
        cache={"enabled": False, "backend": "redis", "host": "localhost", "port": 6379,
                "password_secret_ref": "TEST_CACHE_PW", "db": 0, "ttl_seconds": 60},
        auth=AuthConfig(
            enabled=auth_enabled,
            http_token_secret_ref="TEST_HTTP_TOKEN",
            ws_token_secret_ref="TEST_WS_TOKEN",
            internal_service_token_secret_ref="TEST_SVC_TOKEN",
        ),
        binance={"spot": {"rest_base_url": "https://api.binance.com", "ws_base_url": "wss://stream.binance.com:9443/ws"},
                 "perp": {"rest_base_url": "https://fapi.binance.com", "ws_base_url": "wss://fstream.binance.com/ws"}},
        ingestion={"historical_batch_size": 100, "realtime_enabled": False, "checkpoint_enabled": False, "raw_trace_enabled": False},
        validation={"kline_boundary_validation_enabled": False, "kline_auto_repair_enabled": False,
                     "trade_quote_quantity_tolerance_bps": 0, "mark_index_deviation_warning_bps": 0,
                     "depth_order_validation_enabled": False, "quality_record_enabled": False},
        depth={"default_depth_levels": 20, "freshness_threshold_ms": 1000, "max_snapshot_age_ms": 5000},
        slippage={"slippage_estimation_enabled": False, "default_slippage_depth_levels": 10,
                   "max_slippage_depth_levels": 50, "reference_price_mode": "mid_price",
                   "insufficient_depth_policy": "reject", "allow_partial_fill_estimation": False},
        service={"http_enabled": True, "http_host": "0.0.0.0", "http_port": 8000,
                  "ws_enabled": True, "ws_host": "0.0.0.0", "ws_port": 8001,
                  "default_page_count": 50, "max_page_count": 500,
                  "download_enabled": False, "http_read_timeout_seconds": 30,
                  "http_write_timeout_seconds": 30, "http_keepalive_enabled": True,
                  "request_id_enabled": False, "api_docs_enabled": False},
        websocket=WebSocketConfig(
            heartbeat_enabled=False,
            heartbeat_interval_seconds=30,
            pong_timeout_seconds=10,
            client_idle_timeout_seconds=300,
            max_subscriptions_per_connection=20,
            max_connections=100,
            send_queue_size=64,
            message_max_bytes=65536,
            snapshot_push_enabled=False,
            quality_event_push_enabled=False,
            stream_status_push_enabled=False,
        ),
        query_source={"snapshot_cache_first_enabled": False, "depth_cache_first_enabled": False,
                       "snapshot_mysql_fallback_enabled": False, "depth_mysql_fallback_enabled": False,
                       "cache_backfill_on_fallback_enabled": False, "allow_http_read_from_parquet": False},
        window={"default_recent_kline_count": 100, "max_recent_kline_count": 500,
                 "http_window_refill_enabled": False, "http_window_refill_limit": 1000},
        export={"enabled": False, "base_dir": "/tmp", "default_format": "parquet",
                "compression": "snappy", "partition_rule": "date", "manifest_write_enabled": False,
                "overwrite_same_version_enabled": False},
        download={"download_enabled": False, "download_token_required": False,
                   "download_url_expire_seconds": 60, "max_download_file_size_mb": 100,
                   "download_audit_enabled": False},
        quality={"enabled": False, "warning_thresholds": {}, "error_thresholds": {},
                 "email_alert_enabled": False, "email_recipients": []},
    )


def _make_client(config: DataCollectionConfig) -> TestClient:
    """Create a TestClient; create_app already wires the WS endpoint."""
    app = create_app(config)
    return TestClient(app)


# ── Tests ──────────────────────────────────────────────────────────────

class TestWSHandlerConnection:
    """DC-T034: WebSocket connection/auth tests."""

    @patch.dict(os.environ, {"TEST_WS_TOKEN": "test-ws-secret"})
    def test_successful_connection_with_valid_token(self):
        config = _make_config(auth_enabled=True)
        client = _make_client(config)
        with client.websocket_connect("/ws/v1/marketdata?token=test-ws-secret") as ws:
            # Connection established — send ping to verify round-trip
            ws.send_json({"action": "ping", "request_id": 1, "ts_ms": 123})
            data = ws.receive_json()
            assert data["type"] == "pong"
            assert data["ts_ms"] == 123

    @patch.dict(os.environ, {"TEST_WS_TOKEN": "test-ws-secret"})
    def test_connection_rejected_with_invalid_token(self):
        config = _make_config(auth_enabled=True)
        client = _make_client(config)
        with pytest.raises(Exception):
            # Invalid token -> close code 4001 -> raises in testclient
            with client.websocket_connect("/ws/v1/marketdata?token=wrong-token") as ws:
                pass

    @patch.dict(os.environ, {"TEST_WS_TOKEN": "test-ws-secret"})
    def test_connection_rejected_with_no_token_when_auth_enabled(self):
        config = _make_config(auth_enabled=True)
        client = _make_client(config)
        with pytest.raises(Exception):
            with client.websocket_connect("/ws/v1/marketdata") as ws:
                pass

    @patch.dict(os.environ, {"TEST_WS_TOKEN": "test-ws-secret"})
    def test_connection_succeeds_when_auth_disabled(self):
        config = _make_config(auth_enabled=False)
        client = _make_client(config)
        with client.websocket_connect("/ws/v1/marketdata") as ws:
            ws.send_json({"action": "ping", "request_id": 1, "ts_ms": 999})
            data = ws.receive_json()
            assert data["type"] == "pong"

    @patch.dict(os.environ, {"TEST_WS_TOKEN": "test-ws-secret"})
    def test_clean_disconnect(self):
        config = _make_config(auth_enabled=False)
        client = _make_client(config)
        hub = client.app.state.ws_hub
        with client.websocket_connect("/ws/v1/marketdata") as ws:
            # Send a ping to ensure connection is fully established
            ws.send_json({"action": "ping", "request_id": 1, "ts_ms": 1})
            ws.receive_json()
            assert hub.get_connection_count() == 1
        # After disconnect, hub should have 0 connections
        assert hub.get_connection_count() == 0

    @patch.dict(os.environ, {"TEST_WS_TOKEN": "test-ws-secret"})
    def test_max_connections_exceeded(self):
        config = _make_config(auth_enabled=False)
        config.websocket.max_connections = 1
        client = _make_client(config)
        # First connection takes the slot
        with client.websocket_connect("/ws/v1/marketdata") as ws1:
            # Second should be rejected with 4002
            with pytest.raises(Exception):
                with client.websocket_connect("/ws/v1/marketdata") as ws2:
                    pass
