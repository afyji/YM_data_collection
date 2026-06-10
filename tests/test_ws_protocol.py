"""Tests for WebSocket protocol layer (DC-T035)."""

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
from YM_data_collection.ws.protocol import (
    ErrorCode,
    IncomingMessage,
    InvalidActionError,
    InvalidJsonError,
    InvalidTopicError,
    MaxSubscriptionsExceededError,
    build_error,
    build_pong,
    build_subscribed,
    build_unsubscribed,
    parse_message,
)


# ── Unit tests for parse_message ───────────────────────────────────────

class TestParseMessage:
    def test_valid_subscribe(self):
        msg = parse_message(json.dumps({"action": "subscribe", "request_id": "r1", "topics": ["a"]}))
        assert msg.action == "subscribe"
        assert msg.request_id == "r1"
        assert msg.topics == ["a"]

    def test_valid_ping(self):
        msg = parse_message(json.dumps({"action": "ping", "request_id": 42, "ts_ms": 12345}))
        assert msg.action == "ping"
        assert msg.ts_ms == 12345

    def test_invalid_json_raises(self):
        with pytest.raises(InvalidJsonError):
            parse_message("not json{")

    def test_invalid_action_raises(self):
        with pytest.raises(InvalidActionError):
            parse_message(json.dumps({"action": "explode", "request_id": 1}))

    def test_missing_request_id_raises(self):
        with pytest.raises(InvalidActionError):
            parse_message(json.dumps({"action": "ping"}))

    def test_non_dict_raises(self):
        with pytest.raises(InvalidJsonError):
            parse_message(json.dumps([1, 2, 3]))


# ── Unit tests for response builders ───────────────────────────────────

class TestResponseBuilders:
    def test_subscribed(self):
        r = build_subscribed("r1", ["t1"]).to_dict()
        assert r["type"] == "subscribed"
        assert r["request_id"] == "r1"
        assert r["topics"] == ["t1"]
        assert "ts_ms" in r

    def test_unsubscribed(self):
        r = build_unsubscribed("r2", ["t2"]).to_dict()
        assert r["type"] == "unsubscribed"
        assert r["request_id"] == "r2"
        assert r["topics"] == ["t2"]
        assert "ts_ms" in r

    def test_pong(self):
        r = build_pong(1, 999).to_dict()
        assert r == {"type": "pong", "request_id": 1, "ts_ms": 999}

    def test_error(self):
        r = build_error(0, ErrorCode.INVALID_JSON, "bad").to_dict()
        assert r["type"] == "error"
        assert r["code"] == "INVALID_JSON"
        assert r["message"] == "bad"
        assert "ts_ms" in r


# ── Integration tests via TestClient ───────────────────────────────────

def _make_config() -> DataCollectionConfig:
    return DataCollectionConfig(
        app=AppConfig(app_name="test", env="dev", timezone="UTC", log_level="WARNING"),
        mysql={"host": "localhost", "port": 3306, "database": "test", "username": "u",
                "password_secret_ref": "TEST_MYSQL_PW", "pool_size": 1, "max_overflow": 0,
                "connect_timeout_seconds": 5, "read_timeout_seconds": 5, "write_timeout_seconds": 5},
        cache={"enabled": False, "backend": "redis", "host": "localhost", "port": 6379,
                "password_secret_ref": "TEST_CACHE_PW", "db": 0, "ttl_seconds": 60},
        auth=AuthConfig(
            enabled=False,
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


def _make_client(config: DataCollectionConfig | None = None) -> TestClient:
    cfg = config or _make_config()
    app = create_app(cfg)
    return TestClient(app)


# A valid topic key for testing
TK = "marketdata.mark_price:binance:perp:BTCUSDT"
TK2 = "marketdata.funding_rate:binance:perp:ETHUSDT"
TK_KLINE = "marketdata.kline:binance:spot:BTCUSDT:1h"
INVALID_TK = "invalid.topic:binance:perp:BTCUSDT"


class TestWSProtocol:
    """DC-T035: Protocol-level WS tests via TestClient."""

    def test_subscribe_valid_topics(self):
        client = _make_client()
        with client.websocket_connect("/ws/v1/marketdata") as ws:
            ws.send_json({"action": "subscribe", "request_id": "r1", "topics": [TK]})
            data = ws.receive_json()
            assert data["type"] == "subscribed"
            assert data["request_id"] == "r1"
            assert TK in data["topics"]

    def test_subscribe_invalid_topic_returns_error(self):
        client = _make_client()
        with client.websocket_connect("/ws/v1/marketdata") as ws:
            ws.send_json({"action": "subscribe", "request_id": "r2", "topics": [INVALID_TK]})
            data = ws.receive_json()
            assert data["type"] == "error"
            assert data["code"] == "INVALID_TOPIC"

    def test_unsubscribe_returns_unsubscribed(self):
        client = _make_client()
        with client.websocket_connect("/ws/v1/marketdata") as ws:
            ws.send_json({"action": "subscribe", "request_id": "r1", "topics": [TK]})
            ws.receive_json()  # subscribed
            ws.send_json({"action": "unsubscribe", "request_id": "r2", "topics": [TK]})
            data = ws.receive_json()
            assert data["type"] == "unsubscribed"
            assert TK in data["topics"]

    def test_ping_returns_pong(self):
        client = _make_client()
        with client.websocket_connect("/ws/v1/marketdata") as ws:
            ws.send_json({"action": "ping", "request_id": 99, "ts_ms": 12345})
            data = ws.receive_json()
            assert data["type"] == "pong"
            assert data["request_id"] == 99
            assert data["ts_ms"] == 12345

    def test_invalid_action_returns_error(self):
        client = _make_client()
        with client.websocket_connect("/ws/v1/marketdata") as ws:
            ws.send_json({"action": "explode", "request_id": "r3"})
            data = ws.receive_json()
            assert data["type"] == "error"
            assert data["code"] == "INVALID_ACTION"

    def test_invalid_json_returns_error(self):
        client = _make_client()
        with client.websocket_connect("/ws/v1/marketdata") as ws:
            ws.send_text("not valid json{")
            data = ws.receive_json()
            assert data["type"] == "error"
            assert data["code"] == "INVALID_JSON"

    def test_max_subscriptions_exceeded(self):
        config = _make_config()
        config.websocket.max_subscriptions_per_connection = 1
        client = _make_client(config)
        with client.websocket_connect("/ws/v1/marketdata") as ws:
            # First subscribe succeeds (1 topic)
            ws.send_json({"action": "subscribe", "request_id": "r1", "topics": [TK]})
            ws.receive_json()
            # Second subscribe exceeds limit
            ws.send_json({"action": "subscribe", "request_id": "r2", "topics": [TK2]})
            data = ws.receive_json()
            assert data["type"] == "error"
            assert data["code"] == "MAX_SUBSCRIPTIONS_EXCEEDED"

    def test_subscribe_then_unsubscribe_tracks_state(self):
        """Subscribe, unsubscribe, then re-subscribe to verify state tracking."""
        client = _make_client()
        with client.websocket_connect("/ws/v1/marketdata") as ws:
            ws.send_json({"action": "subscribe", "request_id": "r1", "topics": [TK, TK2]})
            data1 = ws.receive_json()
            assert data1["type"] == "subscribed"
            assert len(data1["topics"]) == 2

            ws.send_json({"action": "unsubscribe", "request_id": "r2", "topics": [TK]})
            data2 = ws.receive_json()
            assert data2["type"] == "unsubscribed"
            assert TK in data2["topics"]

            # Re-subscribe the removed one — should come back as added
            ws.send_json({"action": "subscribe", "request_id": "r3", "topics": [TK]})
            data3 = ws.receive_json()
            assert data3["type"] == "subscribed"
            assert TK in data3["topics"]

    def test_duplicate_subscribe_is_idempotent(self):
        """Subscribing to the same topic twice returns empty added-list the second time."""
        client = _make_client()
        with client.websocket_connect("/ws/v1/marketdata") as ws:
            ws.send_json({"action": "subscribe", "request_id": "r1", "topics": [TK]})
            data1 = ws.receive_json()
            assert TK in data1["topics"]

            ws.send_json({"action": "subscribe", "request_id": "r2", "topics": [TK]})
            data2 = ws.receive_json()
            assert data2["type"] == "subscribed"
            # Duplicate: no new topics actually added
            assert data2["topics"] == []
