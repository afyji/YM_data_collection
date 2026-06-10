"""Tests for YM_data_collection.ws.publishers.system_publisher (DC-T038)."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest

from YM_data_collection.ws.hub import ConnectionHub
from YM_data_collection.ws.publishers.system_publisher import (
    TOPIC_QUALITY_EVENT,
    TOPIC_STREAM_STATUS,
    SystemEventPublisher,
    build_quality_event_message,
    build_stream_status_message,
)

# ---------------------------------------------------------------------------
# Fixtures & constants
# ---------------------------------------------------------------------------

VENUE = "binance"
MARKET_TYPE = "perp"
SYMBOL = "BTCUSDT"

SAMPLE_QUALITY_EVENT_DATA = {
    "data_type": "kline",
    "interval_code": "1h",
    "issue_type": "boundary_error",
    "severity": "warning",
    "description": "Detected non-aligned kline open time",
}

SAMPLE_STREAM_STATUS_DATA = {
    "stream_name": "binance.perp.kline.1h.BTCUSDT",
    "status": "reconnected",
    "description": "WebSocket reconnected successfully",
}


@pytest.fixture()
def hub() -> ConnectionHub:
    """Return a ConnectionHub with broadcast mocked out."""
    h = ConnectionHub()
    h.broadcast = AsyncMock()  # type: ignore[assignment]
    return h


@pytest.fixture()
def publisher(hub: ConnectionHub) -> SystemEventPublisher:
    return SystemEventPublisher(hub)


# ---------------------------------------------------------------------------
# Tests: build_quality_event_message
# ---------------------------------------------------------------------------


class TestBuildQualityEventMessage:
    """Tests for the ``build_quality_event_message`` pure helper."""

    def test_envelope_type_is_event(self):
        msg = build_quality_event_message(
            VENUE, MARKET_TYPE, SYMBOL, SAMPLE_QUALITY_EVENT_DATA
        )
        assert msg["type"] == "event"

    def test_correct_topic(self):
        msg = build_quality_event_message(
            VENUE, MARKET_TYPE, SYMBOL, SAMPLE_QUALITY_EVENT_DATA
        )
        assert msg["topic"] == TOPIC_QUALITY_EVENT

    def test_routing_fields_at_top_level(self):
        msg = build_quality_event_message(
            VENUE, MARKET_TYPE, SYMBOL, SAMPLE_QUALITY_EVENT_DATA
        )
        assert msg["venue"] == VENUE
        assert msg["market_type"] == MARKET_TYPE
        assert msg["symbol"] == SYMBOL

    def test_ts_ms_is_integer(self):
        msg = build_quality_event_message(
            VENUE, MARKET_TYPE, SYMBOL, SAMPLE_QUALITY_EVENT_DATA
        )
        assert "ts_ms" in msg
        assert isinstance(msg["ts_ms"], int)

    def test_data_fields_present(self):
        msg = build_quality_event_message(
            VENUE, MARKET_TYPE, SYMBOL, SAMPLE_QUALITY_EVENT_DATA
        )
        assert msg["data"]["data_type"] == "kline"
        assert msg["data"]["interval_code"] == "1h"
        assert msg["data"]["issue_type"] == "boundary_error"
        assert msg["data"]["severity"] == "warning"
        assert msg["data"]["description"] == "Detected non-aligned kline open time"

    def test_missing_optional_data_fields_handled_gracefully(self):
        """Only provided fields appear in data; missing ones are absent."""
        partial_data = {
            "data_type": "kline",
            "issue_type": "gap",
            "severity": "error",
        }
        msg = build_quality_event_message(VENUE, MARKET_TYPE, SYMBOL, partial_data)
        assert "data_type" in msg["data"]
        assert "issue_type" in msg["data"]
        assert "severity" in msg["data"]
        # interval_code and description were not provided — must be absent
        assert "interval_code" not in msg["data"]
        assert "description" not in msg["data"]

    @pytest.mark.parametrize("severity", ["warning", "error", "info"])
    def test_different_severity_levels(self, severity: str):
        data = {**SAMPLE_QUALITY_EVENT_DATA, "severity": severity}
        msg = build_quality_event_message(VENUE, MARKET_TYPE, SYMBOL, data)
        assert msg["data"]["severity"] == severity

    def test_empty_data_dict(self):
        """Empty data dict should produce envelope with empty data."""
        msg = build_quality_event_message(VENUE, MARKET_TYPE, SYMBOL, {})
        assert msg["data"] == {}
        # Top-level fields still present
        assert msg["type"] == "event"
        assert msg["topic"] == TOPIC_QUALITY_EVENT
        assert msg["venue"] == VENUE


# ---------------------------------------------------------------------------
# Tests: build_stream_status_message
# ---------------------------------------------------------------------------


class TestBuildStreamStatusMessage:
    """Tests for the ``build_stream_status_message`` pure helper."""

    def test_envelope_type_is_event(self):
        msg = build_stream_status_message(SAMPLE_STREAM_STATUS_DATA)
        assert msg["type"] == "event"

    def test_correct_topic(self):
        msg = build_stream_status_message(SAMPLE_STREAM_STATUS_DATA)
        assert msg["topic"] == TOPIC_STREAM_STATUS

    def test_no_venue_market_type_symbol_at_top_level(self):
        """stream_status is global — no routing fields at top level."""
        msg = build_stream_status_message(SAMPLE_STREAM_STATUS_DATA)
        assert "venue" not in msg
        assert "market_type" not in msg
        assert "symbol" not in msg

    def test_ts_ms_is_integer(self):
        msg = build_stream_status_message(SAMPLE_STREAM_STATUS_DATA)
        assert "ts_ms" in msg
        assert isinstance(msg["ts_ms"], int)

    def test_data_fields_present(self):
        msg = build_stream_status_message(SAMPLE_STREAM_STATUS_DATA)
        assert msg["data"]["stream_name"] == "binance.perp.kline.1h.BTCUSDT"
        assert msg["data"]["status"] == "reconnected"
        assert msg["data"]["description"] == "WebSocket reconnected successfully"

    def test_missing_optional_data_fields_handled_gracefully(self):
        partial_data = {
            "stream_name": "binance.perp.kline.1h.BTCUSDT",
            "status": "disconnected",
        }
        msg = build_stream_status_message(partial_data)
        assert "stream_name" in msg["data"]
        assert "status" in msg["data"]
        assert "description" not in msg["data"]

    @pytest.mark.parametrize("status", ["reconnected", "disconnected", "connected"])
    def test_different_statuses(self, status: str):
        data = {**SAMPLE_STREAM_STATUS_DATA, "status": status}
        msg = build_stream_status_message(data)
        assert msg["data"]["status"] == status

    def test_empty_data_dict(self):
        """Empty data dict should produce envelope with empty data."""
        msg = build_stream_status_message({})
        assert msg["data"] == {}
        assert msg["type"] == "event"
        assert msg["topic"] == TOPIC_STREAM_STATUS


# ---------------------------------------------------------------------------
# Tests: SystemEventPublisher async methods
# ---------------------------------------------------------------------------


class TestSystemEventPublisher:
    """Verify publish methods call hub.broadcast with the correct
    topic_key and message envelope.
    """

    @pytest.mark.asyncio
    async def test_publish_quality_event_topic_key(
        self, publisher: SystemEventPublisher, hub: ConnectionHub
    ):
        await publisher.publish_quality_event(
            VENUE, MARKET_TYPE, SYMBOL, SAMPLE_QUALITY_EVENT_DATA
        )
        hub.broadcast.assert_awaited_once()
        topic_key, msg = hub.broadcast.call_args[0]
        assert topic_key == f"system.quality_event:{VENUE}:{MARKET_TYPE}:{SYMBOL}"

    @pytest.mark.asyncio
    async def test_publish_quality_event_message_envelope(
        self, publisher: SystemEventPublisher, hub: ConnectionHub
    ):
        await publisher.publish_quality_event(
            VENUE, MARKET_TYPE, SYMBOL, SAMPLE_QUALITY_EVENT_DATA
        )
        _, msg = hub.broadcast.call_args[0]
        assert msg["type"] == "event"
        assert msg["topic"] == TOPIC_QUALITY_EVENT
        assert msg["venue"] == VENUE
        assert msg["market_type"] == MARKET_TYPE
        assert msg["symbol"] == SYMBOL
        assert msg["data"]["severity"] == "warning"

    @pytest.mark.asyncio
    async def test_publish_stream_status_topic_key(
        self, publisher: SystemEventPublisher, hub: ConnectionHub
    ):
        """stream_status topic key has no routing fields — just the topic name."""
        await publisher.publish_stream_status(SAMPLE_STREAM_STATUS_DATA)
        hub.broadcast.assert_awaited_once()
        topic_key, _ = hub.broadcast.call_args[0]
        assert topic_key == "system.stream_status"

    @pytest.mark.asyncio
    async def test_publish_stream_status_message_envelope(
        self, publisher: SystemEventPublisher, hub: ConnectionHub
    ):
        await publisher.publish_stream_status(SAMPLE_STREAM_STATUS_DATA)
        _, msg = hub.broadcast.call_args[0]
        assert msg["type"] == "event"
        assert msg["topic"] == TOPIC_STREAM_STATUS
        # No routing fields at top level
        assert "venue" not in msg
        assert "market_type" not in msg
        assert "symbol" not in msg
        assert msg["data"]["status"] == "reconnected"

    @pytest.mark.asyncio
    async def test_publish_quality_event_different_severities(
        self, publisher: SystemEventPublisher, hub: ConnectionHub
    ):
        """quality_event broadcasts with warning, error, and info severity."""
        for severity in ("warning", "error", "info"):
            hub.broadcast.reset_mock()
            data = {**SAMPLE_QUALITY_EVENT_DATA, "severity": severity}
            await publisher.publish_quality_event(VENUE, MARKET_TYPE, SYMBOL, data)
            _, msg = hub.broadcast.call_args[0]
            assert msg["data"]["severity"] == severity

    @pytest.mark.asyncio
    async def test_publish_stream_status_different_statuses(
        self, publisher: SystemEventPublisher, hub: ConnectionHub
    ):
        """stream_status broadcasts with reconnected, disconnected, and connected."""
        for status in ("reconnected", "disconnected", "connected"):
            hub.broadcast.reset_mock()
            data = {**SAMPLE_STREAM_STATUS_DATA, "status": status}
            await publisher.publish_stream_status(data)
            _, msg = hub.broadcast.call_args[0]
            assert msg["data"]["status"] == status

    @pytest.mark.asyncio
    async def test_publish_quality_event_with_missing_fields(
        self, publisher: SystemEventPublisher, hub: ConnectionHub
    ):
        """Missing optional data fields should not cause errors."""
        partial_data = {"data_type": "kline", "severity": "error"}
        await publisher.publish_quality_event(VENUE, MARKET_TYPE, SYMBOL, partial_data)
        _, msg = hub.broadcast.call_args[0]
        assert msg["data"]["data_type"] == "kline"
        assert "interval_code" not in msg["data"]

    @pytest.mark.asyncio
    async def test_publish_stream_status_with_missing_fields(
        self, publisher: SystemEventPublisher, hub: ConnectionHub
    ):
        """Missing optional data fields should not cause errors."""
        partial_data = {"stream_name": "binance.perp.kline.1h.BTCUSDT"}
        await publisher.publish_stream_status(partial_data)
        _, msg = hub.broadcast.call_args[0]
        assert msg["data"]["stream_name"] == "binance.perp.kline.1h.BTCUSDT"
        assert "status" not in msg["data"]

    @pytest.mark.asyncio
    async def test_quality_event_topic_key_format(
        self, publisher: SystemEventPublisher, hub: ConnectionHub
    ):
        """Verify the full topic key string matches subscription format."""
        await publisher.publish_quality_event("okx", "spot", "ETHUSDT", {})
        topic_key, _ = hub.broadcast.call_args[0]
        assert topic_key == "system.quality_event:okx:spot:ETHUSDT"

    @pytest.mark.asyncio
    async def test_multiple_publishes(self, publisher: SystemEventPublisher, hub: ConnectionHub):
        """Sequential publishes each call broadcast correctly."""
        await publisher.publish_quality_event(VENUE, MARKET_TYPE, SYMBOL, {})
        await publisher.publish_stream_status({})
        assert hub.broadcast.await_count == 2
