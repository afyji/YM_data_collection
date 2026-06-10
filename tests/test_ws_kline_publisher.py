"""Tests for the kline WebSocket push publisher (DC-T036)."""

from __future__ import annotations

import time
from unittest.mock import AsyncMock, MagicMock

import pytest

from YM_data_collection.ws.publishers.kline_publisher import (
    KlinePublisher,
    build_kline_message,
)


# ── Fixtures ──────────────────────────────────────────────────────────────

SAMPLE_KLINE_DATA = {
    "instrument_code": "crypto.binance.perp.BTCUSDT",
    "interval_code": "1h",
    "open_ts_ms": 1710000000000,
    "close_ts_ms": 1710003599999,
    "open_price": "68000.10",
    "high_price": "68120.50",
    "low_price": "67980.00",
    "close_price": "68080.20",
    "volume": "123.45670000",
    "quote_volume": "8401234.12000000",
    "trade_count": 3201,
    "taker_buy_base_volume": "62.30000000",
    "taker_buy_quote_volume": "4240000.23000000",
    "is_closed": False,
}


def _make_hub() -> MagicMock:
    """Return a mock ConnectionHub with an async ``broadcast`` method."""
    hub = MagicMock()
    hub.broadcast = AsyncMock()
    return hub


# ── build_kline_message tests ─────────────────────────────────────────────


class TestBuildKlineMessage:
    """Tests for the pure ``build_kline_message`` helper."""

    def test_produces_correct_envelope(self):
        msg = build_kline_message(
            venue="binance",
            market_type="perp",
            symbol="BTCUSDT",
            interval="1h",
            kline_data=SAMPLE_KLINE_DATA,
        )

        # Top-level envelope fields
        assert msg["type"] == "update"
        assert msg["topic"] == "marketdata.kline"
        assert msg["venue"] == "binance"
        assert msg["market_type"] == "perp"
        assert msg["symbol"] == "BTCUSDT"
        assert msg["interval"] == "1h"
        assert isinstance(msg["ts_ms"], int)
        # ts_ms should be close to now
        assert abs(msg["ts_ms"] - int(time.time() * 1000)) < 2000

        # Data payload preserves all fields
        data = msg["data"]
        assert data["instrument_code"] == "crypto.binance.perp.BTCUSDT"
        assert data["interval_code"] == "1h"
        assert data["open_ts_ms"] == 1710000000000
        assert data["close_ts_ms"] == 1710003599999
        assert data["open_price"] == "68000.10"
        assert data["high_price"] == "68120.50"
        assert data["low_price"] == "67980.00"
        assert data["close_price"] == "68080.20"
        assert data["volume"] == "123.45670000"
        assert data["quote_volume"] == "8401234.12000000"
        assert data["trade_count"] == 3201
        assert data["taker_buy_base_volume"] == "62.30000000"
        assert data["taker_buy_quote_volume"] == "4240000.23000000"
        assert data["is_closed"] is False

    def test_is_closed_true_propagates(self):
        data = {**SAMPLE_KLINE_DATA, "is_closed": True}
        msg = build_kline_message("binance", "perp", "BTCUSDT", "1h", data)
        assert msg["data"]["is_closed"] is True

    def test_is_closed_false_propagates(self):
        data = {**SAMPLE_KLINE_DATA, "is_closed": False}
        msg = build_kline_message("binance", "perp", "BTCUSDT", "1h", data)
        assert msg["data"]["is_closed"] is False

    def test_missing_optional_fields_handled_gracefully(self):
        """Omit optional fields and ensure no KeyError is raised."""
        minimal_data = {
            "instrument_code": "crypto.binance.perp.BTCUSDT",
            "interval_code": "1h",
            "open_ts_ms": 1710000000000,
            "close_ts_ms": 1710003599999,
            "open_price": "68000.10",
            "high_price": "68120.50",
            "low_price": "67980.00",
            "close_price": "68080.20",
            "volume": "123.45670000",
            "is_closed": False,
            # quote_volume, trade_count, taker_buy_base_volume,
            # taker_buy_quote_volume are missing
        }
        msg = build_kline_message("binance", "perp", "BTCUSDT", "1h", minimal_data)
        data = msg["data"]

        # Present fields should be there
        assert "instrument_code" in data
        assert "volume" in data
        assert "is_closed" in data

        # Missing optional fields should simply be absent
        assert "quote_volume" not in data
        assert "trade_count" not in data
        assert "taker_buy_base_volume" not in data
        assert "taker_buy_quote_volume" not in data

    def test_extra_fields_not_leaked(self):
        """Unknown keys in kline_data must not appear in data payload."""
        data = {**SAMPLE_KLINE_DATA, "unexpected_field": "oops"}
        msg = build_kline_message("binance", "perp", "BTCUSDT", "1h", data)
        assert "unexpected_field" not in msg["data"]


# ── KlinePublisher.publish_kline tests ────────────────────────────────────


class TestKlinePublisher:
    """Tests for the ``KlinePublisher`` class."""

    @pytest.mark.asyncio
    async def test_publish_kline_calls_broadcast_with_correct_topic_key(self):
        hub = _make_hub()
        publisher = KlinePublisher(hub)

        await publisher.publish_kline(
            venue="binance",
            market_type="perp",
            symbol="BTCUSDT",
            interval="1h",
            kline_data=SAMPLE_KLINE_DATA,
        )

        hub.broadcast.assert_awaited_once()
        call_args = hub.broadcast.call_args
        topic_key = call_args[0][0]
        message = call_args[0][1]

        assert topic_key == "marketdata.kline:binance:perp:BTCUSDT:1h"
        assert message["type"] == "update"
        assert message["topic"] == "marketdata.kline"
        assert message["venue"] == "binance"
        assert message["market_type"] == "perp"
        assert message["symbol"] == "BTCUSDT"
        assert message["interval"] == "1h"
        assert isinstance(message["ts_ms"], int)
        assert message["data"]["is_closed"] is False

    @pytest.mark.asyncio
    async def test_publish_kline_is_closed_true(self):
        hub = _make_hub()
        publisher = KlinePublisher(hub)

        data = {**SAMPLE_KLINE_DATA, "is_closed": True}
        await publisher.publish_kline("binance", "perp", "BTCUSDT", "1h", data)

        message = hub.broadcast.call_args[0][1]
        assert message["data"]["is_closed"] is True

    @pytest.mark.asyncio
    async def test_publish_kline_is_closed_false(self):
        hub = _make_hub()
        publisher = KlinePublisher(hub)

        data = {**SAMPLE_KLINE_DATA, "is_closed": False}
        await publisher.publish_kline("binance", "perp", "BTCUSDT", "1h", data)

        message = hub.broadcast.call_args[0][1]
        assert message["data"]["is_closed"] is False

    @pytest.mark.asyncio
    async def test_publish_kline_missing_optional_fields(self):
        hub = _make_hub()
        publisher = KlinePublisher(hub)

        minimal_data = {
            "instrument_code": "crypto.binance.perp.BTCUSDT",
            "interval_code": "5m",
            "open_ts_ms": 1710000000000,
            "close_ts_ms": 1710000299999,
            "open_price": "50000.00",
            "high_price": "50010.00",
            "low_price": "49990.00",
            "close_price": "50005.00",
            "volume": "10.00000000",
            "is_closed": True,
        }
        await publisher.publish_kline("binance", "spot", "ETHUSDT", "5m", minimal_data)

        message = hub.broadcast.call_args[0][1]
        topic_key = hub.broadcast.call_args[0][0]

        assert topic_key == "marketdata.kline:binance:spot:ETHUSDT:5m"
        assert "quote_volume" not in message["data"]
        assert "trade_count" not in message["data"]
        assert message["data"]["is_closed"] is True
