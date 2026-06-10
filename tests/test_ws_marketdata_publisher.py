"""Tests for YM_data_collection.ws.publishers.marketdata_publisher."""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock

import pytest

from YM_data_collection.ws.hub import ConnectionHub
from YM_data_collection.ws.publishers.marketdata_publisher import (
    TOPIC_DEPTH_SNAPSHOT,
    TOPIC_FUNDING_RATE,
    TOPIC_INDEX_PRICE,
    TOPIC_MARK_PRICE,
    TOPIC_OPEN_INTEREST,
    MarketDataPublisher,
    build_depth_snapshot_message,
    build_funding_rate_message,
    build_index_price_message,
    build_mark_price_message,
    build_marketdata_message,
    build_open_interest_message,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

VENUE = "binance"
MARKET_TYPE = "perp"
SYMBOL = "BTCUSDT"

SAMPLE_MARK_PRICE_DATA = {
    "instrument_code": "BTCUSDT",
    "event_ts_ms": 1700000000000,
    "mark_price": "43000.50",
    "funding_rate": "0.0001",
    "next_funding_time_ts_ms": 1700011200000,
}

SAMPLE_INDEX_PRICE_DATA = {
    "instrument_code": "BTCUSDT",
    "event_ts_ms": 1700000000000,
    "index_price": "42995.00",
}

SAMPLE_OPEN_INTEREST_DATA = {
    "instrument_code": "BTCUSDT",
    "event_ts_ms": 1700000000000,
    "open_interest": "12345.67",
}

SAMPLE_FUNDING_RATE_DATA = {
    "instrument_code": "BTCUSDT",
    "funding_time_ts_ms": 1700011200000,
    "funding_rate": "0.0001",
    "mark_price": "43000.50",
}

SAMPLE_DEPTH_SNAPSHOT_DATA = {
    "instrument_code": "BTCUSDT",
    "event_ts_ms": 1700000000000,
    "best_bid_price": "42999.00",
    "best_bid_qty": "1.5",
    "best_ask_price": "43001.00",
    "best_ask_qty": "2.0",
    "mid_price": "43000.00",
    "spread_abs": "2.00",
    "spread_bps": "4.65",
    "depth_levels": 10,
    "bids": [["42999.00", "1.5"]],
    "asks": [["43001.00", "2.0"]],
}


@pytest.fixture()
def hub() -> ConnectionHub:
    """Return a ConnectionHub with broadcast mocked out."""
    h = ConnectionHub()
    h.broadcast = AsyncMock()  # type: ignore[assignment]
    return h


@pytest.fixture()
def publisher(hub: ConnectionHub) -> MarketDataPublisher:
    return MarketDataPublisher(hub)


# ---------------------------------------------------------------------------
# Tests: generic build_marketdata_message
# ---------------------------------------------------------------------------


class TestBuildMarketdataMessage:
    """Tests for the generic ``build_marketdata_message`` helper."""

    def test_basic_envelope_shape(self):
        msg = build_marketdata_message(
            "marketdata.mark_price", "update", VENUE, MARKET_TYPE, SYMBOL, {"k": "v"}
        )
        assert msg["type"] == "update"
        assert msg["topic"] == "marketdata.mark_price"
        assert msg["venue"] == VENUE
        assert msg["market_type"] == MARKET_TYPE
        assert msg["symbol"] == SYMBOL
        assert "ts_ms" in msg
        assert isinstance(msg["ts_ms"], int)
        assert msg["data"] == {"k": "v"}

    def test_snapshot_type(self):
        msg = build_marketdata_message(
            "marketdata.depth_snapshot", "snapshot", VENUE, MARKET_TYPE, SYMBOL, {}
        )
        assert msg["type"] == "snapshot"


# ---------------------------------------------------------------------------
# Tests: convenience build_*_message functions
# ---------------------------------------------------------------------------


class TestBuildMarkPriceMessage:
    def test_type_is_update(self):
        msg = build_mark_price_message(VENUE, MARKET_TYPE, SYMBOL, SAMPLE_MARK_PRICE_DATA)
        assert msg["type"] == "update"
        assert msg["topic"] == TOPIC_MARK_PRICE
        assert msg["venue"] == VENUE
        assert msg["market_type"] == MARKET_TYPE
        assert msg["symbol"] == SYMBOL
        assert msg["data"] == SAMPLE_MARK_PRICE_DATA

    def test_data_passthrough(self):
        data = {"mark_price": "99999.00"}
        msg = build_mark_price_message(VENUE, MARKET_TYPE, SYMBOL, data)
        assert msg["data"] is data


class TestBuildIndexPriceMessage:
    def test_type_is_update(self):
        msg = build_index_price_message(VENUE, MARKET_TYPE, SYMBOL, SAMPLE_INDEX_PRICE_DATA)
        assert msg["type"] == "update"
        assert msg["topic"] == TOPIC_INDEX_PRICE
        assert msg["data"] == SAMPLE_INDEX_PRICE_DATA


class TestBuildOpenInterestMessage:
    def test_type_is_update(self):
        msg = build_open_interest_message(VENUE, MARKET_TYPE, SYMBOL, SAMPLE_OPEN_INTEREST_DATA)
        assert msg["type"] == "update"
        assert msg["topic"] == TOPIC_OPEN_INTEREST
        assert msg["data"] == SAMPLE_OPEN_INTEREST_DATA


class TestBuildFundingRateMessage:
    def test_type_is_update(self):
        msg = build_funding_rate_message(VENUE, MARKET_TYPE, SYMBOL, SAMPLE_FUNDING_RATE_DATA)
        assert msg["type"] == "update"
        assert msg["topic"] == TOPIC_FUNDING_RATE
        assert msg["data"] == SAMPLE_FUNDING_RATE_DATA


class TestBuildDepthSnapshotMessage:
    def test_type_is_snapshot_not_update(self):
        msg = build_depth_snapshot_message(VENUE, MARKET_TYPE, SYMBOL, SAMPLE_DEPTH_SNAPSHOT_DATA)
        # The key requirement: depth_snapshot uses type='snapshot', NOT 'update'
        assert msg["type"] == "snapshot"
        assert msg["type"] != "update"
        assert msg["topic"] == TOPIC_DEPTH_SNAPSHOT
        assert msg["data"] == SAMPLE_DEPTH_SNAPSHOT_DATA

    def test_depth_data_fields_preserved(self):
        msg = build_depth_snapshot_message(VENUE, MARKET_TYPE, SYMBOL, SAMPLE_DEPTH_SNAPSHOT_DATA)
        assert msg["data"]["bids"] == [["42999.00", "1.5"]]
        assert msg["data"]["asks"] == [["43001.00", "2.0"]]
        assert msg["data"]["depth_levels"] == 10


# ---------------------------------------------------------------------------
# Tests: MarketDataPublisher async methods
# ---------------------------------------------------------------------------


class TestMarketDataPublisher:
    """Verify each publish_* method calls hub.broadcast with the correct
    topic_key and message envelope."""

    @pytest.mark.asyncio
    async def test_publish_mark_price(self, publisher: MarketDataPublisher, hub: ConnectionHub):
        await publisher.publish_mark_price(VENUE, MARKET_TYPE, SYMBOL, SAMPLE_MARK_PRICE_DATA)
        hub.broadcast.assert_awaited_once()
        topic_key, msg = hub.broadcast.call_args[0]
        assert topic_key == f"{TOPIC_MARK_PRICE}:{VENUE}:{MARKET_TYPE}:{SYMBOL}"
        assert msg["type"] == "update"
        assert msg["topic"] == TOPIC_MARK_PRICE
        assert msg["data"] == SAMPLE_MARK_PRICE_DATA

    @pytest.mark.asyncio
    async def test_publish_index_price(self, publisher: MarketDataPublisher, hub: ConnectionHub):
        await publisher.publish_index_price(VENUE, MARKET_TYPE, SYMBOL, SAMPLE_INDEX_PRICE_DATA)
        hub.broadcast.assert_awaited_once()
        topic_key, msg = hub.broadcast.call_args[0]
        assert topic_key == f"{TOPIC_INDEX_PRICE}:{VENUE}:{MARKET_TYPE}:{SYMBOL}"
        assert msg["type"] == "update"
        assert msg["topic"] == TOPIC_INDEX_PRICE
        assert msg["data"] == SAMPLE_INDEX_PRICE_DATA

    @pytest.mark.asyncio
    async def test_publish_open_interest(self, publisher: MarketDataPublisher, hub: ConnectionHub):
        await publisher.publish_open_interest(VENUE, MARKET_TYPE, SYMBOL, SAMPLE_OPEN_INTEREST_DATA)
        hub.broadcast.assert_awaited_once()
        topic_key, msg = hub.broadcast.call_args[0]
        assert topic_key == f"{TOPIC_OPEN_INTEREST}:{VENUE}:{MARKET_TYPE}:{SYMBOL}"
        assert msg["type"] == "update"
        assert msg["topic"] == TOPIC_OPEN_INTEREST
        assert msg["data"] == SAMPLE_OPEN_INTEREST_DATA

    @pytest.mark.asyncio
    async def test_publish_funding_rate(self, publisher: MarketDataPublisher, hub: ConnectionHub):
        await publisher.publish_funding_rate(VENUE, MARKET_TYPE, SYMBOL, SAMPLE_FUNDING_RATE_DATA)
        hub.broadcast.assert_awaited_once()
        topic_key, msg = hub.broadcast.call_args[0]
        assert topic_key == f"{TOPIC_FUNDING_RATE}:{VENUE}:{MARKET_TYPE}:{SYMBOL}"
        assert msg["type"] == "update"
        assert msg["topic"] == TOPIC_FUNDING_RATE
        assert msg["data"] == SAMPLE_FUNDING_RATE_DATA

    @pytest.mark.asyncio
    async def test_publish_depth_snapshot(self, publisher: MarketDataPublisher, hub: ConnectionHub):
        await publisher.publish_depth_snapshot(VENUE, MARKET_TYPE, SYMBOL, SAMPLE_DEPTH_SNAPSHOT_DATA)
        hub.broadcast.assert_awaited_once()
        topic_key, msg = hub.broadcast.call_args[0]
        assert topic_key == f"{TOPIC_DEPTH_SNAPSHOT}:{VENUE}:{MARKET_TYPE}:{SYMBOL}"
        # depth_snapshot must use type='snapshot', not type='update'
        assert msg["type"] == "snapshot"
        assert msg["topic"] == TOPIC_DEPTH_SNAPSHOT
        assert msg["data"] == SAMPLE_DEPTH_SNAPSHOT_DATA

    @pytest.mark.asyncio
    async def test_depth_snapshot_topic_key_format(self, publisher: MarketDataPublisher, hub: ConnectionHub):
        """Verify the full topic key string matches subscription format."""
        await publisher.publish_depth_snapshot("okx", "spot", "ETHUSDT", {})
        topic_key, _ = hub.broadcast.call_args[0]
        assert topic_key == "marketdata.depth_snapshot:okx:spot:ETHUSDT"

    @pytest.mark.asyncio
    async def test_multiple_publishes(self, publisher: MarketDataPublisher, hub: ConnectionHub):
        """Verify sequential publishes each call broadcast correctly."""
        await publisher.publish_mark_price(VENUE, MARKET_TYPE, SYMBOL, {})
        await publisher.publish_index_price(VENUE, MARKET_TYPE, SYMBOL, {})
        await publisher.publish_open_interest(VENUE, MARKET_TYPE, SYMBOL, {})
        await publisher.publish_funding_rate(VENUE, MARKET_TYPE, SYMBOL, {})
        await publisher.publish_depth_snapshot(VENUE, MARKET_TYPE, SYMBOL, {})
        assert hub.broadcast.await_count == 5
