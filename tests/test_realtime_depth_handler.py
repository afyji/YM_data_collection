"""Tests for ingestion/realtime_depth_handler.py."""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from YM_data_collection.config.models import (
    RealtimePersistenceConfig,
    WritePolicy,
    WritePolicyConfig,
)
from YM_data_collection.ingestion.realtime_depth_handler import RealtimeDepthHandler


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def redis_client():
    """Mock RedisCacheClient."""
    client = MagicMock()
    client.set_json = MagicMock(return_value=True)
    client.build_key = MagicMock(return_value="ym:binance:depth_snapshot:spot:btcusdt")
    return client


@pytest.fixture()
def config():
    return RealtimePersistenceConfig(
        mysql_flush_interval_seconds=60,
        redis_retention_after_flush_seconds=120,
        write_policy=WritePolicyConfig(depth_snapshot=WritePolicy.redis_first),
    )


@pytest.fixture()
def handler(redis_client, config):
    return RealtimeDepthHandler(
        redis_client=redis_client,
        config=config,
        venue="binance",
    )


def _make_depth_data(bids=None, asks=None):
    """Build a minimal Binance depth message dict."""
    default_bids = [
        ["50000.00", "1.500"],
        ["49999.00", "0.800"],
    ]
    default_asks = [
        ["50001.00", "2.000"],
        ["50002.00", "1.200"],
    ]
    return {
        "lastUpdateId": 160,
        "bids": bids if bids is not None else default_bids,
        "asks": asks if asks is not None else default_asks,
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_depth_snapshot_cached_to_redis(handler, redis_client):
    """Data goes to Redis via set_json."""
    data = _make_depth_data()
    await handler.handle_message("spot", "BTCUSDT", data)

    redis_client.set_json.assert_called_once()
    call_args = redis_client.set_json.call_args
    key_parts = call_args[0]
    assert key_parts == ("depth_snapshot", "spot", "BTCUSDT")

    payload = call_args.kwargs.get("payload") or call_args[1].get("payload")
    assert payload is not None
    assert payload["venue"] == "binance"
    assert payload["symbol"] == "BTCUSDT"
    assert payload["market_type"] == "spot"


@pytest.mark.asyncio
async def test_mid_price_computed_correctly(handler, redis_client):
    """mid_price = (50000 + 50001) / 2 = 50000.5"""
    data = _make_depth_data()
    await handler.handle_message("spot", "BTCUSDT", data)

    payload = redis_client.set_json.call_args.kwargs.get("payload") or redis_client.set_json.call_args[1].get("payload")
    assert Decimal(payload["mid_price"]) == Decimal("50000.5")


@pytest.mark.asyncio
async def test_spread_computed_correctly(handler, redis_client):
    """spread_abs = 50001 - 50000 = 1"""
    data = _make_depth_data()
    await handler.handle_message("spot", "BTCUSDT", data)

    payload = redis_client.set_json.call_args.kwargs.get("payload") or redis_client.set_json.call_args[1].get("payload")
    assert Decimal(payload["spread_abs"]) == Decimal("1")


@pytest.mark.asyncio
async def test_empty_bids_handled(handler, redis_client):
    """No bids — handler should still produce a snapshot with best_bid = 0."""
    data = _make_depth_data(bids=[], asks=[["50001.00", "2.000"]])
    await handler.handle_message("spot", "BTCUSDT", data)

    redis_client.set_json.assert_called_once()
    payload = redis_client.set_json.call_args.kwargs.get("payload") or redis_client.set_json.call_args[1].get("payload")
    assert Decimal(payload["best_bid_price"]) == Decimal("0")
    assert Decimal(payload["best_ask_price"]) == Decimal("50001")
    assert Decimal(payload["spread_abs"]) == Decimal("0")


@pytest.mark.asyncio
async def test_empty_asks_handled(handler, redis_client):
    """No asks — handler should still produce a snapshot with best_ask = 0."""
    data = _make_depth_data(bids=[["50000.00", "1.500"]], asks=[])
    await handler.handle_message("spot", "BTCUSDT", data)

    redis_client.set_json.assert_called_once()
    payload = redis_client.set_json.call_args.kwargs.get("payload") or redis_client.set_json.call_args[1].get("payload")
    assert Decimal(payload["best_bid_price"]) == Decimal("50000")
    assert Decimal(payload["best_ask_price"]) == Decimal("0")
    assert Decimal(payload["spread_abs"]) == Decimal("0")


@pytest.mark.asyncio
async def test_no_mysql_write(handler, redis_client):
    """No session_factory or repository interaction — Redis only."""
    data = _make_depth_data()
    await handler.handle_message("spot", "BTCUSDT", data)

    # Only redis set_json should have been called — no other write methods
    assert redis_client.set_json.call_count == 1
    # Verify the handler has no repo / session_factory attribute
    assert not hasattr(handler, "_repo")
    assert not hasattr(handler, "_session_factory")


@pytest.mark.asyncio
async def test_redis_key_includes_market_type_and_symbol(handler, redis_client):
    """Key parts passed to set_json must be (depth_snapshot, <market_type>, <symbol>)."""
    data = _make_depth_data()
    await handler.handle_message("perp", "ETHUSDT", data)

    call_args = redis_client.set_json.call_args
    key_parts = call_args[0]
    assert key_parts == ("depth_snapshot", "perp", "ETHUSDT")


@pytest.mark.asyncio
async def test_multiple_levels(handler, redis_client):
    """20-level depth processed correctly."""
    bids = [[str(50000 - i), str(1.0 + i * 0.1)] for i in range(20)]
    asks = [[str(50001 + i), str(2.0 + i * 0.1)] for i in range(20)]
    data = _make_depth_data(bids=bids, asks=asks)

    await handler.handle_message("spot", "BTCUSDT", data)

    redis_client.set_json.assert_called_once()
    payload = redis_client.set_json.call_args.kwargs.get("payload") or redis_client.set_json.call_args[1].get("payload")

    assert payload["depth_levels"] == 20
    assert Decimal(payload["best_bid_price"]) == Decimal("50000")
    assert Decimal(payload["best_ask_price"]) == Decimal("50001")
    assert Decimal(payload["mid_price"]) == Decimal("50000.5")
    assert len(payload["bid_depth_json"]) == 20
    assert len(payload["ask_depth_json"]) == 20


@pytest.mark.asyncio
async def test_empty_bids_and_asks_skips(handler, redis_client):
    """Completely empty order book should be skipped (no Redis write)."""
    data = _make_depth_data(bids=[], asks=[])
    await handler.handle_message("spot", "BTCUSDT", data)

    redis_client.set_json.assert_not_called()


@pytest.mark.asyncio
async def test_spread_bps_computed(handler, redis_client):
    """spread_bps = (spread_abs / mid_price) * 10000."""
    data = _make_depth_data()
    await handler.handle_message("spot", "BTCUSDT", data)

    payload = redis_client.set_json.call_args.kwargs.get("payload") or redis_client.set_json.call_args[1].get("payload")
    spread_abs = Decimal(payload["spread_abs"])
    mid_price = Decimal(payload["mid_price"])
    expected_bps = (spread_abs / mid_price) * Decimal("10000")
    # Allow small rounding tolerance
    assert abs(Decimal(payload["spread_bps"]) - expected_bps) < Decimal("0.01")


@pytest.mark.asyncio
async def test_instrument_code_format(handler, redis_client):
    """instrument_code = crypto.<venue>.<market_type>.<symbol>"""
    data = _make_depth_data()
    await handler.handle_message("perp", "BTCUSDT", data)

    payload = redis_client.set_json.call_args.kwargs.get("payload") or redis_client.set_json.call_args[1].get("payload")
    assert payload["instrument_code"] == "crypto.binance.perp.BTCUSDT"


@pytest.mark.asyncio
async def test_futures_depth_b_a_fields_are_supported(handler, redis_client):
    """Binance futures depth streams use compact b/a fields instead of bids/asks."""
    data = {
        "e": "depthUpdate",
        "b": [["50000.00", "1.500"]],
        "a": [["50001.00", "2.000"]],
    }

    await handler.handle_message("perp", "BTCUSDT", data)

    redis_client.set_json.assert_called_once()
    payload = redis_client.set_json.call_args.kwargs.get("payload") or redis_client.set_json.call_args[1].get("payload")
    assert payload["market_type"] == "perp"
    assert Decimal(payload["best_bid_price"]) == Decimal("50000")
    assert Decimal(payload["best_ask_price"]) == Decimal("50001")
