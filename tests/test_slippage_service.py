"""Tests for SlippageService."""

from __future__ import annotations

import time
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from YM_data_collection.config.models import DepthConfig, SlippageConfig
from YM_data_collection.services.slippage_service import SlippageService


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _now_ms():
    return int(time.time() * 1000)


def _make_depth_data(
    event_ts_ms: int | None = None,
    best_bid: str = "50000.00",
    best_ask: str = "50001.00",
    mid: str = "50000.50",
    bid_depth: list | None = None,
    ask_depth: list | None = None,
) -> dict:
    if event_ts_ms is None:
        event_ts_ms = _now_ms()
    if bid_depth is None:
        bid_depth = [
            ["50000.00", "1.0"],
            ["49999.00", "2.0"],
            ["49998.00", "3.0"],
        ]
    if ask_depth is None:
        ask_depth = [
            ["50001.00", "1.0"],
            ["50002.00", "2.0"],
            ["50003.00", "3.0"],
        ]
    return {
        "event_ts_ms": event_ts_ms,
        "best_bid_price": best_bid,
        "best_ask_price": best_ask,
        "mid_price": mid,
        "bid_depth_json": bid_depth,
        "ask_depth_json": ask_depth,
    }


@pytest.fixture()
def cache_client():
    c = MagicMock(name="cache_client")
    c.get_json.return_value = None
    c.build_key.side_effect = lambda *parts: ":".join(parts)
    return c


@pytest.fixture()
def slippage_config():
    return SlippageConfig(
        slippage_estimation_enabled=True,
        default_slippage_depth_levels=20,
        max_slippage_depth_levels=50,
        reference_price_mode="best_bid_ask",
        insufficient_depth_policy="reject",
        allow_partial_fill_estimation=False,
    )


@pytest.fixture()
def depth_config():
    return DepthConfig(
        default_depth_levels=20,
        freshness_threshold_ms=1000,
        max_snapshot_age_ms=60000,
    )


def _make_service(cache_client, slippage_config, depth_config):
    return SlippageService(
        cache_client=cache_client,
        slippage_config=slippage_config,
        depth_config=depth_config,
    )


# ---------------------------------------------------------------------------
# Normal fill (buy)
# ---------------------------------------------------------------------------

class TestNormalFillBuy:
    def test_small_buy_fills_at_best_ask(self, cache_client, slippage_config, depth_config):
        """A small buy order that fits in the first ask level."""
        cache_client.get_json.return_value = _make_depth_data()
        svc = _make_service(cache_client, slippage_config, depth_config)
        result = svc.estimate("perp", "BTCUSDT", "buy", Decimal("1000"))
        assert result["reference_price"] == Decimal("50001.00")
        assert result["filled_qty"] > 0
        assert result["unfilled_qty"] == Decimal("0")
        assert result["slippage_bps"] >= 0
        assert result["meta"].get("error") is None

    def test_multi_level_buy(self, cache_client, slippage_config, depth_config):
        """A buy that spans multiple ask levels."""
        depth = _make_depth_data(ask_depth=[
            ["50001.00", "0.5"],   # 25000.50 quote
            ["50002.00", "0.5"],   # 25001.00 quote
            ["50003.00", "10.0"],  # 500030.00 quote
        ])
        cache_client.get_json.return_value = depth
        svc = _make_service(cache_client, slippage_config, depth_config)
        # Buy 50000 quote – should fill first level entirely, part of second
        result = svc.estimate("perp", "BTCUSDT", "buy", Decimal("50000"))
        assert result["filled_qty"] > 0
        assert result["unfilled_qty"] == Decimal("0")
        assert result["meta"]["filled_levels"] >= 2


# ---------------------------------------------------------------------------
# Normal fill (sell)
# ---------------------------------------------------------------------------

class TestNormalFillSell:
    def test_small_sell_fills_at_best_bid(self, cache_client, slippage_config, depth_config):
        cache_client.get_json.return_value = _make_depth_data()
        svc = _make_service(cache_client, slippage_config, depth_config)
        result = svc.estimate("perp", "BTCUSDT", "sell", Decimal("1000"))
        assert result["reference_price"] == Decimal("50000.00")
        assert result["filled_qty"] > 0
        assert result["unfilled_qty"] == Decimal("0")
        # For a sell, slippage = ref - avg_fill (positive when selling lower)
        assert result["slippage_bps"] >= 0


# ---------------------------------------------------------------------------
# Mid price reference mode
# ---------------------------------------------------------------------------

class TestMidPriceReference:
    def test_mid_price_reference(self, cache_client, slippage_config, depth_config):
        cfg = SlippageConfig(
            slippage_estimation_enabled=True,
            default_slippage_depth_levels=20,
            max_slippage_depth_levels=50,
            reference_price_mode="mid_price",
            insufficient_depth_policy="reject",
            allow_partial_fill_estimation=False,
        )
        cache_client.get_json.return_value = _make_depth_data()
        svc = _make_service(cache_client, cfg, depth_config)
        result = svc.estimate("perp", "BTCUSDT", "buy", Decimal("1000"))
        assert result["reference_price"] == Decimal("50000.50")


# ---------------------------------------------------------------------------
# Insufficient depth (reject policy)
# ---------------------------------------------------------------------------

class TestInsufficientDepthReject:
    def test_reject_policy_returns_error(self, cache_client, slippage_config, depth_config):
        depth = _make_depth_data(ask_depth=[
            ["50001.00", "0.01"],  # only 500.01 quote available
        ])
        cache_client.get_json.return_value = depth
        svc = _make_service(cache_client, slippage_config, depth_config)
        result = svc.estimate("perp", "BTCUSDT", "buy", Decimal("1000000"))
        assert result["meta"]["error"] == "insufficient_depth"
        assert result["filled_qty"] == Decimal("0")


# ---------------------------------------------------------------------------
# Insufficient depth (allow_partial_fill policy)
# ---------------------------------------------------------------------------

class TestInsufficientDepthPartialFill:
    def test_partial_fill_allowed(self, cache_client, slippage_config, depth_config):
        cfg = SlippageConfig(
            slippage_estimation_enabled=True,
            default_slippage_depth_levels=20,
            max_slippage_depth_levels=50,
            reference_price_mode="best_bid_ask",
            insufficient_depth_policy="allow_partial_fill",
            allow_partial_fill_estimation=True,
        )
        depth = _make_depth_data(ask_depth=[
            ["50001.00", "0.5"],  # 25000.5 quote
        ])
        cache_client.get_json.return_value = depth
        svc = _make_service(cache_client, cfg, depth_config)
        result = svc.estimate("perp", "BTCUSDT", "buy", Decimal("100000"))
        assert result["filled_qty"] > 0
        assert result["unfilled_qty"] > 0
        assert result["meta"].get("error") is None


# ---------------------------------------------------------------------------
# Stale depth
# ---------------------------------------------------------------------------

class TestStaleDepth:
    def test_stale_depth_returns_error(self, cache_client, slippage_config, depth_config):
        old_ts = _now_ms() - 120000  # 2 minutes ago, older than max_snapshot_age_ms=60000
        depth = _make_depth_data(event_ts_ms=old_ts)
        cache_client.get_json.return_value = depth
        svc = _make_service(cache_client, slippage_config, depth_config)
        result = svc.estimate("perp", "BTCUSDT", "buy", Decimal("1000"))
        assert result["meta"]["error"] == "stale_depth"


# ---------------------------------------------------------------------------
# No depth data
# ---------------------------------------------------------------------------

class TestNoDepthData:
    def test_missing_depth_in_cache(self, cache_client, slippage_config, depth_config):
        cache_client.get_json.return_value = None
        svc = _make_service(cache_client, slippage_config, depth_config)
        result = svc.estimate("perp", "BTCUSDT", "buy", Decimal("1000"))
        assert result["meta"]["error"] == "no_depth_data"


# ---------------------------------------------------------------------------
# Slippage estimation disabled
# ---------------------------------------------------------------------------

class TestSlippageDisabled:
    def test_disabled_returns_error(self, cache_client, slippage_config, depth_config):
        cfg = SlippageConfig(
            slippage_estimation_enabled=False,
            default_slippage_depth_levels=20,
            max_slippage_depth_levels=50,
            reference_price_mode="best_bid_ask",
            insufficient_depth_policy="reject",
            allow_partial_fill_estimation=False,
        )
        svc = _make_service(cache_client, cfg, depth_config)
        result = svc.estimate("perp", "BTCUSDT", "buy", Decimal("1000"))
        assert result["meta"]["error"] == "slippage_estimation_disabled"
        assert result["unfilled_qty"] == Decimal("1000")


# ---------------------------------------------------------------------------
# Empty order book
# ---------------------------------------------------------------------------

class TestEmptyBook:
    def test_empty_ask_for_buy(self, cache_client, slippage_config, depth_config):
        depth = _make_depth_data(ask_depth=[])
        cache_client.get_json.return_value = depth
        svc = _make_service(cache_client, slippage_config, depth_config)
        result = svc.estimate("perp", "BTCUSDT", "buy", Decimal("1000"))
        assert result["meta"]["error"] == "empty_book"

    def test_empty_bid_for_sell(self, cache_client, slippage_config, depth_config):
        depth = _make_depth_data(bid_depth=[])
        cache_client.get_json.return_value = depth
        svc = _make_service(cache_client, slippage_config, depth_config)
        result = svc.estimate("perp", "BTCUSDT", "sell", Decimal("1000"))
        assert result["meta"]["error"] == "empty_book"
