"""Tests for kline_normalizer module."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from YM_data_collection.domain.models import NormalizedKline
from YM_data_collection.normalization.kline_normalizer import (
    normalize_binance_kline,
    normalize_binance_klines_batch,
)


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

def _make_spot_raw(
    open_ts: int = 1499040000000,
    close_ts: int = 1499644799999,
) -> list:
    """Return a realistic Binance spot kline array."""
    return [
        open_ts,           # 0: Open time
        "0.01634000",      # 1: Open
        "0.80000000",      # 2: High
        "0.01575800",      # 3: Low
        "0.01577100",      # 4: Close
        "148976.11427815", # 5: Volume
        close_ts,          # 6: Close time
        "2434.19055334",   # 7: Quote asset volume
        308,               # 8: Number of trades
        "1756.87402397",   # 9: Taker buy base asset volume
        "28.46694368",     # 10: Taker buy quote asset volume
        "17928899.62484339",  # 11: Ignore
    ]


def _make_perp_raw(
    open_ts: int = 1700000000000,
    close_ts: int = 1700000059999,
) -> list:
    """Return a realistic Binance perp kline array (1m bar)."""
    return [
        open_ts,
        "37000.50",
        "37100.00",
        "36950.00",
        "37050.25",
        "1234.567",
        close_ts,
        "45678901.23",
        1500,
        "600.000",
        "22200000.00",
        "0",
    ]


# ---------------------------------------------------------------------------
# Tests: single normalization
# ---------------------------------------------------------------------------

class TestNormalizeBinanceKline:
    """Tests for normalize_binance_kline."""

    def test_spot_normalization_fields(self):
        raw = _make_spot_raw()
        kline = normalize_binance_kline(
            raw, venue="binance", symbol="ETHUSDT",
            market_type="spot", interval_code="1d",
        )
        assert isinstance(kline, NormalizedKline)
        assert kline.venue == "binance"
        assert kline.symbol == "ETHUSDT"
        assert kline.market_type == "spot"
        assert kline.interval_code == "1d"
        assert kline.instrument_code == "crypto.binance.spot.ETHUSDT"
        assert kline.source == "exchange"

    def test_spot_timestamps(self):
        raw = _make_spot_raw()
        kline = normalize_binance_kline(
            raw, venue="binance", symbol="ETHUSDT",
            market_type="spot", interval_code="1d",
        )
        assert kline.open_ts_ms == 1499040000000
        assert kline.close_ts_ms == 1499644799999
        assert kline.open_dt_utc == datetime.fromtimestamp(
            1499040000000 / 1000.0, tz=timezone.utc
        )
        assert kline.close_dt_utc == datetime.fromtimestamp(
            1499644799999 / 1000.0, tz=timezone.utc
        )

    def test_spot_prices_are_decimal(self):
        raw = _make_spot_raw()
        kline = normalize_binance_kline(
            raw, venue="binance", symbol="ETHUSDT",
            market_type="spot", interval_code="1d",
        )
        assert kline.open_price == Decimal("0.01634000")
        assert kline.high_price == Decimal("0.80000000")
        assert kline.low_price == Decimal("0.01575800")
        assert kline.close_price == Decimal("0.01577100")
        assert isinstance(kline.open_price, Decimal)

    def test_spot_volumes_are_decimal(self):
        raw = _make_spot_raw()
        kline = normalize_binance_kline(
            raw, venue="binance", symbol="ETHUSDT",
            market_type="spot", interval_code="1d",
        )
        assert kline.volume == Decimal("148976.11427815")
        assert kline.quote_volume == Decimal("2434.19055334")
        assert kline.taker_buy_base_volume == Decimal("1756.87402397")
        assert kline.taker_buy_quote_volume == Decimal("28.46694368")

    def test_spot_trade_count(self):
        raw = _make_spot_raw()
        kline = normalize_binance_kline(
            raw, venue="binance", symbol="ETHUSDT",
            market_type="spot", interval_code="1d",
        )
        assert kline.trade_count == 308

    def test_perp_normalization(self):
        raw = _make_perp_raw()
        kline = normalize_binance_kline(
            raw, venue="binance", symbol="BTCUSDT",
            market_type="perp", interval_code="1m",
        )
        assert kline.venue == "binance"
        assert kline.symbol == "BTCUSDT"
        assert kline.market_type == "perp"
        assert kline.instrument_code == "crypto.binance.perp.BTCUSDT"
        assert kline.open_price == Decimal("37000.50")
        assert kline.high_price == Decimal("37100.00")
        assert kline.low_price == Decimal("36950.00")
        assert kline.close_price == Decimal("37050.25")
        assert kline.volume == Decimal("1234.567")
        assert kline.trade_count == 1500
        assert kline.source == "exchange"

    def test_perp_timestamps_utc(self):
        raw = _make_perp_raw()
        kline = normalize_binance_kline(
            raw, venue="binance", symbol="BTCUSDT",
            market_type="perp", interval_code="1m",
        )
        assert kline.open_ts_ms == 1700000000000
        assert kline.close_ts_ms == 1700000059999
        assert kline.open_dt_utc.tzinfo is not None
        assert kline.close_dt_utc.tzinfo is not None


# ---------------------------------------------------------------------------
# Tests: batch normalization
# ---------------------------------------------------------------------------

class TestNormalizeBinanceKlinesBatch:
    """Tests for normalize_binance_klines_batch."""

    def test_batch_returns_list(self):
        raws = [_make_spot_raw(), _make_perp_raw()]
        result = normalize_binance_klines_batch(
            raws, venue="binance", symbol="ETHUSDT",
            market_type="spot", interval_code="1d",
        )
        assert len(result) == 2
        assert all(isinstance(k, NormalizedKline) for k in result)

    def test_batch_empty(self):
        result = normalize_binance_klines_batch(
            [], venue="binance", symbol="ETHUSDT",
            market_type="spot", interval_code="1d",
        )
        assert result == []

    def test_batch_preserves_order(self):
        raw1 = _make_spot_raw(open_ts=100000)
        raw2 = _make_spot_raw(open_ts=200000)
        result = normalize_binance_klines_batch(
            [raw1, raw2], venue="binance", symbol="ETHUSDT",
            market_type="spot", interval_code="1d",
        )
        assert result[0].open_ts_ms == 100000
        assert result[1].open_ts_ms == 200000
