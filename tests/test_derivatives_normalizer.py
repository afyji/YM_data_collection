"""Tests for derivatives_normalizer."""

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from YM_data_collection.domain.models import (
    NormalizedFundingRate,
    NormalizedIndexPrice,
    NormalizedMarkPrice,
    NormalizedOpenInterest,
)
from YM_data_collection.normalization.derivatives_normalizer import (
    normalize_funding_rate,
    normalize_funding_rates_batch,
    normalize_mark_price_kline,
    normalize_mark_price_klines_batch,
    normalize_open_interest_hist,
    normalize_open_interest_hist_batch,
    normalize_premium_index,
)


# ---------------------------------------------------------------------------
# Funding rate
# ---------------------------------------------------------------------------

class TestNormalizeFundingRate:
    """Tests for normalize_funding_rate and batch variant."""

    def test_basic_funding_rate_normalization(self):
        raw = {
            "symbol": "BTCUSDT",
            "fundingTime": 1698768000000,
            "fundingRate": "0.00010000",
            "markPrice": "34567.80000000",
        }
        result = normalize_funding_rate(raw)

        assert isinstance(result, NormalizedFundingRate)
        assert result.venue == "binance"
        assert result.symbol == "BTCUSDT"
        assert result.instrument_code == "crypto.binance.perp.BTCUSDT"
        assert result.funding_time_ts_ms == 1698768000000
        assert result.funding_time_dt_utc == datetime.fromtimestamp(
            1698768000000 / 1000.0, tz=timezone.utc
        )
        assert result.funding_rate == Decimal("0.00010000")
        assert result.mark_price == Decimal("34567.80000000")
        assert result.source == "exchange"

    def test_funding_rate_without_mark_price(self):
        raw = {
            "symbol": "ETHUSDT",
            "fundingTime": 1698768000000,
            "fundingRate": "-0.00005000",
        }
        result = normalize_funding_rate(raw)

        assert result.mark_price is None
        assert result.funding_rate == Decimal("-0.00005000")

    def test_funding_rate_custom_venue(self):
        raw = {
            "symbol": "BTCUSDT",
            "fundingTime": 1698768000000,
            "fundingRate": "0.00010000",
        }
        result = normalize_funding_rate(raw, venue="bybit")
        assert result.venue == "bybit"
        assert result.instrument_code == "crypto.bybit.perp.BTCUSDT"

    def test_funding_rates_batch(self):
        raws = [
            {"symbol": "BTCUSDT", "fundingTime": 1698768000000, "fundingRate": "0.00010000"},
            {"symbol": "ETHUSDT", "fundingTime": 1698768000000, "fundingRate": "0.00020000"},
        ]
        results = normalize_funding_rates_batch(raws)

        assert len(results) == 2
        assert all(isinstance(r, NormalizedFundingRate) for r in results)
        assert results[0].symbol == "BTCUSDT"
        assert results[1].symbol == "ETHUSDT"

    def test_funding_rates_batch_empty(self):
        results = normalize_funding_rates_batch([])
        assert results == []


# ---------------------------------------------------------------------------
# Open interest
# ---------------------------------------------------------------------------

class TestNormalizeOpenInterest:
    """Tests for normalize_open_interest_hist and batch variant."""

    def test_basic_oi_normalization(self):
        raw = {
            "symbol": "BTCUSDT",
            "sumOpenInterest": "12345.678",
            "sumOpenInterestValue": "427654321.12",
            "timestamp": 1698768000000,
        }
        result = normalize_open_interest_hist(raw)

        assert isinstance(result, NormalizedOpenInterest)
        assert result.venue == "binance"
        assert result.symbol == "BTCUSDT"
        assert result.instrument_code == "crypto.binance.perp.BTCUSDT"
        assert result.event_ts_ms == 1698768000000
        assert result.event_dt_utc == datetime.fromtimestamp(
            1698768000000 / 1000.0, tz=timezone.utc
        )
        assert result.open_interest == Decimal("12345.678")
        assert result.open_interest_value == Decimal("427654321.12")
        assert result.source == "exchange"

    def test_oi_without_value(self):
        raw = {
            "symbol": "ETHUSDT",
            "sumOpenInterest": "50000.000",
            "timestamp": 1698768000000,
        }
        result = normalize_open_interest_hist(raw)
        assert result.open_interest_value is None

    def test_oi_batch(self):
        raws = [
            {"symbol": "BTCUSDT", "sumOpenInterest": "100.0", "timestamp": 1698768000000},
            {"symbol": "ETHUSDT", "sumOpenInterest": "200.0", "timestamp": 1698770000000},
            {"symbol": "SOLUSDT", "sumOpenInterest": "300.0", "timestamp": 1698772000000},
        ]
        results = normalize_open_interest_hist_batch(raws)

        assert len(results) == 3
        assert results[0].symbol == "BTCUSDT"
        assert results[2].open_interest == Decimal("300.0")


# ---------------------------------------------------------------------------
# Premium index (splits into mark + index)
# ---------------------------------------------------------------------------

class TestNormalizePremiumIndex:
    """Tests for normalize_premium_index."""

    def test_premium_index_splits_into_mark_and_index(self):
        raw = {
            "symbol": "BTCUSDT",
            "markPrice": "34567.80",
            "indexPrice": "34565.50",
            "lastFundingRate": "0.00010000",
            "nextFundingTime": 1698796800000,
            "time": 1698768001000,
        }
        mark, index = normalize_premium_index(raw)

        # Mark price checks
        assert isinstance(mark, NormalizedMarkPrice)
        assert mark.venue == "binance"
        assert mark.symbol == "BTCUSDT"
        assert mark.instrument_code == "crypto.binance.perp.BTCUSDT"
        assert mark.mark_price == Decimal("34567.80")
        assert mark.funding_rate == Decimal("0.00010000")
        assert mark.next_funding_time_ts_ms == 1698796800000
        assert mark.event_ts_ms == 1698768001000
        assert mark.source == "exchange"

        # Index price checks
        assert isinstance(index, NormalizedIndexPrice)
        assert index.venue == "binance"
        assert index.symbol == "BTCUSDT"
        assert index.instrument_code == "crypto.binance.perp.BTCUSDT"
        assert index.index_price == Decimal("34565.50")
        assert index.event_ts_ms == 1698768001000
        assert index.source == "exchange"

    def test_premium_index_without_next_funding_time(self):
        raw = {
            "symbol": "ETHUSDT",
            "markPrice": "2000.00",
            "indexPrice": "1999.50",
            "lastFundingRate": "0.00005000",
            "nextFundingTime": 0,
            "time": 1698768001000,
        }
        mark, index = normalize_premium_index(raw)

        # nextFundingTime=0 should be treated as falsy
        assert mark.next_funding_time_ts_ms is None

    def test_premium_index_custom_venue(self):
        raw = {
            "symbol": "BTCUSDT",
            "markPrice": "34567.80",
            "indexPrice": "34565.50",
            "lastFundingRate": "0.00010000",
            "nextFundingTime": 1698796800000,
            "time": 1698768001000,
        }
        mark, index = normalize_premium_index(raw, venue="okx")
        assert mark.venue == "okx"
        assert index.venue == "okx"
        assert mark.instrument_code == "crypto.okx.perp.BTCUSDT"


# ---------------------------------------------------------------------------
# Mark price klines
# ---------------------------------------------------------------------------

class TestNormalizeMarkPriceKline:
    """Tests for normalize_mark_price_kline and batch variant."""

    def test_single_mark_price_kline(self):
        raw = [
            1698764400000,
            "34500.00",
            "34600.00",
            "34400.00",
            "34567.80",
            "0",
            1698767999999,
            "0",
            0,
            "0",
            "0",
            "0",
        ]
        result = normalize_mark_price_kline(raw, symbol="BTCUSDT")

        assert isinstance(result, NormalizedMarkPrice)
        assert result.venue == "binance"
        assert result.symbol == "BTCUSDT"
        assert result.instrument_code == "crypto.binance.perp.BTCUSDT"
        assert result.event_ts_ms == 1698764400000
        assert result.event_dt_utc == datetime.fromtimestamp(
            1698764400000 / 1000.0, tz=timezone.utc
        )
        assert result.mark_price == Decimal("34567.80")
        assert result.funding_rate is None
        assert result.next_funding_time_ts_ms is None
        assert result.source == "exchange"

    def test_mark_price_klines_batch(self):
        raws = [
            [1698764400000, "34500.00", "34600.00", "34400.00", "34567.80",
             "0", 1698767999999, "0", 0, "0", "0", "0"],
            [1698768000000, "34567.80", "34700.00", "34500.00", "34650.00",
             "0", 1698771599999, "0", 0, "0", "0", "0"],
        ]
        results = normalize_mark_price_klines_batch(raws, symbol="BTCUSDT")

        assert len(results) == 2
        assert results[0].mark_price == Decimal("34567.80")
        assert results[1].mark_price == Decimal("34650.00")
        assert results[0].event_ts_ms == 1698764400000
        assert results[1].event_ts_ms == 1698768000000

    def test_mark_price_kline_custom_venue(self):
        raw = [
            1698764400000, "34500.00", "34600.00", "34400.00", "34567.80",
            "0", 1698767999999, "0", 0, "0", "0", "0",
        ]
        result = normalize_mark_price_kline(raw, venue="bybit", symbol="BTCUSDT")
        assert result.venue == "bybit"
        assert result.instrument_code == "crypto.bybit.perp.BTCUSDT"
