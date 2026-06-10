"""Tests for kline_validator module."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from YM_data_collection.domain.models import NormalizedKline
from YM_data_collection.validation.kline_validator import (
    INTERVAL_DURATIONS_MS,
    ValidationResult,
    validate_kline,
    validate_klines_batch,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_valid_kline(
    interval_code: str = "1h",
    open_ts_ms: int | None = None,
    close_ts_ms: int | None = None,
    open_price: str = "100.00",
    high_price: str = "110.00",
    low_price: str = "90.00",
    close_price: str = "105.00",
    volume: str = "500.00",
    quote_volume: str = "50000.00",
) -> NormalizedKline:
    """Build a valid NormalizedKline for the given interval.

    If open_ts_ms is not provided, picks an aligned value.
    If close_ts_ms is not provided, uses the Binance convention
    close_ts = open_ts + interval_duration - 1.
    """
    duration = INTERVAL_DURATIONS_MS[interval_code]
    if open_ts_ms is None:
        # Pick a nice aligned timestamp (2024-01-01 00:00 UTC for 1h)
        open_ts_ms = 1704067200000  # 2024-01-01 00:00:00 UTC
        # Align to interval
        open_ts_ms = open_ts_ms - (open_ts_ms % duration)
    if close_ts_ms is None:
        close_ts_ms = open_ts_ms + duration - 1

    return NormalizedKline(
        venue="binance",
        symbol="BTCUSDT",
        instrument_code="crypto.binance.spot.BTCUSDT",
        interval_code=interval_code,
        open_ts_ms=open_ts_ms,
        close_ts_ms=close_ts_ms,
        open_dt_utc=datetime.fromtimestamp(open_ts_ms / 1000.0, tz=timezone.utc),
        close_dt_utc=datetime.fromtimestamp(close_ts_ms / 1000.0, tz=timezone.utc),
        open_price=Decimal(open_price),
        high_price=Decimal(high_price),
        low_price=Decimal(low_price),
        close_price=Decimal(close_price),
        volume=Decimal(volume),
        quote_volume=Decimal(quote_volume),
        trade_count=500,
        taker_buy_base_volume=Decimal("250.00"),
        taker_buy_quote_volume=Decimal("25000.00"),
        source="exchange",
        market_type="spot",
    )


# ---------------------------------------------------------------------------
# Tests: valid kline
# ---------------------------------------------------------------------------

class TestValidateValidKline:
    """A properly constructed kline should pass validation."""

    def test_valid_1h_kline(self):
        kline = _make_valid_kline(interval_code="1h")
        result = validate_kline(kline, "1h")
        assert result.is_valid is True
        assert result.issues == []
        assert result.is_repairable is False
        assert result.repaired_kline is None

    def test_valid_1m_kline(self):
        kline = _make_valid_kline(interval_code="1m")
        result = validate_kline(kline, "1m")
        assert result.is_valid is True

    def test_valid_5m_kline(self):
        kline = _make_valid_kline(interval_code="5m")
        result = validate_kline(kline, "5m")
        assert result.is_valid is True

    def test_valid_1d_kline(self):
        kline = _make_valid_kline(interval_code="1d")
        result = validate_kline(kline, "1d")
        assert result.is_valid is True


# ---------------------------------------------------------------------------
# Tests: open_ts_ms < close_ts_ms
# ---------------------------------------------------------------------------

class TestTimestampOrder:
    """open_ts_ms must be strictly less than close_ts_ms."""

    def test_open_eq_close(self):
        kline = _make_valid_kline(interval_code="1h", open_ts_ms=1704067200000, close_ts_ms=1704067200000)
        result = validate_kline(kline, "1h")
        assert result.is_valid is False
        assert any("open_ts_ms" in i and "close_ts_ms" in i for i in result.issues)

    def test_open_gt_close(self):
        kline = _make_valid_kline(interval_code="1h", open_ts_ms=1704067200000, close_ts_ms=1704067200000 - 1)
        result = validate_kline(kline, "1h")
        assert result.is_valid is False
        assert any("open_ts_ms" in i for i in result.issues)


# ---------------------------------------------------------------------------
# Tests: close_ts_ms interval duration mismatch
# ---------------------------------------------------------------------------

class TestCloseTsMismatch:
    """close_ts_ms should equal open_ts_ms + interval_duration - 1."""

    def test_close_ts_off_by_large_amount(self):
        # Use a close_ts that is 10 seconds off
        kline = _make_valid_kline(interval_code="1h", close_ts_ms=1704067200000 + 3600000 - 1 + 10000)
        result = validate_kline(kline, "1h")
        assert result.is_valid is False
        assert any("close_ts_ms" in i for i in result.issues)

    def test_close_ts_off_by_small_amount_repairable(self):
        # Off by 5 ms — still within one interval, so repairable
        kline = _make_valid_kline(interval_code="1h", close_ts_ms=1704067200000 + 3600000 - 1 + 5)
        result = validate_kline(kline, "1h")
        assert result.is_valid is False
        assert result.is_repairable is True
        assert result.repaired_kline is not None
        expected_close = 1704067200000 + 3600000 - 1
        assert result.repaired_kline.close_ts_ms == expected_close


# ---------------------------------------------------------------------------
# Tests: OHLC > 0
# ---------------------------------------------------------------------------

class TestOHLCPositive:
    """All OHLC values must be > 0."""

    @pytest.mark.parametrize("field,price", [
        ("open_price", "0"),
        ("high_price", "0"),
        ("low_price", "0"),
        ("close_price", "0"),
        ("open_price", "-5.00"),
    ])
    def test_non_positive_price(self, field, price):
        overrides = {field: Decimal(price)}
        base = _make_valid_kline(interval_code="1h")
        kline = base.model_copy(update=overrides)
        result = validate_kline(kline, "1h")
        assert result.is_valid is False
        assert any(field in i for i in result.issues)


# ---------------------------------------------------------------------------
# Tests: OHLC consistency
# ---------------------------------------------------------------------------

class TestOHLCConsistency:
    """high >= max(open, close) and low <= min(open, close)."""

    def test_high_less_than_open(self):
        # open=100, close=105, but high=99
        kline = _make_valid_kline(
            interval_code="1h",
            open_price="100.00",
            high_price="99.00",
            low_price="90.00",
            close_price="105.00",
        )
        result = validate_kline(kline, "1h")
        assert result.is_valid is False
        assert any("high_price" in i for i in result.issues)

    def test_high_less_than_close(self):
        # open=95, close=110, but high=105
        kline = _make_valid_kline(
            interval_code="1h",
            open_price="95.00",
            high_price="105.00",
            low_price="90.00",
            close_price="110.00",
        )
        result = validate_kline(kline, "1h")
        assert result.is_valid is False
        assert any("high_price" in i for i in result.issues)

    def test_low_greater_than_open(self):
        # open=100, close=95, but low=101
        kline = _make_valid_kline(
            interval_code="1h",
            open_price="100.00",
            high_price="110.00",
            low_price="101.00",
            close_price="95.00",
        )
        result = validate_kline(kline, "1h")
        assert result.is_valid is False
        assert any("low_price" in i for i in result.issues)

    def test_low_greater_than_close(self):
        # open=90, close=100, but low=101
        kline = _make_valid_kline(
            interval_code="1h",
            open_price="90.00",
            high_price="110.00",
            low_price="101.00",
            close_price="100.00",
        )
        result = validate_kline(kline, "1h")
        assert result.is_valid is False
        assert any("low_price" in i for i in result.issues)


# ---------------------------------------------------------------------------
# Tests: negative volume
# ---------------------------------------------------------------------------

class TestNegativeVolume:
    """volume and quote_volume must be >= 0."""

    def test_negative_volume(self):
        kline = _make_valid_kline(interval_code="1h", volume="-10.00")
        result = validate_kline(kline, "1h")
        assert result.is_valid is False
        assert any("volume" in i and "negative" in i for i in result.issues)

    def test_negative_quote_volume(self):
        kline = _make_valid_kline(interval_code="1h", quote_volume="-5.00")
        result = validate_kline(kline, "1h")
        assert result.is_valid is False
        assert any("quote_volume" in i and "negative" in i for i in result.issues)

    def test_zero_volume_is_valid(self):
        kline = _make_valid_kline(interval_code="1h", volume="0", quote_volume="0")
        result = validate_kline(kline, "1h")
        assert result.is_valid is True


# ---------------------------------------------------------------------------
# Tests: boundary anomaly detection
# ---------------------------------------------------------------------------

class TestBoundaryAnomaly:
    """open_ts_ms should align to the interval boundary."""

    def test_misaligned_1h_boundary(self):
        # 1h interval — open_ts_ms should be divisible by 3600000.
        # 1704070620000 = 2024-01-01 01:57:00 UTC (not aligned to hour)
        misaligned_ts = 1704070620000
        assert misaligned_ts % 3600000 != 0, "sanity: ts should be misaligned"
        kline = _make_valid_kline(
            interval_code="1h",
            open_ts_ms=misaligned_ts,
            close_ts_ms=misaligned_ts + 3600000 - 1,
        )
        result = validate_kline(kline, "1h")
        assert result.is_valid is False
        assert any("not aligned" in i for i in result.issues)

    def test_misaligned_1h_boundary_14_59(self):
        # Simulate the 14:59 boundary anomaly described in the design
        # 2024-01-01 14:59:00 UTC in ms
        ts_14_59 = 1704121140000
        assert ts_14_59 % 3600000 != 0
        kline = _make_valid_kline(
            interval_code="1h",
            open_ts_ms=ts_14_59,
            close_ts_ms=ts_14_59 + 3600000 - 1,
        )
        result = validate_kline(kline, "1h")
        assert result.is_valid is False
        assert any("not aligned" in i for i in result.issues)

    def test_aligned_1h_boundary_is_ok(self):
        # 2024-01-01 02:00:00 UTC — exactly on 1h boundary
        aligned_ts = 1704074400000
        assert aligned_ts % 3600000 == 0
        kline = _make_valid_kline(
            interval_code="1h",
            open_ts_ms=aligned_ts,
            close_ts_ms=aligned_ts + 3600000 - 1,
        )
        result = validate_kline(kline, "1h")
        assert result.is_valid is True

    def test_misaligned_15m_boundary(self):
        # 15m interval — open_ts_ms should be divisible by 900000.
        misaligned_ts = 1704067500000  # not divisible by 900000
        assert misaligned_ts % 900000 != 0
        kline = _make_valid_kline(
            interval_code="15m",
            open_ts_ms=misaligned_ts,
            close_ts_ms=misaligned_ts + 900000 - 1,
        )
        result = validate_kline(kline, "15m")
        assert result.is_valid is False
        assert any("not aligned" in i for i in result.issues)


# ---------------------------------------------------------------------------
# Tests: repair logic
# ---------------------------------------------------------------------------

class TestRepairLogic:
    """Repair should fix close_ts_ms when it is off by a small amount."""

    def test_repair_close_ts(self):
        open_ts = 1704067200000  # aligned 1h
        expected_close = open_ts + 3600000 - 1
        # Simulate a close_ts that is 3ms off
        actual_close = expected_close + 3
        kline = _make_valid_kline(
            interval_code="1h",
            open_ts_ms=open_ts,
            close_ts_ms=actual_close,
        )
        result = validate_kline(kline, "1h")
        assert result.is_valid is False
        assert result.is_repairable is True
        assert result.repaired_kline is not None
        assert result.repaired_kline.close_ts_ms == expected_close

    def test_no_repair_when_close_ts_correct(self):
        kline = _make_valid_kline(interval_code="1h")
        result = validate_kline(kline, "1h")
        assert result.is_valid is True
        assert result.is_repairable is False
        assert result.repaired_kline is None

    def test_repaired_close_dt_utc(self):
        open_ts = 1704067200000
        expected_close = open_ts + 3600000 - 1
        kline = _make_valid_kline(
            interval_code="1h",
            open_ts_ms=open_ts,
            close_ts_ms=expected_close + 3,
        )
        result = validate_kline(kline, "1h")
        assert result.repaired_kline is not None
        expected_dt = datetime.fromtimestamp(expected_close / 1000.0, tz=timezone.utc)
        assert result.repaired_kline.close_dt_utc == expected_dt

    def test_not_repairable_when_close_ts_off_by_full_interval(self):
        open_ts = 1704067200000
        # Close ts is off by a full interval — not repairable
        actual_close = open_ts + 2 * 3600000 - 1  # two intervals later
        kline = _make_valid_kline(
            interval_code="1h",
            open_ts_ms=open_ts,
            close_ts_ms=actual_close,
        )
        result = validate_kline(kline, "1h")
        assert result.is_valid is False
        assert result.is_repairable is False


# ---------------------------------------------------------------------------
# Tests: unknown interval code
# ---------------------------------------------------------------------------

class TestUnknownInterval:
    def test_unknown_interval_code(self):
        kline = _make_valid_kline(interval_code="1h")
        result = validate_kline(kline, "2w")
        assert result.is_valid is False
        assert any("unknown interval_code" in i for i in result.issues)


# ---------------------------------------------------------------------------
# Tests: batch validation
# ---------------------------------------------------------------------------

class TestValidateKlinesBatch:
    def test_batch_all_valid(self):
        klines = [
            _make_valid_kline(interval_code="1h"),
            _make_valid_kline(interval_code="1h"),
        ]
        results = validate_klines_batch(klines, "1h")
        assert len(results) == 2
        assert all(r.is_valid for r in results)

    def test_batch_mixed(self):
        valid = _make_valid_kline(interval_code="1h")
        invalid = _make_valid_kline(
            interval_code="1h",
            high_price="50.00",  # below open/close
            open_price="100.00",
            close_price="105.00",
        )
        results = validate_klines_batch([valid, invalid], "1h")
        assert results[0].is_valid is True
        assert results[1].is_valid is False

    def test_batch_empty(self):
        results = validate_klines_batch([], "1h")
        assert results == []
