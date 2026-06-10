"""Tests for derivatives_validator."""

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from YM_data_collection.domain.models import (
    NormalizedFundingRate,
    NormalizedIndexPrice,
    NormalizedMarkPrice,
    NormalizedOpenInterest,
)
from YM_data_collection.validation.derivatives_validator import (
    ValidationResult,
    validate_funding_rate,
    validate_index_price,
    validate_mark_price,
    validate_open_interest,
)


def _make_funding_rate(**overrides) -> NormalizedFundingRate:
    defaults = dict(
        venue="binance",
        symbol="BTCUSDT",
        instrument_code="crypto.binance.perp.BTCUSDT",
        funding_time_ts_ms=1698768000000,
        funding_time_dt_utc=datetime.fromtimestamp(1698768000000 / 1000.0, tz=timezone.utc),
        funding_rate=Decimal("0.0001"),
        mark_price=Decimal("34567.80"),
        source="exchange",
    )
    defaults.update(overrides)
    return NormalizedFundingRate(**defaults)


def _make_oi(**overrides) -> NormalizedOpenInterest:
    defaults = dict(
        venue="binance",
        symbol="BTCUSDT",
        instrument_code="crypto.binance.perp.BTCUSDT",
        event_ts_ms=1698768000000,
        event_dt_utc=datetime.fromtimestamp(1698768000000 / 1000.0, tz=timezone.utc),
        open_interest=Decimal("12345.678"),
        open_interest_value=Decimal("427654321.12"),
        source="exchange",
    )
    defaults.update(overrides)
    return NormalizedOpenInterest(**defaults)


def _make_mark_price(**overrides) -> NormalizedMarkPrice:
    defaults = dict(
        venue="binance",
        symbol="BTCUSDT",
        instrument_code="crypto.binance.perp.BTCUSDT",
        event_ts_ms=1698768001000,
        event_dt_utc=datetime.fromtimestamp(1698768001000 / 1000.0, tz=timezone.utc),
        mark_price=Decimal("34567.80"),
        funding_rate=Decimal("0.0001"),
        next_funding_time_ts_ms=1698796800000,
        source="exchange",
    )
    defaults.update(overrides)
    return NormalizedMarkPrice(**defaults)


def _make_index_price(**overrides) -> NormalizedIndexPrice:
    defaults = dict(
        venue="binance",
        symbol="BTCUSDT",
        instrument_code="crypto.binance.perp.BTCUSDT",
        event_ts_ms=1698768001000,
        event_dt_utc=datetime.fromtimestamp(1698768001000 / 1000.0, tz=timezone.utc),
        index_price=Decimal("34565.50"),
        source="exchange",
    )
    defaults.update(overrides)
    return NormalizedIndexPrice(**defaults)


# ---------------------------------------------------------------------------
# Funding rate validation
# ---------------------------------------------------------------------------

class TestValidateFundingRate:
    """Tests for validate_funding_rate."""

    def test_valid_normal_funding_rate(self):
        fr = _make_funding_rate(funding_rate=Decimal("0.0001"))
        result = validate_funding_rate(fr)
        assert result.is_valid is True
        assert result.severity == "ok"
        assert result.issues == []

    def test_valid_negative_funding_rate(self):
        fr = _make_funding_rate(funding_rate=Decimal("-0.0001"))
        result = validate_funding_rate(fr)
        assert result.is_valid is True
        assert result.severity == "ok"

    def test_elevated_funding_rate_warning(self):
        fr = _make_funding_rate(funding_rate=Decimal("0.015"))
        result = validate_funding_rate(fr)
        assert result.is_valid is True  # warning doesn't make it invalid
        assert result.severity == "warning"
        assert any("elevated" in issue for issue in result.issues)

    def test_extreme_funding_rate_error(self):
        fr = _make_funding_rate(funding_rate=Decimal("0.06"))
        result = validate_funding_rate(fr)
        assert result.is_valid is False
        assert result.severity == "error"
        assert any("extreme" in issue for issue in result.issues)

    def test_negative_extreme_funding_rate_error(self):
        fr = _make_funding_rate(funding_rate=Decimal("-0.05"))
        result = validate_funding_rate(fr)
        assert result.is_valid is False
        assert result.severity == "error"

    def test_zero_timestamp_error(self):
        fr = _make_funding_rate(funding_time_ts_ms=0)
        result = validate_funding_rate(fr)
        assert result.is_valid is False
        assert result.severity == "error"
        assert any("funding_time_ts_ms" in issue for issue in result.issues)

    def test_negative_mark_price_error(self):
        fr = _make_funding_rate(mark_price=Decimal("-1.0"))
        result = validate_funding_rate(fr)
        assert result.is_valid is False
        assert any("mark_price" in issue for issue in result.issues)

    def test_none_mark_price_is_ok(self):
        fr = _make_funding_rate(mark_price=None)
        result = validate_funding_rate(fr)
        assert result.is_valid is True
        assert result.severity == "ok"

    def test_boundary_funding_rate_at_0_01(self):
        """Funding rate exactly at 0.01 should trigger warning."""
        fr = _make_funding_rate(funding_rate=Decimal("0.01"))
        result = validate_funding_rate(fr)
        assert result.severity == "warning"

    def test_boundary_funding_rate_at_0_05(self):
        """Funding rate exactly at 0.05 should trigger error."""
        fr = _make_funding_rate(funding_rate=Decimal("0.05"))
        result = validate_funding_rate(fr)
        assert result.is_valid is False
        assert result.severity == "error"


# ---------------------------------------------------------------------------
# Open interest validation
# ---------------------------------------------------------------------------

class TestValidateOpenInterest:
    """Tests for validate_open_interest."""

    def test_valid_oi(self):
        oi = _make_oi()
        result = validate_open_interest(oi)
        assert result.is_valid is True
        assert result.severity == "ok"
        assert result.issues == []

    def test_zero_oi_is_valid(self):
        oi = _make_oi(open_interest=Decimal("0"))
        result = validate_open_interest(oi)
        assert result.is_valid is True

    def test_negative_oi_error(self):
        oi = _make_oi(open_interest=Decimal("-1.0"))
        result = validate_open_interest(oi)
        assert result.is_valid is False
        assert result.severity == "error"
        assert any("open_interest" in issue for issue in result.issues)

    def test_zero_timestamp_error(self):
        oi = _make_oi(event_ts_ms=0)
        result = validate_open_interest(oi)
        assert result.is_valid is False
        assert any("event_ts_ms" in issue for issue in result.issues)

    def test_multiple_issues(self):
        oi = _make_oi(open_interest=Decimal("-1.0"), event_ts_ms=-1)
        result = validate_open_interest(oi)
        assert result.is_valid is False
        assert len(result.issues) == 2


# ---------------------------------------------------------------------------
# Mark price validation
# ---------------------------------------------------------------------------

class TestValidateMarkPrice:
    """Tests for validate_mark_price."""

    def test_valid_mark_price(self):
        mp = _make_mark_price()
        result = validate_mark_price(mp)
        assert result.is_valid is True
        assert result.severity == "ok"

    def test_zero_mark_price_error(self):
        mp = _make_mark_price(mark_price=Decimal("0"))
        result = validate_mark_price(mp)
        assert result.is_valid is False
        assert any("mark_price" in issue for issue in result.issues)

    def test_negative_mark_price_error(self):
        mp = _make_mark_price(mark_price=Decimal("-100.0"))
        result = validate_mark_price(mp)
        assert result.is_valid is False
        assert result.severity == "error"

    def test_zero_timestamp_error(self):
        mp = _make_mark_price(event_ts_ms=0)
        result = validate_mark_price(mp)
        assert result.is_valid is False
        assert any("event_ts_ms" in issue for issue in result.issues)


# ---------------------------------------------------------------------------
# Index price validation
# ---------------------------------------------------------------------------

class TestValidateIndexPrice:
    """Tests for validate_index_price."""

    def test_valid_index_price(self):
        ip = _make_index_price()
        result = validate_index_price(ip)
        assert result.is_valid is True
        assert result.severity == "ok"

    def test_zero_index_price_error(self):
        ip = _make_index_price(index_price=Decimal("0"))
        result = validate_index_price(ip)
        assert result.is_valid is False
        assert any("index_price" in issue for issue in result.issues)

    def test_negative_index_price_error(self):
        ip = _make_index_price(index_price=Decimal("-500.0"))
        result = validate_index_price(ip)
        assert result.is_valid is False
        assert result.severity == "error"

    def test_zero_timestamp_error(self):
        ip = _make_index_price(event_ts_ms=0)
        result = validate_index_price(ip)
        assert result.is_valid is False
        assert any("event_ts_ms" in issue for issue in result.issues)
