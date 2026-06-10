"""Tests for export/resampler.py — DC-T040."""

from __future__ import annotations

import pytest

from YM_data_collection.export.resampler import (
    INTERVAL_MS,
    KlineResampler,
    ResampleConfig,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_1m_bar(ts_ms: int, **overrides) -> dict:
    """Create a minimal 1-minute kline bar for testing."""
    bar = {
        "open_ts_ms": ts_ms,
        "close_ts_ms": ts_ms + INTERVAL_MS["1m"] - 1,
        "open_price": 100.0,
        "high_price": 105.0,
        "low_price": 95.0,
        "close_price": 102.0,
        "volume": 10.0,
        "quote_volume": 1000.0,
        "trade_count": 50,
        "taker_buy_base_volume": 5.0,
        "taker_buy_quote_volume": 500.0,
    }
    bar.update(overrides)
    return bar


def _make_1h_bar(ts_ms: int, **overrides) -> dict:
    """Create a minimal 1-hour kline bar for testing."""
    bar = {
        "open_ts_ms": ts_ms,
        "close_ts_ms": ts_ms + INTERVAL_MS["1h"] - 1,
        "open_price": 200.0,
        "high_price": 210.0,
        "low_price": 190.0,
        "close_price": 205.0,
        "volume": 100.0,
        "quote_volume": 20000.0,
        "trade_count": 500,
        "taker_buy_base_volume": 50.0,
        "taker_buy_quote_volume": 10000.0,
    }
    bar.update(overrides)
    return bar


# ---------------------------------------------------------------------------
# Test: 1m -> 1h aggregation (60 bars -> 1 bar)
# ---------------------------------------------------------------------------

class TestResample1mTo1h:
    """Test aggregation of 60 one-minute bars into a single one-hour bar."""

    def test_produces_single_bar(self):
        base_ts = 0  # epoch
        bars = [_make_1m_bar(base_ts + i * 60_000) for i in range(60)]
        config = ResampleConfig(source_interval="1m", target_interval="1h")
        resampler = KlineResampler(config)
        result = resampler.resample(bars)
        assert len(result) == 1

    def test_ohlc_correctness(self):
        base_ts = 0
        # Build 60 bars with varying prices
        bars = []
        for i in range(60):
            bars.append(
                _make_1m_bar(
                    base_ts + i * 60_000,
                    open_price=100.0 + i,
                    high_price=110.0 + i,
                    low_price=90.0 - i,
                    close_price=100.0 + i + 0.5,
                )
            )
        config = ResampleConfig(source_interval="1m", target_interval="1h")
        resampler = KlineResampler(config)
        result = resampler.resample(bars)
        bar = result[0]

        # open = first bar's open
        assert bar["open_price"] == 100.0
        # close = last bar's close
        assert bar["close_price"] == 100.0 + 59 + 0.5
        # high = max of all highs
        assert bar["high_price"] == 110.0 + 59
        # low = min of all lows
        assert bar["low_price"] == 90.0 - 59

    def test_volume_and_trade_count_summing(self):
        base_ts = 0
        bars = [_make_1m_bar(base_ts + i * 60_000, volume=10.0, trade_count=5) for i in range(60)]
        config = ResampleConfig(source_interval="1m", target_interval="1h")
        resampler = KlineResampler(config)
        result = resampler.resample(bars)
        bar = result[0]

        assert bar["volume"] == pytest.approx(60 * 10.0)
        assert bar["trade_count"] == 60 * 5
        assert bar["quote_volume"] == pytest.approx(60 * 1000.0)
        assert bar["taker_buy_base_volume"] == pytest.approx(60 * 5.0)
        assert bar["taker_buy_quote_volume"] == pytest.approx(60 * 500.0)

    def test_timestamps(self):
        # Use base_ts = 0 (epoch) which is trivially aligned to all intervals
        base_ts = 0
        bars = [_make_1m_bar(base_ts + i * 60_000) for i in range(60)]
        config = ResampleConfig(source_interval="1m", target_interval="1h")
        resampler = KlineResampler(config)
        result = resampler.resample(bars)
        bar = result[0]

        assert bar["open_ts_ms"] == base_ts
        assert bar["close_ts_ms"] == base_ts + INTERVAL_MS["1h"] - 1


# ---------------------------------------------------------------------------
# Test: 1h -> 4h aggregation (4 bars -> 1 bar)
# ---------------------------------------------------------------------------

class TestResample1hTo4h:
    """Test aggregation of 4 one-hour bars into a single 4-hour bar."""

    def test_produces_single_bar(self):
        base_ts = 0
        bars = [_make_1h_bar(base_ts + i * 3_600_000) for i in range(4)]
        config = ResampleConfig(source_interval="1h", target_interval="4h")
        resampler = KlineResampler(config)
        result = resampler.resample(bars)
        assert len(result) == 1

    def test_ohlc_correctness(self):
        base_ts = 0
        bars = [
            _make_1h_bar(base_ts, open_price=100.0, high_price=120.0, low_price=90.0, close_price=110.0),
            _make_1h_bar(base_ts + 3_600_000, open_price=110.0, high_price=130.0, low_price=100.0, close_price=125.0),
            _make_1h_bar(base_ts + 7_200_000, open_price=125.0, high_price=140.0, low_price=115.0, close_price=135.0),
            _make_1h_bar(base_ts + 10_800_000, open_price=135.0, high_price=150.0, low_price=125.0, close_price=145.0),
        ]
        config = ResampleConfig(source_interval="1h", target_interval="4h")
        resampler = KlineResampler(config)
        result = resampler.resample(bars)
        bar = result[0]

        assert bar["open_price"] == 100.0  # first bar's open
        assert bar["close_price"] == 145.0  # last bar's close
        assert bar["high_price"] == 150.0  # max of all highs
        assert bar["low_price"] == 90.0  # min of all lows

    def test_volume_summing(self):
        base_ts = 0
        bars = [
            _make_1h_bar(base_ts, volume=50.0, trade_count=100),
            _make_1h_bar(base_ts + 3_600_000, volume=75.0, trade_count=150),
            _make_1h_bar(base_ts + 7_200_000, volume=60.0, trade_count=120),
            _make_1h_bar(base_ts + 10_800_000, volume=90.0, trade_count=180),
        ]
        config = ResampleConfig(source_interval="1h", target_interval="4h")
        resampler = KlineResampler(config)
        result = resampler.resample(bars)
        bar = result[0]

        assert bar["volume"] == pytest.approx(275.0)
        assert bar["trade_count"] == 550


# ---------------------------------------------------------------------------
# Test: offset shifts bucket boundaries
# ---------------------------------------------------------------------------

class TestResampleOffset:
    """Test that offset_minutes shifts bucket boundaries correctly."""

    def test_offset_30min_shifts_buckets(self):
        """1h -> 4h with offset_minutes=30: bucket starts at :30 marks."""
        # With offset=30m, bucket boundaries are shifted by 30 min.
        # For ts=0 (midnight): bucket = ((0 - 1_800_000) // 14_400_000) * 14_400_000 + 1_800_000
        #   = (-1) * 14_400_000 + 1_800_000 = -12_600_000
        # For ts=3_600_000 (01:00): bucket = ((3_600_000 - 1_800_000) // 14_400_000) * 14_400_000 + 1_800_000
        #   = (1_800_000 // 14_400_000) * 14_400_000 + 1_800_000 = 0 + 1_800_000 = 1_800_000
        # For ts=7_200_000 (02:00): bucket = ((7_200_000 - 1_800_000) // 14_400_000) * 14_400_000 + 1_800_000
        #   = (5_400_000 // 14_400_000) * 14_400_000 + 1_800_000 = 0 + 1_800_000 = 1_800_000
        # For ts=10_800_000 (03:00): same bucket = 1_800_000
        base_ts = 0  # midnight

        bars = [
            _make_1h_bar(base_ts),                # 00:00
            _make_1h_bar(base_ts + 3_600_000),    # 01:00
            _make_1h_bar(base_ts + 7_200_000),    # 02:00
            _make_1h_bar(base_ts + 10_800_000),   # 03:00
        ]

        config = ResampleConfig(
            source_interval="1h", target_interval="4h", offset_minutes=30
        )
        resampler = KlineResampler(config)
        result = resampler.resample(bars)

        # The 00:00 bar lands in a bucket starting at -12_600_000
        # The 01:00-03:00 bars land in a bucket starting at 1_800_000 (00:30)
        assert len(result) == 2

        bucket_starts = sorted(r["open_ts_ms"] for r in result)
        assert bucket_starts[0] == -12_600_000
        assert bucket_starts[1] == 1_800_000

    def test_offset_zero_no_shift(self):
        """offset_minutes=0 should behave identically to no offset."""
        base_ts = 0
        bars = [_make_1h_bar(base_ts + i * 3_600_000) for i in range(4)]
        config_no_offset = ResampleConfig(source_interval="1h", target_interval="4h", offset_minutes=0)
        config_default = ResampleConfig(source_interval="1h", target_interval="4h")

        result_no = KlineResampler(config_no_offset).resample(bars)
        result_def = KlineResampler(config_default).resample(bars)

        assert result_no == result_def


# ---------------------------------------------------------------------------
# Test: partial bucket
# ---------------------------------------------------------------------------

class TestPartialBucket:
    """Test that incomplete source data still produces a valid bar."""

    def test_partial_bucket_still_produces_bar(self):
        """Only 30 of 60 1m bars present -> still get 1 aggregated bar."""
        base_ts = 0
        bars = [_make_1m_bar(base_ts + i * 60_000) for i in range(30)]  # only 30 min
        config = ResampleConfig(source_interval="1m", target_interval="1h")
        resampler = KlineResampler(config)
        result = resampler.resample(bars)

        assert len(result) == 1
        bar = result[0]
        # open from first bar, close from last bar
        assert bar["open_price"] == 100.0
        assert bar["close_price"] == 102.0
        # volume summed over 30 bars only
        assert bar["volume"] == pytest.approx(30 * 10.0)

    def test_single_bar_bucket(self):
        """A bucket with just 1 source bar produces a valid aggregation."""
        base_ts = 0
        bars = [_make_1h_bar(base_ts, open_price=200.0, close_price=200.0)]
        config = ResampleConfig(source_interval="1h", target_interval="4h")
        resampler = KlineResampler(config)
        result = resampler.resample(bars)

        assert len(result) == 1
        bar = result[0]
        assert bar["open_price"] == bar["close_price"] == 200.0
        assert bar["high_price"] == 210.0
        assert bar["low_price"] == 190.0
        assert bar["volume"] == pytest.approx(100.0)


# ---------------------------------------------------------------------------
# Test: empty input
# ---------------------------------------------------------------------------

class TestEmptyInput:
    def test_empty_returns_empty(self):
        config = ResampleConfig(source_interval="1m", target_interval="1h")
        resampler = KlineResampler(config)
        assert resampler.resample([]) == []


# ---------------------------------------------------------------------------
# Test: validation errors
# ---------------------------------------------------------------------------

class TestValidation:
    def test_target_not_greater_than_source_raises(self):
        """target <= source should raise ValueError."""
        with pytest.raises(ValueError, match="must be greater than"):
            KlineResampler(ResampleConfig(source_interval="1h", target_interval="1h"))

        with pytest.raises(ValueError, match="must be greater than"):
            KlineResampler(ResampleConfig(source_interval="4h", target_interval="1h"))

    def test_non_divisible_intervals_raises(self):
        """Non-evenly-divisible target/source should raise ValueError."""
        # Standard intervals are all mutually divisible, so we temporarily
        # inject a non-standard source interval to trigger the check.
        import YM_data_collection.export.resampler as mod
        original = mod.INTERVAL_MS.copy()
        try:
            mod.INTERVAL_MS["7m"] = 420_000  # 7 min, not a divisor of 1h
            with pytest.raises(ValueError, match="evenly divisible"):
                KlineResampler(ResampleConfig(source_interval="7m", target_interval="1h"))
        finally:
            mod.INTERVAL_MS = original

    def test_unknown_source_interval_raises(self):
        with pytest.raises(ValueError, match="Unknown source interval"):
            KlineResampler(ResampleConfig(source_interval="2m", target_interval="1h"))

    def test_unknown_target_interval_raises(self):
        with pytest.raises(ValueError, match="Unknown target interval"):
            KlineResampler(ResampleConfig(source_interval="1m", target_interval="2h"))


# ---------------------------------------------------------------------------
# Test: multi-bucket resampling
# ---------------------------------------------------------------------------

class TestMultiBucket:
    """Test resampling across multiple target buckets."""

    def test_two_4h_buckets(self):
        """8 hours of 1h data should produce 2 four-hour bars."""
        base_ts = 0
        bars = [_make_1h_bar(base_ts + i * 3_600_000, open_price=100.0 + i) for i in range(8)]
        config = ResampleConfig(source_interval="1h", target_interval="4h")
        resampler = KlineResampler(config)
        result = resampler.resample(bars)

        assert len(result) == 2
        # First bucket: bars 0-3, open=100.0, close=103.0+0.5? No, close is default 205.0
        # Wait, we override open_price but not close_price
        # Let's just check the structure
        assert result[0]["open_ts_ms"] == base_ts
        assert result[1]["open_ts_ms"] == base_ts + 4 * 3_600_000
        # First bucket: open from bar 0
        assert result[0]["open_price"] == 100.0
        # Second bucket: open from bar 4
        assert result[1]["open_price"] == 104.0

    def test_unsorted_input(self):
        """Input klines in random order should still produce correct results."""
        base_ts = 0
        bars = [_make_1h_bar(base_ts + i * 3_600_000, open_price=float(i)) for i in range(4)]
        # Reverse the order
        bars.reverse()
        config = ResampleConfig(source_interval="1h", target_interval="4h")
        resampler = KlineResampler(config)
        result = resampler.resample(bars)

        assert len(result) == 1
        # Despite reversed input, open should still be from the earliest bar
        assert result[0]["open_price"] == 0.0
