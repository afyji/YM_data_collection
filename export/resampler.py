"""Kline resampling and offset processing for the export layer.

Aggregates source-interval klines into target-interval klines using
standard OHLCV rules, with optional offset-based bucket boundary shifting.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

# ---------------------------------------------------------------------------
# Interval mapping
# ---------------------------------------------------------------------------

INTERVAL_MS: dict[str, int] = {
    "1m": 60_000,
    "5m": 300_000,
    "15m": 900_000,
    "1h": 3_600_000,
    "4h": 14_400_000,
    "8h": 28_800_000,
    "1d": 86_400_000,
}

# Keys that should be summed during aggregation
_SUM_KEYS = (
    "volume",
    "quote_volume",
    "trade_count",
    "taker_buy_base_volume",
    "taker_buy_quote_volume",
)

# Keys carried through from the appropriate source bar
_OPEN_KEYS = ("open_price",)
_CLOSE_KEYS = ("close_price",)
_HIGH_KEYS = ("high_price",)
_LOW_KEYS = ("low_price",)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass
class ResampleConfig:
    """Configuration for kline resampling.

    Attributes:
        source_interval: Source kline interval code (e.g. '1m', '1h').
        target_interval: Target kline interval code (e.g. '1h', '4h').
        offset_minutes: Number of minutes to shift bucket boundaries.
            Positive values shift boundaries forward in time.
    """

    source_interval: str  # e.g. '1m', '1h'
    target_interval: str  # e.g. '1h', '4h'
    offset_minutes: int = 0  # shift bucket boundaries


# ---------------------------------------------------------------------------
# Resampler
# ---------------------------------------------------------------------------

class KlineResampler:
    """Aggregate source-interval klines into target-interval klines."""

    def __init__(self, config: ResampleConfig) -> None:
        self.config = config
        self._validate()
        self._source_ms = INTERVAL_MS[config.source_interval]
        self._target_ms = INTERVAL_MS[config.target_interval]

    # -- public API ---------------------------------------------------------

    def resample(self, klines: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Aggregate source-interval klines into target-interval klines.

        Aggregation rules:
        - open_price: first bar's open
        - high_price: max of all highs
        - low_price: min of all lows
        - close_price: last bar's close
        - volume: sum
        - quote_volume: sum
        - trade_count: sum
        - taker_buy_base_volume: sum
        - taker_buy_quote_volume: sum
        - open_ts_ms: bucket start timestamp
        - close_ts_ms: bucket end timestamp (open_ts_ms + target_interval_ms - 1)
        """
        if not klines:
            return []

        # Sort by timestamp to ensure correct ordering
        sorted_klines = sorted(klines, key=lambda k: k["open_ts_ms"])

        # Group source klines into target buckets
        buckets: dict[int, list[dict[str, Any]]] = {}
        for kline in sorted_klines:
            bucket_start = self._compute_bucket_start(kline["open_ts_ms"])
            buckets.setdefault(bucket_start, []).append(kline)

        # Aggregate each bucket
        results: list[dict[str, Any]] = []
        for bucket_start in sorted(buckets):
            bars = buckets[bucket_start]
            aggregated = self._aggregate_bucket(bars, bucket_start)
            results.append(aggregated)

        return results

    # -- internal helpers ---------------------------------------------------

    def _compute_bucket_start(self, ts_ms: int) -> int:
        """Compute which target bucket a given timestamp belongs to.

        Applies offset_minutes shift so that bucket boundaries are aligned
        to (epoch + offset) boundaries rather than strict epoch boundaries.
        """
        offset_ms = self.config.offset_minutes * 60_000
        return ((ts_ms - offset_ms) // self._target_ms) * self._target_ms + offset_ms

    def _aggregate_bucket(
        self, bars: list[dict[str, Any]], bucket_start: int
    ) -> dict[str, Any]:
        """Aggregate a list of source bars belonging to one target bucket."""
        result: dict[str, Any] = {
            "open_ts_ms": bucket_start,
            "close_ts_ms": bucket_start + self._target_ms - 1,
        }

        # Open = first bar's open
        for key in _OPEN_KEYS:
            result[key] = bars[0][key]

        # Close = last bar's close
        for key in _CLOSE_KEYS:
            result[key] = bars[-1][key]

        # High = max of all highs
        for key in _HIGH_KEYS:
            result[key] = max(bar[key] for bar in bars)

        # Low = min of all lows
        for key in _LOW_KEYS:
            result[key] = min(bar[key] for bar in bars)

        # Sum keys
        for key in _SUM_KEYS:
            result[key] = sum(bar[key] for bar in bars)

        return result

    def _validate(self) -> None:
        """Validate the resample configuration."""
        src = self.config.source_interval
        tgt = self.config.target_interval

        if src not in INTERVAL_MS:
            raise ValueError(
                f"Unknown source interval '{src}'. "
                f"Valid intervals: {sorted(INTERVAL_MS)}"
            )
        if tgt not in INTERVAL_MS:
            raise ValueError(
                f"Unknown target interval '{tgt}'. "
                f"Valid intervals: {sorted(INTERVAL_MS)}"
            )

        src_ms = INTERVAL_MS[src]
        tgt_ms = INTERVAL_MS[tgt]

        if tgt_ms <= src_ms:
            raise ValueError(
                f"Target interval '{tgt}' ({tgt_ms}ms) must be greater than "
                f"source interval '{src}' ({src_ms}ms)"
            )

        if tgt_ms % src_ms != 0:
            raise ValueError(
                f"Target interval '{tgt}' ({tgt_ms}ms) must be evenly divisible by "
                f"source interval '{src}' ({src_ms}ms)"
            )
