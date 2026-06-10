"""Kline validation and repair logic."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import List, Optional

from YM_data_collection.domain.models import NormalizedKline

# ---------------------------------------------------------------------------
# Interval duration lookup (milliseconds)
# ---------------------------------------------------------------------------
INTERVAL_DURATIONS_MS: dict[str, int] = {
    "1m": 60_000,
    "5m": 300_000,
    "15m": 900_000,
    "1h": 3_600_000,
    "4h": 14_400_000,
    "12h": 43_200_000,
    "1d": 86_400_000,
}

# Binance sets close_ts_ms = open_ts_ms + interval_ms - 1 (end-of-bar minus 1ms).
CLOSE_TS_TOLERANCE_MS: int = 2  # allow ±2 ms leeway beyond expected offset

# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------

@dataclass
class ValidationResult:
    """Outcome of validating a single NormalizedKline."""

    is_valid: bool = True
    issues: List[str] = field(default_factory=list)
    is_repairable: bool = False
    repaired_kline: Optional[NormalizedKline] = None


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------

def _expected_close_ts_ms(open_ts_ms: int, interval_code: str) -> int:
    """Return the expected close_ts_ms given the open time and interval.

    Binance convention: close_ts = open_ts + interval_duration - 1
    """
    duration = INTERVAL_DURATIONS_MS[interval_code]
    return open_ts_ms + duration - 1


def _check_boundary_alignment(open_ts_ms: int, interval_code: str) -> Optional[str]:
    """Return an issue string if open_ts_ms does not align to its interval boundary."""
    duration = INTERVAL_DURATIONS_MS[interval_code]
    if open_ts_ms % duration != 0:
        return (
            f"open_ts_ms {open_ts_ms} is not aligned to {interval_code} boundary "
            f"(remainder {open_ts_ms % duration} ms)"
        )
    return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def validate_kline(kline: NormalizedKline, interval_code: str) -> ValidationResult:
    """Validate a single NormalizedKline and attempt repair when possible.

    Checks performed
    ----------------
    1. open_ts_ms < close_ts_ms
    2. close_ts_ms matches expected interval duration (tolerance applied)
    3. OHLC all > 0
    4. high >= max(open, close) and low <= min(open, close)
    5. volume >= 0, quote_volume >= 0
    6. Boundary anomaly: open_ts_ms should align to interval boundary

    Repair logic
    ------------
    If close_ts_ms is off by a small amount (within tolerance), recalculate
    it to the expected value based on open_ts_ms and interval_code.
    """
    result = ValidationResult()
    needs_repair = False
    repaired = kline.model_copy()

    # 1. open_ts_ms < close_ts_ms
    if kline.open_ts_ms >= kline.close_ts_ms:
        result.issues.append(
            f"open_ts_ms ({kline.open_ts_ms}) >= close_ts_ms ({kline.close_ts_ms})"
        )

    # 2. Interval duration check
    if interval_code not in INTERVAL_DURATIONS_MS:
        result.issues.append(f"unknown interval_code: {interval_code}")
    else:
        expected_close = _expected_close_ts_ms(kline.open_ts_ms, interval_code)
        diff = abs(kline.close_ts_ms - expected_close)
        if diff > CLOSE_TS_TOLERANCE_MS:
            result.issues.append(
                f"close_ts_ms {kline.close_ts_ms} differs from expected "
                f"{expected_close} by {diff} ms (tolerance {CLOSE_TS_TOLERANCE_MS} ms)"
            )
            # Repairable if the difference is modest (within 1 full interval)
            if diff < INTERVAL_DURATIONS_MS[interval_code]:
                needs_repair = True
                repaired = repaired.model_copy(
                    update={
                        "close_ts_ms": expected_close,
                        "close_dt_utc": datetime.fromtimestamp(
                            expected_close / 1000.0, tz=timezone.utc
                        ),
                    }
                )

    # 3. OHLC > 0
    for label, val in [
        ("open_price", kline.open_price),
        ("high_price", kline.high_price),
        ("low_price", kline.low_price),
        ("close_price", kline.close_price),
    ]:
        if val <= 0:
            result.issues.append(f"{label} ({val}) is not positive")

    # 4. high >= max(open, close) and low <= min(open, close)
    max_oc = max(kline.open_price, kline.close_price)
    min_oc = min(kline.open_price, kline.close_price)
    if kline.high_price < max_oc:
        result.issues.append(
            f"high_price ({kline.high_price}) < max(open, close) ({max_oc})"
        )
    if kline.low_price > min_oc:
        result.issues.append(
            f"low_price ({kline.low_price}) > min(open, close) ({min_oc})"
        )

    # 5. volume >= 0, quote_volume >= 0
    if kline.volume < 0:
        result.issues.append(f"volume ({kline.volume}) is negative")
    if kline.quote_volume < 0:
        result.issues.append(f"quote_volume ({kline.quote_volume}) is negative")

    # 6. Boundary anomaly detection
    if interval_code in INTERVAL_DURATIONS_MS:
        boundary_issue = _check_boundary_alignment(kline.open_ts_ms, interval_code)
        if boundary_issue is not None:
            result.issues.append(boundary_issue)

    # Finalise result
    result.is_valid = len(result.issues) == 0
    if needs_repair and not result.is_valid:
        result.is_repairable = True
        result.repaired_kline = repaired

    return result


def validate_klines_batch(
    klines: List[NormalizedKline], interval_code: str
) -> List[ValidationResult]:
    """Validate a batch of NormalizedKline objects.

    Returns one ValidationResult per input kline, in order.
    """
    return [validate_kline(k, interval_code) for k in klines]
