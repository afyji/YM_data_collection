"""Exchange-side availability constraints and validation helpers."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

OPEN_INTEREST_RECENT_WINDOW_DAYS = 30


def format_ts_ms_utc(ts_ms: int) -> str:
    """Render a millisecond timestamp as an ISO-like UTC string."""

    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def compute_open_interest_overlap(
    start_ts_ms: int,
    end_ts_ms: int,
    *,
    now: datetime | None = None,
) -> tuple[int, int] | None:
    """Compute the intersection of [start_ts_ms, end_ts_ms] with Binance's
    available 1-month window for openInterestHist.

    Returns ``(overlap_start, overlap_end)`` in ms, or ``None`` if there is
    no overlap at all.
    """
    current_time = now or datetime.now(timezone.utc)
    cutoff_dt = current_time - timedelta(days=OPEN_INTEREST_RECENT_WINDOW_DAYS)
    cutoff_ts_ms = int(cutoff_dt.timestamp() * 1000)
    now_ts_ms = int(current_time.timestamp() * 1000)

    # Available window: [cutoff_ts_ms, now_ts_ms]
    overlap_start = max(start_ts_ms, cutoff_ts_ms)
    overlap_end = min(end_ts_ms, now_ts_ms)

    if overlap_start > overlap_end:
        return None
    return (overlap_start, overlap_end)


def validate_open_interest_history_range(
    start_ts_ms: int,
    end_ts_ms: int,
    *,
    now: datetime | None = None,
) -> None:
    """Validate Binance open-interest history availability.

    Binance only exposes the latest 1 month of open-interest history via
    ``/futures/data/openInterestHist``. Reject older requests early so callers
    get a clear message instead of an HTTP 400.
    """

    current_time = now or datetime.now(timezone.utc)
    cutoff_dt = current_time - timedelta(days=OPEN_INTEREST_RECENT_WINDOW_DAYS)
    cutoff_ts_ms = int(cutoff_dt.timestamp() * 1000)

    if start_ts_ms < cutoff_ts_ms or end_ts_ms < cutoff_ts_ms:
        raise ValueError(
            "Binance openInterestHist only provides the latest 1 month of data. "
            f"Requested range [{format_ts_ms_utc(start_ts_ms)}, {format_ts_ms_utc(end_ts_ms)}] "
            f"falls outside the available window starting at approximately "
            f"{format_ts_ms_utc(cutoff_ts_ms)}. "
            "Use a newer time range or omit open_interest from --data-types."
        )
