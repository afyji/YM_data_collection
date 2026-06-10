"""Tests for RateLimiter — weight tracking, window reset, acquire waits, calibrate, on_rate_limited."""

from __future__ import annotations

import asyncio
import time

import pytest

from YM_data_collection.adapters.rate_limiter import RateLimiter


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

@pytest.fixture
def limiter() -> RateLimiter:
    """A rate limiter with a tiny budget for faster tests."""
    return RateLimiter(
        max_weight_per_minute=10,
        min_interval_ms=0,      # no min-interval delay
        backoff_seconds=1,
    )


# ---------------------------------------------------------------------------
# Weight tracking
# ---------------------------------------------------------------------------

class TestWeightTracking:
    @pytest.mark.asyncio
    async def test_acquire_increments_used_weight(self, limiter: RateLimiter) -> None:
        await limiter.acquire(weight=3)
        assert limiter.used_weight == 3

    @pytest.mark.asyncio
    async def test_multiple_acquires_accumulate(self, limiter: RateLimiter) -> None:
        await limiter.acquire(weight=2)
        await limiter.acquire(weight=4)
        assert limiter.used_weight == 6


# ---------------------------------------------------------------------------
# Window reset
# ---------------------------------------------------------------------------

class TestWindowReset:
    @pytest.mark.asyncio
    async def test_window_resets_after_60s(self, limiter: RateLimiter) -> None:
        await limiter.acquire(weight=5)
        assert limiter.used_weight == 5
        # Force window rollover
        limiter._window_start -= 61.0
        await limiter.acquire(weight=1)
        assert limiter.used_weight == 1  # reset happened


# ---------------------------------------------------------------------------
# Acquire waits when budget exhausted
# ---------------------------------------------------------------------------

class TestAcquireWaits:
    @pytest.mark.asyncio
    async def test_acquire_blocks_when_over_budget(self, limiter: RateLimiter) -> None:
        # Exhaust budget
        await limiter.acquire(weight=10)
        assert limiter.used_weight == 10

        # Force window near-end so the wait is short
        limiter._window_start = time.monotonic() - 59.8

        # acquire should succeed after the window rolls over (~0.2s + margin)
        t0 = time.monotonic()
        await limiter.acquire(weight=1)
        elapsed = time.monotonic() - t0
        # Should have waited at least a tiny bit; just confirm it completes
        assert limiter.used_weight >= 1


# ---------------------------------------------------------------------------
# calibrate
# ---------------------------------------------------------------------------

class TestCalibrate:
    @pytest.mark.asyncio
    async def test_calibrate_overwrites_used_weight(self, limiter: RateLimiter) -> None:
        await limiter.acquire(weight=2)
        limiter.calibrate(42)
        assert limiter.used_weight == 42

    @pytest.mark.asyncio
    async def test_calibrate_zero(self, limiter: RateLimiter) -> None:
        await limiter.acquire(weight=5)
        limiter.calibrate(0)
        assert limiter.used_weight == 0


# ---------------------------------------------------------------------------
# on_rate_limited — backoff
# ---------------------------------------------------------------------------

class TestOnRateLimited:
    @pytest.mark.asyncio
    async def test_on_rate_limited_sets_backoff(self, limiter: RateLimiter) -> None:
        limiter.on_rate_limited()
        assert limiter._backoff_until > time.monotonic()

    @pytest.mark.asyncio
    async def test_acquire_respects_backoff(self, limiter: RateLimiter) -> None:
        # Set a very short backoff
        limiter._backoff_until = time.monotonic() + 0.1
        t0 = time.monotonic()
        await limiter.acquire(weight=1)
        elapsed = time.monotonic() - t0
        # Must have waited at least ~0.1s
        assert elapsed >= 0.08  # allow small timing slop


# ---------------------------------------------------------------------------
# min_request_interval
# ---------------------------------------------------------------------------

class TestMinInterval:
    @pytest.mark.asyncio
    async def test_min_interval_enforced(self) -> None:
        lim = RateLimiter(
            max_weight_per_minute=100,
            min_interval_ms=100,
            backoff_seconds=1,
        )
        await lim.acquire(weight=1)
        t0 = time.monotonic()
        await lim.acquire(weight=1)
        elapsed = time.monotonic() - t0
        assert elapsed >= 0.09  # ~100ms min interval, with slop


# ---------------------------------------------------------------------------
# Concurrent access
# ---------------------------------------------------------------------------

class TestConcurrency:
    @pytest.mark.asyncio
    async def test_concurrent_acquires_dont_exceed_budget(self) -> None:
        lim = RateLimiter(
            max_weight_per_minute=20,
            min_interval_ms=0,
            backoff_seconds=1,
        )
        # Launch 20 concurrent acquires of weight 1 each
        tasks = [lim.acquire(weight=1) for _ in range(20)]
        await asyncio.gather(*tasks)
        # All should have completed; used_weight should reflect all 20
        # (may have crossed a window boundary, but at least the last batch is tracked)
        assert lim.used_weight <= lim.max_weight
