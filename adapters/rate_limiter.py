"""Token-bucket rate limiter with weight tracking per minute window."""

from __future__ import annotations

import asyncio
import time
import logging

logger = logging.getLogger(__name__)


class RateLimiter:
    """Rate limiter using a sliding minute-window weight budget.

    Tracks cumulative request weight within the current 60-second window.
    When the budget is exhausted, ``acquire`` blocks until the window resets.
    Calibrates against Binance's ``X-MBX-USED-WEIGHT-1M`` response header.
    """

    def __init__(
        self,
        max_weight_per_minute: int,
        min_interval_ms: int,
        backoff_seconds: int,
    ) -> None:
        self._max_weight = max_weight_per_minute
        self._min_interval_s = min_interval_ms / 1000.0
        self._backoff_seconds = backoff_seconds
        self._used_weight = 0
        self._window_start: float = time.monotonic()
        self._last_request_time: float = 0.0
        self._lock = asyncio.Lock()
        self._backoff_until: float = 0.0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def acquire(self, weight: int = 1) -> None:
        """Wait until *weight* is available within the current minute budget."""
        while True:
            async with self._lock:
                self._maybe_reset_window()
                now = time.monotonic()

                # Respect backoff from a previous 429
                if now < self._backoff_until:
                    sleep_time = self._backoff_until - now
                elif self._used_weight + weight > self._max_weight:
                    # Budget exhausted — wait for window to roll over
                    elapsed = now - self._window_start
                    sleep_time = max(60.0 - elapsed + 0.05, 0.0)
                    logger.info(
                        "Rate budget exhausted (%d/%d), waiting %.1fs for window reset",
                        self._used_weight,
                        self._max_weight,
                        sleep_time,
                    )
                else:
                    # Budget available — grant and enforce min interval
                    self._used_weight += weight
                    min_interval_wait = max(
                        0.0, self._min_interval_s - (now - self._last_request_time)
                    )
                    self._last_request_time = now
                    # Release lock, then enforce min interval
                    break

            # Sleep outside the lock so other coroutines can proceed
            await asyncio.sleep(sleep_time)  # type: ignore[possibly-undefined]

        # Enforce minimum request interval outside the lock
        if min_interval_wait > 0:  # type: ignore[possibly-undefined]
            await asyncio.sleep(min_interval_wait)

    def calibrate(self, used_weight: int) -> None:
        """Update internal weight counter from the Binance response header."""
        self._used_weight = used_weight
        logger.debug("Calibrated used weight to %d", used_weight)

    def on_rate_limited(self) -> None:
        """Signal that a 429 was received — trigger backoff."""
        self._backoff_until = time.monotonic() + self._backoff_seconds
        logger.warning(
            "Rate limited (429) — backing off for %ds", self._backoff_seconds
        )

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _maybe_reset_window(self) -> None:
        """Reset the weight counter if the 60-second window has rolled over."""
        now = time.monotonic()
        if now - self._window_start >= 60.0:
            self._window_start = now
            self._used_weight = 0

    # ------------------------------------------------------------------
    # Test helpers (not part of public API)
    # ------------------------------------------------------------------

    @property
    def used_weight(self) -> int:
        return self._used_weight

    @property
    def max_weight(self) -> int:
        return self._max_weight
