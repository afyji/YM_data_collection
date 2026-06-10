"""Binance Spot REST adapter."""

from __future__ import annotations

import logging
from typing import Any

import httpx

from YM_data_collection.adapters.rate_limiter import RateLimiter
from YM_data_collection.config.models import BinanceEndpointConfig

logger = logging.getLogger(__name__)

# Header name used by Binance spot for current minute weight
_SPOT_WEIGHT_HEADER = "X-MBX-USED-WEIGHT-1M"


class BinanceSpotAdapter:
    """Wraps Binance Spot REST endpoints with rate-limit awareness."""

    def __init__(
        self,
        client: httpx.AsyncClient,
        endpoint: BinanceEndpointConfig,
        limiter: RateLimiter,
    ) -> None:
        self._client = client
        self._endpoint = endpoint
        self._limiter = limiter

    # ------------------------------------------------------------------
    # Public methods
    # ------------------------------------------------------------------

    async def fetch_klines(
        self,
        symbol: str,
        interval: str,
        start_ts_ms: int,
        end_ts_ms: int,
        limit: int = 1000,
    ) -> list[list[Any]]:
        """Return raw Binance spot kline arrays.

        GET {rest_base_url}/api/v3/klines
        """
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": start_ts_ms,
            "endTime": end_ts_ms,
            "limit": limit,
        }
        return await self._request("/api/v3/klines", params)  # type: ignore[no-any-return]

    async def fetch_exchange_info(self) -> dict[str, Any]:
        """Return raw Binance spot exchange info."""
        return await self._request("/api/v3/exchangeInfo")  # type: ignore[no-any-return]

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    async def _request(self, path: str, params: dict[str, Any] | None = None) -> Any:
        url = f"{self._endpoint.rest_base_url}{path}"
        await self._limiter.acquire(weight=1)
        response = await self._client.get(url, params=params)
        self._calibrate_from_headers(response)
        response.raise_for_status()
        return response.json()

    def _calibrate_from_headers(self, response: httpx.Response) -> None:
        """Parse Binance weight header and update the rate limiter."""
        used = response.headers.get(_SPOT_WEIGHT_HEADER)
        if used is not None:
            try:
                self._limiter.calibrate(int(used))
            except (ValueError, TypeError):
                logger.debug("Could not parse weight header: %s", used)
