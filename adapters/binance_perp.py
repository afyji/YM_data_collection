"""Binance Perpetual (USDT-M futures) REST adapter."""

from __future__ import annotations

import logging
from typing import Any

import httpx

from YM_data_collection.adapters.rate_limiter import RateLimiter
from YM_data_collection.config.models import BinanceEndpointConfig

logger = logging.getLogger(__name__)

# Header name used by Binance perp for current minute weight
_PERP_WEIGHT_HEADER = "x-mbx-used-weight-1m"


class BinancePerpAdapter:
    """Wraps Binance USDT-M Futures REST endpoints with rate-limit awareness."""

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
        limit: int = 1500,
    ) -> list[list[Any]]:
        """Return raw Binance perp kline arrays.

        GET {rest_base_url}/fapi/v1/klines
        """
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": start_ts_ms,
            "endTime": end_ts_ms,
            "limit": limit,
        }
        return await self._request("/fapi/v1/klines", params)  # type: ignore[no-any-return]

    async def fetch_funding_rates(
        self,
        symbol: str,
        start_ts_ms: int,
        end_ts_ms: int,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        """Return raw Binance funding rate records.

        GET {rest_base_url}/fapi/v1/fundingRate
        """
        params = {
            "symbol": symbol,
            "startTime": start_ts_ms,
            "endTime": end_ts_ms,
            "limit": limit,
        }
        return await self._request("/fapi/v1/fundingRate", params)  # type: ignore[no-any-return]

    async def fetch_mark_price_klines(
        self,
        symbol: str,
        interval: str,
        start_ts_ms: int,
        end_ts_ms: int,
        limit: int = 500,
    ) -> list[list[Any]]:
        """Return raw Binance mark-price kline arrays.

        GET {rest_base_url}/fapi/v1/markPriceKlines
        """
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": start_ts_ms,
            "endTime": end_ts_ms,
            "limit": limit,
        }
        return await self._request("/fapi/v1/markPriceKlines", params)  # type: ignore[no-any-return]

    async def fetch_index_price_klines(
        self,
        symbol: str,
        interval: str,
        start_ts_ms: int,
        end_ts_ms: int,
        limit: int = 500,
    ) -> list[list[Any]]:
        """Return raw Binance index-price kline arrays.

        GET {rest_base_url}/fapi/v1/indexPriceKlines
        Binance names the instrument parameter ``pair`` for this endpoint.
        """
        params = {
            "pair": symbol,
            "interval": interval,
            "startTime": start_ts_ms,
            "endTime": end_ts_ms,
            "limit": limit,
        }
        return await self._request("/fapi/v1/indexPriceKlines", params)  # type: ignore[no-any-return]

    async def fetch_open_interest_hist(
        self,
        symbol: str,
        period: str,
        start_ts_ms: int,
        end_ts_ms: int,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        """Return raw Binance open-interest history records.

        GET {rest_base_url}/futures/data/openInterestHist
        """
        params = {
            "symbol": symbol,
            "period": period,
            "startTime": start_ts_ms,
            "endTime": end_ts_ms,
            "limit": limit,
        }
        return await self._request("/futures/data/openInterestHist", params)  # type: ignore[no-any-return]

    async def fetch_premium_index(self, symbol: str) -> dict[str, Any]:
        """Return current mark/index price snapshot.

        GET {rest_base_url}/fapi/v1/premiumIndex
        """
        params = {"symbol": symbol}
        return await self._request("/fapi/v1/premiumIndex", params)  # type: ignore[no-any-return]

    async def fetch_exchange_info(self) -> dict[str, Any]:
        """Return raw Binance perp exchange info."""
        return await self._request("/fapi/v1/exchangeInfo")  # type: ignore[no-any-return]

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
        used = response.headers.get(_PERP_WEIGHT_HEADER)
        if used is not None:
            try:
                self._limiter.calibrate(int(used))
            except (ValueError, TypeError):
                logger.debug("Could not parse weight header: %s", used)
