"""Unified Binance gateway routing spot/perp calls to the correct adapter."""

from __future__ import annotations

import logging
from typing import Any, Literal

import httpx

from YM_data_collection.adapters.binance_perp import BinancePerpAdapter
from YM_data_collection.adapters.binance_spot import BinanceSpotAdapter
from YM_data_collection.adapters.rate_limiter import RateLimiter
from YM_data_collection.config.models import BinanceConfig

logger = logging.getLogger(__name__)

MarketType = Literal["spot", "perp"]


class BinanceGateway:
    """Top-level façade that routes Binance REST calls by market type.

    Owns the ``httpx.AsyncClient`` lifecycle and creates per-market
    ``RateLimiter`` instances from the central ``BinanceConfig``.
    """

    def __init__(
        self,
        config: BinanceConfig,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._config = config
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(
            timeout=config.http_timeout_seconds,
        )

        rl = config.rate_limit
        self._spot_limiter = RateLimiter(
            max_weight_per_minute=rl.spot_weight_per_minute,
            min_interval_ms=rl.min_request_interval_ms,
            backoff_seconds=rl.backoff_on_429_seconds,
        )
        self._perp_limiter = RateLimiter(
            max_weight_per_minute=rl.perp_weight_per_minute,
            min_interval_ms=rl.min_request_interval_ms,
            backoff_seconds=rl.backoff_on_429_seconds,
        )

        self._spot = BinanceSpotAdapter(
            client=self._client,
            endpoint=config.spot,
            limiter=self._spot_limiter,
        )
        self._perp = BinancePerpAdapter(
            client=self._client,
            endpoint=config.perp,
            limiter=self._perp_limiter,
        )

    # ------------------------------------------------------------------
    # Public API — mirrors adapters with market_type routing
    # ------------------------------------------------------------------

    async def fetch_klines(
        self,
        market_type: MarketType,
        symbol: str,
        interval: str,
        start_ts_ms: int,
        end_ts_ms: int,
        limit: int | None = None,
    ) -> list[list[Any]]:
        """Fetch klines, routing to spot or perp adapter."""
        adapter = self._adapter(market_type)
        if limit is None:
            limit = 1000 if market_type == "spot" else 1500
        return await adapter.fetch_klines(
            symbol=symbol,
            interval=interval,
            start_ts_ms=start_ts_ms,
            end_ts_ms=end_ts_ms,
            limit=limit,
        )

    async def fetch_funding_rates(
        self,
        symbol: str,
        start_ts_ms: int,
        end_ts_ms: int,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        """Fetch funding rates (perp only)."""
        return await self._perp.fetch_funding_rates(
            symbol=symbol,
            start_ts_ms=start_ts_ms,
            end_ts_ms=end_ts_ms,
            limit=limit,
        )

    async def fetch_mark_price_klines(
        self,
        symbol: str,
        interval: str,
        start_ts_ms: int,
        end_ts_ms: int,
        limit: int = 500,
    ) -> list[list[Any]]:
        """Fetch mark-price klines (perp only)."""
        return await self._perp.fetch_mark_price_klines(
            symbol=symbol,
            interval=interval,
            start_ts_ms=start_ts_ms,
            end_ts_ms=end_ts_ms,
            limit=limit,
        )

    async def fetch_index_price_klines(
        self,
        symbol: str,
        interval: str,
        start_ts_ms: int,
        end_ts_ms: int,
        limit: int = 500,
    ) -> list[list[Any]]:
        """Fetch index-price klines (perp only)."""
        return await self._perp.fetch_index_price_klines(
            symbol=symbol,
            interval=interval,
            start_ts_ms=start_ts_ms,
            end_ts_ms=end_ts_ms,
            limit=limit,
        )

    async def fetch_open_interest_hist(
        self,
        symbol: str,
        period: str,
        start_ts_ms: int,
        end_ts_ms: int,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        """Fetch open-interest history (perp only)."""
        return await self._perp.fetch_open_interest_hist(
            symbol=symbol,
            period=period,
            start_ts_ms=start_ts_ms,
            end_ts_ms=end_ts_ms,
            limit=limit,
        )

    async def fetch_premium_index(self, symbol: str) -> dict[str, Any]:
        """Fetch current premium/index price (perp only)."""
        return await self._perp.fetch_premium_index(symbol=symbol)

    async def fetch_exchange_info(
        self, market_type: MarketType
    ) -> dict[str, Any]:
        """Fetch exchange info for spot or perp."""
        return await self._adapter(market_type).fetch_exchange_info()

    async def close(self) -> None:
        """Close the underlying httpx client if we own it."""
        if self._owns_client:
            await self._client.aclose()

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _adapter(self, market_type: MarketType) -> BinanceSpotAdapter | BinancePerpAdapter:
        if market_type == "spot":
            return self._spot
        return self._perp
