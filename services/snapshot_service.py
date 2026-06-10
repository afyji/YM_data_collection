"""Snapshot service – convenience wrappers for latest market-data snapshots.

Each method tries the Redis cache first (when configured), then falls back
to MySQL.  The actual query logic is delegated to the repositories.
"""

from __future__ import annotations

import logging
from typing import Any

from YM_data_collection.cache.redis_client import RedisCacheClient
from YM_data_collection.config.models import QuerySourceConfig

logger = logging.getLogger(__name__)


class SnapshotService:
    """Retrieve the latest snapshot for various data types."""

    def __init__(
        self,
        cache_client: RedisCacheClient,
        session_factory: Any,
        repos: dict[str, Any],
        query_source_config: QuerySourceConfig,
    ) -> None:
        self._cache = cache_client
        self._session_factory = session_factory
        self._repos = repos
        self._cfg = query_source_config

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_latest_mark_price(self, symbol: str) -> dict[str, Any] | None:
        """Return the latest mark price for *symbol*, or None."""
        return self._latest_snapshot("perp", symbol, "mark_price")

    def get_latest_index_price(self, symbol: str) -> dict[str, Any] | None:
        """Return the latest index price for *symbol*, or None."""
        return self._latest_snapshot("perp", symbol, "index_price")

    def get_latest_open_interest(self, symbol: str) -> dict[str, Any] | None:
        """Return the latest open interest for *symbol*, or None."""
        return self._latest_snapshot("perp", symbol, "open_interest")

    def get_latest_funding_rate(self, symbol: str) -> dict[str, Any] | None:
        """Return the latest funding rate for *symbol*, or None."""
        return self._latest_snapshot("perp", symbol, "funding_rate")

    def get_latest_depth(self, market_type: str, symbol: str) -> dict[str, Any] | None:
        """Return the latest depth snapshot for *symbol*, or None."""
        data_type = "depth_snapshot"
        cache_first = self._cfg.depth_cache_first_enabled
        mysql_fallback = self._cfg.depth_mysql_fallback_enabled

        # Cache attempt
        if cache_first:
            cached = self._cache.get_json(data_type, market_type, symbol)
            if cached is not None:
                return cached

        # MySQL fallback
        if mysql_fallback:
            repo = self._repos.get("depth_snapshot")
            if repo is not None:
                table_name = f"{market_type}_depth_snapshots"
                rows = repo.query_latest(self._session_factory, table_name, symbol, limit=1)
                if rows:
                    result = rows[0]
                    # Backfill cache
                    if self._cfg.cache_backfill_on_fallback_enabled:
                        try:
                            self._cache.set_json(data_type, market_type, symbol, payload=result)
                        except Exception:
                            logger.warning("Cache backfill failed for depth/%s/%s", market_type, symbol)
                    return result

        return None

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _latest_snapshot(
        self,
        market_type: str,
        symbol: str,
        data_type: str,
    ) -> dict[str, Any] | None:
        """Generic cache-first / MySQL-fallback for a snapshot data type."""
        cache_first = self._cfg.snapshot_cache_first_enabled
        mysql_fallback = self._cfg.snapshot_mysql_fallback_enabled

        # Cache attempt
        if cache_first:
            cached = self._cache.get_json(data_type, market_type, symbol)
            if cached is not None:
                return cached

        # MySQL fallback
        if mysql_fallback:
            repo = self._repos.get(data_type)
            if repo is not None:
                rows = repo.query_latest(self._session_factory, symbol, limit=1)
                if rows:
                    result = rows[0]
                    # Backfill cache
                    if self._cfg.cache_backfill_on_fallback_enabled:
                        try:
                            self._cache.set_json(data_type, market_type, symbol, payload=result)
                        except Exception:
                            logger.warning(
                                "Cache backfill failed for %s/%s/%s", data_type, market_type, symbol
                            )
                    return result

        return None
