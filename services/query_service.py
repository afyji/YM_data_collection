"""Market-data query service with MySQL+Redis auto-stitch.

Provides range and latest queries across klines, mark/index/open-interest/
funding-rate snapshots, and depth data.  For high-frequency data types the
service auto-stitches MySQL (historical, already flushed) and Redis (latest,
unflushed) results, deduplicating by event_ts_ms.
"""

from __future__ import annotations

import logging
from typing import Any

from YM_data_collection.cache.redis_client import RedisCacheClient
from YM_data_collection.config.models import QuerySourceConfig

logger = logging.getLogger(__name__)

# Data types that use redis_first write policy and benefit from auto-stitch.
_AUTOSTITCH_TYPES = {"mark_price", "index_price", "open_interest", "depth_snapshot"}

# Map data_type -> default MySQL table name (perp variants).
_DATA_TYPE_TABLE: dict[str, str] = {
    "mark_price": "perp_mark_prices",
    "index_price": "perp_index_prices",
    "open_interest": "perp_open_interest",
    "funding_rate": "perp_funding_rates",
}


class MarketDataQueryService:
    """Query market data with cache-first / MySQL-fallback and auto-stitch."""

    def __init__(
        self,
        session_factory: Any,
        cache_client: RedisCacheClient,
        query_source_config: QuerySourceConfig,
        repos: dict[str, Any],
    ) -> None:
        self._session_factory = session_factory
        self._cache = cache_client
        self._cfg = query_source_config
        self._repos = repos

    # ------------------------------------------------------------------
    # Klines
    # ------------------------------------------------------------------

    def query_klines_range(
        self,
        market_type: str,
        symbol: str,
        interval_code: str,
        start_ts_ms: int,
        end_ts_ms: int,
    ) -> dict[str, Any]:
        """Return klines in [start_ts_ms, end_ts_ms] from MySQL only."""
        table_name = f"{market_type}_klines"
        repo: Any = self._repos["kline"]
        data = repo.query_range(
            self._session_factory, table_name, symbol, interval_code, start_ts_ms, end_ts_ms
        )
        return {"data": data, "meta": {"source": "mysql", "fallback_used": False, "cache_refreshed": False}}

    def query_klines_recent(
        self,
        market_type: str,
        symbol: str,
        interval_code: str,
        limit: int,
    ) -> dict[str, Any]:
        """Return the most recent *limit* klines from MySQL."""
        table_name = f"{market_type}_klines"
        repo: Any = self._repos["kline"]
        data = repo.query_latest(
            self._session_factory, table_name, symbol, interval_code, limit=limit
        )
        return {"data": data, "meta": {"source": "mysql", "fallback_used": False, "cache_refreshed": False}}

    # ------------------------------------------------------------------
    # Latest snapshot (mark_price, index_price, open_interest, funding_rate)
    # ------------------------------------------------------------------

    def query_latest_snapshot(
        self,
        market_type: str,
        symbol: str,
        data_type: str,
    ) -> dict[str, Any]:
        """Latest snapshot for a high-freq data type.

        Cache-first with MySQL fallback.  For redis_first types the cache
        is authoritative for the latest value, so we only fall back to
        MySQL when the cache miss occurs.
        """
        cache_first = self._cfg.snapshot_cache_first_enabled
        mysql_fallback = self._cfg.snapshot_mysql_fallback_enabled
        cache_refreshed = False

        cache_data: dict[str, Any] | None = None
        mysql_data: list[dict[str, Any]] = []

        # --- Cache attempt ---
        if cache_first:
            cache_data = self._cache.get_json(data_type, market_type, symbol)

        # --- MySQL fallback (only when cache miss) ---
        if cache_data is None and mysql_fallback:
            repo = self._repos.get(_repo_key(data_type))
            if repo is not None:
                mysql_data = self._query_repo_latest(repo, data_type, symbol)

        # --- Build result ---
        if cache_data is None and not mysql_data:
            return {
                "data": None,
                "meta": {
                    "source": "none",
                    "fallback_used": False,
                    "cache_refreshed": False,
                },
            }

        if cache_data is not None:
            merged = cache_data
            source_label = "cache"
            fallback_used = False
        else:
            merged = mysql_data[0] if len(mysql_data) == 1 else mysql_data[-1]
            source_label = "mysql"
            fallback_used = cache_first and bool(mysql_data)

        # Backfill cache on fallback
        if fallback_used and self._cfg.cache_backfill_on_fallback_enabled and merged is not None:
            try:
                self._cache.set_json(data_type, market_type, symbol, payload=merged)
                cache_refreshed = True
            except Exception:
                logger.warning("Failed to backfill cache for %s/%s/%s", data_type, market_type, symbol)

        return {
            "data": merged,
            "meta": {
                "source": source_label,
                "fallback_used": fallback_used,
                "cache_refreshed": cache_refreshed,
            },
        }

    # ------------------------------------------------------------------
    # Latest depth
    # ------------------------------------------------------------------

    def query_latest_depth(
        self,
        market_type: str,
        symbol: str,
    ) -> dict[str, Any]:
        """Latest depth snapshot.  Cache-first with MySQL fallback."""
        data_type = "depth_snapshot"
        cache_first = self._cfg.depth_cache_first_enabled
        mysql_fallback = self._cfg.depth_mysql_fallback_enabled
        cache_refreshed = False

        cache_data: dict[str, Any] | None = None
        mysql_data: list[dict[str, Any]] = []

        if cache_first:
            cache_data = self._cache.get_json(data_type, market_type, symbol)

        if cache_data is None and mysql_fallback:
            repo = self._repos.get("depth_snapshot")
            if repo is not None:
                table_name = f"{market_type}_depth_snapshots"
                mysql_data = repo.query_latest(self._session_factory, table_name, symbol, limit=1)

        if cache_data is None and not mysql_data:
            return {
                "data": None,
                "meta": {"source": "none", "fallback_used": False, "cache_refreshed": False},
            }

        if cache_data is not None:
            merged = cache_data
            source_label = "cache"
            fallback_used = False
        else:
            merged = mysql_data[0]
            source_label = "mysql"
            fallback_used = cache_first

        if fallback_used and self._cfg.cache_backfill_on_fallback_enabled and merged is not None:
            try:
                self._cache.set_json(data_type, market_type, symbol, payload=merged)
                cache_refreshed = True
            except Exception:
                logger.warning("Failed to backfill cache for depth/%s/%s", market_type, symbol)

        return {
            "data": merged,
            "meta": {"source": source_label, "fallback_used": fallback_used, "cache_refreshed": cache_refreshed},
        }

    # ------------------------------------------------------------------
    # Range queries with auto-stitch for high-freq types
    # ------------------------------------------------------------------

    def query_range(
        self,
        market_type: str,
        symbol: str,
        data_type: str,
        start_ts_ms: int,
        end_ts_ms: int,
        interval_code: str | None = None,
    ) -> dict[str, Any]:
        """Range query with auto-stitch for high-freq data.

        For klines and funding_rate, MySQL only.
        For mark_price, index_price, open_interest, depth_snapshot, auto-stitch
        MySQL historical + Redis latest, deduplicate by event_ts_ms.
        """
        if data_type == "kline":
            return self.query_klines_range(market_type, symbol, interval_code or "1h", start_ts_ms, end_ts_ms)

        ts_field = "funding_time_ts_ms" if data_type == "funding_rate" else "event_ts_ms"
        sources: list[str] = []

        # MySQL historical range
        repo = self._repos.get(_repo_key(data_type))
        mysql_rows: list[dict[str, Any]] = []
        if repo is not None:
            mysql_rows = self._query_repo_range(repo, data_type, symbol, start_ts_ms, end_ts_ms)
            if mysql_rows:
                sources.append("mysql")

        # Redis latest (for auto-stitch types)
        cache_rows: list[dict[str, Any]] = []
        if data_type in _AUTOSTITCH_TYPES:
            cache_data = self._cache.get_json(data_type, market_type, symbol)
            if cache_data is not None:
                cache_ts = cache_data.get(ts_field, 0)
                if start_ts_ms <= cache_ts <= end_ts_ms:
                    cache_rows = [cache_data]
                    sources.append("cache")

        if not mysql_rows and not cache_rows:
            return {"data": [], "meta": {"source": "none", "fallback_used": False, "cache_refreshed": False}}

        # Merge and deduplicate by ts_field
        merged = self._dedup_by_ts(mysql_rows + cache_rows, ts_field)
        source_label = "+".join(sources) if sources else "none"
        fallback_used = "mysql" in sources and "cache" not in sources and data_type in _AUTOSTITCH_TYPES

        return {"data": merged, "meta": {"source": source_label, "fallback_used": fallback_used, "cache_refreshed": False}}

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _query_repo_latest(self, repo: Any, data_type: str, symbol: str) -> list[dict[str, Any]]:
        """Dispatch to the right repo method for latest query."""
        if data_type == "funding_rate":
            return repo.query_latest(self._session_factory, symbol, limit=1)
        if data_type in ("mark_price", "index_price", "open_interest"):
            return repo.query_latest(self._session_factory, symbol, limit=1)
        return []

    def _query_repo_range(self, repo: Any, data_type: str, symbol: str, start_ts_ms: int, end_ts_ms: int) -> list[dict[str, Any]]:
        """Dispatch to the right repo method for range query."""
        if data_type == "funding_rate":
            return repo.query_range(self._session_factory, symbol, start_ts_ms, end_ts_ms)
        if data_type in ("mark_price", "index_price", "open_interest"):
            return repo.query_range(self._session_factory, symbol, start_ts_ms, end_ts_ms)
        if data_type == "depth_snapshot":
            # table_name must be provided by caller context; fallback to perp
            return repo.query_range(self._session_factory, "perp_depth_snapshots", symbol, start_ts_ms, end_ts_ms)
        return []

    @staticmethod
    def _merge_snapshot_and_rows(
        cache_data: dict[str, Any],
        mysql_rows: list[dict[str, Any]],
        data_type: str,
    ) -> dict[str, Any]:
        """Pick the record with the latest event_ts_ms between cache and MySQL."""
        ts_field = "event_ts_ms"
        if data_type == "funding_rate":
            ts_field = "funding_time_ts_ms"

        cache_ts = cache_data.get(ts_field, 0)
        best_mysql_ts = max((r.get(ts_field, 0) for r in mysql_rows), default=0)

        if cache_ts >= best_mysql_ts:
            return cache_data
        # Return the MySQL row with the highest ts
        return max(mysql_rows, key=lambda r: r.get(ts_field, 0))

    @staticmethod
    def _dedup_by_ts(rows: list[dict[str, Any]], ts_field: str) -> list[dict[str, Any]]:
        """Deduplicate rows by ts_field, keeping the latest, then sort ascending."""
        seen: dict[Any, dict[str, Any]] = {}
        for row in rows:
            key = row.get(ts_field)
            if key is not None:
                if key not in seen:
                    seen[key] = row
        return sorted(seen.values(), key=lambda r: r.get(ts_field, 0))


def _repo_key(data_type: str) -> str:
    """Map data_type to repos dict key."""
    mapping = {
        "mark_price": "mark_price",
        "index_price": "index_price",
        "open_interest": "open_interest",
        "funding_rate": "funding_rate",
        "depth_snapshot": "depth_snapshot",
    }
    return mapping.get(data_type, data_type)
