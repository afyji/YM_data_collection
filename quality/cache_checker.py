"""Cache vs MySQL consistency checker."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable

from YM_data_collection.cache.redis_client import RedisCacheClient
from YM_data_collection.persistence.repositories.marketdata_repo import (
    DepthSnapshotRepository,
    FundingRateRepository,
    IndexPriceRepository,
    KlineRepository,
    MarkPriceRepository,
    OpenInterestRepository,
)
from sqlalchemy.orm import Session, sessionmaker


# ---------------------------------------------------------------------------
# Data types → (repo, table_name, key_fields, cache_key_parts)
# ---------------------------------------------------------------------------

# For each data_type, define:
#   - which repo class to use
#   - table_name (if the repo requires it, else None)
#   - which fields to compare between cache and MySQL
#   - how to build the cache key parts

_KLINE_COMPARE_FIELDS = ["close_price", "volume"]
_MARK_PRICE_COMPARE_FIELDS = ["mark_price", "funding_rate"]
_INDEX_PRICE_COMPARE_FIELDS = ["index_price"]
_OPEN_INTEREST_COMPARE_FIELDS = ["open_interest"]
_FUNDING_RATE_COMPARE_FIELDS = ["funding_rate", "mark_price"]
_DEPTH_SNAPSHOT_COMPARE_FIELDS = ["best_bid_price", "best_ask_price", "mid_price"]


@dataclass
class ConsistencyResult:
    """Result of a single symbol+data_type consistency check."""

    symbol: str
    data_type: str
    cache_exists: bool
    mysql_exists: bool
    consistent: bool
    discrepancies: list[str] = field(default_factory=list)
    summary: str = ""


class CacheConsistencyChecker:
    """Compare latest cached data vs latest MySQL data for symbols."""

    def __init__(self, session_factory: sessionmaker[Session], cache_client: RedisCacheClient) -> None:
        self._session_factory = session_factory
        self._cache_client = cache_client

        # Repos
        self._kline_repo = KlineRepository()
        self._funding_rate_repo = FundingRateRepository()
        self._mark_price_repo = MarkPriceRepository()
        self._index_price_repo = IndexPriceRepository()
        self._open_interest_repo = OpenInterestRepository()
        self._depth_repo = DepthSnapshotRepository()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def check_symbol(
        self,
        venue: str,
        market_type: str,
        symbol: str,
        data_type: str,
    ) -> ConsistencyResult:
        """Compare latest cached data vs latest MySQL data for a symbol+data_type."""

        cache_data = self._get_cache_data(venue, market_type, symbol, data_type)
        mysql_data = self._get_mysql_data(venue, market_type, symbol, data_type)

        cache_exists = cache_data is not None
        mysql_exists = mysql_data is not None

        discrepancies: list[str] = []

        if not cache_exists and not mysql_exists:
            return ConsistencyResult(
                symbol=symbol,
                data_type=data_type,
                cache_exists=False,
                mysql_exists=False,
                consistent=True,
                discrepancies=[],
                summary="Neither cache nor MySQL has data",
            )

        if not cache_exists:
            return ConsistencyResult(
                symbol=symbol,
                data_type=data_type,
                cache_exists=False,
                mysql_exists=True,
                consistent=False,
                discrepancies=["Data exists in MySQL but missing from cache"],
                summary="Cache missing",
            )

        if not mysql_exists:
            return ConsistencyResult(
                symbol=symbol,
                data_type=data_type,
                cache_exists=True,
                mysql_exists=False,
                consistent=False,
                discrepancies=["Data exists in cache but missing from MySQL"],
                summary="MySQL missing",
            )

        # Both exist – compare key fields
        compare_fields = self._get_compare_fields(data_type)
        for field_name in compare_fields:
            cache_val = cache_data.get(field_name)
            mysql_val = mysql_data.get(field_name)
            if not _values_equal(cache_val, mysql_val):
                discrepancies.append(
                    f"{field_name}: cache={cache_val!r} vs mysql={mysql_val!r}"
                )

        consistent = len(discrepancies) == 0
        summary = "Consistent" if consistent else f"{len(discrepancies)} discrepancy(ies)"

        return ConsistencyResult(
            symbol=symbol,
            data_type=data_type,
            cache_exists=True,
            mysql_exists=True,
            consistent=consistent,
            discrepancies=discrepancies,
            summary=summary,
        )

    def check_all(
        self,
        venue: str,
        market_type: str,
        symbols: list[str],
        data_types: list[str],
    ) -> list[ConsistencyResult]:
        """Run check_symbol for each symbol x data_type combo."""
        results: list[ConsistencyResult] = []
        for symbol in symbols:
            for data_type in data_types:
                result = self.check_symbol(venue, market_type, symbol, data_type)
                results.append(result)
        return results

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _get_cache_data(
        self, venue: str, market_type: str, symbol: str, data_type: str
    ) -> dict[str, Any] | None:
        """Fetch latest data from cache."""
        key_parts = [venue, market_type, data_type, symbol]
        return self._cache_client.get_json(*key_parts)

    def _get_mysql_data(
        self, venue: str, market_type: str, symbol: str, data_type: str
    ) -> dict[str, Any] | None:
        """Fetch latest row from MySQL for the given data_type."""
        sf = self._session_factory

        if data_type == "kline":
            table = "spot_klines" if market_type == "spot" else "perp_klines"
            rows = self._kline_repo.query_latest(sf, table, symbol, interval_code="1h", limit=1)
        elif data_type == "mark_price":
            rows = self._mark_price_repo.query_latest(sf, symbol, limit=1)
        elif data_type == "index_price":
            rows = self._index_price_repo.query_latest(sf, symbol, limit=1)
        elif data_type == "open_interest":
            rows = self._open_interest_repo.query_latest(sf, symbol, limit=1)
        elif data_type == "funding_rate":
            rows = self._funding_rate_repo.query_latest(sf, symbol, limit=1)
        elif data_type == "depth_snapshot":
            table = "spot_depth_snapshots" if market_type == "spot" else "perp_depth_snapshots"
            rows = self._depth_repo.query_latest(sf, table, symbol, limit=1)
        else:
            return None

        if rows:
            return rows[0]
        return None

    @staticmethod
    def _get_compare_fields(data_type: str) -> list[str]:
        """Return the fields to compare for a given data_type."""
        mapping: dict[str, list[str]] = {
            "kline": _KLINE_COMPARE_FIELDS,
            "mark_price": _MARK_PRICE_COMPARE_FIELDS,
            "index_price": _INDEX_PRICE_COMPARE_FIELDS,
            "open_interest": _OPEN_INTEREST_COMPARE_FIELDS,
            "funding_rate": _FUNDING_RATE_COMPARE_FIELDS,
            "depth_snapshot": _DEPTH_SNAPSHOT_COMPARE_FIELDS,
        }
        return mapping.get(data_type, [])


def _values_equal(a: Any, b: Any) -> bool:
    """Compare two values with numeric tolerance for Decimal/float differences."""
    # If both can be converted to float, compare with tolerance
    try:
        fa = float(a)
        fb = float(b)
        return abs(fa - fb) < 1e-10
    except (TypeError, ValueError):
        pass

    # String comparison fallback
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False

    return str(a) == str(b)
