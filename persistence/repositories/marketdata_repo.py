"""Market-data repository classes – raw SQL via SQLAlchemy text().

Six repository classes cover: klines (spot/perp), funding rates, open
interest, mark prices, index prices, and depth snapshots (spot/perp).

Each repository provides:
  - upsert_batch: batch insert with dialect-aware conflict handling
      (MySQL → INSERT ... ON DUPLICATE KEY UPDATE,
       SQLite → INSERT OR REPLACE)
  - query_range: time-bounded query returning list[dict]
  - query_latest: most-recent N records returning list[dict]
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Sequence

from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

from YM_data_collection.domain.models import (
    NormalizedDepthSnapshot,
    NormalizedFundingRate,
    NormalizedIndexPrice,
    NormalizedKline,
    NormalizedMarkPrice,
    NormalizedOpenInterest,
)
from YM_data_collection.persistence.mysql import session_scope


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _coerce_param(value: Any, dialect: str) -> Any:
    """Coerce a parameter value for the target dialect.

    SQLite does not support Decimal or datetime directly, so we convert
    Decimal → str and datetime → str for SQLite.  MySQL (pymysql) handles
    both natively.
    """
    if dialect == "sqlite":
        if isinstance(value, Decimal):
            return str(value)
        if isinstance(value, datetime):
            return value.isoformat()
    return value


def _coerce_params(params: list[dict[str, Any]], dialect: str) -> list[dict[str, Any]]:
    """Coerce all parameter values for the target dialect."""
    return [{k: _coerce_param(v, dialect) for k, v in row.items()} for row in params]


def _dialect(session: Session) -> str:
    return session.bind.dialect.name  # type: ignore[union-attr]


def _now_utc() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


# ---------------------------------------------------------------------------
# KlineRepository
# ---------------------------------------------------------------------------

class KlineRepository:
    """Repository for spot_klines / perp_klines tables."""

    def upsert_batch(
        self,
        session_factory: sessionmaker[Session],
        table_name: str,
        klines: Sequence[NormalizedKline],
    ) -> int:
        """Upsert a batch of klines. Returns number of rows affected."""
        if not klines:
            return 0

        cols = (
            "venue, symbol, instrument_code, interval_code, "
            "open_ts_ms, close_ts_ms, open_dt_utc, close_dt_utc, "
            "open_price, high_price, low_price, close_price, "
            "volume, quote_volume, trade_count, "
            "taker_buy_base_volume, taker_buy_quote_volume, "
            "source, ingested_at_utc"
        )

        with session_scope(session_factory) as session:
            dialect = _dialect(session)

            if dialect == "sqlite":
                sql = text(
                    f"INSERT OR REPLACE INTO {table_name} ({cols}) VALUES ("
                    ":venue, :symbol, :instrument_code, :interval_code, "
                    ":open_ts_ms, :close_ts_ms, :open_dt_utc, :close_dt_utc, "
                    ":open_price, :high_price, :low_price, :close_price, "
                    ":volume, :quote_volume, :trade_count, "
                    ":taker_buy_base_volume, :taker_buy_quote_volume, "
                    ":source, :ingested_at_utc)"
                )
            else:
                update_cols = (
                    "close_ts_ms=VALUES(close_ts_ms), "
                    "open_dt_utc=VALUES(open_dt_utc), close_dt_utc=VALUES(close_dt_utc), "
                    "open_price=VALUES(open_price), high_price=VALUES(high_price), "
                    "low_price=VALUES(low_price), close_price=VALUES(close_price), "
                    "volume=VALUES(volume), quote_volume=VALUES(quote_volume), "
                    "trade_count=VALUES(trade_count), "
                    "taker_buy_base_volume=VALUES(taker_buy_base_volume), "
                    "taker_buy_quote_volume=VALUES(taker_buy_quote_volume), "
                    "source=VALUES(source), ingested_at_utc=VALUES(ingested_at_utc)"
                )
                sql = text(
                    f"INSERT INTO {table_name} ({cols}) VALUES ("
                    ":venue, :symbol, :instrument_code, :interval_code, "
                    ":open_ts_ms, :close_ts_ms, :open_dt_utc, :close_dt_utc, "
                    ":open_price, :high_price, :low_price, :close_price, "
                    ":volume, :quote_volume, :trade_count, "
                    ":taker_buy_base_volume, :taker_buy_quote_volume, "
                    ":source, :ingested_at_utc) "
                    f"ON DUPLICATE KEY UPDATE {update_cols}"
                )

            now = _now_utc()
            params = [
                {
                    "venue": k.venue,
                    "symbol": k.symbol,
                    "instrument_code": k.instrument_code,
                    "interval_code": k.interval_code,
                    "open_ts_ms": k.open_ts_ms,
                    "close_ts_ms": k.close_ts_ms,
                    "open_dt_utc": k.open_dt_utc,
                    "close_dt_utc": k.close_dt_utc,
                    "open_price": k.open_price,
                    "high_price": k.high_price,
                    "low_price": k.low_price,
                    "close_price": k.close_price,
                    "volume": k.volume,
                    "quote_volume": k.quote_volume,
                    "trade_count": k.trade_count,
                    "taker_buy_base_volume": k.taker_buy_base_volume,
                    "taker_buy_quote_volume": k.taker_buy_quote_volume,
                    "source": k.source,
                    "ingested_at_utc": now,
                }
                for k in klines
            ]
            params = _coerce_params(params, dialect)
            result = session.execute(sql, params)
            return result.rowcount

    def query_range(
        self,
        session_factory: sessionmaker[Session],
        table_name: str,
        symbol: str,
        interval_code: str,
        start_ts_ms: int,
        end_ts_ms: int,
    ) -> list[dict[str, Any]]:
        """Return klines within [start_ts_ms, end_ts_ms] inclusive."""
        sql = text(
            f"SELECT * FROM {table_name} "
            "WHERE symbol = :symbol AND interval_code = :interval_code "
            "AND open_ts_ms >= :start_ts_ms AND open_ts_ms <= :end_ts_ms "
            "ORDER BY open_ts_ms ASC"
        )
        with session_scope(session_factory) as session:
            rows = session.execute(sql, {
                "symbol": symbol,
                "interval_code": interval_code,
                "start_ts_ms": start_ts_ms,
                "end_ts_ms": end_ts_ms,
            }).mappings().all()
            return [dict(r) for r in rows]

    def query_latest(
        self,
        session_factory: sessionmaker[Session],
        table_name: str,
        symbol: str,
        interval_code: str,
        limit: int = 1,
    ) -> list[dict[str, Any]]:
        """Return most recent *limit* klines, newest first."""
        sql = text(
            f"SELECT * FROM {table_name} "
            "WHERE symbol = :symbol AND interval_code = :interval_code "
            "ORDER BY open_ts_ms DESC LIMIT :lim"
        )
        with session_scope(session_factory) as session:
            rows = session.execute(sql, {
                "symbol": symbol,
                "interval_code": interval_code,
                "lim": limit,
            }).mappings().all()
            return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# FundingRateRepository
# ---------------------------------------------------------------------------

class FundingRateRepository:
    """Repository for perp_funding_rates table."""

    _TABLE = "perp_funding_rates"

    def upsert_batch(
        self,
        session_factory: sessionmaker[Session],
        rates: Sequence[NormalizedFundingRate],
    ) -> int:
        if not rates:
            return 0

        cols = (
            "venue, symbol, instrument_code, funding_time_ts_ms, "
            "funding_time_dt_utc, funding_rate, mark_price, "
            "source, ingested_at_utc"
        )

        with session_scope(session_factory) as session:
            dialect = _dialect(session)

            if dialect == "sqlite":
                sql = text(
                    f"INSERT OR REPLACE INTO {self._TABLE} ({cols}) VALUES ("
                    ":venue, :symbol, :instrument_code, :funding_time_ts_ms, "
                    ":funding_time_dt_utc, :funding_rate, :mark_price, "
                    ":source, :ingested_at_utc)"
                )
            else:
                update_cols = (
                    "funding_time_dt_utc=VALUES(funding_time_dt_utc), "
                    "funding_rate=VALUES(funding_rate), "
                    "mark_price=VALUES(mark_price), "
                    "source=VALUES(source), ingested_at_utc=VALUES(ingested_at_utc)"
                )
                sql = text(
                    f"INSERT INTO {self._TABLE} ({cols}) VALUES ("
                    ":venue, :symbol, :instrument_code, :funding_time_ts_ms, "
                    ":funding_time_dt_utc, :funding_rate, :mark_price, "
                    ":source, :ingested_at_utc) "
                    f"ON DUPLICATE KEY UPDATE {update_cols}"
                )

            now = _now_utc()
            params = [
                {
                    "venue": r.venue,
                    "symbol": r.symbol,
                    "instrument_code": r.instrument_code,
                    "funding_time_ts_ms": r.funding_time_ts_ms,
                    "funding_time_dt_utc": r.funding_time_dt_utc,
                    "funding_rate": r.funding_rate,
                    "mark_price": r.mark_price,
                    "source": r.source,
                    "ingested_at_utc": now,
                }
                for r in rates
            ]
            params = _coerce_params(params, dialect)
            result = session.execute(sql, params)
            return result.rowcount

    def query_range(
        self,
        session_factory: sessionmaker[Session],
        symbol: str,
        start_ts_ms: int,
        end_ts_ms: int,
    ) -> list[dict[str, Any]]:
        sql = text(
            f"SELECT * FROM {self._TABLE} "
            "WHERE symbol = :symbol "
            "AND funding_time_ts_ms >= :start_ts_ms "
            "AND funding_time_ts_ms <= :end_ts_ms "
            "ORDER BY funding_time_ts_ms ASC"
        )
        with session_scope(session_factory) as session:
            rows = session.execute(sql, {
                "symbol": symbol,
                "start_ts_ms": start_ts_ms,
                "end_ts_ms": end_ts_ms,
            }).mappings().all()
            return [dict(r) for r in rows]

    def query_latest(
        self,
        session_factory: sessionmaker[Session],
        symbol: str,
        limit: int = 1,
    ) -> list[dict[str, Any]]:
        sql = text(
            f"SELECT * FROM {self._TABLE} "
            "WHERE symbol = :symbol "
            "ORDER BY funding_time_ts_ms DESC LIMIT :lim"
        )
        with session_scope(session_factory) as session:
            rows = session.execute(sql, {
                "symbol": symbol,
                "lim": limit,
            }).mappings().all()
            return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# OpenInterestRepository
# ---------------------------------------------------------------------------

class OpenInterestRepository:
    """Repository for perp_open_interest table."""

    _TABLE = "perp_open_interest"

    def upsert_batch(
        self,
        session_factory: sessionmaker[Session],
        records: Sequence[NormalizedOpenInterest],
    ) -> int:
        if not records:
            return 0

        cols = (
            "venue, symbol, instrument_code, event_ts_ms, event_dt_utc, "
            "open_interest, open_interest_value, source, ingested_at_utc"
        )

        with session_scope(session_factory) as session:
            dialect = _dialect(session)

            if dialect == "sqlite":
                sql = text(
                    f"INSERT OR REPLACE INTO {self._TABLE} ({cols}) VALUES ("
                    ":venue, :symbol, :instrument_code, :event_ts_ms, :event_dt_utc, "
                    ":open_interest, :open_interest_value, :source, :ingested_at_utc)"
                )
            else:
                update_cols = (
                    "event_dt_utc=VALUES(event_dt_utc), "
                    "open_interest=VALUES(open_interest), "
                    "open_interest_value=VALUES(open_interest_value), "
                    "source=VALUES(source), ingested_at_utc=VALUES(ingested_at_utc)"
                )
                sql = text(
                    f"INSERT INTO {self._TABLE} ({cols}) VALUES ("
                    ":venue, :symbol, :instrument_code, :event_ts_ms, :event_dt_utc, "
                    ":open_interest, :open_interest_value, :source, :ingested_at_utc) "
                    f"ON DUPLICATE KEY UPDATE {update_cols}"
                )

            now = _now_utc()
            params = [
                {
                    "venue": r.venue,
                    "symbol": r.symbol,
                    "instrument_code": r.instrument_code,
                    "event_ts_ms": r.event_ts_ms,
                    "event_dt_utc": r.event_dt_utc,
                    "open_interest": r.open_interest,
                    "open_interest_value": r.open_interest_value,
                    "source": r.source,
                    "ingested_at_utc": now,
                }
                for r in records
            ]
            params = _coerce_params(params, dialect)
            result = session.execute(sql, params)
            return result.rowcount

    def query_range(
        self,
        session_factory: sessionmaker[Session],
        symbol: str,
        start_ts_ms: int,
        end_ts_ms: int,
    ) -> list[dict[str, Any]]:
        sql = text(
            f"SELECT * FROM {self._TABLE} "
            "WHERE symbol = :symbol "
            "AND event_ts_ms >= :start_ts_ms AND event_ts_ms <= :end_ts_ms "
            "ORDER BY event_ts_ms ASC"
        )
        with session_scope(session_factory) as session:
            rows = session.execute(sql, {
                "symbol": symbol,
                "start_ts_ms": start_ts_ms,
                "end_ts_ms": end_ts_ms,
            }).mappings().all()
            return [dict(r) for r in rows]

    def query_latest(
        self,
        session_factory: sessionmaker[Session],
        symbol: str,
        limit: int = 1,
    ) -> list[dict[str, Any]]:
        sql = text(
            f"SELECT * FROM {self._TABLE} "
            "WHERE symbol = :symbol "
            "ORDER BY event_ts_ms DESC LIMIT :lim"
        )
        with session_scope(session_factory) as session:
            rows = session.execute(sql, {
                "symbol": symbol,
                "lim": limit,
            }).mappings().all()
            return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# MarkPriceRepository
# ---------------------------------------------------------------------------

class MarkPriceRepository:
    """Repository for perp_mark_prices table."""

    _TABLE = "perp_mark_prices"

    def upsert_batch(
        self,
        session_factory: sessionmaker[Session],
        records: Sequence[NormalizedMarkPrice],
    ) -> int:
        if not records:
            return 0

        cols = (
            "venue, symbol, instrument_code, event_ts_ms, event_dt_utc, "
            "mark_price, funding_rate, next_funding_time_ts_ms, "
            "source, ingested_at_utc"
        )

        with session_scope(session_factory) as session:
            dialect = _dialect(session)

            if dialect == "sqlite":
                sql = text(
                    f"INSERT OR REPLACE INTO {self._TABLE} ({cols}) VALUES ("
                    ":venue, :symbol, :instrument_code, :event_ts_ms, :event_dt_utc, "
                    ":mark_price, :funding_rate, :next_funding_time_ts_ms, "
                    ":source, :ingested_at_utc)"
                )
            else:
                update_cols = (
                    "event_dt_utc=VALUES(event_dt_utc), "
                    "mark_price=VALUES(mark_price), "
                    "funding_rate=VALUES(funding_rate), "
                    "next_funding_time_ts_ms=VALUES(next_funding_time_ts_ms), "
                    "source=VALUES(source), ingested_at_utc=VALUES(ingested_at_utc)"
                )
                sql = text(
                    f"INSERT INTO {self._TABLE} ({cols}) VALUES ("
                    ":venue, :symbol, :instrument_code, :event_ts_ms, :event_dt_utc, "
                    ":mark_price, :funding_rate, :next_funding_time_ts_ms, "
                    ":source, :ingested_at_utc) "
                    f"ON DUPLICATE KEY UPDATE {update_cols}"
                )

            now = _now_utc()
            params = [
                {
                    "venue": r.venue,
                    "symbol": r.symbol,
                    "instrument_code": r.instrument_code,
                    "event_ts_ms": r.event_ts_ms,
                    "event_dt_utc": r.event_dt_utc,
                    "mark_price": r.mark_price,
                    "funding_rate": r.funding_rate,
                    "next_funding_time_ts_ms": r.next_funding_time_ts_ms,
                    "source": r.source,
                    "ingested_at_utc": now,
                }
                for r in records
            ]
            params = _coerce_params(params, dialect)
            result = session.execute(sql, params)
            return result.rowcount

    def query_range(
        self,
        session_factory: sessionmaker[Session],
        symbol: str,
        start_ts_ms: int,
        end_ts_ms: int,
    ) -> list[dict[str, Any]]:
        sql = text(
            f"SELECT * FROM {self._TABLE} "
            "WHERE symbol = :symbol "
            "AND event_ts_ms >= :start_ts_ms AND event_ts_ms <= :end_ts_ms "
            "ORDER BY event_ts_ms ASC"
        )
        with session_scope(session_factory) as session:
            rows = session.execute(sql, {
                "symbol": symbol,
                "start_ts_ms": start_ts_ms,
                "end_ts_ms": end_ts_ms,
            }).mappings().all()
            return [dict(r) for r in rows]

    def query_latest(
        self,
        session_factory: sessionmaker[Session],
        symbol: str,
        limit: int = 1,
    ) -> list[dict[str, Any]]:
        sql = text(
            f"SELECT * FROM {self._TABLE} "
            "WHERE symbol = :symbol "
            "ORDER BY event_ts_ms DESC LIMIT :lim"
        )
        with session_scope(session_factory) as session:
            rows = session.execute(sql, {
                "symbol": symbol,
                "lim": limit,
            }).mappings().all()
            return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# IndexPriceRepository
# ---------------------------------------------------------------------------

class IndexPriceRepository:
    """Repository for perp_index_prices table."""

    _TABLE = "perp_index_prices"

    def upsert_batch(
        self,
        session_factory: sessionmaker[Session],
        records: Sequence[NormalizedIndexPrice],
    ) -> int:
        if not records:
            return 0

        cols = (
            "venue, symbol, instrument_code, event_ts_ms, event_dt_utc, "
            "index_price, source, ingested_at_utc"
        )

        with session_scope(session_factory) as session:
            dialect = _dialect(session)

            if dialect == "sqlite":
                sql = text(
                    f"INSERT OR REPLACE INTO {self._TABLE} ({cols}) VALUES ("
                    ":venue, :symbol, :instrument_code, :event_ts_ms, :event_dt_utc, "
                    ":index_price, :source, :ingested_at_utc)"
                )
            else:
                update_cols = (
                    "event_dt_utc=VALUES(event_dt_utc), "
                    "index_price=VALUES(index_price), "
                    "source=VALUES(source), ingested_at_utc=VALUES(ingested_at_utc)"
                )
                sql = text(
                    f"INSERT INTO {self._TABLE} ({cols}) VALUES ("
                    ":venue, :symbol, :instrument_code, :event_ts_ms, :event_dt_utc, "
                    ":index_price, :source, :ingested_at_utc) "
                    f"ON DUPLICATE KEY UPDATE {update_cols}"
                )

            now = _now_utc()
            params = [
                {
                    "venue": r.venue,
                    "symbol": r.symbol,
                    "instrument_code": r.instrument_code,
                    "event_ts_ms": r.event_ts_ms,
                    "event_dt_utc": r.event_dt_utc,
                    "index_price": r.index_price,
                    "source": r.source,
                    "ingested_at_utc": now,
                }
                for r in records
            ]
            params = _coerce_params(params, dialect)
            result = session.execute(sql, params)
            return result.rowcount

    def query_range(
        self,
        session_factory: sessionmaker[Session],
        symbol: str,
        start_ts_ms: int,
        end_ts_ms: int,
    ) -> list[dict[str, Any]]:
        sql = text(
            f"SELECT * FROM {self._TABLE} "
            "WHERE symbol = :symbol "
            "AND event_ts_ms >= :start_ts_ms AND event_ts_ms <= :end_ts_ms "
            "ORDER BY event_ts_ms ASC"
        )
        with session_scope(session_factory) as session:
            rows = session.execute(sql, {
                "symbol": symbol,
                "start_ts_ms": start_ts_ms,
                "end_ts_ms": end_ts_ms,
            }).mappings().all()
            return [dict(r) for r in rows]

    def query_latest(
        self,
        session_factory: sessionmaker[Session],
        symbol: str,
        limit: int = 1,
    ) -> list[dict[str, Any]]:
        sql = text(
            f"SELECT * FROM {self._TABLE} "
            "WHERE symbol = :symbol "
            "ORDER BY event_ts_ms DESC LIMIT :lim"
        )
        with session_scope(session_factory) as session:
            rows = session.execute(sql, {
                "symbol": symbol,
                "lim": limit,
            }).mappings().all()
            return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# DepthSnapshotRepository
# ---------------------------------------------------------------------------

class DepthSnapshotRepository:
    """Repository for spot_depth_snapshots / perp_depth_snapshots tables."""

    def upsert_batch(
        self,
        session_factory: sessionmaker[Session],
        table_name: str,
        snapshots: Sequence[NormalizedDepthSnapshot],
    ) -> int:
        if not snapshots:
            return 0

        cols = (
            "venue, symbol, instrument_code, event_ts_ms, event_dt_utc, "
            "best_bid_price, best_bid_qty, best_ask_price, best_ask_qty, "
            "mid_price, spread_abs, spread_bps, depth_levels, "
            "bid_depth_json, ask_depth_json, source, ingested_at_utc"
        )

        with session_scope(session_factory) as session:
            dialect = _dialect(session)

            if dialect == "sqlite":
                sql = text(
                    f"INSERT OR REPLACE INTO {table_name} ({cols}) VALUES ("
                    ":venue, :symbol, :instrument_code, :event_ts_ms, :event_dt_utc, "
                    ":best_bid_price, :best_bid_qty, :best_ask_price, :best_ask_qty, "
                    ":mid_price, :spread_abs, :spread_bps, :depth_levels, "
                    ":bid_depth_json, :ask_depth_json, :source, :ingested_at_utc)"
                )
            else:
                update_cols = (
                    "event_dt_utc=VALUES(event_dt_utc), "
                    "best_bid_price=VALUES(best_bid_price), "
                    "best_bid_qty=VALUES(best_bid_qty), "
                    "best_ask_price=VALUES(best_ask_price), "
                    "best_ask_qty=VALUES(best_ask_qty), "
                    "mid_price=VALUES(mid_price), "
                    "spread_abs=VALUES(spread_abs), "
                    "spread_bps=VALUES(spread_bps), "
                    "depth_levels=VALUES(depth_levels), "
                    "bid_depth_json=VALUES(bid_depth_json), "
                    "ask_depth_json=VALUES(ask_depth_json), "
                    "source=VALUES(source), ingested_at_utc=VALUES(ingested_at_utc)"
                )
                sql = text(
                    f"INSERT INTO {table_name} ({cols}) VALUES ("
                    ":venue, :symbol, :instrument_code, :event_ts_ms, :event_dt_utc, "
                    ":best_bid_price, :best_bid_qty, :best_ask_price, :best_ask_qty, "
                    ":mid_price, :spread_abs, :spread_bps, :depth_levels, "
                    ":bid_depth_json, :ask_depth_json, :source, :ingested_at_utc) "
                    f"ON DUPLICATE KEY UPDATE {update_cols}"
                )

            now = _now_utc()
            params = [
                {
                    "venue": s.venue,
                    "symbol": s.symbol,
                    "instrument_code": s.instrument_code,
                    "event_ts_ms": s.event_ts_ms,
                    "event_dt_utc": s.event_dt_utc,
                    "best_bid_price": s.best_bid_price,
                    "best_bid_qty": s.best_bid_qty,
                    "best_ask_price": s.best_ask_price,
                    "best_ask_qty": s.best_ask_qty,
                    "mid_price": s.mid_price,
                    "spread_abs": s.spread_abs,
                    "spread_bps": s.spread_bps,
                    "depth_levels": s.depth_levels,
                    "bid_depth_json": json.dumps(s.bid_depth_json),
                    "ask_depth_json": json.dumps(s.ask_depth_json),
                    "source": s.source,
                    "ingested_at_utc": now,
                }
                for s in snapshots
            ]
            params = _coerce_params(params, dialect)
            result = session.execute(sql, params)
            return result.rowcount

    def query_range(
        self,
        session_factory: sessionmaker[Session],
        table_name: str,
        symbol: str,
        start_ts_ms: int,
        end_ts_ms: int,
    ) -> list[dict[str, Any]]:
        sql = text(
            f"SELECT * FROM {table_name} "
            "WHERE symbol = :symbol "
            "AND event_ts_ms >= :start_ts_ms AND event_ts_ms <= :end_ts_ms "
            "ORDER BY event_ts_ms ASC"
        )
        with session_scope(session_factory) as session:
            rows = session.execute(sql, {
                "symbol": symbol,
                "start_ts_ms": start_ts_ms,
                "end_ts_ms": end_ts_ms,
            }).mappings().all()
            return [dict(r) for r in rows]

    def query_latest(
        self,
        session_factory: sessionmaker[Session],
        table_name: str,
        symbol: str,
        limit: int = 1,
    ) -> list[dict[str, Any]]:
        sql = text(
            f"SELECT * FROM {table_name} "
            "WHERE symbol = :symbol "
            "ORDER BY event_ts_ms DESC LIMIT :lim"
        )
        with session_scope(session_factory) as session:
            rows = session.execute(sql, {
                "symbol": symbol,
                "lim": limit,
            }).mappings().all()
            return [dict(r) for r in rows]
