"""Repository for ingest_checkpoints table operations."""

from __future__ import annotations

from typing import List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

from YM_data_collection.domain.models import IngestCheckpoint
from YM_data_collection.persistence.datetime_utils import normalize_sql_params, utc_now_sql
from YM_data_collection.persistence.mysql import session_scope


_CHECKPOINT_FIELDS = (
    "venue, market_type, symbol, data_type, interval_code, "
    "last_event_ts_ms, last_event_dt_utc, last_trade_id, "
    "last_kline_open_ts_ms, status, last_success_at_utc, "
    "last_error_message, updated_at_utc"
)

_INSERT_SQL = text(
    f"INSERT INTO ingest_checkpoints ({_CHECKPOINT_FIELDS}) "
    f"VALUES (:venue, :market_type, :symbol, :data_type, :interval_code, "
    f":last_event_ts_ms, :last_event_dt_utc, :last_trade_id, "
    f":last_kline_open_ts_ms, :status, :last_success_at_utc, "
    f":last_error_message, :updated_at_utc)"
)

_UPDATE_SQL = text(
    f"UPDATE ingest_checkpoints SET "
    f"last_event_ts_ms = :last_event_ts_ms, "
    f"last_event_dt_utc = :last_event_dt_utc, "
    f"last_trade_id = :last_trade_id, "
    f"last_kline_open_ts_ms = :last_kline_open_ts_ms, "
    f"status = :status, "
    f"last_success_at_utc = :last_success_at_utc, "
    f"last_error_message = :last_error_message, "
    f"updated_at_utc = :updated_at_utc "
    f"WHERE venue = :venue AND market_type = :market_type "
    f"AND symbol = :symbol AND data_type = :data_type "
    f"AND (interval_code = :interval_code OR "
    f"(interval_code IS NULL AND :interval_code IS NULL))"
)

_SELECT_SQL = text(
    f"SELECT id, {_CHECKPOINT_FIELDS} FROM ingest_checkpoints "
    f"WHERE venue = :venue AND market_type = :market_type "
    f"AND symbol = :symbol AND data_type = :data_type "
    f"AND (interval_code = :interval_code OR "
    f"(interval_code IS NULL AND :interval_code IS NULL))"
)

_LIST_BY_STATUS_SQL = text(
    f"SELECT id, {_CHECKPOINT_FIELDS} FROM ingest_checkpoints "
    f"WHERE status = :status"
)

_LIST_ALL_SQL = text(
    f"SELECT id, {_CHECKPOINT_FIELDS} FROM ingest_checkpoints "
    f"ORDER BY venue, market_type, symbol, data_type"
)

_EXISTS_SQL = text(
    "SELECT 1 FROM ingest_checkpoints "
    "WHERE venue = :venue AND market_type = :market_type "
    "AND symbol = :symbol AND data_type = :data_type "
    "AND (interval_code = :interval_code OR "
    "(interval_code IS NULL AND :interval_code IS NULL))"
)


def _row_to_checkpoint(row: tuple) -> IngestCheckpoint:
    """Map a result row to an IngestCheckpoint domain object."""
    return IngestCheckpoint(
        venue=row[1],
        market_type=row[2],
        symbol=row[3],
        data_type=row[4],
        interval_code=row[5],
        last_event_ts_ms=row[6],
        last_event_dt_utc=row[7],
        last_trade_id=row[8],
        last_kline_open_ts_ms=row[9],
        status=row[10],
        last_success_at_utc=row[11],
        last_error_message=row[12],
    )


class CheckpointRepository:
    """CRUD operations for ingest_checkpoints using raw SQL."""

    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        self._session_factory = session_factory

    def upsert(self, checkpoint: IngestCheckpoint) -> None:
        """Insert or update a checkpoint row (idempotent).

        Uses SELECT-then-INSERT/UPDATE so the same SQL works on both
        MySQL and SQLite.  The operation is atomic within the session
        transaction.
        """
        params = {
            "venue": checkpoint.venue,
            "market_type": checkpoint.market_type,
            "symbol": checkpoint.symbol,
            "data_type": checkpoint.data_type,
            "interval_code": checkpoint.interval_code,
            "last_event_ts_ms": checkpoint.last_event_ts_ms,
            "last_event_dt_utc": checkpoint.last_event_dt_utc,
            "last_trade_id": checkpoint.last_trade_id,
            "last_kline_open_ts_ms": checkpoint.last_kline_open_ts_ms,
            "status": checkpoint.status,
            "last_success_at_utc": checkpoint.last_success_at_utc,
            "last_error_message": checkpoint.last_error_message,
            "updated_at_utc": utc_now_sql(),
        }
        params = normalize_sql_params(params)
        with session_scope(self._session_factory) as session:
            exists = session.execute(_EXISTS_SQL, params).fetchone()
            if exists:
                session.execute(_UPDATE_SQL, params)
            else:
                session.execute(_INSERT_SQL, params)

    def get(
        self,
        venue: str,
        market_type: str,
        symbol: str,
        data_type: str,
        interval_code: Optional[str] = None,
    ) -> Optional[IngestCheckpoint]:
        """Fetch a single checkpoint by its natural key."""
        params = {
            "venue": venue,
            "market_type": market_type,
            "symbol": symbol,
            "data_type": data_type,
            "interval_code": interval_code,
        }
        with session_scope(self._session_factory) as session:
            row = session.execute(_SELECT_SQL, params).fetchone()
            if row is None:
                return None
            return _row_to_checkpoint(row)

    def list_by_status(self, status: str) -> List[IngestCheckpoint]:
        """Return all checkpoints with the given status."""
        with session_scope(self._session_factory) as session:
            rows = session.execute(_LIST_BY_STATUS_SQL, {"status": status}).fetchall()
            return [_row_to_checkpoint(r) for r in rows]

    def list_all(self) -> List[IngestCheckpoint]:
        """Return all checkpoints ordered by natural key."""
        with session_scope(self._session_factory) as session:
            rows = session.execute(_LIST_ALL_SQL).fetchall()
            return [_row_to_checkpoint(r) for r in rows]
