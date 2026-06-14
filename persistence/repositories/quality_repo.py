"""Repository for data_quality_issues table operations."""

from __future__ import annotations

from typing import List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

from YM_data_collection.domain.models import DataQualityIssue
from YM_data_collection.persistence.datetime_utils import normalize_sql_params, utc_now_sql
from YM_data_collection.persistence.mysql import session_scope


_ISSUE_FIELDS = (
    "venue, market_type, symbol, data_type, interval_code, "
    "issue_type, severity, detected_at_utc, start_ts_ms, end_ts_ms, "
    "description, status, resolution_note, resolved_at_utc"
)

_INSERT_SQL = text(
    f"INSERT INTO data_quality_issues ({_ISSUE_FIELDS}) "
    f"VALUES (:venue, :market_type, :symbol, :data_type, :interval_code, "
    f":issue_type, :severity, :detected_at_utc, :start_ts_ms, :end_ts_ms, "
    f":description, :status, :resolution_note, :resolved_at_utc)"
)

_LIST_BY_SYMBOL_SQL = text(
    f"SELECT id, {_ISSUE_FIELDS} FROM data_quality_issues "
    f"WHERE symbol = :symbol AND data_type = :data_type "
    f"ORDER BY detected_at_utc DESC"
)

_LIST_BY_SYMBOL_ONLY_SQL = text(
    f"SELECT id, {_ISSUE_FIELDS} FROM data_quality_issues "
    f"WHERE symbol = :symbol "
    f"ORDER BY detected_at_utc DESC"
)

_LIST_BY_STATUS_SQL = text(
    f"SELECT id, {_ISSUE_FIELDS} FROM data_quality_issues "
    f"WHERE status = :status "
    f"ORDER BY detected_at_utc DESC"
)

_RESOLVE_SQL = text(
    "UPDATE data_quality_issues SET "
    "status = 'resolved', "
    "resolution_note = :resolution_note, "
    "resolved_at_utc = :resolved_at_utc "
    "WHERE id = :id"
)


def _row_to_issue(row: tuple) -> DataQualityIssue:
    """Map a result row to a DataQualityIssue domain object."""
    return DataQualityIssue(
        venue=row[1],
        market_type=row[2],
        symbol=row[3],
        data_type=row[4],
        interval_code=row[5],
        issue_type=row[6],
        severity=row[7],
        detected_at_utc=row[8],
        start_ts_ms=row[9],
        end_ts_ms=row[10],
        description=row[11],
        status=row[12],
        resolution_note=row[13],
    )


class QualityIssueRepository:
    """CRUD operations for data_quality_issues using raw SQL."""

    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        self._session_factory = session_factory

    def insert(self, issue: DataQualityIssue) -> int:
        """Insert a new quality issue and return its auto-generated id."""
        params = {
            "venue": issue.venue,
            "market_type": issue.market_type,
            "symbol": issue.symbol,
            "data_type": issue.data_type,
            "interval_code": issue.interval_code,
            "issue_type": issue.issue_type,
            "severity": issue.severity,
            "detected_at_utc": issue.detected_at_utc,
            "start_ts_ms": issue.start_ts_ms,
            "end_ts_ms": issue.end_ts_ms,
            "description": issue.description,
            "status": issue.status,
            "resolution_note": issue.resolution_note,
            "resolved_at_utc": None,
        }
        with session_scope(self._session_factory) as session:
            result = session.execute(_INSERT_SQL, normalize_sql_params(params))
            return result.lastrowid

    def list_by_symbol(
        self,
        symbol: str,
        data_type: Optional[str] = None,
    ) -> List[DataQualityIssue]:
        """Return quality issues for a symbol, optionally filtered by data_type."""
        with session_scope(self._session_factory) as session:
            if data_type is not None:
                rows = session.execute(
                    _LIST_BY_SYMBOL_SQL,
                    {"symbol": symbol, "data_type": data_type},
                ).fetchall()
            else:
                rows = session.execute(
                    _LIST_BY_SYMBOL_ONLY_SQL,
                    {"symbol": symbol},
                ).fetchall()
            return [_row_to_issue(r) for r in rows]

    def list_by_status(self, status: str) -> List[DataQualityIssue]:
        """Return all issues with the given status."""
        with session_scope(self._session_factory) as session:
            rows = session.execute(
                _LIST_BY_STATUS_SQL, {"status": status}
            ).fetchall()
            return [_row_to_issue(r) for r in rows]

    def resolve(self, id: int, resolution_note: str) -> None:
        """Mark an issue as resolved with a resolution note."""
        with session_scope(self._session_factory) as session:
            session.execute(
                _RESOLVE_SQL,
                {"id": id, "resolution_note": resolution_note, "resolved_at_utc": utc_now_sql()},
            )
