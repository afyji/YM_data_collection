"""Datetime serialization helpers for SQL-bound parameters.

Python 3.12 deprecates sqlite3's implicit datetime adapter.  The project uses
raw SQL with SQLAlchemy against both MySQL and SQLite test databases, so write
UTC datetimes as ISO-8601 strings before binding parameters instead of relying
on DB-API adapters.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def utc_now_sql() -> str:
    """Return the current UTC timestamp as a naive ISO string for SQL writes."""

    return datetime.now(timezone.utc).replace(tzinfo=None).isoformat()


def to_sql_datetime(value: datetime | None) -> str | None:
    """Convert a datetime to an ISO string suitable for SQL bind parameters.

    Naive datetimes are treated as already-UTC.  Aware datetimes are normalized
    to UTC and stored without tzinfo to match the existing schema convention.
    """

    if value is None:
        return None
    if value.tzinfo is not None:
        value = value.astimezone(timezone.utc).replace(tzinfo=None)
    return value.isoformat()


def normalize_sql_params(params: dict[str, Any]) -> dict[str, Any]:
    """Return a copy of params with datetime values converted for SQL writes."""

    return {
        key: to_sql_datetime(value) if isinstance(value, datetime) else value
        for key, value in params.items()
    }
