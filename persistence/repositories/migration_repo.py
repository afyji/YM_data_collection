"""Repository for the schema_migrations table using raw SQL."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

from YM_data_collection.persistence.datetime_utils import utc_now_sql
from YM_data_collection.persistence.mysql import session_scope


@dataclass
class MigrationRecord:
    """Lightweight DTO for a schema_migrations row."""

    id: int
    version: str
    name: str
    checksum: str
    executed_at_utc: datetime
    status: str


class SchemaMigrationRepository:
    """CRUD operations on the ``schema_migrations`` table.

    Uses raw SQL via :func:`sqlalchemy.text` – no ORM mapped classes.
    """

    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        self._session_factory = session_factory

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def record_migration(
        self,
        version: str,
        name: str,
        checksum: str,
        status: str,
    ) -> None:
        """Insert a migration execution record."""
        now = utc_now_sql()

        with session_scope(self._session_factory) as session:
            session.execute(
                text("""
                    INSERT INTO schema_migrations (
                        version, name, checksum, executed_at_utc, status
                    ) VALUES (
                        :version, :name, :checksum, :executed_at_utc, :status
                    )
                """),
                {
                    "version": version,
                    "name": name,
                    "checksum": checksum,
                    "executed_at_utc": now,
                    "status": status,
                },
            )

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def get_by_version(self, version: str) -> Optional[MigrationRecord]:
        """Return a single migration record by *version*, or *None*."""
        with session_scope(self._session_factory) as session:
            row = session.execute(
                text("SELECT * FROM schema_migrations WHERE version = :version"),
                {"version": version},
            ).mappings().first()

            if row is None:
                return None
            return self._row_to_record(row)

    def list_all(self) -> List[MigrationRecord]:
        """Return every migration record ordered by version."""
        with session_scope(self._session_factory) as session:
            rows = session.execute(
                text("SELECT * FROM schema_migrations ORDER BY version")
            ).mappings().all()
            return [self._row_to_record(r) for r in rows]

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _row_to_record(row) -> MigrationRecord:
        """Map a DB row mapping to a :class:`MigrationRecord`."""
        return MigrationRecord(
            id=row["id"],
            version=row["version"],
            name=row["name"],
            checksum=row["checksum"],
            executed_at_utc=row["executed_at_utc"],
            status=row["status"],
        )
