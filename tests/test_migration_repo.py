"""Tests for SchemaMigrationRepository using SQLite in-memory."""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from YM_data_collection.persistence.repositories.migration_repo import (
    MigrationRecord,
    SchemaMigrationRepository,
)

# ---------------------------------------------------------------------------
# SQLite DDL (adapted from the MySQL schema)
# ---------------------------------------------------------------------------

_CREATE_MIGRATIONS_TABLE = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    version         VARCHAR(32)   NOT NULL,
    name            VARCHAR(128)  NOT NULL,
    checksum        VARCHAR(128)  NOT NULL,
    executed_at_utc DATETIME      NOT NULL,
    status          VARCHAR(32)   NOT NULL,
    UNIQUE(version)
);
"""


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def session_factory():
    """Create an in-memory SQLite session factory with the schema_migrations table."""
    engine = create_engine("sqlite:///:memory:")
    with engine.connect() as conn:
        conn.execute(text(_CREATE_MIGRATIONS_TABLE))
        conn.commit()
    factory = sessionmaker(bind=engine, autoflush=False, autocommit=False,
                           expire_on_commit=False)
    return factory


@pytest.fixture()
def repo(session_factory):
    return SchemaMigrationRepository(session_factory)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestSchemaMigrationRepository:
    """SchemaMigrationRepository test suite."""

    def test_record_migration_inserts_row(self, repo: SchemaMigrationRepository, session_factory):
        repo.record_migration("001", "create_instruments", "abc123", "success")

        with session_factory() as session:
            count = session.execute(text("SELECT COUNT(*) FROM schema_migrations")).scalar()
        assert count == 1

    def test_get_by_version_found(self, repo: SchemaMigrationRepository):
        repo.record_migration("001", "create_instruments", "abc123", "success")

        result = repo.get_by_version("001")
        assert result is not None
        assert result.version == "001"
        assert result.name == "create_instruments"
        assert result.checksum == "abc123"
        assert result.status == "success"

    def test_get_by_version_not_found(self, repo: SchemaMigrationRepository):
        result = repo.get_by_version("999")
        assert result is None

    def test_list_all_empty(self, repo: SchemaMigrationRepository):
        assert repo.list_all() == []

    def test_list_all_ordered(self, repo: SchemaMigrationRepository):
        repo.record_migration("003", "add_foo", "ccc", "success")
        repo.record_migration("001", "create_instruments", "aaa", "success")
        repo.record_migration("002", "add_bar", "bbb", "success")

        records = repo.list_all()
        assert len(records) == 3
        assert records[0].version == "001"
        assert records[1].version == "002"
        assert records[2].version == "003"

    def test_migration_record_fields(self, repo: SchemaMigrationRepository):
        repo.record_migration("001", "create_instruments", "abc123", "success")

        result = repo.get_by_version("001")
        assert isinstance(result, MigrationRecord)
        assert result.id == 1
        assert result.version == "001"
        assert result.name == "create_instruments"
        assert result.checksum == "abc123"
        assert result.status == "success"
        assert result.executed_at_utc is not None

    def test_record_failed_migration(self, repo: SchemaMigrationRepository):
        repo.record_migration("002", "broken", "def456", "failed")

        result = repo.get_by_version("002")
        assert result is not None
        assert result.status == "failed"
