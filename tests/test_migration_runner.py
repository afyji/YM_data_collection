"""Tests for the SQL-first migration runner."""

from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import create_engine, inspect, text

from YM_data_collection.persistence.migrations import (
    MigrationChecksumError,
    load_migration_files,
    run_migrations,
)


def write_migration(path: Path, filename: str, content: str) -> None:
    """Write one migration file into a temp directory."""

    (path / filename).write_text(content.strip() + "\n", encoding="utf-8")


def build_test_migrations(tmp_path: Path) -> Path:
    """Create a sqlite-compatible migration set for runner tests."""

    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    write_migration(
        migrations_dir,
        "000_schema_migrations.sql",
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            version VARCHAR(32) NOT NULL UNIQUE,
            name VARCHAR(128) NOT NULL,
            checksum VARCHAR(128) NOT NULL,
            executed_at_utc DATETIME NOT NULL,
            status VARCHAR(32) NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_schema_migrations_executed_at
            ON schema_migrations (executed_at_utc);
        CREATE INDEX IF NOT EXISTS idx_schema_migrations_status
            ON schema_migrations (status);
        """,
    )
    write_migration(
        migrations_dir,
        "001_create_sample_table.sql",
        """
        CREATE TABLE IF NOT EXISTS sample_table (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            value TEXT NOT NULL
        );
        """,
    )
    write_migration(
        migrations_dir,
        "002_create_sample_index.sql",
        """
        CREATE INDEX IF NOT EXISTS idx_sample_table_value
            ON sample_table (value);
        """,
    )
    return migrations_dir


def test_load_migration_files_sorted_by_version(tmp_path: Path) -> None:
    migrations_dir = build_test_migrations(tmp_path)
    migrations = load_migration_files(migrations_dir)
    assert [migration.version for migration in migrations] == ["000", "001", "002"]


def test_run_migrations_apply_and_idempotent(tmp_path: Path) -> None:
    migrations_dir = build_test_migrations(tmp_path)
    db_path = tmp_path / "migration_state.db"
    engine = create_engine(f"sqlite+pysqlite:///{db_path}", future=True)

    first_run = run_migrations(engine, migrations_dir, apply=True)
    assert first_run.schema_up_to_date is True
    assert first_run.applied_now == ["000", "001", "002"]

    second_run = run_migrations(engine, migrations_dir, apply=True)
    assert second_run.schema_up_to_date is True
    assert second_run.applied_now == []
    assert second_run.pending_versions == []

    with engine.connect() as connection:
        applied = connection.execute(
            text("SELECT version, status FROM schema_migrations ORDER BY version")
        ).all()
    assert applied == [("000", "success"), ("001", "success"), ("002", "success")]


def test_run_migrations_check_only_reports_pending_without_mutation(tmp_path: Path) -> None:
    migrations_dir = build_test_migrations(tmp_path)
    db_path = tmp_path / "check_only.db"
    engine = create_engine(f"sqlite+pysqlite:///{db_path}", future=True)

    summary = run_migrations(engine, migrations_dir, apply=False)
    assert summary.schema_up_to_date is False
    assert summary.pending_versions == ["000", "001", "002"]

    with engine.connect() as connection:
        assert inspect(connection).has_table("schema_migrations") is False


def test_run_migrations_detects_checksum_mismatch(tmp_path: Path) -> None:
    migrations_dir = build_test_migrations(tmp_path)
    db_path = tmp_path / "checksum.db"
    engine = create_engine(f"sqlite+pysqlite:///{db_path}", future=True)

    run_migrations(engine, migrations_dir, apply=True)
    write_migration(
        migrations_dir,
        "001_create_sample_table.sql",
        """
        CREATE TABLE IF NOT EXISTS sample_table (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            value TEXT NOT NULL,
            updated_value TEXT
        );
        """,
    )

    with pytest.raises(MigrationChecksumError):
        run_migrations(engine, migrations_dir, apply=False)
