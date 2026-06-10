"""Structural checks for formal MySQL migration SQL files."""

from __future__ import annotations

from pathlib import Path

from YM_data_collection.persistence.migrations import load_migration_files
from YM_data_collection.utils.constants import DEFAULT_MIGRATIONS_DIR


def read_sql(filename: str) -> str:
    """Read one formal migration SQL file."""

    return (Path(DEFAULT_MIGRATIONS_DIR) / filename).read_text(encoding="utf-8")


def test_formal_migration_versions_are_complete() -> None:
    migrations = load_migration_files(DEFAULT_MIGRATIONS_DIR)
    assert [migration.version for migration in migrations] == ["000", "001", "002", "003", "004"]


def test_001_contains_metadata_and_state_tables() -> None:
    sql = read_sql("001_init_metadata_and_state_tables.sql")
    for table_name in [
        "CREATE TABLE IF NOT EXISTS `instruments`",
        "CREATE TABLE IF NOT EXISTS `ingest_checkpoints`",
        "CREATE TABLE IF NOT EXISTS `data_quality_issues`",
        "CREATE TABLE IF NOT EXISTS `file_manifests`",
    ]:
        assert table_name in sql
    assert "UNIQUE KEY `uk_checkpoints_scope`" in sql
    assert "`interval_code_key` VARCHAR(16) GENERATED ALWAYS AS" in sql


def test_002_contains_kline_tables_and_indexes() -> None:
    sql = read_sql("002_create_kline_tables.sql")
    assert "CREATE TABLE IF NOT EXISTS `spot_klines`" in sql
    assert "CREATE TABLE IF NOT EXISTS `perp_klines`" in sql
    assert "UNIQUE KEY `uk_spot_klines_scope`" in sql
    assert "UNIQUE KEY `uk_perp_klines_scope`" in sql
    assert "KEY `idx_spot_klines_symbol_interval_time`" in sql
    assert "KEY `idx_perp_klines_symbol_interval_time`" in sql


def test_003_contains_derivatives_tables_and_indexes() -> None:
    sql = read_sql("003_create_perp_derivatives_tables.sql")
    for table_name in [
        "CREATE TABLE IF NOT EXISTS `perp_funding_rates`",
        "CREATE TABLE IF NOT EXISTS `perp_open_interest`",
        "CREATE TABLE IF NOT EXISTS `perp_mark_prices`",
        "CREATE TABLE IF NOT EXISTS `perp_index_prices`",
    ]:
        assert table_name in sql
    assert "UNIQUE KEY `uk_perp_funding_scope`" in sql
    assert "UNIQUE KEY `uk_perp_oi_scope`" in sql
    assert "UNIQUE KEY `uk_perp_mark_scope`" in sql
    assert "UNIQUE KEY `uk_perp_index_scope`" in sql


def test_004_contains_depth_tables_and_indexes() -> None:
    sql = read_sql("004_create_depth_snapshot_tables.sql")
    assert "CREATE TABLE IF NOT EXISTS `spot_depth_snapshots`" in sql
    assert "CREATE TABLE IF NOT EXISTS `perp_depth_snapshots`" in sql
    assert "`bid_depth_json` JSON NOT NULL" in sql
    assert "`ask_depth_json` JSON NOT NULL" in sql
    assert "UNIQUE KEY `uk_spot_depth_scope`" in sql
    assert "UNIQUE KEY `uk_perp_depth_scope`" in sql
