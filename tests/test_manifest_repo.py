"""Tests for ManifestRepository using SQLite in-memory."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from YM_data_collection.domain.models import FileManifest
from YM_data_collection.persistence.repositories.manifest_repo import (
    ManifestRepository,
)

# ---------------------------------------------------------------------------
# SQLite-compatible DDL
# ---------------------------------------------------------------------------

_CREATE_TABLE_SQL = text("""
CREATE TABLE IF NOT EXISTS file_manifests (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    dataset_name        VARCHAR(128) NOT NULL,
    venue               VARCHAR(32)  NOT NULL,
    market_type         VARCHAR(16)  NOT NULL,
    symbol              VARCHAR(64)  NOT NULL,
    data_type           VARCHAR(32)  NOT NULL,
    interval_code       VARCHAR(16),
    time_boundary_rule  VARCHAR(64),
    file_format         VARCHAR(16)  NOT NULL,
    file_path           VARCHAR(1024) NOT NULL,
    partition_key       VARCHAR(128),
    start_ts_ms         BIGINT UNSIGNED NOT NULL,
    end_ts_ms           BIGINT UNSIGNED NOT NULL,
    row_count           BIGINT UNSIGNED NOT NULL,
    file_size_bytes     BIGINT UNSIGNED NOT NULL,
    content_hash        VARCHAR(128) NOT NULL,
    version             INTEGER UNSIGNED NOT NULL,
    generated_by        VARCHAR(128) NOT NULL,
    generated_at_utc    DATETIME(3)  NOT NULL,
    status              VARCHAR(32)  NOT NULL
)
""")

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def engine():
    eng = create_engine("sqlite:///:memory:", future=True)
    with eng.connect() as conn:
        conn.execute(_CREATE_TABLE_SQL)
        conn.commit()
    return eng


@pytest.fixture()
def session_factory(engine):
    return sessionmaker(
        bind=engine,
        autoflush=False,
        autocommit=False,
        expire_on_commit=False,
        future=True,
    )


@pytest.fixture()
def repo(session_factory):
    return ManifestRepository(session_factory)


def _make_manifest(**overrides) -> FileManifest:
    defaults = dict(
        dataset_name="binance_spot_kline_1m",
        venue="binance",
        market_type="spot",
        symbol="BTCUSDT",
        data_type="kline",
        interval_code="1m",
        time_boundary_rule="calendar_day",
        file_format="parquet",
        file_path="/data/binance/spot/BTCUSDT/kline/1m/2023-11-14.parquet",
        partition_key="2023-11-14",
        start_ts_ms=1700000000000,
        end_ts_ms=1700086400000,
        row_count=1440,
        file_size_bytes=102400,
        content_hash="sha256:abcdef1234567890",
        version=1,
        generated_by="pipeline-v1",
        generated_at_utc=datetime(2023, 11, 15, 0, 5, 0, tzinfo=timezone.utc),
        status="ready",
    )
    defaults.update(overrides)
    return FileManifest(**defaults)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestManifestRepository:
    def test_insert_returns_id(self, repo):
        mf = _make_manifest()
        row_id = repo.insert(mf)
        assert row_id is not None
        assert row_id > 0

    def test_get_by_path(self, repo):
        path = "/data/binance/spot/BTCUSDT/kline/1m/2023-11-14.parquet"
        repo.insert(_make_manifest(file_path=path))

        result = repo.get_by_path(path)
        assert result is not None
        assert result.file_path == path
        assert result.symbol == "BTCUSDT"

    def test_get_by_path_not_found(self, repo):
        result = repo.get_by_path("/no/such/file.parquet")
        assert result is None

    def test_list_by_dataset(self, repo):
        repo.insert(
            _make_manifest(
                dataset_name="binance_spot_kline_1m",
                symbol="BTCUSDT",
            )
        )
        repo.insert(
            _make_manifest(
                dataset_name="binance_spot_kline_1m",
                symbol="ETHUSDT",
                file_path="/data/binance/spot/ETHUSDT/kline/1m/2023-11-14.parquet",
            )
        )
        repo.insert(
            _make_manifest(
                dataset_name="binance_spot_trade",
                symbol="BTCUSDT",
                data_type="trade",
                interval_code=None,
                file_path="/data/binance/spot/BTCUSDT/trade/2023-11-14.parquet",
            )
        )

        rows = repo.list_by_dataset("binance_spot_kline_1m")
        assert len(rows) == 2

        trade_rows = repo.list_by_dataset("binance_spot_trade")
        assert len(trade_rows) == 1

    def test_list_by_symbol(self, repo):
        repo.insert(
            _make_manifest(
                symbol="BTCUSDT",
                start_ts_ms=1700000000000,
            )
        )
        repo.insert(
            _make_manifest(
                symbol="ETHUSDT",
                file_path="/data/binance/spot/ETHUSDT/kline/1m/2023-11-14.parquet",
                start_ts_ms=1700000000000,
            )
        )

        rows = repo.list_by_symbol("BTCUSDT")
        assert len(rows) == 1
        assert rows[0].symbol == "BTCUSDT"

    def test_list_by_symbol_with_data_type_filter(self, repo):
        repo.insert(
            _make_manifest(
                symbol="BTCUSDT",
                data_type="kline",
            )
        )
        repo.insert(
            _make_manifest(
                symbol="BTCUSDT",
                data_type="trade",
                interval_code=None,
                file_path="/data/binance/spot/BTCUSDT/trade/2023-11-14.parquet",
            )
        )

        kline_rows = repo.list_by_symbol("BTCUSDT", data_type="kline")
        assert len(kline_rows) == 1
        assert kline_rows[0].data_type == "kline"

        all_btc = repo.list_by_symbol("BTCUSDT")
        assert len(all_btc) == 2

    def test_insert_multiple_manifests(self, repo):
        repo.insert(_make_manifest(symbol="BTCUSDT"))
        repo.insert(
            _make_manifest(
                symbol="BTCUSDT",
                start_ts_ms=1700086400000,
                end_ts_ms=1700172800000,
                file_path="/data/binance/spot/BTCUSDT/kline/1m/2023-11-15.parquet",
                partition_key="2023-11-15",
            )
        )

        rows = repo.list_by_dataset("binance_spot_kline_1m")
        assert len(rows) == 2

    def test_manifest_fields_round_trip(self, repo):
        mf = _make_manifest()
        repo.insert(mf)

        result = repo.get_by_path(mf.file_path)
        assert result is not None
        assert result.dataset_name == mf.dataset_name
        assert result.venue == mf.venue
        assert result.market_type == mf.market_type
        assert result.data_type == mf.data_type
        assert result.interval_code == mf.interval_code
        assert result.time_boundary_rule == mf.time_boundary_rule
        assert result.file_format == mf.file_format
        assert result.partition_key == mf.partition_key
        assert result.start_ts_ms == mf.start_ts_ms
        assert result.end_ts_ms == mf.end_ts_ms
        assert result.row_count == mf.row_count
        assert result.file_size_bytes == mf.file_size_bytes
        assert result.content_hash == mf.content_hash
        assert result.version == mf.version
        assert result.generated_by == mf.generated_by
        assert result.status == mf.status

    def test_get_by_id_found(self, repo):
        mf = _make_manifest()
        row_id = repo.insert(mf)
        assert row_id is not None

        result = repo.get_by_id(row_id)
        assert result is not None
        assert result.id == row_id
        assert result.dataset_name == mf.dataset_name
        assert result.symbol == mf.symbol

    def test_get_by_id_not_found(self, repo):
        result = repo.get_by_id(999999)
        assert result is None
