"""Tests for the export dataset pipeline (DatasetExporter)."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pandas as pd
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from YM_data_collection.domain.models import FileManifest
from YM_data_collection.export.parquet_writer import DatasetExporter, ExportResult
from YM_data_collection.persistence.repositories.manifest_repo import (
    ManifestRepository,
)
from YM_data_collection.persistence.repositories.marketdata_repo import (
    KlineRepository,
)

# ---------------------------------------------------------------------------
# SQLite-compatible DDL
# ---------------------------------------------------------------------------

_CREATE_KLINES_SQL = text("""
CREATE TABLE IF NOT EXISTS spot_klines (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    venue                   VARCHAR(32)  NOT NULL,
    symbol                  VARCHAR(64)  NOT NULL,
    instrument_code         VARCHAR(128) NOT NULL,
    interval_code           VARCHAR(16)  NOT NULL,
    open_ts_ms              BIGINT       NOT NULL,
    close_ts_ms             BIGINT       NOT NULL,
    open_dt_utc             VARCHAR(32)  NOT NULL,
    close_dt_utc            VARCHAR(32)  NOT NULL,
    open_price              VARCHAR(32)  NOT NULL,
    high_price              VARCHAR(32)  NOT NULL,
    low_price               VARCHAR(32)  NOT NULL,
    close_price             VARCHAR(32)  NOT NULL,
    volume                  VARCHAR(32)  NOT NULL,
    quote_volume            VARCHAR(32)  NOT NULL,
    trade_count             BIGINT       NOT NULL,
    taker_buy_base_volume   VARCHAR(32)  NOT NULL,
    taker_buy_quote_volume  VARCHAR(32)  NOT NULL,
    source                  VARCHAR(32)  NOT NULL,
    ingested_at_utc         VARCHAR(32)  NOT NULL
)
""")

_CREATE_MANIFEST_SQL = text("""
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
    start_ts_ms         BIGINT       NOT NULL,
    end_ts_ms           BIGINT       NOT NULL,
    row_count           BIGINT       NOT NULL,
    file_size_bytes     BIGINT       NOT NULL,
    content_hash        VARCHAR(128) NOT NULL,
    version             INTEGER      NOT NULL,
    generated_by        VARCHAR(128) NOT NULL,
    generated_at_utc    VARCHAR(32)  NOT NULL,
    status              VARCHAR(32)  NOT NULL
)
""")

# ---------------------------------------------------------------------------
# Test data helpers
# ---------------------------------------------------------------------------

_BASE_TS = 1700000000000  # 2023-11-14 22:13:20 UTC


def _make_kline_rows(n: int = 5) -> list[dict]:
    """Create *n* kline row dicts suitable for spot_klines INSERT."""
    rows = []
    for i in range(n):
        ts = _BASE_TS + i * 3600_000  # 1 h apart
        rows.append({
            "venue": "binance",
            "symbol": "BTCUSDT",
            "instrument_code": "crypto.binance.spot.BTCUSDT",
            "interval_code": "1h",
            "open_ts_ms": ts,
            "close_ts_ms": ts + 3600_000 - 1,
            "open_dt_utc": datetime.utcfromtimestamp(ts / 1000).isoformat(),
            "close_dt_utc": datetime.utcfromtimestamp((ts + 3600_000 - 1) / 1000).isoformat(),
            "open_price": str(Decimal("40000.01000000") + i),
            "high_price": str(Decimal("40100.00000000") + i),
            "low_price": str(Decimal("39900.00000000") + i),
            "close_price": str(Decimal("40050.00000000") + i),
            "volume": str(Decimal("123.45600000")),
            "quote_volume": str(Decimal("4941234.56780000")),
            "trade_count": 500 + i,
            "taker_buy_base_volume": str(Decimal("60.00000000")),
            "taker_buy_quote_volume": str(Decimal("2400000.00000000")),
            "source": "exchange_rest",
            "ingested_at_utc": datetime.now(timezone.utc).replace(tzinfo=None).isoformat(),
        })
    return rows


def _insert_klines(session_factory, rows: list[dict]) -> None:
    """Insert test kline rows using the KlineRepository."""
    cols = (
        "venue, symbol, instrument_code, interval_code, "
        "open_ts_ms, close_ts_ms, open_dt_utc, close_dt_utc, "
        "open_price, high_price, low_price, close_price, "
        "volume, quote_volume, trade_count, "
        "taker_buy_base_volume, taker_buy_quote_volume, "
        "source, ingested_at_utc"
    )
    sql = text(
        f"INSERT OR REPLACE INTO spot_klines ({cols}) VALUES ("
        ":venue, :symbol, :instrument_code, :interval_code, "
        ":open_ts_ms, :close_ts_ms, :open_dt_utc, :close_dt_utc, "
        ":open_price, :high_price, :low_price, :close_price, "
        ":volume, :quote_volume, :trade_count, "
        ":taker_buy_base_volume, :taker_buy_quote_volume, "
        ":source, :ingested_at_utc)"
    )
    from YM_data_collection.persistence.mysql import session_scope
    with session_scope(session_factory) as session:
        session.execute(sql, rows)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def engine():
    eng = create_engine("sqlite:///:memory:", future=True)
    with eng.connect() as conn:
        conn.execute(_CREATE_KLINES_SQL)
        conn.execute(_CREATE_MANIFEST_SQL)
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
def manifest_repo(session_factory):
    return ManifestRepository(session_factory)


@pytest.fixture()
def exporter(session_factory, manifest_repo):
    return DatasetExporter(session_factory, manifest_repo)


@pytest.fixture()
def kline_data(session_factory):
    """Insert 5 kline rows and return the session_factory for convenience."""
    rows = _make_kline_rows(5)
    _insert_klines(session_factory, rows)
    return session_factory


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestExportKlines:
    """Tests for DatasetExporter.export_klines."""

    def test_produces_valid_parquet(self, exporter, kline_data, tmp_path):
        result = exporter.export_klines(
            venue="binance",
            market_type="spot",
            symbol="BTCUSDT",
            interval_code="1h",
            start_ts_ms=_BASE_TS,
            end_ts_ms=_BASE_TS + 5 * 3600_000,
            output_dir=str(tmp_path),
            dataset_name="test_dataset",
            version="v1",
        )
        assert isinstance(result, ExportResult)
        assert result.file_path.endswith("data.parquet")
        import os
        assert os.path.isfile(result.file_path)

    def test_parquet_correct_columns_and_row_count(self, exporter, kline_data, tmp_path):
        result = exporter.export_klines(
            venue="binance",
            market_type="spot",
            symbol="BTCUSDT",
            interval_code="1h",
            start_ts_ms=_BASE_TS,
            end_ts_ms=_BASE_TS + 5 * 3600_000,
            output_dir=str(tmp_path),
            dataset_name="test_dataset",
        )
        assert result.row_count == 5

        df = pd.read_parquet(result.file_path)
        # Spot klines should have these columns from the DB
        assert "open_ts_ms" in df.columns
        assert "symbol" in df.columns
        assert len(df) == 5

    def test_manifest_written_correctly(self, exporter, kline_data, tmp_path, manifest_repo):
        result = exporter.export_klines(
            venue="binance",
            market_type="spot",
            symbol="BTCUSDT",
            interval_code="1h",
            start_ts_ms=_BASE_TS,
            end_ts_ms=_BASE_TS + 5 * 3600_000,
            output_dir=str(tmp_path),
            dataset_name="test_dataset",
            version="v1",
        )
        # Manifest was returned
        assert result.manifest.dataset_name == "test_dataset"
        assert result.manifest.venue == "binance"
        assert result.manifest.market_type == "spot"
        assert result.manifest.symbol == "BTCUSDT"
        assert result.manifest.data_type == "kline"
        assert result.manifest.interval_code == "1h"
        assert result.manifest.row_count == 5
        assert result.manifest.file_format == "parquet"
        assert result.manifest.version == 1
        assert result.manifest.content_hash == result.content_hash

        # Manifest can be retrieved by path
        fetched = manifest_repo.get_by_path(result.file_path)
        assert fetched is not None
        assert fetched.row_count == 5
        assert fetched.content_hash == result.content_hash

    def test_re_export_overwrites_idempotent(self, exporter, kline_data, tmp_path, manifest_repo):
        start = _BASE_TS
        end = _BASE_TS + 5 * 3600_000

        result1 = exporter.export_klines(
            venue="binance",
            market_type="spot",
            symbol="BTCUSDT",
            interval_code="1h",
            start_ts_ms=start,
            end_ts_ms=end,
            output_dir=str(tmp_path),
            dataset_name="test_dataset",
            version="v1",
        )

        result2 = exporter.export_klines(
            venue="binance",
            market_type="spot",
            symbol="BTCUSDT",
            interval_code="1h",
            start_ts_ms=start,
            end_ts_ms=end,
            output_dir=str(tmp_path),
            dataset_name="test_dataset",
            version="v1",
        )

        # Same file path
        assert result1.file_path == result2.file_path
        # Same row count
        assert result1.row_count == result2.row_count
        # Same content hash (data didn't change)
        assert result1.content_hash == result2.content_hash

        # Two manifests exist for same path (insert creates new row)
        # but the file was overwritten — idempotent at file level
        import os
        assert os.path.isfile(result2.file_path)

    def test_empty_result_produces_empty_parquet(self, exporter, tmp_path, manifest_repo):
        """No matching rows in DB → empty parquet with row_count=0."""
        result = exporter.export_klines(
            venue="binance",
            market_type="spot",
            symbol="BTCUSDT",
            interval_code="1h",
            start_ts_ms=_BASE_TS,
            end_ts_ms=_BASE_TS + 5 * 3600_000,
            output_dir=str(tmp_path),
            dataset_name="test_dataset",
        )
        assert result.row_count == 0
        assert result.manifest.row_count == 0
        import os
        assert os.path.isfile(result.file_path)

        df = pd.read_parquet(result.file_path)
        assert len(df) == 0

    def test_file_path_structure(self, exporter, kline_data, tmp_path):
        result = exporter.export_klines(
            venue="binance",
            market_type="spot",
            symbol="BTCUSDT",
            interval_code="1h",
            start_ts_ms=_BASE_TS,
            end_ts_ms=_BASE_TS + 5 * 3600_000,
            output_dir=str(tmp_path),
            dataset_name="my_ds",
            version="v2",
        )
        import os
        expected = os.path.join(
            str(tmp_path), "my_ds", "binance", "spot", "BTCUSDT", "1h", "v2", "data.parquet"
        )
        assert result.file_path == expected
