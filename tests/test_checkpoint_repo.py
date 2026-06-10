"""Tests for CheckpointRepository using SQLite in-memory."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from YM_data_collection.domain.models import IngestCheckpoint
from YM_data_collection.persistence.repositories.checkpoint_repo import (
    CheckpointRepository,
)

# ---------------------------------------------------------------------------
# SQLite-compatible DDL
# ---------------------------------------------------------------------------

_CREATE_TABLE_SQL = text("""
CREATE TABLE IF NOT EXISTS ingest_checkpoints (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    venue           VARCHAR(32)  NOT NULL,
    market_type     VARCHAR(16)  NOT NULL,
    symbol          VARCHAR(64)  NOT NULL,
    data_type       VARCHAR(32)  NOT NULL,
    interval_code   VARCHAR(16),
    last_event_ts_ms   BIGINT UNSIGNED,
    last_event_dt_utc  DATETIME(3),
    last_trade_id      BIGINT UNSIGNED,
    last_kline_open_ts_ms BIGINT UNSIGNED,
    status          VARCHAR(32)  NOT NULL,
    last_success_at_utc DATETIME(3),
    last_error_message  VARCHAR(1024),
    updated_at_utc  DATETIME(3)  NOT NULL,
    UNIQUE (venue, market_type, symbol, data_type, interval_code)
)
""")

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def engine():
    """Create an in-memory SQLite engine with the table created."""
    eng = create_engine("sqlite:///:memory:", future=True)
    with eng.connect() as conn:
        conn.execute(_CREATE_TABLE_SQL)
        conn.commit()
    return eng


@pytest.fixture()
def session_factory(engine):
    """Session factory bound to the in-memory engine."""
    return sessionmaker(
        bind=engine,
        autoflush=False,
        autocommit=False,
        expire_on_commit=False,
        future=True,
    )


@pytest.fixture()
def repo(session_factory):
    """CheckpointRepository wired to the in-memory DB."""
    return CheckpointRepository(session_factory)


def _make_checkpoint(**overrides) -> IngestCheckpoint:
    defaults = dict(
        venue="binance",
        market_type="spot",
        symbol="BTCUSDT",
        data_type="kline",
        interval_code="1m",
        last_event_ts_ms=1700000000000,
        last_event_dt_utc=datetime(2023, 11, 14, 22, 13, 20, tzinfo=timezone.utc),
        last_trade_id=None,
        last_kline_open_ts_ms=1700000000000,
        status="ok",
        last_success_at_utc=datetime(2023, 11, 14, 22, 13, 20, tzinfo=timezone.utc),
        last_error_message=None,
    )
    defaults.update(overrides)
    return IngestCheckpoint(**defaults)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestCheckpointRepository:
    def test_upsert_inserts_new_row(self, repo):
        cp = _make_checkpoint()
        repo.upsert(cp)

        result = repo.get("binance", "spot", "BTCUSDT", "kline", interval_code="1m")
        assert result is not None
        assert result.venue == "binance"
        assert result.symbol == "BTCUSDT"
        assert result.status == "ok"

    def test_upsert_updates_existing_row(self, repo):
        cp = _make_checkpoint()
        repo.upsert(cp)

        # Upsert with updated fields
        updated = _make_checkpoint(
            last_event_ts_ms=1700000099000,
            status="error",
            last_error_message="timeout",
        )
        repo.upsert(updated)

        result = repo.get("binance", "spot", "BTCUSDT", "kline", interval_code="1m")
        assert result is not None
        assert result.last_event_ts_ms == 1700000099000
        assert result.status == "error"
        assert result.last_error_message == "timeout"

    def test_upsert_idempotent(self, repo):
        cp = _make_checkpoint()
        repo.upsert(cp)
        repo.upsert(cp)  # second call should not raise

        result = repo.get("binance", "spot", "BTCUSDT", "kline", interval_code="1m")
        assert result is not None
        assert result.status == "ok"

    def test_get_returns_none_for_missing(self, repo):
        result = repo.get("unknown", "spot", "XXX", "kline")
        assert result is None

    def test_get_with_null_interval_code(self, repo):
        cp = _make_checkpoint(interval_code=None)
        repo.upsert(cp)

        result = repo.get("binance", "spot", "BTCUSDT", "kline", interval_code=None)
        assert result is not None
        assert result.interval_code is None

    def test_list_by_status(self, repo):
        repo.upsert(_make_checkpoint(symbol="BTCUSDT", status="ok"))
        repo.upsert(_make_checkpoint(symbol="ETHUSDT", status="error"))
        repo.upsert(_make_checkpoint(symbol="SOLUSDT", status="ok"))

        ok_rows = repo.list_by_status("ok")
        error_rows = repo.list_by_status("error")

        assert len(ok_rows) == 2
        assert len(error_rows) == 1
        assert error_rows[0].symbol == "ETHUSDT"

    def test_list_all(self, repo):
        repo.upsert(_make_checkpoint(symbol="BTCUSDT"))
        repo.upsert(
            _make_checkpoint(
                symbol="ETHUSDT",
                data_type="trade",
                interval_code=None,
            )
        )

        rows = repo.list_all()
        assert len(rows) == 2

    def test_different_interval_codes_are_separate(self, repo):
        repo.upsert(_make_checkpoint(interval_code="1m"))
        repo.upsert(_make_checkpoint(interval_code="5m"))

        r1 = repo.get("binance", "spot", "BTCUSDT", "kline", interval_code="1m")
        r2 = repo.get("binance", "spot", "BTCUSDT", "kline", interval_code="5m")
        assert r1 is not None
        assert r2 is not None
        # Both exist as separate rows
        rows = repo.list_all()
        assert len(rows) == 2
