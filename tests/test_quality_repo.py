"""Tests for QualityIssueRepository using SQLite in-memory."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from YM_data_collection.domain.models import DataQualityIssue
from YM_data_collection.persistence.repositories.quality_repo import (
    QualityIssueRepository,
)

# ---------------------------------------------------------------------------
# SQLite-compatible DDL
# ---------------------------------------------------------------------------

_CREATE_TABLE_SQL = text("""
CREATE TABLE IF NOT EXISTS data_quality_issues (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    venue           VARCHAR(32)  NOT NULL,
    market_type     VARCHAR(16)  NOT NULL,
    symbol          VARCHAR(64)  NOT NULL,
    data_type       VARCHAR(32)  NOT NULL,
    interval_code   VARCHAR(16),
    issue_type      VARCHAR(64)  NOT NULL,
    severity        VARCHAR(16)  NOT NULL,
    detected_at_utc DATETIME(3)  NOT NULL,
    start_ts_ms     BIGINT UNSIGNED,
    end_ts_ms       BIGINT UNSIGNED,
    description     VARCHAR(2048) NOT NULL,
    status          VARCHAR(32)  NOT NULL,
    resolution_note VARCHAR(2048),
    resolved_at_utc DATETIME(3)
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
    return QualityIssueRepository(session_factory)


def _make_issue(**overrides) -> DataQualityIssue:
    defaults = dict(
        venue="binance",
        market_type="spot",
        symbol="BTCUSDT",
        data_type="kline",
        interval_code="1m",
        issue_type="gap",
        severity="warning",
        detected_at_utc=datetime(2023, 11, 14, 22, 0, 0, tzinfo=timezone.utc),
        start_ts_ms=1700000000000,
        end_ts_ms=1700000060000,
        description="Missing kline bar",
        status="open",
        resolution_note=None,
    )
    defaults.update(overrides)
    return DataQualityIssue(**defaults)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestQualityIssueRepository:
    def test_insert_returns_id(self, repo):
        issue = _make_issue()
        row_id = repo.insert(issue)
        assert row_id is not None
        assert row_id > 0

    def test_list_by_symbol(self, repo):
        repo.insert(_make_issue(symbol="BTCUSDT"))
        repo.insert(_make_issue(symbol="ETHUSDT"))

        rows = repo.list_by_symbol("BTCUSDT")
        assert len(rows) == 1
        assert rows[0].symbol == "BTCUSDT"

    def test_list_by_symbol_with_data_type_filter(self, repo):
        repo.insert(_make_issue(symbol="BTCUSDT", data_type="kline"))
        repo.insert(_make_issue(symbol="BTCUSDT", data_type="trade", interval_code=None))

        rows = repo.list_by_symbol("BTCUSDT", data_type="kline")
        assert len(rows) == 1
        assert rows[0].data_type == "kline"

    def test_list_by_status(self, repo):
        repo.insert(_make_issue(symbol="BTCUSDT", status="open"))
        repo.insert(_make_issue(symbol="ETHUSDT", status="resolved"))

        open_rows = repo.list_by_status("open")
        resolved_rows = repo.list_by_status("resolved")

        assert len(open_rows) == 1
        assert len(resolved_rows) == 1
        assert resolved_rows[0].symbol == "ETHUSDT"

    def test_resolve(self, repo):
        row_id = repo.insert(_make_issue(status="open"))

        repo.resolve(row_id, "Backfilled from secondary source")

        rows = repo.list_by_status("resolved")
        assert len(rows) == 1
        assert rows[0].status == "resolved"
        assert rows[0].resolution_note == "Backfilled from secondary source"

    def test_insert_multiple_issues(self, repo):
        repo.insert(_make_issue(issue_type="gap", symbol="BTCUSDT"))
        repo.insert(_make_issue(issue_type="stale", symbol="BTCUSDT"))
        repo.insert(_make_issue(issue_type="gap", symbol="ETHUSDT"))

        all_btc = repo.list_by_symbol("BTCUSDT")
        assert len(all_btc) == 2

        gaps = repo.list_by_symbol("BTCUSDT", data_type="kline")
        assert len(gaps) == 2
