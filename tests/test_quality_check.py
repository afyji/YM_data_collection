"""Tests for QualityChecker using SQLite in-memory."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from YM_data_collection.domain.models import DataQualityIssue
from YM_data_collection.persistence.mysql import session_scope
from YM_data_collection.persistence.repositories.quality_repo import (
    QualityIssueRepository,
)
from YM_data_collection.quality.checkers import INTERVAL_MS, QualityChecker

# ---------------------------------------------------------------------------
# SQLite-compatible DDL
# ---------------------------------------------------------------------------

_KLINES_DDL = text("""
CREATE TABLE IF NOT EXISTS spot_klines (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    venue           VARCHAR(32)   NOT NULL,
    symbol          VARCHAR(64)   NOT NULL,
    instrument_code VARCHAR(128)  NOT NULL,
    interval_code   VARCHAR(16)   NOT NULL,
    open_ts_ms      BIGINT        NOT NULL,
    close_ts_ms     BIGINT        NOT NULL,
    open_dt_utc     DATETIME(3)   NOT NULL,
    close_dt_utc    DATETIME(3)   NOT NULL,
    open_price      DECIMAL(20,8) NOT NULL,
    high_price      DECIMAL(20,8) NOT NULL,
    low_price       DECIMAL(20,8) NOT NULL,
    close_price     DECIMAL(20,8) NOT NULL,
    volume          DECIMAL(20,8) NOT NULL,
    quote_volume    DECIMAL(24,8) NOT NULL,
    trade_count     BIGINT        NOT NULL,
    taker_buy_base_volume  DECIMAL(20,8) NOT NULL,
    taker_buy_quote_volume DECIMAL(24,8) NOT NULL,
    source          VARCHAR(32)   NOT NULL,
    ingested_at_utc DATETIME(3)   NOT NULL
);
""")

_PERP_KLINES_DDL = text("""
CREATE TABLE IF NOT EXISTS perp_klines (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    venue           VARCHAR(32)   NOT NULL,
    symbol          VARCHAR(64)   NOT NULL,
    instrument_code VARCHAR(128)  NOT NULL,
    interval_code   VARCHAR(16)   NOT NULL,
    open_ts_ms      BIGINT        NOT NULL,
    close_ts_ms     BIGINT        NOT NULL,
    open_dt_utc     DATETIME(3)   NOT NULL,
    close_dt_utc    DATETIME(3)   NOT NULL,
    open_price      DECIMAL(20,8) NOT NULL,
    high_price      DECIMAL(20,8) NOT NULL,
    low_price       DECIMAL(20,8) NOT NULL,
    close_price     DECIMAL(20,8) NOT NULL,
    volume          DECIMAL(20,8) NOT NULL,
    quote_volume    DECIMAL(24,8) NOT NULL,
    trade_count     BIGINT        NOT NULL,
    taker_buy_base_volume  DECIMAL(20,8) NOT NULL,
    taker_buy_quote_volume DECIMAL(24,8) NOT NULL,
    source          VARCHAR(32)   NOT NULL,
    ingested_at_utc DATETIME(3)   NOT NULL
);
""")

_DEPTH_DDL = text("""
CREATE TABLE IF NOT EXISTS spot_depth_snapshots (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    venue           VARCHAR(32)   NOT NULL,
    symbol          VARCHAR(64)   NOT NULL,
    instrument_code VARCHAR(128)  NOT NULL,
    event_ts_ms     BIGINT        NOT NULL,
    event_dt_utc    DATETIME(3)   NOT NULL,
    best_bid_price  DECIMAL(20,8) NOT NULL,
    best_bid_qty    DECIMAL(20,8) NOT NULL,
    best_ask_price  DECIMAL(20,8) NOT NULL,
    best_ask_qty    DECIMAL(20,8) NOT NULL,
    mid_price       DECIMAL(20,8) NOT NULL,
    spread_abs      DECIMAL(20,8) NOT NULL,
    spread_bps      DECIMAL(20,10) NOT NULL,
    depth_levels    INT           NOT NULL,
    bid_depth_json  TEXT          NOT NULL,
    ask_depth_json  TEXT          NOT NULL,
    source          VARCHAR(32)   NOT NULL,
    ingested_at_utc DATETIME(3)   NOT NULL
);
""")

_QUALITY_DDL = text("""
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
    start_ts_ms     BIGINT,
    end_ts_ms       BIGINT,
    description     VARCHAR(2048) NOT NULL,
    status          VARCHAR(32)  NOT NULL,
    resolution_note VARCHAR(2048),
    resolved_at_utc DATETIME(3)
);
""")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_BASE_TS_MS = 1700000000000  # A fixed reference timestamp


def _make_kline_row(
    open_ts_ms: int,
    interval_code: str = "1h",
    symbol: str = "BTCUSDT",
    venue: str = "binance",
) -> dict[str, Any]:
    """Build a kline row dict for direct SQL insert."""
    interval_ms = INTERVAL_MS[interval_code]
    close_ts_ms = open_ts_ms + interval_ms - 1
    return {
        "venue": venue,
        "symbol": symbol,
        "instrument_code": f"crypto.{venue}.spot.{symbol}",
        "interval_code": interval_code,
        "open_ts_ms": open_ts_ms,
        "close_ts_ms": close_ts_ms,
        "open_dt_utc": "2023-11-14 22:00:00",
        "close_dt_utc": "2023-11-14 22:59:59",
        "open_price": "50000.00000000",
        "high_price": "50100.00000000",
        "low_price": "49900.00000000",
        "close_price": "50050.00000000",
        "volume": "100.00000000",
        "quote_volume": "5000000.00000000",
        "trade_count": 1000,
        "taker_buy_base_volume": "50.00000000",
        "taker_buy_quote_volume": "2500000.00000000",
        "source": "exchange",
        "ingested_at_utc": "2023-11-14 23:00:00",
    }


def _insert_klines(session: Session, table: str, rows: list[dict[str, Any]]) -> None:
    """Insert kline rows into the given table."""
    if not rows:
        return
    cols = ", ".join(rows[0].keys())
    placeholders = ", ".join(f":{k}" for k in rows[0].keys())
    sql = text(f"INSERT INTO {table} ({cols}) VALUES ({placeholders})")
    session.execute(sql, rows)


def _insert_depth(
    session: Session,
    table: str,
    symbol: str,
    event_ts_ms: int,
    venue: str = "binance",
) -> None:
    """Insert a depth snapshot row."""
    sql = text(
        f"INSERT INTO {table} ("
        "venue, symbol, instrument_code, event_ts_ms, event_dt_utc, "
        "best_bid_price, best_bid_qty, best_ask_price, best_ask_qty, "
        "mid_price, spread_abs, spread_bps, depth_levels, "
        "bid_depth_json, ask_depth_json, source, ingested_at_utc"
        ") VALUES ("
        ":venue, :symbol, :instrument_code, :event_ts_ms, :event_dt_utc, "
        ":best_bid_price, :best_bid_qty, :best_ask_price, :best_ask_qty, "
        ":mid_price, :spread_abs, :spread_bps, :depth_levels, "
        ":bid_depth_json, :ask_depth_json, :source, :ingested_at_utc"
        ")"
    )
    params = {
        "venue": venue,
        "symbol": symbol,
        "instrument_code": f"crypto.{venue}.spot.{symbol}",
        "event_ts_ms": event_ts_ms,
        "event_dt_utc": "2023-11-14 22:00:00",
        "best_bid_price": "50000.00000000",
        "best_bid_qty": "1.00000000",
        "best_ask_price": "50001.00000000",
        "best_ask_qty": "1.00000000",
        "mid_price": "50000.50000000",
        "spread_abs": "1.00000000",
        "spread_bps": "2.0000000000",
        "depth_levels": 10,
        "bid_depth_json": "[]",
        "ask_depth_json": "[]",
        "source": "exchange",
        "ingested_at_utc": "2023-11-14 23:00:00",
    }
    session.execute(sql, params)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def engine():
    eng = create_engine("sqlite:///:memory:", future=True)
    with eng.connect() as conn:
        conn.execute(_KLINES_DDL)
        conn.execute(_PERP_KLINES_DDL)
        conn.execute(_DEPTH_DDL)
        conn.execute(_QUALITY_DDL)
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
def quality_repo(session_factory):
    return QualityIssueRepository(session_factory)


@pytest.fixture()
def checker(session_factory, quality_repo):
    return QualityChecker(session_factory, quality_repo)


# ---------------------------------------------------------------------------
# Tests: check_kline_gaps
# ---------------------------------------------------------------------------


class TestCheckKlineGaps:
    """Tests for gap detection."""

    def test_detects_single_gap(self, checker, session_factory, quality_repo):
        """Two bars with a missing bar in between should be detected."""
        interval_ms = INTERVAL_MS["1h"]  # 3600000
        ts0 = _BASE_TS_MS
        ts1 = ts0 + interval_ms
        ts3 = ts0 + 3 * interval_ms  # gap at ts2 = ts0 + 2*interval_ms

        rows = [_make_kline_row(ts0), _make_kline_row(ts1), _make_kline_row(ts3)]
        with session_scope(checker._session_factory) as session:
            _insert_klines(session, "spot_klines", rows)

        result = checker.check_kline_gaps(
            "binance", "spot", "BTCUSDT", "1h", ts0, ts3
        )

        assert result.check_type == "gap"
        assert not result.passed
        assert len(result.issues) == 1
        assert result.issues[0].issue_type == "gap"
        assert result.issues[0].start_ts_ms == ts0 + 2 * interval_ms

        # Verify issue was persisted
        persisted = quality_repo.list_by_symbol("BTCUSDT", data_type="kline")
        assert len(persisted) >= 1
        assert persisted[0].issue_type == "gap"

    def test_clean_data_passes(self, checker):
        """Continuous bars with no gaps should pass."""
        interval_ms = INTERVAL_MS["1h"]
        ts0 = _BASE_TS_MS
        rows = [_make_kline_row(ts0 + i * interval_ms) for i in range(5)]

        with session_scope(checker._session_factory) as session:
            _insert_klines(session, "spot_klines", rows)

        result = checker.check_kline_gaps(
            "binance", "spot", "BTCUSDT", "1h", ts0, ts0 + 4 * interval_ms
        )

        assert result.passed
        assert result.check_type == "gap"
        assert len(result.issues) == 0

    def test_detects_multiple_gap_ranges(self, checker):
        """Two separate gap ranges should produce two issues."""
        interval_ms = INTERVAL_MS["1h"]
        ts0 = _BASE_TS_MS
        # Bars: ts0, ts1, gap(ts2,ts3), ts4, gap(ts5), ts6
        rows = [
            _make_kline_row(ts0),
            _make_kline_row(ts0 + interval_ms),
            # skip ts0 + 2*interval_ms, ts0 + 3*interval_ms
            _make_kline_row(ts0 + 4 * interval_ms),
            # skip ts0 + 5*interval_ms
            _make_kline_row(ts0 + 6 * interval_ms),
        ]

        with session_scope(checker._session_factory) as session:
            _insert_klines(session, "spot_klines", rows)

        result = checker.check_kline_gaps(
            "binance", "spot", "BTCUSDT", "1h", ts0, ts0 + 6 * interval_ms
        )

        assert not result.passed
        assert len(result.issues) == 2

    def test_empty_data_is_gap(self, checker):
        """No bars in range means everything is missing."""
        interval_ms = INTERVAL_MS["1h"]
        ts0 = _BASE_TS_MS
        ts_end = ts0 + 2 * interval_ms

        result = checker.check_kline_gaps(
            "binance", "spot", "BTCUSDT", "1h", ts0, ts_end
        )

        assert not result.passed
        assert len(result.issues) >= 1


# ---------------------------------------------------------------------------
# Tests: check_kline_duplicates
# ---------------------------------------------------------------------------


class TestCheckKlineDuplicates:
    """Tests for duplicate detection."""

    def test_detects_duplicates(self, checker, quality_repo):
        """Two rows with same open_ts_ms should be detected."""
        ts0 = _BASE_TS_MS
        row1 = _make_kline_row(ts0)
        row2 = _make_kline_row(ts0)
        # SQLite without UNIQUE constraint allows this
        rows = [row1, row2]

        with session_scope(checker._session_factory) as session:
            _insert_klines(session, "spot_klines", rows)

        result = checker.check_kline_duplicates(
            "binance", "spot", "BTCUSDT", "1h", ts0, ts0 + 3600000
        )

        assert result.check_type == "duplicate"
        assert not result.passed
        assert len(result.issues) == 1
        assert result.issues[0].issue_type == "duplicate"

        # Verify issue was persisted
        persisted = quality_repo.list_by_symbol("BTCUSDT", data_type="kline")
        assert any(i.issue_type == "duplicate" for i in persisted)

    def test_no_duplicates_passes(self, checker):
        """Unique bars should pass duplicate check."""

        interval_ms = INTERVAL_MS["1h"]
        ts0 = _BASE_TS_MS
        rows = [_make_kline_row(ts0 + i * interval_ms) for i in range(3)]

        with session_scope(checker._session_factory) as session:
            _insert_klines(session, "spot_klines", rows)

        result = checker.check_kline_duplicates(
            "binance", "spot", "BTCUSDT", "1h", ts0, ts0 + 2 * interval_ms
        )

        assert result.passed
        assert len(result.issues) == 0


# ---------------------------------------------------------------------------
# Tests: check_kline_boundary
# ---------------------------------------------------------------------------


class TestCheckKlineBoundary:
    """Tests for boundary alignment detection."""

    def test_detects_misaligned_bars(self, checker, quality_repo):
        """Bars with open_ts_ms not aligned to interval should be detected."""
        interval_ms = INTERVAL_MS["1h"]  # 3600000
        # Aligned: 3600000 * N
        aligned_ts = interval_ms * 100  # a nice round number
        # Misaligned: add some offset
        misaligned_ts = aligned_ts + 12345

        rows = [_make_kline_row(aligned_ts), _make_kline_row(misaligned_ts)]

        with session_scope(checker._session_factory) as session:
            _insert_klines(session, "spot_klines", rows)

        result = checker.check_kline_boundary(
            "binance", "spot", "BTCUSDT", "1h", aligned_ts, misaligned_ts
        )

        assert result.check_type == "boundary"
        assert not result.passed
        assert len(result.issues) == 1
        assert result.issues[0].issue_type == "boundary_error"
        assert result.issues[0].start_ts_ms == misaligned_ts

        # Verify issue was persisted
        persisted = quality_repo.list_by_symbol("BTCUSDT", data_type="kline")
        assert any(i.issue_type == "boundary_error" for i in persisted)

    def test_aligned_bars_pass(self, checker):
        """All bars aligned to interval boundary should pass."""
        interval_ms = INTERVAL_MS["1h"]
        aligned_base = interval_ms * 100  # 360000000
        rows = [_make_kline_row(aligned_base + i * interval_ms) for i in range(3)]

        with session_scope(checker._session_factory) as session:
            _insert_klines(session, "spot_klines", rows)

        result = checker.check_kline_boundary(
            "binance",
            "spot",
            "BTCUSDT",
            "1h",
            aligned_base,
            aligned_base + 2 * interval_ms,
        )

        assert result.passed
        assert len(result.issues) == 0


# ---------------------------------------------------------------------------
# Tests: check_depth_freshness
# ---------------------------------------------------------------------------


class TestCheckDepthFreshness:
    """Tests for depth snapshot freshness."""

    def test_fresh_data_passes(self, checker):
        """A recent depth snapshot should pass freshness check."""
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        recent_ts = now_ms - 60_000  # 60 seconds ago

        with session_scope(checker._session_factory) as session:
            _insert_depth(session, "spot_depth_snapshots", "BTCUSDT", recent_ts)

        result = checker.check_depth_freshness(
            "binance", "spot", "BTCUSDT", max_age_seconds=300
        )

        assert result.check_type == "freshness"
        assert result.passed
        assert len(result.issues) == 0

    def test_stale_data_fails(self, checker, quality_repo):
        """An old depth snapshot should fail freshness check."""
        stale_ts = _BASE_TS_MS  # Very old timestamp

        with session_scope(checker._session_factory) as session:
            _insert_depth(session, "spot_depth_snapshots", "BTCUSDT", stale_ts)

        result = checker.check_depth_freshness(
            "binance", "spot", "BTCUSDT", max_age_seconds=300
        )

        assert result.check_type == "freshness"
        assert not result.passed
        assert len(result.issues) == 1
        assert result.issues[0].issue_type == "stale"

        # Verify issue was persisted
        persisted = quality_repo.list_by_symbol("BTCUSDT", data_type="depth_snapshot")
        assert len(persisted) >= 1
        assert persisted[0].issue_type == "stale"

    def test_no_data_fails(self, checker, quality_repo):
        """No depth snapshot at all should produce a no_data issue."""
        result = checker.check_depth_freshness(
            "binance", "spot", "ETHUSDT", max_age_seconds=300
        )

        assert not result.passed
        assert len(result.issues) == 1
        assert result.issues[0].issue_type == "no_data"

        # Verify issue was persisted
        persisted = quality_repo.list_by_symbol("ETHUSDT", data_type="depth_snapshot")
        assert len(persisted) >= 1
        assert persisted[0].issue_type == "no_data"


# ---------------------------------------------------------------------------
# Tests: quality issues written to repo
# ---------------------------------------------------------------------------


class TestQualityIssuesPersisted:
    """Verify that issues are written to the quality_repo."""

    def test_gap_issues_persisted(self, checker, quality_repo):
        interval_ms = INTERVAL_MS["1h"]
        ts0 = _BASE_TS_MS
        rows = [_make_kline_row(ts0), _make_kline_row(ts0 + 3 * interval_ms)]

        with session_scope(checker._session_factory) as session:
            _insert_klines(session, "spot_klines", rows)

        checker.check_kline_gaps(
            "binance", "spot", "BTCUSDT", "1h", ts0, ts0 + 3 * interval_ms
        )

        persisted = quality_repo.list_by_symbol("BTCUSDT", data_type="kline")
        assert len(persisted) >= 1
        for p in persisted:
            assert p.venue == "binance"
            assert p.market_type == "spot"
            assert p.symbol == "BTCUSDT"
            assert p.data_type == "kline"
            assert p.status == "open"

    def test_boundary_issues_persisted(self, checker, quality_repo):
        interval_ms = INTERVAL_MS["1h"]
        misaligned_ts = interval_ms * 100 + 999

        with session_scope(checker._session_factory) as session:
            _insert_klines(session, "spot_klines", [_make_kline_row(misaligned_ts)])

        checker.check_kline_boundary(
            "binance", "spot", "BTCUSDT", "1h", misaligned_ts, misaligned_ts
        )

        persisted = quality_repo.list_by_symbol("BTCUSDT", data_type="kline")
        assert len(persisted) >= 1
        assert persisted[0].issue_type == "boundary_error"


# ---------------------------------------------------------------------------
# Tests: clean data passes all checks
# ---------------------------------------------------------------------------


class TestCleanDataPassesAll:
    """Fully clean kline data should pass gap, duplicate, and boundary checks."""

    def test_all_kline_checks_pass(self, checker):
        interval_ms = INTERVAL_MS["1h"]
        aligned_base = interval_ms * 100
        rows = [_make_kline_row(aligned_base + i * interval_ms) for i in range(5)]

        with session_scope(checker._session_factory) as session:
            _insert_klines(session, "spot_klines", rows)

        start = aligned_base
        end = aligned_base + 4 * interval_ms

        gap_result = checker.check_kline_gaps(
            "binance", "spot", "BTCUSDT", "1h", start, end
        )
        dup_result = checker.check_kline_duplicates(
            "binance", "spot", "BTCUSDT", "1h", start, end
        )
        bnd_result = checker.check_kline_boundary(
            "binance", "spot", "BTCUSDT", "1h", start, end
        )

        assert gap_result.passed, f"Gap check failed: {gap_result.summary}"
        assert dup_result.passed, f"Duplicate check failed: {dup_result.summary}"
        assert bnd_result.passed, f"Boundary check failed: {bnd_result.summary}"

    def test_fresh_depth_passes(self, checker):
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        recent_ts = now_ms - 10_000  # 10 seconds ago

        with session_scope(checker._session_factory) as session:
            _insert_depth(session, "spot_depth_snapshots", "BTCUSDT", recent_ts)

        result = checker.check_depth_freshness(
            "binance", "spot", "BTCUSDT", max_age_seconds=300
        )

        assert result.passed, f"Freshness check failed: {result.summary}"
