"""Tests for market-data repository classes.

Uses SQLite in-memory databases with the same schema shape as the MySQL
production tables.  The repository code auto-detects the dialect and uses
INSERT OR REPLACE for SQLite instead of ON DUPLICATE KEY UPDATE.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from YM_data_collection.domain.models import (
    NormalizedDepthSnapshot,
    NormalizedFundingRate,
    NormalizedIndexPrice,
    NormalizedKline,
    NormalizedMarkPrice,
    NormalizedOpenInterest,
)
from YM_data_collection.persistence.mysql import session_scope
from YM_data_collection.persistence.repositories.marketdata_repo import (
    DepthSnapshotRepository,
    FundingRateRepository,
    IndexPriceRepository,
    KlineRepository,
    MarkPriceRepository,
    OpenInterestRepository,
)

# ---------------------------------------------------------------------------
# SQLite DDL – mirrors the MySQL schema with UNIQUE constraints
# ---------------------------------------------------------------------------

_KLINES_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
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
    ingested_at_utc DATETIME(3)   NOT NULL,
    UNIQUE(venue, symbol, interval_code, open_ts_ms)
);
"""

_FUNDING_DDL = """
CREATE TABLE IF NOT EXISTS perp_funding_rates (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    venue                 VARCHAR(32)    NOT NULL,
    symbol                VARCHAR(64)    NOT NULL,
    instrument_code       VARCHAR(128)   NOT NULL,
    funding_time_ts_ms    BIGINT         NOT NULL,
    funding_time_dt_utc   DATETIME(3)    NOT NULL,
    funding_rate          DECIMAL(20,10) NOT NULL,
    mark_price            DECIMAL(20,8),
    source                VARCHAR(32)    NOT NULL,
    ingested_at_utc       DATETIME(3)    NOT NULL,
    UNIQUE(venue, symbol, funding_time_ts_ms)
);
"""

_OI_DDL = """
CREATE TABLE IF NOT EXISTS perp_open_interest (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    venue                 VARCHAR(32)    NOT NULL,
    symbol                VARCHAR(64)    NOT NULL,
    instrument_code       VARCHAR(128)   NOT NULL,
    event_ts_ms           BIGINT         NOT NULL,
    event_dt_utc          DATETIME(3)    NOT NULL,
    open_interest         DECIMAL(24,8)  NOT NULL,
    open_interest_value   DECIMAL(24,8),
    source                VARCHAR(32)    NOT NULL,
    ingested_at_utc       DATETIME(3)    NOT NULL,
    UNIQUE(venue, symbol, event_ts_ms)
);
"""

_MARK_DDL = """
CREATE TABLE IF NOT EXISTS perp_mark_prices (
    id                       INTEGER PRIMARY KEY AUTOINCREMENT,
    venue                    VARCHAR(32)    NOT NULL,
    symbol                   VARCHAR(64)    NOT NULL,
    instrument_code          VARCHAR(128)   NOT NULL,
    event_ts_ms              BIGINT         NOT NULL,
    event_dt_utc             DATETIME(3)    NOT NULL,
    mark_price               DECIMAL(20,8)  NOT NULL,
    funding_rate             DECIMAL(20,10),
    next_funding_time_ts_ms  BIGINT,
    source                   VARCHAR(32)    NOT NULL,
    ingested_at_utc          DATETIME(3)    NOT NULL,
    UNIQUE(venue, symbol, event_ts_ms)
);
"""

_INDEX_DDL = """
CREATE TABLE IF NOT EXISTS perp_index_prices (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    venue                 VARCHAR(32)    NOT NULL,
    symbol                VARCHAR(64)    NOT NULL,
    instrument_code       VARCHAR(128)   NOT NULL,
    event_ts_ms           BIGINT         NOT NULL,
    event_dt_utc          DATETIME(3)    NOT NULL,
    index_price           DECIMAL(20,8)  NOT NULL,
    source                VARCHAR(32)    NOT NULL,
    ingested_at_utc       DATETIME(3)    NOT NULL,
    UNIQUE(venue, symbol, event_ts_ms)
);
"""

_DEPTH_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    venue           VARCHAR(32)    NOT NULL,
    symbol          VARCHAR(64)    NOT NULL,
    instrument_code VARCHAR(128)   NOT NULL,
    event_ts_ms     BIGINT         NOT NULL,
    event_dt_utc    DATETIME(3)    NOT NULL,
    best_bid_price  DECIMAL(20,8)  NOT NULL,
    best_bid_qty    DECIMAL(20,8)  NOT NULL,
    best_ask_price  DECIMAL(20,8)  NOT NULL,
    best_ask_qty    DECIMAL(20,8)  NOT NULL,
    mid_price       DECIMAL(20,8)  NOT NULL,
    spread_abs      DECIMAL(20,8)  NOT NULL,
    spread_bps      DECIMAL(20,10) NOT NULL,
    depth_levels    INTEGER        NOT NULL,
    bid_depth_json  TEXT           NOT NULL,
    ask_depth_json  TEXT           NOT NULL,
    source          VARCHAR(32)    NOT NULL,
    ingested_at_utc DATETIME(3)    NOT NULL,
    UNIQUE(venue, symbol, event_ts_ms)
);
"""


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def engine():
    """Create a fresh SQLite in-memory engine."""
    eng = create_engine("sqlite:///:memory:", future=True)
    with eng.connect() as conn:
        conn.execute(text(_KLINES_DDL.format(table="spot_klines")))
        conn.execute(text(_KLINES_DDL.format(table="perp_klines")))
        conn.execute(text(_FUNDING_DDL))
        conn.execute(text(_OI_DDL))
        conn.execute(text(_MARK_DDL))
        conn.execute(text(_INDEX_DDL))
        conn.execute(text(_DEPTH_DDL.format(table="spot_depth_snapshots")))
        conn.execute(text(_DEPTH_DDL.format(table="perp_depth_snapshots")))
        conn.commit()
    return eng


@pytest.fixture()
def session_factory(engine):
    return sessionmaker(bind=engine, autoflush=False, autocommit=False,
                        expire_on_commit=False, future=True)


def _count(session_factory, table: str) -> int:
    with session_scope(session_factory) as s:
        return s.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()


# ---------------------------------------------------------------------------
# Test data factories
# ---------------------------------------------------------------------------

def _ts_to_dt(ts_ms: int) -> datetime:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).replace(tzinfo=None)


def _make_kline(open_ts_ms: int, close_ts_ms: int, interval: str = "1m",
                symbol: str = "BTCUSDT", venue: str = "binance") -> NormalizedKline:
    return NormalizedKline(
        venue=venue,
        symbol=symbol,
        instrument_code=f"{symbol}",
        interval_code=interval,
        open_ts_ms=open_ts_ms,
        close_ts_ms=close_ts_ms,
        open_dt_utc=_ts_to_dt(open_ts_ms),
        close_dt_utc=_ts_to_dt(close_ts_ms),
        open_price=Decimal("100.00000000"),
        high_price=Decimal("105.00000000"),
        low_price=Decimal("99.00000000"),
        close_price=Decimal("102.00000000"),
        volume=Decimal("500.00000000"),
        quote_volume=Decimal("50000.00000000"),
        trade_count=1200,
        taker_buy_base_volume=Decimal("250.00000000"),
        taker_buy_quote_volume=Decimal("25000.00000000"),
        source="exchange",
    )


def _make_funding(ts_ms: int, symbol: str = "BTCUSDT",
                  venue: str = "binance") -> NormalizedFundingRate:
    return NormalizedFundingRate(
        venue=venue,
        symbol=symbol,
        instrument_code=symbol,
        funding_time_ts_ms=ts_ms,
        funding_time_dt_utc=_ts_to_dt(ts_ms),
        funding_rate=Decimal("0.00010000"),
        mark_price=Decimal("100.00000000"),
        source="exchange",
    )


def _make_oi(ts_ms: int, symbol: str = "BTCUSDT",
             venue: str = "binance") -> NormalizedOpenInterest:
    return NormalizedOpenInterest(
        venue=venue,
        symbol=symbol,
        instrument_code=symbol,
        event_ts_ms=ts_ms,
        event_dt_utc=_ts_to_dt(ts_ms),
        open_interest=Decimal("10000.00000000"),
        open_interest_value=Decimal("1000000.00000000"),
        source="exchange",
    )


def _make_mark(ts_ms: int, symbol: str = "BTCUSDT",
               venue: str = "binance") -> NormalizedMarkPrice:
    return NormalizedMarkPrice(
        venue=venue,
        symbol=symbol,
        instrument_code=symbol,
        event_ts_ms=ts_ms,
        event_dt_utc=_ts_to_dt(ts_ms),
        mark_price=Decimal("100.00000000"),
        funding_rate=Decimal("0.00010000"),
        next_funding_time_ts_ms=ts_ms + 28800000,
        source="exchange",
    )


def _make_index(ts_ms: int, symbol: str = "BTCUSDT",
                venue: str = "binance") -> NormalizedIndexPrice:
    return NormalizedIndexPrice(
        venue=venue,
        symbol=symbol,
        instrument_code=symbol,
        event_ts_ms=ts_ms,
        event_dt_utc=_ts_to_dt(ts_ms),
        index_price=Decimal("100.00000000"),
        source="exchange",
    )


def _make_depth(ts_ms: int, symbol: str = "BTCUSDT",
                venue: str = "binance") -> NormalizedDepthSnapshot:
    return NormalizedDepthSnapshot(
        venue=venue,
        symbol=symbol,
        instrument_code=symbol,
        event_ts_ms=ts_ms,
        event_dt_utc=_ts_to_dt(ts_ms),
        best_bid_price=Decimal("99.50000000"),
        best_bid_qty=Decimal("10.00000000"),
        best_ask_price=Decimal("100.50000000"),
        best_ask_qty=Decimal("8.00000000"),
        mid_price=Decimal("100.00000000"),
        spread_abs=Decimal("1.00000000"),
        spread_bps=Decimal("10.0000000000"),
        depth_levels=5,
        bid_depth_json=[["99.5", "10"], ["99.0", "20"]],
        ask_depth_json=[["100.5", "8"], ["101.0", "15"]],
        source="exchange",
    )


# ===========================================================================
# KlineRepository tests
# ===========================================================================

class TestKlineRepository:

    def test_upsert_batch_inserts_rows(self, session_factory):
        repo = KlineRepository()
        klines = [
            _make_kline(1000, 2000),
            _make_kline(2000, 3000),
        ]
        n = repo.upsert_batch(session_factory, "spot_klines", klines)
        assert n == 2
        assert _count(session_factory, "spot_klines") == 2

    def test_upsert_batch_idempotent(self, session_factory):
        repo = KlineRepository()
        klines = [_make_kline(1000, 2000), _make_kline(2000, 3000)]
        repo.upsert_batch(session_factory, "spot_klines", klines)
        repo.upsert_batch(session_factory, "spot_klines", klines)
        assert _count(session_factory, "spot_klines") == 2

    def test_upsert_batch_perp_table(self, session_factory):
        repo = KlineRepository()
        klines = [_make_kline(1000, 2000)]
        repo.upsert_batch(session_factory, "perp_klines", klines)
        assert _count(session_factory, "perp_klines") == 1

    def test_query_range_returns_matching_rows(self, session_factory):
        repo = KlineRepository()
        klines = [
            _make_kline(1000, 2000),
            _make_kline(2000, 3000),
            _make_kline(3000, 4000),
            _make_kline(4000, 5000),
        ]
        repo.upsert_batch(session_factory, "spot_klines", klines)

        results = repo.query_range(
            session_factory, "spot_klines", "BTCUSDT", "1m", 2000, 4000
        )
        assert len(results) == 3
        assert results[0]["open_ts_ms"] == 2000
        assert results[2]["open_ts_ms"] == 4000

    def test_query_range_empty(self, session_factory):
        repo = KlineRepository()
        results = repo.query_range(
            session_factory, "spot_klines", "BTCUSDT", "1m", 0, 100
        )
        assert results == []

    def test_query_latest_returns_newest_first(self, session_factory):
        repo = KlineRepository()
        klines = [
            _make_kline(1000, 2000),
            _make_kline(2000, 3000),
            _make_kline(3000, 4000),
        ]
        repo.upsert_batch(session_factory, "spot_klines", klines)

        results = repo.query_latest(
            session_factory, "spot_klines", "BTCUSDT", "1m", limit=2
        )
        assert len(results) == 2
        assert results[0]["open_ts_ms"] == 3000
        assert results[1]["open_ts_ms"] == 2000

    def test_query_latest_default_limit(self, session_factory):
        repo = KlineRepository()
        klines = [_make_kline(i * 1000, i * 1000 + 1000) for i in range(5)]
        repo.upsert_batch(session_factory, "spot_klines", klines)

        results = repo.query_latest(
            session_factory, "spot_klines", "BTCUSDT", "1m"
        )
        assert len(results) == 1
        assert results[0]["open_ts_ms"] == 4000

    def test_upsert_batch_empty(self, session_factory):
        repo = KlineRepository()
        n = repo.upsert_batch(session_factory, "spot_klines", [])
        assert n == 0

    def test_different_symbols_dont_collide(self, session_factory):
        repo = KlineRepository()
        klines_a = [_make_kline(1000, 2000, symbol="BTCUSDT")]
        klines_b = [_make_kline(1000, 2000, symbol="ETHUSDT")]
        repo.upsert_batch(session_factory, "spot_klines", klines_a)
        repo.upsert_batch(session_factory, "spot_klines", klines_b)
        assert _count(session_factory, "spot_klines") == 2


# ===========================================================================
# FundingRateRepository tests
# ===========================================================================

class TestFundingRateRepository:

    def test_upsert_batch_inserts_rows(self, session_factory):
        repo = FundingRateRepository()
        rates = [_make_funding(1000), _make_funding(2000)]
        n = repo.upsert_batch(session_factory, rates)
        assert n == 2
        assert _count(session_factory, "perp_funding_rates") == 2

    def test_upsert_batch_idempotent(self, session_factory):
        repo = FundingRateRepository()
        rates = [_make_funding(1000), _make_funding(2000)]
        repo.upsert_batch(session_factory, rates)
        repo.upsert_batch(session_factory, rates)
        assert _count(session_factory, "perp_funding_rates") == 2

    def test_query_range_returns_matching_rows(self, session_factory):
        repo = FundingRateRepository()
        rates = [_make_funding(1000), _make_funding(2000), _make_funding(3000)]
        repo.upsert_batch(session_factory, rates)

        results = repo.query_range(session_factory, "BTCUSDT", 1000, 2000)
        assert len(results) == 2
        assert results[0]["funding_time_ts_ms"] == 1000
        assert results[1]["funding_time_ts_ms"] == 2000

    def test_query_range_excludes_out_of_bounds(self, session_factory):
        repo = FundingRateRepository()
        rates = [_make_funding(1000), _make_funding(5000)]
        repo.upsert_batch(session_factory, rates)

        results = repo.query_range(session_factory, "BTCUSDT", 2000, 4000)
        assert results == []

    def test_query_latest_returns_newest_first(self, session_factory):
        repo = FundingRateRepository()
        rates = [_make_funding(1000), _make_funding(2000), _make_funding(3000)]
        repo.upsert_batch(session_factory, rates)

        results = repo.query_latest(session_factory, "BTCUSDT", limit=2)
        assert len(results) == 2
        assert results[0]["funding_time_ts_ms"] == 3000
        assert results[1]["funding_time_ts_ms"] == 2000

    def test_query_latest_default_limit(self, session_factory):
        repo = FundingRateRepository()
        rates = [_make_funding(i * 1000) for i in range(5)]
        repo.upsert_batch(session_factory, rates)

        results = repo.query_latest(session_factory, "BTCUSDT")
        assert len(results) == 1
        assert results[0]["funding_time_ts_ms"] == 4000

    def test_upsert_batch_empty(self, session_factory):
        repo = FundingRateRepository()
        n = repo.upsert_batch(session_factory, [])
        assert n == 0


# ===========================================================================
# OpenInterestRepository tests
# ===========================================================================

class TestOpenInterestRepository:

    def test_upsert_batch_idempotent(self, session_factory):
        repo = OpenInterestRepository()
        records = [_make_oi(1000), _make_oi(2000)]
        repo.upsert_batch(session_factory, records)
        repo.upsert_batch(session_factory, records)
        assert _count(session_factory, "perp_open_interest") == 2

    def test_query_range(self, session_factory):
        repo = OpenInterestRepository()
        records = [_make_oi(1000), _make_oi(2000), _make_oi(3000)]
        repo.upsert_batch(session_factory, records)

        results = repo.query_range(session_factory, "BTCUSDT", 1000, 2000)
        assert len(results) == 2

    def test_query_latest(self, session_factory):
        repo = OpenInterestRepository()
        records = [_make_oi(1000), _make_oi(2000), _make_oi(3000)]
        repo.upsert_batch(session_factory, records)

        results = repo.query_latest(session_factory, "BTCUSDT", limit=2)
        assert len(results) == 2
        assert results[0]["event_ts_ms"] == 3000


# ===========================================================================
# MarkPriceRepository tests
# ===========================================================================

class TestMarkPriceRepository:

    def test_upsert_batch_idempotent(self, session_factory):
        repo = MarkPriceRepository()
        records = [_make_mark(1000), _make_mark(2000)]
        repo.upsert_batch(session_factory, records)
        repo.upsert_batch(session_factory, records)
        assert _count(session_factory, "perp_mark_prices") == 2

    def test_query_range(self, session_factory):
        repo = MarkPriceRepository()
        records = [_make_mark(1000), _make_mark(2000), _make_mark(3000)]
        repo.upsert_batch(session_factory, records)

        results = repo.query_range(session_factory, "BTCUSDT", 1500, 3000)
        assert len(results) == 2
        assert results[0]["event_ts_ms"] == 2000

    def test_query_latest(self, session_factory):
        repo = MarkPriceRepository()
        records = [_make_mark(1000), _make_mark(5000)]
        repo.upsert_batch(session_factory, records)

        results = repo.query_latest(session_factory, "BTCUSDT")
        assert len(results) == 1
        assert results[0]["event_ts_ms"] == 5000


# ===========================================================================
# IndexPriceRepository tests
# ===========================================================================

class TestIndexPriceRepository:

    def test_upsert_batch_idempotent(self, session_factory):
        repo = IndexPriceRepository()
        records = [_make_index(1000), _make_index(2000)]
        repo.upsert_batch(session_factory, records)
        repo.upsert_batch(session_factory, records)
        assert _count(session_factory, "perp_index_prices") == 2

    def test_query_range(self, session_factory):
        repo = IndexPriceRepository()
        records = [_make_index(1000), _make_index(2000), _make_index(3000)]
        repo.upsert_batch(session_factory, records)

        results = repo.query_range(session_factory, "BTCUSDT", 0, 2000)
        assert len(results) == 2

    def test_query_latest(self, session_factory):
        repo = IndexPriceRepository()
        records = [_make_index(1000), _make_index(2000)]
        repo.upsert_batch(session_factory, records)

        results = repo.query_latest(session_factory, "BTCUSDT")
        assert results[0]["event_ts_ms"] == 2000


# ===========================================================================
# DepthSnapshotRepository tests
# ===========================================================================

class TestDepthSnapshotRepository:

    def test_upsert_batch_inserts_rows(self, session_factory):
        repo = DepthSnapshotRepository()
        snaps = [_make_depth(1000), _make_depth(2000)]
        n = repo.upsert_batch(session_factory, "spot_depth_snapshots", snaps)
        assert n == 2
        assert _count(session_factory, "spot_depth_snapshots") == 2

    def test_upsert_batch_idempotent(self, session_factory):
        repo = DepthSnapshotRepository()
        snaps = [_make_depth(1000), _make_depth(2000)]
        repo.upsert_batch(session_factory, "spot_depth_snapshots", snaps)
        repo.upsert_batch(session_factory, "spot_depth_snapshots", snaps)
        assert _count(session_factory, "spot_depth_snapshots") == 2

    def test_upsert_batch_perp_table(self, session_factory):
        repo = DepthSnapshotRepository()
        snaps = [_make_depth(1000)]
        repo.upsert_batch(session_factory, "perp_depth_snapshots", snaps)
        assert _count(session_factory, "perp_depth_snapshots") == 1

    def test_query_range_returns_matching_rows(self, session_factory):
        repo = DepthSnapshotRepository()
        snaps = [_make_depth(1000), _make_depth(2000), _make_depth(3000)]
        repo.upsert_batch(session_factory, "spot_depth_snapshots", snaps)

        results = repo.query_range(
            session_factory, "spot_depth_snapshots", "BTCUSDT", 1500, 3000
        )
        assert len(results) == 2
        assert results[0]["event_ts_ms"] == 2000
        assert results[1]["event_ts_ms"] == 3000

    def test_query_range_empty(self, session_factory):
        repo = DepthSnapshotRepository()
        results = repo.query_range(
            session_factory, "spot_depth_snapshots", "BTCUSDT", 0, 100
        )
        assert results == []

    def test_query_latest_returns_newest_first(self, session_factory):
        repo = DepthSnapshotRepository()
        snaps = [_make_depth(1000), _make_depth(2000), _make_depth(3000)]
        repo.upsert_batch(session_factory, "spot_depth_snapshots", snaps)

        results = repo.query_latest(
            session_factory, "spot_depth_snapshots", "BTCUSDT", limit=2
        )
        assert len(results) == 2
        assert results[0]["event_ts_ms"] == 3000
        assert results[1]["event_ts_ms"] == 2000

    def test_query_latest_default_limit(self, session_factory):
        repo = DepthSnapshotRepository()
        snaps = [_make_depth(i * 1000) for i in range(5)]
        repo.upsert_batch(session_factory, "spot_depth_snapshots", snaps)

        results = repo.query_latest(
            session_factory, "spot_depth_snapshots", "BTCUSDT"
        )
        assert len(results) == 1
        assert results[0]["event_ts_ms"] == 4000

    def test_depth_json_stored_correctly(self, session_factory):
        repo = DepthSnapshotRepository()
        snap = _make_depth(1000)
        repo.upsert_batch(session_factory, "spot_depth_snapshots", [snap])

        results = repo.query_latest(
            session_factory, "spot_depth_snapshots", "BTCUSDT"
        )
        assert len(results) == 1
        # SQLite returns JSON text as-is
        bid_json = results[0]["bid_depth_json"]
        assert json.loads(bid_json) == [["99.5", "10"], ["99.0", "20"]]

    def test_upsert_batch_empty(self, session_factory):
        repo = DepthSnapshotRepository()
        n = repo.upsert_batch(session_factory, "spot_depth_snapshots", [])
        assert n == 0
