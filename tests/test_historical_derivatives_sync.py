"""Tests for run_historical_derivatives_sync.

Uses mocked gateway + in-memory SQLite for checkpoint/marketdata repos.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from YM_data_collection.domain.models import IngestCheckpoint
from YM_data_collection.persistence.repositories.checkpoint_repo import (
    CheckpointRepository,
)
from YM_data_collection.persistence.repositories.marketdata_repo import (
    FundingRateRepository,
    MarkPriceRepository,
    OpenInterestRepository,
)

# ---------------------------------------------------------------------------
# Import the module under test
# ---------------------------------------------------------------------------
from YM_data_collection.apps.run_historical_derivatives_sync import (
    AUTO_DATA_TYPE_TOKEN,
    _resolve_requested_data_types,
    _sync_funding_rate,
    _sync_mark_price,
    _sync_open_interest,
    main,
    run_sync,
)
from YM_data_collection.utils.binance_constraints import (
    validate_open_interest_history_range,
)
from YM_data_collection.apps._cli_common import CliArgumentError

# ---------------------------------------------------------------------------
# SQLite DDL for tables used by the repos
# ---------------------------------------------------------------------------

_CHECKPOINT_DDL = text("""
CREATE TABLE IF NOT EXISTS ingest_checkpoints (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    venue           VARCHAR(32)  NOT NULL,
    market_type     VARCHAR(16)  NOT NULL,
    symbol          VARCHAR(64)  NOT NULL,
    data_type       VARCHAR(32)  NOT NULL,
    interval_code   VARCHAR(16),
    last_event_ts_ms   BIGINT,
    last_event_dt_utc  DATETIME(3),
    last_trade_id      BIGINT,
    last_kline_open_ts_ms BIGINT,
    status          VARCHAR(32)  NOT NULL,
    last_success_at_utc DATETIME(3),
    last_error_message  VARCHAR(1024),
    updated_at_utc  DATETIME(3)  NOT NULL,
    UNIQUE (venue, market_type, symbol, data_type, interval_code)
)
""")

_FUNDING_DDL = text("""
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
)
""")

_MARK_PRICE_DDL = text("""
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
)
""")

_OPEN_INTEREST_DDL = text("""
CREATE TABLE IF NOT EXISTS perp_open_interest (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    venue                 VARCHAR(32)    NOT NULL,
    symbol                VARCHAR(64)    NOT NULL,
    instrument_code       VARCHAR(128)   NOT NULL,
    event_ts_ms           BIGINT         NOT NULL,
    event_dt_utc          DATETIME(3)    NOT NULL,
    open_interest         DECIMAL(20,8)  NOT NULL,
    open_interest_value   DECIMAL(24,8),
    source                VARCHAR(32)    NOT NULL,
    ingested_at_utc       DATETIME(3)    NOT NULL,
    UNIQUE(venue, symbol, event_ts_ms)
)
""")

_INDEX_PRICE_DDL = text("""
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
)
""")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def engine():
    """Create an in-memory SQLite engine with all required tables."""
    eng = create_engine("sqlite:///:memory:", future=True)
    with eng.connect() as conn:
        conn.execute(_CHECKPOINT_DDL)
        conn.execute(_FUNDING_DDL)
        conn.execute(_MARK_PRICE_DDL)
        conn.execute(_OPEN_INTEREST_DDL)
        conn.execute(_INDEX_PRICE_DDL)
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
def checkpoint_repo(session_factory):
    return CheckpointRepository(session_factory)


@pytest.fixture()
def logger():
    """Return a mock logger that captures all calls."""
    return MagicMock()


# ---------------------------------------------------------------------------
# Sample data factories
# ---------------------------------------------------------------------------


def _funding_rate_raw(symbol: str = "BTCUSDT", ts_ms: int = 1700000000000) -> dict:
    return {
        "symbol": symbol,
        "fundingTime": ts_ms,
        "fundingRate": "0.00010000",
        "markPrice": "34567.80",
    }


def _mark_price_kline_raw(ts_ms: int = 1700000000000) -> list:
    return [
        ts_ms,        # 0 open_time
        "34500.00",   # 1 open
        "34600.00",   # 2 high
        "34400.00",   # 3 low
        "34567.80",   # 4 close -> mark_price
        "0",          # 5 volume
        ts_ms + 3599999,  # 6 close_time
        "0",          # 7 quote_volume
        0,            # 8 trade_count
        "0",          # 9
        "0",          # 10
        "0",          # 11
    ]

def _index_price_kline_raw(ts_ms: int = 1700000000000) -> list:
    return [
        ts_ms,
        "34490.00",
        "34580.00",
        "34420.00",
        "34550.25",   # close -> index_price
        "0",
        ts_ms + 3599999,
        "0",
        0,
        "0",
        "0",
        "0",
    ]


def _open_interest_raw(symbol: str = "BTCUSDT", ts_ms: int = 1700000000000) -> dict:
    return {
        "symbol": symbol,
        "sumOpenInterest": "12345.678",
        "sumOpenInterestValue": "427654321.12",
        "timestamp": ts_ms,
    }


# ---------------------------------------------------------------------------
# Tests: funding_rate sync
# ---------------------------------------------------------------------------


class TestSyncFundingRate:
    @pytest.mark.asyncio
    async def test_basic_sync(self, session_factory, checkpoint_repo, logger):
        """Single batch of funding rates is normalized, validated, and upserted."""
        gateway = AsyncMock()
        raw = [_funding_rate_raw(ts_ms=1700000000000)]
        # First call returns data, second returns empty to stop pagination
        gateway.fetch_funding_rates.side_effect = [raw, []]

        count = await _sync_funding_rate(
            gateway, session_factory, checkpoint_repo,
            "binance", "BTCUSDT", 1700000000000, 1700086400000, logger,
        )

        assert count == 1
        assert gateway.fetch_funding_rates.await_count == 2

        # Check checkpoint was written
        cp = checkpoint_repo.get(
            "binance", "usdt_perpetual", "BTCUSDT", "funding_rate",
        )
        assert cp is not None
        assert cp.last_event_ts_ms == 1700000000000
        assert cp.status == "ok"

    @pytest.mark.asyncio
    async def test_empty_batch_stops_pagination(
        self, session_factory, checkpoint_repo, logger,
    ):
        """An empty batch from the gateway should stop pagination immediately."""
        gateway = AsyncMock()
        gateway.fetch_funding_rates.return_value = []

        count = await _sync_funding_rate(
            gateway, session_factory, checkpoint_repo,
            "binance", "BTCUSDT", 1700000000000, 1700086400000, logger,
        )

        assert count == 0

    @pytest.mark.asyncio
    async def test_pagination(self, session_factory, checkpoint_repo, logger):
        """Two batches: second batch starts after last timestamp of first."""
        gateway = AsyncMock()
        batch1 = [_funding_rate_raw(ts_ms=1700000000000)]
        batch2 = [_funding_rate_raw(ts_ms=1700008000000)]
        gateway.fetch_funding_rates.side_effect = [batch1, batch2, []]

        count = await _sync_funding_rate(
            gateway, session_factory, checkpoint_repo,
            "binance", "BTCUSDT", 1700000000000, 1700086400000, logger,
        )

        assert count == 2
        # First call with original start, second with last_ts+1
        call_args = gateway.fetch_funding_rates.call_args_list
        assert call_args[0].kwargs["start_ts_ms"] == 1700000000000
        assert call_args[1].kwargs["start_ts_ms"] == 1700000000001


# ---------------------------------------------------------------------------
# Tests: mark_price sync
# ---------------------------------------------------------------------------


class TestSyncMarkPrice:
    @pytest.mark.asyncio
    async def test_basic_sync(self, session_factory, checkpoint_repo, logger):
        gateway = AsyncMock()
        raw = [_mark_price_kline_raw(ts_ms=1700000000000)]
        # First call returns data, second returns empty to stop pagination
        gateway.fetch_mark_price_klines.side_effect = [raw, []]

        count = await _sync_mark_price(
            gateway, session_factory, checkpoint_repo,
            "binance", "BTCUSDT", 1700000000000, 1700086400000, logger,
        )

        assert count == 1
        assert gateway.fetch_mark_price_klines.await_count == 2

        cp = checkpoint_repo.get(
            "binance", "usdt_perpetual", "BTCUSDT", "mark_price",
            interval_code="1h",
        )
        assert cp is not None
        assert cp.status == "ok"

    @pytest.mark.asyncio
    async def test_empty_batch(self, session_factory, checkpoint_repo, logger):
        gateway = AsyncMock()
        gateway.fetch_mark_price_klines.return_value = []

        count = await _sync_mark_price(
            gateway, session_factory, checkpoint_repo,
            "binance", "BTCUSDT", 1700000000000, 1700086400000, logger,
        )

        assert count == 0


# ---------------------------------------------------------------------------
# Tests: open_interest sync
# ---------------------------------------------------------------------------


class TestSyncOpenInterest:
    @pytest.mark.asyncio
    async def test_basic_sync(self, session_factory, checkpoint_repo, logger):
        gateway = AsyncMock()
        raw = [_open_interest_raw(ts_ms=1700000000000)]
        # First call returns data, second returns empty to stop pagination
        gateway.fetch_open_interest_hist.side_effect = [raw, []]

        count = await _sync_open_interest(
            gateway, session_factory, checkpoint_repo,
            "binance", "BTCUSDT", 1700000000000, 1700086400000, logger,
        )

        assert count == 1
        assert gateway.fetch_open_interest_hist.await_count == 2

        cp = checkpoint_repo.get(
            "binance", "usdt_perpetual", "BTCUSDT", "open_interest",
            interval_code="5m",
        )
        assert cp is not None
        assert cp.status == "ok"

    @pytest.mark.asyncio
    async def test_empty_batch(self, session_factory, checkpoint_repo, logger):
        gateway = AsyncMock()
        gateway.fetch_open_interest_hist.return_value = []

        count = await _sync_open_interest(
            gateway, session_factory, checkpoint_repo,
            "binance", "BTCUSDT", 1700000000000, 1700086400000, logger,
        )

        assert count == 0

    def test_rejects_range_older_than_latest_month(self) -> None:
        now = datetime(2026, 6, 10, tzinfo=timezone.utc)
        start_ts_ms = int(datetime(2024, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
        end_ts_ms = int(datetime(2024, 1, 7, 23, 59, 59, tzinfo=timezone.utc).timestamp() * 1000)

        with pytest.raises(ValueError, match="latest 1 month"):
            validate_open_interest_history_range(start_ts_ms, end_ts_ms, now=now)


class TestResolveRequestedDataTypes:
    def test_auto_mode_skips_old_open_interest_when_no_overlap(self, logger) -> None:
        """Auto mode should skip open_interest when the requested range has NO overlap
        with Binance's available 1-month window."""
        start_ts_ms = int(datetime(2024, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
        end_ts_ms = int(datetime(2024, 1, 7, 23, 59, 59, tzinfo=timezone.utc).timestamp() * 1000)

        resolved, overrides = _resolve_requested_data_types(
            [AUTO_DATA_TYPE_TOKEN],
            start_ts_ms=start_ts_ms,
            end_ts_ms=end_ts_ms,
            logger=logger,
            now=datetime(2026, 6, 10, tzinfo=timezone.utc),
        )

        assert resolved == ["mark_price", "index_price", "funding_rate"]
        assert "open_interest" not in overrides

    def test_auto_mode_includes_open_interest_with_partial_overlap(self, logger) -> None:
        """Auto mode should include open_interest when the requested range partially
        overlaps the available 1-month window, and provide a clamped range."""
        now = datetime(2026, 6, 10, tzinfo=timezone.utc)
        cutoff = datetime(2026, 5, 11, tzinfo=timezone.utc)  # now - 30 days
        # Request range starts BEFORE cutoff but ends AFTER cutoff
        start_ts_ms = int(datetime(2026, 4, 1, tzinfo=timezone.utc).timestamp() * 1000)
        end_ts_ms = int(datetime(2026, 5, 15, tzinfo=timezone.utc).timestamp() * 1000)

        resolved, overrides = _resolve_requested_data_types(
            [AUTO_DATA_TYPE_TOKEN],
            start_ts_ms=start_ts_ms,
            end_ts_ms=end_ts_ms,
            logger=logger,
            now=now,
        )

        assert "open_interest" in resolved
        assert "open_interest" in overrides
        clamped_start, clamped_end = overrides["open_interest"]
        # Clamped start should be at or after cutoff
        cutoff_ts_ms = int(cutoff.timestamp() * 1000)
        assert clamped_start >= cutoff_ts_ms
        # Clamped end should be the original end (since it's before now)
        assert clamped_end == end_ts_ms

    def test_auto_mode_includes_open_interest_with_range_extending_into_future(self, logger) -> None:
        """Auto mode should include open_interest when the requested range extends
        beyond 'now', clamping the end to 'now'."""
        now = datetime(2026, 6, 10, tzinfo=timezone.utc)
        cutoff = datetime(2026, 5, 11, tzinfo=timezone.utc)
        # Request range starts within the window but ends in the future
        start_ts_ms = int(datetime(2026, 6, 1, tzinfo=timezone.utc).timestamp() * 1000)
        end_ts_ms = int(datetime(2026, 7, 1, tzinfo=timezone.utc).timestamp() * 1000)

        resolved, overrides = _resolve_requested_data_types(
            [AUTO_DATA_TYPE_TOKEN],
            start_ts_ms=start_ts_ms,
            end_ts_ms=end_ts_ms,
            logger=logger,
            now=now,
        )

        assert "open_interest" in resolved
        assert "open_interest" in overrides
        clamped_start, clamped_end = overrides["open_interest"]
        now_ts_ms = int(now.timestamp() * 1000)
        assert clamped_start == start_ts_ms
        assert clamped_end <= now_ts_ms

    def test_auto_mode_no_range_override_when_fully_within_window(self, logger) -> None:
        """When the entire requested range is within the available window,
        open_interest should be included WITHOUT a range override."""
        now = datetime(2026, 6, 10, tzinfo=timezone.utc)
        # Range is entirely within the last 30 days
        start_ts_ms = int(datetime(2026, 6, 1, tzinfo=timezone.utc).timestamp() * 1000)
        end_ts_ms = int(datetime(2026, 6, 5, tzinfo=timezone.utc).timestamp() * 1000)

        resolved, overrides = _resolve_requested_data_types(
            [AUTO_DATA_TYPE_TOKEN],
            start_ts_ms=start_ts_ms,
            end_ts_ms=end_ts_ms,
            logger=logger,
            now=now,
        )

        assert "open_interest" in resolved
        # No override needed - full range is available
        assert "open_interest" not in overrides

    def test_auto_cannot_be_combined_with_specific_types(self, logger) -> None:
        with pytest.raises(CliArgumentError, match="cannot be combined"):
            _resolve_requested_data_types(
                [AUTO_DATA_TYPE_TOKEN, "mark_price"],
                start_ts_ms=1700000000000,
                end_ts_ms=1700086400000,
                logger=logger,
            )

    def test_explicit_old_open_interest_raises(self, logger) -> None:
        start_ts_ms = int(datetime(2024, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
        end_ts_ms = int(datetime(2024, 1, 7, 23, 59, 59, tzinfo=timezone.utc).timestamp() * 1000)

        with pytest.raises(CliArgumentError, match="latest 1 month"):
            _resolve_requested_data_types(
                ["open_interest"],
                start_ts_ms=start_ts_ms,
                end_ts_ms=end_ts_ms,
                logger=logger,
                now=datetime(2026, 6, 10, tzinfo=timezone.utc),
            )


# ---------------------------------------------------------------------------
# Tests: index_price skip behavior
# ---------------------------------------------------------------------------


class TestIndexPriceSync:
    @pytest.mark.asyncio
    async def test_index_price_is_synced_from_index_price_klines(
        self, session_factory, checkpoint_repo, logger,
    ):
        gateway = AsyncMock()
        gateway.fetch_index_price_klines.side_effect = [
            [_index_price_kline_raw(ts_ms=1700000000000)], []
        ]

        results = await run_sync(
            gateway=gateway,
            session_factory=session_factory,
            checkpoint_repo=checkpoint_repo,
            venue="binance",
            symbols=["BTCUSDT"],
            data_types=["index_price"],
            start_ts_ms=1700000000000,
            end_ts_ms=1700086400000,
            logger=logger,
        )

        assert results["BTCUSDT/index_price"] >= 1
        assert gateway.fetch_index_price_klines.await_count == 2
        cp = checkpoint_repo.get("binance", "usdt_perpetual", "BTCUSDT", "index_price", "1h")
        assert cp is not None
        assert cp.status == "ok"


# ---------------------------------------------------------------------------
# Tests: checkpoint resume
# ---------------------------------------------------------------------------


class TestCheckpointResume:
    @pytest.mark.asyncio
    async def test_resume_from_checkpoint(
        self, session_factory, checkpoint_repo, logger,
    ):
        """If a checkpoint exists, sync should resume from checkpoint+1."""
        # Pre-populate checkpoint
        checkpoint_repo.upsert(
            IngestCheckpoint(
                venue="binance",
                market_type="usdt_perpetual",
                symbol="BTCUSDT",
                data_type="funding_rate",
                interval_code=None,
                last_event_ts_ms=1700004000000,
                last_event_dt_utc=datetime(2023, 11, 14, 22, 13, 20, tzinfo=timezone.utc),
                status="ok",
                last_success_at_utc=datetime.now(timezone.utc),
            )
        )

        gateway = AsyncMock()
        gateway.fetch_funding_rates.return_value = []

        results = await run_sync(
            gateway=gateway,
            session_factory=session_factory,
            checkpoint_repo=checkpoint_repo,
            venue="binance",
            symbols=["BTCUSDT"],
            data_types=["funding_rate"],
            start_ts_ms=1700000000000,
            end_ts_ms=1700086400000,
            logger=logger,
        )

        # Gateway should have been called with the resume point, not the original start
        call_kwargs = gateway.fetch_funding_rates.call_args.kwargs
        assert call_kwargs["start_ts_ms"] == 1700004000001  # checkpoint + 1

    @pytest.mark.asyncio
    async def test_no_resume_when_checkpoint_is_old(
        self, session_factory, checkpoint_repo, logger,
    ):
        """If checkpoint timestamp < start_ts_ms, use start_ts_ms."""
        checkpoint_repo.upsert(
            IngestCheckpoint(
                venue="binance",
                market_type="usdt_perpetual",
                symbol="BTCUSDT",
                data_type="funding_rate",
                interval_code=None,
                last_event_ts_ms=1699900000000,  # before start
                last_event_dt_utc=datetime(2023, 11, 14, 22, 13, 20, tzinfo=timezone.utc),
                status="ok",
                last_success_at_utc=datetime.now(timezone.utc),
            )
        )

        gateway = AsyncMock()
        gateway.fetch_funding_rates.return_value = []

        await run_sync(
            gateway=gateway,
            session_factory=session_factory,
            checkpoint_repo=checkpoint_repo,
            venue="binance",
            symbols=["BTCUSDT"],
            data_types=["funding_rate"],
            start_ts_ms=1700000000000,
            end_ts_ms=1700086400000,
            logger=logger,
        )

        call_kwargs = gateway.fetch_funding_rates.call_args.kwargs
        assert call_kwargs["start_ts_ms"] == 1700000000000  # original start

    @pytest.mark.asyncio
    async def test_resume_past_end_skips_sync(
        self, session_factory, checkpoint_repo, logger,
    ):
        """If checkpoint resume point > end_ts_ms, skip the sync entirely."""
        checkpoint_repo.upsert(
            IngestCheckpoint(
                venue="binance",
                market_type="usdt_perpetual",
                symbol="BTCUSDT",
                data_type="funding_rate",
                interval_code=None,
                last_event_ts_ms=1700100000000,  # way past end
                last_event_dt_utc=datetime(2023, 11, 14, 22, 13, 20, tzinfo=timezone.utc),
                status="ok",
                last_success_at_utc=datetime.now(timezone.utc),
            )
        )

        gateway = AsyncMock()
        gateway.fetch_funding_rates.return_value = []

        results = await run_sync(
            gateway=gateway,
            session_factory=session_factory,
            checkpoint_repo=checkpoint_repo,
            venue="binance",
            symbols=["BTCUSDT"],
            data_types=["funding_rate"],
            start_ts_ms=1700000000000,
            end_ts_ms=1700086400000,
            logger=logger,
        )

        assert results["BTCUSDT/funding_rate"] == 0
        gateway.fetch_funding_rates.assert_not_awaited()


# ---------------------------------------------------------------------------
# Tests: error handling
# ---------------------------------------------------------------------------


class TestErrorHandling:
    @pytest.mark.asyncio
    async def test_gateway_error_records_error_checkpoint(
        self, session_factory, checkpoint_repo, logger,
    ):
        """If gateway raises, error is logged and checkpoint is updated."""
        gateway = AsyncMock()
        gateway.fetch_funding_rates.side_effect = Exception("API timeout")

        results = await run_sync(
            gateway=gateway,
            session_factory=session_factory,
            checkpoint_repo=checkpoint_repo,
            venue="binance",
            symbols=["BTCUSDT"],
            data_types=["funding_rate"],
            start_ts_ms=1700000000000,
            end_ts_ms=1700086400000,
            logger=logger,
        )

        assert results["BTCUSDT/funding_rate"] == -1

        # Checkpoint should record the error
        cp = checkpoint_repo.get(
            "binance", "usdt_perpetual", "BTCUSDT", "funding_rate",
        )
        assert cp is not None
        assert cp.status == "error"
        assert "API timeout" in (cp.last_error_message or "")

    @pytest.mark.asyncio
    async def test_error_in_one_data_type_does_not_block_others(
        self, session_factory, checkpoint_repo, logger,
    ):
        """An error in one data type should not prevent other data types from syncing."""
        gateway = AsyncMock()
        gateway.fetch_funding_rates.side_effect = Exception("API error")
        gateway.fetch_mark_price_klines.side_effect = [
            [_mark_price_kline_raw(ts_ms=1700000000000)], [],
        ]
        gateway.fetch_open_interest_hist.side_effect = [
            [_open_interest_raw(ts_ms=1700000000000)], [],
        ]

        results = await run_sync(
            gateway=gateway,
            session_factory=session_factory,
            checkpoint_repo=checkpoint_repo,
            venue="binance",
            symbols=["BTCUSDT"],
            data_types=["funding_rate", "mark_price", "open_interest"],
            start_ts_ms=1700000000000,
            end_ts_ms=1700086400000,
            logger=logger,
        )

        assert results["BTCUSDT/funding_rate"] == -1
        assert results["BTCUSDT/mark_price"] >= 1
        assert results["BTCUSDT/open_interest"] >= 1

    @pytest.mark.asyncio
    async def test_unknown_data_type_is_skipped(
        self, session_factory, checkpoint_repo, logger,
    ):
        gateway = AsyncMock()

        results = await run_sync(
            gateway=gateway,
            session_factory=session_factory,
            checkpoint_repo=checkpoint_repo,
            venue="binance",
            symbols=["BTCUSDT"],
            data_types=["unknown_type"],
            start_ts_ms=1700000000000,
            end_ts_ms=1700086400000,
            logger=logger,
        )

        assert results["BTCUSDT/unknown_type"] == 0


# ---------------------------------------------------------------------------
# Tests: multiple symbols
# ---------------------------------------------------------------------------


class TestMultipleSymbols:
    @pytest.mark.asyncio
    async def test_sync_across_multiple_symbols(
        self, session_factory, checkpoint_repo, logger,
    ):
        """Sync should iterate over all symbols."""
        gateway = AsyncMock()
        gateway.fetch_funding_rates.return_value = []

        await run_sync(
            gateway=gateway,
            session_factory=session_factory,
            checkpoint_repo=checkpoint_repo,
            venue="binance",
            symbols=["BTCUSDT", "ETHUSDT"],
            data_types=["funding_rate"],
            start_ts_ms=1700000000000,
            end_ts_ms=1700086400000,
            logger=logger,
        )

        # Called once per symbol
        assert gateway.fetch_funding_rates.await_count == 2

    @pytest.mark.asyncio
    async def test_all_four_data_types(
        self, session_factory, checkpoint_repo, logger,
    ):
        """All four data types including index_price are handled without error."""
        gateway = AsyncMock()
        gateway.fetch_funding_rates.side_effect = [[_funding_rate_raw()], []]
        gateway.fetch_mark_price_klines.side_effect = [[_mark_price_kline_raw()], []]
        gateway.fetch_open_interest_hist.side_effect = [[_open_interest_raw()], []]
        gateway.fetch_index_price_klines.side_effect = [[_index_price_kline_raw()], []]

        results = await run_sync(
            gateway=gateway,
            session_factory=session_factory,
            checkpoint_repo=checkpoint_repo,
            venue="binance",
            symbols=["BTCUSDT"],
            data_types=["funding_rate", "mark_price", "open_interest", "index_price"],
            start_ts_ms=1700000000000,
            end_ts_ms=1700086400000,
            logger=logger,
        )

        assert results["BTCUSDT/funding_rate"] >= 1
        assert results["BTCUSDT/mark_price"] >= 1
        assert results["BTCUSDT/open_interest"] >= 1
        assert results["BTCUSDT/index_price"] >= 1


class TestAutoOIWithClampedRange:
    """Integration tests: run_sync should pass clamped range to _sync_open_interest
    when open_interest was auto-selected with a range override."""

    @pytest.mark.asyncio
    async def test_run_sync_uses_clamped_range_for_auto_oi(
        self, session_factory, checkpoint_repo, logger,
    ) -> None:
        """When open_interest is auto-selected with a range override, run_sync
        should call _sync_open_interest with the clamped range, not the original."""
        now = datetime(2026, 6, 10, tzinfo=timezone.utc)
        cutoff = datetime(2026, 5, 11, tzinfo=timezone.utc)
        # Original range starts before cutoff, ends after cutoff
        start_ts_ms = int(datetime(2026, 4, 1, tzinfo=timezone.utc).timestamp() * 1000)
        end_ts_ms = int(datetime(2026, 5, 15, tzinfo=timezone.utc).timestamp() * 1000)
        cutoff_ts_ms = int(cutoff.timestamp() * 1000)

        # Use a timestamp inside the clamped range for the mock data
        oi_ts = cutoff_ts_ms + 3600_000  # 1 hour after cutoff
        gateway = AsyncMock()
        gateway.fetch_mark_price_klines.return_value = []
        gateway.fetch_open_interest_hist.side_effect = [
            [_open_interest_raw(ts_ms=oi_ts)], [],
        ]

        # Patch validate_open_interest_history_range so it doesn't fail
        # due to the test's future timestamps vs real clock.
        with patch(
            "YM_data_collection.apps.run_historical_derivatives_sync.validate_open_interest_history_range",
        ):
            results = await run_sync(
                gateway=gateway,
                session_factory=session_factory,
                checkpoint_repo=checkpoint_repo,
                venue="binance",
                symbols=["BTCUSDT"],
                data_types=["mark_price", "open_interest"],
                start_ts_ms=start_ts_ms,
                end_ts_ms=end_ts_ms,
                logger=logger,
                data_type_range_overrides={"open_interest": (cutoff_ts_ms, end_ts_ms)},
            )

        # open_interest should have been called with the clamped start
        assert results["BTCUSDT/open_interest"] >= 1
        call_kwargs = gateway.fetch_open_interest_hist.call_args.kwargs
        assert call_kwargs["start_ts_ms"] >= cutoff_ts_ms
        assert call_kwargs["end_ts_ms"] == end_ts_ms


class TestMainEntryPoint:
    @patch("YM_data_collection.apps.run_historical_derivatives_sync.run_sync", new_callable=AsyncMock)
    @patch("YM_data_collection.apps.run_historical_derivatives_sync.create_session_factory")
    @patch("YM_data_collection.apps.run_historical_derivatives_sync.create_mysql_engine")
    @patch("YM_data_collection.apps.run_historical_derivatives_sync.load_config")
    def test_symbols_fall_back_to_config(
        self,
        mock_load_config,
        mock_create_engine,
        mock_create_session_factory,
        mock_run_sync,
    ) -> None:
        config = MagicMock()
        config.mysql = MagicMock()
        config.binance = MagicMock()
        config.binance.symbols = ["BTCUSDT", "ETHUSDT"]
        mock_load_config.return_value = config
        mock_create_engine.return_value = MagicMock()
        mock_create_session_factory.return_value = MagicMock()
        mock_run_sync.return_value = {}

        gateway = MagicMock()
        gateway.close = AsyncMock()

        with patch("YM_data_collection.adapters.binance_gateway.BinanceGateway", return_value=gateway):
            exit_code = main([
                "--config", "YM_data_collection/config/base.yaml",
                "--env", "dev",
                "--venue", "binance",
                "--start-ts-ms", "2024-1-1",
                "--end-ts-ms", "2024-1-7",
            ])

        assert exit_code == 0
        assert mock_run_sync.await_args.kwargs["symbols"] == ["BTCUSDT", "ETHUSDT"]
