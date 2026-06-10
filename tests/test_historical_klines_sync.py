"""Tests for run_historical_klines_sync — mocked integration tests."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from YM_data_collection.apps.run_historical_klines_sync import (
    build_parser,
    _sync_symbol_interval,
    _update_checkpoint,
)
from YM_data_collection.domain.models import IngestCheckpoint, NormalizedKline


# ---------------------------------------------------------------------------
# Helpers to build realistic Binance kline arrays
# ---------------------------------------------------------------------------

# A 1h-aligned timestamp: 2023-11-15 00:00:00 UTC
ALIGNED_TS = 1700006400000  # 1700006400000 % 3_600_000 == 0


def make_binance_kline(
    open_ts_ms: int,
    interval_ms: int = 3_600_000,
    open_price: str = "50000.00",
    high_price: str = "50500.00",
    low_price: str = "49500.00",
    close_price: str = "50200.00",
    volume: str = "123.456",
) -> list[Any]:
    """Build a realistic 12-element Binance kline array."""
    close_ts_ms = open_ts_ms + interval_ms - 1
    return [
        open_ts_ms,           # 0: Open time
        open_price,           # 1: Open
        high_price,           # 2: High
        low_price,            # 3: Low
        close_price,          # 4: Close
        volume,               # 5: Volume
        close_ts_ms,          # 6: Close time
        "6172800.00",         # 7: Quote asset volume
        1000,                 # 8: Number of trades
        "61.728",             # 9: Taker buy base asset volume
        "3086400.00",         # 10: Taker buy quote asset volume
        "0",                  # 11: Unused field
    ]


def make_normalized_kline(open_ts_ms: int) -> NormalizedKline:
    """Build a NormalizedKline for spot/BTCUSDT/1h."""
    interval_ms = 3_600_000
    close_ts_ms = open_ts_ms + interval_ms - 1
    return NormalizedKline(
        venue="binance",
        symbol="BTCUSDT",
        instrument_code="crypto.binance.spot.BTCUSDT",
        interval_code="1h",
        open_ts_ms=open_ts_ms,
        close_ts_ms=close_ts_ms,
        open_dt_utc=datetime.fromtimestamp(open_ts_ms / 1000.0, tz=timezone.utc),
        close_dt_utc=datetime.fromtimestamp(close_ts_ms / 1000.0, tz=timezone.utc),
        open_price=Decimal("50000.00"),
        high_price=Decimal("50500.00"),
        low_price=Decimal("49500.00"),
        close_price=Decimal("50200.00"),
        volume=Decimal("123.456"),
        quote_volume=Decimal("6172800.00"),
        trade_count=1000,
        taker_buy_base_volume=Decimal("61.728"),
        taker_buy_quote_volume=Decimal("3086400.00"),
        source="exchange",
        market_type="spot",
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def gateway():
    """Mock BinanceGateway with AsyncMock for fetch_klines."""
    gw = MagicMock()
    gw.fetch_klines = AsyncMock()
    gw.close = AsyncMock()
    return gw


@pytest.fixture()
def kline_repo():
    """Mock KlineRepository."""
    repo = MagicMock()
    repo.upsert_batch = MagicMock(return_value=0)
    return repo


@pytest.fixture()
def checkpoint_repo():
    """Mock CheckpointRepository."""
    repo = MagicMock()
    repo.get = MagicMock(return_value=None)
    repo.upsert = MagicMock()
    return repo


@pytest.fixture()
def session_factory():
    """Dummy session factory (not used in mocked tests)."""
    return MagicMock()


@pytest.fixture()
def logger():
    """Mock logger."""
    return MagicMock()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestParserDefaults:
    def test_default_intervals_are_core_1m_1h_1d(self):
        parser = build_parser()
        args = parser.parse_args([
            "--market-type", "perp",
            "--symbols", "BTCUSDT",
            "--start-ts-ms", "2024-1-1",
            "--end-ts-ms", "2024-1-2",
        ])

        assert args.intervals == ["1m", "1h", "1d"]

    def test_help_mentions_core_default_intervals(self):
        help_text = " ".join(build_parser().format_help().split())
        assert "Default: 1m 1h 1d" in help_text



class TestSingleBatchSync:
    """Single batch — all data fits in one fetch."""

    @pytest.mark.asyncio
    async def test_single_batch_persists_and_updates_checkpoint(
        self, gateway, kline_repo, checkpoint_repo, session_factory, logger
    ):
        start_ts = ALIGNED_TS
        end_ts = start_ts + 3_600_000
        klines = [make_binance_kline(start_ts)]
        gateway.fetch_klines.return_value = klines
        kline_repo.upsert_batch.return_value = 1

        total = await _sync_symbol_interval(
            gateway=gateway,
            kline_repo=kline_repo,
            checkpoint_repo=checkpoint_repo,
            session_factory=session_factory,
            venue="binance",
            market_type="spot",
            symbol="BTCUSDT",
            interval="1h",
            start_ts_ms=start_ts,
            end_ts_ms=end_ts,
            dry_run=False,
            logger=logger,
        )

        assert total == 1
        gateway.fetch_klines.assert_awaited_once_with(
            market_type="spot",
            symbol="BTCUSDT",
            interval="1h",
            start_ts_ms=start_ts,
            end_ts_ms=end_ts,
            limit=1000,
        )
        kline_repo.upsert_batch.assert_called_once()
        # Check checkpoint was updated with 'ok' status
        checkpoint_repo.upsert.assert_called()
        last_call_cp = checkpoint_repo.upsert.call_args[0][0]
        assert last_call_cp.status == "ok"
        assert last_call_cp.last_kline_open_ts_ms == start_ts

    @pytest.mark.asyncio
    async def test_single_batch_no_data(
        self, gateway, kline_repo, checkpoint_repo, session_factory, logger
    ):
        gateway.fetch_klines.return_value = []

        total = await _sync_symbol_interval(
            gateway=gateway,
            kline_repo=kline_repo,
            checkpoint_repo=checkpoint_repo,
            session_factory=session_factory,
            venue="binance",
            market_type="spot",
            symbol="BTCUSDT",
            interval="1h",
            start_ts_ms=ALIGNED_TS,
            end_ts_ms=ALIGNED_TS + 3_600_000,
            dry_run=False,
            logger=logger,
        )

        assert total == 0
        kline_repo.upsert_batch.assert_not_called()
        checkpoint_repo.upsert.assert_not_called()


class TestPagination:
    """Two batches — data spans two fetches."""

    @pytest.mark.asyncio
    async def test_two_batches(
        self, gateway, kline_repo, checkpoint_repo, session_factory, logger
    ):
        start_ts = ALIGNED_TS
        # First batch: 3 klines, less than limit => stops after
        first_batch = [
            make_binance_kline(start_ts),
            make_binance_kline(start_ts + 3_600_000),
            make_binance_kline(start_ts + 7_200_000),
        ]
        gateway.fetch_klines.return_value = first_batch
        kline_repo.upsert_batch.return_value = 3

        total = await _sync_symbol_interval(
            gateway=gateway,
            kline_repo=kline_repo,
            checkpoint_repo=checkpoint_repo,
            session_factory=session_factory,
            venue="binance",
            market_type="spot",
            symbol="BTCUSDT",
            interval="1h",
            start_ts_ms=start_ts,
            end_ts_ms=start_ts + 10_800_000,
            dry_run=False,
            logger=logger,
        )

        assert total == 3
        # fetch_klines called once since batch size < limit
        gateway.fetch_klines.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_two_batches_exact_limit(
        self, gateway, kline_repo, checkpoint_repo, session_factory, logger
    ):
        start_ts = ALIGNED_TS
        interval_ms = 3_600_000
        # First batch: exactly 1000 items (spot limit), so pagination continues
        first_batch = [
            make_binance_kline(start_ts + i * interval_ms)
            for i in range(1000)
        ]
        # Second batch: 5 items (< limit => done)
        second_batch_start = start_ts + 1000 * interval_ms
        second_batch = [
            make_binance_kline(second_batch_start + i * interval_ms)
            for i in range(5)
        ]
        gateway.fetch_klines.side_effect = [first_batch, second_batch]
        kline_repo.upsert_batch.return_value = 1000

        total = await _sync_symbol_interval(
            gateway=gateway,
            kline_repo=kline_repo,
            checkpoint_repo=checkpoint_repo,
            session_factory=session_factory,
            venue="binance",
            market_type="spot",
            symbol="BTCUSDT",
            interval="1h",
            start_ts_ms=start_ts,
            end_ts_ms=second_batch_start + 5 * interval_ms,
            dry_run=False,
            logger=logger,
        )

        assert total == 2000  # 1000 from first + 1000 from second (mock returns 1000 each)
        assert gateway.fetch_klines.await_count == 2
        # Second call should start from last kline open_ts + 1
        second_call = gateway.fetch_klines.call_args_list[1]
        expected_next_start = start_ts + 999 * interval_ms + 1
        assert second_call.kwargs["start_ts_ms"] == expected_next_start


class TestDryRun:
    """Dry-run mode should skip MySQL writes."""

    @pytest.mark.asyncio
    async def test_dry_run_skips_persist(
        self, gateway, kline_repo, checkpoint_repo, session_factory, logger
    ):
        start_ts = ALIGNED_TS
        klines = [make_binance_kline(start_ts)]
        gateway.fetch_klines.return_value = klines

        total = await _sync_symbol_interval(
            gateway=gateway,
            kline_repo=kline_repo,
            checkpoint_repo=checkpoint_repo,
            session_factory=session_factory,
            venue="binance",
            market_type="spot",
            symbol="BTCUSDT",
            interval="1h",
            start_ts_ms=start_ts,
            end_ts_ms=start_ts + 3_600_000,
            dry_run=True,
            logger=logger,
        )

        # Total rows counted but not persisted
        assert total == 1
        kline_repo.upsert_batch.assert_not_called()
        # Checkpoint should NOT be updated in dry-run mode
        checkpoint_repo.upsert.assert_not_called()


class TestCheckpointResume:
    """Checkpoint determines start point when no --start-ts-ms override."""

    @pytest.mark.asyncio
    async def test_resumes_from_checkpoint(
        self, gateway, kline_repo, checkpoint_repo, session_factory, logger
    ):
        start_ts = ALIGNED_TS
        checkpoint_ts = start_ts + 3_600_000  # checkpoint is 1 hour in
        # Set up checkpoint
        existing_cp = IngestCheckpoint(
            venue="binance",
            market_type="spot",
            symbol="BTCUSDT",
            data_type="kline",
            interval_code="1h",
            last_kline_open_ts_ms=checkpoint_ts,
            status="ok",
        )
        checkpoint_repo.get.return_value = existing_cp

        # New data from after checkpoint
        new_ts = checkpoint_ts + 3_600_000
        new_klines = [make_binance_kline(new_ts)]
        gateway.fetch_klines.return_value = new_klines
        kline_repo.upsert_batch.return_value = 1

        total = await _sync_symbol_interval(
            gateway=gateway,
            kline_repo=kline_repo,
            checkpoint_repo=checkpoint_repo,
            session_factory=session_factory,
            venue="binance",
            market_type="spot",
            symbol="BTCUSDT",
            interval="1h",
            start_ts_ms=start_ts,
            end_ts_ms=new_ts + 3_600_000,
            dry_run=False,
            logger=logger,
        )

        assert total == 1
        # Should have fetched starting from checkpoint_ts + 1
        call_args = gateway.fetch_klines.call_args
        assert call_args.kwargs["start_ts_ms"] == checkpoint_ts + 1

    @pytest.mark.asyncio
    async def test_no_checkpoint_uses_start_ts(
        self, gateway, kline_repo, checkpoint_repo, session_factory, logger
    ):
        start_ts = ALIGNED_TS
        # No checkpoint (default mock returns None)
        klines = [make_binance_kline(start_ts)]
        gateway.fetch_klines.return_value = klines
        kline_repo.upsert_batch.return_value = 1

        total = await _sync_symbol_interval(
            gateway=gateway,
            kline_repo=kline_repo,
            checkpoint_repo=checkpoint_repo,
            session_factory=session_factory,
            venue="binance",
            market_type="spot",
            symbol="BTCUSDT",
            interval="1h",
            start_ts_ms=start_ts,
            end_ts_ms=start_ts + 3_600_000,
            dry_run=False,
            logger=logger,
        )

        assert total == 1
        call_args = gateway.fetch_klines.call_args
        assert call_args.kwargs["start_ts_ms"] == start_ts


class TestErrorHandling:
    """Error handling: fetch error, persist error."""

    @pytest.mark.asyncio
    async def test_fetch_error_raises_and_sets_checkpoint_error(
        self, gateway, kline_repo, checkpoint_repo, session_factory, logger
    ):
        start_ts = ALIGNED_TS
        gateway.fetch_klines.side_effect = ConnectionError("Binance timeout")

        with pytest.raises(ConnectionError, match="Binance timeout"):
            await _sync_symbol_interval(
                gateway=gateway,
                kline_repo=kline_repo,
                checkpoint_repo=checkpoint_repo,
                session_factory=session_factory,
                venue="binance",
                market_type="spot",
                symbol="BTCUSDT",
                interval="1h",
                start_ts_ms=start_ts,
                end_ts_ms=start_ts + 3_600_000,
                dry_run=False,
                logger=logger,
            )

        # Checkpoint should be set to error status
        checkpoint_repo.upsert.assert_called_once()
        cp = checkpoint_repo.upsert.call_args[0][0]
        assert cp.status == "error"
        assert "Binance timeout" in cp.last_error_message

    @pytest.mark.asyncio
    async def test_persist_error_raises_and_sets_checkpoint_error(
        self, gateway, kline_repo, checkpoint_repo, session_factory, logger
    ):
        start_ts = ALIGNED_TS
        klines = [make_binance_kline(start_ts)]
        gateway.fetch_klines.return_value = klines
        kline_repo.upsert_batch.side_effect = RuntimeError("MySQL down")

        with pytest.raises(RuntimeError, match="MySQL down"):
            await _sync_symbol_interval(
                gateway=gateway,
                kline_repo=kline_repo,
                checkpoint_repo=checkpoint_repo,
                session_factory=session_factory,
                venue="binance",
                market_type="spot",
                symbol="BTCUSDT",
                interval="1h",
                start_ts_ms=start_ts,
                end_ts_ms=start_ts + 3_600_000,
                dry_run=False,
                logger=logger,
            )

        # Checkpoint should be set to error status
        checkpoint_repo.upsert.assert_called_once()
        cp = checkpoint_repo.upsert.call_args[0][0]
        assert cp.status == "error"
        assert "MySQL down" in cp.last_error_message


class TestMarketTypeRouting:
    """Spot vs perp uses different limits and table names."""

    @pytest.mark.asyncio
    async def test_perp_uses_1500_limit(
        self, gateway, kline_repo, checkpoint_repo, session_factory, logger
    ):
        start_ts = ALIGNED_TS
        klines = [make_binance_kline(start_ts)]
        gateway.fetch_klines.return_value = klines
        kline_repo.upsert_batch.return_value = 1

        total = await _sync_symbol_interval(
            gateway=gateway,
            kline_repo=kline_repo,
            checkpoint_repo=checkpoint_repo,
            session_factory=session_factory,
            venue="binance",
            market_type="perp",
            symbol="BTCUSDT",
            interval="1h",
            start_ts_ms=start_ts,
            end_ts_ms=start_ts + 3_600_000,
            dry_run=False,
            logger=logger,
        )

        assert total == 1
        call_args = gateway.fetch_klines.call_args
        assert call_args.kwargs["limit"] == 1500
        # Verify table_name used in upsert
        upsert_call = kline_repo.upsert_batch.call_args
        assert upsert_call[0][1] == "perp_klines"

    @pytest.mark.asyncio
    async def test_spot_uses_1000_limit(
        self, gateway, kline_repo, checkpoint_repo, session_factory, logger
    ):
        start_ts = ALIGNED_TS
        klines = [make_binance_kline(start_ts)]
        gateway.fetch_klines.return_value = klines
        kline_repo.upsert_batch.return_value = 1

        await _sync_symbol_interval(
            gateway=gateway,
            kline_repo=kline_repo,
            checkpoint_repo=checkpoint_repo,
            session_factory=session_factory,
            venue="binance",
            market_type="spot",
            symbol="BTCUSDT",
            interval="1h",
            start_ts_ms=start_ts,
            end_ts_ms=start_ts + 3_600_000,
            dry_run=False,
            logger=logger,
        )

        call_args = gateway.fetch_klines.call_args
        assert call_args.kwargs["limit"] == 1000
        upsert_call = kline_repo.upsert_batch.call_args
        assert upsert_call[0][1] == "spot_klines"


class TestUpdateCheckpoint:
    """Unit tests for _update_checkpoint helper."""

    def test_ok_status_sets_last_kline_open_ts(self, checkpoint_repo):
        _update_checkpoint(
            checkpoint_repo=checkpoint_repo,
            venue="binance",
            market_type="spot",
            symbol="BTCUSDT",
            interval="1h",
            data_type="kline",
            status="ok",
            last_kline_open_ts_ms=ALIGNED_TS,
            error_message=None,
        )

        checkpoint_repo.upsert.assert_called_once()
        cp = checkpoint_repo.upsert.call_args[0][0]
        assert cp.status == "ok"
        assert cp.last_kline_open_ts_ms == ALIGNED_TS
        assert cp.last_error_message is None
        assert cp.last_success_at_utc is not None

    def test_error_status_sets_error_message(self, checkpoint_repo):
        _update_checkpoint(
            checkpoint_repo=checkpoint_repo,
            venue="binance",
            market_type="spot",
            symbol="BTCUSDT",
            interval="1h",
            data_type="kline",
            status="error",
            last_kline_open_ts_ms=None,
            error_message="timeout",
        )

        checkpoint_repo.upsert.assert_called_once()
        cp = checkpoint_repo.upsert.call_args[0][0]
        assert cp.status == "error"
        assert cp.last_kline_open_ts_ms is None
        assert cp.last_error_message == "timeout"
        assert cp.last_success_at_utc is None


class TestStartAfterEnd:
    """When start_ts_ms > end_ts_ms, nothing happens."""

    @pytest.mark.asyncio
    async def test_start_after_end_returns_zero(
        self, gateway, kline_repo, checkpoint_repo, session_factory, logger
    ):
        total = await _sync_symbol_interval(
            gateway=gateway,
            kline_repo=kline_repo,
            checkpoint_repo=checkpoint_repo,
            session_factory=session_factory,
            venue="binance",
            market_type="spot",
            symbol="BTCUSDT",
            interval="1h",
            start_ts_ms=ALIGNED_TS + 3_600_000,
            end_ts_ms=ALIGNED_TS,
            dry_run=False,
            logger=logger,
        )

        assert total == 0
        gateway.fetch_klines.assert_not_awaited()
        kline_repo.upsert_batch.assert_not_called()
