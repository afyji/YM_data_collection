"""Tests for run_resync_range — mocked integration tests."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from YM_data_collection.apps._cli_common import CliArgumentError
from YM_data_collection.apps.run_resync_range import (
    _resync_range,
    _resync_kline,
    _resync_funding_rate,
    _validate_args,
    build_parser,
    main,
)
from YM_data_collection.domain.models import IngestCheckpoint, NormalizedKline


# ---------------------------------------------------------------------------
# Helpers to build realistic Binance data
# ---------------------------------------------------------------------------

# A 1h-aligned timestamp: 2023-11-15 00:00:00 UTC
ALIGNED_TS = 1700006400000


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


def make_funding_rate_raw(ts_ms: int, symbol: str = "BTCUSDT") -> dict[str, Any]:
    """Build a raw Binance funding rate record."""
    return {
        "symbol": symbol,
        "fundingTime": ts_ms,
        "fundingRate": "0.00010000",
        "markPrice": "34567.80000000",
    }


def make_mark_price_kline_raw(open_ts_ms: int) -> list[Any]:
    """Build a raw Binance mark-price kline array."""
    return [
        open_ts_ms,           # 0: open_time
        "34500.00",           # 1: open
        "34600.00",           # 2: high
        "34400.00",           # 3: low
        "34567.80",           # 4: close -> mark_price
        "0",                  # 5: volume
        open_ts_ms + 3_599_999,  # 6: close_time
        "0",                  # 7: quote_volume
        0,                    # 8: trade_count
        "0",                  # 9: taker_buy_base_volume
        "0",                  # 10: taker_buy_quote_volume
        "0",                  # 11: ignore
    ]


def make_open_interest_raw(ts_ms: int, symbol: str = "BTCUSDT") -> dict[str, Any]:
    """Build a raw Binance open-interest history record."""
    return {
        "symbol": symbol,
        "sumOpenInterest": "12345.678",
        "sumOpenInterestValue": "427654321.12",
        "timestamp": ts_ms,
    }


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def gateway():
    """Mock BinanceGateway with AsyncMock for all fetch methods."""
    gw = MagicMock()
    gw.fetch_klines = AsyncMock()
    gw.fetch_funding_rates = AsyncMock()
    gw.fetch_mark_price_klines = AsyncMock()
    gw.fetch_open_interest_hist = AsyncMock()
    gw.close = AsyncMock()
    return gw


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


class TestKlineResync:
    """Test kline resync flow."""

    @pytest.mark.asyncio
    async def test_kline_resync_fetches_and_persists(
        self, gateway, checkpoint_repo, session_factory, logger
    ):
        """Mock kline flow, verify rows persisted."""
        start_ts = ALIGNED_TS
        end_ts = start_ts + 3_600_000
        klines = [make_binance_kline(start_ts)]
        gateway.fetch_klines.return_value = klines

        with patch(
            "YM_data_collection.apps.run_resync_range.KlineRepository"
        ) as MockKlineRepo:
            mock_repo = MagicMock()
            mock_repo.upsert_batch = MagicMock(return_value=1)
            MockKlineRepo.return_value = mock_repo

            total = await _resync_range(
                gateway=gateway,
                session_factory=session_factory,
                checkpoint_repo=checkpoint_repo,
                venue="binance",
                market_type="spot",
                symbol="BTCUSDT",
                data_type="kline",
                interval="1h",
                start_ts_ms=start_ts,
                end_ts_ms=end_ts,
                force=True,
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
        mock_repo.upsert_batch.assert_called_once()
        # Checkpoint should be updated with 'ok' status
        checkpoint_repo.upsert.assert_called()
        last_cp = checkpoint_repo.upsert.call_args[0][0]
        assert last_cp.status == "ok"
        assert last_cp.last_event_ts_ms == start_ts


class TestFundingRateResync:
    """Test funding_rate resync flow."""

    @pytest.mark.asyncio
    async def test_funding_rate_resync(
        self, gateway, checkpoint_repo, session_factory, logger
    ):
        """Mock funding_rate flow, verify rows persisted."""
        start_ts = ALIGNED_TS
        end_ts = start_ts + 28_800_000  # 8 hours
        raw_records = [make_funding_rate_raw(start_ts)]
        # First call returns data, second call returns empty to stop pagination
        gateway.fetch_funding_rates.side_effect = [raw_records, []]

        with patch(
            "YM_data_collection.apps.run_resync_range.FundingRateRepository"
        ) as MockFRRepo:
            mock_repo = MagicMock()
            mock_repo.upsert_batch = MagicMock(return_value=1)
            MockFRRepo.return_value = mock_repo

            total = await _resync_range(
                gateway=gateway,
                session_factory=session_factory,
                checkpoint_repo=checkpoint_repo,
                venue="binance",
                market_type="usdt_perpetual",
                symbol="BTCUSDT",
                data_type="funding_rate",
                interval=None,
                start_ts_ms=start_ts,
                end_ts_ms=end_ts,
                force=True,
                logger=logger,
            )

        assert total == 1
        gateway.fetch_funding_rates.assert_awaited()
        mock_repo.upsert_batch.assert_called_once()
        # Checkpoint updated
        checkpoint_repo.upsert.assert_called()


class TestForceFlag:
    """Test --force flag behavior."""

    @pytest.mark.asyncio
    async def test_force_flag_ignores_checkpoint(
        self, gateway, checkpoint_repo, session_factory, logger
    ):
        """With --force, checkpoint is not checked."""
        start_ts = ALIGNED_TS
        end_ts = start_ts + 3_600_000

        # Set up a checkpoint that covers the range
        existing_cp = IngestCheckpoint(
            venue="binance",
            market_type="spot",
            symbol="BTCUSDT",
            data_type="kline",
            interval_code="1h",
            last_event_ts_ms=end_ts + 10000,  # beyond end_ts_ms
            status="ok",
        )
        checkpoint_repo.get.return_value = existing_cp

        # With force=True, should still fetch data
        klines = [make_binance_kline(start_ts)]
        gateway.fetch_klines.return_value = klines

        with patch(
            "YM_data_collection.apps.run_resync_range.KlineRepository"
        ) as MockKlineRepo:
            mock_repo = MagicMock()
            mock_repo.upsert_batch = MagicMock(return_value=1)
            MockKlineRepo.return_value = mock_repo

            total = await _resync_range(
                gateway=gateway,
                session_factory=session_factory,
                checkpoint_repo=checkpoint_repo,
                venue="binance",
                market_type="spot",
                symbol="BTCUSDT",
                data_type="kline",
                interval="1h",
                start_ts_ms=start_ts,
                end_ts_ms=end_ts,
                force=True,
                logger=logger,
            )

        # Should have fetched data despite checkpoint covering range
        assert total == 1
        gateway.fetch_klines.assert_awaited_once()
        # checkpoint_repo.get should NOT have been called with force=True
        checkpoint_repo.get.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_force_skips_if_already_synced(
        self, gateway, checkpoint_repo, session_factory, logger
    ):
        """Without --force, checkpoint covers range, returns 0."""
        start_ts = ALIGNED_TS
        end_ts = start_ts + 3_600_000

        # Checkpoint that covers the full range
        existing_cp = IngestCheckpoint(
            venue="binance",
            market_type="spot",
            symbol="BTCUSDT",
            data_type="kline",
            interval_code="1h",
            last_event_ts_ms=end_ts,  # exactly at end
            status="ok",
        )
        checkpoint_repo.get.return_value = existing_cp

        total = await _resync_range(
            gateway=gateway,
            session_factory=session_factory,
            checkpoint_repo=checkpoint_repo,
            venue="binance",
            market_type="spot",
            symbol="BTCUSDT",
            data_type="kline",
            interval="1h",
            start_ts_ms=start_ts,
            end_ts_ms=end_ts,
            force=False,
            logger=logger,
        )

        assert total == 0
        # Should NOT have fetched data
        gateway.fetch_klines.assert_not_awaited()
        # Should have checked the checkpoint
        checkpoint_repo.get.assert_called_once()


class TestArgumentValidation:
    """Test argument validation."""

    def test_missing_symbol_raises(self):
        """No --symbol raises CliArgumentError."""
        args = argparse.Namespace(
            symbol=None, data_type="kline", start_ts_ms=1000, interval="1h",
        )
        with pytest.raises(CliArgumentError, match="--symbol is required"):
            _validate_args(args)

    def test_missing_data_type_raises(self):
        """No --data-type raises CliArgumentError."""
        args = argparse.Namespace(
            symbol="BTCUSDT", data_type=None, start_ts_ms=1000, interval="1h",
        )
        with pytest.raises(CliArgumentError, match="--data-type is required"):
            _validate_args(args)

    def test_missing_start_ts_ms_raises(self):
        """No --start-ts-ms raises CliArgumentError."""
        args = argparse.Namespace(
            symbol="BTCUSDT", data_type="kline", start_ts_ms=None, interval="1h",
        )
        with pytest.raises(CliArgumentError, match="--start-ts-ms is required"):
            _validate_args(args)

    def test_kline_requires_interval(self):
        """data_type=kline without --interval raises error."""
        args = argparse.Namespace(
            symbol="BTCUSDT", data_type="kline", start_ts_ms=1000, interval=None,
        )
        with pytest.raises(CliArgumentError, match="--interval is required"):
            _validate_args(args)

    def test_kline_with_interval_passes(self):
        """data_type=kline WITH interval should pass validation."""
        args = argparse.Namespace(
            symbol="BTCUSDT", data_type="kline", start_ts_ms=1000, interval="1h",
        )
        _validate_args(args)  # Should not raise

    def test_non_kline_without_interval_passes(self):
        """data_type=funding_rate without interval should pass validation."""
        args = argparse.Namespace(
            symbol="BTCUSDT", data_type="funding_rate", start_ts_ms=1000, interval=None,
        )
        _validate_args(args)  # Should not raise


class TestPagination:
    """Test that multiple batches are fetched correctly."""

    @pytest.mark.asyncio
    async def test_pagination_two_batches_kline(
        self, gateway, checkpoint_repo, session_factory, logger
    ):
        """Two batches for kline — first batch hits limit, second is smaller."""
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

        with patch(
            "YM_data_collection.apps.run_resync_range.KlineRepository"
        ) as MockKlineRepo:
            mock_repo = MagicMock()
            mock_repo.upsert_batch = MagicMock(return_value=1000)
            MockKlineRepo.return_value = mock_repo

            total = await _resync_kline(
                gateway=gateway,
                session_factory=session_factory,
                checkpoint_repo=checkpoint_repo,
                venue="binance",
                market_type="spot",
                symbol="BTCUSDT",
                interval="1h",
                start_ts_ms=start_ts,
                end_ts_ms=second_batch_start + 5 * interval_ms,
                logger=logger,
            )

        assert total == 2000  # 1000 from each batch
        assert gateway.fetch_klines.await_count == 2
        # Second call should start from last kline open_ts + 1
        second_call = gateway.fetch_klines.call_args_list[1]
        expected_next_start = start_ts + 999 * interval_ms + 1
        assert second_call.kwargs["start_ts_ms"] == expected_next_start


class TestCLIMain:
    """Test main() CLI entry point."""

    def test_build_parser_accepts_date_strings(self):
        parser = build_parser()
        args = parser.parse_args([
            "--symbol", "BTCUSDT",
            "--data-type", "kline",
            "--interval", "1h",
            "--start-ts-ms", "2024-1-1",
            "--end-ts-ms", "2024-1-2",
        ])

        assert args.start_ts_ms == 1704067200000
        assert args.end_ts_ms == 1704239999999

    def test_cli_main_returns_exit_code(self):
        """main([valid args]) returns 0 on success."""
        with patch(
            "YM_data_collection.apps.run_resync_range.run_async",
            new_callable=AsyncMock,
        ) as mock_run_async, \
        patch(
            "YM_data_collection.apps.run_resync_range.load_config"
        ) as mock_load_config, \
        patch(
            "YM_data_collection.apps.run_resync_range.create_mysql_engine"
        ) as mock_engine, \
        patch(
            "YM_data_collection.apps.run_resync_range.create_session_factory"
        ) as mock_session_factory, \
        patch(
            "YM_data_collection.apps.run_resync_range.BinanceGateway"
        ) as mock_gw_class, \
        patch(
            "YM_data_collection.apps.run_resync_range.CheckpointRepository"
        ):

            mock_run_async.return_value = 5
            mock_load_config.return_value = MagicMock()
            mock_engine.return_value = MagicMock()
            mock_session_factory.return_value = MagicMock()
            mock_gw_instance = MagicMock()
            mock_gw_instance.close = AsyncMock()
            mock_gw_class.return_value = mock_gw_instance

            result = main([
                "--symbol", "BTCUSDT",
                "--data-type", "kline",
                "--interval", "1h",
                "--start-ts-ms", "1700006400000",
                "--end-ts-ms", "1700010000000",
            ])

        assert result == 0

    def test_cli_main_missing_symbol_returns_error(self):
        """main() with missing --symbol returns non-zero exit code."""
        result = main([
            "--data-type", "kline",
            "--start-ts-ms", "1700006400000",
        ])
        assert result != 0

    def test_cli_main_missing_data_type_returns_error(self):
        """main() with missing --data-type returns non-zero exit code."""
        result = main([
            "--symbol", "BTCUSDT",
            "--start-ts-ms", "1700006400000",
        ])
        assert result != 0
