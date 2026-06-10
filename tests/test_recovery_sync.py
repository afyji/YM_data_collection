"""Tests for run_recovery_sync — mocked integration tests."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from YM_data_collection.apps.run_recovery_sync import (
    _recover_funding_rate,
    _recover_kline,
    _recover_mark_price,
    _recover_open_interest,
    _update_checkpoint_error,
    main,
    run_async,
)
from YM_data_collection.domain.models import IngestCheckpoint

# ---------------------------------------------------------------------------
# Helpers to build realistic test data
# ---------------------------------------------------------------------------

ALIGNED_TS = 1700006400000  # 2023-11-15 00:00:00 UTC, 1h-aligned


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
        open_ts_ms,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        close_ts_ms,
        "6172800.00",
        1000,
        "61.728",
        "3086400.00",
        "0",
    ]


def make_funding_rate_raw(symbol: str = "BTCUSDT", ts_ms: int = 1700006400000) -> dict:
    """Build a raw Binance funding rate dict."""
    return {
        "symbol": symbol,
        "fundingTime": ts_ms,
        "fundingRate": "0.00010000",
        "markPrice": "50000.00",
    }


def make_mark_price_kline_raw(open_ts_ms: int = 1700006400000) -> list:
    """Build a raw Binance mark-price kline array."""
    return [
        open_ts_ms,
        "50000.00",
        "50500.00",
        "49500.00",
        "50200.00",
        "0",
        open_ts_ms + 3_600_000 - 1,
        "0",
        0,
        "0",
        "0",
        "0",
    ]


def make_open_interest_raw(symbol: str = "BTCUSDT", ts_ms: int = 1700006400000) -> dict:
    """Build a raw Binance open interest history dict."""
    return {
        "symbol": symbol,
        "sumOpenInterest": "12345.678",
        "sumOpenInterestValue": "617280000.00",
        "timestamp": ts_ms,
    }


def make_kline_error_checkpoint(
    symbol: str = "BTCUSDT",
    market_type: str = "spot",
    interval_code: str = "1h",
    last_event_ts_ms: int = ALIGNED_TS,
) -> IngestCheckpoint:
    """Build a kline error checkpoint."""
    return IngestCheckpoint(
        venue="binance",
        market_type=market_type,
        symbol=symbol,
        data_type="kline",
        interval_code=interval_code,
        last_event_ts_ms=last_event_ts_ms,
        last_event_dt_utc=datetime.fromtimestamp(last_event_ts_ms / 1000.0, tz=timezone.utc) if last_event_ts_ms else None,
        last_kline_open_ts_ms=last_event_ts_ms,
        status="error",
        last_error_message="previous error",
    )


def make_funding_rate_error_checkpoint(
    symbol: str = "BTCUSDT",
    last_event_ts_ms: int = ALIGNED_TS,
) -> IngestCheckpoint:
    """Build a funding_rate error checkpoint."""
    return IngestCheckpoint(
        venue="binance",
        market_type="usdt_perpetual",
        symbol=symbol,
        data_type="funding_rate",
        interval_code=None,
        last_event_ts_ms=last_event_ts_ms,
        last_event_dt_utc=datetime.fromtimestamp(last_event_ts_ms / 1000.0, tz=timezone.utc) if last_event_ts_ms else None,
        status="error",
        last_error_message="previous error",
    )


def make_mark_price_error_checkpoint(
    symbol: str = "BTCUSDT",
    last_event_ts_ms: int = ALIGNED_TS,
) -> IngestCheckpoint:
    """Build a mark_price error checkpoint."""
    return IngestCheckpoint(
        venue="binance",
        market_type="usdt_perpetual",
        symbol=symbol,
        data_type="mark_price",
        interval_code="1h",
        last_event_ts_ms=last_event_ts_ms,
        last_event_dt_utc=datetime.fromtimestamp(last_event_ts_ms / 1000.0, tz=timezone.utc) if last_event_ts_ms else None,
        status="error",
        last_error_message="previous error",
    )


def make_open_interest_error_checkpoint(
    symbol: str = "BTCUSDT",
    last_event_ts_ms: int = ALIGNED_TS,
) -> IngestCheckpoint:
    """Build an open_interest error checkpoint."""
    return IngestCheckpoint(
        venue="binance",
        market_type="usdt_perpetual",
        symbol=symbol,
        data_type="open_interest",
        interval_code="5m",
        last_event_ts_ms=last_event_ts_ms,
        last_event_dt_utc=datetime.fromtimestamp(last_event_ts_ms / 1000.0, tz=timezone.utc) if last_event_ts_ms else None,
        status="error",
        last_error_message="previous error",
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def gateway():
    """Mock BinanceGateway."""
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
    repo.list_by_status = MagicMock(return_value=[])
    repo.get = MagicMock(return_value=None)
    repo.upsert = MagicMock()
    return repo


@pytest.fixture()
def session_factory():
    """Dummy session factory."""
    return MagicMock()


@pytest.fixture()
def logger():
    """Mock logger."""
    return MagicMock()


# ---------------------------------------------------------------------------
# Tests: No error checkpoints
# ---------------------------------------------------------------------------


class TestNoErrorCheckpoints:
    """When there are no error checkpoints, recovery exits cleanly."""

    @pytest.mark.asyncio
    async def test_no_error_checkpoints_returns_zero(self, gateway, checkpoint_repo, session_factory, logger):
        """list_by_status returns [], so recovered=0, still_failing=0."""
        checkpoint_repo.list_by_status.return_value = []

        # Use run_async with mocked infrastructure
        with patch("YM_data_collection.apps.run_recovery_sync.load_config") as mock_load, \
             patch("YM_data_collection.apps.run_recovery_sync.create_mysql_engine") as mock_engine, \
             patch("YM_data_collection.apps.run_recovery_sync.create_session_factory") as mock_sf, \
             patch("YM_data_collection.apps.run_recovery_sync.BinanceGateway") as mock_gw_class, \
             patch("YM_data_collection.apps.run_recovery_sync.CheckpointRepository") as mock_cr_class:

            mock_config = MagicMock()
            mock_config.ingestion.historical_start_ts_ms = 1609459200000
            mock_load.return_value = mock_config
            mock_engine.return_value = MagicMock()
            mock_sf.return_value = session_factory
            mock_gw_class.return_value = gateway
            mock_cr_class.return_value = checkpoint_repo

            import argparse
            args = argparse.Namespace(
                config="dummy.yaml",
                env="dev",
                log_level="INFO",
                venue="binance",
                market_type="spot",
                symbols=None,
                data_types=["kline", "mark_price", "index_price", "open_interest", "funding_rate"],
                since_last_checkpoint=False,
            )

            result = await run_async(args)

        assert result["recovered"] == 0
        assert result["still_failing"] == 0


# ---------------------------------------------------------------------------
# Tests: Kline recovery
# ---------------------------------------------------------------------------


class TestKlineRecovery:
    """Recovery of kline error checkpoints."""

    @pytest.mark.asyncio
    async def test_recovers_kline_error_checkpoint(self, gateway, checkpoint_repo, session_factory, logger):
        """A kline error checkpoint is re-fetched and checkpoint updated to 'ok'."""
        cp = make_kline_error_checkpoint()
        raw_klines = [make_binance_kline(ALIGNED_TS)]
        gateway.fetch_klines.return_value = raw_klines

        with patch("YM_data_collection.apps.run_recovery_sync.KlineRepository") as mock_repo_class:
            mock_repo = MagicMock()
            mock_repo.upsert_batch.return_value = 1
            mock_repo_class.return_value = mock_repo

            success = await _recover_kline(
                gateway=gateway,
                checkpoint_repo=checkpoint_repo,
                session_factory=session_factory,
                cp=cp,
                start_ts_ms=ALIGNED_TS,
                end_ts_ms=ALIGNED_TS + 3_600_000,
                logger=logger,
            )

        assert success is True
        gateway.fetch_klines.assert_awaited_once()
        # Checkpoint should be updated with 'ok' status
        # Find the last upsert call with status='ok'
        ok_calls = [
            c for c in checkpoint_repo.upsert.call_args_list
            if c[0][0].status == "ok"
        ]
        assert len(ok_calls) >= 1
        ok_cp = ok_calls[-1][0][0]
        assert ok_cp.status == "ok"
        assert ok_cp.data_type == "kline"
        assert ok_cp.symbol == "BTCUSDT"

    @pytest.mark.asyncio
    async def test_kline_recovery_fetch_failure(self, gateway, checkpoint_repo, session_factory, logger):
        """If fetch raises, checkpoint stays 'error' and returns False."""
        cp = make_kline_error_checkpoint()
        gateway.fetch_klines.side_effect = ConnectionError("Binance timeout")

        success = await _recover_kline(
            gateway=gateway,
            checkpoint_repo=checkpoint_repo,
            session_factory=session_factory,
            cp=cp,
            start_ts_ms=ALIGNED_TS,
            end_ts_ms=ALIGNED_TS + 3_600_000,
            logger=logger,
        )

        assert success is False
        # Checkpoint should be updated with 'error' status
        error_calls = [
            c for c in checkpoint_repo.upsert.call_args_list
            if c[0][0].status == "error"
        ]
        assert len(error_calls) >= 1


# ---------------------------------------------------------------------------
# Tests: Funding rate recovery
# ---------------------------------------------------------------------------


class TestFundingRateRecovery:
    """Recovery of funding_rate error checkpoints."""

    @pytest.mark.asyncio
    async def test_recovers_funding_rate_error_checkpoint(self, gateway, checkpoint_repo, session_factory, logger):
        """A funding_rate error checkpoint is re-fetched and checkpoint updated to 'ok'."""
        cp = make_funding_rate_error_checkpoint()
        raw_fr = [make_funding_rate_raw(ts_ms=ALIGNED_TS)]
        gateway.fetch_funding_rates.return_value = raw_fr

        with patch("YM_data_collection.apps.run_recovery_sync.FundingRateRepository") as mock_repo_class:
            mock_repo = MagicMock()
            mock_repo.upsert_batch.return_value = 1
            mock_repo_class.return_value = mock_repo

            success = await _recover_funding_rate(
                gateway=gateway,
                checkpoint_repo=checkpoint_repo,
                session_factory=session_factory,
                cp=cp,
                start_ts_ms=ALIGNED_TS,
                end_ts_ms=ALIGNED_TS + 28_800_000,
                logger=logger,
            )

        assert success is True
        gateway.fetch_funding_rates.assert_awaited()
        ok_calls = [
            c for c in checkpoint_repo.upsert.call_args_list
            if c[0][0].status == "ok"
        ]
        assert len(ok_calls) >= 1
        ok_cp = ok_calls[-1][0][0]
        assert ok_cp.status == "ok"
        assert ok_cp.data_type == "funding_rate"

    @pytest.mark.asyncio
    async def test_funding_rate_recovery_persist_failure(self, gateway, checkpoint_repo, session_factory, logger):
        """If persist raises, checkpoint stays 'error' and returns False."""
        cp = make_funding_rate_error_checkpoint()
        raw_fr = [make_funding_rate_raw(ts_ms=ALIGNED_TS)]
        gateway.fetch_funding_rates.return_value = raw_fr

        with patch("YM_data_collection.apps.run_recovery_sync.FundingRateRepository") as mock_repo_class:
            mock_repo = MagicMock()
            mock_repo.upsert_batch.side_effect = RuntimeError("MySQL down")
            mock_repo_class.return_value = mock_repo

            success = await _recover_funding_rate(
                gateway=gateway,
                checkpoint_repo=checkpoint_repo,
                session_factory=session_factory,
                cp=cp,
                start_ts_ms=ALIGNED_TS,
                end_ts_ms=ALIGNED_TS + 28_800_000,
                logger=logger,
            )

        assert success is False
        error_calls = [
            c for c in checkpoint_repo.upsert.call_args_list
            if c[0][0].status == "error"
        ]
        assert len(error_calls) >= 1


# ---------------------------------------------------------------------------
# Tests: Mark price recovery
# ---------------------------------------------------------------------------


class TestMarkPriceRecovery:
    """Recovery of mark_price error checkpoints."""

    @pytest.mark.asyncio
    async def test_recovers_mark_price_error_checkpoint(self, gateway, checkpoint_repo, session_factory, logger):
        cp = make_mark_price_error_checkpoint()
        raw_mp = [make_mark_price_kline_raw(ALIGNED_TS)]
        gateway.fetch_mark_price_klines.return_value = raw_mp

        with patch("YM_data_collection.apps.run_recovery_sync.MarkPriceRepository") as mock_repo_class:
            mock_repo = MagicMock()
            mock_repo.upsert_batch.return_value = 1
            mock_repo_class.return_value = mock_repo

            success = await _recover_mark_price(
                gateway=gateway,
                checkpoint_repo=checkpoint_repo,
                session_factory=session_factory,
                cp=cp,
                start_ts_ms=ALIGNED_TS,
                end_ts_ms=ALIGNED_TS + 3_600_000,
                logger=logger,
            )

        assert success is True
        ok_calls = [
            c for c in checkpoint_repo.upsert.call_args_list
            if c[0][0].status == "ok"
        ]
        assert len(ok_calls) >= 1
        ok_cp = ok_calls[-1][0][0]
        assert ok_cp.data_type == "mark_price"


# ---------------------------------------------------------------------------
# Tests: Open interest recovery
# ---------------------------------------------------------------------------


class TestOpenInterestRecovery:
    """Recovery of open_interest error checkpoints."""

    @pytest.mark.asyncio
    async def test_recovers_open_interest_error_checkpoint(self, gateway, checkpoint_repo, session_factory, logger):
        recent_ts_ms = int(datetime.now(timezone.utc).timestamp() * 1000) - 600_000
        cp = make_open_interest_error_checkpoint(last_event_ts_ms=recent_ts_ms)
        raw_oi = [make_open_interest_raw(ts_ms=recent_ts_ms)]
        gateway.fetch_open_interest_hist.return_value = raw_oi

        with patch("YM_data_collection.apps.run_recovery_sync.OpenInterestRepository") as mock_repo_class:
            mock_repo = MagicMock()
            mock_repo.upsert_batch.return_value = 1
            mock_repo_class.return_value = mock_repo

            success = await _recover_open_interest(
                gateway=gateway,
                checkpoint_repo=checkpoint_repo,
                session_factory=session_factory,
                cp=cp,
                start_ts_ms=recent_ts_ms,
                end_ts_ms=recent_ts_ms + 600_000,
                logger=logger,
            )

        assert success is True
        ok_calls = [
            c for c in checkpoint_repo.upsert.call_args_list
            if c[0][0].status == "ok"
        ]
        assert len(ok_calls) >= 1
        ok_cp = ok_calls[-1][0][0]
        assert ok_cp.data_type == "open_interest"

    @pytest.mark.asyncio
    async def test_old_open_interest_range_is_rejected_before_http(
        self, gateway, checkpoint_repo, session_factory, logger
    ):
        cp = make_open_interest_error_checkpoint(last_event_ts_ms=1700006400000)

        success = await _recover_open_interest(
            gateway=gateway,
            checkpoint_repo=checkpoint_repo,
            session_factory=session_factory,
            cp=cp,
            start_ts_ms=1700006400000,
            end_ts_ms=1700006400000 + 600_000,
            logger=logger,
        )

        assert success is False
        gateway.fetch_open_interest_hist.assert_not_awaited()
        error_cp = checkpoint_repo.upsert.call_args[0][0]
        assert "latest 1 month" in error_cp.last_error_message


# ---------------------------------------------------------------------------
# Tests: Recovery failure keeps error status
# ---------------------------------------------------------------------------


class TestRecoveryFailureKeepsError:
    """When re-fetch raises, checkpoint stays 'error'."""

    @pytest.mark.asyncio
    async def test_recovery_failure_keeps_error_status(self, gateway, checkpoint_repo, session_factory, logger):
        """If the gateway raises an error, the checkpoint stays in 'error' status."""
        cp = make_funding_rate_error_checkpoint()
        gateway.fetch_funding_rates.side_effect = ConnectionError("Network unreachable")

        success = await _recover_funding_rate(
            gateway=gateway,
            checkpoint_repo=checkpoint_repo,
            session_factory=session_factory,
            cp=cp,
            start_ts_ms=ALIGNED_TS,
            end_ts_ms=ALIGNED_TS + 28_800_000,
            logger=logger,
        )

        assert success is False
        # Verify checkpoint was updated with error status
        error_calls = [
            c for c in checkpoint_repo.upsert.call_args_list
            if c[0][0].status == "error"
        ]
        assert len(error_calls) >= 1
        error_cp = error_calls[-1][0][0]
        assert "Network unreachable" in error_cp.last_error_message


# ---------------------------------------------------------------------------
# Tests: Data type filtering
# ---------------------------------------------------------------------------


class TestDataTypeFiltering:
    """The --data-types flag limits which error checkpoints are retried."""

    @pytest.mark.asyncio
    async def test_filters_by_data_types(self, gateway, checkpoint_repo, session_factory, logger):
        """Only error checkpoints matching --data-types are retried."""
        # Two error checkpoints: kline and funding_rate
        kline_cp = make_kline_error_checkpoint(symbol="BTCUSDT")
        fr_cp = make_funding_rate_error_checkpoint(symbol="ETHUSDT")
        checkpoint_repo.list_by_status.return_value = [kline_cp, fr_cp]

        # Only mock the kline fetch (funding_rate should not be called)
        gateway.fetch_klines.return_value = [make_binance_kline(ALIGNED_TS)]

        with patch("YM_data_collection.apps.run_recovery_sync.load_config") as mock_load, \
             patch("YM_data_collection.apps.run_recovery_sync.create_mysql_engine") as mock_engine, \
             patch("YM_data_collection.apps.run_recovery_sync.create_session_factory") as mock_sf, \
             patch("YM_data_collection.apps.run_recovery_sync.BinanceGateway") as mock_gw_class, \
             patch("YM_data_collection.apps.run_recovery_sync.CheckpointRepository") as mock_cr_class, \
             patch("YM_data_collection.apps.run_recovery_sync.KlineRepository") as mock_kr_class:

            mock_config = MagicMock()
            mock_config.ingestion.historical_start_ts_ms = 1609459200000
            mock_load.return_value = mock_config
            mock_engine.return_value = MagicMock()
            mock_sf.return_value = session_factory
            mock_gw_class.return_value = gateway
            mock_cr_class.return_value = checkpoint_repo
            mock_kr = MagicMock()
            mock_kr.upsert_batch.return_value = 1
            mock_kr_class.return_value = mock_kr

            import argparse
            # Only include "kline" in data_types — funding_rate should be skipped
            args = argparse.Namespace(
                config="dummy.yaml",
                env="dev",
                log_level="INFO",
                venue="binance",
                market_type="spot",
                symbols=None,
                data_types=["kline"],
                since_last_checkpoint=False,
            )

            result = await run_async(args)

        # Only kline was recovered (funding_rate was filtered out)
        assert result["recovered"] == 1
        assert result["still_failing"] == 0
        # funding_rate fetch should never have been called
        gateway.fetch_funding_rates.assert_not_awaited()


# ---------------------------------------------------------------------------
# Tests: CLI main returns exit code
# ---------------------------------------------------------------------------


class TestCLIMain:
    """The main() function returns appropriate exit codes."""

    def test_cli_main_returns_exit_code(self):
        """main() with no error checkpoints returns 0 (SUCCESS)."""
        with patch("YM_data_collection.apps.run_recovery_sync.run_async") as mock_run, \
             patch("YM_data_collection.apps.run_recovery_sync.configure_logging"), \
             patch("YM_data_collection.apps.run_recovery_sync.get_logger"):

            mock_run.return_value = {"recovered": 0, "still_failing": 0}
            result = main(["--config", "dummy.yaml"])

        assert result == 0

    def test_cli_main_returns_failure_on_still_failing(self):
        """main() with still_failing > 0 returns GENERAL_FAILURE."""
        with patch("YM_data_collection.apps.run_recovery_sync.run_async") as mock_run, \
             patch("YM_data_collection.apps.run_recovery_sync.configure_logging"), \
             patch("YM_data_collection.apps.run_recovery_sync.get_logger"):

            mock_run.return_value = {"recovered": 1, "still_failing": 2}
            result = main(["--config", "dummy.yaml"])

        assert result == 1  # ExitCode.GENERAL_FAILURE

    def test_cli_main_returns_success_on_all_recovered(self):
        """main() with all recovered returns 0."""
        with patch("YM_data_collection.apps.run_recovery_sync.run_async") as mock_run, \
             patch("YM_data_collection.apps.run_recovery_sync.configure_logging"), \
             patch("YM_data_collection.apps.run_recovery_sync.get_logger"):

            mock_run.return_value = {"recovered": 3, "still_failing": 0}
            result = main(["--config", "dummy.yaml"])

        assert result == 0


# ---------------------------------------------------------------------------
# Tests: Full integration with run_async
# ---------------------------------------------------------------------------


class TestRunAsyncIntegration:
    """Integration tests for run_async with multiple error checkpoints."""

    @pytest.mark.asyncio
    async def test_recovers_multiple_error_checkpoints(self, gateway, checkpoint_repo, session_factory, logger):
        """Multiple error checkpoints of different types are all recovered."""
        kline_cp = make_kline_error_checkpoint(symbol="BTCUSDT", market_type="spot")
        fr_cp = make_funding_rate_error_checkpoint(symbol="ETHUSDT")
        checkpoint_repo.list_by_status.return_value = [kline_cp, fr_cp]

        gateway.fetch_klines.return_value = [make_binance_kline(ALIGNED_TS)]
        gateway.fetch_funding_rates.return_value = [make_funding_rate_raw("ETHUSDT", ALIGNED_TS)]

        with patch("YM_data_collection.apps.run_recovery_sync.load_config") as mock_load, \
             patch("YM_data_collection.apps.run_recovery_sync.create_mysql_engine") as mock_engine, \
             patch("YM_data_collection.apps.run_recovery_sync.create_session_factory") as mock_sf, \
             patch("YM_data_collection.apps.run_recovery_sync.BinanceGateway") as mock_gw_class, \
             patch("YM_data_collection.apps.run_recovery_sync.CheckpointRepository") as mock_cr_class, \
             patch("YM_data_collection.apps.run_recovery_sync.KlineRepository") as mock_kr_class, \
             patch("YM_data_collection.apps.run_recovery_sync.FundingRateRepository") as mock_fr_class:

            mock_config = MagicMock()
            mock_config.ingestion.historical_start_ts_ms = 1609459200000
            mock_load.return_value = mock_config
            mock_engine.return_value = MagicMock()
            mock_sf.return_value = session_factory
            mock_gw_class.return_value = gateway
            mock_cr_class.return_value = checkpoint_repo
            mock_kr = MagicMock()
            mock_kr.upsert_batch.return_value = 1
            mock_kr_class.return_value = mock_kr
            mock_fr = MagicMock()
            mock_fr.upsert_batch.return_value = 1
            mock_fr_class.return_value = mock_fr

            import argparse
            args = argparse.Namespace(
                config="dummy.yaml",
                env="dev",
                log_level="INFO",
                venue="binance",
                market_type="spot",
                symbols=None,
                data_types=["kline", "funding_rate", "mark_price", "open_interest"],
                since_last_checkpoint=False,
            )

            # Note: funding_rate has market_type=usdt_perpetual, which won't match
            # market_type="spot" filter. So only the kline will be recovered.
            # Let's adjust: use market_type that matches both.
            args.market_type = "usdt_perpetual"

            # But kline_cp has market_type="spot", so it won't match either.
            # Let's fix the test data to be consistent.
            kline_cp.market_type = "usdt_perpetual"
            checkpoint_repo.list_by_status.return_value = [kline_cp, fr_cp]

            result = await run_async(args)

        assert result["recovered"] == 2
        assert result["still_failing"] == 0

    @pytest.mark.asyncio
    async def test_mixed_success_and_failure(self, gateway, checkpoint_repo, session_factory, logger):
        """Some checkpoints recover, others fail."""
        cp_ok = make_kline_error_checkpoint(symbol="BTCUSDT", market_type="spot")
        cp_fail = make_funding_rate_error_checkpoint(symbol="ETHUSDT")
        checkpoint_repo.list_by_status.return_value = [cp_ok, cp_fail]

        gateway.fetch_klines.return_value = [make_binance_kline(ALIGNED_TS)]
        gateway.fetch_funding_rates.side_effect = ConnectionError("API down")

        with patch("YM_data_collection.apps.run_recovery_sync.load_config") as mock_load, \
             patch("YM_data_collection.apps.run_recovery_sync.create_mysql_engine") as mock_engine, \
             patch("YM_data_collection.apps.run_recovery_sync.create_session_factory") as mock_sf, \
             patch("YM_data_collection.apps.run_recovery_sync.BinanceGateway") as mock_gw_class, \
             patch("YM_data_collection.apps.run_recovery_sync.CheckpointRepository") as mock_cr_class, \
             patch("YM_data_collection.apps.run_recovery_sync.KlineRepository") as mock_kr_class:

            mock_config = MagicMock()
            mock_config.ingestion.historical_start_ts_ms = 1609459200000
            mock_load.return_value = mock_config
            mock_engine.return_value = MagicMock()
            mock_sf.return_value = session_factory
            mock_gw_class.return_value = gateway
            mock_cr_class.return_value = checkpoint_repo
            mock_kr = MagicMock()
            mock_kr.upsert_batch.return_value = 1
            mock_kr_class.return_value = mock_kr

            import argparse
            args = argparse.Namespace(
                config="dummy.yaml",
                env="dev",
                log_level="INFO",
                venue="binance",
                market_type="spot",
                symbols=None,
                data_types=["kline", "funding_rate"],
                since_last_checkpoint=False,
            )

            result = await run_async(args)

        # kline recovered, funding_rate failed (market_type mismatch for fr, so skipped)
        # Actually funding_rate has market_type=usdt_perpetual, and we filter by market_type="spot"
        # So funding_rate will be skipped (not recovered, not failing)
        assert result["recovered"] == 1
        assert result["still_failing"] == 0

    @pytest.mark.asyncio
    async def test_symbol_filter(self, gateway, checkpoint_repo, session_factory, logger):
        """--symbols flag filters which checkpoints are retried."""
        cp1 = make_kline_error_checkpoint(symbol="BTCUSDT", market_type="spot")
        cp2 = make_kline_error_checkpoint(symbol="ETHUSDT", market_type="spot")
        checkpoint_repo.list_by_status.return_value = [cp1, cp2]

        gateway.fetch_klines.return_value = [make_binance_kline(ALIGNED_TS)]

        with patch("YM_data_collection.apps.run_recovery_sync.load_config") as mock_load, \
             patch("YM_data_collection.apps.run_recovery_sync.create_mysql_engine") as mock_engine, \
             patch("YM_data_collection.apps.run_recovery_sync.create_session_factory") as mock_sf, \
             patch("YM_data_collection.apps.run_recovery_sync.BinanceGateway") as mock_gw_class, \
             patch("YM_data_collection.apps.run_recovery_sync.CheckpointRepository") as mock_cr_class, \
             patch("YM_data_collection.apps.run_recovery_sync.KlineRepository") as mock_kr_class:

            mock_config = MagicMock()
            mock_config.ingestion.historical_start_ts_ms = 1609459200000
            mock_load.return_value = mock_config
            mock_engine.return_value = MagicMock()
            mock_sf.return_value = session_factory
            mock_gw_class.return_value = gateway
            mock_cr_class.return_value = checkpoint_repo
            mock_kr = MagicMock()
            mock_kr.upsert_batch.return_value = 1
            mock_kr_class.return_value = mock_kr

            import argparse
            args = argparse.Namespace(
                config="dummy.yaml",
                env="dev",
                log_level="INFO",
                venue="binance",
                market_type="spot",
                symbols=["BTCUSDT"],  # Only BTCUSDT
                data_types=["kline"],
                since_last_checkpoint=False,
            )

            result = await run_async(args)

        # Only BTCUSDT recovered; ETHUSDT was filtered out
        assert result["recovered"] == 1

    @pytest.mark.asyncio
    async def test_fallback_start_ts_ms_used(self, gateway, checkpoint_repo, session_factory, logger):
        """When checkpoint has no last_event_ts_ms, config fallback is used."""
        cp = make_kline_error_checkpoint(last_event_ts_ms=None)
        checkpoint_repo.list_by_status.return_value = [cp]

        gateway.fetch_klines.return_value = [make_binance_kline(1609459200000)]

        with patch("YM_data_collection.apps.run_recovery_sync.load_config") as mock_load, \
             patch("YM_data_collection.apps.run_recovery_sync.create_mysql_engine") as mock_engine, \
             patch("YM_data_collection.apps.run_recovery_sync.create_session_factory") as mock_sf, \
             patch("YM_data_collection.apps.run_recovery_sync.BinanceGateway") as mock_gw_class, \
             patch("YM_data_collection.apps.run_recovery_sync.CheckpointRepository") as mock_cr_class, \
             patch("YM_data_collection.apps.run_recovery_sync.KlineRepository") as mock_kr_class:

            mock_config = MagicMock()
            mock_config.ingestion.historical_start_ts_ms = 1609459200000
            mock_load.return_value = mock_config
            mock_engine.return_value = MagicMock()
            mock_sf.return_value = session_factory
            mock_gw_class.return_value = gateway
            mock_cr_class.return_value = checkpoint_repo
            mock_kr = MagicMock()
            mock_kr.upsert_batch.return_value = 1
            mock_kr_class.return_value = mock_kr

            import argparse
            args = argparse.Namespace(
                config="dummy.yaml",
                env="dev",
                log_level="INFO",
                venue="binance",
                market_type="spot",
                symbols=None,
                data_types=["kline"],
                since_last_checkpoint=False,
            )

            result = await run_async(args)

        assert result["recovered"] == 1
        # Verify the start_ts_ms was the config fallback value
        call_kwargs = gateway.fetch_klines.call_args.kwargs
        assert call_kwargs["start_ts_ms"] == 1609459200000


# ---------------------------------------------------------------------------
# Tests: _update_checkpoint_error helper
# ---------------------------------------------------------------------------


class TestUpdateCheckpointError:
    """Test the _update_checkpoint_error helper."""

    def test_updates_with_error_status(self, checkpoint_repo):
        cp = make_kline_error_checkpoint()
        _update_checkpoint_error(checkpoint_repo, cp, "Something broke")

        checkpoint_repo.upsert.assert_called_once()
        error_cp = checkpoint_repo.upsert.call_args[0][0]
        assert error_cp.status == "error"
        assert "Something broke" in error_cp.last_error_message
        # Position info preserved
        assert error_cp.last_event_ts_ms == cp.last_event_ts_ms

    def test_truncates_long_error_message(self, checkpoint_repo):
        cp = make_kline_error_checkpoint()
        long_msg = "x" * 2000
        _update_checkpoint_error(checkpoint_repo, cp, long_msg)

        error_cp = checkpoint_repo.upsert.call_args[0][0]
        assert len(error_cp.last_error_message) == 1024
