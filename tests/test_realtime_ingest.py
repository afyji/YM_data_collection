"""Tests for apps/run_realtime_ingest.py — realtime ingestion entry point."""

from __future__ import annotations

import argparse
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from YM_data_collection.apps.run_realtime_ingest import (
    APP_NAME,
    build_parser,
    build_streams,
    make_dispatch,
    run_async,
    main,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_args(**overrides) -> argparse.Namespace:
    """Build a default args namespace with sensible defaults for testing."""
    defaults = {
        "config": "config.yaml",
        "env": "dev",
        "venue": "binance",
        "market_type": "spot",
        "symbols": ["BTCUSDT", "ETHUSDT"],
        "topics": ["kline", "mark_price", "index_price", "funding_rate", "depth_snapshot"],
        "log_level": "INFO",
    }
    defaults.update(overrides)
    return argparse.Namespace(**defaults)


# ---------------------------------------------------------------------------
# Stream building tests
# ---------------------------------------------------------------------------

class TestBuildStreams:
    """Test build_streams() subscription generation."""

    def test_builds_correct_spot_streams(self):
        symbols = ["BTCUSDT", "ETHUSDT"]
        intervals = ["1h", "4h"]
        topics = ["kline", "depth_snapshot"]

        spot, perp = build_streams(symbols, intervals, topics)

        # Spot: kline streams + depth streams
        assert "btcusdt@kline_1h" in spot
        assert "btcusdt@kline_4h" in spot
        assert "ethusdt@kline_1h" in spot
        assert "ethusdt@kline_4h" in spot
        assert "btcusdt@depth20@100ms" in spot
        assert "ethusdt@depth20@100ms" in spot
        # markPrice should NOT be in spot
        assert all("@markPrice" not in s for s in spot)

    def test_builds_correct_perp_streams(self):
        symbols = ["BTCUSDT"]
        intervals = ["1h"]
        topics = ["kline", "mark_price", "depth_snapshot"]

        spot, perp = build_streams(symbols, intervals, topics)

        # Perp: kline + markPrice + depth
        assert "btcusdt@kline_1h" in perp
        assert "btcusdt@markPrice@1s" in perp
        assert "btcusdt@depth20@100ms" in perp

    def test_mark_price_in_perp_when_funding_rate_topic(self):
        symbols = ["BTCUSDT"]
        intervals = ["1h"]
        topics = ["funding_rate"]

        spot, perp = build_streams(symbols, intervals, topics)

        assert "btcusdt@markPrice@1s" in perp
        assert len(spot) == 0  # No spot streams for funding_rate only

    def test_mark_price_in_perp_when_index_price_topic(self):
        symbols = ["BTCUSDT"]
        intervals = ["1h"]
        topics = ["index_price"]

        spot, perp = build_streams(symbols, intervals, topics)

        assert "btcusdt@markPrice@1s" in perp

    def test_no_streams_when_no_matching_topics(self):
        symbols = ["BTCUSDT"]
        intervals = ["1h"]
        topics = ["open_interest"]  # No WS stream for open_interest

        spot, perp = build_streams(symbols, intervals, topics)

        assert len(spot) == 0
        assert len(perp) == 0

    def test_topics_filter(self):
        """Only subscribed topics get streams built."""
        symbols = ["BTCUSDT"]
        intervals = ["1h", "4h"]
        topics = ["kline"]  # Only kline, no depth or mark_price

        spot, perp = build_streams(symbols, intervals, topics)

        # Kline streams on both
        assert "btcusdt@kline_1h" in spot
        assert "btcusdt@kline_4h" in spot
        assert "btcusdt@kline_1h" in perp
        assert "btcusdt@kline_4h" in perp
        # No depth or markPrice
        assert all("@depth" not in s for s in spot)
        assert all("@markPrice" not in s for s in perp)

    def test_multiple_symbols(self):
        symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
        intervals = ["1d"]
        topics = ["kline", "depth_snapshot"]

        spot, perp = build_streams(symbols, intervals, topics)

        assert len(spot) == 6  # 3 symbols * (1 kline + 1 depth)
        assert len(perp) == 6  # 3 symbols * (1 kline + 1 depth) — depth goes to perp too


# ---------------------------------------------------------------------------
# Dispatch routing tests
# ---------------------------------------------------------------------------

class TestDispatch:
    """Test make_dispatch() message routing."""

    def setup_method(self):
        self.kline_handler = AsyncMock()
        self.derivatives_handler = AsyncMock()
        self.depth_handler = AsyncMock()
        self.logger = MagicMock()
        self.spot_stream_set = {
            "btcusdt@kline_1h",
            "btcusdt@depth20@100ms",
        }
        self.dispatch = make_dispatch(
            kline_handler=self.kline_handler,
            derivatives_handler=self.derivatives_handler,
            depth_handler=self.depth_handler,
            spot_stream_set=self.spot_stream_set,
            logger=self.logger,
        )

    @pytest.mark.asyncio
    async def test_dispatch_routes_kline(self):
        data = {"e": "kline", "k": {"s": "BTCUSDT"}}
        await self.dispatch("btcusdt@kline_1h", data)

        self.kline_handler.handle_message.assert_awaited_once_with("spot", data)
        self.derivatives_handler.handle_message.assert_not_awaited()
        self.depth_handler.handle_message.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_dispatch_routes_kline_perp(self):
        data = {"e": "kline", "k": {"s": "BTCUSDT"}}
        # This stream is NOT in spot_stream_set, so it's perp
        await self.dispatch("btcusdt@kline_4h", data)

        self.kline_handler.handle_message.assert_awaited_once_with("perp", data)

    @pytest.mark.asyncio
    async def test_dispatch_routes_mark_price(self):
        data = {"e": "markPriceUpdate", "s": "BTCUSDT", "p": "50000"}
        await self.dispatch("btcusdt@markPrice@1s", data)

        self.derivatives_handler.handle_message.assert_awaited_once_with(
            "btcusdt@markPrice@1s", data
        )
        self.kline_handler.handle_message.assert_not_awaited()
        self.depth_handler.handle_message.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_dispatch_routes_depth(self):
        data = {"bids": [["50000", "1.5"]], "asks": [["50100", "2.0"]]}
        await self.dispatch("btcusdt@depth20@100ms", data)

        self.depth_handler.handle_message.assert_awaited_once_with(
            "spot", "BTCUSDT", data
        )
        self.kline_handler.handle_message.assert_not_awaited()
        self.derivatives_handler.handle_message.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_dispatch_routes_depth_perp(self):
        data = {"bids": [["50000", "1.5"]], "asks": [["50100", "2.0"]]}
        # ethusdt@depth20@100ms not in spot_stream_set → perp
        await self.dispatch("ethusdt@depth20@100ms", data)

        self.depth_handler.handle_message.assert_awaited_once_with(
            "perp", "ETHUSDT", data
        )


    @pytest.mark.asyncio
    async def test_dispatch_routes_prefixed_perp_kline_even_when_stream_name_overlaps_spot(self):
        data = {"e": "kline", "k": {"s": "BTCUSDT"}}
        await self.dispatch("perp:btcusdt@kline_1h", data)

        self.kline_handler.handle_message.assert_awaited_once_with("perp", data)

    @pytest.mark.asyncio
    async def test_dispatch_routes_prefixed_perp_depth_even_when_stream_name_overlaps_spot(self):
        data = {"bids": [["50000", "1.5"]], "asks": [["50100", "2.0"]]}
        await self.dispatch("perp:btcusdt@depth20@100ms", data)

        self.depth_handler.handle_message.assert_awaited_once_with(
            "perp", "BTCUSDT", data
        )

    @pytest.mark.asyncio
    async def test_dispatch_unknown_stream_no_error(self):
        """Unknown stream should not crash; just log debug."""
        data = {"e": "some_event"}
        await self.dispatch("btcusdt@unknownStream", data)

        self.kline_handler.handle_message.assert_not_awaited()
        self.derivatives_handler.handle_message.assert_not_awaited()
        self.depth_handler.handle_message.assert_not_awaited()
        self.logger.debug.assert_called_once()

    @pytest.mark.asyncio
    async def test_dispatch_handler_exception_no_crash(self):
        """If a handler raises, dispatch should catch and log."""
        self.kline_handler.handle_message.side_effect = RuntimeError("boom")
        data = {"e": "kline"}

        # Should not raise
        await self.dispatch("btcusdt@kline_1h", data)

        self.logger.error.assert_called_once()
        # Error is logged via logger.error("Error handling stream %s: %s", stream, exc)
        # The exception object is passed as the second arg to %s formatting
        call_args = self.logger.error.call_args
        assert "boom" in str(call_args)


# ---------------------------------------------------------------------------
# CLI parser tests
# ---------------------------------------------------------------------------

class TestCLIParser:
    """Test build_parser() argument handling."""

    def test_parser_accepts_all_expected_args(self):
        parser = build_parser()
        args = parser.parse_args([
            "--config", "myconfig.yaml",
            "--env", "prod",
            "--venue", "binance",
            "--market-type", "perp",
            "--symbols", "BTCUSDT", "ETHUSDT",
            "--topics", "kline", "mark_price",
        ])

        assert args.config == "myconfig.yaml"
        assert args.env == "prod"
        assert args.venue == "binance"
        assert args.market_type == "perp"
        assert args.symbols == ["BTCUSDT", "ETHUSDT"]
        assert args.topics == ["kline", "mark_price"]

    def test_parser_defaults(self):
        parser = build_parser()
        args = parser.parse_args([])

        assert args.env is None
        assert args.venue == "binance"
        assert args.market_type == "spot"
        assert args.symbols is None
        assert args.topics == [
            "kline",
            "mark_price",
            "index_price",
            "open_interest",
            "funding_rate",
            "depth_snapshot",
        ]


# ---------------------------------------------------------------------------
# run_async and shutdown tests
# ---------------------------------------------------------------------------

class TestRunAsync:
    """Test run_async() orchestration and graceful shutdown."""

    @pytest.mark.asyncio
    async def test_shutdown_stops_flush_worker(self):
        """On shutdown signal, flush_worker.stop() should be called."""
        args = _make_args()

        # We patch everything so no real connections are made
        with patch(
            "YM_data_collection.apps.run_realtime_ingest.load_config"
        ) as mock_load, patch(
            "YM_data_collection.apps.run_realtime_ingest.create_mysql_engine"
        ) as mock_engine, patch(
            "YM_data_collection.apps.run_realtime_ingest.create_session_factory"
        ) as mock_sf, patch(
            "YM_data_collection.apps.run_realtime_ingest.build_redis_client"
        ) as mock_redis, patch(
            "YM_data_collection.apps.run_realtime_ingest.BinanceWSManager"
        ) as mock_ws_cls, patch(
            "YM_data_collection.apps.run_realtime_ingest.RealtimeKlineHandler"
        ), patch(
            "YM_data_collection.apps.run_realtime_ingest.RealtimeDerivativesHandler"
        ), patch(
            "YM_data_collection.apps.run_realtime_ingest.RealtimeDepthHandler"
        ), patch(
            "YM_data_collection.apps.run_realtime_ingest.FlushWorker"
        ) as mock_flush_cls:
            # Setup mock config
            mock_config = MagicMock()
            mock_config.binance.intervals = ["1h"]
            mock_config.binance.symbols = ["BTCUSDT"]
            mock_load.return_value = mock_config

            # Mock engine
            mock_engine_instance = MagicMock()
            mock_engine.return_value = mock_engine_instance

            # Mock WS manager: on_message is sync; connection methods are async.
            mock_ws = MagicMock()
            mock_ws.connect = AsyncMock()
            mock_ws.subscribe = AsyncMock()
            mock_ws.run_forever = AsyncMock()
            mock_ws.close = AsyncMock()
            mock_ws_cls.return_value = mock_ws

            # Mock flush worker
            mock_flush = AsyncMock()
            mock_flush_cls.return_value = mock_flush

            # Make run_forever block until cancelled
            async def run_forever_block():
                await asyncio.sleep(10)

            mock_ws.run_forever.side_effect = run_forever_block

            # Run run_async but trigger shutdown quickly
            async def trigger_shutdown():
                await asyncio.sleep(0.05)
                # Simulate SIGTERM
                import os
                os.kill(os.getpid(), signal.SIGTERM)

            import signal
            task = asyncio.create_task(run_async(args))
            shutdown_task = asyncio.create_task(trigger_shutdown())

            await asyncio.gather(task, shutdown_task, return_exceptions=True)

            # Verify flush_worker.stop was called
            mock_flush.stop.assert_awaited()

    @pytest.mark.asyncio
    async def test_run_async_connects_and_subscribes(self):
        """run_async should connect and subscribe on both spot and perp."""
        args = _make_args()

        with patch(
            "YM_data_collection.apps.run_realtime_ingest.load_config"
        ) as mock_load, patch(
            "YM_data_collection.apps.run_realtime_ingest.create_mysql_engine"
        ) as mock_engine, patch(
            "YM_data_collection.apps.run_realtime_ingest.create_session_factory"
        ), patch(
            "YM_data_collection.apps.run_realtime_ingest.build_redis_client"
        ), patch(
            "YM_data_collection.apps.run_realtime_ingest.BinanceWSManager"
        ) as mock_ws_cls, patch(
            "YM_data_collection.apps.run_realtime_ingest.RealtimeKlineHandler"
        ), patch(
            "YM_data_collection.apps.run_realtime_ingest.RealtimeDerivativesHandler"
        ), patch(
            "YM_data_collection.apps.run_realtime_ingest.RealtimeDepthHandler"
        ), patch(
            "YM_data_collection.apps.run_realtime_ingest.FlushWorker"
        ) as mock_flush_cls:
            mock_config = MagicMock()
            mock_config.binance.intervals = ["1h", "4h"]
            mock_config.binance.symbols = ["BTCUSDT"]
            mock_load.return_value = mock_config

            mock_engine.return_value = MagicMock()

            mock_ws = MagicMock()
            mock_ws.connect = AsyncMock()
            mock_ws.subscribe = AsyncMock()
            mock_ws.run_forever = AsyncMock()
            mock_ws.close = AsyncMock()
            mock_ws_cls.return_value = mock_ws

            mock_flush = AsyncMock()
            mock_flush_cls.return_value = mock_flush

            async def run_forever_block():
                await asyncio.sleep(10)

            mock_ws.run_forever.side_effect = run_forever_block

            import os
            import signal as sig_mod

            async def trigger_shutdown():
                await asyncio.sleep(0.05)
                os.kill(os.getpid(), sig_mod.SIGTERM)

            task = asyncio.create_task(run_async(args))
            shutdown_task = asyncio.create_task(trigger_shutdown())
            await asyncio.gather(task, shutdown_task, return_exceptions=True)

            # Should have connected to both spot and perp
            mock_ws.connect.assert_any_await("spot")
            mock_ws.connect.assert_any_await("perp")

            # Should have subscribed on both
            spot_call = mock_ws.subscribe.call_args_list[0]
            perp_call = mock_ws.subscribe.call_args_list[1]
            assert spot_call[0][0] == "spot"
            assert perp_call[0][0] == "perp"

    @pytest.mark.asyncio
    async def test_run_async_config_error(self):
        """run_async should raise CliConfigError on config load failure."""
        args = _make_args()

        with patch(
            "YM_data_collection.apps.run_realtime_ingest.load_config",
            side_effect=Exception("bad config"),
        ):
            with pytest.raises(Exception, match="bad config"):
                await run_async(args)


# ---------------------------------------------------------------------------
# main() tests
# ---------------------------------------------------------------------------

class TestMain:
    """Test the main() synchronous entry point."""

    def test_main_returns_exit_code(self):
        """main() should return 0 on success."""
        with patch(
            "YM_data_collection.apps.run_realtime_ingest.asyncio.run",
        ) as mock_run, patch(
            "YM_data_collection.apps.run_realtime_ingest.load_config"
        ):
            mock_run.return_value = None

            result = main([])

            assert result == 0

    def test_main_returns_failure_on_exception(self):
        """main() should return non-zero on unhandled exception."""
        with patch(
            "YM_data_collection.apps.run_realtime_ingest.asyncio.run",
            side_effect=RuntimeError("oops"),
        ):
            result = main([])

            assert result != 0

    def test_main_config_error_exit_code(self):
        """main() should return CONFIG_ERROR (3) for CliConfigError."""
        from YM_data_collection.apps._cli_common import CliConfigError

        with patch(
            "YM_data_collection.apps.run_realtime_ingest.asyncio.run",
            side_effect=CliConfigError("bad config"),
        ):
            result = main([])

            assert result == 3  # CONFIG_ERROR


# ---------------------------------------------------------------------------
# Integration: build_streams + dispatch together
# ---------------------------------------------------------------------------

class TestIntegration:
    """Integration-style tests combining stream building and dispatch."""

    @pytest.mark.asyncio
    async def test_full_dispatch_flow_spot_kline(self):
        """Spot kline stream is correctly identified and routed."""
        symbols = ["BTCUSDT"]
        intervals = ["1h"]
        topics = ["kline", "mark_price", "depth_snapshot"]

        spot_streams, perp_streams = build_streams(symbols, intervals, topics)
        spot_stream_set = set(spot_streams)

        kline_handler = AsyncMock()
        derivatives_handler = AsyncMock()
        depth_handler = AsyncMock()
        logger = MagicMock()

        dispatch = make_dispatch(
            kline_handler=kline_handler,
            derivatives_handler=derivatives_handler,
            depth_handler=depth_handler,
            spot_stream_set=spot_stream_set,
            logger=logger,
        )

        # Simulate a spot kline message
        data = {"e": "kline", "k": {"s": "BTCUSDT"}}
        await dispatch("btcusdt@kline_1h", data)
        kline_handler.handle_message.assert_awaited_once_with("spot", data)

    @pytest.mark.asyncio
    async def test_full_dispatch_flow_perp_mark_price(self):
        """Perp markPrice stream is correctly identified and routed."""
        symbols = ["BTCUSDT"]
        intervals = ["1h"]
        topics = ["kline", "mark_price", "depth_snapshot"]

        spot_streams, perp_streams = build_streams(symbols, intervals, topics)
        spot_stream_set = set(spot_streams)

        kline_handler = AsyncMock()
        derivatives_handler = AsyncMock()
        depth_handler = AsyncMock()
        logger = MagicMock()

        dispatch = make_dispatch(
            kline_handler=kline_handler,
            derivatives_handler=derivatives_handler,
            depth_handler=depth_handler,
            spot_stream_set=spot_stream_set,
            logger=logger,
        )

        data = {"e": "markPriceUpdate", "s": "BTCUSDT"}
        await dispatch("btcusdt@markPrice@1s", data)
        derivatives_handler.handle_message.assert_awaited_once_with(
            "btcusdt@markPrice@1s", data
        )
