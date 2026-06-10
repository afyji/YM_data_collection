"""Run realtime ingestion service.

Main entry point for the realtime data ingestion pipeline.
1. Loads config
2. Creates MySQL engine/session_factory
3. Creates Redis client
4. Creates BinanceWSManager
5. Creates all handlers (kline, derivatives, depth)
6. Creates FlushWorker
7. Subscribes to all relevant WS streams
8. Wires WS messages to handlers
9. Starts flush worker
10. Runs until SIGINT/SIGTERM (graceful shutdown)
"""

from __future__ import annotations

import argparse
import asyncio
import signal
import sys

from pathlib import Path
from typing import Any

if __package__ is None or __package__ == "":
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from YM_data_collection.apps._cli_common import (
    CliConfigError,
    add_common_arguments,
    add_list_argument,
    emit_final_status,
)
from YM_data_collection.adapters.binance_ws_manager import BinanceWSManager
from YM_data_collection.cache.redis_client import build_redis_client
from YM_data_collection.config.loader import load_config
from YM_data_collection.ingestion.realtime_depth_handler import RealtimeDepthHandler
from YM_data_collection.ingestion.realtime_derivatives_handler import (
    RealtimeDerivativesHandler,
)
from YM_data_collection.ingestion.realtime_kline_handler import RealtimeKlineHandler
from YM_data_collection.persistence.flush_worker import FlushWorker
from YM_data_collection.persistence.mysql import create_mysql_engine, create_session_factory
from YM_data_collection.utils.exit_codes import ExitCode
from YM_data_collection.utils.logging_utils import configure_logging, get_logger

APP_NAME = "run_realtime_ingest"

# Default topics when none specified
DEFAULT_TOPICS = [
    "kline",
    "mark_price",
    "index_price",
    "open_interest",
    "funding_rate",
    "depth_snapshot",
]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run realtime ingestion. "
            "kline subscribes configured Binance kline intervals; "
            "mark_price, index_price, and funding_rate come from perp @markPrice@1s; "
            "open_interest is listed as a topic but is not currently backed by an active "
            "subscription in this command."
        )
    )
    add_common_arguments(
        parser,
        include_config=True,
        include_env=True,
        include_venue=True,
        include_market_type=True,
        include_symbols=True,
    )
    add_list_argument(
        parser,
        "--topics",
        default=DEFAULT_TOPICS,
        choices=DEFAULT_TOPICS,
        help_text=(
            "Realtime topics to subscribe to. "
            "mark_price, index_price, and funding_rate share the same perp @markPrice@1s stream. "
            "open_interest is a reserved topic name and is not currently wired to a live stream here."
        ),
    )
    return parser


def build_streams(
    symbols: list[str],
    intervals: list[str],
    topics: list[str],
) -> tuple[list[str], list[str]]:
    """Build spot and perp stream lists based on symbols, intervals, and topics.

    Returns (spot_streams, perp_streams).
    """
    symbols_lower = [s.lower() for s in symbols]
    spot_streams: list[str] = []
    perp_streams: list[str] = []

    for sym in symbols_lower:
        if "kline" in topics:
            for interval in intervals:
                spot_streams.append(f"{sym}@kline_{interval}")
                perp_streams.append(f"{sym}@kline_{interval}")
        if (
            "mark_price" in topics
            or "index_price" in topics
            or "funding_rate" in topics
        ):
            perp_streams.append(f"{sym}@markPrice@1s")
        if "depth_snapshot" in topics:
            spot_streams.append(f"{sym}@depth20@100ms")
            perp_streams.append(f"{sym}@depth20@100ms")

    return spot_streams, perp_streams


def make_dispatch(
    *,
    kline_handler: RealtimeKlineHandler,
    derivatives_handler: RealtimeDerivativesHandler,
    depth_handler: RealtimeDepthHandler,
    spot_stream_set: set[str],
    logger: Any,
) -> Any:
    """Create the message dispatch callback.

    Routes messages based on stream name to the appropriate handler.
    Uses spot_stream_set to determine market_type (spot vs perp).
    """

    async def dispatch(stream: str, data: dict) -> None:
        forced_market_type: str | None = None
        raw_stream = stream
        if ":" in stream:
            maybe_market_type, maybe_stream = stream.split(":", 1)
            if maybe_market_type in {"spot", "perp"}:
                forced_market_type = maybe_market_type
                raw_stream = maybe_stream

        market_type = forced_market_type or ("spot" if raw_stream in spot_stream_set else "perp")
        symbol = raw_stream.split("@")[0].upper()
        try:
            if "@kline_" in raw_stream:
                await kline_handler.handle_message(market_type, data)
            elif "@markPrice" in raw_stream:
                await derivatives_handler.handle_message(raw_stream, data)
            elif "@depth" in raw_stream:
                await depth_handler.handle_message(market_type, symbol, data)
            else:
                logger.debug("Unhandled stream: %s", stream)
        except Exception as exc:
            logger.error("Error handling stream %s: %s", stream, exc)

    return dispatch


async def run_async(args: argparse.Namespace) -> None:
    """Main async orchestration for realtime ingestion."""
    configure_logging(args.log_level)
    logger = get_logger(APP_NAME)

    # Load config
    try:
        config = load_config(config_path=args.config, env_name=args.env)
    except Exception as exc:
        raise CliConfigError(f"Failed to load config: {exc}")

    # Create infrastructure
    engine = create_mysql_engine(config.mysql)
    session_factory = create_session_factory(engine)
    redis_client = build_redis_client(config.cache)

    # Determine symbols and topics
    symbols = args.symbols or config.binance.symbols
    topics = args.topics
    venue = args.venue

    logger.info(
        "Starting realtime ingestion: venue=%s symbols=%s topics=%s",
        venue,
        symbols,
        topics,
    )

    # Build stream subscriptions
    spot_streams, perp_streams = build_streams(
        symbols=symbols,
        intervals=config.binance.intervals,
        topics=topics,
    )
    spot_stream_set = set(spot_streams)

    logger.info("Spot streams: %d, Perp streams: %d", len(spot_streams), len(perp_streams))

    # Create handlers
    ws_manager = BinanceWSManager(config.binance, logger=logger)
    kline_handler = RealtimeKlineHandler(
        session_factory, redis_client, config.realtime_persistence, venue=venue, logger=logger,
    )
    derivatives_handler = RealtimeDerivativesHandler(
        session_factory, redis_client, config.realtime_persistence, venue=venue, logger=logger,
    )
    depth_handler = RealtimeDepthHandler(
        redis_client, config.realtime_persistence, venue=venue, logger=logger,
    )
    flush_worker = FlushWorker(
        redis_client, session_factory, config.realtime_persistence, logger=logger,
    )

    # Wire dispatch
    dispatch = make_dispatch(
        kline_handler=kline_handler,
        derivatives_handler=derivatives_handler,
        depth_handler=depth_handler,
        spot_stream_set=spot_stream_set,
        logger=logger,
    )
    ws_manager.on_message(dispatch)

    # Connect and subscribe
    if spot_streams:
        await ws_manager.connect("spot")
        await ws_manager.subscribe("spot", spot_streams)
    if perp_streams:
        await ws_manager.connect("perp")
        await ws_manager.subscribe("perp", perp_streams)

    # Start flush worker
    await flush_worker.start()

    # Shutdown handling
    shutdown_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, shutdown_event.set)

    logger.info("Realtime ingestion running. Waiting for shutdown signal...")

    # Run WS in background, wait for shutdown
    ws_task = asyncio.create_task(ws_manager.run_forever())
    await shutdown_event.wait()

    # Cleanup
    logger.info("Shutting down...")
    await flush_worker.stop()
    await ws_manager.close()
    ws_task.cancel()
    try:
        await ws_task
    except asyncio.CancelledError:
        pass

    engine.dispose()
    logger.info("Realtime ingest stopped gracefully")


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    try:
        args = parser.parse_args(argv)
        asyncio.run(run_async(args))
        return emit_final_status(APP_NAME, ExitCode.SUCCESS, "Realtime ingest stopped gracefully")
    except CliConfigError as exc:
        return emit_final_status(APP_NAME, exc.exit_code, str(exc))
    except Exception as exc:
        return emit_final_status(APP_NAME, ExitCode.GENERAL_FAILURE, str(exc))


if __name__ == "__main__":
    sys.exit(main())
