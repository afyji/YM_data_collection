"""Run historical kline synchronization.

Fetches historical kline data from Binance, normalizes and validates it,
then persists to MySQL with checkpoint tracking for resumability.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

if __package__ is None or __package__ == "":
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from YM_data_collection.apps._cli_common import (
    CliConfigError,
    add_common_arguments,
    add_flag_argument,
    add_list_argument,
    add_ts_ms_argument,
    emit_final_status,
)
from YM_data_collection.adapters.binance_gateway import BinanceGateway, MarketType
from YM_data_collection.config.loader import load_config
from YM_data_collection.domain.models import IngestCheckpoint, NormalizedKline
from YM_data_collection.normalization.kline_normalizer import normalize_binance_klines_batch
from YM_data_collection.persistence.mysql import create_mysql_engine, create_session_factory
from YM_data_collection.persistence.repositories.checkpoint_repo import CheckpointRepository
from YM_data_collection.persistence.repositories.marketdata_repo import KlineRepository
from YM_data_collection.utils.exit_codes import ExitCode
from YM_data_collection.utils.logging_utils import configure_logging, get_logger
from YM_data_collection.validation.kline_validator import (
    INTERVAL_DURATIONS_MS,
    validate_klines_batch,
)

APP_NAME = "run_historical_klines_sync"
SUPPORTED_KLINE_INTERVALS = list(INTERVAL_DURATIONS_MS.keys())
DEFAULT_KLINE_INTERVALS = ["1m", "1h", "1d"]

# Batch limits per market type
BATCH_LIMITS: dict[str, int] = {
    "spot": 1000,
    "perp": 1500,
}

# Table names per market type
TABLE_NAMES: dict[str, str] = {
    "spot": "spot_klines",
    "perp": "perp_klines",
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Sync historical klines from Binance to MySQL.")
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
        "--intervals",
        default=DEFAULT_KLINE_INTERVALS,
        choices=SUPPORTED_KLINE_INTERVALS,
        help_text="Interval list. Default: 1m 1h 1d.",
    )
    add_ts_ms_argument(parser, "--start-ts-ms", boundary="start", help_prefix="Inclusive start time.")
    add_ts_ms_argument(parser, "--end-ts-ms", boundary="end", help_prefix="Inclusive end time.")
    add_flag_argument(parser, "--dry-run", "Fetch + normalize + validate but skip writing to MySQL.")
    return parser


async def _sync_symbol_interval(
    *,
    gateway: BinanceGateway,
    kline_repo: KlineRepository,
    checkpoint_repo: CheckpointRepository,
    session_factory: Any,
    venue: str,
    market_type: str,
    symbol: str,
    interval: str,
    start_ts_ms: int,
    end_ts_ms: int,
    dry_run: bool,
    logger: Any,
) -> int:
    """Sync a single symbol × interval pair. Returns total rows persisted."""

    table_name = TABLE_NAMES[market_type]
    limit = BATCH_LIMITS[market_type]
    data_type = "kline"

    # Determine start point from checkpoint
    checkpoint = checkpoint_repo.get(
        venue=venue,
        market_type=market_type,
        symbol=symbol,
        data_type=data_type,
        interval_code=interval,
    )
    if checkpoint is not None and checkpoint.last_kline_open_ts_ms is not None:
        # Resume from last successful position + 1 to avoid overlap
        resume_start = checkpoint.last_kline_open_ts_ms + 1
        if resume_start > start_ts_ms:
            logger.info(
                f"Resuming {symbol}/{interval} from checkpoint ts={resume_start} "
                f"(override start_ts_ms={start_ts_ms})"
            )
            start_ts_ms = resume_start

    if start_ts_ms > end_ts_ms:
        logger.info(f"{symbol}/{interval}: start_ts_ms > end_ts_ms, nothing to sync")
        return 0

    total_rows = 0
    batch_count = 0
    current_start = start_ts_ms

    while current_start <= end_ts_ms:
        batch_count += 1
        try:
            raw_klines = await gateway.fetch_klines(
                market_type=market_type,
                symbol=symbol,
                interval=interval,
                start_ts_ms=current_start,
                end_ts_ms=end_ts_ms,
                limit=limit,
            )
        except Exception as exc:
            logger.error(f"Error fetching {symbol}/{interval} batch {batch_count}: {exc}")
            # Update checkpoint with error status
            _update_checkpoint(
                checkpoint_repo=checkpoint_repo,
                venue=venue,
                market_type=market_type,
                symbol=symbol,
                interval=interval,
                data_type=data_type,
                status="error",
                last_kline_open_ts_ms=None,
                error_message=str(exc),
            )
            # Re-raise to let the caller handle continuing to next symbol/interval
            raise

        if not raw_klines:
            logger.info(f"{symbol}/{interval}: no more data returned at batch {batch_count}")
            break

        # Normalize
        normalized = normalize_binance_klines_batch(
            raw_klines, venue, symbol, market_type, interval
        )

        # Validate
        validation_results = validate_klines_batch(normalized, interval)

        # Separate valid from invalid
        valid_klines: list[NormalizedKline] = []
        for kline, vr in zip(normalized, validation_results):
            if vr.is_valid:
                valid_klines.append(kline)
            elif vr.is_repairable and vr.repaired_kline is not None:
                valid_klines.append(vr.repaired_kline)
            else:
                logger.warning(
                    f"Skipping invalid kline {symbol}/{interval} "
                    f"open_ts_ms={kline.open_ts_ms}: {vr.issues}"
                )

        # Find last open_ts_ms from raw data for checkpoint
        last_open_ts_ms = max(int(k[0]) for k in raw_klines)

        if not dry_run and valid_klines:
            try:
                rows_affected = kline_repo.upsert_batch(
                    session_factory, table_name, valid_klines
                )
                total_rows += rows_affected
            except Exception as exc:
                logger.error(
                    f"Error persisting {symbol}/{interval} batch {batch_count}: {exc}"
                )
                _update_checkpoint(
                    checkpoint_repo=checkpoint_repo,
                    venue=venue,
                    market_type=market_type,
                    symbol=symbol,
                    interval=interval,
                    data_type=data_type,
                    status="error",
                    last_kline_open_ts_ms=None,
                    error_message=str(exc),
                )
                raise
        elif dry_run and valid_klines:
            total_rows += len(valid_klines)

        # Update checkpoint after each successful batch
        if not dry_run:
            _update_checkpoint(
                checkpoint_repo=checkpoint_repo,
                venue=venue,
                market_type=market_type,
                symbol=symbol,
                interval=interval,
                data_type=data_type,
                status="ok",
                last_kline_open_ts_ms=last_open_ts_ms,
                error_message=None,
            )

        logger.info(
            f"{symbol}/{interval} batch {batch_count}: "
            f"fetched={len(raw_klines)} valid={len(valid_klines)} "
            f"total_rows={total_rows} last_open_ts_ms={last_open_ts_ms}"
        )

        # Next batch starts from last_open_ts_ms + 1 to avoid overlap
        current_start = last_open_ts_ms + 1

        # If we got fewer results than the limit, we've likely reached the end
        if len(raw_klines) < limit:
            break

    logger.info(
        f"{symbol}/{interval} complete: batches={batch_count} total_rows={total_rows}"
    )
    return total_rows


def _update_checkpoint(
    *,
    checkpoint_repo: CheckpointRepository,
    venue: str,
    market_type: str,
    symbol: str,
    interval: str,
    data_type: str,
    status: str,
    last_kline_open_ts_ms: int | None,
    error_message: str | None,
) -> None:
    """Upsert checkpoint after a batch."""
    now = datetime.now(timezone.utc)
    last_event_dt_utc = None
    if last_kline_open_ts_ms is not None:
        last_event_dt_utc = datetime.fromtimestamp(
            last_kline_open_ts_ms / 1000.0, tz=timezone.utc
        )

    checkpoint = IngestCheckpoint(
        venue=venue,
        market_type=market_type,
        symbol=symbol,
        data_type=data_type,
        interval_code=interval,
        last_event_ts_ms=last_kline_open_ts_ms,
        last_event_dt_utc=last_event_dt_utc,
        last_kline_open_ts_ms=last_kline_open_ts_ms,
        status=status,
        last_success_at_utc=now if status == "ok" else None,
        last_error_message=error_message,
    )
    checkpoint_repo.upsert(checkpoint)


async def run_async(args: argparse.Namespace) -> int:
    """Main async orchestration."""
    configure_logging(args.log_level)
    logger = get_logger(APP_NAME)

    # Load config
    try:
        config = load_config(config_path=args.config, env_name=args.env)
    except Exception as exc:
        raise CliConfigError(f"Failed to load config: {exc}")

    # Determine parameters
    venue = args.venue
    market_type: MarketType = args.market_type  # type: ignore[assignment]
    symbols = args.symbols
    intervals = args.intervals
    dry_run = args.dry_run

    # Determine time range
    start_ts_ms = args.start_ts_ms
    if start_ts_ms is None:
        start_ts_ms = config.ingestion.historical_start_ts_ms
    if start_ts_ms is None:
        raise CliConfigError(
            "No start timestamp provided. Use --start-ts-ms or set "
            "ingestion.historical_start_ts_ms in config."
        )

    end_ts_ms = args.end_ts_ms
    if end_ts_ms is None:
        end_ts_ms = int(time.time() * 1000)

    logger.info(
        f"Starting historical kline sync: venue={venue} market_type={market_type} "
        f"symbols={symbols} intervals={intervals} "
        f"start_ts_ms={start_ts_ms} end_ts_ms={end_ts_ms} dry_run={dry_run}"
    )

    # Create infrastructure
    engine = create_mysql_engine(config.mysql)
    session_factory = create_session_factory(engine)
    gateway = BinanceGateway(config.binance)
    kline_repo = KlineRepository()
    checkpoint_repo = CheckpointRepository(session_factory)

    grand_total = 0
    errors = 0

    try:
        for symbol in symbols:
            for interval in intervals:
                try:
                    rows = await _sync_symbol_interval(
                        gateway=gateway,
                        kline_repo=kline_repo,
                        checkpoint_repo=checkpoint_repo,
                        session_factory=session_factory,
                        venue=venue,
                        market_type=market_type,
                        symbol=symbol,
                        interval=interval,
                        start_ts_ms=start_ts_ms,
                        end_ts_ms=end_ts_ms,
                        dry_run=dry_run,
                        logger=logger,
                    )
                    grand_total += rows
                except Exception as exc:
                    errors += 1
                    logger.error(
                        f"Failed syncing {symbol}/{interval}: {exc}. Continuing."
                    )
    finally:
        await gateway.close()

    logger.info(f"Historical kline sync complete: total_rows={grand_total} errors={errors}")
    return grand_total


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    try:
        args = parser.parse_args(argv)
        configure_logging(args.log_level)
        logger = get_logger(APP_NAME)

        total_rows = asyncio.run(run_async(args))

        message = f"Synced {total_rows} kline rows"
        return emit_final_status(APP_NAME, ExitCode.SUCCESS, message)

    except CliConfigError as exc:
        return emit_final_status(APP_NAME, exc.exit_code, str(exc))
    except Exception as exc:
        logger = get_logger(APP_NAME)
        logger.exception(f"Unexpected error: {exc}")
        return emit_final_status(APP_NAME, ExitCode.GENERAL_FAILURE, str(exc))


if __name__ == "__main__":
    sys.exit(main())
