"""Resync a specific symbol and data_type for a given time range.

Manual resync script that fetches data from Binance for a specific
symbol + data_type + time range, normalizes, validates, upserts to
MySQL, and updates checkpoints.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

if __package__ is None or __package__ == "":
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from YM_data_collection.apps._cli_common import (
    CliArgumentError,
    CliConfigError,
    add_common_arguments,
    add_flag_argument,
    add_ts_ms_argument,
    emit_final_status,
    render_choice_help,
)
from YM_data_collection.adapters.binance_gateway import BinanceGateway, MarketType
from YM_data_collection.config.loader import load_config
from YM_data_collection.domain.models import IngestCheckpoint, NormalizedKline
from YM_data_collection.normalization.derivatives_normalizer import (
    normalize_funding_rates_batch,
    normalize_mark_price_klines_batch,
    normalize_open_interest_hist_batch,
)
from YM_data_collection.normalization.kline_normalizer import normalize_binance_klines_batch
from YM_data_collection.persistence.mysql import create_mysql_engine, create_session_factory
from YM_data_collection.persistence.repositories.checkpoint_repo import CheckpointRepository
from YM_data_collection.persistence.repositories.marketdata_repo import (
    FundingRateRepository,
    KlineRepository,
    MarkPriceRepository,
    OpenInterestRepository,
)
from YM_data_collection.utils.exit_codes import ExitCode
from YM_data_collection.utils.logging_utils import configure_logging, get_logger
from YM_data_collection.utils.binance_constraints import (
    validate_open_interest_history_range,
)
from YM_data_collection.validation.kline_validator import INTERVAL_DURATIONS_MS
from YM_data_collection.validation.derivatives_validator import (
    validate_funding_rate,
    validate_mark_price,
    validate_open_interest,
)
from YM_data_collection.validation.kline_validator import validate_klines_batch

APP_NAME = "run_resync_range"
SUPPORTED_RESYNC_DATA_TYPES = ["kline", "funding_rate", "mark_price", "open_interest"]
SUPPORTED_KLINE_INTERVALS = list(INTERVAL_DURATIONS_MS.keys())

# Batch limits per data type
BATCH_LIMITS: dict[str, dict[str, int]] = {
    "kline": {"spot": 1000, "perp": 1500},
    "funding_rate": {"_default": 1000},
    "mark_price": {"_default": 500},
    "open_interest": {"_default": 500},
}

# Table names per market type for klines
KLINE_TABLE_NAMES: dict[str, str] = {
    "spot": "spot_klines",
    "perp": "perp_klines",
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Manual partial resync for one symbol and time range. "
            "Use this for local gap repair, not full historical backfill. "
            "Supports kline, funding_rate, mark_price, and open_interest. "
            "index_price is not supported here."
        )
    )
    add_common_arguments(
        parser,
        include_config=True,
        include_env=True,
        include_venue=True,
        include_market_type=True,
    )
    parser.add_argument("--symbol", required=False, help="Target symbol.")
    parser.add_argument(
        "--data-type",
        required=False,
        choices=SUPPORTED_RESYNC_DATA_TYPES,
        help=render_choice_help(
            "Target data type. kline requires --interval; "
            "funding_rate has no interval; mark_price is fixed to 1h; "
            "open_interest is fixed to 5m and limited by Binance to the latest 1 month.",
            SUPPORTED_RESYNC_DATA_TYPES,
        ),
    )
    parser.add_argument(
        "--interval",
        required=False,
        choices=SUPPORTED_KLINE_INTERVALS,
        help=render_choice_help(
            "Target interval for kline data only. "
            "Ignored for funding_rate, mark_price, and open_interest.",
            SUPPORTED_KLINE_INTERVALS,
        ),
    )
    add_ts_ms_argument(parser, "--start-ts-ms", boundary="start", help_prefix="Inclusive start time.")
    add_ts_ms_argument(parser, "--end-ts-ms", boundary="end", help_prefix="Inclusive end time.")
    add_flag_argument(parser, "--force", "Force resync even if local checkpoints exist.")
    return parser


def _validate_args(args: argparse.Namespace) -> None:
    """Validate required runtime arguments."""
    if not args.symbol:
        raise CliArgumentError("--symbol is required")
    if not args.data_type:
        raise CliArgumentError("--data-type is required")
    if args.start_ts_ms is None:
        raise CliArgumentError("--start-ts-ms is required")
    if args.data_type == "kline" and not args.interval:
        raise CliArgumentError("--interval is required when data_type is 'kline'")


def _ts_ms_to_utc(ts_ms: int) -> datetime:
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)


def _get_batch_limit(data_type: str, market_type: str) -> int:
    """Return the batch limit for a data_type + market_type combination."""
    limits = BATCH_LIMITS.get(data_type, {})
    return limits.get(market_type, limits.get("_default", 1000))


def _update_checkpoint(
    checkpoint_repo: CheckpointRepository,
    venue: str,
    market_type: str,
    symbol: str,
    data_type: str,
    interval_code: str | None,
    status: str,
    last_event_ts_ms: int | None,
    last_kline_open_ts_ms: int | None = None,
    error_message: str | None = None,
) -> None:
    """Upsert checkpoint after a batch."""
    now = datetime.now(timezone.utc)
    last_event_dt_utc = None
    if last_event_ts_ms is not None:
        last_event_dt_utc = _ts_ms_to_utc(last_event_ts_ms)

    checkpoint = IngestCheckpoint(
        venue=venue,
        market_type=market_type,
        symbol=symbol,
        data_type=data_type,
        interval_code=interval_code,
        last_event_ts_ms=last_event_ts_ms,
        last_event_dt_utc=last_event_dt_utc,
        last_kline_open_ts_ms=last_kline_open_ts_ms,
        status=status,
        last_success_at_utc=now if status == "ok" else None,
        last_error_message=error_message,
    )
    checkpoint_repo.upsert(checkpoint)


# ---------------------------------------------------------------------------
# Per-data-type resync logic
# ---------------------------------------------------------------------------


async def _resync_kline(
    gateway: BinanceGateway,
    session_factory: Any,
    checkpoint_repo: CheckpointRepository,
    venue: str,
    market_type: str,
    symbol: str,
    interval: str,
    start_ts_ms: int,
    end_ts_ms: int,
    logger: Any,
) -> int:
    """Paginated resync for kline data_type. Returns total rows upserted."""
    table_name = KLINE_TABLE_NAMES[market_type]
    limit = _get_batch_limit("kline", market_type)
    kline_repo = KlineRepository()
    total_rows = 0
    current_start = start_ts_ms

    while current_start <= end_ts_ms:
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
            logger.error("Error fetching klines %s/%s: %s", symbol, interval, exc)
            _update_checkpoint(
                checkpoint_repo, venue, market_type, symbol, "kline",
                interval, "error", None, None, str(exc),
            )
            raise

        if not raw_klines:
            logger.info("kline %s/%s: no more data at start=%d", symbol, interval, current_start)
            break

        # Normalize
        normalized = normalize_binance_klines_batch(
            raw_klines, venue, symbol, market_type, interval
        )

        # Validate
        validation_results = validate_klines_batch(normalized, interval)
        valid_klines: list[NormalizedKline] = []
        for kline, vr in zip(normalized, validation_results):
            if vr.is_valid:
                valid_klines.append(kline)
            elif vr.is_repairable and vr.repaired_kline is not None:
                valid_klines.append(vr.repaired_kline)
            else:
                logger.warning(
                    "Skipping invalid kline %s/%s open_ts_ms=%d: %s",
                    symbol, interval, kline.open_ts_ms, vr.issues,
                )

        # Last open_ts_ms from raw data for checkpoint/pagination
        last_open_ts_ms = max(int(k[0]) for k in raw_klines)

        if valid_klines:
            rows_affected = kline_repo.upsert_batch(
                session_factory, table_name, valid_klines
            )
            total_rows += rows_affected

        # Update checkpoint
        _update_checkpoint(
            checkpoint_repo, venue, market_type, symbol, "kline",
            interval, "ok", last_open_ts_ms, last_open_ts_ms,
        )

        logger.info(
            "kline %s/%s: fetched=%d valid=%d total_rows=%d last_open_ts_ms=%d",
            symbol, interval, len(raw_klines), len(valid_klines),
            total_rows, last_open_ts_ms,
        )

        # Next batch starts from last_open_ts_ms + 1
        current_start = last_open_ts_ms + 1

        # If fewer results than limit, we've likely reached the end
        if len(raw_klines) < limit:
            break

    return total_rows


async def _resync_funding_rate(
    gateway: BinanceGateway,
    session_factory: Any,
    checkpoint_repo: CheckpointRepository,
    venue: str,
    market_type: str,
    symbol: str,
    start_ts_ms: int,
    end_ts_ms: int,
    logger: Any,
) -> int:
    """Paginated resync for funding_rate data_type. Returns total rows upserted."""
    repo = FundingRateRepository()
    limit = _get_batch_limit("funding_rate", market_type)
    total_upserted = 0
    current_start = start_ts_ms

    while current_start <= end_ts_ms:
        raw_batch = await gateway.fetch_funding_rates(
            symbol=symbol,
            start_ts_ms=current_start,
            end_ts_ms=end_ts_ms,
            limit=limit,
        )
        if not raw_batch:
            logger.info("funding_rate %s: empty batch at start=%d", symbol, current_start)
            break

        normalized = normalize_funding_rates_batch(raw_batch, venue=venue)

        # Validate; skip invalid records
        valid = []
        for rec in normalized:
            vr = validate_funding_rate(rec)
            if vr.is_valid:
                valid.append(rec)
            else:
                logger.warning(
                    "funding_rate %s ts=%d invalid: %s",
                    symbol, rec.funding_time_ts_ms, vr.issues,
                )

        if valid:
            upserted = repo.upsert_batch(session_factory, valid)
            total_upserted += upserted

        last_ts: int | None = None
        if normalized:
            last_ts = max(r.funding_time_ts_ms for r in normalized)
        if last_ts is None:
            break

        # Update checkpoint
        _update_checkpoint(
            checkpoint_repo, venue, market_type, symbol, "funding_rate",
            None, "ok", last_ts,
        )

        next_start = last_ts + 1
        if next_start <= current_start:
            logger.warning("funding_rate %s: pagination stall at ts=%d", symbol, last_ts)
            break
        current_start = next_start

    logger.info(
        "funding_rate %s: synced %d rows in [%d, %d]",
        symbol, total_upserted, start_ts_ms, end_ts_ms,
    )
    return total_upserted


async def _resync_mark_price(
    gateway: BinanceGateway,
    session_factory: Any,
    checkpoint_repo: CheckpointRepository,
    venue: str,
    market_type: str,
    symbol: str,
    start_ts_ms: int,
    end_ts_ms: int,
    logger: Any,
) -> int:
    """Paginated resync for mark_price data_type. Returns total rows upserted."""
    repo = MarkPriceRepository()
    limit = _get_batch_limit("mark_price", market_type)
    interval = "1h"
    total_upserted = 0
    current_start = start_ts_ms

    while current_start <= end_ts_ms:
        raw_batch = await gateway.fetch_mark_price_klines(
            symbol=symbol,
            interval=interval,
            start_ts_ms=current_start,
            end_ts_ms=end_ts_ms,
            limit=limit,
        )
        if not raw_batch:
            logger.info("mark_price %s: empty batch at start=%d", symbol, current_start)
            break

        normalized = normalize_mark_price_klines_batch(raw_batch, venue=venue, symbol=symbol)

        # Validate
        valid = []
        for rec in normalized:
            vr = validate_mark_price(rec)
            if vr.is_valid:
                valid.append(rec)
            else:
                logger.warning(
                    "mark_price %s ts=%d invalid: %s",
                    symbol, rec.event_ts_ms, vr.issues,
                )

        if valid:
            upserted = repo.upsert_batch(session_factory, valid)
            total_upserted += upserted

        last_ts: int | None = None
        if normalized:
            last_ts = max(r.event_ts_ms for r in normalized)
        if last_ts is None:
            break

        # Update checkpoint
        _update_checkpoint(
            checkpoint_repo, venue, market_type, symbol, "mark_price",
            interval, "ok", last_ts, last_ts,
        )

        next_start = last_ts + 1
        if next_start <= current_start:
            logger.warning("mark_price %s: pagination stall at ts=%d", symbol, last_ts)
            break
        current_start = next_start

    logger.info(
        "mark_price %s: synced %d rows in [%d, %d]",
        symbol, total_upserted, start_ts_ms, end_ts_ms,
    )
    return total_upserted


async def _resync_open_interest(
    gateway: BinanceGateway,
    session_factory: Any,
    checkpoint_repo: CheckpointRepository,
    venue: str,
    market_type: str,
    symbol: str,
    start_ts_ms: int,
    end_ts_ms: int,
    logger: Any,
) -> int:
    """Paginated resync for open_interest data_type. Returns total rows upserted."""
    validate_open_interest_history_range(start_ts_ms, end_ts_ms)

    repo = OpenInterestRepository()
    limit = _get_batch_limit("open_interest", market_type)
    period = "5m"
    total_upserted = 0
    current_start = start_ts_ms

    while current_start <= end_ts_ms:
        raw_batch = await gateway.fetch_open_interest_hist(
            symbol=symbol,
            period=period,
            start_ts_ms=current_start,
            end_ts_ms=end_ts_ms,
            limit=limit,
        )
        if not raw_batch:
            logger.info("open_interest %s: empty batch at start=%d", symbol, current_start)
            break

        normalized = normalize_open_interest_hist_batch(raw_batch, venue=venue)

        # Validate
        valid = []
        for rec in normalized:
            vr = validate_open_interest(rec)
            if vr.is_valid:
                valid.append(rec)
            else:
                logger.warning(
                    "open_interest %s ts=%d invalid: %s",
                    symbol, rec.event_ts_ms, vr.issues,
                )

        if valid:
            upserted = repo.upsert_batch(session_factory, valid)
            total_upserted += upserted

        last_ts: int | None = None
        if normalized:
            last_ts = max(r.event_ts_ms for r in normalized)
        if last_ts is None:
            break

        # Update checkpoint
        _update_checkpoint(
            checkpoint_repo, venue, market_type, symbol, "open_interest",
            period, "ok", last_ts,
        )

        next_start = last_ts + 1
        if next_start <= current_start:
            logger.warning("open_interest %s: pagination stall at ts=%d", symbol, last_ts)
            break
        current_start = next_start

    logger.info(
        "open_interest %s: synced %d rows in [%d, %d]",
        symbol, total_upserted, start_ts_ms, end_ts_ms,
    )
    return total_upserted


# ---------------------------------------------------------------------------
# Main resync dispatcher
# ---------------------------------------------------------------------------


async def _resync_range(
    gateway: BinanceGateway,
    session_factory: Any,
    checkpoint_repo: CheckpointRepository,
    venue: str,
    market_type: str,
    symbol: str,
    data_type: str,
    interval: str | None,
    start_ts_ms: int,
    end_ts_ms: int,
    force: bool,
    logger: Any,
) -> int:
    """Dispatch resync based on data_type. Returns total rows synced."""

    # Check checkpoint if not forced
    if not force:
        interval_code = interval if data_type == "kline" else None
        if data_type == "mark_price":
            interval_code = "1h"
        elif data_type == "open_interest":
            interval_code = "5m"

        checkpoint = checkpoint_repo.get(
            venue=venue,
            market_type=market_type,
            symbol=symbol,
            data_type=data_type,
            interval_code=interval_code,
        )
        if checkpoint is not None and checkpoint.last_event_ts_ms is not None:
            if checkpoint.last_event_ts_ms >= end_ts_ms:
                logger.info(
                    "%s/%s: checkpoint (%d) >= end_ts_ms (%d), skipping",
                    symbol, data_type, checkpoint.last_event_ts_ms, end_ts_ms,
                )
                return 0

    # Dispatch to per-type handler
    if data_type == "kline":
        return await _resync_kline(
            gateway, session_factory, checkpoint_repo,
            venue, market_type, symbol, interval,  # type: ignore[arg-type]
            start_ts_ms, end_ts_ms, logger,
        )
    elif data_type == "funding_rate":
        return await _resync_funding_rate(
            gateway, session_factory, checkpoint_repo,
            venue, market_type, symbol,
            start_ts_ms, end_ts_ms, logger,
        )
    elif data_type == "mark_price":
        return await _resync_mark_price(
            gateway, session_factory, checkpoint_repo,
            venue, market_type, symbol,
            start_ts_ms, end_ts_ms, logger,
        )
    elif data_type == "open_interest":
        return await _resync_open_interest(
            gateway, session_factory, checkpoint_repo,
            venue, market_type, symbol,
            start_ts_ms, end_ts_ms, logger,
        )
    else:
        raise CliArgumentError(f"Unknown data_type: {data_type}")


async def run_async(args: argparse.Namespace) -> int:
    """Main async orchestration."""
    configure_logging(args.log_level)
    logger = get_logger(APP_NAME)

    # Validate arguments
    _validate_args(args)

    # Load config
    try:
        config = load_config(config_path=args.config, env_name=args.env)
    except Exception as exc:
        raise CliConfigError(f"Failed to load config: {exc}")

    # Determine parameters
    venue = args.venue
    market_type: MarketType = args.market_type  # type: ignore[assignment]
    symbol = args.symbol
    data_type = args.data_type
    interval = args.interval
    force = args.force

    # Time range
    start_ts_ms = args.start_ts_ms
    end_ts_ms = args.end_ts_ms
    if end_ts_ms is None:
        end_ts_ms = int(time.time() * 1000)

    logger.info(
        "Starting resync: venue=%s market_type=%s symbol=%s data_type=%s "
        "interval=%s start_ts_ms=%d end_ts_ms=%d force=%s",
        venue, market_type, symbol, data_type, interval,
        start_ts_ms, end_ts_ms, force,
    )

    # Create infrastructure
    engine = create_mysql_engine(config.mysql)
    session_factory = create_session_factory(engine)
    gateway = BinanceGateway(config.binance)
    checkpoint_repo = CheckpointRepository(session_factory)

    try:
        total_rows = await _resync_range(
            gateway=gateway,
            session_factory=session_factory,
            checkpoint_repo=checkpoint_repo,
            venue=venue,
            market_type=market_type,
            symbol=symbol,
            data_type=data_type,
            interval=interval,
            start_ts_ms=start_ts_ms,
            end_ts_ms=end_ts_ms,
            force=force,
            logger=logger,
        )
    finally:
        await gateway.close()

    logger.info("Resync complete: %d rows synced", total_rows)
    return total_rows


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    try:
        args = parser.parse_args(argv)
        configure_logging(args.log_level)
        logger = get_logger(APP_NAME)

        total_rows = asyncio.run(run_async(args))

        message = f"Synced {total_rows} rows"
        return emit_final_status(APP_NAME, ExitCode.SUCCESS, message)

    except CliArgumentError as exc:
        return emit_final_status(APP_NAME, exc.exit_code, str(exc))
    except CliConfigError as exc:
        return emit_final_status(APP_NAME, exc.exit_code, str(exc))
    except Exception as exc:
        logger = get_logger(APP_NAME)
        logger.exception("Unexpected error: %s", exc)
        return emit_final_status(APP_NAME, ExitCode.GENERAL_FAILURE, str(exc))


if __name__ == "__main__":
    sys.exit(main())
