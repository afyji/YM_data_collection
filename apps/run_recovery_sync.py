"""Recover recent gaps based on checkpoints.

Finds all checkpoints with status='error', re-fetches data from
the appropriate Binance endpoint using the checkpoint's
last_event_ts_ms as the resume point, and updates the checkpoint
on success.
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

# Load .env early so that secret references resolve correctly
try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

from YM_data_collection.apps._cli_common import (
    CliConfigError,
    add_common_arguments,
    add_flag_argument,
    add_list_argument,
    emit_final_status,
)
from YM_data_collection.adapters.binance_gateway import BinanceGateway
from YM_data_collection.config.loader import load_config
from YM_data_collection.domain.models import IngestCheckpoint
from YM_data_collection.normalization.derivatives_normalizer import (
    normalize_funding_rates_batch,
    normalize_mark_price_klines_batch,
    normalize_open_interest_hist_batch,
)
from YM_data_collection.normalization.kline_normalizer import (
    normalize_binance_klines_batch,
)
from YM_data_collection.persistence.mysql import (
    create_mysql_engine,
    create_session_factory,
)
from YM_data_collection.persistence.repositories.checkpoint_repo import (
    CheckpointRepository,
)
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
from YM_data_collection.validation.derivatives_validator import (
    validate_funding_rate,
    validate_mark_price,
    validate_open_interest,
)
from YM_data_collection.validation.kline_validator import validate_klines_batch

APP_NAME = "run_recovery_sync"
SUPPORTED_RECOVERY_DATA_TYPES = ["kline", "mark_price", "open_interest", "funding_rate"]

# Batch limits per market type for kline recovery
BATCH_LIMITS: dict[str, int] = {
    "spot": 1000,
    "perp": 1500,
}

# Table names per market type for kline recovery
TABLE_NAMES: dict[str, str] = {
    "spot": "spot_klines",
    "perp": "perp_klines",
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Recover recent gaps using checkpoints.")
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
        "--data-types",
        default=SUPPORTED_RECOVERY_DATA_TYPES,
        choices=SUPPORTED_RECOVERY_DATA_TYPES,
        help_text="Data types to recover.",
    )
    add_flag_argument(parser, "--since-last-checkpoint", "Recover only from the last stored checkpoint.")
    return parser


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _ts_ms_to_utc(ts_ms: int) -> datetime:
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)


# ---------------------------------------------------------------------------
# Per-data-type recovery logic
# ---------------------------------------------------------------------------


async def _recover_kline(
    *,
    gateway: BinanceGateway,
    checkpoint_repo: CheckpointRepository,
    session_factory: Any,
    cp: IngestCheckpoint,
    start_ts_ms: int,
    end_ts_ms: int,
    logger: Any,
) -> bool:
    """Recover a single kline error checkpoint. Returns True on success."""
    kline_repo = KlineRepository()
    table_name = TABLE_NAMES.get(cp.market_type, "spot_klines")
    limit = BATCH_LIMITS.get(cp.market_type, 1000)
    total_rows = 0
    current_start = start_ts_ms

    while current_start <= end_ts_ms:
        try:
            raw_klines = await gateway.fetch_klines(
                market_type=cp.market_type,
                symbol=cp.symbol,
                interval=cp.interval_code,
                start_ts_ms=current_start,
                end_ts_ms=end_ts_ms,
                limit=limit,
            )
        except Exception as exc:
            logger.error(
                "Recovery kline %s/%s fetch error at ts=%d: %s",
                cp.symbol, cp.interval_code, current_start, exc,
            )
            _update_checkpoint_error(checkpoint_repo, cp, str(exc))
            return False

        if not raw_klines:
            break

        # Normalize
        normalized = normalize_binance_klines_batch(
            raw_klines, cp.venue, cp.symbol, cp.market_type, cp.interval_code,
        )

        # Validate
        validation_results = validate_klines_batch(normalized, cp.interval_code)
        valid_klines = []
        for kline, vr in zip(normalized, validation_results):
            if vr.is_valid:
                valid_klines.append(kline)
            elif vr.is_repairable and vr.repaired_kline is not None:
                valid_klines.append(vr.repaired_kline)
            else:
                logger.warning(
                    "Skipping invalid kline %s/%s open_ts_ms=%d: %s",
                    cp.symbol, cp.interval_code, kline.open_ts_ms, vr.issues,
                )

        # Persist
        if valid_klines:
            try:
                rows = kline_repo.upsert_batch(session_factory, table_name, valid_klines)
                total_rows += rows
            except Exception as exc:
                logger.error(
                    "Recovery kline %s/%s persist error: %s",
                    cp.symbol, cp.interval_code, exc,
                )
                _update_checkpoint_error(checkpoint_repo, cp, str(exc))
                return False

        # Find last open_ts_ms for checkpoint update
        last_open_ts_ms = max(int(k[0]) for k in raw_klines)

        # Update checkpoint after each successful batch
        checkpoint_repo.upsert(
            IngestCheckpoint(
                venue=cp.venue,
                market_type=cp.market_type,
                symbol=cp.symbol,
                data_type="kline",
                interval_code=cp.interval_code,
                last_event_ts_ms=last_open_ts_ms,
                last_event_dt_utc=_ts_ms_to_utc(last_open_ts_ms),
                last_kline_open_ts_ms=last_open_ts_ms,
                status="ok",
                last_success_at_utc=datetime.now(timezone.utc),
            )
        )

        logger.info(
            "Recovery kline %s/%s: batch fetched=%d valid=%d total_rows=%d",
            cp.symbol, cp.interval_code, len(raw_klines), len(valid_klines), total_rows,
        )

        # Advance pagination
        current_start = last_open_ts_ms + 1
        if len(raw_klines) < limit:
            break

    logger.info(
        "Recovery kline %s/%s complete: total_rows=%d", cp.symbol, cp.interval_code, total_rows,
    )
    return True


async def _recover_funding_rate(
    *,
    gateway: BinanceGateway,
    checkpoint_repo: CheckpointRepository,
    session_factory: Any,
    cp: IngestCheckpoint,
    start_ts_ms: int,
    end_ts_ms: int,
    logger: Any,
) -> bool:
    """Recover a single funding_rate error checkpoint. Returns True on success."""
    repo = FundingRateRepository()
    total_upserted = 0
    current_start = start_ts_ms

    while current_start <= end_ts_ms:
        try:
            raw_batch = await gateway.fetch_funding_rates(
                symbol=cp.symbol,
                start_ts_ms=current_start,
                end_ts_ms=end_ts_ms,
                limit=1000,
            )
        except Exception as exc:
            logger.error("Recovery funding_rate %s fetch error: %s", cp.symbol, exc)
            _update_checkpoint_error(checkpoint_repo, cp, str(exc))
            return False

        if not raw_batch:
            break

        normalized = normalize_funding_rates_batch(raw_batch, venue=cp.venue)

        valid = []
        for rec in normalized:
            vr = validate_funding_rate(rec)
            if vr.is_valid:
                valid.append(rec)
            else:
                logger.warning(
                    "funding_rate %s ts=%d invalid: %s",
                    cp.symbol, rec.funding_time_ts_ms, vr.issues,
                )

        if valid:
            try:
                upserted = repo.upsert_batch(session_factory, valid)
                total_upserted += upserted
            except Exception as exc:
                logger.error("Recovery funding_rate %s persist error: %s", cp.symbol, exc)
                _update_checkpoint_error(checkpoint_repo, cp, str(exc))
                return False

        # Determine last timestamp
        last_ts = max(r.funding_time_ts_ms for r in normalized) if normalized else None
        if last_ts is None:
            break

        checkpoint_repo.upsert(
            IngestCheckpoint(
                venue=cp.venue,
                market_type=cp.market_type,
                symbol=cp.symbol,
                data_type="funding_rate",
                interval_code=None,
                last_event_ts_ms=last_ts,
                last_event_dt_utc=_ts_ms_to_utc(last_ts),
                status="ok",
                last_success_at_utc=datetime.now(timezone.utc),
            )
        )

        next_start = last_ts + 1
        if next_start <= current_start:
            break
        current_start = next_start

    logger.info(
        "Recovery funding_rate %s complete: total_rows=%d", cp.symbol, total_upserted,
    )
    return True


async def _recover_mark_price(
    *,
    gateway: BinanceGateway,
    checkpoint_repo: CheckpointRepository,
    session_factory: Any,
    cp: IngestCheckpoint,
    start_ts_ms: int,
    end_ts_ms: int,
    logger: Any,
) -> bool:
    """Recover a single mark_price error checkpoint. Returns True on success."""
    repo = MarkPriceRepository()
    total_upserted = 0
    current_start = start_ts_ms
    interval = cp.interval_code or "1h"

    while current_start <= end_ts_ms:
        try:
            raw_batch = await gateway.fetch_mark_price_klines(
                symbol=cp.symbol,
                interval=interval,
                start_ts_ms=current_start,
                end_ts_ms=end_ts_ms,
                limit=500,
            )
        except Exception as exc:
            logger.error("Recovery mark_price %s fetch error: %s", cp.symbol, exc)
            _update_checkpoint_error(checkpoint_repo, cp, str(exc))
            return False

        if not raw_batch:
            break

        normalized = normalize_mark_price_klines_batch(
            raw_batch, venue=cp.venue, symbol=cp.symbol,
        )

        valid = []
        for rec in normalized:
            vr = validate_mark_price(rec)
            if vr.is_valid:
                valid.append(rec)
            else:
                logger.warning(
                    "mark_price %s ts=%d invalid: %s",
                    cp.symbol, rec.event_ts_ms, vr.issues,
                )

        if valid:
            try:
                upserted = repo.upsert_batch(session_factory, valid)
                total_upserted += upserted
            except Exception as exc:
                logger.error("Recovery mark_price %s persist error: %s", cp.symbol, exc)
                _update_checkpoint_error(checkpoint_repo, cp, str(exc))
                return False

        last_ts = max(r.event_ts_ms for r in normalized) if normalized else None
        if last_ts is None:
            break

        checkpoint_repo.upsert(
            IngestCheckpoint(
                venue=cp.venue,
                market_type=cp.market_type,
                symbol=cp.symbol,
                data_type="mark_price",
                interval_code=interval,
                last_event_ts_ms=last_ts,
                last_event_dt_utc=_ts_ms_to_utc(last_ts),
                last_kline_open_ts_ms=last_ts,
                status="ok",
                last_success_at_utc=datetime.now(timezone.utc),
            )
        )

        next_start = last_ts + 1
        if next_start <= current_start:
            break
        current_start = next_start

    logger.info(
        "Recovery mark_price %s complete: total_rows=%d", cp.symbol, total_upserted,
    )
    return True


async def _recover_open_interest(
    *,
    gateway: BinanceGateway,
    checkpoint_repo: CheckpointRepository,
    session_factory: Any,
    cp: IngestCheckpoint,
    start_ts_ms: int,
    end_ts_ms: int,
    logger: Any,
) -> bool:
    """Recover a single open_interest error checkpoint. Returns True on success."""
    try:
        validate_open_interest_history_range(start_ts_ms, end_ts_ms)
    except ValueError as exc:
        logger.error("Recovery open_interest %s unavailable: %s", cp.symbol, exc)
        _update_checkpoint_error(checkpoint_repo, cp, str(exc))
        return False

    repo = OpenInterestRepository()
    total_upserted = 0
    current_start = start_ts_ms
    period = cp.interval_code or "5m"

    while current_start <= end_ts_ms:
        try:
            raw_batch = await gateway.fetch_open_interest_hist(
                symbol=cp.symbol,
                period=period,
                start_ts_ms=current_start,
                end_ts_ms=end_ts_ms,
                limit=500,
            )
        except Exception as exc:
            logger.error("Recovery open_interest %s fetch error: %s", cp.symbol, exc)
            _update_checkpoint_error(checkpoint_repo, cp, str(exc))
            return False

        if not raw_batch:
            break

        normalized = normalize_open_interest_hist_batch(raw_batch, venue=cp.venue)

        valid = []
        for rec in normalized:
            vr = validate_open_interest(rec)
            if vr.is_valid:
                valid.append(rec)
            else:
                logger.warning(
                    "open_interest %s ts=%d invalid: %s",
                    cp.symbol, rec.event_ts_ms, vr.issues,
                )

        if valid:
            try:
                upserted = repo.upsert_batch(session_factory, valid)
                total_upserted += upserted
            except Exception as exc:
                logger.error("Recovery open_interest %s persist error: %s", cp.symbol, exc)
                _update_checkpoint_error(checkpoint_repo, cp, str(exc))
                return False

        last_ts = max(r.event_ts_ms for r in normalized) if normalized else None
        if last_ts is None:
            break

        checkpoint_repo.upsert(
            IngestCheckpoint(
                venue=cp.venue,
                market_type=cp.market_type,
                symbol=cp.symbol,
                data_type="open_interest",
                interval_code=period,
                last_event_ts_ms=last_ts,
                last_event_dt_utc=_ts_ms_to_utc(last_ts),
                status="ok",
                last_success_at_utc=datetime.now(timezone.utc),
            )
        )

        next_start = last_ts + 1
        if next_start <= current_start:
            break
        current_start = next_start

    logger.info(
        "Recovery open_interest %s complete: total_rows=%d", cp.symbol, total_upserted,
    )
    return True


# ---------------------------------------------------------------------------
# Checkpoint error update helper
# ---------------------------------------------------------------------------


def _update_checkpoint_error(
    checkpoint_repo: CheckpointRepository,
    cp: IngestCheckpoint,
    error_message: str,
) -> None:
    """Update checkpoint with error status, preserving existing position info."""
    checkpoint_repo.upsert(
        IngestCheckpoint(
            venue=cp.venue,
            market_type=cp.market_type,
            symbol=cp.symbol,
            data_type=cp.data_type,
            interval_code=cp.interval_code,
            last_event_ts_ms=cp.last_event_ts_ms,
            last_event_dt_utc=cp.last_event_dt_utc,
            last_kline_open_ts_ms=cp.last_kline_open_ts_ms,
            status="error",
            last_error_message=error_message[:1024],
        )
    )


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


async def run_async(args: argparse.Namespace) -> dict[str, int]:
    """Main async orchestration. Returns summary dict."""
    configure_logging(args.log_level)
    logger = get_logger(APP_NAME)

    # Load config
    try:
        config = load_config(config_path=args.config, env_name=args.env)
    except Exception as exc:
        raise CliConfigError(f"Failed to load config: {exc}") from exc

    # Determine parameters
    venue = args.venue
    market_type = args.market_type
    symbols = args.symbols
    data_types_filter = set(args.data_types)

    # End timestamp: now
    end_ts_ms = int(time.time() * 1000)

    # Start timestamp fallback
    fallback_start_ts_ms = config.ingestion.historical_start_ts_ms

    # Create infrastructure
    engine = create_mysql_engine(config.mysql)
    session_factory = create_session_factory(engine)
    gateway = BinanceGateway(config.binance)
    checkpoint_repo = CheckpointRepository(session_factory)

    recovered = 0
    still_failing = 0

    try:
        # Query all error checkpoints
        error_checkpoints = checkpoint_repo.list_by_status("error")

        logger.info(
            "Found %d error checkpoint(s) for potential recovery.", len(error_checkpoints),
        )

        for cp in error_checkpoints:
            # Filter by data types
            if cp.data_type not in data_types_filter:
                logger.info(
                    "Skipping %s/%s: data_type=%s not in filter.",
                    cp.symbol, cp.data_type, cp.data_type,
                )
                continue

            # Filter by venue
            if cp.venue != venue:
                logger.debug(
                    "Skipping %s/%s: venue=%s != %s.",
                    cp.symbol, cp.data_type, cp.venue, venue,
                )
                continue

            # Filter by market_type
            if cp.market_type != market_type:
                logger.debug(
                    "Skipping %s/%s: market_type=%s != %s.",
                    cp.symbol, cp.data_type, cp.market_type, market_type,
                )
                continue

            # Filter by symbols (if specified)
            if symbols is not None and cp.symbol not in symbols:
                logger.debug(
                    "Skipping %s/%s: symbol not in --symbols filter.",
                    cp.symbol, cp.data_type,
                )
                continue

            # Determine start point
            start_ts_ms = cp.last_event_ts_ms if cp.last_event_ts_ms is not None else fallback_start_ts_ms
            if start_ts_ms is None:
                logger.warning(
                    "Skipping %s/%s: no last_event_ts_ms and no historical_start_ts_ms in config.",
                    cp.symbol, cp.data_type,
                )
                continue

            logger.info(
                "Recovering %s/%s start_ts_ms=%d end_ts_ms=%d",
                cp.symbol, cp.data_type, start_ts_ms, end_ts_ms,
            )

            # Dispatch to the appropriate recovery function
            try:
                if cp.data_type == "kline":
                    success = await _recover_kline(
                        gateway=gateway,
                        checkpoint_repo=checkpoint_repo,
                        session_factory=session_factory,
                        cp=cp,
                        start_ts_ms=start_ts_ms,
                        end_ts_ms=end_ts_ms,
                        logger=logger,
                    )
                elif cp.data_type == "funding_rate":
                    success = await _recover_funding_rate(
                        gateway=gateway,
                        checkpoint_repo=checkpoint_repo,
                        session_factory=session_factory,
                        cp=cp,
                        start_ts_ms=start_ts_ms,
                        end_ts_ms=end_ts_ms,
                        logger=logger,
                    )
                elif cp.data_type == "mark_price":
                    success = await _recover_mark_price(
                        gateway=gateway,
                        checkpoint_repo=checkpoint_repo,
                        session_factory=session_factory,
                        cp=cp,
                        start_ts_ms=start_ts_ms,
                        end_ts_ms=end_ts_ms,
                        logger=logger,
                    )
                elif cp.data_type == "open_interest":
                    success = await _recover_open_interest(
                        gateway=gateway,
                        checkpoint_repo=checkpoint_repo,
                        session_factory=session_factory,
                        cp=cp,
                        start_ts_ms=start_ts_ms,
                        end_ts_ms=end_ts_ms,
                        logger=logger,
                    )
                else:
                    logger.warning(
                        "Unknown data_type=%s for checkpoint %s/%s, skipping.",
                        cp.data_type, cp.symbol, cp.data_type,
                    )
                    continue

                if success:
                    recovered += 1
                else:
                    still_failing += 1

            except Exception as exc:
                logger.error(
                    "Unexpected error recovering %s/%s: %s",
                    cp.symbol, cp.data_type, exc, exc_info=True,
                )
                _update_checkpoint_error(checkpoint_repo, cp, str(exc))
                still_failing += 1

    finally:
        await gateway.close()

    summary = {"recovered": recovered, "still_failing": still_failing}
    logger.info(
        "Recovery sync complete: recovered=%d still_failing=%d",
        recovered, still_failing,
    )
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()

    try:
        args = parser.parse_args(argv)
        configure_logging(args.log_level)
        logger = get_logger(APP_NAME)

        summary = asyncio.run(run_async(args))

        message = (
            f"Recovery complete: recovered={summary['recovered']} "
            f"still_failing={summary['still_failing']}"
        )
        if summary["still_failing"] > 0:
            return emit_final_status(APP_NAME, ExitCode.GENERAL_FAILURE, message)
        return emit_final_status(APP_NAME, ExitCode.SUCCESS, message)

    except CliConfigError as exc:
        return emit_final_status(APP_NAME, exc.exit_code, str(exc))
    except Exception as exc:
        logger = get_logger(APP_NAME)
        logger.error("Unexpected error: %s", exc, exc_info=True)
        return emit_final_status(APP_NAME, ExitCode.GENERAL_FAILURE, str(exc))


if __name__ == "__main__":
    sys.exit(main())
