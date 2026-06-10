"""Run historical derivatives synchronization.

Paginated fetch → normalize → validate → upsert loop for each
symbol × data_type combination, with checkpoint-based resume support.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

if __package__ is None or __package__ == "":
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

# ---------------------------------------------------------------------------
# Load .env early so that secret references resolve correctly
# ---------------------------------------------------------------------------
try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

from YM_data_collection.apps._cli_common import (
    CliArgumentError,
    CliConfigError,
    ExternalDependencyError,
    add_common_arguments,
    add_list_argument,
    add_ts_ms_argument,
    emit_final_status,
)
from YM_data_collection.config.loader import load_config
from YM_data_collection.domain.models import IngestCheckpoint
from YM_data_collection.normalization.derivatives_normalizer import (
    normalize_funding_rates_batch,
    normalize_index_price_klines_batch,
    normalize_mark_price_klines_batch,
    normalize_open_interest_hist_batch,
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
    IndexPriceRepository,
    MarkPriceRepository,
    OpenInterestRepository,
)
from YM_data_collection.utils.exit_codes import ExitCode
from YM_data_collection.utils.logging_utils import configure_logging, get_logger
from YM_data_collection.utils.binance_constraints import (
    compute_open_interest_overlap,
    validate_open_interest_history_range,
)
from YM_data_collection.validation.derivatives_validator import (
    validate_funding_rate,
    validate_index_price,
    validate_mark_price,
    validate_open_interest,
)

APP_NAME = "run_historical_derivatives_sync"
AUTO_DATA_TYPE_TOKEN = "auto"

# Data types that have dedicated Binance historical endpoints
_FETCHABLE_DATA_TYPES = {"funding_rate", "mark_price", "index_price", "open_interest"}
BEST_EFFORT_DATA_TYPE_ORDER = ["mark_price", "index_price", "open_interest", "funding_rate"]
FETCHABLE_DATA_TYPE_CHOICES = [
    AUTO_DATA_TYPE_TOKEN,
    "funding_rate",
    "mark_price",
    "index_price",
    "open_interest",
]

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Historical backfill for perp derivatives data. "
            "Supports funding_rate (no interval), mark_price and index_price "
            "(fixed 1h endpoints), and open_interest (fixed 5m endpoint). "
            "index_price uses Binance indexPriceKlines."
        ),
    )
    add_common_arguments(
        parser,
        include_config=True,
        include_env=True,
        include_venue=True,
        include_symbols=True,
    )
    add_list_argument(
        parser,
        "--data-types",
        default=[AUTO_DATA_TYPE_TOKEN],
        choices=FETCHABLE_DATA_TYPE_CHOICES,
        help_text=(
            "Derivative data types to backfill. "
            "Use 'auto' or omit this flag for best-effort mode. "
            "funding_rate uses the funding-rate history endpoint; "
            "mark_price uses fixed 1h mark-price klines; "
            "index_price uses fixed 1h index-price klines; "
            "open_interest uses fixed 5m open-interest history and is limited by Binance "
            "to the latest 1 month."
        ),
    )
    add_ts_ms_argument(parser, "--start-ts-ms", boundary="start", help_prefix="Inclusive start time.")
    add_ts_ms_argument(parser, "--end-ts-ms", boundary="end", help_prefix="Inclusive end time.")
    return parser


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _ts_ms_to_utc(ts_ms: int) -> datetime:
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)


def _last_ts_from_funding_rates(records: Sequence[Any]) -> int | None:
    """Return the latest funding_time_ts_ms from a batch."""
    if not records:
        return None
    return max(r.funding_time_ts_ms for r in records)


def _last_ts_from_mark_prices(records: Sequence[Any]) -> int | None:
    """Return the latest event_ts_ms from a batch of NormalizedMarkPrice."""
    if not records:
        return None
    return max(r.event_ts_ms for r in records)


def _last_ts_from_index_prices(records: Sequence[Any]) -> int | None:
    """Return the latest event_ts_ms from a batch of NormalizedIndexPrice."""
    if not records:
        return None
    return max(r.event_ts_ms for r in records)


def _last_ts_from_open_interest(records: Sequence[Any]) -> int | None:
    """Return the latest event_ts_ms from a batch of NormalizedOpenInterest."""
    if not records:
        return None
    return max(r.event_ts_ms for r in records)


def _resolve_requested_data_types(
    requested_data_types: Sequence[str] | None,
    *,
    start_ts_ms: int,
    end_ts_ms: int,
    logger: Any,
    now: datetime | None = None,
) -> tuple[list[str], dict[str, tuple[int, int]]]:
    """Resolve requested derivative data types for the given time range.

    Returns ``(resolved_types, range_overrides)`` where *range_overrides*
    maps a data type to ``(clamped_start_ts_ms, clamped_end_ts_ms)`` when
    the originally requested range must be narrowed for that type.

    - ``auto`` means "pull everything that is currently available for this range"
      - open_interest is included if the requested range *overlaps* Binance's
        latest-1-month window, and the range override reflects the overlap.
    - explicit ``open_interest`` remains strict and raises if Binance cannot
      serve the *entire* requested range.
    """

    requested = list(requested_data_types or [AUTO_DATA_TYPE_TOKEN])
    if AUTO_DATA_TYPE_TOKEN in requested and len(requested) > 1:
        raise CliArgumentError(
            f"--data-types {AUTO_DATA_TYPE_TOKEN} cannot be combined with specific data types"
        )

    range_overrides: dict[str, tuple[int, int]] = {}

    if requested == [AUTO_DATA_TYPE_TOKEN]:
        resolved: list[str] = []
        skipped_reasons: dict[str, str] = {}
        for data_type in BEST_EFFORT_DATA_TYPE_ORDER:
            if data_type == "open_interest":
                overlap = compute_open_interest_overlap(
                    start_ts_ms, end_ts_ms, now=now,
                )
                if overlap is None:
                    skipped_reasons[data_type] = (
                        "Binance openInterestHist only provides the latest 1 month of data; "
                        "requested range has no overlap with the available window"
                    )
                    continue
                # Only set an override if the overlap is strictly smaller
                # than the originally requested range.
                if overlap != (start_ts_ms, end_ts_ms):
                    range_overrides["open_interest"] = overlap
            resolved.append(data_type)

        logger.info("Auto-selected derivative data types: %s", resolved)
        for data_type, reason in skipped_reasons.items():
            logger.info("Auto-skipping %s: %s", data_type, reason)
        return resolved, range_overrides

    resolved = []
    for data_type in requested:
        if data_type == "open_interest":
            try:
                validate_open_interest_history_range(start_ts_ms, end_ts_ms, now=now)
            except ValueError as exc:
                raise CliArgumentError(str(exc)) from exc
        if data_type not in resolved:
            resolved.append(data_type)
    return resolved, range_overrides


# ---------------------------------------------------------------------------
# Per-data-type sync logic
# ---------------------------------------------------------------------------


async def _sync_funding_rate(
    gateway: Any,
    session_factory: Any,
    checkpoint_repo: CheckpointRepository,
    venue: str,
    symbol: str,
    start_ts_ms: int,
    end_ts_ms: int,
    logger: Any,
) -> int:
    """Paginated sync for funding_rate. Returns total rows upserted."""
    repo = FundingRateRepository()
    total_upserted = 0
    current_start = start_ts_ms

    while current_start <= end_ts_ms:
        raw_batch = await gateway.fetch_funding_rates(
            symbol=symbol,
            start_ts_ms=current_start,
            end_ts_ms=end_ts_ms,
            limit=1000,
        )
        if not raw_batch:
            logger.info(
                "funding_rate %s: empty batch at start=%d, stopping pagination.",
                symbol, current_start,
            )
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

        last_ts = _last_ts_from_funding_rates(normalized)
        if last_ts is None:
            break

        # Update checkpoint
        checkpoint_repo.upsert(
            IngestCheckpoint(
                venue=venue,
                market_type="usdt_perpetual",
                symbol=symbol,
                data_type="funding_rate",
                interval_code=None,
                last_event_ts_ms=last_ts,
                last_event_dt_utc=_ts_ms_to_utc(last_ts),
                status="ok",
                last_success_at_utc=datetime.now(timezone.utc),
            )
        )

        # Advance pagination: last timestamp + 1
        next_start = last_ts + 1
        if next_start <= current_start:
            # Guard against stale pagination
            logger.warning(
                "funding_rate %s: pagination stall at ts=%d, breaking.", symbol, last_ts,
            )
            break
        current_start = next_start

    logger.info(
        "funding_rate %s: synced %d rows in [%d, %d].",
        symbol, total_upserted, start_ts_ms, end_ts_ms,
    )
    return total_upserted


async def _sync_mark_price(
    gateway: Any,
    session_factory: Any,
    checkpoint_repo: CheckpointRepository,
    venue: str,
    symbol: str,
    start_ts_ms: int,
    end_ts_ms: int,
    logger: Any,
) -> int:
    """Paginated sync for mark_price. Returns total rows upserted."""
    repo = MarkPriceRepository()
    total_upserted = 0
    current_start = start_ts_ms
    interval = "1h"

    while current_start <= end_ts_ms:
        raw_batch = await gateway.fetch_mark_price_klines(
            symbol=symbol,
            interval=interval,
            start_ts_ms=current_start,
            end_ts_ms=end_ts_ms,
            limit=500,
        )
        if not raw_batch:
            logger.info(
                "mark_price %s: empty batch at start=%d, stopping pagination.",
                symbol, current_start,
            )
            break

        normalized = normalize_mark_price_klines_batch(
            raw_batch, venue=venue, symbol=symbol,
        )

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

        last_ts = _last_ts_from_mark_prices(normalized)
        if last_ts is None:
            break

        # Update checkpoint
        checkpoint_repo.upsert(
            IngestCheckpoint(
                venue=venue,
                market_type="usdt_perpetual",
                symbol=symbol,
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
            logger.warning(
                "mark_price %s: pagination stall at ts=%d, breaking.", symbol, last_ts,
            )
            break
        current_start = next_start

    logger.info(
        "mark_price %s: synced %d rows in [%d, %d].",
        symbol, total_upserted, start_ts_ms, end_ts_ms,
    )
    return total_upserted


async def _sync_index_price(
    gateway: Any,
    session_factory: Any,
    checkpoint_repo: CheckpointRepository,
    venue: str,
    symbol: str,
    start_ts_ms: int,
    end_ts_ms: int,
    logger: Any,
) -> int:
    """Paginated sync for index_price. Returns total rows upserted."""
    repo = IndexPriceRepository()
    total_upserted = 0
    current_start = start_ts_ms
    interval = "1h"

    while current_start <= end_ts_ms:
        raw_batch = await gateway.fetch_index_price_klines(
            symbol=symbol,
            interval=interval,
            start_ts_ms=current_start,
            end_ts_ms=end_ts_ms,
            limit=500,
        )
        if not raw_batch:
            logger.info(
                "index_price %s: empty batch at start=%d, stopping pagination.",
                symbol, current_start,
            )
            break

        normalized = normalize_index_price_klines_batch(
            raw_batch, venue=venue, symbol=symbol,
        )

        valid = []
        for rec in normalized:
            vr = validate_index_price(rec)
            if vr.is_valid:
                valid.append(rec)
            else:
                logger.warning(
                    "index_price %s ts=%d invalid: %s",
                    symbol, rec.event_ts_ms, vr.issues,
                )

        if valid:
            upserted = repo.upsert_batch(session_factory, valid)
            total_upserted += upserted

        last_ts = _last_ts_from_index_prices(normalized)
        if last_ts is None:
            break

        checkpoint_repo.upsert(
            IngestCheckpoint(
                venue=venue,
                market_type="usdt_perpetual",
                symbol=symbol,
                data_type="index_price",
                interval_code=interval,
                last_event_ts_ms=last_ts,
                last_event_dt_utc=_ts_ms_to_utc(last_ts),
                status="ok",
                last_success_at_utc=datetime.now(timezone.utc),
            )
        )

        next_start = last_ts + 1
        if next_start <= current_start:
            logger.warning(
                "index_price %s: pagination stall at ts=%d, breaking.", symbol, last_ts,
            )
            break
        current_start = next_start

    logger.info(
        "index_price %s: synced %d rows in [%d, %d].",
        symbol, total_upserted, start_ts_ms, end_ts_ms,
    )
    return total_upserted


async def _sync_open_interest(
    gateway: Any,
    session_factory: Any,
    checkpoint_repo: CheckpointRepository,
    venue: str,
    symbol: str,
    start_ts_ms: int,
    end_ts_ms: int,
    logger: Any,
) -> int:
    """Paginated sync for open_interest. Returns total rows upserted.

    Range availability is resolved by the caller. Auto mode may pass a clamped
    latest-1-month overlap; direct/unit-test callers keep full control of the
    range they pass in.
    """

    repo = OpenInterestRepository()
    total_upserted = 0
    current_start = start_ts_ms
    period = "5m"

    while current_start <= end_ts_ms:
        raw_batch = await gateway.fetch_open_interest_hist(
            symbol=symbol,
            period=period,
            start_ts_ms=current_start,
            end_ts_ms=end_ts_ms,
            limit=500,
        )
        if not raw_batch:
            logger.info(
                "open_interest %s: empty batch at start=%d, stopping pagination.",
                symbol, current_start,
            )
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

        last_ts = _last_ts_from_open_interest(normalized)
        if last_ts is None:
            break

        # Update checkpoint
        checkpoint_repo.upsert(
            IngestCheckpoint(
                venue=venue,
                market_type="usdt_perpetual",
                symbol=symbol,
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
            logger.warning(
                "open_interest %s: pagination stall at ts=%d, breaking.", symbol, last_ts,
            )
            break
        current_start = next_start

    logger.info(
        "open_interest %s: synced %d rows in [%d, %d].",
        symbol, total_upserted, start_ts_ms, end_ts_ms,
    )
    return total_upserted


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


async def run_sync(
    gateway: Any,
    session_factory: Any,
    checkpoint_repo: CheckpointRepository,
    venue: str,
    symbols: list[str],
    data_types: list[str],
    start_ts_ms: int,
    end_ts_ms: int,
    logger: Any,
    *,
    data_type_range_overrides: dict[str, tuple[int, int]] | None = None,
) -> dict[str, int]:
    """Run the full sync across all symbol × data_type combinations.

    Returns a dict mapping ``"symbol/data_type"`` to rows-upserted count.

    *data_type_range_overrides* maps a data type name to
    ``(clamped_start_ts_ms, clamped_end_ts_ms)``.  When present the sync
    uses the clamped range instead of the original *start_ts_ms* /
    *end_ts_ms* for that data type.
    """
    results: dict[str, int] = {}
    range_overrides = data_type_range_overrides or {}

    for symbol in symbols:
        for data_type in data_types:
            key = f"{symbol}/{data_type}"

            if data_type not in _FETCHABLE_DATA_TYPES:
                logger.warning("Unknown data_type %s; skipping.", data_type)
                results[key] = 0
                continue

            # Determine the effective range for this data type.
            # Use range override (clamped window) if one was computed.
            if data_type in range_overrides:
                dt_start, dt_end = range_overrides[data_type]
            else:
                dt_start, dt_end = start_ts_ms, end_ts_ms

            # Check checkpoint for resume point
            interval_code = None
            if data_type in ("mark_price", "index_price"):
                interval_code = "1h"
            elif data_type == "open_interest":
                interval_code = "5m"

            cp = checkpoint_repo.get(
                venue=venue,
                market_type="usdt_perpetual",
                symbol=symbol,
                data_type=data_type,
                interval_code=interval_code,
            )
            effective_start = dt_start
            if cp is not None and cp.last_event_ts_ms is not None:
                resume_ts = cp.last_event_ts_ms + 1
                if resume_ts > effective_start:
                    logger.info(
                        "%s: resuming from checkpoint ts=%d (was %d).",
                        key, resume_ts, effective_start,
                    )
                    effective_start = resume_ts

            if effective_start > dt_end:
                logger.info(
                    "%s: effective start %d > end %d; nothing to sync.",
                    key, effective_start, dt_end,
                )
                results[key] = 0
                continue

            try:
                if data_type == "funding_rate":
                    count = await _sync_funding_rate(
                        gateway, session_factory, checkpoint_repo,
                        venue, symbol, effective_start, dt_end, logger,
                    )
                elif data_type == "mark_price":
                    count = await _sync_mark_price(
                        gateway, session_factory, checkpoint_repo,
                        venue, symbol, effective_start, dt_end, logger,
                    )
                elif data_type == "index_price":
                    count = await _sync_index_price(
                        gateway, session_factory, checkpoint_repo,
                        venue, symbol, effective_start, dt_end, logger,
                    )
                elif data_type == "open_interest":
                    count = await _sync_open_interest(
                        gateway, session_factory, checkpoint_repo,
                        venue, symbol, effective_start, dt_end, logger,
                    )
                else:
                    count = 0
                results[key] = count
            except Exception as exc:
                logger.error(
                    "%s: sync failed: %s", key, exc, exc_info=True,
                )
                # Record error in checkpoint
                checkpoint_repo.upsert(
                    IngestCheckpoint(
                        venue=venue,
                        market_type="usdt_perpetual",
                        symbol=symbol,
                        data_type=data_type,
                        interval_code=interval_code,
                        status="error",
                        last_error_message=str(exc)[:1024],
                    )
                )
                results[key] = -1

    return results


# ---------------------------------------------------------------------------
# Main entrypoint
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()

    try:
        args = parser.parse_args(argv)
        configure_logging(args.log_level)
        logger = get_logger(APP_NAME)

        # Validate required timestamp args
        if args.start_ts_ms is None:
            raise CliArgumentError("--start-ts-ms is required")
        if args.end_ts_ms is None:
            raise CliArgumentError("--end-ts-ms is required")
        if args.start_ts_ms >= args.end_ts_ms:
            raise CliArgumentError(
                f"--start-ts-ms ({args.start_ts_ms}) must be < --end-ts-ms ({args.end_ts_ms})"
            )

        # Load config
        try:
            config = load_config(
                config_path=args.config,
                env_name=args.env,
            )
        except Exception as exc:
            raise CliConfigError(f"Failed to load config: {exc}") from exc

        # Build engine + session_factory
        try:
            engine = create_mysql_engine(config.mysql)
            session_factory = create_session_factory(engine)
        except Exception as exc:
            raise ExternalDependencyError(
                f"Failed to create DB engine: {exc}"
            ) from exc

        # Build gateway
        from YM_data_collection.adapters.binance_gateway import BinanceGateway

        gateway = BinanceGateway(config.binance)

        # Repos
        checkpoint_repo = CheckpointRepository(session_factory)

        data_types, oi_range_overrides = _resolve_requested_data_types(
            args.data_types,
            start_ts_ms=args.start_ts_ms,
            end_ts_ms=args.end_ts_ms,
            logger=logger,
        )
        symbols = args.symbols or config.binance.symbols

        # Run async sync
        try:
            results = asyncio.run(
                run_sync(
                    gateway=gateway,
                    session_factory=session_factory,
                    checkpoint_repo=checkpoint_repo,
                    venue=args.venue,
                    symbols=symbols,
                    data_types=data_types,
                    start_ts_ms=args.start_ts_ms,
                    end_ts_ms=args.end_ts_ms,
                    logger=logger,
                    data_type_range_overrides=oi_range_overrides if oi_range_overrides else None,
                )
            )
        finally:
            # Clean up gateway's httpx client
            try:
                asyncio.run(gateway.close())
            except Exception:
                pass

        # Summarize
        total = sum(v for v in results.values() if v > 0)
        errors = sum(1 for v in results.values() if v < 0)
        logger.info(
            "Sync complete: %d rows upserted, %d errors. Details: %s",
            total, errors, results,
        )

        if errors > 0:
            return emit_final_status(
                APP_NAME, ExitCode.GENERAL_FAILURE,
                f"Sync finished with {errors} error(s): {results}",
            )

        return emit_final_status(
            APP_NAME, ExitCode.SUCCESS,
            f"Sync complete: {total} rows upserted",
        )

    except CliArgumentError as exc:
        return emit_final_status(APP_NAME, exc.exit_code, str(exc))
    except CliConfigError as exc:
        return emit_final_status(APP_NAME, exc.exit_code, str(exc))
    except ExternalDependencyError as exc:
        return emit_final_status(APP_NAME, exc.exit_code, str(exc))
    except Exception as exc:
        logger = get_logger(APP_NAME)
        logger.error("Unexpected error: %s", exc, exc_info=True)
        return emit_final_status(
            APP_NAME, ExitCode.GENERAL_FAILURE, str(exc),
        )


if __name__ == "__main__":
    sys.exit(main())
