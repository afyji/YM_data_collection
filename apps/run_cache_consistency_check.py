"""Run cache consistency audit."""

from __future__ import annotations

import argparse
import sys

from pathlib import Path

if __package__ is None or __package__ == "":
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from YM_data_collection.apps._cli_common import (
    CliConfigError,
    CliError,
    add_common_arguments,
    add_list_argument,
    emit_final_status,
)
from YM_data_collection.utils.exit_codes import ExitCode
from YM_data_collection.utils.logging_utils import configure_logging, get_logger

APP_NAME = "run_cache_consistency_check"
SUPPORTED_CACHE_CHECK_DATA_TYPES = [
    "kline",
    "mark_price",
    "index_price",
    "open_interest",
    "funding_rate",
    "depth_snapshot",
]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run cache consistency checks.")
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
        default=SUPPORTED_CACHE_CHECK_DATA_TYPES,
        choices=SUPPORTED_CACHE_CHECK_DATA_TYPES,
        help_text="Data types to compare.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    try:
        args = build_parser().parse_args(argv)
        configure_logging(args.log_level)
        logger = get_logger(APP_NAME)

        # Lazy imports to avoid heavy deps on --help
        from YM_data_collection.cache.redis_client import build_redis_client
        from YM_data_collection.config.loader import load_config
        from YM_data_collection.persistence.mysql import create_mysql_engine, create_session_factory
        from YM_data_collection.quality.cache_checker import CacheConsistencyChecker

        # Load config
        try:
            config = load_config(config_path=args.config, env_name=args.env)
        except Exception as exc:
            raise CliConfigError(f"Failed to load config: {exc}") from exc

        # Build dependencies
        engine = create_mysql_engine(config.mysql)
        session_factory = create_session_factory(engine)
        cache_client = build_redis_client(config.cache)

        # Verify connectivity
        if not cache_client.ping():
            logger.warning("Redis ping failed; cache checks may be incomplete")

        # Resolve symbols
        symbols = args.symbols or config.binance.symbols
        data_types = args.data_types

        venue = args.venue
        market_type = args.market_type

        logger.info(
            "Running cache consistency check: venue=%s market_type=%s symbols=%s data_types=%s",
            venue, market_type, symbols, data_types,
        )

        # Run checks
        checker = CacheConsistencyChecker(session_factory, cache_client)
        results = checker.check_all(venue, market_type, symbols, data_types)

        # Print summary table
        _print_summary(results, logger)

        # Determine exit code
        inconsistent = [r for r in results if not r.consistent]
        if inconsistent:
            return emit_final_status(
                APP_NAME,
                ExitCode.AUDIT_FAILURE,
                f"{len(inconsistent)} inconsistency(ies) detected out of {len(results)} checks",
            )

        return emit_final_status(
            APP_NAME,
            ExitCode.SUCCESS,
            f"All {len(results)} checks passed",
        )

    except CliError as exc:
        return emit_final_status(APP_NAME, exc.exit_code, str(exc))


def _print_summary(results: list, logger) -> None:
    """Print a human-readable summary table of consistency results."""
    if not results:
        logger.info("No checks performed")
        return

    header = f"{'Symbol':<12} {'Data Type':<18} {'Cache':<7} {'MySQL':<7} {'OK':<5} Summary"
    logger.info(header)
    logger.info("-" * len(header))

    for r in results:
        cache_flag = "Y" if r.cache_exists else "N"
        mysql_flag = "Y" if r.mysql_exists else "N"
        ok_flag = "Y" if r.consistent else "N"
        logger.info(
            "%-12s %-18s %-7s %-7s %-5s %s",
            r.symbol, r.data_type, cache_flag, mysql_flag, ok_flag, r.summary,
        )

    # Print detailed discrepancies if any
    inconsistent = [r for r in results if not r.consistent and r.discrepancies]
    if inconsistent:
        logger.info("")
        logger.info("Discrepancy details:")
        for r in inconsistent:
            for d in r.discrepancies:
                logger.info("  %s/%s: %s", r.symbol, r.data_type, d)


if __name__ == "__main__":
    sys.exit(main())
