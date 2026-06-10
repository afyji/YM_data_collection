"""Run data quality checks."""

from __future__ import annotations

import argparse
import sys

from pathlib import Path

if __package__ is None or __package__ == "":
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

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
from YM_data_collection.persistence.mysql import create_mysql_engine, create_session_factory
from YM_data_collection.persistence.repositories.quality_repo import (
    QualityIssueRepository,
)
from YM_data_collection.quality.checkers import INTERVAL_MS, QualityChecker
from YM_data_collection.utils.exit_codes import ExitCode
from YM_data_collection.utils.logging_utils import configure_logging, get_logger

APP_NAME = "run_quality_check"

# Default intervals per kline data-type
_KLINE_INTERVALS = ["1m", "5m", "15m", "1h", "4h", "1d"]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run quality checks for standardized data.")
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
        default=["kline", "depth_snapshot"],
        choices=["kline", "depth_snapshot"],
        help_text="Data types to check.",
    )
    add_ts_ms_argument(parser, "--start-ts-ms", boundary="start", help_prefix="Inclusive start time.")
    add_ts_ms_argument(parser, "--end-ts-ms", boundary="end", help_prefix="Inclusive end time.")
    parser.add_argument(
        "--intervals",
        nargs="+",
        default=_KLINE_INTERVALS,
        choices=_KLINE_INTERVALS,
        help=f"Interval codes to check for kline data. Allowed values: {', '.join(_KLINE_INTERVALS)}.",
    )
    parser.add_argument(
        "--max-depth-age",
        type=int,
        default=300,
        help="Max depth snapshot age in seconds (default 300).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    try:
        args = parser.parse_args(argv)
        configure_logging(args.log_level)
        logger = get_logger(APP_NAME)

        # -- Validate required args ------------------------------------------------
        if not args.start_ts_ms or not args.end_ts_ms:
            raise CliArgumentError(
                "--start-ts-ms and --end-ts-ms are required"
            )
        if args.start_ts_ms >= args.end_ts_ms:
            raise CliArgumentError(
                "--start-ts-ms must be less than --end-ts-ms"
            )

        # -- Load config -----------------------------------------------------------
        try:
            config = load_config(config_path=args.config, env_name=args.env)
        except Exception as exc:
            raise CliConfigError(f"Failed to load config: {exc}") from exc

        # -- Create engine & repos -------------------------------------------------
        try:
            engine = create_mysql_engine(config.mysql)
        except Exception as exc:
            raise ExternalDependencyError(
                f"Failed to create MySQL engine: {exc}"
            ) from exc

        session_factory = create_session_factory(engine)
        quality_repo = QualityIssueRepository(session_factory)
        checker = QualityChecker(session_factory, quality_repo)

        venue = args.venue
        market_type = args.market_type
        symbols = args.symbols or config.binance.symbols
        data_types = args.data_types
        start_ts_ms = args.start_ts_ms
        end_ts_ms = args.end_ts_ms

        results = []
        total_issues = 0
        checks_run = 0

        # -- Run checks per symbol x data_type ------------------------------------
        for symbol in symbols:
            for data_type in data_types:
                if data_type == "kline":
                    for interval_code in args.intervals:
                        if interval_code not in INTERVAL_MS:
                            logger.warning(
                                "Skipping unknown interval: %s", interval_code
                            )
                            continue

                        # Gap check
                        gap_result = checker.check_kline_gaps(
                            venue, market_type, symbol,
                            interval_code, start_ts_ms, end_ts_ms,
                        )
                        results.append(gap_result)
                        total_issues += len(gap_result.issues)
                        checks_run += 1
                        status = "PASS" if gap_result.passed else "FAIL"
                        logger.info(
                            "[%s] gap %s %s %s [%s-%s]: %s",
                            status, symbol, interval_code, venue,
                            start_ts_ms, end_ts_ms, gap_result.summary,
                        )

                        # Duplicate check
                        dup_result = checker.check_kline_duplicates(
                            venue, market_type, symbol,
                            interval_code, start_ts_ms, end_ts_ms,
                        )
                        results.append(dup_result)
                        total_issues += len(dup_result.issues)
                        checks_run += 1
                        status = "PASS" if dup_result.passed else "FAIL"
                        logger.info(
                            "[%s] duplicate %s %s %s [%s-%s]: %s",
                            status, symbol, interval_code, venue,
                            start_ts_ms, end_ts_ms, dup_result.summary,
                        )

                        # Boundary check
                        bnd_result = checker.check_kline_boundary(
                            venue, market_type, symbol,
                            interval_code, start_ts_ms, end_ts_ms,
                        )
                        results.append(bnd_result)
                        total_issues += len(bnd_result.issues)
                        checks_run += 1
                        status = "PASS" if bnd_result.passed else "FAIL"
                        logger.info(
                            "[%s] boundary %s %s %s [%s-%s]: %s",
                            status, symbol, interval_code, venue,
                            start_ts_ms, end_ts_ms, bnd_result.summary,
                        )

                elif data_type == "depth_snapshot":
                    freshness_result = checker.check_depth_freshness(
                        venue, market_type, symbol,
                        max_age_seconds=args.max_depth_age,
                    )
                    results.append(freshness_result)
                    total_issues += len(freshness_result.issues)
                    checks_run += 1
                    status = "PASS" if freshness_result.passed else "FAIL"
                    logger.info(
                        "[%s] freshness %s %s %s: %s",
                        status, symbol, venue, market_type,
                        freshness_result.summary,
                    )

                else:
                    logger.warning("Unsupported data_type for quality check: %s", data_type)

        # -- Summary ---------------------------------------------------------------
        passed = sum(1 for r in results if r.passed)
        failed = sum(1 for r in results if not r.passed)
        summary = (
            f"Quality checks complete: {checks_run} checks, "
            f"{passed} passed, {failed} failed, "
            f"{total_issues} issue(s) recorded"
        )
        logger.info(summary)
        print(summary)

        if failed > 0:
            return emit_final_status(APP_NAME, ExitCode.GENERAL_FAILURE, summary)
        return emit_final_status(APP_NAME, ExitCode.SUCCESS, summary)

    except CliArgumentError as exc:
        return emit_final_status(APP_NAME, exc.exit_code, str(exc))
    except CliConfigError as exc:
        return emit_final_status(APP_NAME, exc.exit_code, str(exc))
    except ExternalDependencyError as exc:
        return emit_final_status(APP_NAME, exc.exit_code, str(exc))


if __name__ == "__main__":
    sys.exit(main())
