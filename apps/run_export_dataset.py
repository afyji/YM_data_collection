"""Export standardized data into Parquet datasets."""

from __future__ import annotations

import argparse
import sys

from pathlib import Path

if __package__ is None or __package__ == "":
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from YM_data_collection.apps._cli_common import (
    CliArgumentError,
    add_common_arguments,
    add_flag_argument,
    add_ts_ms_argument,
    emit_final_status,
    render_choice_help,
)
from YM_data_collection.config.loader import load_config
from YM_data_collection.export.parquet_writer import DatasetExporter
from YM_data_collection.persistence.mysql import (
    create_mysql_engine,
    create_session_factory,
)
from YM_data_collection.persistence.repositories.manifest_repo import (
    ManifestRepository,
)
from YM_data_collection.utils.exit_codes import ExitCode
from YM_data_collection.utils.logging_utils import configure_logging, get_logger

APP_NAME = "run_export_dataset"

KLINE_DATA_TYPES = {"kline"}
DERIVATIVE_DATA_TYPES = {"mark_price", "index_price", "open_interest", "funding_rate"}
ALL_DATA_TYPES = KLINE_DATA_TYPES | DERIVATIVE_DATA_TYPES
DATA_TYPE_CHOICES = ["kline", "mark_price", "index_price", "open_interest", "funding_rate"]
SOURCE_INTERVAL_HINTS = ["1m", "5m", "15m", "1h", "4h", "8h", "12h", "1d"]
TARGET_INTERVAL_CHOICES = ["1m", "5m", "15m", "1h", "4h", "8h", "1d"]
AGGREGATION_MODE_CHOICES = ["default"]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Export standardized data into datasets.")
    add_common_arguments(
        parser,
        include_config=True,
        include_env=True,
        include_venue=True,
        include_market_type=True,
    )
    parser.add_argument("--dataset-name", required=True, help="Dataset identifier.")
    parser.add_argument("--symbol", required=True, help="Target symbol.")
    parser.add_argument(
        "--data-type",
        required=True,
        choices=DATA_TYPE_CHOICES,
        help=render_choice_help("Target data type.", DATA_TYPE_CHOICES),
    )
    parser.add_argument(
        "--source-interval",
        required=False,
        help=f"Source interval (required for kline). Common values: {', '.join(SOURCE_INTERVAL_HINTS)}.",
    )
    parser.add_argument(
        "--target-interval",
        required=False,
        choices=TARGET_INTERVAL_CHOICES,
        help=render_choice_help("Target interval for resampling.", TARGET_INTERVAL_CHOICES),
    )
    add_ts_ms_argument(
        parser,
        "--start-ts-ms",
        boundary="start",
        required=True,
        help_prefix="Inclusive start time.",
    )
    add_ts_ms_argument(
        parser,
        "--end-ts-ms",
        boundary="end",
        required=True,
        help_prefix="Inclusive end time.",
    )
    parser.add_argument("--output-dir", default="artifacts/datasets", help="Export directory.")
    parser.add_argument("--version", default="v1", help="Dataset version.")
    add_flag_argument(parser, "--resample-enabled", "Enable resampling on export.")
    parser.add_argument("--offset-minutes", type=int, default=0, help="Offset minutes for export boundaries.")
    parser.add_argument(
        "--aggregation-mode",
        default="default",
        choices=AGGREGATION_MODE_CHOICES,
        help=render_choice_help("Aggregation mode.", AGGREGATION_MODE_CHOICES),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    try:
        parser = build_parser()
        args = parser.parse_args(argv)
        configure_logging(args.log_level)
        logger = get_logger(APP_NAME)

        data_type = args.data_type
        if data_type not in ALL_DATA_TYPES:
            raise CliArgumentError(
                f"Unsupported --data-type '{data_type}'. "
                f"Expected one of: {sorted(ALL_DATA_TYPES)}"
            )

        if data_type in KLINE_DATA_TYPES and not args.source_interval:
            raise CliArgumentError(
                "--source-interval is required when --data-type is 'kline'"
            )

        # 1. Load config
        config = load_config(config_path=args.config, env_name=args.env)

        # 2. Create engine + session_factory
        engine = create_mysql_engine(config.mysql)
        session_factory = create_session_factory(engine)

        # 3. Create ManifestRepo and DatasetExporter
        manifest_repo = ManifestRepository(session_factory)
        exporter = DatasetExporter(session_factory, manifest_repo)

        # 4. Call the right export method
        common_kwargs = dict(
            venue=args.venue,
            market_type=args.market_type,
            symbol=args.symbol,
            start_ts_ms=args.start_ts_ms,
            end_ts_ms=args.end_ts_ms,
            output_dir=args.output_dir,
            dataset_name=args.dataset_name,
            version=args.version,
        )

        if data_type in KLINE_DATA_TYPES:
            result = exporter.export_klines(
                **common_kwargs,
                interval_code=args.source_interval,
            )
        else:
            result = exporter.export_derivatives(
                **common_kwargs,
                data_type=data_type,
            )

        # 5. Print export result summary
        logger.info(
            "Export complete: file=%s  rows=%d  size=%d  hash=%s",
            result.file_path,
            result.row_count,
            result.file_size_bytes,
            result.content_hash,
        )
        print(
            f"Exported {result.row_count} rows → {result.file_path}  "
            f"({result.file_size_bytes} bytes, sha256={result.content_hash[:16]}…)"
        )

        return emit_final_status(
            APP_NAME,
            ExitCode.SUCCESS,
            f"Exported {result.row_count} rows to {result.file_path}",
        )

    except CliArgumentError as exc:
        return emit_final_status(APP_NAME, exc.exit_code, str(exc))
    except Exception as exc:
        logger = get_logger(APP_NAME)
        logger.exception("Export failed: %s", exc)
        return emit_final_status(
            APP_NAME, ExitCode.GENERAL_FAILURE, str(exc)
        )


if __name__ == "__main__":
    sys.exit(main())
