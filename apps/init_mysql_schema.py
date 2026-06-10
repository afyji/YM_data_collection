"""Initialize or verify MySQL schema migrations."""

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
    add_flag_argument,
    emit_final_status,
)
from YM_data_collection.config.loader import load_config
from YM_data_collection.persistence.migrations import (
    MigrationChecksumError,
    MigrationDefinitionError,
    MigrationExecutionError,
    MigrationStateError,
    run_migrations,
    summarize_run,
)
from YM_data_collection.persistence.mysql import create_mysql_engine
from YM_data_collection.utils.constants import DEFAULT_MIGRATIONS_DIR
from YM_data_collection.utils.exit_codes import ExitCode
from YM_data_collection.utils.logging_utils import configure_logging, get_logger


APP_NAME = "init_mysql_schema"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Initialize or verify MySQL schema.")
    add_common_arguments(parser, include_config=True, include_env=True)
    add_flag_argument(parser, "--apply", "Apply unapplied SQL migrations.")
    add_flag_argument(parser, "--check-only", "Only verify schema state without applying changes.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    try:
        args = parser.parse_args(argv)
        if args.apply and args.check_only:
            raise CliArgumentError("--apply and --check-only cannot be used together")

        configure_logging(args.log_level)
        logger = get_logger(APP_NAME)

        try:
            config = load_config(config_path=args.config, env_name=args.env)
        except Exception as exc:
            raise CliConfigError(f"Failed to load config: {exc}") from exc

        try:
            engine = create_mysql_engine(config.mysql)
        except Exception as exc:
            raise ExternalDependencyError(f"Failed to create MySQL engine: {exc}") from exc

        summary = run_migrations(
            engine,
            Path(DEFAULT_MIGRATIONS_DIR),
            apply=bool(args.apply),
        )
        message = summarize_run(summary)
        logger.info(message)
        if summary.schema_up_to_date:
            return emit_final_status(APP_NAME, ExitCode.SUCCESS, message)
        return emit_final_status(APP_NAME, ExitCode.GENERAL_FAILURE, message)
    except CliArgumentError as exc:
        return emit_final_status(APP_NAME, exc.exit_code, str(exc))
    except CliConfigError as exc:
        return emit_final_status(APP_NAME, exc.exit_code, str(exc))
    except ExternalDependencyError as exc:
        return emit_final_status(APP_NAME, exc.exit_code, str(exc))
    except (MigrationDefinitionError, MigrationChecksumError, MigrationStateError) as exc:
        return emit_final_status(APP_NAME, ExitCode.DATA_VALIDATION_ERROR, str(exc))
    except MigrationExecutionError as exc:
        return emit_final_status(APP_NAME, ExitCode.GENERAL_FAILURE, str(exc))


if __name__ == "__main__":
    sys.exit(main())
