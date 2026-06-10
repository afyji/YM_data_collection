"""Common CLI helpers for script entrypoints."""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from functools import partial
import json
import re
from dataclasses import dataclass
from typing import Iterable, Sequence

try:
    from dotenv import load_dotenv
    from pathlib import Path
    _env_path = Path(__file__).resolve().parents[1] / '.env'
    if _env_path.exists():
        load_dotenv(_env_path)
except ImportError:
    pass

from YM_data_collection.utils.constants import DEFAULT_CONFIG_PATH, DEFAULT_LOG_LEVEL
from YM_data_collection.utils.exit_codes import ExitCode, describe_exit_code
from YM_data_collection.utils.logging_utils import configure_logging, get_logger

SUPPORTED_ENVS = ("dev", "prod")
SUPPORTED_VENUES = ("binance",)
COMMON_MARKET_TYPES = ("spot", "perp")


class CliError(Exception):
    """Base class for mapped CLI failures."""

    exit_code = ExitCode.GENERAL_FAILURE


class CliArgumentError(CliError):
    """Raised for argument validation failures."""

    exit_code = ExitCode.ARGUMENT_ERROR


class CliConfigError(CliError):
    """Raised for configuration failures."""

    exit_code = ExitCode.CONFIG_ERROR


class ExternalDependencyError(CliError):
    """Raised for external dependency failures."""

    exit_code = ExitCode.DEPENDENCY_ERROR


@dataclass(frozen=True)
class CliStatus:
    """Final status payload printed by scripts."""

    app: str
    code: int
    status: str
    message: str


def emit_final_status(app_name: str, exit_code: ExitCode, message: str) -> int:
    """Print the stable final status payload."""

    payload = CliStatus(
        app=app_name,
        code=int(exit_code),
        status=describe_exit_code(exit_code),
        message=message,
    )
    print(json.dumps(payload.__dict__, ensure_ascii=True, sort_keys=True))
    return int(exit_code)


def add_common_arguments(
    parser: argparse.ArgumentParser,
    *,
    include_config: bool = True,
    include_env: bool = False,
    include_venue: bool = False,
    include_market_type: bool = False,
    include_symbols: bool = False,
    venue_choices: Sequence[str] | None = None,
    market_type_choices: Sequence[str] | None = None,
) -> None:
    """Attach shared CLI options."""

    parser.add_argument(
        "--log-level",
        default=DEFAULT_LOG_LEVEL,
        help="Log level for the current command.",
    )
    if include_config:
        parser.add_argument(
            "--config",
            default=DEFAULT_CONFIG_PATH,
            help="Project config path.",
        )
    if include_env:
        parser.add_argument(
            "--env",
            default=None,
            choices=list(SUPPORTED_ENVS),
            help=render_choice_help(
                "Target runtime environment. Defaults to app.env in the base config file.",
                SUPPORTED_ENVS,
            ),
        )
    if include_venue:
        allowed_venues = list(venue_choices or SUPPORTED_VENUES)
        parser.add_argument(
            "--venue",
            default=allowed_venues[0],
            choices=allowed_venues,
            help=render_choice_help("Venue identifier.", allowed_venues),
        )
    if include_market_type:
        allowed_market_types = list(market_type_choices or COMMON_MARKET_TYPES)
        parser.add_argument(
            "--market-type",
            default="spot",
            choices=allowed_market_types,
            help=render_choice_help("Market type identifier.", allowed_market_types),
        )
    if include_symbols:
        parser.add_argument(
            "--symbols",
            nargs="+",
            default=None,
            help="Symbol list (default: from config).",
        )


def add_list_argument(
    parser: argparse.ArgumentParser,
    name: str,
    *,
    default: Sequence[str] | None = None,
    help_text: str,
    choices: Sequence[str] | None = None,
) -> None:
    """Attach a space-separated multi-value argument."""

    allowed_choices = list(choices or [])

    parser.add_argument(
        name,
        nargs="+",
        default=list(default or []),
        choices=allowed_choices or None,
        help=render_choice_help(help_text, allowed_choices),
    )


def add_flag_argument(parser: argparse.ArgumentParser, name: str, help_text: str) -> None:
    """Attach a boolean flag argument."""

    parser.add_argument(name, action="store_true", help=help_text)


def render_choice_help(help_text: str, choices: Sequence[str] | None = None) -> str:
    """Append allowed values to help text when the argument is enum-like."""

    allowed_choices = list(choices or [])
    if not allowed_choices:
        return help_text
    return f"{help_text} Allowed values: {', '.join(allowed_choices)}."


_INTEGER_TS_PATTERN = re.compile(r"^-?\d+$")
_DATETIME_TS_PATTERN = re.compile(
    r"^(?P<year>\d{4})-(?P<month>\d{1,2})-(?P<day>\d{1,2})"
    r"(?:(?:[ T])(?P<hour>\d{1,2})"
    r"(?::(?P<minute>\d{1,2}))?"
    r"(?::(?P<second>\d{1,2})(?:\.(?P<fraction>\d{1,6}))?)?)?"
    r"(?P<tz>Z|[+-]\d{2}:\d{2})?$"
)


def _parse_timezone_offset(raw_tz: str | None) -> timezone:
    """Parse an optional timezone suffix, defaulting to UTC."""

    if raw_tz in (None, "", "Z"):
        return timezone.utc
    sign = 1 if raw_tz[0] == "+" else -1
    hours, minutes = raw_tz[1:].split(":")
    offset = timedelta(hours=int(hours), minutes=int(minutes))
    return timezone(sign * offset)


def parse_ts_ms_argument(raw_value: str, *, boundary: str = "start") -> int:
    """Parse millisecond timestamps or flexible UTC date strings."""

    value = raw_value.strip()
    if _INTEGER_TS_PATTERN.fullmatch(value):
        return int(value)

    match = _DATETIME_TS_PATTERN.fullmatch(value)
    if match is None:
        raise argparse.ArgumentTypeError(
            "Expected a millisecond timestamp or date string like "
            "'2020-1-1' or '2020-1-1 12:30:00'."
        )

    parts = match.groupdict()
    has_time = parts["hour"] is not None
    if has_time:
        hour = int(parts["hour"] or 0)
        minute = int(parts["minute"] or 0)
        second = int(parts["second"] or 0)
        fraction = parts["fraction"] or ""
        microsecond = int(fraction.ljust(6, "0")) if fraction else 0
    elif boundary == "end":
        hour = 23
        minute = 59
        second = 59
        microsecond = 999_000
    else:
        hour = 0
        minute = 0
        second = 0
        microsecond = 0

    try:
        dt = datetime(
            year=int(parts["year"]),
            month=int(parts["month"]),
            day=int(parts["day"]),
            hour=hour,
            minute=minute,
            second=second,
            microsecond=microsecond,
            tzinfo=_parse_timezone_offset(parts["tz"]),
        )
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Invalid datetime value '{raw_value}': {exc}") from exc

    return int(dt.astimezone(timezone.utc).timestamp() * 1000)


def add_ts_ms_argument(
    parser: argparse.ArgumentParser,
    name: str,
    *,
    boundary: str,
    required: bool = False,
    help_prefix: str,
) -> None:
    """Attach a timestamp argument that accepts ms integers or date strings."""

    date_hint = "Date-only values use 00:00:00.000 UTC." if boundary == "start" else (
        "Date-only values use 23:59:59.999 UTC."
    )
    parser.add_argument(
        name,
        type=partial(parse_ts_ms_argument, boundary=boundary),
        required=required,
        help=(
            f"{help_prefix} Accepts millisecond timestamps or date strings like "
            f"'2020-1-1' or '2020-1-1 12:30:00'. {date_hint}"
        ),
    )


def validate_non_empty(values: Iterable[str], *, field_name: str) -> None:
    """Ensure a multi-value argument is not empty."""

    if not list(values):
        raise CliArgumentError(f"{field_name} must not be empty")


def run_placeholder(app_name: str, parser: argparse.ArgumentParser, argv: Sequence[str] | None = None) -> int:
    """Parse arguments and emit a standard placeholder status."""

    try:
        args = parser.parse_args(argv)
        configure_logging(args.log_level)
        logger = get_logger(app_name)
        logger.info("E01 skeleton entrypoint loaded")
        return emit_final_status(
            app_name,
            ExitCode.SUCCESS,
            "E01 skeleton ready; implementation pending",
        )
    except CliError as exc:
        return emit_final_status(app_name, exc.exit_code, str(exc))
