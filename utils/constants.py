"""Shared enums and constants."""

from __future__ import annotations

from enum import Enum
from pathlib import Path


class Venue(str, Enum):
    """Supported venues for phase 1."""

    BINANCE = "binance"


class MarketType(str, Enum):
    """Supported market types for phase 1."""

    SPOT = "spot"
    USDT_PERPETUAL = "usdt_perpetual"


class Interval(str, Enum):
    """Common interval values used across the system."""

    ONE_MINUTE = "1m"
    ONE_HOUR = "1h"
    FOUR_HOURS = "4h"
    EIGHT_HOURS = "8h"
    TWELVE_HOURS = "12h"
    ONE_DAY = "1d"


class ResponseCode(str, Enum):
    """Minimal response code placeholders for future API use."""

    SUCCESS = "SUCCESS"
    INVALID_ARGUMENT = "INVALID_ARGUMENT"
    UNAUTHORIZED = "UNAUTHORIZED"
    NOT_FOUND = "NOT_FOUND"
    DEPENDENCY_ERROR = "DEPENDENCY_ERROR"
    INTERNAL_ERROR = "INTERNAL_ERROR"


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG_PATH = str(PROJECT_ROOT / "config" / "base.yaml")
DEFAULT_MIGRATIONS_DIR = PROJECT_ROOT / "sql" / "migrations"
SCHEMA_MIGRATIONS_TABLE = "schema_migrations"
DEFAULT_LOG_LEVEL = "INFO"
