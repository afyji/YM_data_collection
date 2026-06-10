"""Logging setup helpers."""

from __future__ import annotations

import logging
from typing import Final

from YM_data_collection.utils.constants import DEFAULT_LOG_LEVEL

_LOG_FORMAT: Final[str] = (
    "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)


def configure_logging(level: str = DEFAULT_LOG_LEVEL) -> None:
    """Configure root logging once for CLI scripts."""

    root_logger = logging.getLogger()
    if root_logger.handlers:
        root_logger.setLevel(level.upper())
        return

    logging.basicConfig(
        level=level.upper(),
        format=_LOG_FORMAT,
    )


def get_logger(name: str) -> logging.Logger:
    """Return a module logger."""

    return logging.getLogger(name)
