"""Basic tests for logging helpers."""

import logging

from YM_data_collection.utils.logging_utils import configure_logging, get_logger


def test_configure_logging_and_get_logger() -> None:
    configure_logging("INFO")
    logger = get_logger("test_logger")
    assert isinstance(logger, logging.Logger)
