"""Tests for shared CLI argument helpers."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone

import pytest

from YM_data_collection.apps._cli_common import parse_ts_ms_argument


def test_parse_ts_ms_argument_accepts_millisecond_integer() -> None:
    assert parse_ts_ms_argument("1704067200000") == 1704067200000


def test_parse_ts_ms_argument_accepts_date_string_for_start_boundary() -> None:
    expected = int(datetime(2020, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
    assert parse_ts_ms_argument("2020-1-1", boundary="start") == expected


def test_parse_ts_ms_argument_uses_end_of_day_for_end_boundary() -> None:
    expected = int(datetime(2020, 1, 1, 23, 59, 59, 999000, tzinfo=timezone.utc).timestamp() * 1000)
    assert parse_ts_ms_argument("2020-1-1", boundary="end") == expected


def test_parse_ts_ms_argument_accepts_datetime_string() -> None:
    expected = int(datetime(2020, 1, 1, 12, 34, 56, tzinfo=timezone.utc).timestamp() * 1000)
    assert parse_ts_ms_argument("2020-1-1 12:34:56", boundary="start") == expected


def test_parse_ts_ms_argument_accepts_timezone_suffix() -> None:
    expected = int(datetime(2020, 1, 1, 12, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
    assert parse_ts_ms_argument("2020-1-1 20:00:00+08:00", boundary="start") == expected


def test_parse_ts_ms_argument_rejects_invalid_input() -> None:
    with pytest.raises(argparse.ArgumentTypeError):
        parse_ts_ms_argument("2020-13-40", boundary="start")
