"""CLI help coverage for enum-like arguments."""

from __future__ import annotations

import pytest

from YM_data_collection.apps._cli_common import add_common_arguments
from YM_data_collection.apps.run_cache_consistency_check import (
    build_parser as build_cache_consistency_parser,
)
from YM_data_collection.apps.run_export_dataset import (
    build_parser as build_export_dataset_parser,
)
from YM_data_collection.apps.run_historical_derivatives_sync import (
    build_parser as build_historical_derivatives_parser,
)
from YM_data_collection.apps.run_recovery_sync import (
    build_parser as build_recovery_sync_parser,
)
from YM_data_collection.apps.run_realtime_ingest import (
    build_parser as build_realtime_ingest_parser,
)
from YM_data_collection.apps.run_resync_range import (
    build_parser as build_resync_range_parser,
)


def _normalize_help_text(help_text: str) -> str:
    return " ".join(help_text.split())


def test_common_env_help_lists_allowed_values() -> None:
    import argparse

    parser = argparse.ArgumentParser()
    add_common_arguments(parser, include_env=True)

    help_text = _normalize_help_text(parser.format_help())

    assert "Allowed values: dev, prod." in help_text


def test_export_dataset_help_lists_enum_values() -> None:
    help_text = _normalize_help_text(build_export_dataset_parser().format_help())

    assert "Allowed values: kline, mark_price, index_price, open_interest, funding_rate." in help_text
    assert "Allowed values: default." in help_text


def test_resync_range_help_lists_enum_values() -> None:
    help_text = _normalize_help_text(build_resync_range_parser().format_help())

    assert "Allowed values: kline, funding_rate, mark_price, open_interest." in help_text
    assert "Allowed values: 1m, 5m, 15m, 1h, 4h, 12h, 1d." in help_text
    assert "Use this for local gap repair, not full historical backfill." in help_text
    assert "mark_price is fixed to 1h" in help_text
    assert "open_interest is fixed to 5m" in help_text
    assert "latest 1 month" in help_text


def test_recovery_sync_rejects_index_price() -> None:
    parser = build_recovery_sync_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(["--data-types", "index_price"])


def test_cache_consistency_help_lists_enum_values() -> None:
    help_text = _normalize_help_text(build_cache_consistency_parser().format_help())

    assert (
        "Allowed values: kline, mark_price, index_price, open_interest, funding_rate, depth_snapshot."
        in help_text
    )


def test_historical_derivatives_help_explains_supported_paths() -> None:
    help_text = _normalize_help_text(build_historical_derivatives_parser().format_help())

    assert "Supports funding_rate (no interval), mark_price and index_price (fixed 1h endpoints), and open_interest (fixed 5m endpoint)." in help_text
    assert "index_price uses Binance indexPriceKlines." in help_text
    assert "latest 1 month" in help_text
    assert "Use 'auto' or omit this flag for best-effort mode." in help_text


def test_realtime_ingest_help_explains_shared_mark_price_stream() -> None:
    help_text = _normalize_help_text(build_realtime_ingest_parser().format_help())

    assert "mark_price, index_price, and funding_rate come from perp @markPrice@1s" in help_text
    assert "open_interest is listed as a topic but is not currently backed by an active subscription" in help_text
