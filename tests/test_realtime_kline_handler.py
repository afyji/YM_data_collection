"""Tests for ingestion.realtime_kline_handler module."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from YM_data_collection.config.models import RealtimePersistenceConfig, WritePolicyConfig
from YM_data_collection.domain.models import NormalizedKline
from YM_data_collection.ingestion.realtime_kline_handler import (
    RealtimeKlineHandler,
    _kline_to_cache_payload,
    _table_name_for_market_type,
    _ws_kline_to_raw_array,
)
from YM_data_collection.validation.kline_validator import ValidationResult


# ---------------------------------------------------------------------------
# Fixtures & helpers
# ---------------------------------------------------------------------------

# 1h-aligned timestamp: 2023-01-01 00:00:00 UTC
_ALIGNED_1H_OPEN_MS = 1672531200000
_ALIGNED_1H_CLOSE_MS = _ALIGNED_1H_OPEN_MS + 3600000 - 1  # 1672534799999


def _make_ws_closed_kline(
    symbol: str = "BTCUSDT",
    interval: str = "1h",
    open_ts_ms: int = _ALIGNED_1H_OPEN_MS,
    close_ts_ms: int = _ALIGNED_1H_CLOSE_MS,
    open_price: str = "50000.00",
    high_price: str = "50500.00",
    low_price: str = "49500.00",
    close_price: str = "50200.00",
    volume: str = "123.456",
    quote_volume: str = "6172800.00",
    trade_count: int = 1000,
    taker_buy_base_volume: str = "61.728",
    taker_buy_quote_volume: str = "3086400.00",
    is_closed: bool = True,
) -> dict:
    """Build a Binance kline WS message dict (closed kline by default)."""
    return {
        "e": "kline",
        "E": open_ts_ms + 2136,
        "s": symbol,
        "k": {
            "t": open_ts_ms,
            "T": close_ts_ms,
            "s": symbol,
            "i": interval,
            "f": 100,
            "L": 200,
            "o": open_price,
            "c": close_price,
            "h": high_price,
            "l": low_price,
            "v": volume,
            "n": trade_count,
            "x": is_closed,
            "q": quote_volume,
            "V": taker_buy_base_volume,
            "Q": taker_buy_quote_volume,
            "B": "0",
        },
    }


def _make_valid_1h_kline() -> NormalizedKline:
    """Create a valid NormalizedKline matching the WS fixture."""
    open_ts = 1672531200000  # aligned 1h boundary
    close_ts = open_ts + 3600000 - 1
    return NormalizedKline(
        venue="binance",
        symbol="BTCUSDT",
        instrument_code="crypto.binance.spot.BTCUSDT",
        interval_code="1h",
        open_ts_ms=open_ts,
        close_ts_ms=close_ts,
        open_dt_utc=datetime.fromtimestamp(open_ts / 1000.0, tz=timezone.utc),
        close_dt_utc=datetime.fromtimestamp(close_ts / 1000.0, tz=timezone.utc),
        open_price=Decimal("50000.00"),
        high_price=Decimal("50500.00"),
        low_price=Decimal("49500.00"),
        close_price=Decimal("50200.00"),
        volume=Decimal("123.456"),
        quote_volume=Decimal("6172800.00"),
        trade_count=1000,
        taker_buy_base_volume=Decimal("61.728"),
        taker_buy_quote_volume=Decimal("3086400.00"),
        source="exchange",
        market_type="spot",
    )


@pytest.fixture
def mock_session_factory() -> MagicMock:
    return MagicMock()


@pytest.fixture
def mock_redis() -> MagicMock:
    redis = MagicMock()
    redis.set_json = MagicMock(return_value=True)
    redis.build_key = MagicMock(return_value="ym:binance:spot:btcusdt:kline:1h")
    return redis


@pytest.fixture
def config() -> RealtimePersistenceConfig:
    return RealtimePersistenceConfig()


@pytest.fixture
def handler(mock_session_factory, mock_redis, config) -> RealtimeKlineHandler:
    return RealtimeKlineHandler(
        session_factory=mock_session_factory,
        redis_client=mock_redis,
        config=config,
        venue="binance",
    )


# ---------------------------------------------------------------------------
# Unit tests: helper functions
# ---------------------------------------------------------------------------

class TestWsKlineToRawArray:
    def test_converts_all_fields(self):
        k = {
            "t": 1672515780000,
            "o": "50000.00",
            "h": "50500.00",
            "l": "49500.00",
            "c": "50200.00",
            "v": "123.456",
            "T": 1672519379999,
            "q": "6172800.00",
            "n": 1000,
            "V": "61.728",
            "Q": "3086400.00",
            "B": "0",
        }
        result = _ws_kline_to_raw_array(k)
        assert result == [
            1672515780000, "50000.00", "50500.00", "49500.00", "50200.00",
            "123.456", 1672519379999, "6172800.00", 1000, "61.728",
            "3086400.00", "0",
        ]

    def test_missing_B_field_defaults_to_zero(self):
        k = {
            "t": 100, "o": "1", "h": "2", "l": "0.5", "c": "1.5",
            "v": "10", "T": 200, "q": "20", "n": 5, "V": "3", "Q": "4",
        }
        result = _ws_kline_to_raw_array(k)
        assert result[11] == "0"


class TestTableNameForMarketType:
    def test_spot(self):
        assert _table_name_for_market_type("spot") == "spot_klines"

    def test_perp(self):
        assert _table_name_for_market_type("perp") == "perp_klines"

    def test_unknown_defaults_to_spot(self):
        assert _table_name_for_market_type("other") == "spot_klines"


class TestKlineToCachePayload:
    def test_converts_decimal_to_string(self):
        kline = _make_valid_1h_kline()
        payload = _kline_to_cache_payload(kline)
        assert isinstance(payload["open_price"], str)
        assert payload["open_price"] == "50000.00"
        assert isinstance(payload["volume"], str)


# ---------------------------------------------------------------------------
# Integration tests: handler.handle_message
# ---------------------------------------------------------------------------

class TestClosedKlinePersistsToMysql:
    """x=True triggers MySQL upsert."""

    @pytest.mark.asyncio
    async def test_closed_kline_persists(self, handler, mock_session_factory):
        msg = _make_ws_closed_kline(is_closed=True)
        with patch.object(
            handler, "_update_checkpoint", MagicMock()
        ) as mock_ckpt:
            await handler.handle_message("spot", msg)
            mock_ckpt.assert_called_once()

        # KlineRepository.upsert_batch should have been called via to_thread,
        # which means the handler should have completed without error.
        # We verify the checkpoint was called, confirming MySQL write succeeded.


class TestOpenKlineUpdatesRedisOnly:
    """x=False skips MySQL, updates Redis."""

    @pytest.mark.asyncio
    async def test_open_kline_no_mysql(self, handler, mock_redis):
        msg = _make_ws_closed_kline(is_closed=False)
        with patch(
            "YM_data_collection.ingestion.realtime_kline_handler.asyncio.to_thread",
        ) as mock_to_thread:
            await handler.handle_message("spot", msg)
            # to_thread should NOT have been called for MySQL upsert
            mock_to_thread.assert_not_called()

        # Redis should still be updated
        mock_redis.set_json.assert_called_once()


class TestNormalizesAndValidates:
    """Valid kline flows through the full pipeline."""

    @pytest.mark.asyncio
    async def test_pipeline_calls_normalize_and_validate(self, handler, mock_redis):
        msg = _make_ws_closed_kline(is_closed=True)
        with patch(
            "YM_data_collection.ingestion.realtime_kline_handler.normalize_binance_kline",
        ) as mock_norm, \
             patch(
                "YM_data_collection.ingestion.realtime_kline_handler.validate_kline",
            ) as mock_val, \
             patch(
                "YM_data_collection.ingestion.realtime_kline_handler.asyncio.to_thread",
                return_value=None,
            ) as mock_to_thread, \
             patch.object(handler, "_update_checkpoint", MagicMock()):

            mock_norm.return_value = _make_valid_1h_kline()
            mock_val.return_value = ValidationResult(is_valid=True)

            await handler.handle_message("spot", msg)

            mock_norm.assert_called_once()
            mock_val.assert_called_once()
            mock_to_thread.assert_called_once()


class TestInvalidKlineSkipped:
    """Validation failure (non-repairable) skips persist."""

    @pytest.mark.asyncio
    async def test_invalid_kline_no_mysql(self, handler, mock_redis):
        msg = _make_ws_closed_kline(is_closed=True)
        with patch(
            "YM_data_collection.ingestion.realtime_kline_handler.validate_kline",
        ) as mock_val, \
             patch(
                "YM_data_collection.ingestion.realtime_kline_handler.asyncio.to_thread",
            ) as mock_to_thread:

            mock_val.return_value = ValidationResult(
                is_valid=False,
                issues=["open_ts_ms (0) >= close_ts_ms (0)"],
                is_repairable=False,
            )

            await handler.handle_message("spot", msg)

            mock_to_thread.assert_not_called()


class TestCheckpointUpdatedOnPersist:
    """Checkpoint upserted after MySQL write."""

    @pytest.mark.asyncio
    async def test_checkpoint_called(self, handler):
        msg = _make_ws_closed_kline(is_closed=True)
        with patch(
            "YM_data_collection.ingestion.realtime_kline_handler.asyncio.to_thread",
            return_value=1,
        ), \
             patch.object(
                handler._checkpoint_repo, "upsert", MagicMock()
            ) as mock_ckpt_upsert:

            await handler.handle_message("spot", msg)

            mock_ckpt_upsert.assert_called_once()
            checkpoint = mock_ckpt_upsert.call_args[0][0]
            assert checkpoint.venue == "binance"
            assert checkpoint.market_type == "spot"
            assert checkpoint.data_type == "kline"
            assert checkpoint.status == "ok"


class TestRedisCacheUpdated:
    """set_json called with correct key parts."""

    @pytest.mark.asyncio
    async def test_redis_set_json_called(self, handler, mock_redis):
        msg = _make_ws_closed_kline(is_closed=False)
        await handler.handle_message("spot", msg)

        mock_redis.set_json.assert_called_once()
        call_args = mock_redis.set_json.call_args
        # Key parts: market_type, symbol, "kline", interval_code
        assert call_args[0][0] == "spot"
        assert call_args[0][1] == "BTCUSDT"
        assert call_args[0][2] == "kline"
        assert call_args[0][3] == "1h"
        # Payload should be a dict
        assert isinstance(call_args[1]["payload"], dict)


class TestSpotUsesSpotKlinesTable:
    """market_type='spot' routes to spot_klines table."""

    @pytest.mark.asyncio
    async def test_spot_table(self, handler):
        msg = _make_ws_closed_kline(is_closed=True)
        with patch(
            "YM_data_collection.ingestion.realtime_kline_handler.asyncio.to_thread",
            return_value=1,
        ) as mock_to_thread, \
             patch.object(handler._checkpoint_repo, "upsert", MagicMock()):

            await handler.handle_message("spot", msg)

            # to_thread is called with (func, session_factory, table_name, [kline])
            call_args = mock_to_thread.call_args[0]
            # call_args[0] is the function, [1] session_factory, [2] table_name
            assert call_args[2] == "spot_klines"


class TestPerpUsesPerpKlinesTable:
    """market_type='perp' routes to perp_klines table."""

    @pytest.mark.asyncio
    async def test_perp_table(self, handler):
        msg = _make_ws_closed_kline(is_closed=True)
        with patch(
            "YM_data_collection.ingestion.realtime_kline_handler.asyncio.to_thread",
            return_value=1,
        ) as mock_to_thread, \
             patch.object(handler._checkpoint_repo, "upsert", MagicMock()):

            await handler.handle_message("perp", msg)

            call_args = mock_to_thread.call_args[0]
            assert call_args[2] == "perp_klines"


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

class TestMissingKField:
    """Message without 'k' field should be skipped gracefully."""

    @pytest.mark.asyncio
    async def test_missing_k_field(self, handler, mock_redis):
        msg = {"e": "kline", "E": 12345, "s": "BTCUSDT"}
        await handler.handle_message("spot", msg)
        mock_redis.set_json.assert_not_called()


class TestRepairableKline:
    """Repairable invalid kline should be persisted with repaired data."""

    @pytest.mark.asyncio
    async def test_repairable_kline_persisted(self, handler, mock_redis):
        msg = _make_ws_closed_kline(is_closed=True)
        repaired_kline = _make_valid_1h_kline()

        with patch(
            "YM_data_collection.ingestion.realtime_kline_handler.validate_kline",
        ) as mock_val, \
             patch(
                "YM_data_collection.ingestion.realtime_kline_handler.asyncio.to_thread",
                return_value=1,
            ) as mock_to_thread, \
             patch.object(handler._checkpoint_repo, "upsert", MagicMock()):

            mock_val.return_value = ValidationResult(
                is_valid=False,
                issues=["close_ts_ms off by 3 ms"],
                is_repairable=True,
                repaired_kline=repaired_kline,
            )

            await handler.handle_message("spot", msg)

            # to_thread should have been called — repaired kline is persisted
            mock_to_thread.assert_called_once()
