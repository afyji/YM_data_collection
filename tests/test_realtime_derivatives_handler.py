"""Tests for RealtimeDerivativesHandler."""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from YM_data_collection.config.models import RealtimePersistenceConfig, WritePolicy, WritePolicyConfig
from YM_data_collection.ingestion.realtime_derivatives_handler import RealtimeDerivativesHandler


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_redis():
    """Mock RedisCacheClient."""
    redis = MagicMock()
    redis.set_json = MagicMock(return_value=True)
    redis.build_key = MagicMock(return_value="ym:binance:test")
    return redis


@pytest.fixture
def mock_session_factory():
    """Mock session factory (not used directly by handler repos)."""
    return MagicMock()


@pytest.fixture
def config():
    """Default RealtimePersistenceConfig."""
    return RealtimePersistenceConfig()


@pytest.fixture
def handler(mock_session_factory, mock_redis, config):
    """Handler with mocked deps."""
    return RealtimeDerivativesHandler(
        session_factory=mock_session_factory,
        redis_client=mock_redis,
        config=config,
        venue="binance",
        logger=MagicMock(),
    )


@pytest.fixture
def mark_price_update_data():
    """Sample markPriceUpdate event data."""
    return {
        "e": "markPriceUpdate",
        "E": 1672515782136,
        "s": "BTCUSDT",
        "p": "50000.00",
        "i": "50100.00",
        "P": "50050.00",
        "r": "0.00010000",
        "T": 1672531200000,
    }


@pytest.fixture
def open_interest_data():
    """Sample open interest data (from REST poll)."""
    return {
        "symbol": "BTCUSDT",
        "sumOpenInterest": "12345.678",
        "sumOpenInterestValue": "427654321.12",
        "timestamp": 1672515782136,
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_mark_price_update_writes_redis(handler, mock_redis, mark_price_update_data):
    """Mark price cached to Redis on markPriceUpdate."""
    await handler.handle_mark_price_update(mark_price_update_data)

    # Find the set_json call for mark_price
    mark_price_calls = [
        c for c in mock_redis.set_json.call_args_list
        if c[0][0] == "mark_price"
    ]
    assert len(mark_price_calls) == 1
    call = mark_price_calls[0]
    assert call[0][1] == "binance"
    assert call[0][2] == "BTCUSDT"
    payload = call[1]["payload"]
    assert payload["mark_price"] == "50000.00"
    assert payload["symbol"] == "BTCUSDT"
    assert payload["venue"] == "binance"


@pytest.mark.asyncio
async def test_index_price_extracted_and_cached(handler, mock_redis, mark_price_update_data):
    """Index price from markPriceUpdate cached to Redis."""
    await handler.handle_mark_price_update(mark_price_update_data)

    index_price_calls = [
        c for c in mock_redis.set_json.call_args_list
        if c[0][0] == "index_price"
    ]
    assert len(index_price_calls) == 1
    call = index_price_calls[0]
    payload = call[1]["payload"]
    assert payload["index_price"] == "50100.00"
    assert payload["symbol"] == "BTCUSDT"


@pytest.mark.asyncio
async def test_funding_rate_persisted_to_mysql(handler, mock_session_factory, mark_price_update_data):
    """Funding_rate written to MySQL (mysql_first)."""
    with patch.object(
        handler._funding_rate_repo, "upsert_batch", return_value=1
    ) as mock_upsert:
        await handler.handle_mark_price_update(mark_price_update_data)

        mock_upsert.assert_called_once()
        args = mock_upsert.call_args
        records = args[0][1]  # second positional arg = list of records
        assert len(records) == 1
        fr = records[0]
        assert fr.funding_rate == Decimal("0.00010000")
        assert fr.symbol == "BTCUSDT"
        assert fr.mark_price == Decimal("50000.00")
        assert fr.funding_time_ts_ms == 1672531200000


@pytest.mark.asyncio
async def test_funding_rate_also_cached(handler, mock_redis, mark_price_update_data):
    """Funding rate also written to Redis."""
    with patch.object(handler._funding_rate_repo, "upsert_batch", return_value=1):
        await handler.handle_mark_price_update(mark_price_update_data)

    funding_rate_calls = [
        c for c in mock_redis.set_json.call_args_list
        if c[0][0] == "funding_rate"
    ]
    assert len(funding_rate_calls) == 1
    payload = funding_rate_calls[0][1]["payload"]
    assert payload["funding_rate"] == "0.00010000"
    assert payload["symbol"] == "BTCUSDT"


@pytest.mark.asyncio
async def test_open_interest_cached_to_redis(handler, mock_redis, open_interest_data):
    """OI goes to Redis."""
    await handler.handle_open_interest("BTCUSDT", open_interest_data)

    mock_redis.set_json.assert_called_once()
    call = mock_redis.set_json.call_args
    assert call[0][0] == "open_interest"
    assert call[0][1] == "binance"
    assert call[0][2] == "BTCUSDT"
    payload = call[1]["payload"]
    assert payload["open_interest"] == "12345.678"
    assert payload["symbol"] == "BTCUSDT"


@pytest.mark.asyncio
async def test_handle_message_dispatches_correctly(handler, mark_price_update_data):
    """markPriceUpdate event routed to the correct handler."""
    with patch.object(
        handler, "handle_mark_price_update", new_callable=AsyncMock
    ) as mock_mp:
        await handler.handle_message("btcusdt@markPrice", mark_price_update_data)
        mock_mp.assert_called_once_with(mark_price_update_data)


@pytest.mark.asyncio
async def test_invalid_mark_price_skipped(handler, mock_redis):
    """Validation failure for mark_price skips the Redis write."""
    bad_data = {
        "e": "markPriceUpdate",
        "E": 1672515782136,
        "s": "BTCUSDT",
        "p": "0",        # mark_price <= 0 → validation error
        "i": "50100.00",
        "r": "0.00010000",
        "T": 1672531200000,
    }
    await handler.handle_mark_price_update(bad_data)

    # mark_price should NOT be written to Redis (validation failed)
    mark_price_calls = [
        c for c in mock_redis.set_json.call_args_list
        if c[0][0] == "mark_price"
    ]
    assert len(mark_price_calls) == 0


@pytest.mark.asyncio
async def test_mysql_error_doesnt_crash(handler, mock_redis, mark_price_update_data):
    """MySQL write failure logged, not raised."""
    with patch.object(
        handler._funding_rate_repo,
        "upsert_batch",
        side_effect=Exception("MySQL connection lost"),
    ):
        # Should NOT raise
        await handler.handle_mark_price_update(mark_price_update_data)

    # Logger should have recorded the error
    handler._logger.error.assert_called()
    # Redis cache for funding_rate should still be attempted (after MySQL fails)
    funding_rate_calls = [
        c for c in mock_redis.set_json.call_args_list
        if c[0][0] == "funding_rate"
    ]
    assert len(funding_rate_calls) == 1


@pytest.mark.asyncio
async def test_handle_message_unhandled_event(handler):
    """Unknown event type does not crash, just logs debug."""
    await handler.handle_message("unknown@stream", {"e": "unknownEvent", "data": 42})
    # No crash, logger.debug may have been called
    assert True


@pytest.mark.asyncio
async def test_handle_message_open_interest_dispatch(handler, open_interest_data):
    """openInterest event type dispatched to handle_open_interest."""
    oi_event = {
        "e": "openInterest",
        "s": "BTCUSDT",
        "sumOpenInterest": "12345.678",
        "sumOpenInterestValue": "427654321.12",
        "timestamp": 1672515782136,
    }
    with patch.object(
        handler, "handle_open_interest", new_callable=AsyncMock
    ) as mock_oi:
        await handler.handle_message("btcusdt@openInterest", oi_event)
        mock_oi.assert_called_once_with("BTCUSDT", oi_event)


@pytest.mark.asyncio
async def test_missing_symbol_skipped(handler, mock_redis):
    """Missing symbol in markPriceUpdate skips processing gracefully."""
    bad_data = {
        "e": "markPriceUpdate",
        "E": 1672515782136,
        "s": "",
        "p": "50000.00",
        "i": "50100.00",
        "r": "0.00010000",
        "T": 1672531200000,
    }
    await handler.handle_mark_price_update(bad_data)
    # No writes should happen
    mock_redis.set_json.assert_not_called()


@pytest.mark.asyncio
async def test_invalid_open_interest_skipped(handler, mock_redis):
    """Validation failure for open_interest skips the Redis write."""
    bad_oi = {
        "symbol": "BTCUSDT",
        "sumOpenInterest": "-5",  # negative → validation error
        "sumOpenInterestValue": "427654321.12",
        "timestamp": 1672515782136,
    }
    await handler.handle_open_interest("BTCUSDT", bad_oi)
    mock_redis.set_json.assert_not_called()


@pytest.mark.asyncio
async def test_redis_error_doesnt_crash_mark_price(handler, mark_price_update_data):
    """Redis write error for mark_price is logged, not raised."""
    handler._redis.set_json = MagicMock(side_effect=Exception("Redis down"))
    with patch.object(handler._funding_rate_repo, "upsert_batch", return_value=1):
        # Should not raise
        await handler.handle_mark_price_update(mark_price_update_data)
    handler._logger.error.assert_called()


@pytest.mark.asyncio
async def test_redis_error_doesnt_crash_open_interest(handler, open_interest_data):
    """Redis write error for open_interest is logged, not raised."""
    handler._redis.set_json = MagicMock(side_effect=Exception("Redis down"))
    # Should not raise
    await handler.handle_open_interest("BTCUSDT", open_interest_data)
    handler._logger.error.assert_called()


@pytest.mark.asyncio
async def test_funding_rate_missing_fields_not_crashed(handler, mock_redis):
    """markPriceUpdate without funding rate fields still writes mark/index."""
    data = {
        "e": "markPriceUpdate",
        "E": 1672515782136,
        "s": "BTCUSDT",
        "p": "50000.00",
        "i": "50100.00",
        # no 'r' or 'T' → no funding rate record
    }
    await handler.handle_mark_price_update(data)

    # mark_price and index_price should still be written
    mark_calls = [c for c in mock_redis.set_json.call_args_list if c[0][0] == "mark_price"]
    index_calls = [c for c in mock_redis.set_json.call_args_list if c[0][0] == "index_price"]
    assert len(mark_calls) == 1
    assert len(index_calls) == 1
