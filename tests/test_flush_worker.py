"""Tests for persistence/flush_worker.py — all mocked (no real Redis/MySQL)."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from YM_data_collection.cache.keyspace import CacheKeyBuilder
from YM_data_collection.config.models import (
    RealtimePersistenceConfig,
    WritePolicy,
    WritePolicyConfig,
)
from YM_data_collection.domain.models import DataQualityIssue
from YM_data_collection.persistence.flush_worker import FlushWorker


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

def _make_redis_client(key_prefix: str = "ym:binance") -> MagicMock:
    """Return a mock RedisCacheClient with a key_builder and inner _client."""
    mock = MagicMock()
    mock.key_builder = CacheKeyBuilder(key_prefix)
    mock._client = MagicMock()
    # build_key delegates to key_builder.build
    mock.build_key = lambda *parts: mock.key_builder.build(*parts)
    return mock


def _make_config(
    interval: int = 60,
    retention: int = 120,
    write_policy: WritePolicyConfig | None = None,
) -> RealtimePersistenceConfig:
    wp = write_policy or WritePolicyConfig()  # defaults all redis_first except kline/funding
    return RealtimePersistenceConfig(
        mysql_flush_interval_seconds=interval,
        redis_retention_after_flush_seconds=retention,
        write_policy=wp,
    )


def _mark_price_payload(symbol: str = "btcusdt") -> dict:
    return {
        "venue": "binance",
        "symbol": symbol.upper(),
        "instrument_code": symbol.upper(),
        "event_ts_ms": 1700000000000,
        "event_dt_utc": "2023-11-14T22:13:20",
        "mark_price": "42000.50",
        "funding_rate": "0.0001",
        "next_funding_time_ts_ms": 1700008000000,
        "source": "exchange",
    }


def _open_interest_payload(symbol: str = "btcusdt") -> dict:
    return {
        "venue": "binance",
        "symbol": symbol.upper(),
        "instrument_code": symbol.upper(),
        "event_ts_ms": 1700000000000,
        "event_dt_utc": "2023-11-14T22:13:20",
        "open_interest": "10000.5",
        "open_interest_value": "420005000.0",
        "source": "exchange",
    }


def _depth_snapshot_payload(symbol: str = "btcusdt") -> dict:
    return {
        "venue": "binance",
        "symbol": symbol.upper(),
        "instrument_code": symbol.upper(),
        "event_ts_ms": 1700000000000,
        "event_dt_utc": "2023-11-14T22:13:20",
        "best_bid_price": "41999.00",
        "best_bid_qty": "1.5",
        "best_ask_price": "42001.00",
        "best_ask_qty": "2.0",
        "mid_price": "42000.00",
        "spread_abs": "2.00",
        "spread_bps": "4.76",
        "depth_levels": 20,
        "bid_depth_json": [["41999.00", "1.5"]],
        "ask_depth_json": [["42001.00", "2.0"]],
        "source": "exchange",
        "market_type": "perp",
    }


def _index_price_payload(symbol: str = "btcusdt") -> dict:
    return {
        "venue": "binance",
        "symbol": symbol.upper(),
        "instrument_code": symbol.upper(),
        "event_ts_ms": 1700000000000,
        "event_dt_utc": "2023-11-14T22:13:20",
        "index_price": "42001.25",
        "source": "exchange",
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestFlushWorker:
    """Tests for FlushWorker."""

    @pytest.mark.asyncio
    async def test_flush_once_reads_redis_and_upserts_mysql(self):
        """Happy path: one data type (mark_price) scanned, upserted."""
        redis = _make_redis_client()
        session_factory = MagicMock()
        quality_repo = MagicMock()
        config = _make_config()

        # Simulate one mark_price key in Redis
        key = "ym:binance:mark_price:btcusdt"
        redis._client.scan.return_value = (0, [key])
        redis._client.get.return_value = json.dumps(_mark_price_payload())
        redis._client.expire.return_value = True

        worker = FlushWorker(redis, session_factory, config, quality_repo=quality_repo)

        # Patch MarkPriceRepository.upsert_batch to return 1
        with patch(
            "YM_data_collection.persistence.flush_worker.MarkPriceRepository.upsert_batch",
            return_value=1,
        ):
            result = await worker.flush_once()

        assert result["mark_price"] == 1
        redis._client.expire.assert_called_once_with(key, 120)

    @pytest.mark.asyncio
    async def test_flush_once_handles_all_data_types(self):
        """mark_price + open_interest + depth_snapshot + index_price all flush."""
        redis = _make_redis_client()
        session_factory = MagicMock()
        quality_repo = MagicMock()
        config = _make_config()

        mp_key = "ym:binance:mark_price:btcusdt"
        oi_key = "ym:binance:open_interest:btcusdt"
        ds_key = "ym:binance:depth_snapshot:btcusdt"
        ip_key = "ym:binance:index_price:btcusdt"

        all_keys = [mp_key, oi_key, ds_key, ip_key]

        # scan returns all keys on first call, then cursor=0
        call_count = {"n": 0}
        def scan_side_effect(cursor, match, count):
            # Return all keys matching this pattern
            suffix = match.split(":")[-1].replace("*", "")
            matched = [k for k in all_keys if suffix in k]
            return (0, matched)

        redis._client.scan.side_effect = scan_side_effect

        # get returns appropriate payload per key
        def get_side_effect(key):
            payloads = {
                mp_key: json.dumps(_mark_price_payload()),
                oi_key: json.dumps(_open_interest_payload()),
                ds_key: json.dumps(_depth_snapshot_payload()),
                ip_key: json.dumps(_index_price_payload()),
            }
            return payloads.get(key)

        redis._client.get.side_effect = get_side_effect
        redis._client.expire.return_value = True

        worker = FlushWorker(redis, session_factory, config, quality_repo=quality_repo)

        with patch(
            "YM_data_collection.persistence.flush_worker.MarkPriceRepository.upsert_batch",
            return_value=1,
        ), patch(
            "YM_data_collection.persistence.flush_worker.OpenInterestRepository.upsert_batch",
            return_value=1,
        ), patch(
            "YM_data_collection.persistence.flush_worker.DepthSnapshotRepository.upsert_batch",
            return_value=1,
        ), patch(
            "YM_data_collection.persistence.flush_worker.IndexPriceRepository.upsert_batch",
            return_value=1,
        ):
            result = await worker.flush_once()

        assert result["mark_price"] == 1
        assert result["open_interest"] == 1
        assert result["depth_snapshot"] == 1
        assert result["index_price"] == 1

    @pytest.mark.asyncio
    async def test_flush_failure_records_quality_issue(self):
        """MySQL upsert error triggers quality issue recording."""
        redis = _make_redis_client()
        session_factory = MagicMock()
        quality_repo = MagicMock()
        config = _make_config()

        key = "ym:binance:mark_price:btcusdt"
        redis._client.scan.return_value = (0, [key])
        redis._client.get.return_value = json.dumps(_mark_price_payload())

        worker = FlushWorker(redis, session_factory, config, quality_repo=quality_repo)

        with patch(
            "YM_data_collection.persistence.flush_worker.MarkPriceRepository.upsert_batch",
            side_effect=RuntimeError("MySQL connection lost"),
        ):
            result = await worker.flush_once()

        assert result["mark_price"] == 0
        quality_repo.insert.assert_called_once()
        issue = quality_repo.insert.call_args[0][0]
        assert isinstance(issue, DataQualityIssue)
        assert issue.issue_type == "flush_failure"
        assert "MySQL connection lost" in issue.description

    @pytest.mark.asyncio
    async def test_flush_failure_continues_to_next_type(self):
        """One data type failure does not block others from flushing."""
        redis = _make_redis_client()
        session_factory = MagicMock()
        quality_repo = MagicMock()
        config = _make_config()

        mp_key = "ym:binance:mark_price:btcusdt"
        oi_key = "ym:binance:open_interest:btcusdt"

        def scan_side_effect(cursor, match, count):
            if "mark_price" in match:
                return (0, [mp_key])
            return (0, [oi_key])

        redis._client.scan.side_effect = scan_side_effect

        def get_side_effect(key):
            if "mark_price" in key:
                return json.dumps(_mark_price_payload())
            return json.dumps(_open_interest_payload())

        redis._client.get.side_effect = get_side_effect
        redis._client.expire.return_value = True

        worker = FlushWorker(redis, session_factory, config, quality_repo=quality_repo)

        with patch(
            "YM_data_collection.persistence.flush_worker.MarkPriceRepository.upsert_batch",
            side_effect=RuntimeError("MySQL down"),
        ), patch(
            "YM_data_collection.persistence.flush_worker.OpenInterestRepository.upsert_batch",
            return_value=1,
        ):
            result = await worker.flush_once()

        assert result["mark_price"] == 0
        assert result["open_interest"] == 1
        # Quality issue recorded for mark_price
        quality_repo.insert.assert_called_once()

    @pytest.mark.asyncio
    async def test_sets_redis_expiry_after_flush(self):
        """Flushed keys get TTL set to redis_retention_after_flush_seconds."""
        redis = _make_redis_client()
        session_factory = MagicMock()
        quality_repo = MagicMock()
        config = _make_config(retention=300)

        key1 = "ym:binance:mark_price:btcusdt"
        key2 = "ym:binance:mark_price:ethusdt"
        redis._client.scan.return_value = (0, [key1, key2])

        def get_side_effect(key):
            if "btcusdt" in key:
                return json.dumps(_mark_price_payload("btcusdt"))
            return json.dumps(_mark_price_payload("ethusdt"))

        redis._client.get.side_effect = get_side_effect
        redis._client.expire.return_value = True

        worker = FlushWorker(redis, session_factory, config, quality_repo=quality_repo)

        with patch(
            "YM_data_collection.persistence.flush_worker.MarkPriceRepository.upsert_batch",
            return_value=2,
        ):
            result = await worker.flush_once()

        assert result["mark_price"] == 2
        assert redis._client.expire.call_count == 2
        # Verify the retention value passed
        for call in redis._client.expire.call_args_list:
            assert call[0][1] == 300

    @pytest.mark.asyncio
    async def test_no_data_in_redis_returns_zero(self):
        """Empty Redis scan returns 0 rows for all data types."""
        redis = _make_redis_client()
        session_factory = MagicMock()
        quality_repo = MagicMock()
        config = _make_config()

        redis._client.scan.return_value = (0, [])

        worker = FlushWorker(redis, session_factory, config, quality_repo=quality_repo)
        result = await worker.flush_once()

        # All redis_first types should report 0
        for dtype in ("mark_price", "index_price", "open_interest", "depth_snapshot"):
            assert result.get(dtype, 0) == 0

    @pytest.mark.asyncio
    async def test_start_stop_lifecycle(self):
        """start() creates an asyncio task; stop() cancels it gracefully."""
        redis = _make_redis_client()
        session_factory = MagicMock()
        config = _make_config(interval=9999)  # long interval so it won't fire

        worker = FlushWorker(redis, session_factory, config)

        assert not worker.is_running
        await worker.start()
        assert worker.is_running
        assert worker._task is not None

        await worker.stop()
        assert not worker.is_running
        assert worker._task is None

    @pytest.mark.asyncio
    async def test_is_running_property(self):
        """is_running reflects task state accurately."""
        redis = _make_redis_client()
        session_factory = MagicMock()
        config = _make_config(interval=9999)

        worker = FlushWorker(redis, session_factory, config)
        assert worker.is_running is False

        await worker.start()
        assert worker.is_running is True

        await worker.stop()
        assert worker.is_running is False

    @pytest.mark.asyncio
    async def test_flush_count_increments(self):
        """Counter goes up after each flush cycle."""
        redis = _make_redis_client()
        session_factory = MagicMock()
        quality_repo = MagicMock()
        config = _make_config()

        redis._client.scan.return_value = (0, [])

        worker = FlushWorker(redis, session_factory, config, quality_repo=quality_repo)
        assert worker.flush_count == 0

        await worker.flush_once()
        assert worker.flush_count == 1

        await worker.flush_once()
        assert worker.flush_count == 2

    @pytest.mark.asyncio
    async def test_last_flush_at_updated(self):
        """Timestamp is updated after each flush."""
        redis = _make_redis_client()
        session_factory = MagicMock()
        quality_repo = MagicMock()
        config = _make_config()

        redis._client.scan.return_value = (0, [])

        worker = FlushWorker(redis, session_factory, config, quality_repo=quality_repo)
        assert worker.last_flush_at is None

        await worker.flush_once()
        first = worker.last_flush_at
        assert first is not None
        assert isinstance(first, datetime)

        await worker.flush_once()
        second = worker.last_flush_at
        assert second is not None
        # Second flush should be at or after the first
        assert second >= first


    @pytest.mark.asyncio
    async def test_depth_snapshots_route_to_spot_and_perp_tables(self):
        """Depth snapshots must not all be forced into perp_depth_snapshots."""
        redis = _make_redis_client()
        session_factory = MagicMock()
        config = _make_config()

        spot_key = "ym:binance:depth_snapshot:spot:btcusdt"
        perp_key = "ym:binance:depth_snapshot:perp:btcusdt"
        redis._client.scan.side_effect = lambda cursor, match, count: (
            (0, [spot_key, perp_key]) if "depth_snapshot" in match else (0, [])
        )

        spot_payload = _depth_snapshot_payload()
        spot_payload["market_type"] = "spot"
        perp_payload = _depth_snapshot_payload()
        perp_payload["market_type"] = "perp"
        redis._client.get.side_effect = lambda key: json.dumps(spot_payload if key == spot_key else perp_payload)
        redis._client.expire.return_value = True

        worker = FlushWorker(redis, session_factory, config)

        calls = []
        def fake_upsert(session_factory_arg, table_name, records):
            calls.append((table_name, [r.market_type for r in records]))
            return len(records)

        with patch(
            "YM_data_collection.persistence.flush_worker.DepthSnapshotRepository.upsert_batch",
            side_effect=fake_upsert,
        ):
            result = await worker.flush_once()

        assert result["depth_snapshot"] == 2
        assert ("spot_depth_snapshots", ["spot"]) in calls
        assert ("perp_depth_snapshots", ["perp"]) in calls
