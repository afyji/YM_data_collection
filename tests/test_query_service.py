"""Tests for MarketDataQueryService."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from YM_data_collection.config.models import QuerySourceConfig
from YM_data_collection.services.query_service import MarketDataQueryService


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def session_factory():
    return MagicMock(name="session_factory")


@pytest.fixture()
def cache_client():
    c = MagicMock(name="cache_client")
    c.get_json.return_value = None
    c.set_json.return_value = True
    c.build_key.side_effect = lambda *parts: ":".join(parts)
    return c


@pytest.fixture()
def cfg_cache_first_with_fallback():
    return QuerySourceConfig(
        snapshot_cache_first_enabled=True,
        depth_cache_first_enabled=True,
        snapshot_mysql_fallback_enabled=True,
        depth_mysql_fallback_enabled=True,
        cache_backfill_on_fallback_enabled=True,
        allow_http_read_from_parquet=False,
    )


@pytest.fixture()
def cfg_mysql_only():
    return QuerySourceConfig(
        snapshot_cache_first_enabled=False,
        depth_cache_first_enabled=False,
        snapshot_mysql_fallback_enabled=True,
        depth_mysql_fallback_enabled=True,
        cache_backfill_on_fallback_enabled=False,
        allow_http_read_from_parquet=False,
    )


@pytest.fixture()
def repos():
    kline_repo = MagicMock(name="kline_repo")
    mark_repo = MagicMock(name="mark_price_repo")
    index_repo = MagicMock(name="index_price_repo")
    oi_repo = MagicMock(name="oi_repo")
    fr_repo = MagicMock(name="funding_rate_repo")
    depth_repo = MagicMock(name="depth_repo")
    return {
        "kline": kline_repo,
        "mark_price": mark_repo,
        "index_price": index_repo,
        "open_interest": oi_repo,
        "funding_rate": fr_repo,
        "depth_snapshot": depth_repo,
    }


def _make_service(cache_client, cfg, repos, session_factory):
    return MarketDataQueryService(
        session_factory=session_factory,
        cache_client=cache_client,
        query_source_config=cfg,
        repos=repos,
    )


# ---------------------------------------------------------------------------
# Klines
# ---------------------------------------------------------------------------

class TestQueryKlinesRange:
    def test_returns_mysql_data(self, session_factory, cache_client, cfg_mysql_only, repos):
        repos["kline"].query_range.return_value = [
            {"open_ts_ms": 1000, "close_price": "50000.00"},
            {"open_ts_ms": 2000, "close_price": "50100.00"},
        ]
        svc = _make_service(cache_client, cfg_mysql_only, repos, session_factory)
        result = svc.query_klines_range("spot", "BTCUSDT", "1h", 1000, 2000)
        assert result["data"] == repos["kline"].query_range.return_value
        assert result["meta"]["source"] == "mysql"
        repos["kline"].query_range.assert_called_once_with(
            session_factory, "spot_klines", "BTCUSDT", "1h", 1000, 2000
        )

    def test_empty_range(self, session_factory, cache_client, cfg_mysql_only, repos):
        repos["kline"].query_range.return_value = []
        svc = _make_service(cache_client, cfg_mysql_only, repos, session_factory)
        result = svc.query_klines_range("perp", "ETHUSDT", "4h", 0, 100)
        assert result["data"] == []
        assert result["meta"]["source"] == "mysql"


class TestQueryKlinesRecent:
    def test_returns_latest_klines(self, session_factory, cache_client, cfg_mysql_only, repos):
        repos["kline"].query_latest.return_value = [{"open_ts_ms": 9000}]
        svc = _make_service(cache_client, cfg_mysql_only, repos, session_factory)
        result = svc.query_klines_recent("spot", "BTCUSDT", "1h", 5)
        assert result["data"] == [{"open_ts_ms": 9000}]
        assert result["meta"]["source"] == "mysql"
        repos["kline"].query_latest.assert_called_once_with(
            session_factory, "spot_klines", "BTCUSDT", "1h", limit=5
        )


# ---------------------------------------------------------------------------
# Latest snapshot – cache hit
# ---------------------------------------------------------------------------

class TestQueryLatestSnapshotCacheHit:
    def test_cache_hit_no_mysql(self, session_factory, cache_client, cfg_cache_first_with_fallback, repos):
        cache_client.get_json.return_value = {
            "event_ts_ms": 5000,
            "mark_price": "50000.00",
        }
        svc = _make_service(cache_client, cfg_cache_first_with_fallback, repos, session_factory)
        result = svc.query_latest_snapshot("perp", "BTCUSDT", "mark_price")
        assert result["data"]["mark_price"] == "50000.00"
        assert result["meta"]["source"] == "cache"
        assert result["meta"]["fallback_used"] is False
        # MySQL repo should NOT be called because cache hit
        repos["mark_price"].query_latest.assert_not_called()


# ---------------------------------------------------------------------------
# Latest snapshot – cache miss, MySQL fallback
# ---------------------------------------------------------------------------

class TestQueryLatestSnapshotCacheMissFallback:
    def test_cache_miss_mysql_fallback(self, session_factory, cache_client, cfg_cache_first_with_fallback, repos):
        cache_client.get_json.return_value = None
        repos["mark_price"].query_latest.return_value = [
            {"event_ts_ms": 3000, "mark_price": "49000.00"}
        ]
        svc = _make_service(cache_client, cfg_cache_first_with_fallback, repos, session_factory)
        result = svc.query_latest_snapshot("perp", "BTCUSDT", "mark_price")
        assert result["data"]["mark_price"] == "49000.00"
        assert result["meta"]["source"] == "mysql"
        assert result["meta"]["fallback_used"] is True
        assert result["meta"]["cache_refreshed"] is True
        # Backfill should have been called
        cache_client.set_json.assert_called_once()

    def test_cache_miss_mysql_also_empty(self, session_factory, cache_client, cfg_cache_first_with_fallback, repos):
        cache_client.get_json.return_value = None
        repos["mark_price"].query_latest.return_value = []
        svc = _make_service(cache_client, cfg_cache_first_with_fallback, repos, session_factory)
        result = svc.query_latest_snapshot("perp", "BTCUSDT", "mark_price")
        assert result["data"] is None
        assert result["meta"]["source"] == "none"


# ---------------------------------------------------------------------------
# Auto-stitch (cache + mysql) – applies to range queries only
# ---------------------------------------------------------------------------

class TestAutoStitch:
    def test_cache_newer_than_mysql_range(self, session_factory, cache_client, cfg_cache_first_with_fallback, repos):
        """Auto-stitch in range queries: cache has latest, MySQL has historical."""
        repos["mark_price"].query_range.return_value = [
            {"event_ts_ms": 3000, "mark_price": "49000.00"},
            {"event_ts_ms": 5000, "mark_price": "50000.00"},
        ]
        cache_client.get_json.return_value = {
            "event_ts_ms": 7000,
            "mark_price": "51000.00",
        }
        svc = _make_service(cache_client, cfg_cache_first_with_fallback, repos, session_factory)
        result = svc.query_range("perp", "BTCUSDT", "mark_price", 0, 10000)
        assert len(result["data"]) == 3
        assert result["data"][-1]["mark_price"] == "51000.00"
        assert "mysql" in result["meta"]["source"]
        assert "cache" in result["meta"]["source"]

    def test_mysql_only_when_no_cache_range(self, session_factory, cache_client, cfg_cache_first_with_fallback, repos):
        """Range query with no cache returns MySQL data only."""
        cache_client.get_json.return_value = None
        repos["mark_price"].query_range.return_value = [
            {"event_ts_ms": 5000, "mark_price": "50000.00"}
        ]
        svc = _make_service(cache_client, cfg_cache_first_with_fallback, repos, session_factory)
        result = svc.query_range("perp", "BTCUSDT", "mark_price", 0, 10000)
        assert result["meta"]["source"] == "mysql"


# ---------------------------------------------------------------------------
# Latest depth
# ---------------------------------------------------------------------------

class TestQueryLatestDepth:
    def test_cache_hit(self, session_factory, cache_client, cfg_cache_first_with_fallback, repos):
        cache_client.get_json.return_value = {
            "event_ts_ms": 9000,
            "best_bid_price": "50000.00",
        }
        svc = _make_service(cache_client, cfg_cache_first_with_fallback, repos, session_factory)
        result = svc.query_latest_depth("perp", "BTCUSDT")
        assert result["data"]["best_bid_price"] == "50000.00"
        assert result["meta"]["source"] == "cache"

    def test_cache_miss_mysql_fallback(self, session_factory, cache_client, cfg_cache_first_with_fallback, repos):
        cache_client.get_json.return_value = None
        repos["depth_snapshot"].query_latest.return_value = [
            {"event_ts_ms": 8000, "best_bid_price": "49900.00"}
        ]
        svc = _make_service(cache_client, cfg_cache_first_with_fallback, repos, session_factory)
        result = svc.query_latest_depth("spot", "BTCUSDT")
        assert result["data"]["best_bid_price"] == "49900.00"
        assert result["meta"]["source"] == "mysql"
        assert result["meta"]["fallback_used"] is True
        repos["depth_snapshot"].query_latest.assert_called_once_with(
            session_factory, "spot_depth_snapshots", "BTCUSDT", limit=1
        )

    def test_no_data(self, session_factory, cache_client, cfg_cache_first_with_fallback, repos):
        cache_client.get_json.return_value = None
        repos["depth_snapshot"].query_latest.return_value = []
        svc = _make_service(cache_client, cfg_cache_first_with_fallback, repos, session_factory)
        result = svc.query_latest_depth("perp", "BTCUSDT")
        assert result["data"] is None
        assert result["meta"]["source"] == "none"


# ---------------------------------------------------------------------------
# Range query with auto-stitch
# ---------------------------------------------------------------------------

class TestRangeAutoStitch:
    def test_kline_range_delegates(self, session_factory, cache_client, cfg_mysql_only, repos):
        repos["kline"].query_range.return_value = [{"open_ts_ms": 100}]
        svc = _make_service(cache_client, cfg_mysql_only, repos, session_factory)
        result = svc.query_range("spot", "BTCUSDT", "kline", 0, 1000, interval_code="1h")
        assert result["data"] == [{"open_ts_ms": 100}]

    def test_mark_price_range_stitch(self, session_factory, cache_client, cfg_cache_first_with_fallback, repos):
        repos["mark_price"].query_range.return_value = [
            {"event_ts_ms": 1000, "mark_price": "50000.00"},
            {"event_ts_ms": 2000, "mark_price": "50100.00"},
        ]
        cache_client.get_json.return_value = {
            "event_ts_ms": 3000,
            "mark_price": "50200.00",
        }
        svc = _make_service(cache_client, cfg_cache_first_with_fallback, repos, session_factory)
        result = svc.query_range("perp", "BTCUSDT", "mark_price", 0, 5000)
        assert len(result["data"]) == 3
        assert result["data"][0]["event_ts_ms"] == 1000
        assert result["data"][-1]["mark_price"] == "50200.00"

    def test_dedup_by_ts(self, session_factory, cache_client, cfg_cache_first_with_fallback, repos):
        repos["mark_price"].query_range.return_value = [
            {"event_ts_ms": 2000, "mark_price": "50000.00"},
        ]
        cache_client.get_json.return_value = {
            "event_ts_ms": 2000,
            "mark_price": "50000.00_UPDATED",
        }
        svc = _make_service(cache_client, cfg_cache_first_with_fallback, repos, session_factory)
        result = svc.query_range("perp", "BTCUSDT", "mark_price", 0, 5000)
        # Should be deduplicated – only 1 row at ts 2000
        # First seen wins in our dedup
        assert len(result["data"]) == 1
