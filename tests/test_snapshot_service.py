"""Tests for SnapshotService."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from YM_data_collection.config.models import QuerySourceConfig
from YM_data_collection.services.snapshot_service import SnapshotService


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
def cfg():
    return QuerySourceConfig(
        snapshot_cache_first_enabled=True,
        depth_cache_first_enabled=True,
        snapshot_mysql_fallback_enabled=True,
        depth_mysql_fallback_enabled=True,
        cache_backfill_on_fallback_enabled=True,
        allow_http_read_from_parquet=False,
    )


@pytest.fixture()
def repos():
    return {
        "mark_price": MagicMock(name="mark_price_repo"),
        "index_price": MagicMock(name="index_price_repo"),
        "open_interest": MagicMock(name="oi_repo"),
        "funding_rate": MagicMock(name="funding_rate_repo"),
        "depth_snapshot": MagicMock(name="depth_repo"),
    }


def _make_service(cache_client, cfg, repos, session_factory):
    return SnapshotService(
        cache_client=cache_client,
        session_factory=session_factory,
        repos=repos,
        query_source_config=cfg,
    )


# ---------------------------------------------------------------------------
# mark_price
# ---------------------------------------------------------------------------

class TestGetLatestMarkPrice:
    def test_cache_hit(self, session_factory, cache_client, cfg, repos):
        cache_client.get_json.return_value = {"event_ts_ms": 5000, "mark_price": "50000.00"}
        svc = _make_service(cache_client, cfg, repos, session_factory)
        result = svc.get_latest_mark_price("BTCUSDT")
        assert result["mark_price"] == "50000.00"
        repos["mark_price"].query_latest.assert_not_called()

    def test_cache_miss_mysql_fallback(self, session_factory, cache_client, cfg, repos):
        cache_client.get_json.return_value = None
        repos["mark_price"].query_latest.return_value = [
            {"event_ts_ms": 4000, "mark_price": "49000.00"}
        ]
        svc = _make_service(cache_client, cfg, repos, session_factory)
        result = svc.get_latest_mark_price("BTCUSDT")
        assert result["mark_price"] == "49000.00"
        repos["mark_price"].query_latest.assert_called_once_with(session_factory, "BTCUSDT", limit=1)
        # Cache backfill
        cache_client.set_json.assert_called_once()

    def test_no_data_anywhere(self, session_factory, cache_client, cfg, repos):
        cache_client.get_json.return_value = None
        repos["mark_price"].query_latest.return_value = []
        svc = _make_service(cache_client, cfg, repos, session_factory)
        result = svc.get_latest_mark_price("BTCUSDT")
        assert result is None


# ---------------------------------------------------------------------------
# index_price
# ---------------------------------------------------------------------------

class TestGetLatestIndexPrice:
    def test_cache_hit(self, session_factory, cache_client, cfg, repos):
        cache_client.get_json.return_value = {"event_ts_ms": 6000, "index_price": "49999.00"}
        svc = _make_service(cache_client, cfg, repos, session_factory)
        result = svc.get_latest_index_price("BTCUSDT")
        assert result["index_price"] == "49999.00"

    def test_mysql_fallback(self, session_factory, cache_client, cfg, repos):
        cache_client.get_json.return_value = None
        repos["index_price"].query_latest.return_value = [
            {"event_ts_ms": 5000, "index_price": "49998.00"}
        ]
        svc = _make_service(cache_client, cfg, repos, session_factory)
        result = svc.get_latest_index_price("BTCUSDT")
        assert result["index_price"] == "49998.00"


# ---------------------------------------------------------------------------
# open_interest
# ---------------------------------------------------------------------------

class TestGetLatestOpenInterest:
    def test_cache_hit(self, session_factory, cache_client, cfg, repos):
        cache_client.get_json.return_value = {"event_ts_ms": 7000, "open_interest": "1000.5"}
        svc = _make_service(cache_client, cfg, repos, session_factory)
        result = svc.get_latest_open_interest("BTCUSDT")
        assert result["open_interest"] == "1000.5"


# ---------------------------------------------------------------------------
# funding_rate
# ---------------------------------------------------------------------------

class TestGetLatestFundingRate:
    def test_cache_hit(self, session_factory, cache_client, cfg, repos):
        cache_client.get_json.return_value = {"funding_time_ts_ms": 8000, "funding_rate": "0.0001"}
        svc = _make_service(cache_client, cfg, repos, session_factory)
        result = svc.get_latest_funding_rate("BTCUSDT")
        assert result["funding_rate"] == "0.0001"

    def test_mysql_fallback(self, session_factory, cache_client, cfg, repos):
        cache_client.get_json.return_value = None
        repos["funding_rate"].query_latest.return_value = [
            {"funding_time_ts_ms": 7000, "funding_rate": "0.0002"}
        ]
        svc = _make_service(cache_client, cfg, repos, session_factory)
        result = svc.get_latest_funding_rate("BTCUSDT")
        assert result["funding_rate"] == "0.0002"


# ---------------------------------------------------------------------------
# depth
# ---------------------------------------------------------------------------

class TestGetLatestDepth:
    def test_cache_hit(self, session_factory, cache_client, cfg, repos):
        cache_client.get_json.return_value = {"event_ts_ms": 9000, "best_bid_price": "50000.00"}
        svc = _make_service(cache_client, cfg, repos, session_factory)
        result = svc.get_latest_depth("perp", "BTCUSDT")
        assert result["best_bid_price"] == "50000.00"

    def test_mysql_fallback(self, session_factory, cache_client, cfg, repos):
        cache_client.get_json.return_value = None
        repos["depth_snapshot"].query_latest.return_value = [
            {"event_ts_ms": 8000, "best_bid_price": "49900.00"}
        ]
        svc = _make_service(cache_client, cfg, repos, session_factory)
        result = svc.get_latest_depth("spot", "BTCUSDT")
        assert result["best_bid_price"] == "49900.00"
        repos["depth_snapshot"].query_latest.assert_called_once_with(
            session_factory, "spot_depth_snapshots", "BTCUSDT", limit=1
        )

    def test_no_cache_no_mysql(self, session_factory, cache_client, cfg, repos):
        cache_client.get_json.return_value = None
        repos["depth_snapshot"].query_latest.return_value = []
        svc = _make_service(cache_client, cfg, repos, session_factory)
        result = svc.get_latest_depth("perp", "BTCUSDT")
        assert result is None


# ---------------------------------------------------------------------------
# Config with cache disabled
# ---------------------------------------------------------------------------

class TestCacheDisabled:
    def test_goes_straight_to_mysql(self, session_factory, cache_client, repos):
        cfg = QuerySourceConfig(
            snapshot_cache_first_enabled=False,
            depth_cache_first_enabled=False,
            snapshot_mysql_fallback_enabled=True,
            depth_mysql_fallback_enabled=True,
            cache_backfill_on_fallback_enabled=False,
            allow_http_read_from_parquet=False,
        )
        repos["mark_price"].query_latest.return_value = [
            {"event_ts_ms": 3000, "mark_price": "48000.00"}
        ]
        svc = _make_service(cache_client, cfg, repos, session_factory)
        result = svc.get_latest_mark_price("BTCUSDT")
        assert result["mark_price"] == "48000.00"
        cache_client.get_json.assert_not_called()
