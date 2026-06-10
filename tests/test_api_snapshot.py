"""Tests for snapshot and depth query API endpoints (DC-T030)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from YM_data_collection.api.routes.snapshot import router


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_none_result():
    """Result shape when no data is found."""
    return {
        "data": None,
        "meta": {"source": "none", "fallback_used": False, "cache_refreshed": False},
    }


def _make_cache_result(data: dict):
    """Result shape for a cache hit."""
    return {
        "data": data,
        "meta": {"source": "cache", "fallback_used": False, "cache_refreshed": False},
    }


def _make_mysql_fallback_result(data: dict):
    """Result shape for a MySQL fallback."""
    return {
        "data": data,
        "meta": {"source": "mysql", "fallback_used": True, "cache_refreshed": False},
    }


def _make_cache_refreshed_result(data: dict):
    """Result shape for a cache hit with backfill refresh."""
    return {
        "data": data,
        "meta": {"source": "cache", "fallback_used": False, "cache_refreshed": True},
    }


def _create_app(mock_service: MagicMock) -> FastAPI:
    """Create a minimal FastAPI app with the snapshot router and injected mock."""
    app = FastAPI()
    app.state.query_service = mock_service
    app.include_router(router, prefix="/api/v1")
    return app


# ---------------------------------------------------------------------------
# Tests: GET /api/v1/marketdata/snapshot/latest
# ---------------------------------------------------------------------------

class TestSnapshotLatest:
    """Tests for the combined snapshot endpoint."""

    def test_combines_all_data_types(self):
        """Snapshot combines mark_price, index_price, open_interest, funding_rate, depth_snapshot."""
        svc = MagicMock()
        svc.query_latest_snapshot.side_effect = [
            _make_cache_result({"price": 42000.0}),
            _make_cache_result({"price": 41950.0}),
            _make_cache_result({"value": 12345.5}),
            _make_cache_result({"rate": 0.0001}),
        ]
        svc.query_latest_depth.return_value = _make_cache_result(
            {"bids": [[42000, 1.5]], "asks": [[42001, 2.0]]}
        )

        app = _create_app(svc)
        client = TestClient(app)

        resp = client.get(
            "/api/v1/marketdata/snapshot/latest",
            params={"venue": "binance", "market_type": "perp", "symbol": "BTCUSDT"},
        )
        assert resp.status_code == 200
        body = resp.json()

        assert body["success"] is True
        data = body["data"]
        assert data["mark_price"] == {"price": 42000.0}
        assert data["index_price"] == {"price": 41950.0}
        assert data["open_interest"] == {"value": 12345.5}
        assert data["funding_rate"] == {"rate": 0.0001}
        assert data["depth_snapshot"] == {"bids": [[42000, 1.5]], "asks": [[42001, 2.0]]}

        # Meta should reflect cache source since all are cache hits
        assert body["meta"]["source"] == "cache"
        assert body["meta"]["fallback_used"] is False

    def test_meta_reflects_fallback(self):
        """When some data types use MySQL fallback, meta reflects it."""
        svc = MagicMock()
        svc.query_latest_snapshot.side_effect = [
            _make_cache_result({"price": 42000.0}),       # mark_price: cache
            _make_mysql_fallback_result({"price": 41950}), # index_price: mysql fallback
            _make_cache_result({"value": 12345}),          # open_interest: cache
            _make_cache_result({"rate": 0.0001}),          # funding_rate: cache
        ]
        svc.query_latest_depth.return_value = _make_cache_result({"bids": [], "asks": []})

        app = _create_app(svc)
        client = TestClient(app)

        resp = client.get(
            "/api/v1/marketdata/snapshot/latest",
            params={"venue": "binance", "market_type": "perp", "symbol": "ETHUSDT"},
        )
        body = resp.json()
        assert body["meta"]["fallback_used"] is True

    def test_meta_reflects_cache_refreshed(self):
        """When a sub-query has cache_refreshed, combined meta reflects it."""
        svc = MagicMock()
        svc.query_latest_snapshot.side_effect = [
            _make_cache_result({"price": 100}),
            _make_cache_result({"price": 200}),
            _make_cache_refreshed_result({"value": 300}),
            _make_cache_result({"rate": 0.01}),
        ]
        svc.query_latest_depth.return_value = _make_cache_result({"bids": [], "asks": []})

        app = _create_app(svc)
        client = TestClient(app)

        resp = client.get(
            "/api/v1/marketdata/snapshot/latest",
            params={"venue": "binance", "market_type": "perp", "symbol": "BTCUSDT"},
        )
        body = resp.json()
        assert body["meta"]["cache_refreshed"] is True

    def test_all_none_returns_source_none(self):
        """When every data type returns None, meta.source is 'none'."""
        svc = MagicMock()
        svc.query_latest_snapshot.return_value = _make_none_result()
        svc.query_latest_depth.return_value = _make_none_result()

        app = _create_app(svc)
        client = TestClient(app)

        resp = client.get(
            "/api/v1/marketdata/snapshot/latest",
            params={"venue": "binance", "market_type": "perp", "symbol": "UNKNOWN"},
        )
        body = resp.json()
        assert body["success"] is True
        data = body["data"]
        for key in ("mark_price", "index_price", "open_interest", "funding_rate", "depth_snapshot"):
            assert data[key] is None
        assert body["meta"]["source"] == "none"
        assert body["meta"]["fallback_used"] is False

    def test_query_latest_snapshot_called_with_correct_params(self):
        """Verify query_latest_snapshot is called with correct market_type, symbol, data_type."""
        svc = MagicMock()
        svc.query_latest_snapshot.return_value = _make_none_result()
        svc.query_latest_depth.return_value = _make_none_result()

        app = _create_app(svc)
        client = TestClient(app)

        client.get(
            "/api/v1/marketdata/snapshot/latest",
            params={"venue": "binance", "market_type": "perp", "symbol": "BTCUSDT"},
        )

        calls = svc.query_latest_snapshot.call_args_list
        assert len(calls) == 4
        called_types = [c[0][2] for c in calls]
        assert called_types == ["mark_price", "index_price", "open_interest", "funding_rate"]
        for c in calls:
            assert c[0][0] == "perp"
            assert c[0][1] == "BTCUSDT"

    def test_query_latest_depth_called_with_correct_params(self):
        """Verify query_latest_depth is called with market_type and symbol."""
        svc = MagicMock()
        svc.query_latest_snapshot.return_value = _make_none_result()
        svc.query_latest_depth.return_value = _make_none_result()

        app = _create_app(svc)
        client = TestClient(app)

        client.get(
            "/api/v1/marketdata/snapshot/latest",
            params={"venue": "binance", "market_type": "perp", "symbol": "ETHUSDT"},
        )

        svc.query_latest_depth.assert_called_once_with("perp", "ETHUSDT")


# ---------------------------------------------------------------------------
# Tests: GET /api/v1/marketdata/depth/latest
# ---------------------------------------------------------------------------

class TestDepthLatest:
    """Tests for the depth snapshot endpoint."""

    def test_returns_depth_data_and_meta(self):
        """Depth endpoint returns data and meta from query_latest_depth."""
        svc = MagicMock()
        depth_data = {"bids": [[42000, 1.0], [41999, 2.0]], "asks": [[42001, 1.5], [42002, 0.5]]}
        svc.query_latest_depth.return_value = _make_cache_result(depth_data)

        app = _create_app(svc)
        client = TestClient(app)

        resp = client.get(
            "/api/v1/marketdata/depth/latest",
            params={"venue": "binance", "market_type": "perp", "symbol": "BTCUSDT"},
        )
        assert resp.status_code == 200
        body = resp.json()

        assert body["success"] is True
        assert body["data"] == depth_data
        assert body["meta"]["source"] == "cache"
        assert body["meta"]["fallback_used"] is False

    def test_returns_none_when_no_data(self):
        """Depth endpoint returns None data and source='none' when no data available."""
        svc = MagicMock()
        svc.query_latest_depth.return_value = _make_none_result()

        app = _create_app(svc)
        client = TestClient(app)

        resp = client.get(
            "/api/v1/marketdata/depth/latest",
            params={"venue": "binance", "market_type": "perp", "symbol": "UNKNOWN"},
        )
        body = resp.json()
        assert body["data"] is None
        assert body["meta"]["source"] == "none"

    def test_depth_with_mysql_fallback(self):
        """Depth endpoint returns mysql source when fallback is used."""
        svc = MagicMock()
        depth_data = {"bids": [[100, 1]], "asks": [[101, 1]]}
        svc.query_latest_depth.return_value = _make_mysql_fallback_result(depth_data)

        app = _create_app(svc)
        client = TestClient(app)

        resp = client.get(
            "/api/v1/marketdata/depth/latest",
            params={"venue": "binance", "market_type": "perp", "symbol": "BTCUSDT"},
        )
        body = resp.json()
        assert body["meta"]["source"] == "mysql"
        assert body["meta"]["fallback_used"] is True

    def test_levels_param_accepted(self):
        """The optional levels param is accepted without error (passed through)."""
        svc = MagicMock()
        svc.query_latest_depth.return_value = _make_cache_result({"bids": [], "asks": []})

        app = _create_app(svc)
        client = TestClient(app)

        resp = client.get(
            "/api/v1/marketdata/depth/latest",
            params={"venue": "binance", "market_type": "perp", "symbol": "BTCUSDT", "levels": 10},
        )
        assert resp.status_code == 200

    def test_query_latest_depth_called_with_correct_params(self):
        """Verify query_latest_depth is called with market_type and symbol."""
        svc = MagicMock()
        svc.query_latest_depth.return_value = _make_none_result()

        app = _create_app(svc)
        client = TestClient(app)

        client.get(
            "/api/v1/marketdata/depth/latest",
            params={"venue": "bybit", "market_type": "perp", "symbol": "ETHUSDT"},
        )

        svc.query_latest_depth.assert_called_once_with("perp", "ETHUSDT")


# ---------------------------------------------------------------------------
# Tests: Missing required query params
# ---------------------------------------------------------------------------

class TestMissingParams:
    """Ensure required query params are validated."""

    def test_snapshot_missing_symbol_returns_422(self):
        svc = MagicMock()
        app = _create_app(svc)
        client = TestClient(app)

        resp = client.get(
            "/api/v1/marketdata/snapshot/latest",
            params={"venue": "binance", "market_type": "perp"},
        )
        assert resp.status_code == 422

    def test_depth_missing_market_type_returns_422(self):
        svc = MagicMock()
        app = _create_app(svc)
        client = TestClient(app)

        resp = client.get(
            "/api/v1/marketdata/depth/latest",
            params={"venue": "binance", "symbol": "BTCUSDT"},
        )
        assert resp.status_code == 422
