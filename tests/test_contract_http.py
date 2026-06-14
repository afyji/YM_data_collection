"""HTTP contract tests: verify API response shapes match the OpenAPI schema contract.

Each test mocks service/repo dependencies to return plausible data, then asserts:
- HTTP 200
- Top-level envelope: success (bool), code (str), message (str)
- Data shape: required fields present with correct types
- Meta shape: required fields present with correct types
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from YM_data_collection.api.app import create_app
from YM_data_collection.config.models import DataCollectionConfig
from YM_data_collection.domain.models import (
    DataQualityIssue,
    FileManifest,
    IngestCheckpoint,
    InstrumentInfo,
)
from YM_data_collection.quality.health_checker import HealthStatus, SystemHealth


# ---------------------------------------------------------------------------
# Config helper
# ---------------------------------------------------------------------------


def _make_config() -> DataCollectionConfig:
    """Build a minimal DataCollectionConfig for testing."""
    return DataCollectionConfig(
        app={"app_name": "test", "env": "dev", "timezone": "UTC", "log_level": "INFO"},
        mysql={
            "host": "localhost",
            "port": 3306,
            "database": "test",
            "username": "root",
            "password_secret_ref": "secret",
            "pool_size": 5,
            "max_overflow": 2,
            "connect_timeout_seconds": 5,
            "read_timeout_seconds": 5,
            "write_timeout_seconds": 5,
        },
        cache={
            "enabled": False,
            "backend": "redis",
            "host": "localhost",
            "port": 6379,
            "password_secret_ref": "secret",
            "db": 0,
            "ttl_seconds": 60,
        },
        auth={
            "enabled": False,
            "http_token_secret_ref": "secret",
            "ws_token_secret_ref": "secret",
            "internal_service_token_secret_ref": "secret",
        },
        binance={},
        ingestion={
            "historical_batch_size": 100,
            "realtime_enabled": False,
            "checkpoint_enabled": False,
            "raw_trace_enabled": False,
        },
        validation={
            "kline_boundary_validation_enabled": False,
            "kline_auto_repair_enabled": False,
            "trade_quote_quantity_tolerance_bps": 0,
            "mark_index_deviation_warning_bps": 0,
            "depth_order_validation_enabled": False,
            "quality_record_enabled": False,
        },
        depth={
            "default_depth_levels": 20,
            "freshness_threshold_ms": 1000,
            "max_snapshot_age_ms": 60000,
        },
        slippage={
            "slippage_estimation_enabled": False,
            "default_slippage_depth_levels": 10,
            "max_slippage_depth_levels": 50,
            "reference_price_mode": "mid_price",
            "insufficient_depth_policy": "reject",
            "allow_partial_fill_estimation": False,
        },
        service={
            "http_enabled": True,
            "http_host": "0.0.0.0",
            "http_port": 8000,
            "ws_enabled": False,
            "ws_host": "0.0.0.0",
            "ws_port": 8001,
            "default_page_count": 20,
            "max_page_count": 100,
            "download_enabled": False,
            "http_read_timeout_seconds": 30,
            "http_write_timeout_seconds": 30,
            "http_keepalive_enabled": True,
            "request_id_enabled": False,
            "api_docs_enabled": False,
        },
        websocket={
            "heartbeat_enabled": False,
            "heartbeat_interval_seconds": 30,
            "pong_timeout_seconds": 60,
            "client_idle_timeout_seconds": 300,
            "max_subscriptions_per_connection": 10,
            "max_connections": 5,
            "send_queue_size": 100,
            "message_max_bytes": 1048576,
            "snapshot_push_enabled": False,
            "quality_event_push_enabled": False,
            "stream_status_push_enabled": False,
        },
        query_source={
            "snapshot_cache_first_enabled": False,
            "depth_cache_first_enabled": False,
            "snapshot_mysql_fallback_enabled": False,
            "depth_mysql_fallback_enabled": False,
            "cache_backfill_on_fallback_enabled": False,
            "allow_http_read_from_parquet": False,
        },
        window={
            "default_recent_kline_count": 20,
            "max_recent_kline_count": 1000,
            "http_window_refill_enabled": False,
            "http_window_refill_limit": 100,
        },
        export={
            "enabled": False,
            "base_dir": "/tmp",
            "default_format": "parquet",
            "compression": "snappy",
            "partition_rule": "date",
            "manifest_write_enabled": False,
            "overwrite_same_version_enabled": False,
        },
        download={
            "download_enabled": False,
            "download_token_required": False,
            "download_url_expire_seconds": 300,
            "max_download_file_size_mb": 100,
            "download_audit_enabled": False,
        },
        quality={
            "enabled": False,
            "warning_thresholds": {},
            "error_thresholds": {},
            "email_alert_enabled": False,
            "email_recipients": [],
        },
    )


# ---------------------------------------------------------------------------
# Domain model factories
# ---------------------------------------------------------------------------


def _make_instrument(
    venue: str = "binance",
    market_type: str = "spot",
    symbol: str = "BTCUSDT",
) -> InstrumentInfo:
    return InstrumentInfo(
        venue=venue,
        market_type=market_type,
        symbol=symbol,
        base_asset="BTC",
        quote_asset="USDT",
        instrument_code="BTCUSDT",
        is_active=True,
        tick_size=Decimal("0.01"),
        step_size=Decimal("0.000001"),
        min_qty=Decimal("0.000001"),
        min_notional=Decimal("10"),
        contract_type=None,
    )


def _make_checkpoint() -> IngestCheckpoint:
    return IngestCheckpoint(
        venue="binance",
        market_type="spot",
        symbol="BTCUSDT",
        data_type="kline",
        interval_code="1h",
        last_event_ts_ms=1700000000000,
        last_event_dt_utc=datetime(2023, 11, 14, 22, 13, 20, tzinfo=timezone.utc),
        last_trade_id=None,
        last_kline_open_ts_ms=1700000000000,
        status="ok",
        last_success_at_utc=datetime(2023, 11, 14, 22, 13, 20, tzinfo=timezone.utc),
        last_error_message=None,
    )


def _make_quality_issue(symbol: str = "BTCUSDT") -> DataQualityIssue:
    return DataQualityIssue(
        venue="binance",
        market_type="spot",
        symbol=symbol,
        data_type="kline",
        interval_code="1h",
        issue_type="gap",
        severity="warning",
        detected_at_utc=datetime(2023, 11, 14, 22, 0, 0, tzinfo=timezone.utc),
        start_ts_ms=1700000000000,
        end_ts_ms=1700003600000,
        description="Gap detected",
        status="open",
        resolution_note=None,
    )


def _make_manifest(symbol: str = "BTCUSDT", id: int | None = 1) -> FileManifest:
    return FileManifest(
        id=id,
        dataset_name="kline_spot",
        venue="binance",
        market_type="spot",
        symbol=symbol,
        data_type="kline",
        interval_code="1h",
        time_boundary_rule="daily",
        file_format="parquet",
        file_path="/data/kline_spot/BTCUSDT/20231114.parquet",
        partition_key="20231114",
        start_ts_ms=1700000000000,
        end_ts_ms=1700086400000,
        row_count=24,
        file_size_bytes=1024,
        content_hash="abc123",
        version=1,
        generated_by="historical_sync",
        generated_at_utc=datetime(2023, 11, 14, 23, 0, 0, tzinfo=timezone.utc),
        status="ready",
    )


# ---------------------------------------------------------------------------
# Mock service / repo factories
# ---------------------------------------------------------------------------


def _make_query_service() -> MagicMock:
    """Mock MarketDataQueryService with plausible kline + snapshot + depth data."""
    svc = MagicMock()

    # Kline data — prices as strings per contract
    svc.query_klines_recent.return_value = {
        "data": [
            {
                "open_ts_ms": 1700000000000,
                "close_ts_ms": 1700003600000,
                "open": "42000.00",
                "high": "42100.00",
                "low": "41900.00",
                "close": "42050.00",
                "volume": "123.45",
            },
        ],
        "meta": {"source": "mysql", "fallback_used": False, "cache_refreshed": False},
    }

    svc.query_klines_range.return_value = {
        "data": [
            {
                "open_ts_ms": i * 3600000,
                "close_ts_ms": (i + 1) * 3600000,
                "open": "42000.00",
                "close": "42050.00",
            }
            for i in range(3)
        ],
        "meta": {"source": "mysql", "fallback_used": False, "cache_refreshed": False},
    }

    # Snapshot data — return different shapes based on data_type for individual endpoints
    _snapshot_results = {
        "mark_price": {"data": {"price": "42000.50"}, "meta": {"source": "cache", "fallback_used": False, "cache_refreshed": False}},
        "index_price": {"data": {"price": "41950.25"}, "meta": {"source": "cache", "fallback_used": False, "cache_refreshed": False}},
        "open_interest": {"data": {"value": "12345.5"}, "meta": {"source": "cache", "fallback_used": False, "cache_refreshed": False}},
        "funding_rate": {"data": {"rate": "0.0001"}, "meta": {"source": "cache", "fallback_used": False, "cache_refreshed": False}},
    }

    def _snapshot_side_effect(market_type, symbol, data_type):
        return _snapshot_results.get(
            data_type,
            {"data": {"price": "42000.50"}, "meta": {"source": "cache", "fallback_used": False, "cache_refreshed": False}},
        )

    svc.query_latest_snapshot.side_effect = _snapshot_side_effect

    # Depth data — DepthLevel is [string, string] per contract
    svc.query_latest_depth.return_value = {
        "data": {
            "bids": [["42000.00", "1.500"], ["41999.00", "2.000"]],
            "asks": [["42001.00", "1.000"], ["42002.00", "0.500"]],
        },
        "meta": {"source": "cache", "fallback_used": False, "cache_refreshed": False},
    }

    return svc


def _make_slippage_service() -> MagicMock:
    """Mock SlippageService with plausible estimation result."""
    svc = MagicMock()
    svc.estimate.return_value = {
        "reference_price": Decimal("50000.00"),
        "estimated_avg_fill_price": Decimal("50001.50"),
        "slippage_abs": Decimal("1.50"),
        "slippage_bps": Decimal("0.30"),
        "filled_qty": Decimal("0.01999994"),
        "unfilled_qty": Decimal("0"),
        "depth_levels_used": 3,
        "depth_event_ts_ms": 1700000000000,
        "meta": {"filled_levels": 3, "age_ms": 120},
    }
    return svc


def _make_instrument_repo() -> MagicMock:
    repo = MagicMock()
    repo.list_active.return_value = [_make_instrument(symbol="BTCUSDT")]
    repo.list_all.return_value = [_make_instrument(symbol="BTCUSDT")]
    return repo


def _make_coverage_service() -> MagicMock:
    svc = MagicMock()
    svc.get_coverage.return_value = {
        "venue": "binance",
        "market_type": "spot",
        "symbol": "BTCUSDT",
        "data_type": "kline",
        "interval_code": "1h",
        "start_ts_ms": 1700000000000,
        "end_ts_ms": 1700086400000,
        "bar_count": 24,
    }
    return svc


def _make_checkpoint_repo() -> MagicMock:
    repo = MagicMock()
    repo.get.return_value = _make_checkpoint()
    return repo


def _make_quality_repo() -> MagicMock:
    repo = MagicMock()
    repo.list_by_symbol.return_value = [_make_quality_issue("BTCUSDT")]
    repo.list_by_status.return_value = [_make_quality_issue("BTCUSDT")]
    return repo


def _make_manifest_repo() -> MagicMock:
    repo = MagicMock()
    manifest = _make_manifest("BTCUSDT")
    repo.list_by_dataset.return_value = [manifest]
    repo.list_by_symbol.return_value = [manifest]
    # get_by_id returns the manifest for id=1; None for unknown ids
    def _get_by_id(manifest_id):
        if manifest_id == 1:
            return manifest
        return None
    repo.get_by_id.side_effect = _get_by_id
    return repo


def _make_health_checker() -> MagicMock:
    checker = MagicMock()
    checker.run_all.return_value = SystemHealth(
        overall_healthy=True,
        statuses=[
            HealthStatus(component="mysql", healthy=True, latency_ms=2.3, detail="SELECT 1 ok"),
            HealthStatus(component="redis", healthy=True, latency_ms=1.1, detail="ping()=True"),
        ],
        checked_at_utc=datetime.now(timezone.utc).isoformat(),
    )
    return checker


# ---------------------------------------------------------------------------
# Client fixture
# ---------------------------------------------------------------------------


@pytest.fixture()
def client():
    config = _make_config()
    query_service = _make_query_service()
    slippage_service = _make_slippage_service()
    instrument_repo = _make_instrument_repo()
    coverage_service = _make_coverage_service()
    checkpoint_repo = _make_checkpoint_repo()
    quality_repo = _make_quality_repo()
    manifest_repo = _make_manifest_repo()
    health_checker = _make_health_checker()

    app = create_app(
        config,
        query_service=query_service,
        slippage_service=slippage_service,
        instrument_repo=instrument_repo,
        coverage_service=coverage_service,
        checkpoint_repo=checkpoint_repo,
        quality_repo=quality_repo,
        manifest_repo=manifest_repo,
        health_checker=health_checker,
    )

    tc = TestClient(app)
    return tc


# ---------------------------------------------------------------------------
# Contract assertion helpers
# ---------------------------------------------------------------------------


def _assert_envelope(body: dict) -> None:
    """Assert top-level envelope keys exist with correct types."""
    assert "success" in body
    assert isinstance(body["success"], bool)
    assert "code" in body
    assert isinstance(body["code"], str)
    assert "message" in body
    assert isinstance(body["message"], str)


# ===========================================================================
# 1. GET /api/v1/marketdata/klines/recent
# ===========================================================================


class TestContractKlinesRecent:
    """Contract: success, code, message, data.items(list), meta.count, meta.source."""

    def test_envelope_and_shape(self, client):
        resp = client.get(
            "/api/v1/marketdata/klines/recent",
            params={
                "venue": "binance",
                "market_type": "spot",
                "symbol": "BTCUSDT",
                "interval": "1h",
                "count": 1,
            },
        )
        assert resp.status_code == 200
        body = resp.json()

        _assert_envelope(body)
        assert body["success"] is True

        # data.items is a list
        assert "items" in body["data"]
        assert isinstance(body["data"]["items"], list)

        # meta.count and meta.source
        assert "count" in body["meta"]
        assert isinstance(body["meta"]["count"], int)
        assert "source" in body["meta"]
        assert isinstance(body["meta"]["source"], str)


# ===========================================================================
# 2. GET /api/v1/marketdata/klines/range
# ===========================================================================


class TestContractKlinesRange:
    """Contract: same as recent + meta.page, meta.has_next, meta.total."""

    def test_envelope_and_shape(self, client):
        resp = client.get(
            "/api/v1/marketdata/klines/range",
            params={
                "venue": "binance",
                "market_type": "spot",
                "symbol": "BTCUSDT",
                "interval": "1h",
                "start_ts_ms": 0,
                "end_ts_ms": 1700003600000,
            },
        )
        assert resp.status_code == 200
        body = resp.json()

        _assert_envelope(body)
        assert body["success"] is True

        # data.items is a list
        assert "items" in body["data"]
        assert isinstance(body["data"]["items"], list)

        # meta pagination fields
        meta = body["meta"]
        assert "count" in meta and isinstance(meta["count"], int)
        assert "source" in meta and isinstance(meta["source"], str)
        assert "page" in meta and isinstance(meta["page"], int)
        assert "has_next" in meta and isinstance(meta["has_next"], bool)
        assert "total" in meta and isinstance(meta["total"], int)


# ===========================================================================
# 3. GET /api/v1/marketdata/snapshot/latest
# ===========================================================================


class TestContractSnapshotLatest:
    """Contract: data has symbol, market_type + optional mark_price / index_price /
    open_interest / funding_rate / depth_snapshot."""

    def test_envelope_and_shape(self, client):
        resp = client.get(
            "/api/v1/marketdata/snapshot/latest",
            params={
                "venue": "binance",
                "market_type": "perp",
                "symbol": "BTCUSDT",
            },
        )
        assert resp.status_code == 200
        body = resp.json()

        _assert_envelope(body)
        assert body["success"] is True

        data = body["data"]
        # Combined snapshot keys
        assert "mark_price" in data
        assert "index_price" in data
        assert "open_interest" in data
        assert "funding_rate" in data
        assert "depth_snapshot" in data

        # depth_snapshot is either None or an object
        if data["depth_snapshot"] is not None:
            assert isinstance(data["depth_snapshot"], dict)

        # meta has source
        assert "source" in body["meta"]


# ===========================================================================
# 4. GET /api/v1/marketdata/depth/latest
# ===========================================================================


class TestContractDepthLatest:
    """Contract: data is a depth snapshot object with bids/asks."""

    def test_envelope_and_shape(self, client):
        resp = client.get(
            "/api/v1/marketdata/depth/latest",
            params={
                "venue": "binance",
                "market_type": "perp",
                "symbol": "BTCUSDT",
            },
        )
        assert resp.status_code == 200
        body = resp.json()

        _assert_envelope(body)
        assert body["success"] is True

        data = body["data"]
        # Depth snapshot object
        assert isinstance(data, dict)
        assert "bids" in data
        assert "asks" in data
        assert isinstance(data["bids"], list)
        assert isinstance(data["asks"], list)

        # DepthLevel is [string, string]
        if data["bids"]:
            level = data["bids"][0]
            assert isinstance(level, list)
            assert len(level) == 2
            assert isinstance(level[0], str)
            assert isinstance(level[1], str)

        # meta has source
        assert "source" in body["meta"]


# ===========================================================================
# 5. GET /api/v1/marketdata/slippage/estimate
# ===========================================================================


class TestContractSlippageEstimate:
    """Contract: data must have reference_price, estimated_avg_fill_price,
    slippage_abs, slippage_bps, depth_levels_used, depth_event_ts_ms.
    All price fields are strings (not floats)."""

    def test_envelope_and_shape(self, client):
        resp = client.get(
            "/api/v1/marketdata/slippage/estimate",
            params={
                "venue": "binance",
                "market_type": "spot",
                "symbol": "BTCUSDT",
                "side": "buy",
                "quote_asset_amount": "1000",
            },
        )
        assert resp.status_code == 200
        body = resp.json()

        _assert_envelope(body)
        assert body["success"] is True

        data = body["data"]

        # Required price fields — must be strings per contract
        assert "reference_price" in data
        assert isinstance(data["reference_price"], str)

        assert "estimated_avg_fill_price" in data
        assert isinstance(data["estimated_avg_fill_price"], str)

        assert "slippage_abs" in data
        assert isinstance(data["slippage_abs"], str)

        assert "slippage_bps" in data
        assert isinstance(data["slippage_bps"], str)

        # Contract fields
        assert "depth_levels_used" in data
        assert isinstance(data["depth_levels_used"], int)

        assert "depth_event_ts_ms" in data
        assert isinstance(data["depth_event_ts_ms"], int)


# ===========================================================================
# 6. GET /api/v1/metadata/instruments
# ===========================================================================


class TestContractInstruments:
    """Contract: data.items list, meta.page (or meta.count), meta.source."""

    def test_envelope_and_shape(self, client):
        resp = client.get("/api/v1/metadata/instruments")
        assert resp.status_code == 200
        body = resp.json()

        _assert_envelope(body)
        assert body["success"] is True

        # data.items is a list
        assert "items" in body["data"]
        assert isinstance(body["data"]["items"], list)

        # meta has count and source
        meta = body["meta"]
        assert "count" in meta and isinstance(meta["count"], int)
        assert "source" in meta and isinstance(meta["source"], str)

        # Verify instrument item shape
        if body["data"]["items"]:
            item = body["data"]["items"][0]
            assert "venue" in item
            assert "market_type" in item
            assert "symbol" in item
            # Price fields are strings
            assert isinstance(item["tick_size"], str)
            assert isinstance(item["step_size"], str)


# ===========================================================================
# 7. GET /api/v1/metadata/coverage
# ===========================================================================


class TestContractCoverage:
    """Contract: success + data object with coverage info."""

    def test_envelope_and_shape(self, client):
        resp = client.get(
            "/api/v1/metadata/coverage",
            params={
                "venue": "binance",
                "market_type": "spot",
                "symbol": "BTCUSDT",
                "data_type": "kline",
            },
        )
        assert resp.status_code == 200
        body = resp.json()

        _assert_envelope(body)
        assert body["success"] is True

        data = body["data"]
        assert isinstance(data, dict)
        assert "venue" in data
        assert "market_type" in data
        assert "symbol" in data
        assert "data_type" in data

        # meta has source
        assert "source" in body["meta"]


# ===========================================================================
# 8. GET /api/v1/metadata/status
# ===========================================================================


class TestContractStatus:
    """Contract: success + flattened data with identity, status, and checkpoint."""

    def test_envelope_and_shape(self, client):
        resp = client.get(
            "/api/v1/metadata/status",
            params={
                "venue": "binance",
                "market_type": "spot",
                "symbol": "BTCUSDT",
                "data_type": "kline",
            },
        )
        assert resp.status_code == 200
        body = resp.json()

        _assert_envelope(body)
        assert body["success"] is True

        data = body["data"]
        assert isinstance(data, dict)

        # Top-level identity and status fields
        assert data["venue"] == "binance"
        assert data["market_type"] == "spot"
        assert data["symbol"] == "BTCUSDT"
        assert data["data_type"] == "kline"
        assert "status" in data
        assert "last_success_at_utc" in data
        assert "last_error_message" in data

        # Checkpoint sub-object has remaining fields
        assert "checkpoint" in data
        cp = data["checkpoint"]
        assert "last_event_ts_ms" in cp

        # meta has source
        assert "source" in body["meta"]


# ===========================================================================
# 9. GET /api/v1/metadata/quality-issues
# ===========================================================================


class TestContractQualityIssues:
    """Contract: data.items list, meta with pagination."""

    def test_envelope_and_shape(self, client):
        resp = client.get(
            "/api/v1/metadata/quality-issues",
            params={"symbol": "BTCUSDT"},
        )
        assert resp.status_code == 200
        body = resp.json()

        _assert_envelope(body)
        assert body["success"] is True

        # data.items is a list
        assert "items" in body["data"]
        assert isinstance(body["data"]["items"], list)

        # meta has pagination and source
        meta = body["meta"]
        assert "page" in meta and isinstance(meta["page"], int)
        assert "count" in meta and isinstance(meta["count"], int)
        assert "total" in meta and isinstance(meta["total"], int)
        assert "source" in meta and isinstance(meta["source"], str)

        # Verify quality issue item shape
        if body["data"]["items"]:
            item = body["data"]["items"][0]
            assert "venue" in item
            assert "symbol" in item
            assert "data_type" in item
            assert "issue_type" in item
            assert "severity" in item
            assert "status" in item


# ===========================================================================
# 10. GET /api/v1/datasets/manifests
# ===========================================================================


class TestContractManifests:
    """Contract: data.items list, meta with pagination."""

    def test_envelope_and_shape(self, client):
        resp = client.get(
            "/api/v1/datasets/manifests",
            params={"dataset_name": "kline_spot"},
        )
        assert resp.status_code == 200
        body = resp.json()

        _assert_envelope(body)
        assert body["success"] is True

        # data.items is a list
        assert "items" in body["data"]
        assert isinstance(body["data"]["items"], list)

        # meta has pagination and source
        meta = body["meta"]
        assert "page" in meta and isinstance(meta["page"], int)
        assert "count" in meta and isinstance(meta["count"], int)
        assert "total" in meta and isinstance(meta["total"], int)
        assert "source" in meta and isinstance(meta["source"], str)

        # Verify manifest item shape
        if body["data"]["items"]:
            item = body["data"]["items"][0]
            assert "manifest_id" in item
            assert "dataset_name" in item
            assert "venue" in item
            assert "symbol" in item
            assert "file_path" in item
            assert "status" in item


# ===========================================================================
# 11. GET /api/v1/system/health
# ===========================================================================


class TestContractSystemHealth:
    """Contract: success + data with components list and overall_healthy."""

    def test_envelope_and_shape(self, client):
        resp = client.get("/api/v1/system/health")
        assert resp.status_code == 200
        body = resp.json()

        _assert_envelope(body)
        assert body["success"] is True

        data = body["data"]
        assert isinstance(data, dict)
        assert "overall_healthy" in data
        assert isinstance(data["overall_healthy"], bool)
        assert "components" in data
        assert isinstance(data["components"], list)
        assert "checked_at_utc" in data

        # Component shape
        if data["components"]:
            comp = data["components"][0]
            assert "component" in comp
            assert "healthy" in comp
            assert isinstance(comp["healthy"], bool)


# ===========================================================================
# 12. GET /api/v1/system/runtime-status
# ===========================================================================


class TestContractRuntimeStatus:
    """Contract: success + data with uptime_s, version."""

    def test_envelope_and_shape(self, client):
        resp = client.get("/api/v1/system/runtime-status")
        assert resp.status_code == 200
        body = resp.json()

        _assert_envelope(body)
        assert body["success"] is True

        data = body["data"]
        assert isinstance(data, dict)

        # uptime_seconds (contract says uptime_s but code uses uptime_seconds)
        assert "uptime_seconds" in data
        assert isinstance(data["uptime_seconds"], (int, float))
        assert data["uptime_seconds"] >= 0

        # version
        assert "version" in data
        assert isinstance(data["version"], str)

        # ws_connections
        assert "ws_connections" in data
        assert isinstance(data["ws_connections"], int)


# ===========================================================================
# DC-T065: Contract smoke tests for 4 new latest endpoints (6-9)
# ===========================================================================


class TestContractMarkPriceLatest:
    """Contract: GET /api/v1/marketdata/mark-price/latest returns envelope with data + meta."""

    def test_envelope_and_shape(self, client):
        resp = client.get(
            "/api/v1/marketdata/mark-price/latest",
            params={
                "venue": "binance",
                "market_type": "perp",
                "symbol": "BTCUSDT",
            },
        )
        assert resp.status_code == 200
        body = resp.json()

        _assert_envelope(body)
        assert body["success"] is True

        # data is a dict with price key (or null)
        data = body["data"]
        assert isinstance(data, dict)
        assert "price" in data

        # meta has source
        assert "source" in body["meta"]


class TestContractIndexPriceLatest:
    """Contract: GET /api/v1/marketdata/index-price/latest returns envelope with data + meta."""

    def test_envelope_and_shape(self, client):
        resp = client.get(
            "/api/v1/marketdata/index-price/latest",
            params={
                "venue": "binance",
                "market_type": "perp",
                "symbol": "BTCUSDT",
            },
        )
        assert resp.status_code == 200
        body = resp.json()

        _assert_envelope(body)
        assert body["success"] is True

        data = body["data"]
        assert isinstance(data, dict)
        assert "price" in data

        assert "source" in body["meta"]


class TestContractOpenInterestLatest:
    """Contract: GET /api/v1/marketdata/open-interest/latest returns envelope with data + meta."""

    def test_envelope_and_shape(self, client):
        resp = client.get(
            "/api/v1/marketdata/open-interest/latest",
            params={
                "venue": "binance",
                "market_type": "perp",
                "symbol": "BTCUSDT",
            },
        )
        assert resp.status_code == 200
        body = resp.json()

        _assert_envelope(body)
        assert body["success"] is True

        data = body["data"]
        assert isinstance(data, dict)
        # open_interest uses 'value' key
        assert "value" in data

        assert "source" in body["meta"]


class TestContractFundingRateLatest:
    """Contract: GET /api/v1/marketdata/funding-rate/latest returns envelope with data + meta."""

    def test_envelope_and_shape(self, client):
        resp = client.get(
            "/api/v1/marketdata/funding-rate/latest",
            params={
                "venue": "binance",
                "market_type": "perp",
                "symbol": "BTCUSDT",
            },
        )
        assert resp.status_code == 200
        body = resp.json()

        _assert_envelope(body)
        assert body["success"] is True

        data = body["data"]
        assert isinstance(data, dict)
        # funding_rate uses 'rate' key
        assert "rate" in data

        assert "source" in body["meta"]


# ===========================================================================
# DC-T065: Contract smoke tests for datasets/manifests/detail (17)
# ===========================================================================


class TestContractManifestsDetail:
    """Contract: GET /api/v1/datasets/manifests/detail returns envelope with data + meta."""

    def test_envelope_and_shape(self, client):
        resp = client.get(
            "/api/v1/datasets/manifests/detail",
            params={"manifest_id": 1},
        )
        assert resp.status_code == 200
        body = resp.json()

        _assert_envelope(body)
        assert body["success"] is True

        data = body["data"]
        assert isinstance(data, dict)
        assert "manifest_id" in data
        assert "dataset_name" in data
        assert "symbol" in data
        assert "status" in data

        assert "source" in body["meta"]


# ===========================================================================
# DC-T065: Contract smoke test for datasets/download (18)
# ===========================================================================


class TestContractDatasetsDownload:
    """Contract: GET /api/v1/datasets/download returns binary file or error JSON.

    Since the mock manifest has a file_path that does not exist on disk,
    the endpoint returns a JSON error envelope (file not found on disk).
    This is sufficient to verify the route is registered and responds correctly.
    """

    def test_manifest_not_found_returns_error_envelope(self, client):
        resp = client.get(
            "/api/v1/datasets/download",
            params={"manifest_id": 999},
        )
        assert resp.status_code == 404
        body = resp.json()
        assert body["success"] is False
        assert body["code"] == "NOT_FOUND"

    def test_manifest_found_but_file_missing_returns_error_envelope(self, client):
        """Manifest exists in repo, but the file_path does not exist on disk -> 404 JSON."""
        resp = client.get(
            "/api/v1/datasets/download",
            params={"manifest_id": 1},
        )
        assert resp.status_code == 404
        body = resp.json()
        assert body["success"] is False
        assert body["code"] == "NOT_FOUND"


# ===========================================================================
# DC-T065: Authoritative first-phase HTTP endpoint list & coverage matrix
# ===========================================================================

FIRST_PHASE_HTTP_ENDPOINTS: list[tuple[str, str]] = [
    # (method, path) — authoritative list per 19_第一阶段正式API补完与契约对齐.md
    ("GET", "/api/v1/marketdata/klines/recent"),
    ("GET", "/api/v1/marketdata/klines/range"),
    ("GET", "/api/v1/marketdata/snapshot/latest"),
    ("GET", "/api/v1/marketdata/depth/latest"),
    ("GET", "/api/v1/marketdata/slippage/estimate"),
    ("GET", "/api/v1/marketdata/mark-price/latest"),
    ("GET", "/api/v1/marketdata/index-price/latest"),
    ("GET", "/api/v1/marketdata/open-interest/latest"),
    ("GET", "/api/v1/marketdata/funding-rate/latest"),
    ("GET", "/api/v1/metadata/instruments"),
    ("GET", "/api/v1/metadata/coverage"),
    ("GET", "/api/v1/metadata/status"),
    ("GET", "/api/v1/metadata/quality-issues"),
    ("GET", "/api/v1/system/health"),
    ("GET", "/api/v1/system/runtime-status"),
    ("GET", "/api/v1/datasets/manifests"),
    ("GET", "/api/v1/datasets/manifests/detail"),
    ("GET", "/api/v1/datasets/download"),
]


def _collect_app_routes(app) -> set[tuple[str, str]]:
    """Collect (method, path) from a FastAPI app's routes."""
    routes: set[tuple[str, str]] = set()
    for route in app.routes:
        # APIRoute has methods; Mount does not
        if hasattr(route, "methods") and hasattr(route, "path"):
            for method in route.methods:
                if method in ("GET", "POST", "PUT", "DELETE", "PATCH"):
                    routes.add((method, route.path))
        # Recurse into mounts
        if hasattr(route, "routes"):
            routes |= _collect_app_routes_from_list(route.routes)
    return routes


def _collect_app_routes_from_list(route_list) -> set[tuple[str, str]]:
    """Collect (method, path) from a list of route objects."""
    routes: set[tuple[str, str]] = set()
    for route in route_list:
        if hasattr(route, "methods") and hasattr(route, "path"):
            for method in route.methods:
                if method in ("GET", "POST", "PUT", "DELETE", "PATCH"):
                    routes.add((method, route.path))
        if hasattr(route, "routes"):
            routes |= _collect_app_routes_from_list(route.routes)
    return routes


class TestCoverageMatrixRouteRegistration:
    """Every endpoint in FIRST_PHASE_HTTP_ENDPOINTS must be registered in the FastAPI app."""

    def test_all_formal_endpoints_registered_in_app(self, client):
        app = client.app  # type: ignore[attr-defined]
        registered = _collect_app_routes(app)

        missing = []
        for method, path in FIRST_PHASE_HTTP_ENDPOINTS:
            if (method, path) not in registered:
                missing.append(f"{method} {path}")

        assert not missing, (
            f"Missing routes in FastAPI app: {missing}\n"
            f"Registered: {sorted(registered)}"
        )


class TestCoverageMatrixOpenAPI:
    """Every endpoint in FIRST_PHASE_HTTP_ENDPOINTS must exist in the OpenAPI draft."""

    def test_all_formal_endpoints_in_openapi_draft(self):
        import yaml
        from pathlib import Path

        openapi_path = Path(
            "/mnt/mac_quant_system/docs/01_data_collection_system/17_http_openapi_draft.yaml"
        )
        with open(openapi_path) as f:
            spec = yaml.safe_load(f)

        paths = spec.get("paths", {})

        missing = []
        for method, path in FIRST_PHASE_HTTP_ENDPOINTS:
            if path not in paths:
                missing.append(f"{method} {path}")
            else:
                path_item = paths[path]
                if method.lower() not in path_item:
                    missing.append(f"{method} {path} (method missing)")

        assert not missing, (
            f"Missing endpoints in OpenAPI draft: {missing}\n"
            f"Draft paths: {sorted(paths.keys())}"
        )


# Mapping: formal endpoint -> contract test class name in this file
_CONTRACT_TEST_MAP: dict[str, str] = {
    "GET /api/v1/marketdata/klines/recent": "TestContractKlinesRecent",
    "GET /api/v1/marketdata/klines/range": "TestContractKlinesRange",
    "GET /api/v1/marketdata/snapshot/latest": "TestContractSnapshotLatest",
    "GET /api/v1/marketdata/depth/latest": "TestContractDepthLatest",
    "GET /api/v1/marketdata/slippage/estimate": "TestContractSlippageEstimate",
    "GET /api/v1/marketdata/mark-price/latest": "TestContractMarkPriceLatest",
    "GET /api/v1/marketdata/index-price/latest": "TestContractIndexPriceLatest",
    "GET /api/v1/marketdata/open-interest/latest": "TestContractOpenInterestLatest",
    "GET /api/v1/marketdata/funding-rate/latest": "TestContractFundingRateLatest",
    "GET /api/v1/metadata/instruments": "TestContractInstruments",
    "GET /api/v1/metadata/coverage": "TestContractCoverage",
    "GET /api/v1/metadata/status": "TestContractStatus",
    "GET /api/v1/metadata/quality-issues": "TestContractQualityIssues",
    "GET /api/v1/system/health": "TestContractSystemHealth",
    "GET /api/v1/system/runtime-status": "TestContractRuntimeStatus",
    "GET /api/v1/datasets/manifests": "TestContractManifests",
    "GET /api/v1/datasets/manifests/detail": "TestContractManifestsDetail",
    "GET /api/v1/datasets/download": "TestContractDatasetsDownload",
}


class TestCoverageMatrixContractTests:
    """No formal endpoint may be silently missing from the contract test matrix."""

    def test_every_formal_endpoint_has_contract_test_entry(self):
        """Every endpoint in FIRST_PHASE_HTTP_ENDPOINTS must have a _CONTRACT_TEST_MAP entry."""
        formal_keys = {f"{m} {p}" for m, p in FIRST_PHASE_HTTP_ENDPOINTS}
        covered_keys = set(_CONTRACT_TEST_MAP.keys())
        missing = formal_keys - covered_keys
        assert not missing, (
            f"Endpoints missing from _CONTRACT_TEST_MAP: {sorted(missing)}\n"
            f"Add a contract test class and register it in _CONTRACT_TEST_MAP."
        )

    def test_contract_test_classes_exist_in_module(self):
        """Every class referenced in _CONTRACT_TEST_MAP must exist in this module."""
        import sys

        module = sys.modules[__name__]
        for key, class_name in _CONTRACT_TEST_MAP.items():
            assert hasattr(module, class_name), (
                f"Contract test class {class_name} for {key} not found in {__name__}"
            )

    def test_no_stale_entries_in_contract_test_map(self):
        """_CONTRACT_TEST_MAP should not reference endpoints not in the formal list."""
        formal_keys = {f"{m} {p}" for m, p in FIRST_PHASE_HTTP_ENDPOINTS}
        stale = set(_CONTRACT_TEST_MAP.keys()) - formal_keys
        assert not stale, (
            f"Stale entries in _CONTRACT_TEST_MAP (not in formal list): {sorted(stale)}"
        )
