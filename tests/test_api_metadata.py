"""Tests for metadata, status, quality, and manifest API routes."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
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


OPENAPI_DRAFT_PATH = (
    Path(__file__).resolve().parents[2]
    / "docs"
    / "01_data_collection_system"
    / "17_http_openapi_draft.yaml"
)


# ── Config helper ─────────────────────────────────────────────────────────


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


# ── Domain model factories ───────────────────────────────────────────────


def _make_instrument(
    venue: str = "binance",
    market_type: str = "spot",
    symbol: str = "BTCUSDT",
    is_active: bool = True,
) -> InstrumentInfo:
    return InstrumentInfo(
        venue=venue,
        market_type=market_type,
        symbol=symbol,
        base_asset="BTC",
        quote_asset="USDT",
        instrument_code="BTCUSDT",
        is_active=is_active,
        tick_size=Decimal("0.01"),
        step_size=Decimal("0.000001"),
        min_qty=Decimal("0.000001"),
        min_notional=Decimal("10"),
        contract_type=None,
    )


def _make_checkpoint(
    venue: str = "binance",
    market_type: str = "spot",
    symbol: str = "BTCUSDT",
    data_type: str = "kline",
    interval_code: str | None = "1h",
) -> IngestCheckpoint:
    return IngestCheckpoint(
        venue=venue,
        market_type=market_type,
        symbol=symbol,
        data_type=data_type,
        interval_code=interval_code,
        last_event_ts_ms=1700000000000,
        last_event_dt_utc=datetime(2023, 11, 14, 22, 13, 20, tzinfo=timezone.utc),
        last_trade_id=None,
        last_kline_open_ts_ms=1700000000000,
        status="ok",
        last_success_at_utc=datetime(2023, 11, 14, 22, 13, 20, tzinfo=timezone.utc),
        last_error_message=None,
    )


def _make_quality_issue(
    symbol: str = "BTCUSDT",
    data_type: str = "kline",
    status: str = "open",
    issue_type: str = "gap",
    severity: str = "warning",
) -> DataQualityIssue:
    return DataQualityIssue(
        venue="binance",
        market_type="spot",
        symbol=symbol,
        data_type=data_type,
        interval_code="1h",
        issue_type=issue_type,
        severity=severity,
        detected_at_utc=datetime(2023, 11, 14, 22, 0, 0, tzinfo=timezone.utc),
        start_ts_ms=1700000000000,
        end_ts_ms=1700003600000,
        description="Gap detected",
        status=status,
        resolution_note=None,
    )


def _make_manifest(
    dataset_name: str = "kline_spot",
    symbol: str = "BTCUSDT",
    data_type: str = "kline",
) -> FileManifest:
    return FileManifest(
        dataset_name=dataset_name,
        venue="binance",
        market_type="spot",
        symbol=symbol,
        data_type=data_type,
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


# ── Mock factories ───────────────────────────────────────────────────────


def _make_instrument_repo() -> MagicMock:
    repo = MagicMock()
    repo.list_active.return_value = [
        _make_instrument(symbol="BTCUSDT"),
        _make_instrument(symbol="ETHUSDT"),
        _make_instrument(venue="okx", symbol="BTCUSDT"),
    ]
    repo.list_all.return_value = [
        _make_instrument(symbol="BTCUSDT"),
        _make_instrument(symbol="ETHUSDT", is_active=False),
        _make_instrument(venue="okx", symbol="BTCUSDT"),
    ]
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
    issues = [_make_quality_issue(symbol="BTCUSDT") for _ in range(5)] + [
        _make_quality_issue(symbol="ETHUSDT", status="closed")
    ]
    repo.list_by_symbol.return_value = issues[:5]
    repo.list_by_status.return_value = issues
    return repo


def _make_manifest_repo() -> MagicMock:
    repo = MagicMock()
    manifests = [_make_manifest(symbol=s) for s in ["BTCUSDT", "ETHUSDT", "SOLUSDT"]]
    repo.list_by_dataset.return_value = manifests
    repo.list_by_symbol.return_value = manifests[:2]
    return repo


# ── Fixtures ─────────────────────────────────────────────────────────────


@pytest.fixture()
def client():
    config = _make_config()
    instrument_repo = _make_instrument_repo()
    coverage_service = _make_coverage_service()
    checkpoint_repo = _make_checkpoint_repo()
    quality_repo = _make_quality_repo()
    manifest_repo = _make_manifest_repo()
    app = create_app(
        config,
        instrument_repo=instrument_repo,
        coverage_service=coverage_service,
        checkpoint_repo=checkpoint_repo,
        quality_repo=quality_repo,
        manifest_repo=manifest_repo,
    )
    tc = TestClient(app)
    return tc, {
        "instrument_repo": instrument_repo,
        "coverage_service": coverage_service,
        "checkpoint_repo": checkpoint_repo,
        "quality_repo": quality_repo,
        "manifest_repo": manifest_repo,
    }


# ── Instruments ──────────────────────────────────────────────────────────


class TestListInstruments:
    def test_returns_ok(self, client):
        tc, _ = client
        resp = tc.get("/api/v1/metadata/instruments")
        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert body["code"] == "OK"

    def test_default_active_only(self, client):
        tc, _ = client
        resp = tc.get("/api/v1/metadata/instruments")
        body = resp.json()
        assert len(body["data"]["items"]) == 3
        assert body["meta"]["count"] == 3
        assert body["meta"]["source"] == "mysql"

    def test_is_active_false(self, client):
        tc, repos = client
        resp = tc.get("/api/v1/metadata/instruments", params={"is_active": False})
        body = resp.json()
        assert len(body["data"]["items"]) == 3
        repos["instrument_repo"].list_all.assert_called_once()

    def test_filter_by_venue(self, client):
        tc, _ = client
        resp = tc.get("/api/v1/metadata/instruments", params={"venue": "okx"})
        body = resp.json()
        items = body["data"]["items"]
        assert len(items) == 1
        assert items[0]["venue"] == "okx"

    def test_filter_by_market_type(self, client):
        tc, _ = client
        resp = tc.get(
            "/api/v1/metadata/instruments", params={"market_type": "spot"}
        )
        body = resp.json()
        for item in body["data"]["items"]:
            assert item["market_type"] == "spot"

    def test_decimal_fields_are_strings(self, client):
        tc, _ = client
        resp = tc.get("/api/v1/metadata/instruments")
        body = resp.json()
        item = body["data"]["items"][0]
        assert isinstance(item["tick_size"], str)
        assert isinstance(item["step_size"], str)


# ── Coverage ─────────────────────────────────────────────────────────────


class TestGetCoverage:
    def test_returns_ok(self, client):
        tc, _ = client
        resp = tc.get(
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
        assert body["success"] is True
        assert body["meta"]["source"] == "mysql"

    def test_returns_coverage_data(self, client):
        tc, _ = client
        resp = tc.get(
            "/api/v1/metadata/coverage",
            params={
                "venue": "binance",
                "market_type": "spot",
                "symbol": "BTCUSDT",
                "data_type": "kline",
            },
        )
        body = resp.json()
        assert body["data"]["symbol"] == "BTCUSDT"

    def test_404_when_no_coverage(self, client):
        tc, repos = client
        repos["coverage_service"].get_coverage.return_value = None
        resp = tc.get(
            "/api/v1/metadata/coverage",
            params={
                "venue": "binance",
                "market_type": "spot",
                "symbol": "FOOBAR",
                "data_type": "kline",
            },
        )
        assert resp.status_code == 404
        body = resp.json()
        assert body["success"] is False
        assert body["code"] == "NOT_FOUND"

    def test_service_called_with_interval(self, client):
        tc, repos = client
        tc.get(
            "/api/v1/metadata/coverage",
            params={
                "venue": "binance",
                "market_type": "spot",
                "symbol": "BTCUSDT",
                "data_type": "kline",
                "interval": "1h",
            },
        )
        repos["coverage_service"].get_coverage.assert_called_once_with(
            "binance", "spot", "BTCUSDT", "kline", "1h"
        )


# ── Status ───────────────────────────────────────────────────────────────


class TestGetStatus:
    def test_returns_ok(self, client):
        tc, _ = client
        resp = tc.get(
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
        assert body["success"] is True
        assert body["meta"]["source"] == "mysql"

    def test_returns_checkpoint_sub_object(self, client):
        tc, _ = client
        resp = tc.get(
            "/api/v1/metadata/status",
            params={
                "venue": "binance",
                "market_type": "spot",
                "symbol": "BTCUSDT",
                "data_type": "kline",
            },
        )
        body = resp.json()
        # Flattened shape: top-level identity + status + checkpoint sub-object
        assert body["data"]["symbol"] == "BTCUSDT"
        assert body["data"]["status"] == "ok"
        assert "checkpoint" in body["data"]

    def test_404_when_no_checkpoint(self, client):
        tc, repos = client
        repos["checkpoint_repo"].get.return_value = None
        resp = tc.get(
            "/api/v1/metadata/status",
            params={
                "venue": "binance",
                "market_type": "spot",
                "symbol": "FOOBAR",
                "data_type": "kline",
            },
        )
        assert resp.status_code == 404
        body = resp.json()
        assert body["code"] == "NOT_FOUND"

    def test_repo_called_with_interval(self, client):
        tc, repos = client
        tc.get(
            "/api/v1/metadata/status",
            params={
                "venue": "binance",
                "market_type": "spot",
                "symbol": "BTCUSDT",
                "data_type": "kline",
                "interval": "5m",
            },
        )
        repos["checkpoint_repo"].get.assert_called_once_with(
            "binance", "spot", "BTCUSDT", "kline", "5m"
        )


# ── Quality Issues ───────────────────────────────────────────────────────


class TestListQualityIssues:
    def test_by_symbol_returns_ok(self, client):
        tc, _ = client
        resp = tc.get(
            "/api/v1/metadata/quality-issues",
            params={"symbol": "BTCUSDT"},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert body["meta"]["source"] == "mysql"

    def test_by_symbol_returns_items(self, client):
        tc, _ = client
        resp = tc.get(
            "/api/v1/metadata/quality-issues",
            params={"symbol": "BTCUSDT"},
        )
        body = resp.json()
        assert len(body["data"]["items"]) == 5
        assert body["meta"]["total"] == 5

    def test_by_status_filter(self, client):
        tc, repos = client
        resp = tc.get("/api/v1/metadata/quality-issues")
        body = resp.json()
        repos["quality_repo"].list_by_status.assert_called_once_with("open")

    def test_custom_status_filter(self, client):
        tc, repos = client
        resp = tc.get(
            "/api/v1/metadata/quality-issues",
            params={"status_filter": "closed"},
        )
        repos["quality_repo"].list_by_status.assert_called_once_with("closed")

    def test_pagination(self, client):
        tc, repos = client
        # Create 25 issues so we can paginate
        issues = [_make_quality_issue(symbol=f"SYM{i}") for i in range(25)]
        repos["quality_repo"].list_by_status.return_value = issues
        resp = tc.get(
            "/api/v1/metadata/quality-issues",
            params={"status_filter": "open", "page": 1, "count": 10},
        )
        body = resp.json()
        assert len(body["data"]["items"]) == 10
        assert body["meta"]["page"] == 1
        assert body["meta"]["total"] == 25

    def test_pagination_page2(self, client):
        tc, repos = client
        issues = [_make_quality_issue(symbol=f"SYM{i}") for i in range(25)]
        repos["quality_repo"].list_by_status.return_value = issues
        resp = tc.get(
            "/api/v1/metadata/quality-issues",
            params={"status_filter": "open", "page": 2, "count": 10},
        )
        body = resp.json()
        assert len(body["data"]["items"]) == 10
        assert body["meta"]["page"] == 2

    def test_pagination_last_page(self, client):
        tc, repos = client
        issues = [_make_quality_issue(symbol=f"SYM{i}") for i in range(25)]
        repos["quality_repo"].list_by_status.return_value = issues
        resp = tc.get(
            "/api/v1/metadata/quality-issues",
            params={"status_filter": "open", "page": 3, "count": 10},
        )
        body = resp.json()
        assert len(body["data"]["items"]) == 5
        assert body["meta"]["total"] == 25

    def test_symbol_with_data_type(self, client):
        tc, repos = client
        resp = tc.get(
            "/api/v1/metadata/quality-issues",
            params={"symbol": "BTCUSDT", "data_type": "kline"},
        )
        repos["quality_repo"].list_by_symbol.assert_called_once_with(
            "BTCUSDT", "kline"
        )


# ── Manifests ────────────────────────────────────────────────────────────


class TestListManifests:
    def test_by_dataset_returns_ok(self, client):
        tc, _ = client
        resp = tc.get(
            "/api/v1/datasets/manifests",
            params={"dataset_name": "kline_spot"},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert len(body["data"]["items"]) == 3
        assert body["meta"]["source"] == "mysql"

    def test_by_symbol_returns_items(self, client):
        tc, repos = client
        resp = tc.get(
            "/api/v1/datasets/manifests",
            params={"symbol": "BTCUSDT"},
        )
        body = resp.json()
        assert len(body["data"]["items"]) == 2
        repos["manifest_repo"].list_by_symbol.assert_called_once_with(
            "BTCUSDT", None
        )

    def test_no_params_returns_empty(self, client):
        tc, _ = client
        resp = tc.get("/api/v1/datasets/manifests")
        body = resp.json()
        assert body["data"]["items"] == []
        assert body["meta"]["total"] == 0

    def test_pagination(self, client):
        tc, repos = client
        manifests = [_make_manifest(symbol=f"SYM{i}") for i in range(30)]
        repos["manifest_repo"].list_by_dataset.return_value = manifests
        resp = tc.get(
            "/api/v1/datasets/manifests",
            params={"dataset_name": "kline_spot", "page": 1, "count": 10},
        )
        body = resp.json()
        assert len(body["data"]["items"]) == 10
        assert body["meta"]["page"] == 1
        assert body["meta"]["total"] == 30

    def test_pagination_page2(self, client):
        tc, repos = client
        manifests = [_make_manifest(symbol=f"SYM{i}") for i in range(30)]
        repos["manifest_repo"].list_by_dataset.return_value = manifests
        resp = tc.get(
            "/api/v1/datasets/manifests",
            params={"dataset_name": "kline_spot", "page": 2, "count": 10},
        )
        body = resp.json()
        assert len(body["data"]["items"]) == 10
        assert body["meta"]["page"] == 2

    def test_symbol_with_data_type(self, client):
        tc, repos = client
        resp = tc.get(
            "/api/v1/datasets/manifests",
            params={"symbol": "BTCUSDT", "data_type": "kline"},
        )
        repos["manifest_repo"].list_by_symbol.assert_called_once_with(
            "BTCUSDT", "kline"
        )


# ── Manifest Detail ──────────────────────────────────────────────────────


class TestGetManifestDetail:
    """DC-T063: detail now requires manifest_id (int), not dataset_name+symbol."""

    def test_returns_ok_with_manifest_id(self, client):
        tc, repos = client
        m = _make_manifest()
        m.id = 42
        repos["manifest_repo"].get_by_id.return_value = m
        resp = tc.get(
            "/api/v1/datasets/manifests/detail",
            params={"manifest_id": 42},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert body["meta"]["source"] == "mysql"

    def test_returns_manifest_with_manifest_id(self, client):
        tc, repos = client
        m = _make_manifest()
        m.id = 42
        repos["manifest_repo"].get_by_id.return_value = m
        resp = tc.get(
            "/api/v1/datasets/manifests/detail",
            params={"manifest_id": 42},
        )
        body = resp.json()
        assert isinstance(body["data"], dict)
        assert body["data"]["manifest_id"] == 42

    def test_404_when_id_not_found(self, client):
        tc, repos = client
        repos["manifest_repo"].get_by_id.return_value = None
        resp = tc.get(
            "/api/v1/datasets/manifests/detail",
            params={"manifest_id": 9999},
        )
        assert resp.status_code == 404
        body = resp.json()
        assert body["code"] == "NOT_FOUND"

    def test_missing_manifest_id_returns_422(self, client):
        tc, _ = client
        # No manifest_id param at all
        resp = tc.get("/api/v1/datasets/manifests/detail")
        assert resp.status_code == 422

    def test_old_dataset_name_symbol_only_returns_422(self, client):
        tc, _ = client
        # Old contract: dataset_name+symbol no longer accepted as required params
        resp = tc.get(
            "/api/v1/datasets/manifests/detail",
            params={"dataset_name": "kline_spot", "symbol": "BTCUSDT"},
        )
        assert resp.status_code == 422

    def test_repo_get_by_id_called(self, client):
        tc, repos = client
        m = _make_manifest()
        m.id = 7
        repos["manifest_repo"].get_by_id.return_value = m
        tc.get(
            "/api/v1/datasets/manifests/detail",
            params={"manifest_id": 7},
        )
        repos["manifest_repo"].get_by_id.assert_called_once_with(7)


# ══════════════════════════════════════════════════════════════════════════
# DC-T062: Contract alignment tests
# ══════════════════════════════════════════════════════════════════════════


# ── Instruments: pagination + filter meta ─────────────────────────────────


class TestInstrumentsPaginationDC062:
    """DC-T062: instruments endpoint must support page/count and return
    meta with page, count, total, source."""

    def test_meta_has_page_count_total_source(self, client):
        tc, _ = client
        resp = tc.get("/api/v1/metadata/instruments")
        body = resp.json()
        meta = body["meta"]
        assert "page" in meta, "meta must include 'page'"
        assert "count" in meta, "meta must include 'count'"
        assert "total" in meta, "meta must include 'total'"
        assert "source" in meta, "meta must include 'source'"

    def test_default_page_is_1(self, client):
        tc, _ = client
        resp = tc.get("/api/v1/metadata/instruments")
        body = resp.json()
        assert body["meta"]["page"] == 1

    def test_pagination_page2(self, client):
        tc, repos = client
        # 5 active instruments
        repos["instrument_repo"].list_active.return_value = [
            _make_instrument(symbol=f"SYM{i}") for i in range(5)
        ]
        resp = tc.get(
            "/api/v1/metadata/instruments",
            params={"page": 2, "count": 2},
        )
        body = resp.json()
        assert body["meta"]["page"] == 2
        assert body["meta"]["count"] == 2
        assert body["meta"]["total"] == 5
        assert len(body["data"]["items"]) == 2

    def test_filter_with_pagination(self, client):
        tc, repos = client
        repos["instrument_repo"].list_active.return_value = [
            _make_instrument(venue="binance", symbol=f"SYM{i}")
            for i in range(4)
        ] + [
            _make_instrument(venue="okx", symbol=f"OKX{i}")
            for i in range(3)
        ]
        resp = tc.get(
            "/api/v1/metadata/instruments",
            params={"venue": "okx", "page": 1, "count": 2},
        )
        body = resp.json()
        assert body["meta"]["total"] == 3
        assert len(body["data"]["items"]) == 2


# ── Coverage: interval normalization ─────────────────────────────────────


class TestCoverageIntervalDC062:
    """DC-T062: coverage response data should use 'interval' key instead of
    'interval_code' when the underlying result contains interval_code."""

    def test_response_uses_interval_not_interval_code(self, client):
        tc, _ = client
        resp = tc.get(
            "/api/v1/metadata/coverage",
            params={
                "venue": "binance",
                "market_type": "spot",
                "symbol": "BTCUSDT",
                "data_type": "kline",
            },
        )
        body = resp.json()
        data = body["data"]
        # The service returns interval_code; the API must normalize to interval
        assert "interval" in data, "response data must use 'interval' key"
        assert "interval_code" not in data, (
            "response data should not expose raw 'interval_code'"
        )


# ── Status: flattened shape ──────────────────────────────────────────────


class TestStatusFlattenedDC062:
    """DC-T062: status endpoint must return a flattened shape with top-level
    fields venue, market_type, symbol, data_type, interval, status,
    last_success_at_utc, last_error_message, and a checkpoint sub-object."""

    def test_data_has_top_level_identity_fields(self, client):
        tc, _ = client
        resp = tc.get(
            "/api/v1/metadata/status",
            params={
                "venue": "binance",
                "market_type": "spot",
                "symbol": "BTCUSDT",
                "data_type": "kline",
            },
        )
        body = resp.json()
        data = body["data"]
        assert data["venue"] == "binance"
        assert data["market_type"] == "spot"
        assert data["symbol"] == "BTCUSDT"
        assert data["data_type"] == "kline"

    def test_data_has_status_fields(self, client):
        tc, _ = client
        resp = tc.get(
            "/api/v1/metadata/status",
            params={
                "venue": "binance",
                "market_type": "spot",
                "symbol": "BTCUSDT",
                "data_type": "kline",
            },
        )
        body = resp.json()
        data = body["data"]
        assert "status" in data
        assert "last_success_at_utc" in data
        assert "last_error_message" in data

    def test_data_has_interval_from_interval_code(self, client):
        tc, _ = client
        resp = tc.get(
            "/api/v1/metadata/status",
            params={
                "venue": "binance",
                "market_type": "spot",
                "symbol": "BTCUSDT",
                "data_type": "kline",
                "interval": "1h",
            },
        )
        body = resp.json()
        data = body["data"]
        assert "interval" in data
        assert "interval_code" not in data

    def test_data_has_checkpoint_subobject(self, client):
        tc, _ = client
        resp = tc.get(
            "/api/v1/metadata/status",
            params={
                "venue": "binance",
                "market_type": "spot",
                "symbol": "BTCUSDT",
                "data_type": "kline",
            },
        )
        body = resp.json()
        data = body["data"]
        assert "checkpoint" in data
        # checkpoint should contain key timestamp fields
        assert "last_event_ts_ms" in data["checkpoint"]
        assert "last_kline_open_ts_ms" in data["checkpoint"]


# ── OpenAPI parameter alignment ─────────────────────────────────────────


class TestOpenAPIParameterAlignmentDC062:
    """DC-T062: OpenAPI draft must list all route-level parameters for
    metadata endpoints."""

    @pytest.fixture()
    def openapi_spec(self):
        import yaml

        with open(OPENAPI_DRAFT_PATH, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    def _path_params(self, spec, path_suffix):
        path_key = f"/api/v1/{path_suffix}"
        params = spec["paths"][path_key]["get"].get("parameters", [])
        names = []
        for p in params:
            if "$ref" in p:
                # Resolve $ref like '#/components/parameters/Venue'
                ref_path = p["$ref"].lstrip("#/").split("/")
                resolved = spec
                for seg in ref_path:
                    resolved = resolved[seg]
                names.append(resolved["name"])
            else:
                names.append(p.get("name"))
        return names

    def test_instruments_has_venue_market_type_is_active_page_count(self, openapi_spec):
        names = self._path_params(openapi_spec, "metadata/instruments")
        for expected in ["venue", "market_type", "is_active", "page", "count"]:
            assert expected in names, (
                f"instruments params missing '{expected}': got {names}"
            )

    def test_coverage_has_data_type_and_interval(self, openapi_spec):
        names = self._path_params(openapi_spec, "metadata/coverage")
        for expected in ["venue", "market_type", "symbol", "data_type", "interval"]:
            assert expected in names, (
                f"coverage params missing '{expected}': got {names}"
            )

    def test_status_has_data_type_and_interval(self, openapi_spec):
        names = self._path_params(openapi_spec, "metadata/status")
        for expected in ["venue", "market_type", "symbol", "data_type", "interval"]:
            assert expected in names, (
                f"status params missing '{expected}': got {names}"
            )

    def test_quality_issues_has_symbol_data_type_status_filter_page_count(self, openapi_spec):
        names = self._path_params(openapi_spec, "metadata/quality-issues")
        for expected in ["symbol", "data_type", "status_filter", "page", "count"]:
            assert expected in names, (
                f"quality-issues params missing '{expected}': got {names}"
            )


# ══════════════════════════════════════════════════════════════════════════
# DC-T063: manifest_id contract & datasets/download
# ══════════════════════════════════════════════════════════════════════════


# ── Manifest list items include manifest_id ────────────────────────────────


class TestListManifestsManifestIdDC063:
    """DC-T063: /datasets/manifests list items must include manifest_id."""

    def test_list_items_include_manifest_id(self, client):
        tc, repos = client
        m = _make_manifest()
        m.id = 101
        repos["manifest_repo"].list_by_dataset.return_value = [m]
        resp = tc.get(
            "/api/v1/datasets/manifests",
            params={"dataset_name": "kline_spot"},
        )
        body = resp.json()
        items = body["data"]["items"]
        assert len(items) == 1
        assert items[0]["manifest_id"] == 101

    def test_list_items_without_id_omits_manifest_id(self, client):
        tc, repos = client
        # Manifest with no id set (mock without id)
        m = _make_manifest()
        # Don't set m.id - it should not be in output or be None
        repos["manifest_repo"].list_by_dataset.return_value = [m]
        resp = tc.get(
            "/api/v1/datasets/manifests",
            params={"dataset_name": "kline_spot"},
        )
        body = resp.json()
        items = body["data"]["items"]
        assert len(items) == 1
        # When id is None, manifest_id should either be absent or None
        assert items[0].get("manifest_id") is None or "manifest_id" not in items[0]


# ── OpenAPI: manifest_id for detail/download ──────────────────────────────


class TestOpenAPIManifestIdDC063:
    """DC-T063: OpenAPI must list manifest_id as required param for detail/download."""

    @pytest.fixture()
    def openapi_spec(self):
        import yaml

        with open(OPENAPI_DRAFT_PATH, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    def _path_params(self, spec, path_suffix):
        path_key = f"/api/v1/{path_suffix}"
        params = spec["paths"][path_key]["get"].get("parameters", [])
        names = []
        for p in params:
            if "$ref" in p:
                ref_path = p["$ref"].lstrip("#/").split("/")
                resolved = spec
                for seg in ref_path:
                    resolved = resolved[seg]
                names.append(resolved["name"])
            else:
                names.append(p.get("name"))
        return names

    def test_manifests_list_has_filter_params(self, openapi_spec):
        names = self._path_params(openapi_spec, "datasets/manifests")
        for expected in ["dataset_name", "symbol", "data_type", "page", "count"]:
            assert expected in names, (
                f"datasets/manifests params missing '{expected}': got {names}"
            )

    def test_manifest_detail_has_manifest_id(self, openapi_spec):
        names = self._path_params(openapi_spec, "datasets/manifests/detail")
        assert "manifest_id" in names, (
            f"datasets/manifests/detail must have manifest_id param: got {names}"
        )

    def test_download_has_manifest_id(self, openapi_spec):
        names = self._path_params(openapi_spec, "datasets/download")
        assert "manifest_id" in names, (
            f"datasets/download must have manifest_id param: got {names}"
        )


# ── Datasets download ─────────────────────────────────────────────────────


class TestDatasetsDownloadDC063:
    """DC-T063: /datasets/download endpoint tests."""

    def test_download_existing_file_returns_200(self, client, tmp_path):
        tc, repos = client
        # Create a tiny test file
        test_file = tmp_path / "test.parquet"
        test_content = b"PAR1fake_data_for_test"
        test_file.write_bytes(test_content)

        m = _make_manifest()
        m.id = 10
        m.file_path = str(test_file)
        repos["manifest_repo"].get_by_id.return_value = m

        resp = tc.get(
            "/api/v1/datasets/download",
            params={"manifest_id": 10},
        )
        assert resp.status_code == 200
        assert resp.content == test_content

    def test_download_has_content_disposition(self, client, tmp_path):
        tc, repos = client
        test_file = tmp_path / "test.parquet"
        test_file.write_bytes(b"data")
        m = _make_manifest()
        m.id = 10
        m.file_path = str(test_file)
        repos["manifest_repo"].get_by_id.return_value = m

        resp = tc.get(
            "/api/v1/datasets/download",
            params={"manifest_id": 10},
        )
        assert resp.status_code == 200
        assert "content-disposition" in resp.headers
        assert "attachment" in resp.headers["content-disposition"]

    def test_download_has_content_length(self, client, tmp_path):
        tc, repos = client
        test_file = tmp_path / "test.parquet"
        test_content = b"1234567890"
        test_file.write_bytes(test_content)
        m = _make_manifest()
        m.id = 10
        m.file_path = str(test_file)
        repos["manifest_repo"].get_by_id.return_value = m

        resp = tc.get(
            "/api/v1/datasets/download",
            params={"manifest_id": 10},
        )
        assert resp.status_code == 200
        assert "content-length" in resp.headers
        assert int(resp.headers["content-length"]) == len(test_content)

    def test_download_unknown_manifest_id_returns_404(self, client):
        tc, repos = client
        repos["manifest_repo"].get_by_id.return_value = None
        resp = tc.get(
            "/api/v1/datasets/download",
            params={"manifest_id": 9999},
        )
        assert resp.status_code == 404
        body = resp.json()
        assert body["code"] == "NOT_FOUND"

    def test_download_missing_file_returns_404(self, client):
        tc, repos = client
        m = _make_manifest()
        m.id = 10
        m.file_path = "/nonexistent/path/file.parquet"
        repos["manifest_repo"].get_by_id.return_value = m

        resp = tc.get(
            "/api/v1/datasets/download",
            params={"manifest_id": 10},
        )
        assert resp.status_code == 404
        body = resp.json()
        assert body["code"] == "NOT_FOUND"

    def test_download_missing_manifest_id_returns_422(self, client):
        tc, _ = client
        resp = tc.get("/api/v1/datasets/download")
        assert resp.status_code == 422
