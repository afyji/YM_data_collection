"""Tests for CoverageService."""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock

import pytest

from YM_data_collection.domain.models import IngestCheckpoint
from YM_data_collection.services.coverage_service import CoverageService


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def session_factory():
    return MagicMock(name="session_factory")


@pytest.fixture()
def checkpoint_repo():
    return MagicMock(name="checkpoint_repo")


def _make_service(session_factory, checkpoint_repo):
    return CoverageService(
        session_factory=session_factory,
        checkpoint_repo=checkpoint_repo,
    )


def _make_checkpoint(
    venue="binance",
    market_type="perp",
    symbol="BTCUSDT",
    data_type="mark_price",
    interval_code=None,
    last_event_ts_ms=1700000000000,
    status="ok",
    last_success_at_utc=None,
) -> IngestCheckpoint:
    return IngestCheckpoint(
        venue=venue,
        market_type=market_type,
        symbol=symbol,
        data_type=data_type,
        interval_code=interval_code,
        last_event_ts_ms=last_event_ts_ms,
        last_event_dt_utc=datetime(2023, 11, 14, 22, 13, 20),
        status=status,
        last_success_at_utc=last_success_at_utc or datetime(2023, 11, 14, 22, 13, 21),
    )


# ---------------------------------------------------------------------------
# get_coverage
# ---------------------------------------------------------------------------

class TestGetCoverage:
    def test_returns_checkpoint_data(self, session_factory, checkpoint_repo):
        cp = _make_checkpoint()
        checkpoint_repo.get.return_value = cp
        svc = _make_service(session_factory, checkpoint_repo)
        result = svc.get_coverage("binance", "perp", "BTCUSDT", "mark_price")
        assert result is not None
        assert result["venue"] == "binance"
        assert result["market_type"] == "perp"
        assert result["symbol"] == "BTCUSDT"
        assert result["data_type"] == "mark_price"
        assert result["last_event_ts_ms"] == 1700000000000
        assert result["status"] == "ok"
        assert result["last_success_at_utc"] == cp.last_success_at_utc

    def test_checkpoint_not_found(self, session_factory, checkpoint_repo):
        checkpoint_repo.get.return_value = None
        svc = _make_service(session_factory, checkpoint_repo)
        result = svc.get_coverage("binance", "spot", "ETHUSDT", "kline", interval_code="1h")
        assert result is None

    def test_passes_interval_code(self, session_factory, checkpoint_repo):
        cp = _make_checkpoint(data_type="kline", interval_code="1h")
        checkpoint_repo.get.return_value = cp
        svc = _make_service(session_factory, checkpoint_repo)
        result = svc.get_coverage("binance", "spot", "ETHUSDT", "kline", interval_code="1h")
        assert result is not None
        assert result["interval_code"] == "1h"
        checkpoint_repo.get.assert_called_once_with(
            venue="binance",
            market_type="spot",
            symbol="ETHUSDT",
            data_type="kline",
            interval_code="1h",
        )

    def test_error_status(self, session_factory, checkpoint_repo):
        cp = _make_checkpoint(status="error")
        checkpoint_repo.get.return_value = cp
        svc = _make_service(session_factory, checkpoint_repo)
        result = svc.get_coverage("binance", "perp", "BTCUSDT", "mark_price")
        assert result["status"] == "error"


# ---------------------------------------------------------------------------
# list_all_coverage
# ---------------------------------------------------------------------------

class TestListAllCoverage:
    def test_returns_all_checkpoints(self, session_factory, checkpoint_repo):
        cp1 = _make_checkpoint(symbol="BTCUSDT", data_type="mark_price")
        cp2 = _make_checkpoint(symbol="ETHUSDT", data_type="funding_rate")
        checkpoint_repo.list_all.return_value = [cp1, cp2]
        svc = _make_service(session_factory, checkpoint_repo)
        results = svc.list_all_coverage()
        assert len(results) == 2
        assert results[0]["symbol"] == "BTCUSDT"
        assert results[0]["data_type"] == "mark_price"
        assert results[1]["symbol"] == "ETHUSDT"
        assert results[1]["data_type"] == "funding_rate"

    def test_empty_list(self, session_factory, checkpoint_repo):
        checkpoint_repo.list_all.return_value = []
        svc = _make_service(session_factory, checkpoint_repo)
        results = svc.list_all_coverage()
        assert results == []

    def test_coverage_fields_complete(self, session_factory, checkpoint_repo):
        cp = _make_checkpoint()
        checkpoint_repo.list_all.return_value = [cp]
        svc = _make_service(session_factory, checkpoint_repo)
        results = svc.list_all_coverage()
        expected_keys = {
            "venue", "market_type", "symbol", "data_type",
            "interval_code", "last_event_ts_ms", "status",
            "last_success_at_utc",
        }
        assert set(results[0].keys()) == expected_keys
