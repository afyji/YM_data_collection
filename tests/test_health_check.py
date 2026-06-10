"""Tests for quality/health_checker.py."""

from __future__ import annotations

import urllib.error
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from YM_data_collection.quality.health_checker import (
    HealthStatus,
    ServiceHealthChecker,
    SystemHealth,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def checker_no_deps() -> ServiceHealthChecker:
    """Checker with no session_factory or cache_client."""
    return ServiceHealthChecker(session_factory=None, cache_client=None)


@pytest.fixture
def mock_session_factory():
    """A mock session_factory that succeeds on SELECT 1."""
    session = MagicMock()
    session.execute.return_value = None
    session.commit.return_value = None
    session.rollback.return_value = None
    session.close.return_value = None

    sf = MagicMock(return_value=session)
    return sf


@pytest.fixture
def mock_cache_client():
    """A mock cache_client whose ping() returns True."""
    client = MagicMock()
    client.ping.return_value = True
    return client


# ---------------------------------------------------------------------------
# MySQL check
# ---------------------------------------------------------------------------

class TestCheckMysql:
    def test_healthy_when_select_1_succeeds(self, mock_session_factory):
        checker = ServiceHealthChecker(session_factory=mock_session_factory)
        status = checker.check_mysql()
        assert status.component == "mysql"
        assert status.healthy is True
        assert status.latency_ms is not None
        assert status.latency_ms > 0
        assert status.error is None

    def test_unhealthy_when_exception(self, mock_session_factory):
        mock_session_factory.return_value.execute.side_effect = Exception("connection lost")
        checker = ServiceHealthChecker(session_factory=mock_session_factory)
        status = checker.check_mysql()
        assert status.healthy is False
        assert status.error is not None
        assert "connection lost" in status.error

    def test_unhealthy_when_no_session_factory(self, checker_no_deps):
        status = checker_no_deps.check_mysql()
        assert status.healthy is False
        assert status.error is not None

    def test_latency_measured(self, mock_session_factory):
        checker = ServiceHealthChecker(session_factory=mock_session_factory)
        status = checker.check_mysql()
        assert status.latency_ms is not None
        assert status.latency_ms > 0


# ---------------------------------------------------------------------------
# Redis check
# ---------------------------------------------------------------------------

class TestCheckRedis:
    def test_healthy_when_ping_true(self, mock_cache_client):
        checker = ServiceHealthChecker(cache_client=mock_cache_client)
        status = checker.check_redis()
        assert status.component == "redis"
        assert status.healthy is True
        assert status.latency_ms is not None
        assert status.latency_ms > 0
        assert status.error is None

    def test_unhealthy_when_ping_exception(self, mock_cache_client):
        mock_cache_client.ping.side_effect = Exception("redis down")
        checker = ServiceHealthChecker(cache_client=mock_cache_client)
        status = checker.check_redis()
        assert status.healthy is False
        assert "redis down" in (status.error or "")

    def test_unhealthy_when_no_cache_client(self, checker_no_deps):
        status = checker_no_deps.check_redis()
        assert status.healthy is False
        assert status.error is not None

    def test_latency_measured(self, mock_cache_client):
        checker = ServiceHealthChecker(cache_client=mock_cache_client)
        status = checker.check_redis()
        assert status.latency_ms is not None
        assert status.latency_ms > 0


# ---------------------------------------------------------------------------
# HTTP API check
# ---------------------------------------------------------------------------

class TestCheckHttpApi:
    @patch("YM_data_collection.quality.health_checker.urllib.request.urlopen")
    def test_healthy_when_200(self, mock_urlopen):
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp

        checker = ServiceHealthChecker()
        status = checker.check_http_api("http://127.0.0.1:8000")
        assert status.component == "http_api"
        assert status.healthy is True
        assert status.latency_ms is not None
        assert status.latency_ms > 0
        assert status.error is None

    @patch("YM_data_collection.quality.health_checker.urllib.request.urlopen")
    def test_unhealthy_when_http_error(self, mock_urlopen):
        mock_urlopen.side_effect = urllib.error.HTTPError(
            url="http://test",
            code=500,
            msg="Internal Server Error",
            hdrs=None,
            fp=None,
        )

        checker = ServiceHealthChecker()
        status = checker.check_http_api("http://127.0.0.1:8000")
        assert status.healthy is False
        assert status.error is not None

    @patch("YM_data_collection.quality.health_checker.urllib.request.urlopen")
    def test_unhealthy_when_connection_error(self, mock_urlopen):
        mock_urlopen.side_effect = ConnectionError("refused")

        checker = ServiceHealthChecker()
        status = checker.check_http_api("http://127.0.0.1:8000")
        assert status.healthy is False
        assert "connection error" in status.detail

    @patch("YM_data_collection.quality.health_checker.urllib.request.urlopen")
    def test_latency_measured(self, mock_urlopen):
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp

        checker = ServiceHealthChecker()
        status = checker.check_http_api("http://127.0.0.1:8000")
        assert status.latency_ms is not None
        assert status.latency_ms > 0


# ---------------------------------------------------------------------------
# Data freshness check
# ---------------------------------------------------------------------------

class TestCheckDataFreshness:
    def test_unhealthy_when_no_session_factory(self, checker_no_deps):
        status = checker_no_deps.check_data_freshness()
        assert status.component == "data_freshness"
        assert status.healthy is False
        assert status.error is not None

    @patch.object(ServiceHealthChecker, "_kline_repo", create=True)
    def test_healthy_when_fresh_data(self, mock_session_factory):
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        # Data 60 seconds old, well within default 600s
        recent_ts = now_ms - 60_000

        kline_repo = MagicMock()
        kline_repo.query_latest.return_value = [{"open_ts_ms": recent_ts}]

        checker = ServiceHealthChecker(session_factory=mock_session_factory)
        checker._kline_repo = kline_repo

        status = checker.check_data_freshness(max_age_seconds=600)
        assert status.healthy is True
        assert status.error is None

    @patch.object(ServiceHealthChecker, "_kline_repo", create=True)
    def test_unhealthy_when_stale_data(self, mock_session_factory):
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        # Data 1200 seconds old, older than default 600s
        stale_ts = now_ms - 1_200_000

        kline_repo = MagicMock()
        kline_repo.query_latest.return_value = [{"open_ts_ms": stale_ts}]

        checker = ServiceHealthChecker(session_factory=mock_session_factory)
        checker._kline_repo = kline_repo

        status = checker.check_data_freshness(max_age_seconds=600)
        assert status.healthy is False
        assert status.error is not None

    @patch.object(ServiceHealthChecker, "_kline_repo", create=True)
    def test_unhealthy_when_empty_result(self, mock_session_factory):
        kline_repo = MagicMock()
        kline_repo.query_latest.return_value = []

        checker = ServiceHealthChecker(session_factory=mock_session_factory)
        checker._kline_repo = kline_repo

        status = checker.check_data_freshness()
        assert status.healthy is False
        assert "no kline data" in status.detail

    @patch.object(ServiceHealthChecker, "_kline_repo", create=True)
    def test_unhealthy_when_query_exception(self, mock_session_factory):
        kline_repo = MagicMock()
        kline_repo.query_latest.side_effect = Exception("query failed")

        checker = ServiceHealthChecker(session_factory=mock_session_factory)
        checker._kline_repo = kline_repo

        status = checker.check_data_freshness()
        assert status.healthy is False
        assert "query failed" in (status.error or "")


# ---------------------------------------------------------------------------
# run_all aggregation
# ---------------------------------------------------------------------------

class TestRunAll:
    def test_all_healthy_overall_true(self, mock_session_factory, mock_cache_client):
        checker = ServiceHealthChecker(
            session_factory=mock_session_factory,
            cache_client=mock_cache_client,
        )

        # Mock kline repo for data freshness
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        checker._kline_repo = MagicMock()
        checker._kline_repo.query_latest.return_value = [{"open_ts_ms": now_ms - 60_000}]

        # Mock HTTP
        with patch("YM_data_collection.quality.health_checker.urllib.request.urlopen") as mock_urlopen:
            mock_resp = MagicMock()
            mock_resp.status = 200
            mock_resp.__enter__ = lambda s: s
            mock_resp.__exit__ = MagicMock(return_value=False)
            mock_urlopen.return_value = mock_resp

            health = checker.run_all(http_url="http://127.0.0.1:8000")

        assert isinstance(health, SystemHealth)
        assert health.overall_healthy is True
        assert len(health.statuses) == 4  # mysql, redis, http_api, data_freshness
        assert health.checked_at_utc != ""

    def test_one_unhealthy_overall_false(self, mock_cache_client):
        # session_factory is None -> mysql unhealthy, data_freshness unhealthy
        checker = ServiceHealthChecker(
            session_factory=None,
            cache_client=mock_cache_client,
        )

        health = checker.run_all(http_url=None)

        assert health.overall_healthy is False
        # At least mysql should be unhealthy
        mysql_status = next(s for s in health.statuses if s.component == "mysql")
        assert mysql_status.healthy is False

    def test_no_http_url_skips_http_check(self, mock_session_factory, mock_cache_client):
        checker = ServiceHealthChecker(
            session_factory=mock_session_factory,
            cache_client=mock_cache_client,
        )

        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        checker._kline_repo = MagicMock()
        checker._kline_repo.query_latest.return_value = [{"open_ts_ms": now_ms - 60_000}]

        health = checker.run_all(http_url=None)

        # Should have mysql, redis, data_freshness (no http_api)
        components = [s.component for s in health.statuses]
        assert "http_api" not in components
        assert "mysql" in components
        assert "redis" in components
        assert "data_freshness" in components
