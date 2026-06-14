"""Service health checker for infrastructure components."""

from __future__ import annotations

import time
import urllib.request
import urllib.error
from dataclasses import dataclass, field
from datetime import datetime, timezone

from sqlalchemy import text

from YM_data_collection.persistence.mysql import session_scope
from YM_data_collection.persistence.repositories.marketdata_repo import KlineRepository


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class HealthStatus:
    """Status of a single health check."""

    component: str  # 'mysql', 'redis', 'http_api', 'data_freshness'
    healthy: bool
    latency_ms: float | None = None
    detail: str = ""
    error: str | None = None


@dataclass
class SystemHealth:
    """Aggregated health of all checked components."""

    overall_healthy: bool
    statuses: list[HealthStatus] = field(default_factory=list)
    checked_at_utc: str = ""


# ---------------------------------------------------------------------------
# Checker
# ---------------------------------------------------------------------------

class ServiceHealthChecker:
    """Run health checks against MySQL, Redis, HTTP API, and data freshness."""

    def __init__(self, session_factory=None, cache_client=None) -> None:
        self._session_factory = session_factory
        self._cache_client = cache_client
        self._kline_repo = KlineRepository()

    # -- individual checks ---------------------------------------------------

    def check_mysql(self) -> HealthStatus:
        """Try SELECT 1, measure latency."""
        if self._session_factory is None:
            return HealthStatus(
                component="mysql",
                healthy=False,
                detail="no session_factory configured",
                error="session_factory is None",
            )
        t0 = time.perf_counter()
        try:
            with session_scope(self._session_factory) as session:
                session.execute(text("SELECT 1"))
            elapsed_ms = (time.perf_counter() - t0) * 1000
            return HealthStatus(
                component="mysql",
                healthy=True,
                latency_ms=round(elapsed_ms, 2),
                detail="SELECT 1 ok",
            )
        except Exception as exc:
            elapsed_ms = (time.perf_counter() - t0) * 1000
            return HealthStatus(
                component="mysql",
                healthy=False,
                latency_ms=round(elapsed_ms, 2),
                detail="SELECT 1 failed",
                error=str(exc),
            )

    def check_redis(self) -> HealthStatus:
        """Try ping(), measure latency."""
        if self._cache_client is None:
            return HealthStatus(
                component="redis",
                healthy=False,
                detail="no cache_client configured",
                error="cache_client is None",
            )
        t0 = time.perf_counter()
        try:
            ok = self._cache_client.ping()
            elapsed_ms = (time.perf_counter() - t0) * 1000
            return HealthStatus(
                component="redis",
                healthy=bool(ok),
                latency_ms=round(elapsed_ms, 2),
                detail=f"ping()={ok}",
            )
        except Exception as exc:
            elapsed_ms = (time.perf_counter() - t0) * 1000
            return HealthStatus(
                component="redis",
                healthy=False,
                latency_ms=round(elapsed_ms, 2),
                detail="ping() raised",
                error=str(exc),
            )

    def check_http_api(self, base_url: str) -> HealthStatus:
        """GET {base_url}/api/v1/system/health, check 200 response."""
        url = f"{base_url.rstrip('/')}/api/v1/system/health"
        t0 = time.perf_counter()
        try:
            req = urllib.request.Request(url, method="GET")
            with urllib.request.urlopen(req, timeout=10) as resp:
                status_code = resp.status
            elapsed_ms = (time.perf_counter() - t0) * 1000
            healthy = status_code == 200
            return HealthStatus(
                component="http_api",
                healthy=healthy,
                latency_ms=round(elapsed_ms, 2),
                detail=f"GET {url} -> {status_code}",
            )
        except urllib.error.HTTPError as exc:
            elapsed_ms = (time.perf_counter() - t0) * 1000
            return HealthStatus(
                component="http_api",
                healthy=False,
                latency_ms=round(elapsed_ms, 2),
                detail=f"GET {url} -> HTTP {exc.code}",
                error=str(exc),
            )
        except Exception as exc:
            elapsed_ms = (time.perf_counter() - t0) * 1000
            return HealthStatus(
                component="http_api",
                healthy=False,
                latency_ms=round(elapsed_ms, 2),
                detail=f"GET {url} -> connection error",
                error=str(exc),
            )

    def check_data_freshness(self, max_age_seconds: int = 600) -> HealthStatus:
        """Query latest kline from MySQL, check if it's fresh enough.

        If no session_factory, return unknown status.
        """
        if self._session_factory is None:
            return HealthStatus(
                component="data_freshness",
                healthy=False,
                detail="no session_factory configured",
                error="session_factory is None",
            )
        t0 = time.perf_counter()
        try:
            rows = self._kline_repo.query_latest(
                self._session_factory,
                table_name="spot_klines",
                symbol="BTCUSDT",
                interval_code="1h",
                limit=1,
            )
            elapsed_ms = (time.perf_counter() - t0) * 1000

            if not rows:
                return HealthStatus(
                    component="data_freshness",
                    healthy=False,
                    latency_ms=round(elapsed_ms, 2),
                    detail="no kline data found",
                    error="empty result",
                )

            # The kline repo returns open_ts_ms for the most recent kline
            latest_ts_ms = rows[0].get("open_ts_ms")
            if latest_ts_ms is None:
                return HealthStatus(
                    component="data_freshness",
                    healthy=False,
                    latency_ms=round(elapsed_ms, 2),
                    detail="kline record missing open_ts_ms",
                    error="open_ts_ms is None",
                )

            now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
            age_seconds = (now_ms - latest_ts_ms) / 1000
            fresh = age_seconds <= max_age_seconds

            return HealthStatus(
                component="data_freshness",
                healthy=fresh,
                latency_ms=round(elapsed_ms, 2),
                detail=f"latest kline age={age_seconds:.0f}s, max={max_age_seconds}s",
                error=None if fresh else f"data is {age_seconds:.0f}s old (max {max_age_seconds}s)",
            )
        except Exception as exc:
            elapsed_ms = (time.perf_counter() - t0) * 1000
            return HealthStatus(
                component="data_freshness",
                healthy=False,
                latency_ms=round(elapsed_ms, 2),
                detail="freshness query failed",
                error=str(exc),
            )

    # -- aggregate -----------------------------------------------------------

    def run_all(
        self,
        http_url: str | None = None,
        max_data_age_seconds: int = 7200,
    ) -> SystemHealth:
        """Run all checks, aggregate results.

        overall_healthy = all individual checks healthy.
        """
        statuses: list[HealthStatus] = []

        statuses.append(self.check_mysql())
        statuses.append(self.check_redis())

        if http_url is not None:
            statuses.append(self.check_http_api(http_url))

        statuses.append(self.check_data_freshness(max_age_seconds=max_data_age_seconds))

        overall = all(s.healthy for s in statuses)
        checked_at = datetime.now(timezone.utc).isoformat()

        return SystemHealth(
            overall_healthy=overall,
            statuses=statuses,
            checked_at_utc=checked_at,
        )
