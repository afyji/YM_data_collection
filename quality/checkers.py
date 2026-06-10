"""Quality checking implementations for market data."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

from YM_data_collection.domain.models import DataQualityIssue
from YM_data_collection.persistence.mysql import session_scope
from YM_data_collection.persistence.repositories.quality_repo import (
    QualityIssueRepository,
)

# ---------------------------------------------------------------------------
# Interval -> milliseconds mapping
# ---------------------------------------------------------------------------

INTERVAL_MS: dict[str, int] = {
    "1m": 60_000,
    "5m": 300_000,
    "15m": 900_000,
    "1h": 3_600_000,
    "4h": 14_400_000,
    "1d": 86_400_000,
}

# ---------------------------------------------------------------------------
# Market type -> kline table name
# ---------------------------------------------------------------------------

_KLINE_TABLE: dict[str, str] = {
    "spot": "spot_klines",
    "perp": "perp_klines",
}

_DEPTH_TABLE: dict[str, str] = {
    "spot": "spot_depth_snapshots",
    "perp": "perp_depth_snapshots",
}


# ---------------------------------------------------------------------------
# QualityCheckResult
# ---------------------------------------------------------------------------


@dataclass
class QualityCheckResult:
    """Outcome of a single quality check."""

    check_type: str  # 'gap', 'duplicate', 'boundary', 'freshness'
    passed: bool
    issues: list[DataQualityIssue] = field(default_factory=list)
    summary: str = ""


# ---------------------------------------------------------------------------
# QualityChecker
# ---------------------------------------------------------------------------


class QualityChecker:
    """Run data-quality checks against stored market data."""

    def __init__(
        self,
        session_factory: sessionmaker[Session],
        quality_repo: QualityIssueRepository,
    ) -> None:
        self._session_factory = session_factory
        self._quality_repo = quality_repo

    # -- helpers ---------------------------------------------------------------

    @staticmethod
    def _kline_table(market_type: str) -> str:
        tbl = _KLINE_TABLE.get(market_type)
        if tbl is None:
            raise ValueError(f"Unknown market_type for klines: {market_type!r}")
        return tbl

    @staticmethod
    def _depth_table(market_type: str) -> str:
        tbl = _DEPTH_TABLE.get(market_type)
        if tbl is None:
            raise ValueError(f"Unknown market_type for depth: {market_type!r}")
        return tbl

    def _now_utc(self) -> datetime:
        return datetime.now(timezone.utc).replace(tzinfo=None)

    def _insert_issue(self, issue: DataQualityIssue) -> int:
        """Persist a quality issue via the repository."""
        return self._quality_repo.insert(issue)

    # -- kline queries ---------------------------------------------------------

    def _query_klines(
        self,
        table_name: str,
        symbol: str,
        interval_code: str,
        start_ts_ms: int,
        end_ts_ms: int,
    ) -> list[dict[str, Any]]:
        """Return klines within [start_ts_ms, end_ts_ms] ordered by open_ts_ms."""
        sql = text(
            f"SELECT * FROM {table_name} "
            "WHERE symbol = :symbol AND interval_code = :interval_code "
            "AND open_ts_ms >= :start_ts_ms AND open_ts_ms <= :end_ts_ms "
            "ORDER BY open_ts_ms ASC"
        )
        with session_scope(self._session_factory) as session:
            rows = session.execute(
                sql,
                {
                    "symbol": symbol,
                    "interval_code": interval_code,
                    "start_ts_ms": start_ts_ms,
                    "end_ts_ms": end_ts_ms,
                },
            ).mappings().all()
            return [dict(r) for r in rows]

    def _query_latest_depth(
        self,
        table_name: str,
        symbol: str,
    ) -> list[dict[str, Any]]:
        """Return the most recent depth snapshot for *symbol*."""
        sql = text(
            f"SELECT * FROM {table_name} "
            "WHERE symbol = :symbol "
            "ORDER BY event_ts_ms DESC LIMIT 1"
        )
        with session_scope(self._session_factory) as session:
            rows = session.execute(
                sql, {"symbol": symbol}
            ).mappings().all()
            return [dict(r) for r in rows]

    # -- check implementations -------------------------------------------------

    def check_kline_gaps(
        self,
        venue: str,
        market_type: str,
        symbol: str,
        interval_code: str,
        start_ts_ms: int,
        end_ts_ms: int,
    ) -> QualityCheckResult:
        """Check for missing kline bars in the time range.

        Expected: continuous sequence of bars at *interval_code* spacing.
        Gaps = missing open_ts_ms values.
        """
        interval_ms = INTERVAL_MS.get(interval_code)
        if interval_ms is None:
            return QualityCheckResult(
                check_type="gap",
                passed=False,
                summary=f"Unknown interval_code: {interval_code!r}",
            )

        table_name = self._kline_table(market_type)
        rows = self._query_klines(
            table_name, symbol, interval_code, start_ts_ms, end_ts_ms
        )

        if not rows:
            # No data at all in the range — report entire range as gap
            issue = DataQualityIssue(
                venue=venue,
                market_type=market_type,
                symbol=symbol,
                data_type="kline",
                interval_code=interval_code,
                issue_type="gap",
                severity="warning",
                detected_at_utc=self._now_utc(),
                start_ts_ms=start_ts_ms,
                end_ts_ms=end_ts_ms,
                description=(
                    f"No kline data found for {interval_code} interval "
                    f"from {start_ts_ms} to {end_ts_ms}"
                ),
            )
            self._insert_issue(issue)
            return QualityCheckResult(
                check_type="gap",
                passed=False,
                issues=[issue],
                summary="No kline data found in range",
            )

        # Build set of actual open_ts_ms values
        actual_ts = {r["open_ts_ms"] for r in rows}

        # Anchor expected sequence from the first actual bar's open_ts_ms.
        # This keeps gap detection independent of boundary alignment
        # (boundary misalignment is a separate check).
        first_ts = min(actual_ts)
        expected_ts = set()
        ts = first_ts
        while ts <= end_ts_ms:
            expected_ts.add(ts)
            ts += interval_ms
        # Also go backwards from first_ts to start_ts_ms
        ts = first_ts - interval_ms
        while ts >= start_ts_ms:
            expected_ts.add(ts)
            ts -= interval_ms

        missing = sorted(expected_ts - actual_ts)

        if not missing:
            return QualityCheckResult(
                check_type="gap",
                passed=True,
                summary="No gaps found",
            )

        # Group consecutive missing bars into ranges
        issues: list[DataQualityIssue] = []
        gap_start = missing[0]
        gap_prev = missing[0]
        for m in missing[1:]:
            if m == gap_prev + interval_ms:
                gap_prev = m
            else:
                issue = DataQualityIssue(
                    venue=venue,
                    market_type=market_type,
                    symbol=symbol,
                    data_type="kline",
                    interval_code=interval_code,
                    issue_type="gap",
                    severity="warning",
                    detected_at_utc=self._now_utc(),
                    start_ts_ms=gap_start,
                    end_ts_ms=gap_prev,
                    description=(
                        f"Missing {int((gap_prev - gap_start) / interval_ms) + 1} "
                        f"kline bar(s) for {interval_code} interval "
                        f"from {gap_start} to {gap_prev}"
                    ),
                )
                issues.append(issue)
                self._insert_issue(issue)
                gap_start = m
                gap_prev = m

        # Final group
        issue = DataQualityIssue(
            venue=venue,
            market_type=market_type,
            symbol=symbol,
            data_type="kline",
            interval_code=interval_code,
            issue_type="gap",
            severity="warning",
            detected_at_utc=self._now_utc(),
            start_ts_ms=gap_start,
            end_ts_ms=gap_prev,
            description=(
                f"Missing {int((gap_prev - gap_start) / interval_ms) + 1} "
                f"kline bar(s) for {interval_code} interval "
                f"from {gap_start} to {gap_prev}"
            ),
        )
        issues.append(issue)
        self._insert_issue(issue)

        total_missing = len(missing)
        return QualityCheckResult(
            check_type="gap",
            passed=False,
            issues=issues,
            summary=f"Found {total_missing} missing bar(s) in {len(issues)} gap range(s)",
        )

    def check_kline_duplicates(
        self,
        venue: str,
        market_type: str,
        symbol: str,
        interval_code: str,
        start_ts_ms: int,
        end_ts_ms: int,
    ) -> QualityCheckResult:
        """Check for duplicate kline bars (same open_ts_ms)."""
        table_name = self._kline_table(market_type)
        sql = text(
            f"SELECT open_ts_ms, COUNT(*) as cnt FROM {table_name} "
            "WHERE symbol = :symbol AND interval_code = :interval_code "
            "AND open_ts_ms >= :start_ts_ms AND open_ts_ms <= :end_ts_ms "
            "GROUP BY open_ts_ms HAVING cnt > 1 "
            "ORDER BY open_ts_ms ASC"
        )
        with session_scope(self._session_factory) as session:
            dup_rows = session.execute(
                sql,
                {
                    "symbol": symbol,
                    "interval_code": interval_code,
                    "start_ts_ms": start_ts_ms,
                    "end_ts_ms": end_ts_ms,
                },
            ).mappings().all()

        if not dup_rows:
            return QualityCheckResult(
                check_type="duplicate",
                passed=True,
                summary="No duplicates found",
            )

        issues: list[DataQualityIssue] = []
        for row in dup_rows:
            ts = row["open_ts_ms"]
            cnt = row["cnt"]
            issue = DataQualityIssue(
                venue=venue,
                market_type=market_type,
                symbol=symbol,
                data_type="kline",
                interval_code=interval_code,
                issue_type="duplicate",
                severity="warning",
                detected_at_utc=self._now_utc(),
                start_ts_ms=ts,
                end_ts_ms=ts,
                description=(
                    f"Duplicate kline bar at open_ts_ms={ts} "
                    f"({cnt} rows) for {interval_code} interval"
                ),
            )
            issues.append(issue)
            self._insert_issue(issue)

        total_dupes = sum(r["cnt"] - 1 for r in dup_rows)
        return QualityCheckResult(
            check_type="duplicate",
            passed=False,
            issues=issues,
            summary=(
                f"Found {len(dup_rows)} open_ts_ms value(s) with "
                f"{total_dupes} duplicate row(s)"
            ),
        )

    def check_kline_boundary(
        self,
        venue: str,
        market_type: str,
        symbol: str,
        interval_code: str,
        start_ts_ms: int,
        end_ts_ms: int,
    ) -> QualityCheckResult:
        """Check kline open_ts_ms alignment to interval boundaries.

        E.g. 1h klines should have open_ts_ms % 3600000 == 0.
        """
        interval_ms = INTERVAL_MS.get(interval_code)
        if interval_ms is None:
            return QualityCheckResult(
                check_type="boundary",
                passed=False,
                summary=f"Unknown interval_code: {interval_code!r}",
            )

        table_name = self._kline_table(market_type)
        rows = self._query_klines(
            table_name, symbol, interval_code, start_ts_ms, end_ts_ms
        )

        misaligned = [r for r in rows if r["open_ts_ms"] % interval_ms != 0]

        if not misaligned:
            return QualityCheckResult(
                check_type="boundary",
                passed=True,
                summary="All bars aligned to interval boundaries",
            )

        issues: list[DataQualityIssue] = []
        for row in misaligned:
            ts = row["open_ts_ms"]
            remainder = ts % interval_ms
            issue = DataQualityIssue(
                venue=venue,
                market_type=market_type,
                symbol=symbol,
                data_type="kline",
                interval_code=interval_code,
                issue_type="boundary_error",
                severity="warning",
                detected_at_utc=self._now_utc(),
                start_ts_ms=ts,
                end_ts_ms=ts,
                description=(
                    f"Kline open_ts_ms={ts} not aligned to {interval_code} "
                    f"boundary (remainder={remainder}ms)"
                ),
            )
            issues.append(issue)
            self._insert_issue(issue)

        return QualityCheckResult(
            check_type="boundary",
            passed=False,
            issues=issues,
            summary=(
                f"Found {len(misaligned)} misaligned bar(s) "
                f"out of {len(rows)} total"
            ),
        )

    def check_depth_freshness(
        self,
        venue: str,
        market_type: str,
        symbol: str,
        max_age_seconds: int = 300,
    ) -> QualityCheckResult:
        """Check that most recent depth snapshot is not stale (< max_age_seconds old)."""
        table_name = self._depth_table(market_type)
        rows = self._query_latest_depth(table_name, symbol)

        if not rows:
            issue = DataQualityIssue(
                venue=venue,
                market_type=market_type,
                symbol=symbol,
                data_type="depth_snapshot",
                interval_code=None,
                issue_type="no_data",
                severity="error",
                detected_at_utc=self._now_utc(),
                description=f"No depth snapshot data found for {symbol}",
            )
            self._insert_issue(issue)
            return QualityCheckResult(
                check_type="freshness",
                passed=False,
                issues=[issue],
                summary="No depth snapshot data found",
            )

        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        latest_ts_ms = rows[0]["event_ts_ms"]
        age_seconds = (now_ms - latest_ts_ms) / 1000.0

        if age_seconds <= max_age_seconds:
            return QualityCheckResult(
                check_type="freshness",
                passed=True,
                summary=(
                    f"Latest depth snapshot is {age_seconds:.1f}s old "
                    f"(within {max_age_seconds}s threshold)"
                ),
            )

        issue = DataQualityIssue(
            venue=venue,
            market_type=market_type,
            symbol=symbol,
            data_type="depth_snapshot",
            interval_code=None,
            issue_type="stale",
            severity="warning",
            detected_at_utc=self._now_utc(),
            start_ts_ms=latest_ts_ms,
            end_ts_ms=now_ms,
            description=(
                f"Latest depth snapshot for {symbol} is "
                f"{age_seconds:.1f}s old (threshold={max_age_seconds}s)"
            ),
        )
        self._insert_issue(issue)
        return QualityCheckResult(
            check_type="freshness",
            passed=False,
            issues=[issue],
            summary=(
                f"Stale depth snapshot: {age_seconds:.1f}s old "
                f"(max allowed={max_age_seconds}s)"
            ),
        )
