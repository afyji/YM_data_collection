"""Validators for normalized derivatives data."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import List

from YM_data_collection.domain.models import (
    NormalizedFundingRate,
    NormalizedIndexPrice,
    NormalizedMarkPrice,
    NormalizedOpenInterest,
)


@dataclass
class ValidationResult:
    """Result of validating a single normalized record."""

    is_valid: bool
    issues: List[str] = field(default_factory=list)
    severity: str = "ok"  # ok | warning | error


# ---------------------------------------------------------------------------
# Severity helpers
# ---------------------------------------------------------------------------

def _worst_severity(current: str, new: str) -> str:
    """Return the more severe of two severity levels."""
    rank = {"ok": 0, "warning": 1, "error": 2}
    return new if rank.get(new, 0) > rank.get(current, 0) else current


def _add_issue(result: ValidationResult, message: str, severity: str) -> None:
    """Append an issue and upgrade severity if needed."""
    result.issues.append(message)
    result.severity = _worst_severity(result.severity, severity)
    if severity == "error":
        result.is_valid = False


# ---------------------------------------------------------------------------
# Funding rate
# ---------------------------------------------------------------------------

def validate_funding_rate(fr: NormalizedFundingRate) -> ValidationResult:
    """Validate a NormalizedFundingRate record.

    Rules:
    - funding_time_ts_ms > 0
    - |funding_rate| < 0.01 is normal, < 0.05 is warning, >= 0.05 is error
    - mark_price > 0 if present
    """
    result = ValidationResult(is_valid=True, severity="ok")

    if fr.funding_time_ts_ms <= 0:
        _add_issue(result, f"funding_time_ts_ms must be > 0, got {fr.funding_time_ts_ms}", "error")

    abs_rate = abs(fr.funding_rate)
    if abs_rate >= Decimal("0.05"):
        _add_issue(result, f"funding_rate {fr.funding_rate} is extreme (>=0.05)", "error")
    elif abs_rate >= Decimal("0.01"):
        _add_issue(result, f"funding_rate {fr.funding_rate} is elevated (>=0.01)", "warning")

    if fr.mark_price is not None and fr.mark_price <= 0:
        _add_issue(result, f"mark_price must be > 0, got {fr.mark_price}", "error")

    return result


# ---------------------------------------------------------------------------
# Open interest
# ---------------------------------------------------------------------------

def validate_open_interest(oi: NormalizedOpenInterest) -> ValidationResult:
    """Validate a NormalizedOpenInterest record.

    Rules:
    - event_ts_ms > 0
    - open_interest >= 0
    """
    result = ValidationResult(is_valid=True, severity="ok")

    if oi.event_ts_ms <= 0:
        _add_issue(result, f"event_ts_ms must be > 0, got {oi.event_ts_ms}", "error")

    if oi.open_interest < 0:
        _add_issue(result, f"open_interest must be >= 0, got {oi.open_interest}", "error")

    return result


# ---------------------------------------------------------------------------
# Mark price
# ---------------------------------------------------------------------------

def validate_mark_price(mp: NormalizedMarkPrice) -> ValidationResult:
    """Validate a NormalizedMarkPrice record.

    Rules:
    - event_ts_ms > 0
    - mark_price > 0
    """
    result = ValidationResult(is_valid=True, severity="ok")

    if mp.event_ts_ms <= 0:
        _add_issue(result, f"event_ts_ms must be > 0, got {mp.event_ts_ms}", "error")

    if mp.mark_price <= 0:
        _add_issue(result, f"mark_price must be > 0, got {mp.mark_price}", "error")

    return result


# ---------------------------------------------------------------------------
# Index price
# ---------------------------------------------------------------------------

def validate_index_price(ip: NormalizedIndexPrice) -> ValidationResult:
    """Validate a NormalizedIndexPrice record.

    Rules:
    - event_ts_ms > 0
    - index_price > 0
    """
    result = ValidationResult(is_valid=True, severity="ok")

    if ip.event_ts_ms <= 0:
        _add_issue(result, f"event_ts_ms must be > 0, got {ip.event_ts_ms}", "error")

    if ip.index_price <= 0:
        _add_issue(result, f"index_price must be > 0, got {ip.index_price}", "error")

    return result
