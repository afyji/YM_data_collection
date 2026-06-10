#!/usr/bin/env python3
"""DC-T047: Acceptance checklist for YM_data_collection.

Standalone script that verifies system readiness end-to-end.
Run with:  cd /mnt/mac_quant_system && python -m YM_data_collection.scripts.acceptance_checklist
"""

from __future__ import annotations

import importlib
import subprocess
import sys
import urllib.request
from pathlib import Path
from typing import Callable

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

PASS = "PASS"
FAIL = "FAIL"
SKIP = "SKIP"

results: list[tuple[str, str, str]] = []  # (status, check_id, message)


def _record(status: str, check_id: str, message: str) -> None:
    results.append((status, check_id, message))
    print(f"[{status}] {check_id}: {message}")


def _try_import(module_fqn: str) -> tuple[bool, str]:
    """Attempt to import *module_fqn*. Returns (ok, error_msg)."""
    try:
        importlib.import_module(module_fqn)
        return True, ""
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


# ---------------------------------------------------------------------------
# GROUP 1 – Configuration & Setup
# ---------------------------------------------------------------------------

def check_01_config_loads() -> None:
    """Config loads successfully (base.yaml exists and parses)."""
    try:
        import yaml  # type: ignore[import-untyped]

        from YM_data_collection.utils.constants import DEFAULT_CONFIG_PATH

        cfg_path = Path(DEFAULT_CONFIG_PATH)
        if not cfg_path.exists():
            _record(FAIL, "CHECK-01", f"base.yaml not found at {cfg_path}")
            return
        with open(cfg_path) as fh:
            yaml.safe_load(fh)
        _record(PASS, "CHECK-01", "Config loads successfully")
    except Exception as exc:
        _record(FAIL, "CHECK-01", f"Config load error - {type(exc).__name__}: {exc}")


def check_02_domain_models() -> None:
    """Domain models importable and constructible."""
    try:
        from YM_data_collection.domain.models import InstrumentInfo, NormalizedKline  # noqa: F401

        # Quick construction sanity check
        InstrumentInfo(
            venue="binance",
            market_type="spot",
            symbol="BTCUSDT",
            base_asset="BTC",
            quote_asset="USDT",
            instrument_code="BTCUSDT",
            tick_size="0.01",
            step_size="0.001",
            min_qty="0.001",
            min_notional="10",
        )
        _record(PASS, "CHECK-02", "Domain models importable and constructible")
    except Exception as exc:
        _record(FAIL, "CHECK-02", f"Domain models - {type(exc).__name__}: {exc}")


def check_03_migration_sql() -> None:
    """Migration SQL files exist and are valid SQL."""
    try:
        from YM_data_collection.utils.constants import DEFAULT_MIGRATIONS_DIR

        sql_dir = Path(DEFAULT_MIGRATIONS_DIR)
        if not sql_dir.exists():
            _record(FAIL, "CHECK-03", f"Migrations dir not found at {sql_dir}")
            return

        sql_files = sorted(sql_dir.glob("*.sql"))
        if not sql_files:
            _record(FAIL, "CHECK-03", "No SQL migration files found")
            return

        for f in sql_files:
            text = f.read_text()
            if not text.strip():
                _record(FAIL, "CHECK-03", f"Empty SQL file: {f.name}")
                return
            # Very light syntax guard — every file should contain at least one CREATE or ALTER
            upper = text.upper()
            if "CREATE" not in upper and "ALTER" not in upper and "INSERT" not in upper:
                _record(FAIL, "CHECK-03", f"SQL file lacks DDL/DML keywords: {f.name}")
                return

        _record(PASS, "CHECK-03", f"Migration SQL files exist and are valid ({len(sql_files)} files)")
    except Exception as exc:
        _record(FAIL, "CHECK-03", f"Migration SQL - {type(exc).__name__}: {exc}")


# ---------------------------------------------------------------------------
# GROUP 2 – Module Import Smoke
# ---------------------------------------------------------------------------

_ADAPTER_MODULES = [
    "YM_data_collection.adapters.binance_gateway",
    "YM_data_collection.adapters.binance_perp",
    "YM_data_collection.adapters.binance_spot",
    "YM_data_collection.adapters.binance_ws_manager",
    "YM_data_collection.adapters.rate_limiter",
]

_SERVICE_MODULES = [
    "YM_data_collection.services.coverage_service",
    "YM_data_collection.services.query_service",
    "YM_data_collection.services.slippage_service",
    "YM_data_collection.services.snapshot_service",
]

_API_ROUTE_MODULES = [
    "YM_data_collection.api.routes.klines",
    "YM_data_collection.api.routes.metadata",
    "YM_data_collection.api.routes.slippage",
    "YM_data_collection.api.routes.snapshot",
    "YM_data_collection.api.routes.system",
]

_WS_MODULES = [
    "YM_data_collection.ws.handler",
    "YM_data_collection.ws.hub",
    "YM_data_collection.ws.protocol",
    "YM_data_collection.ws.subscription",
    "YM_data_collection.ws.publishers.kline_publisher",
    "YM_data_collection.ws.publishers.marketdata_publisher",
    "YM_data_collection.ws.publishers.system_publisher",
]

_APP_MODULES = [
    "YM_data_collection.apps.sync_instruments",
    "YM_data_collection.apps.run_historical_klines_sync",
    "YM_data_collection.apps.run_historical_derivatives_sync",
    "YM_data_collection.apps.run_realtime_ingest",
    "YM_data_collection.apps.run_recovery_sync",
    "YM_data_collection.apps.run_resync_range",
    "YM_data_collection.apps.run_cache_consistency_check",
    "YM_data_collection.apps.run_data_api",
    "YM_data_collection.apps.run_export_dataset",
    "YM_data_collection.apps.run_quality_check",
    "YM_data_collection.apps.run_service_health_check",
]


def _check_import_group(check_id: str, description: str, modules: list[str]) -> None:
    failed: list[str] = []
    for mod in modules:
        ok, err = _try_import(mod)
        if not ok:
            failed.append(f"{mod} ({err})")
    if failed:
        _record(FAIL, check_id, f"{description} - failed: {'; '.join(failed)}")
    else:
        _record(PASS, check_id, f"{description} ({len(modules)} modules)")


def check_04_adapter_imports() -> None:
    _check_import_group("CHECK-04", "All adapter modules import without error", _ADAPTER_MODULES)


def check_05_service_imports() -> None:
    _check_import_group("CHECK-05", "All service modules import without error", _SERVICE_MODULES)


def check_06_api_imports() -> None:
    _check_import_group("CHECK-06", "All API route modules import without error", _API_ROUTE_MODULES)


def check_07_ws_imports() -> None:
    _check_import_group("CHECK-07", "All WS modules import without error", _WS_MODULES)


def check_08_app_imports() -> None:
    _check_import_group("CHECK-08", "All CLI app modules import without error", _APP_MODULES)


# ---------------------------------------------------------------------------
# GROUP 3 – Unit Test Suite
# ---------------------------------------------------------------------------

def check_09_pytest() -> None:
    """Run pytest with --tb=no -q, report pass/fail/error counts."""
    try:
        proc = subprocess.run(
            [sys.executable, "-m", "pytest", "--tb=no", "-q", "YM_data_collection/tests/"],
            capture_output=True,
            text=True,
            cwd="/mnt/mac_quant_system",
            timeout=120,
        )
        output = proc.stdout.strip() or proc.stderr.strip()

        # Parse the pytest summary line (last non-empty line)
        summary_line = ""
        for line in reversed(output.splitlines()):
            if line.strip():
                summary_line = line.strip()
                break

        if proc.returncode == 0:
            _record(PASS, "CHECK-09", f"pytest passed - {summary_line}")
        else:
            _record(FAIL, "CHECK-09", f"pytest failures - {summary_line}")
    except FileNotFoundError:
        _record(SKIP, "CHECK-09", "pytest not installed")
    except subprocess.TimeoutExpired:
        _record(FAIL, "CHECK-09", "pytest timed out (120s)")
    except Exception as exc:
        _record(FAIL, "CHECK-09", f"pytest error - {type(exc).__name__}: {exc}")


# ---------------------------------------------------------------------------
# GROUP 4 – Infrastructure Connectivity
# ---------------------------------------------------------------------------

def check_10_mysql() -> None:
    """MySQL connection test (skip if unavailable)."""
    try:
        import pymysql  # type: ignore[import-untyped]

        conn = pymysql.connect(
            host="127.0.0.1",
            port=3306,
            user="root",
            database="quant_data_dev",
            connect_timeout=5,
        )
        conn.close()
        _record(PASS, "CHECK-10", "MySQL connection successful")
    except ImportError:
        _record(SKIP, "CHECK-10", "pymysql not installed")
    except Exception as exc:
        _record(SKIP, "CHECK-10", f"MySQL connection - {type(exc).__name__}: {exc}")


def check_11_redis() -> None:
    """Redis connection test (skip if unavailable)."""
    try:
        import redis  # type: ignore[import-untyped]

        r = redis.Redis(host="127.0.0.1", port=6379, socket_connect_timeout=5)
        r.ping()
        _record(PASS, "CHECK-11", "Redis connection successful")
    except ImportError:
        _record(SKIP, "CHECK-11", "redis package not installed")
    except Exception as exc:
        _record(SKIP, "CHECK-11", f"Redis connection - {type(exc).__name__}: {exc}")


# ---------------------------------------------------------------------------
# GROUP 5 – API Smoke
# ---------------------------------------------------------------------------

_API_BASE = "http://localhost:18081/api/v1"


def _check_http(check_id: str, description: str, url: str) -> None:
    try:
        resp = urllib.request.urlopen(url, timeout=3)
        if resp.status == 200:
            _record(PASS, check_id, f"{description} (HTTP 200)")
        else:
            _record(FAIL, check_id, f"{description} (HTTP {resp.status})")
    except urllib.error.URLError as exc:
        _record(SKIP, check_id, f"{description} - service not running ({exc.reason})")
    except Exception as exc:
        _record(SKIP, check_id, f"{description} - {type(exc).__name__}: {exc}")


def check_12_health() -> None:
    _check_http("CHECK-12", "GET /api/v1/system/health returns 200", f"{_API_BASE}/system/health")


def check_13_runtime_status() -> None:
    _check_http("CHECK-13", "GET /api/v1/system/runtime-status returns 200", f"{_API_BASE}/system/runtime-status")


# ---------------------------------------------------------------------------
# GROUP 6 – Data Integrity
# ---------------------------------------------------------------------------

def check_14_binance_config_sections() -> None:
    """Config models.py has BinanceConfig with all expected sections."""
    try:
        from YM_data_collection.config.models import BinanceConfig

        expected_fields = {
            "spot", "perp", "spot_enabled", "perp_enabled",
            "symbols", "intervals", "http_timeout_seconds",
            "http_retry_times", "ws_reconnect_backoff_seconds",
            "ws_ping_interval_seconds", "proxy", "rate_limit",
        }
        actual_fields = set(BinanceConfig.model_fields.keys())
        missing = expected_fields - actual_fields
        if missing:
            _record(FAIL, "CHECK-14", f"BinanceConfig missing fields: {missing}")
        else:
            _record(PASS, "CHECK-14", f"BinanceConfig has all expected sections ({len(expected_fields)} fields)")
    except Exception as exc:
        _record(FAIL, "CHECK-14", f"BinanceConfig check - {type(exc).__name__}: {exc}")


def check_15_exit_codes() -> None:
    """Exit codes module has all required codes."""
    try:
        from YM_data_collection.utils.exit_codes import ExitCode

        required = {"SUCCESS", "GENERAL_FAILURE", "ARGUMENT_ERROR", "CONFIG_ERROR",
                     "DEPENDENCY_ERROR", "DATA_VALIDATION_ERROR", "AUDIT_FAILURE",
                     "FILE_EXPORT_FAILURE"}
        actual = {e.name for e in ExitCode}
        missing = required - actual
        if missing:
            _record(FAIL, "CHECK-15", f"ExitCode missing values: {missing}")
        else:
            _record(PASS, "CHECK-15", f"Exit codes module has all required codes ({len(required)} codes)")
    except Exception as exc:
        _record(FAIL, "CHECK-15", f"Exit codes check - {type(exc).__name__}: {exc}")


def check_16_constants() -> None:
    """Constants module has required venue/market_type values."""
    try:
        from YM_data_collection.utils.constants import MarketType, Venue

        venue_names = {v.value for v in Venue}
        market_names = {m.value for m in MarketType}

        if "binance" not in venue_names:
            _record(FAIL, "CHECK-16", "Venue enum missing 'binance'")
            return
        if "spot" not in market_names or "usdt_perpetual" not in market_names:
            _record(FAIL, "CHECK-16", "MarketType enum missing required values")
            return

        _record(PASS, "CHECK-16", f"Constants module has required venue/market_type values (venues={venue_names}, market_types={market_names})")
    except Exception as exc:
        _record(FAIL, "CHECK-16", f"Constants check - {type(exc).__name__}: {exc}")


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

CHECKS: list[tuple[str, Callable[[], None]]] = [
    ("GROUP 1 - Configuration & Setup", None),
    ("CHECK-01", check_01_config_loads),
    ("CHECK-02", check_02_domain_models),
    ("CHECK-03", check_03_migration_sql),
    ("GROUP 2 - Module Import Smoke", None),
    ("CHECK-04", check_04_adapter_imports),
    ("CHECK-05", check_05_service_imports),
    ("CHECK-06", check_06_api_imports),
    ("CHECK-07", check_07_ws_imports),
    ("CHECK-08", check_08_app_imports),
    ("GROUP 3 - Unit Test Suite", None),
    ("CHECK-09", check_09_pytest),
    ("GROUP 4 - Infrastructure Connectivity", None),
    ("CHECK-10", check_10_mysql),
    ("CHECK-11", check_11_redis),
    ("GROUP 5 - API Smoke", None),
    ("CHECK-12", check_12_health),
    ("CHECK-13", check_13_runtime_status),
    ("GROUP 6 - Data Integrity", None),
    ("CHECK-14", check_14_binance_config_sections),
    ("CHECK-15", check_15_exit_codes),
    ("CHECK-16", check_16_constants),
]


def main() -> None:
    print("=" * 70)
    print("YM_data_collection — Acceptance Checklist")
    print("=" * 70)
    print()

    for label, fn in CHECKS:
        if fn is None:
            print(f"\n--- {label} ---")
            continue
        fn()

    # Summary
    print()
    print("=" * 70)
    passed = sum(1 for s, _, _ in results if s == PASS)
    failed = sum(1 for s, _, _ in results if s == FAIL)
    skipped = sum(1 for s, _, _ in results if s == SKIP)
    total = len(results)
    print(f"PASSED: {passed} | FAILED: {failed} | SKIPPED: {skipped} | TOTAL: {total}")
    print("=" * 70)

    if failed > 0:
        print("\nFailed checks:")
        for s, cid, msg in results:
            if s == FAIL:
                print(f"  {cid}: {msg}")

    sys.exit(1 if failed > 0 else 0)


if __name__ == "__main__":
    main()
