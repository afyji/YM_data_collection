"""Tests for rebuild_manifest and run_manifest_audit helpers.

TDD order: these tests are written FIRST (RED phase), then
implementation modules are created to make them pass (GREEN phase).
"""

from __future__ import annotations

import hashlib
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import List
from unittest.mock import MagicMock

import pandas as pd
import pytest

from YM_data_collection.domain.models import FileManifest
from YM_data_collection.utils.exit_codes import ExitCode


# ---------------------------------------------------------------------------
# Helper: create a tiny parquet file on disk for testing
# ---------------------------------------------------------------------------

_KLINE_INTERVALS = {"1m", "5m", "15m", "1h", "4h", "8h", "12h", "1d"}


def _write_kline_parquet(
    path: Path,
    *,
    n_rows: int = 5,
    start_ts_ms: int = 1700000000000,
    interval_ms: int = 60_000,
) -> pd.DataFrame:
    """Write a tiny kline-like parquet with ``open_ts_ms`` column."""
    rows = []
    for i in range(n_rows):
        ts = start_ts_ms + i * interval_ms
        rows.append({
            "open_ts_ms": ts,
            "close_ts_ms": ts + interval_ms - 1,
            "open_price": 40000.0 + i,
            "high_price": 40100.0 + i,
            "low_price": 39900.0 + i,
            "close_price": 40050.0 + i,
            "volume": 123.45,
        })
    df = pd.DataFrame(rows)
    df.to_parquet(str(path), engine="pyarrow", index=False)
    return df


def _write_derivative_parquet(
    path: Path,
    *,
    ts_col: str = "event_ts_ms",
    n_rows: int = 3,
    start_ts_ms: int = 1700000000000,
    interval_ms: int = 8 * 3600_000,
) -> pd.DataFrame:
    """Write a tiny derivative-like parquet with a timestamp column."""
    rows = []
    for i in range(n_rows):
        ts = start_ts_ms + i * interval_ms
        rows.append({
            ts_col: ts,
            "value": 0.001 + i * 0.0001,
        })
    df = pd.DataFrame(rows)
    df.to_parquet(str(path), engine="pyarrow", index=False)
    return df


def _sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


# ===========================================================================
# DC-T055: rebuild_manifest  —  parse_export_file
# ===========================================================================


class TestParseExportFile:
    """Tests for ``parse_export_file`` helper."""

    def test_kline_path_parses_correctly(self, tmp_path: Path):
        """A kline parquet file with interval segment produces the right fields."""
        from YM_data_collection.apps.rebuild_manifest import parse_export_file

        export_root = tmp_path
        # Build directory structure: <root>/<dataset>/<venue>/<mkt>/<symbol>/<interval>/<version>/data.parquet
        file_path = export_root / "ds1" / "binance" / "spot" / "BTCUSDT" / "1h" / "v1" / "data.parquet"
        file_path.parent.mkdir(parents=True, exist_ok=True)
        df = _write_kline_parquet(file_path, n_rows=5, start_ts_ms=1700000000000)

        manifest = parse_export_file(str(file_path), str(export_root))

        assert manifest.dataset_name == "ds1"
        assert manifest.venue == "binance"
        assert manifest.market_type == "spot"
        assert manifest.symbol == "BTCUSDT"
        assert manifest.data_type == "kline"
        assert manifest.interval_code == "1h"
        assert manifest.version == 1
        assert manifest.row_count == 5
        assert manifest.file_size_bytes == os.path.getsize(str(file_path))
        assert manifest.content_hash == _sha256_file(str(file_path))
        assert manifest.status == "ready"
        assert manifest.file_format == "parquet"
        # Timestamps derived from open_ts_ms
        assert manifest.start_ts_ms == int(df["open_ts_ms"].min())
        assert manifest.end_ts_ms == int(df["open_ts_ms"].max())

    def test_derivative_path_parses_correctly(self, tmp_path: Path):
        """A derivative parquet file (e.g. funding_rate) parses with data_type from segment."""
        from YM_data_collection.apps.rebuild_manifest import parse_export_file

        export_root = tmp_path
        file_path = export_root / "ds2" / "binance" / "perp" / "ETHUSDT" / "funding_rate" / "v2" / "data.parquet"
        file_path.parent.mkdir(parents=True, exist_ok=True)
        df = _write_derivative_parquet(file_path, ts_col="funding_time_ts_ms", n_rows=3)

        manifest = parse_export_file(str(file_path), str(export_root))

        assert manifest.dataset_name == "ds2"
        assert manifest.venue == "binance"
        assert manifest.market_type == "perp"
        assert manifest.symbol == "ETHUSDT"
        assert manifest.data_type == "funding_rate"
        assert manifest.interval_code is None
        assert manifest.version == 2
        assert manifest.row_count == 3

    def test_event_ts_ms_used_for_derivatives(self, tmp_path: Path):
        """Derivative data with event_ts_ms extracts timestamps from that column."""
        from YM_data_collection.apps.rebuild_manifest import parse_export_file

        export_root = tmp_path
        file_path = export_root / "ds3" / "binance" / "perp" / "BTCUSDT" / "mark_price" / "v1" / "data.parquet"
        file_path.parent.mkdir(parents=True, exist_ok=True)
        df = _write_derivative_parquet(file_path, ts_col="event_ts_ms", n_rows=4)

        manifest = parse_export_file(str(file_path), str(export_root))

        assert manifest.data_type == "mark_price"
        assert manifest.start_ts_ms == int(df["event_ts_ms"].min())
        assert manifest.end_ts_ms == int(df["event_ts_ms"].max())

    def test_empty_parquet_produces_zero_row_manifest(self, tmp_path: Path):
        """An empty parquet still produces a valid manifest with row_count=0."""
        from YM_data_collection.apps.rebuild_manifest import parse_export_file

        export_root = tmp_path
        file_path = export_root / "ds4" / "binance" / "spot" / "BTCUSDT" / "5m" / "v1" / "data.parquet"
        file_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame({"open_ts_ms": pd.Series([], dtype="int64")}).to_parquet(
            str(file_path), engine="pyarrow", index=False
        )

        manifest = parse_export_file(str(file_path), str(export_root))

        assert manifest.row_count == 0
        assert manifest.start_ts_ms == 0
        assert manifest.end_ts_ms == 0

    def test_file_path_stored_as_relative(self, tmp_path: Path):
        """The manifest file_path is stored as the relative path from export_root."""
        from YM_data_collection.apps.rebuild_manifest import parse_export_file

        export_root = tmp_path
        file_path = export_root / "my_ds" / "binance" / "spot" / "BTCUSDT" / "1h" / "v1" / "data.parquet"
        file_path.parent.mkdir(parents=True, exist_ok=True)
        _write_kline_parquet(file_path)

        manifest = parse_export_file(str(file_path), str(export_root))

        # file_path in manifest should be the path relative to export_root
        expected_rel = os.path.join("my_ds", "binance", "spot", "BTCUSDT", "1h", "v1", "data.parquet")
        assert manifest.file_path == expected_rel


# ===========================================================================
# DC-T055: rebuild_manifest  —  scan & rebuild
# ===========================================================================


class TestRebuildManifest:
    """Tests for ``rebuild_manifests`` helper."""

    def test_dry_run_does_not_call_repo_insert(self, tmp_path: Path):
        """In dry-run mode, repo.insert must never be called."""
        from YM_data_collection.apps.rebuild_manifest import rebuild_manifests

        export_root = tmp_path
        file_path = export_root / "ds1" / "binance" / "spot" / "BTCUSDT" / "1h" / "v1" / "data.parquet"
        file_path.parent.mkdir(parents=True, exist_ok=True)
        _write_kline_parquet(file_path)

        mock_repo = MagicMock()
        manifests = rebuild_manifests(str(export_root), mock_repo, dry_run=True)

        mock_repo.insert.assert_not_called()
        assert len(manifests) >= 1

    def test_non_dry_run_calls_repo_insert(self, tmp_path: Path):
        """In non-dry-run mode, repo.insert is called for each discovered parquet."""
        from YM_data_collection.apps.rebuild_manifest import rebuild_manifests

        export_root = tmp_path
        file_path = export_root / "ds1" / "binance" / "spot" / "BTCUSDT" / "1h" / "v1" / "data.parquet"
        file_path.parent.mkdir(parents=True, exist_ok=True)
        _write_kline_parquet(file_path)

        mock_repo = MagicMock()
        manifests = rebuild_manifests(str(export_root), mock_repo, dry_run=False)

        mock_repo.insert.assert_called_once()
        assert len(manifests) >= 1
        # The argument to insert should be a FileManifest
        call_args = mock_repo.insert.call_args
        assert isinstance(call_args[0][0], FileManifest)

    def test_rebuild_multiple_files(self, tmp_path: Path):
        """Multiple parquet files are all discovered and processed."""
        from YM_data_collection.apps.rebuild_manifest import rebuild_manifests

        export_root = tmp_path
        for symbol in ("BTCUSDT", "ETHUSDT"):
            fp = export_root / "ds1" / "binance" / "spot" / symbol / "1h" / "v1" / "data.parquet"
            fp.parent.mkdir(parents=True, exist_ok=True)
            _write_kline_parquet(fp)

        mock_repo = MagicMock()
        manifests = rebuild_manifests(str(export_root), mock_repo, dry_run=False)

        assert mock_repo.insert.call_count == 2
        assert len(manifests) == 2

    def test_rebuild_with_dataset_filter(self, tmp_path: Path):
        """When dataset_name filter is provided, only matching files are processed."""
        from YM_data_collection.apps.rebuild_manifest import rebuild_manifests

        export_root = tmp_path
        # Create files in two different datasets
        fp1 = export_root / "ds_alpha" / "binance" / "spot" / "BTCUSDT" / "1h" / "v1" / "data.parquet"
        fp1.parent.mkdir(parents=True, exist_ok=True)
        _write_kline_parquet(fp1)

        fp2 = export_root / "ds_beta" / "binance" / "spot" / "ETHUSDT" / "1h" / "v1" / "data.parquet"
        fp2.parent.mkdir(parents=True, exist_ok=True)
        _write_kline_parquet(fp2)

        mock_repo = MagicMock()
        manifests = rebuild_manifests(str(export_root), mock_repo, dry_run=True, dataset_name="ds_alpha")

        assert len(manifests) == 1
        assert manifests[0].dataset_name == "ds_alpha"


# ===========================================================================
# DC-T056: run_manifest_audit  —  audit_manifest
# ===========================================================================


class TestAuditManifest:
    """Tests for ``audit_manifest`` helper."""

    def test_consistent_manifest_passes(self, tmp_path: Path):
        """A manifest that matches the file on disk should pass audit."""
        from YM_data_collection.apps.run_manifest_audit import audit_manifest

        file_path = tmp_path / "ds1" / "binance" / "spot" / "BTCUSDT" / "1h" / "v1" / "data.parquet"
        file_path.parent.mkdir(parents=True, exist_ok=True)
        df = _write_kline_parquet(file_path, n_rows=5)

        manifest = FileManifest(
            dataset_name="ds1",
            venue="binance",
            market_type="spot",
            symbol="BTCUSDT",
            data_type="kline",
            interval_code="1h",
            file_format="parquet",
            file_path=str(file_path),
            partition_key="1700000000000_1700030000000",
            start_ts_ms=int(df["open_ts_ms"].min()),
            end_ts_ms=int(df["open_ts_ms"].max()),
            row_count=5,
            file_size_bytes=os.path.getsize(str(file_path)),
            content_hash=_sha256_file(str(file_path)),
            version=1,
            generated_by="test",
            generated_at_utc=datetime.now(timezone.utc),
            status="ready",
        )

        findings, passed = audit_manifest(manifest, str(tmp_path))
        assert passed is True
        assert len(findings) == 0

    def test_missing_file_fails(self, tmp_path: Path):
        """A manifest whose file does not exist on disk should fail."""
        from YM_data_collection.apps.run_manifest_audit import audit_manifest

        manifest = FileManifest(
            dataset_name="ds1",
            venue="binance",
            market_type="spot",
            symbol="BTCUSDT",
            data_type="kline",
            interval_code="1h",
            file_format="parquet",
            file_path=str(tmp_path / "nonexistent" / "data.parquet"),
            partition_key="p",
            start_ts_ms=0,
            end_ts_ms=0,
            row_count=0,
            file_size_bytes=0,
            content_hash="sha256:0",
            version=1,
            generated_by="test",
            generated_at_utc=datetime.now(timezone.utc),
            status="ready",
        )

        findings, passed = audit_manifest(manifest, str(tmp_path))
        assert passed is False
        assert any("not found" in f.lower() or "missing" in f.lower() for f in findings)

    def test_row_count_drift_fails(self, tmp_path: Path):
        """A manifest whose row_count doesn't match the parquet should fail."""
        from YM_data_collection.apps.run_manifest_audit import audit_manifest

        file_path = tmp_path / "ds1" / "binance" / "spot" / "BTCUSDT" / "1h" / "v1" / "data.parquet"
        file_path.parent.mkdir(parents=True, exist_ok=True)
        df = _write_kline_parquet(file_path, n_rows=5)

        manifest = FileManifest(
            dataset_name="ds1",
            venue="binance",
            market_type="spot",
            symbol="BTCUSDT",
            data_type="kline",
            interval_code="1h",
            file_format="parquet",
            file_path=str(file_path),
            partition_key="p",
            start_ts_ms=int(df["open_ts_ms"].min()),
            end_ts_ms=int(df["open_ts_ms"].max()),
            row_count=999,  # WRONG
            file_size_bytes=os.path.getsize(str(file_path)),
            content_hash=_sha256_file(str(file_path)),
            version=1,
            generated_by="test",
            generated_at_utc=datetime.now(timezone.utc),
            status="ready",
        )

        findings, passed = audit_manifest(manifest, str(tmp_path))
        assert passed is False
        assert any("row_count" in f.lower() or "row count" in f.lower() for f in findings)

    def test_file_size_drift_fails(self, tmp_path: Path):
        """A manifest whose file_size_bytes doesn't match should fail."""
        from YM_data_collection.apps.run_manifest_audit import audit_manifest

        file_path = tmp_path / "ds1" / "binance" / "spot" / "BTCUSDT" / "1h" / "v1" / "data.parquet"
        file_path.parent.mkdir(parents=True, exist_ok=True)
        df = _write_kline_parquet(file_path, n_rows=5)

        manifest = FileManifest(
            dataset_name="ds1",
            venue="binance",
            market_type="spot",
            symbol="BTCUSDT",
            data_type="kline",
            interval_code="1h",
            file_format="parquet",
            file_path=str(file_path),
            partition_key="p",
            start_ts_ms=int(df["open_ts_ms"].min()),
            end_ts_ms=int(df["open_ts_ms"].max()),
            row_count=5,
            file_size_bytes=99999,  # WRONG
            content_hash=_sha256_file(str(file_path)),
            version=1,
            generated_by="test",
            generated_at_utc=datetime.now(timezone.utc),
            status="ready",
        )

        findings, passed = audit_manifest(manifest, str(tmp_path))
        assert passed is False
        assert any("size" in f.lower() for f in findings)

    def test_content_hash_drift_fails(self, tmp_path: Path):
        """A manifest whose content_hash doesn't match should fail."""
        from YM_data_collection.apps.run_manifest_audit import audit_manifest

        file_path = tmp_path / "ds1" / "binance" / "spot" / "BTCUSDT" / "1h" / "v1" / "data.parquet"
        file_path.parent.mkdir(parents=True, exist_ok=True)
        df = _write_kline_parquet(file_path, n_rows=5)

        manifest = FileManifest(
            dataset_name="ds1",
            venue="binance",
            market_type="spot",
            symbol="BTCUSDT",
            data_type="kline",
            interval_code="1h",
            file_format="parquet",
            file_path=str(file_path),
            partition_key="p",
            start_ts_ms=int(df["open_ts_ms"].min()),
            end_ts_ms=int(df["open_ts_ms"].max()),
            row_count=5,
            file_size_bytes=os.path.getsize(str(file_path)),
            content_hash="sha256:deadbeef",  # WRONG
            version=1,
            generated_by="test",
            generated_at_utc=datetime.now(timezone.utc),
            status="ready",
        )

        findings, passed = audit_manifest(manifest, str(tmp_path))
        assert passed is False
        assert any("hash" in f.lower() for f in findings)

    def test_relative_file_path_resolved_against_export_root(self, tmp_path: Path):
        """If manifest.file_path is relative, it should be resolved against export_root."""
        from YM_data_collection.apps.run_manifest_audit import audit_manifest

        rel = os.path.join("ds1", "binance", "spot", "BTCUSDT", "1h", "v1", "data.parquet")
        file_path = tmp_path / rel
        file_path.parent.mkdir(parents=True, exist_ok=True)
        df = _write_kline_parquet(file_path, n_rows=5)

        manifest = FileManifest(
            dataset_name="ds1",
            venue="binance",
            market_type="spot",
            symbol="BTCUSDT",
            data_type="kline",
            interval_code="1h",
            file_format="parquet",
            file_path=rel,  # relative path
            partition_key="p",
            start_ts_ms=int(df["open_ts_ms"].min()),
            end_ts_ms=int(df["open_ts_ms"].max()),
            row_count=5,
            file_size_bytes=os.path.getsize(str(file_path)),
            content_hash=_sha256_file(str(file_path)),
            version=1,
            generated_by="test",
            generated_at_utc=datetime.now(timezone.utc),
            status="ready",
        )

        findings, passed = audit_manifest(manifest, str(tmp_path))
        assert passed is True


# ===========================================================================
# DC-T056: run_manifest_audit  —  run_audit (integration-style with mock repo)
# ===========================================================================


class TestRunAudit:
    """Tests for ``run_audit`` that loads manifests from repo and audits them."""

    def test_all_pass_returns_success(self, tmp_path: Path):
        """When all manifests pass, the result exit code is SUCCESS."""
        from YM_data_collection.apps.run_manifest_audit import run_audit

        file_path = tmp_path / "ds1" / "binance" / "spot" / "BTCUSDT" / "1h" / "v1" / "data.parquet"
        file_path.parent.mkdir(parents=True, exist_ok=True)
        df = _write_kline_parquet(file_path, n_rows=5)

        manifest = FileManifest(
            dataset_name="ds1",
            venue="binance",
            market_type="spot",
            symbol="BTCUSDT",
            data_type="kline",
            interval_code="1h",
            file_format="parquet",
            file_path=str(file_path),
            partition_key="p",
            start_ts_ms=int(df["open_ts_ms"].min()),
            end_ts_ms=int(df["open_ts_ms"].max()),
            row_count=5,
            file_size_bytes=os.path.getsize(str(file_path)),
            content_hash=_sha256_file(str(file_path)),
            version=1,
            generated_by="test",
            generated_at_utc=datetime.now(timezone.utc),
            status="ready",
        )

        mock_repo = MagicMock()
        mock_repo.list_by_dataset.return_value = [manifest]

        exit_code, total, failures = run_audit(mock_repo, str(tmp_path), dataset_name="ds1")
        assert exit_code == ExitCode.SUCCESS
        assert total == 1
        assert failures == 0

    def test_audit_failure_returns_data_validation_error(self, tmp_path: Path):
        """When a manifest fails validation, the exit code is DATA_VALIDATION_ERROR."""
        from YM_data_collection.apps.run_manifest_audit import run_audit

        file_path = tmp_path / "ds1" / "binance" / "spot" / "BTCUSDT" / "1h" / "v1" / "data.parquet"
        file_path.parent.mkdir(parents=True, exist_ok=True)
        df = _write_kline_parquet(file_path, n_rows=5)

        manifest = FileManifest(
            dataset_name="ds1",
            venue="binance",
            market_type="spot",
            symbol="BTCUSDT",
            data_type="kline",
            interval_code="1h",
            file_format="parquet",
            file_path=str(file_path),
            partition_key="p",
            start_ts_ms=int(df["open_ts_ms"].min()),
            end_ts_ms=int(df["open_ts_ms"].max()),
            row_count=999,  # WRONG — triggers data validation error
            file_size_bytes=os.path.getsize(str(file_path)),
            content_hash=_sha256_file(str(file_path)),
            version=1,
            generated_by="test",
            generated_at_utc=datetime.now(timezone.utc),
            status="ready",
        )

        mock_repo = MagicMock()
        mock_repo.list_by_dataset.return_value = [manifest]

        exit_code, total, failures = run_audit(mock_repo, str(tmp_path), dataset_name="ds1")
        assert exit_code == ExitCode.DATA_VALIDATION_ERROR
        assert failures >= 1

    def test_missing_file_returns_audit_failure(self, tmp_path: Path):
        """When file is missing entirely, the exit code is AUDIT_FAILURE."""
        from YM_data_collection.apps.run_manifest_audit import run_audit

        manifest = FileManifest(
            dataset_name="ds1",
            venue="binance",
            market_type="spot",
            symbol="BTCUSDT",
            data_type="kline",
            interval_code="1h",
            file_format="parquet",
            file_path=str(tmp_path / "no_such_file.parquet"),
            partition_key="p",
            start_ts_ms=0,
            end_ts_ms=0,
            row_count=0,
            file_size_bytes=0,
            content_hash="sha256:0",
            version=1,
            generated_by="test",
            generated_at_utc=datetime.now(timezone.utc),
            status="ready",
        )

        mock_repo = MagicMock()
        mock_repo.list_by_dataset.return_value = [manifest]

        exit_code, total, failures = run_audit(mock_repo, str(tmp_path), dataset_name="ds1")
        assert exit_code == ExitCode.AUDIT_FAILURE
