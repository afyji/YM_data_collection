"""DC-T056: Audit export files vs file_manifests consistency.

Checks that each manifest record matches the actual file on disk:
file exists, size, sha256 hash, row count, and timestamp range where possible.

Exit codes:
    0  SUCCESS             — all manifests passed audit
    5  DATA_VALIDATION_ERROR — one or more manifests have data drift
    6  AUDIT_FAILURE       — one or more manifest files are missing

Does NOT auto-fix audit failures.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd

if __package__ is None or __package__ == "":
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from YM_data_collection.apps._cli_common import (
    add_common_arguments,
    emit_final_status,
)
from YM_data_collection.domain.models import FileManifest
from YM_data_collection.utils.exit_codes import ExitCode
from YM_data_collection.utils.logging_utils import configure_logging, get_logger

APP_NAME = "run_manifest_audit"


def _sha256_file(path: str) -> str:
    """Compute the SHA-256 hex digest of a file."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _resolve_file_path(manifest: FileManifest, export_root: str) -> str:
    """Resolve the manifest's file_path against export_root.

    If file_path is already absolute and exists, use it directly.
    Otherwise, treat it as relative to export_root.
    """
    if os.path.isabs(manifest.file_path) and os.path.isfile(manifest.file_path):
        return manifest.file_path
    return os.path.join(export_root, manifest.file_path)


# Timestamp columns to check in order for row_count and time range verification
_KLINE_TS_COLUMNS = ["open_ts_ms"]
_DERIVATIVE_TS_COLUMNS = ["event_ts_ms", "funding_time_ts_ms"]


def audit_manifest(
    manifest: FileManifest,
    export_root: str,
) -> Tuple[List[str], bool]:
    """Audit a single manifest against the actual file on disk.

    Returns:
        (findings, passed) where findings is a list of human-readable
        issue descriptions and passed is True if no issues were found.
    """
    findings: List[str] = []

    resolved_path = _resolve_file_path(manifest, export_root)

    # 1. File existence
    if not os.path.isfile(resolved_path):
        findings.append(f"File not found: {resolved_path}")
        return findings, False

    # 2. File size
    actual_size = os.path.getsize(resolved_path)
    if actual_size != manifest.file_size_bytes:
        findings.append(
            f"File size mismatch: manifest={manifest.file_size_bytes}, actual={actual_size}"
        )

    # 3. Content hash
    actual_hash = _sha256_file(resolved_path)
    if actual_hash != manifest.content_hash:
        findings.append(
            f"Content hash mismatch: manifest={manifest.content_hash}, actual={actual_hash}"
        )

    # 4. Row count and timestamp range from parquet
    try:
        df = pd.read_parquet(resolved_path, engine="pyarrow")
        actual_rows = len(df)
        if actual_rows != manifest.row_count:
            findings.append(
                f"Row count mismatch: manifest={manifest.row_count}, actual={actual_rows}"
            )

        # 5. Timestamp range check (only for non-empty files with known columns)
        if actual_rows > 0:
            ts_columns = (
                _KLINE_TS_COLUMNS
                if manifest.data_type == "kline"
                else _DERIVATIVE_TS_COLUMNS
            )
            for col in ts_columns:
                if col in df.columns:
                    actual_start = int(df[col].min())
                    actual_end = int(df[col].max())
                    if actual_start != manifest.start_ts_ms:
                        findings.append(
                            f"Start timestamp mismatch: manifest={manifest.start_ts_ms}, "
                            f"actual={actual_start}"
                        )
                    if actual_end != manifest.end_ts_ms:
                        findings.append(
                            f"End timestamp mismatch: manifest={manifest.end_ts_ms}, "
                            f"actual={actual_end}"
                        )
                    break
    except Exception as exc:
        findings.append(f"Failed to read parquet: {exc}")

    passed = len(findings) == 0
    return findings, passed


def run_audit(
    repo,
    export_root: str,
    *,
    dataset_name: str | None = None,
    symbol: str | None = None,
    data_type: str | None = None,
) -> Tuple[ExitCode, int, int]:
    """Run audit across manifests loaded from the repository.

    Args:
        repo: ManifestRepository (or compatible mock).
        export_root: Root directory for resolving relative file paths.
        dataset_name: If provided, audit only this dataset.
        symbol: If provided, filter by symbol.
        data_type: If provided, filter by data_type.

    Returns:
        (exit_code, total_manifests, total_failures)
    """
    # Load manifests
    if dataset_name:
        manifests = repo.list_by_dataset(dataset_name)
    elif symbol:
        manifests = repo.list_by_symbol(symbol, data_type=data_type)
    else:
        # If no filter, we need a way to list all. Use dataset_name='' convention
        # or require one filter. For now, require dataset_name or symbol.
        manifests = []

    total = len(manifests)
    failures = 0
    missing_files = 0

    for mf in manifests:
        findings, passed = audit_manifest(mf, export_root)
        if not passed:
            failures += 1
            if any("not found" in f.lower() for f in findings):
                missing_files += 1

    # Determine exit code
    if failures == 0:
        exit_code = ExitCode.SUCCESS
    elif missing_files > 0:
        # At least one file is completely missing — audit failure
        exit_code = ExitCode.AUDIT_FAILURE
    else:
        # Data drift but files exist — data validation error
        exit_code = ExitCode.DATA_VALIDATION_ERROR

    return exit_code, total, failures


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Audit export files against file manifests."
    )
    add_common_arguments(
        parser,
        include_config=True,
        include_env=True,
    )
    parser.add_argument(
        "--export-root",
        required=True,
        help="Root directory of exported parquet datasets.",
    )
    parser.add_argument(
        "--dataset-name",
        default=None,
        help="Audit only this dataset.",
    )
    parser.add_argument(
        "--symbol",
        default=None,
        help="Filter audit by symbol.",
    )
    parser.add_argument(
        "--data-type",
        default=None,
        help="Filter audit by data type.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    try:
        parser = build_parser()
        args = parser.parse_args(argv)
        configure_logging(args.log_level)
        logger = get_logger(APP_NAME)

        from YM_data_collection.config.loader import load_config
        from YM_data_collection.persistence.mysql import (
            create_mysql_engine,
            create_session_factory,
        )
        from YM_data_collection.persistence.repositories.manifest_repo import (
            ManifestRepository,
        )

        config = load_config(config_path=args.config, env_name=args.env)
        engine = create_mysql_engine(config.mysql)
        session_factory = create_session_factory(engine)
        repo = ManifestRepository(session_factory)

        exit_code, total, failures = run_audit(
            repo,
            export_root=args.export_root,
            dataset_name=args.dataset_name,
            symbol=args.symbol,
            data_type=args.data_type,
        )

        if failures == 0:
            summary = f"Audit complete: {total} manifest(s) passed"
        else:
            summary = f"Audit complete: {total} manifest(s) checked, {failures} failure(s)"

        logger.info(summary)
        print(summary)

        return emit_final_status(APP_NAME, exit_code, summary)

    except Exception as exc:
        logger = get_logger(APP_NAME)
        logger.exception("Audit failed: %s", exc)
        return emit_final_status(APP_NAME, ExitCode.GENERAL_FAILURE, str(exc))


if __name__ == "__main__":
    sys.exit(main())
