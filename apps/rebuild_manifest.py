"""DC-T055: Rebuild file manifests from exported parquet files.

Scans the export directory tree, parses file metadata from the path
convention and parquet content, then inserts into the manifest repository.

Supports dry-run mode where manifests are computed but NOT inserted.
Does NOT re-export data files or modify file content.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

import pandas as pd

if __package__ is None or __package__ == "":
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from YM_data_collection.apps._cli_common import (
    add_common_arguments,
    emit_final_status,
)
from YM_data_collection.domain.models import FileManifest
from YM_data_collection.persistence.datetime_utils import utc_now_sql
from YM_data_collection.utils.exit_codes import ExitCode
from YM_data_collection.utils.logging_utils import configure_logging, get_logger

APP_NAME = "rebuild_manifest"

# Intervals that indicate kline data in the path structure
_KLINE_INTERVALS = {"1m", "5m", "15m", "1h", "4h", "8h", "12h", "1d"}

# Timestamp columns checked in order for deriving start/end timestamps
_KLINE_TS_COLUMNS = ["open_ts_ms"]
_DERIVATIVE_TS_COLUMNS = ["event_ts_ms", "funding_time_ts_ms"]


def _sha256_file(path: str) -> str:
    """Compute the SHA-256 hex digest of a file."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def parse_export_file(file_path: str, export_root: str) -> FileManifest:
    """Parse an exported parquet file and produce a FileManifest.

    Path convention:
        <export_root>/<dataset_name>/<venue>/<market_type>/<symbol>/<interval_or_data_type>/<version>/data.parquet

    For kline: if the segment before <version> looks like an interval code
    (1m/5m/15m/1h/4h/8h/12h/1d), data_type='kline' and interval_code=segment.
    For derivatives: the segment is the data_type, interval_code=None.
    """
    abs_path = os.path.abspath(file_path)
    root_abs = os.path.abspath(export_root)

    # Compute relative path from export_root
    rel_path = os.path.relpath(abs_path, root_abs)

    # Split relative path into parts
    parts = rel_path.replace("\\", "/").split("/")
    # Expected: dataset_name / venue / market_type / symbol / interval_or_data_type / version / data.parquet
    if len(parts) < 7 or parts[-1] != "data.parquet":
        raise ValueError(f"File path does not match export convention: {rel_path}")

    dataset_name = parts[0]
    venue = parts[1]
    market_type = parts[2]
    symbol = parts[3]
    segment = parts[4]  # interval_code for kline, or data_type for derivatives
    version_str = parts[5]

    # Determine data_type and interval_code
    if segment in _KLINE_INTERVALS:
        data_type = "kline"
        interval_code = segment
    else:
        data_type = segment
        interval_code = None

    # Parse version string (e.g. 'v1' -> 1)
    version_int = int(version_str.lstrip("v")) if version_str.startswith("v") else int(version_str)

    # Read parquet metadata
    df = pd.read_parquet(abs_path, engine="pyarrow")
    row_count = len(df)
    file_size_bytes = os.path.getsize(abs_path)
    content_hash = _sha256_file(abs_path)

    # Derive start/end timestamps from known columns
    start_ts_ms, end_ts_ms = _extract_timestamp_range(df, data_type)

    # Partition key from time range
    partition_key = f"{start_ts_ms}_{end_ts_ms}" if row_count > 0 else None

    return FileManifest(
        dataset_name=dataset_name,
        venue=venue,
        market_type=market_type,
        symbol=symbol,
        data_type=data_type,
        interval_code=interval_code,
        time_boundary_rule=None,
        file_format="parquet",
        file_path=rel_path,
        partition_key=partition_key,
        start_ts_ms=start_ts_ms,
        end_ts_ms=end_ts_ms,
        row_count=row_count,
        file_size_bytes=file_size_bytes,
        content_hash=content_hash,
        version=version_int,
        generated_by=APP_NAME,
        generated_at_utc=datetime.now(timezone.utc),
        status="ready",
    )


def _extract_timestamp_range(df: pd.DataFrame, data_type: str) -> tuple[int, int]:
    """Extract (start_ts_ms, end_ts_ms) from the DataFrame using known timestamp columns."""
    if len(df) == 0:
        return 0, 0

    ts_columns = _KLINE_TS_COLUMNS if data_type == "kline" else _DERIVATIVE_TS_COLUMNS

    for col in ts_columns:
        if col in df.columns:
            return int(df[col].min()), int(df[col].max())

    # Fallback: no known timestamp column found
    return 0, 0


def rebuild_manifests(
    export_root: str,
    repo,
    *,
    dry_run: bool = True,
    dataset_name: str | None = None,
) -> List[FileManifest]:
    """Scan the export directory and rebuild manifests.

    Args:
        export_root: Root directory of exported parquet datasets.
        repo: ManifestRepository instance (or compatible mock).
        dry_run: If True, compute manifests but do NOT insert into repo.
        dataset_name: If provided, only process files under this dataset directory.

    Returns:
        List of computed FileManifest objects.
    """
    export_path = Path(export_root)
    manifests: List[FileManifest] = []

    # Glob for all data.parquet files
    # Structure: <dataset>/<venue>/<market_type>/<symbol>/<interval_or_data_type>/<version>/data.parquet
    # Use rglob to find all data.parquet files, then filter by depth.
    if dataset_name:
        base = export_path / dataset_name
    else:
        base = export_path

    for parquet_file in sorted(base.rglob("data.parquet")):
        rel = os.path.relpath(str(parquet_file), str(export_path))
        parts = rel.replace("\\", "/").split("/")
        # Expect exactly 7 parts: dataset/venue/mkt/symbol/segment/version/data.parquet
        if len(parts) != 7 or parts[-1] != "data.parquet":
            continue
        if dataset_name and parts[0] != dataset_name:
            continue
        try:
            manifest = parse_export_file(str(parquet_file), str(export_path))
            manifests.append(manifest)
            if not dry_run:
                repo.insert(manifest)
        except Exception:
            # Skip files that can't be parsed (log in real CLI usage)
            continue

    return manifests


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Rebuild file manifests from exported data.")
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
        help="Only rebuild manifests for this dataset (default: all).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Compute manifests but do not insert into the database.",
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

        manifests = rebuild_manifests(
            export_root=args.export_root,
            repo=repo,
            dry_run=args.dry_run,
            dataset_name=args.dataset_name,
        )

        summary = (
            f"Rebuild {'(dry-run) ' if args.dry_run else ''}complete: "
            f"{len(manifests)} manifest(s) computed"
        )
        logger.info(summary)
        print(summary)

        return emit_final_status(APP_NAME, ExitCode.SUCCESS, summary)

    except Exception as exc:
        logger = get_logger(APP_NAME)
        logger.exception("Rebuild failed: %s", exc)
        return emit_final_status(APP_NAME, ExitCode.GENERAL_FAILURE, str(exc))


if __name__ == "__main__":
    sys.exit(main())
