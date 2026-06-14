"""Parquet export logic for standardized datasets."""

from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy.orm import Session, sessionmaker

from YM_data_collection.domain.models import FileManifest
from YM_data_collection.persistence.datetime_utils import utc_now_sql
from YM_data_collection.persistence.repositories.manifest_repo import (
    ManifestRepository,
)
from YM_data_collection.persistence.repositories.marketdata_repo import (
    FundingRateRepository,
    IndexPriceRepository,
    KlineRepository,
    MarkPriceRepository,
    OpenInterestRepository,
)


@dataclass
class ExportResult:
    """Result of a dataset export operation."""

    file_path: str
    row_count: int
    file_size_bytes: int
    content_hash: str
    manifest: FileManifest


def _sha256_file(path: str) -> str:
    """Compute the SHA-256 hex digest of a file."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


class DatasetExporter:
    """Export market-data from the DB into partitioned Parquet datasets."""

    def __init__(
        self,
        session_factory: sessionmaker[Session],
        manifest_repo: ManifestRepository,
    ) -> None:
        self._session_factory = session_factory
        self._manifest_repo = manifest_repo
        self._kline_repo = KlineRepository()
        self._funding_repo = FundingRateRepository()
        self._oi_repo = OpenInterestRepository()
        self._mark_repo = MarkPriceRepository()
        self._index_repo = IndexPriceRepository()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def export_klines(
        self,
        venue: str,
        market_type: str,
        symbol: str,
        interval_code: str,
        start_ts_ms: int,
        end_ts_ms: int,
        output_dir: str,
        dataset_name: str,
        version: str = "v1",
    ) -> ExportResult:
        """Export kline data to a Parquet file and record the manifest."""

        table_name = "spot_klines" if market_type == "spot" else "perp_klines"
        rows = self._kline_repo.query_range(
            self._session_factory,
            table_name,
            symbol,
            interval_code,
            start_ts_ms,
            end_ts_ms,
        )
        df = pd.DataFrame(rows) if rows else pd.DataFrame()

        file_path = os.path.join(
            output_dir,
            dataset_name,
            venue,
            market_type,
            symbol,
            interval_code,
            version,
            "data.parquet",
        )

        return self._write_and_manifest(
            df=df,
            file_path=file_path,
            venue=venue,
            market_type=market_type,
            symbol=symbol,
            data_type="kline",
            interval_code=interval_code,
            start_ts_ms=start_ts_ms,
            end_ts_ms=end_ts_ms,
            dataset_name=dataset_name,
            version=version,
        )

    def export_derivatives(
        self,
        venue: str,
        market_type: str,
        symbol: str,
        data_type: str,
        start_ts_ms: int,
        end_ts_ms: int,
        output_dir: str,
        dataset_name: str,
        version: str = "v1",
    ) -> ExportResult:
        """Export derivative data (mark_price, index_price, open_interest,
        funding_rate) to a Parquet file and record the manifest."""

        rows = self._query_derivatives(data_type, symbol, start_ts_ms, end_ts_ms)
        df = pd.DataFrame(rows) if rows else pd.DataFrame()

        file_path = os.path.join(
            output_dir,
            dataset_name,
            venue,
            market_type,
            symbol,
            data_type,
            version,
            "data.parquet",
        )

        return self._write_and_manifest(
            df=df,
            file_path=file_path,
            venue=venue,
            market_type=market_type,
            symbol=symbol,
            data_type=data_type,
            interval_code=None,
            start_ts_ms=start_ts_ms,
            end_ts_ms=end_ts_ms,
            dataset_name=dataset_name,
            version=version,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _query_derivatives(
        self,
        data_type: str,
        symbol: str,
        start_ts_ms: int,
        end_ts_ms: int,
    ) -> list[dict[str, Any]]:
        """Dispatch to the correct derivative repository."""

        repo_map = {
            "funding_rate": (self._funding_repo, "query_range"),
            "mark_price": (self._mark_repo, "query_range"),
            "index_price": (self._index_repo, "query_range"),
            "open_interest": (self._oi_repo, "query_range"),
        }

        if data_type not in repo_map:
            raise ValueError(
                f"Unknown derivative data_type '{data_type}'. "
                f"Expected one of: {list(repo_map)}"
            )

        repo, method_name = repo_map[data_type]
        method = getattr(repo, method_name)
        return method(self._session_factory, symbol, start_ts_ms, end_ts_ms)

    def _write_and_manifest(
        self,
        df: pd.DataFrame,
        file_path: str,
        venue: str,
        market_type: str,
        symbol: str,
        data_type: str,
        interval_code: str | None,
        start_ts_ms: int,
        end_ts_ms: int,
        dataset_name: str,
        version: str,
    ) -> ExportResult:
        """Write a DataFrame to Parquet, compute hash, and insert manifest."""

        # Ensure output directory exists
        Path(file_path).parent.mkdir(parents=True, exist_ok=True)

        # Write Parquet (overwrite if exists — idempotent re-export)
        df.to_parquet(file_path, engine="pyarrow", index=False)

        # Compute file stats
        row_count = len(df)
        file_size_bytes = os.path.getsize(file_path)
        content_hash = _sha256_file(file_path)

        # Derive partition key from time range
        partition_key = f"{start_ts_ms}_{end_ts_ms}"

        # Parse version string like 'v1' → int 1
        version_int = int(version.lstrip("v")) if isinstance(version, str) else version

        manifest = FileManifest(
            dataset_name=dataset_name,
            venue=venue,
            market_type=market_type,
            symbol=symbol,
            data_type=data_type,
            interval_code=interval_code,
            time_boundary_rule=None,
            file_format="parquet",
            file_path=file_path,
            partition_key=partition_key,
            start_ts_ms=start_ts_ms,
            end_ts_ms=end_ts_ms,
            row_count=row_count,
            file_size_bytes=file_size_bytes,
            content_hash=content_hash,
            version=version_int,
            generated_by="run_export_dataset",
            generated_at_utc=utc_now_sql(),
            status="ready",
        )

        self._manifest_repo.insert(manifest)

        return ExportResult(
            file_path=file_path,
            row_count=row_count,
            file_size_bytes=file_size_bytes,
            content_hash=content_hash,
            manifest=manifest,
        )
