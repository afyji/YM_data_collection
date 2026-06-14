"""Repository for file_manifests table operations."""

from __future__ import annotations

from typing import List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

from YM_data_collection.domain.models import FileManifest
from YM_data_collection.persistence.datetime_utils import normalize_sql_params
from YM_data_collection.persistence.mysql import session_scope


_MANIFEST_FIELDS = (
    "dataset_name, venue, market_type, symbol, data_type, interval_code, "
    "time_boundary_rule, file_format, file_path, partition_key, "
    "start_ts_ms, end_ts_ms, row_count, file_size_bytes, "
    "content_hash, version, generated_by, generated_at_utc, status"
)

_INSERT_SQL = text(
    f"INSERT INTO file_manifests ({_MANIFEST_FIELDS}) "
    f"VALUES (:dataset_name, :venue, :market_type, :symbol, :data_type, "
    f":interval_code, :time_boundary_rule, :file_format, :file_path, "
    f":partition_key, :start_ts_ms, :end_ts_ms, :row_count, "
    f":file_size_bytes, :content_hash, :version, :generated_by, "
    f":generated_at_utc, :status)"
)

_GET_BY_PATH_SQL = text(
    f"SELECT id, {_MANIFEST_FIELDS} FROM file_manifests "
    f"WHERE file_path = :file_path"
)

_LIST_BY_DATASET_SQL = text(
    f"SELECT id, {_MANIFEST_FIELDS} FROM file_manifests "
    f"WHERE dataset_name = :dataset_name "
    f"ORDER BY start_ts_ms"
)

_LIST_BY_SYMBOL_SQL = text(
    f"SELECT id, {_MANIFEST_FIELDS} FROM file_manifests "
    f"WHERE symbol = :symbol AND data_type = :data_type "
    f"ORDER BY start_ts_ms"
)

_LIST_BY_SYMBOL_ONLY_SQL = text(
    f"SELECT id, {_MANIFEST_FIELDS} FROM file_manifests "
    f"WHERE symbol = :symbol "
    f"ORDER BY start_ts_ms"
)


def _row_to_manifest(row: tuple) -> FileManifest:
    """Map a result row to a FileManifest domain object."""
    return FileManifest(
        dataset_name=row[1],
        venue=row[2],
        market_type=row[3],
        symbol=row[4],
        data_type=row[5],
        interval_code=row[6],
        time_boundary_rule=row[7],
        file_format=row[8],
        file_path=row[9],
        partition_key=row[10],
        start_ts_ms=row[11],
        end_ts_ms=row[12],
        row_count=row[13],
        file_size_bytes=row[14],
        content_hash=row[15],
        version=row[16],
        generated_by=row[17],
        generated_at_utc=row[18],
        status=row[19],
    )


class ManifestRepository:
    """CRUD operations for file_manifests using raw SQL."""

    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        self._session_factory = session_factory

    def insert(self, manifest: FileManifest) -> int:
        """Insert a new file manifest and return its auto-generated id."""
        params = {
            "dataset_name": manifest.dataset_name,
            "venue": manifest.venue,
            "market_type": manifest.market_type,
            "symbol": manifest.symbol,
            "data_type": manifest.data_type,
            "interval_code": manifest.interval_code,
            "time_boundary_rule": manifest.time_boundary_rule,
            "file_format": manifest.file_format,
            "file_path": manifest.file_path,
            "partition_key": manifest.partition_key,
            "start_ts_ms": manifest.start_ts_ms,
            "end_ts_ms": manifest.end_ts_ms,
            "row_count": manifest.row_count,
            "file_size_bytes": manifest.file_size_bytes,
            "content_hash": manifest.content_hash,
            "version": manifest.version,
            "generated_by": manifest.generated_by,
            "generated_at_utc": manifest.generated_at_utc,
            "status": manifest.status,
        }
        with session_scope(self._session_factory) as session:
            result = session.execute(_INSERT_SQL, normalize_sql_params(params))
            return result.lastrowid

    def get_by_path(self, file_path: str) -> Optional[FileManifest]:
        """Fetch a manifest by its file path."""
        with session_scope(self._session_factory) as session:
            row = session.execute(
                _GET_BY_PATH_SQL, {"file_path": file_path}
            ).fetchone()
            if row is None:
                return None
            return _row_to_manifest(row)

    def list_by_dataset(self, dataset_name: str) -> List[FileManifest]:
        """Return all manifests for a given dataset."""
        with session_scope(self._session_factory) as session:
            rows = session.execute(
                _LIST_BY_DATASET_SQL, {"dataset_name": dataset_name}
            ).fetchall()
            return [_row_to_manifest(r) for r in rows]

    def list_by_symbol(
        self,
        symbol: str,
        data_type: Optional[str] = None,
    ) -> List[FileManifest]:
        """Return manifests for a symbol, optionally filtered by data_type."""
        with session_scope(self._session_factory) as session:
            if data_type is not None:
                rows = session.execute(
                    _LIST_BY_SYMBOL_SQL,
                    {"symbol": symbol, "data_type": data_type},
                ).fetchall()
            else:
                rows = session.execute(
                    _LIST_BY_SYMBOL_ONLY_SQL,
                    {"symbol": symbol},
                ).fetchall()
            return [_row_to_manifest(r) for r in rows]
