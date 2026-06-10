"""Background flush worker: periodically reads high-freq data from Redis and
batch-upserts to MySQL.

Only handles redis_first data types (mark_price, index_price,
open_interest, depth_snapshot).  Low-freq types (kline, funding_rate) go
direct to MySQL and are NOT managed by this worker.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Callable

from YM_data_collection.cache.redis_client import RedisCacheClient
from YM_data_collection.config.models import RealtimePersistenceConfig, WritePolicy
from YM_data_collection.domain.models import (
    DataQualityIssue,
    NormalizedDepthSnapshot,
    NormalizedIndexPrice,
    NormalizedMarkPrice,
    NormalizedOpenInterest,
)
from YM_data_collection.persistence.repositories.marketdata_repo import (
    DepthSnapshotRepository,
    IndexPriceRepository,
    MarkPriceRepository,
    OpenInterestRepository,
)
from YM_data_collection.persistence.repositories.quality_repo import (
    QualityIssueRepository,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Data-type descriptor
# ---------------------------------------------------------------------------

class _DataTypeHandler:
    """Groups the bits needed to flush one data-type."""

    __slots__ = ("key_suffix", "model_cls", "repo", "upsert_method")

    def __init__(
        self,
        key_suffix: str,
        model_cls: type,
        repo: Any,
        upsert_method: str,
    ) -> None:
        self.key_suffix = key_suffix          # e.g. "mark_price"
        self.model_cls = model_cls            # e.g. NormalizedMarkPrice
        self.repo = repo                      # repository instance
        self.upsert_method = upsert_method    # e.g. "upsert_batch"


# ---------------------------------------------------------------------------
# FlushWorker
# ---------------------------------------------------------------------------

class FlushWorker:
    """Periodically flushes redis_first data from Redis → MySQL."""

    def __init__(
        self,
        redis_client: RedisCacheClient,
        session_factory: Any,
        config: RealtimePersistenceConfig,
        quality_repo: QualityIssueRepository | None = None,
        logger: Any = None,
    ) -> None:
        self._redis = redis_client
        self._session_factory = session_factory
        self._config = config
        self._quality_repo = quality_repo
        self._log = logger or logging.getLogger(__name__)

        # Internal state
        self._task: asyncio.Task | None = None
        self._running = False
        self._flush_count: int = 0
        self._errors_count: int = 0
        self._last_flush_at: datetime | None = None

        # Build handlers for redis_first data types only
        self._handlers = self._build_handlers()

    # -- public properties --------------------------------------------------

    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def last_flush_at(self) -> datetime | None:
        return self._last_flush_at

    @property
    def flush_count(self) -> int:
        return self._flush_count

    @property
    def errors_count(self) -> int:
        return self._errors_count

    # -- lifecycle ----------------------------------------------------------

    async def start(self) -> None:
        """Start the periodic flush loop as an asyncio task."""
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._loop(), name="flush_worker")
        self._log.info("FlushWorker started (interval=%ds)", self._config.mysql_flush_interval_seconds)

    async def stop(self) -> None:
        """Stop the flush loop gracefully."""
        self._running = False
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        self._log.info("FlushWorker stopped")

    # -- main loop ----------------------------------------------------------

    async def _loop(self) -> None:
        interval = self._config.mysql_flush_interval_seconds
        while self._running:
            try:
                await asyncio.sleep(interval)
                await self.flush_once()
            except asyncio.CancelledError:
                break
            except Exception:
                self._log.exception("Unexpected error in flush loop")
                self._errors_count += 1

    # -- single flush -------------------------------------------------------

    async def flush_once(self) -> dict[str, int]:
        """Execute one flush cycle. Returns {data_type: rows_flushed}.

        1. For each redis_first data type:
           a. Scan Redis keys matching the data type pattern
           b. Read all cached records
           c. Deserialize to domain objects
           d. Batch upsert to MySQL
           e. Set TTL on Redis keys = redis_retention_after_flush_seconds
        2. On error per type: log, record quality issue, continue to next
        """
        result: dict[str, int] = {}
        prefix = self._redis.key_builder.prefix
        retention = self._config.redis_retention_after_flush_seconds

        for handler in self._handlers:
            try:
                rows = await self._flush_data_type(handler, prefix, retention)
                result[handler.key_suffix] = rows
            except Exception as exc:
                self._log.error("Flush failed for %s: %s", handler.key_suffix, exc)
                self._errors_count += 1
                self._record_quality_issue(handler.key_suffix, str(exc))
                result[handler.key_suffix] = 0

        self._flush_count += 1
        self._last_flush_at = datetime.now(timezone.utc)
        return result

    # -- per-type flush -----------------------------------------------------

    async def _flush_data_type(
        self,
        handler: _DataTypeHandler,
        prefix: str,
        retention: int,
    ) -> int:
        """Flush one data type: scan keys → read → deserialize → upsert → set TTL."""
        pattern = f"{prefix}:{handler.key_suffix}:*"
        keys = self._scan_keys(pattern)

        if not keys:
            return 0

        # Read JSON payloads from all matched keys
        records: list[Any] = []
        for key in keys:
            raw = self._redis._client.get(key)
            if raw is None:
                continue
            import json
            try:
                payload = json.loads(raw) if isinstance(raw, str) else raw
            except (json.JSONDecodeError, TypeError):
                self._log.warning("Skipping non-JSON value at key %s", key)
                continue
            try:
                obj = handler.model_cls.model_validate(payload)
                records.append(obj)
            except Exception as exc:
                self._log.warning("Failed to deserialize key %s: %s", key, exc)

        if not records:
            return 0

        # Batch upsert to MySQL (repo methods are synchronous, run in thread)
        upsert_fn = getattr(handler.repo, handler.upsert_method)
        if handler.key_suffix == "depth_snapshot":
            rows = 0
            by_table: dict[str, list[Any]] = {
                "spot_depth_snapshots": [],
                "perp_depth_snapshots": [],
            }
            for record in records:
                table_name = (
                    "spot_depth_snapshots"
                    if getattr(record, "market_type", "perp") == "spot"
                    else "perp_depth_snapshots"
                )
                by_table[table_name].append(record)
            for table_name, table_records in by_table.items():
                if table_records:
                    table_rows = await asyncio.to_thread(
                        upsert_fn, self._session_factory, table_name, table_records
                    )
                    rows += table_rows if table_rows else 0
        else:
            rows = await asyncio.to_thread(
                upsert_fn, self._session_factory, records
            )

        # Set TTL on flushed keys so they expire after retention period
        for key in keys:
            try:
                self._redis._client.expire(key, retention)
            except Exception:
                self._log.warning("Failed to set TTL on key %s", key)

        return rows if rows else 0

    def _scan_keys(self, pattern: str) -> list[str]:
        """Scan Redis for keys matching *pattern*. Uses SCAN for robustness."""
        client = self._redis._client
        keys: list[str] = []
        cursor = 0
        while True:
            cursor, batch = client.scan(cursor=cursor, match=pattern, count=200)
            keys.extend(batch)
            if cursor == 0:
                break
        return keys

    # -- quality issue recording --------------------------------------------

    def _record_quality_issue(self, data_type: str, description: str) -> None:
        """Record a flush failure as a quality issue (best-effort)."""
        if self._quality_repo is None:
            return
        try:
            issue = DataQualityIssue(
                venue="binance",
                market_type="perp",
                symbol="*",
                data_type=data_type,
                issue_type="flush_failure",
                severity="warning",
                detected_at_utc=datetime.now(timezone.utc),
                description=description,
            )
            self._quality_repo.insert(issue)
        except Exception:
            self._log.exception("Failed to record quality issue for %s", data_type)

    # -- handler construction -----------------------------------------------

    def _build_handlers(self) -> list[_DataTypeHandler]:
        """Build handlers for data types with redis_first write policy."""
        wp = self._config.write_policy
        handlers: list[_DataTypeHandler] = []

        if wp.mark_price == WritePolicy.redis_first:
            handlers.append(_DataTypeHandler(
                key_suffix="mark_price",
                model_cls=NormalizedMarkPrice,
                repo=MarkPriceRepository(),
                upsert_method="upsert_batch",
            ))

        if wp.index_price == WritePolicy.redis_first:
            handlers.append(_DataTypeHandler(
                key_suffix="index_price",
                model_cls=NormalizedIndexPrice,
                repo=IndexPriceRepository(),
                upsert_method="upsert_batch",
            ))

        if wp.open_interest == WritePolicy.redis_first:
            handlers.append(_DataTypeHandler(
                key_suffix="open_interest",
                model_cls=NormalizedOpenInterest,
                repo=OpenInterestRepository(),
                upsert_method="upsert_batch",
            ))

        if wp.depth_snapshot == WritePolicy.redis_first:
            handlers.append(_DataTypeHandler(
                key_suffix="depth_snapshot",
                model_cls=NormalizedDepthSnapshot,
                repo=DepthSnapshotRepository(),
                upsert_method="upsert_batch",
            ))

        return handlers
