"""Coverage service – expose checkpoint data as a coverage summary.

Provides the last-event timestamp and status for each data stream,
allowing consumers to gauge data completeness.
"""

from __future__ import annotations

from typing import Any

from YM_data_collection.persistence.repositories.checkpoint_repo import CheckpointRepository


class CoverageService:
    """Query checkpoint data to report ingestion coverage."""

    def __init__(
        self,
        session_factory: Any,
        checkpoint_repo: CheckpointRepository,
    ) -> None:
        self._session_factory = session_factory
        self._checkpoint_repo = checkpoint_repo

    def get_coverage(
        self,
        venue: str,
        market_type: str,
        symbol: str,
        data_type: str,
        interval_code: str | None = None,
    ) -> dict[str, Any] | None:
        """Return coverage info for a single stream.

        Returns a dict with keys: venue, market_type, symbol, data_type,
        interval_code, last_event_ts_ms, status, last_success_at_utc,
        or None if no checkpoint exists.
        """
        checkpoint = self._checkpoint_repo.get(
            venue=venue,
            market_type=market_type,
            symbol=symbol,
            data_type=data_type,
            interval_code=interval_code,
        )
        if checkpoint is None:
            return None

        return {
            "venue": checkpoint.venue,
            "market_type": checkpoint.market_type,
            "symbol": checkpoint.symbol,
            "data_type": checkpoint.data_type,
            "interval_code": checkpoint.interval_code,
            "last_event_ts_ms": checkpoint.last_event_ts_ms,
            "status": checkpoint.status,
            "last_success_at_utc": checkpoint.last_success_at_utc,
        }

    def list_all_coverage(self) -> list[dict[str, Any]]:
        """Return coverage summary for all checkpoints."""
        checkpoints = self._checkpoint_repo.list_all()
        return [
            {
                "venue": cp.venue,
                "market_type": cp.market_type,
                "symbol": cp.symbol,
                "data_type": cp.data_type,
                "interval_code": cp.interval_code,
                "last_event_ts_ms": cp.last_event_ts_ms,
                "status": cp.status,
                "last_success_at_utc": cp.last_success_at_utc,
            }
            for cp in checkpoints
        ]
