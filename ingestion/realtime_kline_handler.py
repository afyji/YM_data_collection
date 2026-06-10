"""Realtime kline WebSocket message handler.

Receives raw Binance kline WS messages, normalizes, validates,
persists to MySQL (closed klines only), and updates Redis cache.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Optional

from YM_data_collection.cache.redis_client import RedisCacheClient
from YM_data_collection.config.models import RealtimePersistenceConfig, WritePolicy
from YM_data_collection.domain.models import IngestCheckpoint, NormalizedKline
from YM_data_collection.normalization.kline_normalizer import normalize_binance_kline
from YM_data_collection.persistence.repositories.checkpoint_repo import CheckpointRepository
from YM_data_collection.persistence.repositories.marketdata_repo import KlineRepository
from YM_data_collection.validation.kline_validator import validate_kline


def _ws_kline_to_raw_array(k: dict) -> list:
    """Convert a Binance WS kline sub-dict to the 12-element REST-style array.

    REST format indices:
        0: open time          (t)
        1: open price         (o)
        2: high price         (h)
        3: low price          (l)
        4: close price        (c)
        5: volume             (v)
        6: close time         (T)
        7: quote asset volume (q)
        8: number of trades   (n)
        9: taker buy base vol (V)
       10: taker buy quote vol(Q)
       11: ignore             (B)
    """
    return [
        k["t"],
        k["o"],
        k["h"],
        k["l"],
        k["c"],
        k["v"],
        k["T"],
        k["q"],
        k["n"],
        k["V"],
        k["Q"],
        k.get("B", "0"),
    ]


def _kline_to_cache_payload(kline: NormalizedKline) -> dict[str, Any]:
    """Serialise a NormalizedKline into a JSON-safe dict for Redis."""
    return {
        "venue": kline.venue,
        "symbol": kline.symbol,
        "instrument_code": kline.instrument_code,
        "interval_code": kline.interval_code,
        "open_ts_ms": kline.open_ts_ms,
        "close_ts_ms": kline.close_ts_ms,
        "open_price": str(kline.open_price),
        "high_price": str(kline.high_price),
        "low_price": str(kline.low_price),
        "close_price": str(kline.close_price),
        "volume": str(kline.volume),
        "quote_volume": str(kline.quote_volume),
        "trade_count": kline.trade_count,
        "taker_buy_base_volume": str(kline.taker_buy_base_volume),
        "taker_buy_quote_volume": str(kline.taker_buy_quote_volume),
        "source": kline.source,
        "market_type": kline.market_type,
    }


def _table_name_for_market_type(market_type: str) -> str:
    """Return the MySQL table name for the given market type."""
    if market_type == "perp":
        return "perp_klines"
    return "spot_klines"


class RealtimeKlineHandler:
    """Handler for realtime Binance kline WebSocket messages.

    Pipeline:
        1. Parse kline data from WS message
        2. Only persist CLOSED klines (x == True) to MySQL.
           Open klines only update the Redis cache.
        3. Convert WS format → 12-element array for the normalizer
        4. Normalize → Validate → Upsert to MySQL
        5. Update Redis cache
        6. Update checkpoint
    """

    def __init__(
        self,
        session_factory: Any,
        redis_client: RedisCacheClient,
        config: RealtimePersistenceConfig,
        venue: str = "binance",
        logger: Any = None,
    ) -> None:
        self._session_factory = session_factory
        self._redis_client = redis_client
        self._config = config
        self._venue = venue
        self._logger = logger
        self._kline_repo = KlineRepository()
        self._checkpoint_repo = CheckpointRepository(session_factory)

    async def handle_message(self, market_type: str, data: dict) -> None:
        """Process a single kline WS message.

        Parameters
        ----------
        market_type : str
            ``"spot"`` or ``"perp"`` — determines the target MySQL table.
        data : dict
            The full Binance kline WS payload (contains ``"e"``, ``"k"``, etc.).
        """
        # --- 1. Extract the kline sub-object ---
        k_raw: Optional[dict] = data.get("k")
        if k_raw is None:
            if self._logger:
                self._logger.warning("kline_ws: message missing 'k' field, skipping")
            return

        is_closed: bool = k_raw.get("x", False)
        symbol: str = k_raw.get("s", "")
        interval: str = k_raw.get("i", "")
        if self._logger:
            self._logger.debug(
                "kline_ws: received market_type=%s symbol=%s interval=%s closed=%s",
                market_type,
                symbol,
                interval,
                is_closed,
            )

        # --- 2. Convert to REST-like array & normalize ---
        raw_array = _ws_kline_to_raw_array(k_raw)
        try:
            normalized: NormalizedKline = normalize_binance_kline(
                raw=raw_array,
                venue=self._venue,
                symbol=symbol,
                market_type=market_type,
                interval_code=interval,
            )
        except Exception as exc:
            if self._logger:
                self._logger.error("kline_ws: normalization failed: %s", exc)
            return

        # --- 3. Always update Redis cache (latest kline, open or closed) ---
        self._update_redis_cache(normalized, market_type)

        # --- 4. For closed klines, validate & persist ---
        if is_closed:
            validation = validate_kline(normalized, interval)

            kline_to_persist: Optional[NormalizedKline] = None
            if validation.is_valid:
                kline_to_persist = normalized
            elif validation.is_repairable and validation.repaired_kline is not None:
                kline_to_persist = validation.repaired_kline

            if kline_to_persist is not None:
                table_name = _table_name_for_market_type(market_type)
                try:
                    await asyncio.to_thread(
                        self._kline_repo.upsert_batch,
                        self._session_factory,
                        table_name,
                        [kline_to_persist],
                    )
                except Exception as exc:
                    if self._logger:
                        self._logger.error(
                            "kline_ws: MySQL upsert failed for %s/%s: %s",
                            symbol, interval, exc,
                        )
                    return

                # Update checkpoint after successful persist
                self._update_checkpoint(kline_to_persist, market_type)
            else:
                if self._logger:
                    self._logger.warning(
                        "kline_ws: validation failed, skipping persist: %s",
                        validation.issues,
                    )

    # ------------------------------------------------------------------
    # Redis cache
    # ------------------------------------------------------------------

    def _update_redis_cache(
        self, kline: NormalizedKline, market_type: str
    ) -> None:
        """Write the latest kline to Redis for fast retrieval."""
        payload = _kline_to_cache_payload(kline)
        self._redis_client.set_json(
            market_type,
            kline.symbol,
            "kline",
            kline.interval_code,
            payload=payload,
        )

    # ------------------------------------------------------------------
    # Checkpoint
    # ------------------------------------------------------------------

    def _update_checkpoint(
        self, kline: NormalizedKline, market_type: str
    ) -> None:
        """Upsert the ingestion checkpoint after a successful MySQL write."""
        now = datetime.now(timezone.utc)
        checkpoint = IngestCheckpoint(
            venue=self._venue,
            market_type=market_type,
            symbol=kline.symbol,
            data_type="kline",
            interval_code=kline.interval_code,
            last_event_ts_ms=kline.close_ts_ms,
            last_event_dt_utc=kline.close_dt_utc,
            last_kline_open_ts_ms=kline.open_ts_ms,
            status="ok",
            last_success_at_utc=now,
        )
        try:
            self._checkpoint_repo.upsert(checkpoint)
        except Exception as exc:
            if self._logger:
                self._logger.error("kline_ws: checkpoint upsert failed: %s", exc)
