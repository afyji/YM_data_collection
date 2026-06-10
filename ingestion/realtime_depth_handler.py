"""Handler for realtime Binance depth snapshot WebSocket messages.

Receives partial/full depth book messages, computes derived fields
(mid-price, spread, spread_bps), builds a NormalizedDepthSnapshot, and
writes to Redis ONLY (redis_first policy).  The flush_worker is
responsible for persisting to MySQL.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from YM_data_collection.cache.redis_client import RedisCacheClient
from YM_data_collection.config.models import RealtimePersistenceConfig
from YM_data_collection.domain.models import NormalizedDepthSnapshot


class RealtimeDepthHandler:
    """Process Binance depth-snapshot WS messages into Redis cache."""

    def __init__(
        self,
        redis_client: RedisCacheClient,
        config: RealtimePersistenceConfig,
        venue: str = "binance",
        logger: Any = None,
    ) -> None:
        self._redis = redis_client
        self._config = config
        self._venue = venue
        self._logger = logger or logging.getLogger(__name__)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def handle_message(
        self,
        market_type: str,
        symbol: str,
        data: dict[str, Any],
    ) -> None:
        """Process a depth snapshot message.

        1. Extract bids/asks from *data*
        2. Compute: best_bid, best_ask, mid_price, spread, spread_bps
        3. Build NormalizedDepthSnapshot
        4. Write to Redis cache (key: depth_snapshot:<market_type>:<symbol>)
        """
        # Spot partial-book streams use bids/asks; futures depth streams use b/a.
        bids: list[list[str]] = data.get("bids") or data.get("b") or []
        asks: list[list[str]] = data.get("asks") or data.get("a") or []

        if not bids and not asks:
            self._logger.warning(
                "Depth snapshot has no bids and no asks for %s/%s — skipping",
                market_type,
                symbol,
            )
            return

        snapshot = self._build_snapshot(market_type, symbol, bids, asks)
        await self._write_to_redis(market_type, symbol, snapshot)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _build_snapshot(
        self,
        market_type: str,
        symbol: str,
        bids: list[list[str]],
        asks: list[list[str]],
    ) -> NormalizedDepthSnapshot:
        now_ms = int(time.time() * 1000)
        now_dt = datetime.now(timezone.utc).replace(tzinfo=None)

        instrument_code = f"crypto.{self._venue}.{market_type}.{symbol}"

        # --- best bid / ask ------------------------------------------------
        if bids:
            best_bid_price = Decimal(bids[0][0])
            best_bid_qty = Decimal(bids[0][1])
        else:
            best_bid_price = Decimal("0")
            best_bid_qty = Decimal("0")

        if asks:
            best_ask_price = Decimal(asks[0][0])
            best_ask_qty = Decimal(asks[0][1])
        else:
            best_ask_price = Decimal("0")
            best_ask_qty = Decimal("0")

        # --- derived fields ------------------------------------------------
        if bids and asks:
            mid_price = (best_bid_price + best_ask_price) / 2
            spread_abs = best_ask_price - best_bid_price
            if mid_price != 0:
                spread_bps = (spread_abs / mid_price) * Decimal("10000")
            else:
                spread_bps = Decimal("0")
        else:
            mid_price = best_bid_price or best_ask_price
            spread_abs = Decimal("0")
            spread_bps = Decimal("0")

        depth_levels = max(len(bids), len(asks))

        # Normalise depth lists to [[price_str, qty_str], ...]
        bid_depth: list[list[str]] = [[p, q] for p, q in bids]
        ask_depth: list[list[str]] = [[p, q] for p, q in asks]

        return NormalizedDepthSnapshot(
            venue=self._venue,
            symbol=symbol,
            instrument_code=instrument_code,
            event_ts_ms=now_ms,
            event_dt_utc=now_dt,
            best_bid_price=best_bid_price,
            best_bid_qty=best_bid_qty,
            best_ask_price=best_ask_price,
            best_ask_qty=best_ask_qty,
            mid_price=mid_price,
            spread_abs=spread_abs,
            spread_bps=spread_bps,
            depth_levels=depth_levels,
            bid_depth_json=bid_depth,
            ask_depth_json=ask_depth,
            source="exchange",
            market_type=market_type,
        )

    async def _write_to_redis(
        self,
        market_type: str,
        symbol: str,
        snapshot: NormalizedDepthSnapshot,
    ) -> None:
        key_parts = ("depth_snapshot", market_type, symbol)
        payload = snapshot.model_dump(mode="json")
        ttl = self._config.redis_retention_after_flush_seconds or None

        self._redis.set_json(*key_parts, payload=payload, ttl_seconds=ttl)

        self._logger.debug(
            "Cached depth snapshot for %s/%s (mid=%s, spread=%s)",
            market_type,
            symbol,
            snapshot.mid_price,
            snapshot.spread_abs,
        )
