"""Handler for realtime derivatives WebSocket messages.

Processes 4 types of derivative data from Binance WS:
  - mark_price  (from markPriceUpdate event, redis_first)
  - index_price (from markPriceUpdate event, redis_first)
  - funding_rate (from markPriceUpdate event, mysql_first)
  - open_interest (from REST poll, redis_first)

The markPriceUpdate event contains mark_price, index_price, AND funding_rate
in a single message.  Mark price and index price are always cached to Redis
(latest snapshot).  Funding rate is persisted to MySQL (mysql_first) and also
cached to Redis.  Open interest data is cached to Redis only.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from YM_data_collection.cache.redis_client import RedisCacheClient
from YM_data_collection.config.models import RealtimePersistenceConfig, WritePolicy
from YM_data_collection.domain.models import (
    NormalizedFundingRate,
    NormalizedIndexPrice,
    NormalizedMarkPrice,
    NormalizedOpenInterest,
)
from YM_data_collection.normalization.derivatives_normalizer import (
    _make_instrument_code,
    _ts_ms_to_utc,
)
from YM_data_collection.persistence.repositories.marketdata_repo import (
    FundingRateRepository,
    MarkPriceRepository,
    OpenInterestRepository,
)
from YM_data_collection.validation.derivatives_validator import (
    validate_funding_rate,
    validate_index_price,
    validate_mark_price,
    validate_open_interest,
)


class RealtimeDerivativesHandler:
    """Handle realtime derivatives WS messages and persist them.

    Write policies:
      mark_price   -> redis_first  (Redis only; flush_worker handles MySQL)
      index_price  -> redis_first
      funding_rate -> mysql_first  (MySQL + Redis cache)
      open_interest -> redis_first
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
        self._redis = redis_client
        self._config = config
        self._venue = venue
        self._logger = logger or logging.getLogger(__name__)

        # Repositories for mysql_first writes
        self._funding_rate_repo = FundingRateRepository()
        self._mark_price_repo = MarkPriceRepository()
        self._open_interest_repo = OpenInterestRepository()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def handle_mark_price_update(self, data: dict) -> None:
        """Handle a markPriceUpdate event.

        Extracts mark_price, index_price, and funding_rate from the same event.
        - mark_price: write to Redis (redis_first)
        - index_price: write to Redis (redis_first)
        - funding_rate: write to MySQL (mysql_first) + cache to Redis
        """
        symbol = data.get("s", "")
        event_ts_ms = int(data.get("E", 0))

        # ---- Normalise ----
        instrument_code = _make_instrument_code(self._venue, symbol)

        mark_price_val = data.get("p")
        index_price_val = data.get("i")
        funding_rate_val = data.get("r")
        next_funding_ts = data.get("T")

        if not symbol or event_ts_ms == 0 or mark_price_val is None:
            self._logger.warning(
                "markPriceUpdate missing required fields: symbol=%s E=%s p=%s",
                symbol, event_ts_ms, mark_price_val,
            )
            return

        # Build NormalizedMarkPrice
        norm_mark = NormalizedMarkPrice(
            venue=self._venue,
            symbol=symbol,
            instrument_code=instrument_code,
            event_ts_ms=event_ts_ms,
            event_dt_utc=_ts_ms_to_utc(event_ts_ms),
            mark_price=Decimal(str(mark_price_val)),
            funding_rate=Decimal(str(funding_rate_val)) if funding_rate_val is not None else None,
            next_funding_time_ts_ms=int(next_funding_ts) if next_funding_ts is not None else None,
            source="exchange",
        )

        # Build NormalizedIndexPrice
        norm_index: NormalizedIndexPrice | None = None
        if index_price_val is not None:
            norm_index = NormalizedIndexPrice(
                venue=self._venue,
                symbol=symbol,
                instrument_code=instrument_code,
                event_ts_ms=event_ts_ms,
                event_dt_utc=_ts_ms_to_utc(event_ts_ms),
                index_price=Decimal(str(index_price_val)),
                source="exchange",
            )

        # Build NormalizedFundingRate
        norm_funding: NormalizedFundingRate | None = None
        if funding_rate_val is not None and next_funding_ts is not None:
            norm_funding = NormalizedFundingRate(
                venue=self._venue,
                symbol=symbol,
                instrument_code=instrument_code,
                funding_time_ts_ms=int(next_funding_ts),
                funding_time_dt_utc=_ts_ms_to_utc(int(next_funding_ts)),
                funding_rate=Decimal(str(funding_rate_val)),
                mark_price=Decimal(str(mark_price_val)),
                source="exchange",
            )

        # ---- Validate & persist mark_price (redis_first) ----
        mark_vr = validate_mark_price(norm_mark)
        if mark_vr.is_valid:
            try:
                self._redis.set_json(
                    "mark_price", self._venue, symbol,
                    payload=norm_mark.model_dump(mode="json"),
                )
            except Exception as exc:
                self._logger.error("Redis write failed for mark_price %s: %s", symbol, exc)
        else:
            self._logger.warning(
                "mark_price validation failed for %s: %s", symbol, mark_vr.issues
            )

        # ---- Validate & persist index_price (redis_first) ----
        if norm_index is not None:
            index_vr = validate_index_price(norm_index)
            if index_vr.is_valid:
                try:
                    self._redis.set_json(
                        "index_price", self._venue, symbol,
                        payload=norm_index.model_dump(mode="json"),
                    )
                except Exception as exc:
                    self._logger.error("Redis write failed for index_price %s: %s", symbol, exc)
            else:
                self._logger.warning(
                    "index_price validation failed for %s: %s", symbol, index_vr.issues
                )

        # ---- Validate & persist funding_rate (mysql_first + Redis cache) ----
        if norm_funding is not None:
            fund_vr = validate_funding_rate(norm_funding)
            if fund_vr.is_valid:
                # mysql_first: write to MySQL first
                try:
                    self._funding_rate_repo.upsert_batch(
                        self._session_factory, [norm_funding]
                    )
                except Exception as exc:
                    self._logger.error(
                        "MySQL write failed for funding_rate %s: %s", symbol, exc
                    )
                # Also cache to Redis
                try:
                    self._redis.set_json(
                        "funding_rate", self._venue, symbol,
                        payload=norm_funding.model_dump(mode="json"),
                    )
                except Exception as exc:
                    self._logger.error("Redis write failed for funding_rate %s: %s", symbol, exc)
            else:
                self._logger.warning(
                    "funding_rate validation failed for %s: %s", symbol, fund_vr.issues
                )

    async def handle_open_interest(self, symbol: str, data: dict) -> None:
        """Handle OI data (from REST poll or any source). Write to Redis."""
        event_ts_ms = int(data.get("timestamp", 0))
        oi_val = data.get("sumOpenInterest")
        oi_value_val = data.get("sumOpenInterestValue")

        if not symbol or event_ts_ms == 0 or oi_val is None:
            self._logger.warning(
                "open_interest missing required fields: symbol=%s ts=%s oi=%s",
                symbol, event_ts_ms, oi_val,
            )
            return

        instrument_code = _make_instrument_code(self._venue, symbol)
        norm_oi = NormalizedOpenInterest(
            venue=self._venue,
            symbol=symbol,
            instrument_code=instrument_code,
            event_ts_ms=event_ts_ms,
            event_dt_utc=_ts_ms_to_utc(event_ts_ms),
            open_interest=Decimal(str(oi_val)),
            open_interest_value=Decimal(str(oi_value_val)) if oi_value_val is not None else None,
            source="exchange",
        )

        oi_vr = validate_open_interest(norm_oi)
        if oi_vr.is_valid:
            try:
                self._redis.set_json(
                    "open_interest", self._venue, symbol,
                    payload=norm_oi.model_dump(mode="json"),
                )
            except Exception as exc:
                self._logger.error("Redis write failed for open_interest %s: %s", symbol, exc)
        else:
            self._logger.warning(
                "open_interest validation failed for %s: %s", symbol, oi_vr.issues
            )

    async def handle_message(self, stream: str, data: dict) -> None:
        """Dispatch to appropriate handler based on stream/event type."""
        event_type = data.get("e", "")

        if event_type == "markPriceUpdate":
            await self.handle_mark_price_update(data)
        elif event_type == "openInterest":
            # In case a future WS stream provides OI directly
            symbol = data.get("s", "")
            await self.handle_open_interest(symbol, data)
        else:
            self._logger.debug("Unhandled derivatives event type: %s (stream=%s)", event_type, stream)
