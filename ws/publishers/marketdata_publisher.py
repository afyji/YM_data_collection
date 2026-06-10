"""Market data multi-topic WebSocket push module (DC-T037).

Provides pure helper functions for building market-data messages and an
async ``MarketDataPublisher`` class that broadcasts those messages through
a :class:`~YM_data_collection.ws.hub.ConnectionHub`.

Topic key format
----------------
``{topic}:{venue}:{market_type}:{symbol}``

e.g. ``marketdata.mark_price:binance:perp:BTCUSDT``
"""

from __future__ import annotations

import time
from typing import Any

from YM_data_collection.ws.hub import ConnectionHub

# ---------------------------------------------------------------------------
# Topic name constants (must match VALID_TOPICS in subscription.py)
# ---------------------------------------------------------------------------

TOPIC_MARK_PRICE = "marketdata.mark_price"
TOPIC_INDEX_PRICE = "marketdata.index_price"
TOPIC_OPEN_INTEREST = "marketdata.open_interest"
TOPIC_FUNDING_RATE = "marketdata.funding_rate"
TOPIC_DEPTH_SNAPSHOT = "marketdata.depth_snapshot"

# ---------------------------------------------------------------------------
# Generic message builder
# ---------------------------------------------------------------------------


def build_marketdata_message(
    topic: str,
    msg_type: str,
    venue: str,
    market_type: str,
    symbol: str,
    data: dict[str, Any],
) -> dict[str, Any]:
    """Build a market-data message envelope.

    Parameters
    ----------
    topic:
        Topic name, e.g. ``"marketdata.mark_price"``.
    msg_type:
        Message type – ``"update"`` for streaming topics, ``"snapshot"``
        for depth snapshots.
    venue:
        Exchange identifier, e.g. ``"binance"``.
    market_type:
        Market type, e.g. ``"perp"``.
    symbol:
        Instrument symbol, e.g. ``"BTCUSDT"``.
    data:
        Payload dictionary (passed through verbatim).

    Returns
    -------
    dict
        Complete message envelope ready for ``hub.broadcast()``.
    """
    return {
        "type": msg_type,
        "topic": topic,
        "venue": venue,
        "market_type": market_type,
        "symbol": symbol,
        "ts_ms": int(time.time() * 1000),
        "data": data,
    }


# ---------------------------------------------------------------------------
# Convenience builders for each topic
# ---------------------------------------------------------------------------


def build_mark_price_message(
    venue: str, market_type: str, symbol: str, data: dict[str, Any]
) -> dict[str, Any]:
    """Build a mark-price update message (type='update')."""
    return build_marketdata_message(
        TOPIC_MARK_PRICE, "update", venue, market_type, symbol, data
    )


def build_index_price_message(
    venue: str, market_type: str, symbol: str, data: dict[str, Any]
) -> dict[str, Any]:
    """Build an index-price update message (type='update')."""
    return build_marketdata_message(
        TOPIC_INDEX_PRICE, "update", venue, market_type, symbol, data
    )


def build_open_interest_message(
    venue: str, market_type: str, symbol: str, data: dict[str, Any]
) -> dict[str, Any]:
    """Build an open-interest update message (type='update')."""
    return build_marketdata_message(
        TOPIC_OPEN_INTEREST, "update", venue, market_type, symbol, data
    )


def build_funding_rate_message(
    venue: str, market_type: str, symbol: str, data: dict[str, Any]
) -> dict[str, Any]:
    """Build a funding-rate update message (type='update')."""
    return build_marketdata_message(
        TOPIC_FUNDING_RATE, "update", venue, market_type, symbol, data
    )


def build_depth_snapshot_message(
    venue: str, market_type: str, symbol: str, data: dict[str, Any]
) -> dict[str, Any]:
    """Build a depth-snapshot message (type='snapshot')."""
    return build_marketdata_message(
        TOPIC_DEPTH_SNAPSHOT, "snapshot", venue, market_type, symbol, data
    )


# ---------------------------------------------------------------------------
# Topic-key helper
# ---------------------------------------------------------------------------


def _topic_key(topic: str, venue: str, market_type: str, symbol: str) -> str:
    """Construct a topic key: ``{topic}:{venue}:{market_type}:{symbol}``."""
    return f"{topic}:{venue}:{market_type}:{symbol}"


# ---------------------------------------------------------------------------
# Publisher class
# ---------------------------------------------------------------------------


class MarketDataPublisher:
    """Async publisher that pushes market-data messages through a

    :class:`~YM_data_collection.ws.hub.ConnectionHub`.
    """

    def __init__(self, hub: ConnectionHub) -> None:
        self._hub = hub

    # -- mark_price ----------------------------------------------------------

    async def publish_mark_price(
        self,
        venue: str,
        market_type: str,
        symbol: str,
        data: dict[str, Any],
    ) -> None:
        msg = build_mark_price_message(venue, market_type, symbol, data)
        await self._hub.broadcast(
            _topic_key(TOPIC_MARK_PRICE, venue, market_type, symbol), msg
        )

    # -- index_price ---------------------------------------------------------

    async def publish_index_price(
        self,
        venue: str,
        market_type: str,
        symbol: str,
        data: dict[str, Any],
    ) -> None:
        msg = build_index_price_message(venue, market_type, symbol, data)
        await self._hub.broadcast(
            _topic_key(TOPIC_INDEX_PRICE, venue, market_type, symbol), msg
        )

    # -- open_interest -------------------------------------------------------

    async def publish_open_interest(
        self,
        venue: str,
        market_type: str,
        symbol: str,
        data: dict[str, Any],
    ) -> None:
        msg = build_open_interest_message(venue, market_type, symbol, data)
        await self._hub.broadcast(
            _topic_key(TOPIC_OPEN_INTEREST, venue, market_type, symbol), msg
        )

    # -- funding_rate --------------------------------------------------------

    async def publish_funding_rate(
        self,
        venue: str,
        market_type: str,
        symbol: str,
        data: dict[str, Any],
    ) -> None:
        msg = build_funding_rate_message(venue, market_type, symbol, data)
        await self._hub.broadcast(
            _topic_key(TOPIC_FUNDING_RATE, venue, market_type, symbol), msg
        )

    # -- depth_snapshot ------------------------------------------------------

    async def publish_depth_snapshot(
        self,
        venue: str,
        market_type: str,
        symbol: str,
        data: dict[str, Any],
    ) -> None:
        msg = build_depth_snapshot_message(venue, market_type, symbol, data)
        await self._hub.broadcast(
            _topic_key(TOPIC_DEPTH_SNAPSHOT, venue, market_type, symbol), msg
        )
