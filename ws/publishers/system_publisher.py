"""System event WebSocket push publisher (DC-T038).

Provides pure helper functions for building system event messages and an
async ``SystemEventPublisher`` class that broadcasts those messages through
a :class:`~YM_data_collection.ws.hub.ConnectionHub`.

Topics
------
- ``system.quality_event``  — data quality issue notifications (per venue/symbol).
- ``system.stream_status``  — WebSocket stream lifecycle events (global).
"""

from __future__ import annotations

import time
from typing import Any

from YM_data_collection.ws.hub import ConnectionHub

# ---------------------------------------------------------------------------
# Topic name constants (must match VALID_TOPICS in subscription.py)
# ---------------------------------------------------------------------------

TOPIC_QUALITY_EVENT = "system.quality_event"
TOPIC_STREAM_STATUS = "system.stream_status"

# ---------------------------------------------------------------------------
# Expected data fields (in canonical order; absent keys are silently omitted)
# ---------------------------------------------------------------------------

_QUALITY_EVENT_FIELDS = [
    "data_type",
    "interval_code",
    "issue_type",
    "severity",
    "status",
    "symbol",
    "detected_at_utc",
    "description",
]

_STREAM_STATUS_FIELDS = [
    "stream_name",
    "status",
    "description",
]

# ---------------------------------------------------------------------------
# Pure message builders
# ---------------------------------------------------------------------------


def build_quality_event_message(
    venue: str,
    market_type: str,
    symbol: str,
    data: dict[str, Any],
) -> dict[str, Any]:
    """Build a quality-event message envelope.

    Parameters
    ----------
    venue:
        Exchange identifier, e.g. ``"binance"``.
    market_type:
        Market category, e.g. ``"perp"``.
    symbol:
        Instrument symbol, e.g. ``"BTCUSDT"``.
    data:
        Dict containing quality-event fields.  Expected keys:
        ``data_type``, ``interval_code``, ``issue_type``, ``severity``,
        ``status``, ``symbol``, ``detected_at_utc``, ``description``.
        Missing optional fields are silently omitted
        from the *data* payload rather than causing an error.

    Returns
    -------
    dict
        The full message envelope ready for JSON serialisation.
    """
    payload: dict[str, Any] = {}
    for field in _QUALITY_EVENT_FIELDS:
        if field in data:
            payload[field] = data[field]

    return {
        "type": "event",
        "topic": TOPIC_QUALITY_EVENT,
        "venue": venue,
        "market_type": market_type,
        "symbol": symbol,
        "ts_ms": int(time.time() * 1000),
        "data": payload,
    }


def build_stream_status_message(
    data: dict[str, Any],
) -> dict[str, Any]:
    """Build a stream-status message envelope.

    Stream status messages are *global* — they do not carry
    venue/market_type/symbol at the top level.

    Parameters
    ----------
    data:
        Dict containing stream-status fields.  Expected keys:
        ``stream_name``, ``status``, ``description``.  Missing optional
        fields are silently omitted from the *data* payload.

    Returns
    -------
    dict
        The full message envelope ready for JSON serialisation.
    """
    payload: dict[str, Any] = {}
    for field in _STREAM_STATUS_FIELDS:
        if field in data:
            payload[field] = data[field]

    return {
        "type": "event",
        "topic": TOPIC_STREAM_STATUS,
        "ts_ms": int(time.time() * 1000),
        "data": payload,
    }


# ---------------------------------------------------------------------------
# Topic-key helpers
# ---------------------------------------------------------------------------


def _quality_event_topic_key(venue: str, market_type: str, symbol: str) -> str:
    """Construct a quality-event topic key.

    Format: ``system.quality_event:{venue}:{market_type}:{symbol}``
    """
    return f"{TOPIC_QUALITY_EVENT}:{venue}:{market_type}:{symbol}"


# Stream status is global — no routing fields appended.
_STREAM_STATUS_TOPIC_KEY = TOPIC_STREAM_STATUS


# ---------------------------------------------------------------------------
# Publisher class
# ---------------------------------------------------------------------------


class SystemEventPublisher:
    """Async publisher that pushes system event messages through a

    :class:`~YM_data_collection.ws.hub.ConnectionHub`.
    """

    def __init__(self, hub: ConnectionHub) -> None:
        self._hub = hub

    # -- quality_event -------------------------------------------------------

    async def publish_quality_event(
        self,
        venue: str,
        market_type: str,
        symbol: str,
        data: dict[str, Any],
    ) -> None:
        """Build and broadcast a quality-event notification.

        The topic key follows the convention
        ``system.quality_event:{venue}:{market_type}:{symbol}``
        so that :class:`SubscriptionManager` can route the message to the
        correct set of WebSocket connections.
        """
        message = build_quality_event_message(venue, market_type, symbol, data)
        topic_key = _quality_event_topic_key(venue, market_type, symbol)
        await self._hub.broadcast(topic_key, message)

    # -- stream_status -------------------------------------------------------

    async def publish_stream_status(
        self,
        data: dict[str, Any],
    ) -> None:
        """Build and broadcast a stream-status notification.

        The topic key is simply ``system.stream_status`` (no routing fields)
        because stream-status events are global.
        """
        message = build_stream_status_message(data)
        await self._hub.broadcast(_STREAM_STATUS_TOPIC_KEY, message)
