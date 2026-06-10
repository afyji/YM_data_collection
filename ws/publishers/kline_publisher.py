"""Kline WebSocket push publisher.

Builds and broadcasts normalised kline update messages to all
WebSocket clients subscribed to a given venue/market_type/symbol/interval.
"""

from __future__ import annotations

import time
from typing import Any

from YM_data_collection.ws.hub import ConnectionHub


def build_kline_message(
    venue: str,
    market_type: str,
    symbol: str,
    interval: str,
    kline_data: dict[str, Any],
) -> dict[str, Any]:
    """Pure helper that builds a kline push-message envelope.

    Parameters
    ----------
    venue:
        Exchange identifier, e.g. ``"binance"``.
    market_type:
        Market category, e.g. ``"perp"``.
    symbol:
        Instrument symbol, e.g. ``"BTCUSDT"``.
    interval:
        Candle interval, e.g. ``"1h"``.
    kline_data:
        Dict containing the normalised candle fields.  Missing optional
        fields are silently omitted from the *data* payload rather than
        causing an error.

    Returns
    -------
    dict
        The full message envelope ready for JSON serialisation.
    """
    # Define expected fields in canonical order; absent keys are skipped.
    _kline_fields = [
        "instrument_code",
        "interval_code",
        "open_ts_ms",
        "close_ts_ms",
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "volume",
        "quote_volume",
        "trade_count",
        "taker_buy_base_volume",
        "taker_buy_quote_volume",
        "is_closed",
    ]

    data: dict[str, Any] = {}
    for field in _kline_fields:
        if field in kline_data:
            data[field] = kline_data[field]

    return {
        "type": "update",
        "topic": "marketdata.kline",
        "venue": venue,
        "market_type": market_type,
        "symbol": symbol,
        "interval": interval,
        "ts_ms": int(time.time() * 1000),
        "data": data,
    }


class KlinePublisher:
    """Publishes kline updates over the WebSocket layer."""

    def __init__(self, hub: ConnectionHub) -> None:
        self._hub = hub

    async def publish_kline(
        self,
        venue: str,
        market_type: str,
        symbol: str,
        interval: str,
        kline_data: dict[str, Any],
    ) -> None:
        """Build and broadcast a kline update to subscribed connections.

        The topic key follows the convention
        ``marketdata.kline:{venue}:{market_type}:{symbol}:{interval}``
        so that :class:`SubscriptionManager` can route the message to the
        correct set of WebSocket connections.
        """
        message = build_kline_message(venue, market_type, symbol, interval, kline_data)
        topic_key = f"marketdata.kline:{venue}:{market_type}:{symbol}:{interval}"
        await self._hub.broadcast(topic_key, message)
