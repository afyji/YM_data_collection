"""Binance kline normalization: raw list -> NormalizedKline."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import List

from YM_data_collection.domain.models import NormalizedKline


def normalize_binance_kline(
    raw: list,
    venue: str,
    symbol: str,
    market_type: str,
    interval_code: str,
) -> NormalizedKline:
    """Convert a single Binance raw kline array into a NormalizedKline.

    Parameters
    ----------
    raw : list
        Binance kline array (12 elements).
    venue : str
        e.g. "binance"
    symbol : str
        e.g. "BTCUSDT"
    market_type : str
        "spot" or "perp"
    interval_code : str
        e.g. "1m", "5m", "1h"

    Returns
    -------
    NormalizedKline
    """
    instrument_code = f"crypto.{venue}.{market_type}.{symbol}"

    open_ts_ms: int = int(raw[0])
    close_ts_ms: int = int(raw[6])

    open_dt_utc = datetime.fromtimestamp(open_ts_ms / 1000.0, tz=timezone.utc)
    close_dt_utc = datetime.fromtimestamp(close_ts_ms / 1000.0, tz=timezone.utc)

    return NormalizedKline(
        venue=venue,
        symbol=symbol,
        instrument_code=instrument_code,
        interval_code=interval_code,
        open_ts_ms=open_ts_ms,
        close_ts_ms=close_ts_ms,
        open_dt_utc=open_dt_utc,
        close_dt_utc=close_dt_utc,
        open_price=Decimal(str(raw[1])),
        high_price=Decimal(str(raw[2])),
        low_price=Decimal(str(raw[3])),
        close_price=Decimal(str(raw[4])),
        volume=Decimal(str(raw[5])),
        quote_volume=Decimal(str(raw[7])),
        trade_count=int(raw[8]),
        taker_buy_base_volume=Decimal(str(raw[9])),
        taker_buy_quote_volume=Decimal(str(raw[10])),
        source="exchange",
        market_type=market_type,
    )


def normalize_binance_klines_batch(
    raws: List[list],
    venue: str,
    symbol: str,
    market_type: str,
    interval_code: str,
) -> List[NormalizedKline]:
    """Normalize multiple Binance raw kline arrays.

    Parameters
    ----------
    raws : list[list]
        List of Binance kline arrays.
    venue, symbol, market_type, interval_code
        Same as :func:`normalize_binance_kline`.

    Returns
    -------
    list[NormalizedKline]
    """
    return [
        normalize_binance_kline(raw, venue, symbol, market_type, interval_code)
        for raw in raws
    ]
