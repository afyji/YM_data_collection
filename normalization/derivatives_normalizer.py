"""Normalizers for derivatives data (funding rate, open interest, mark/index price)."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

from YM_data_collection.domain.models import (
    NormalizedFundingRate,
    NormalizedIndexPrice,
    NormalizedMarkPrice,
    NormalizedOpenInterest,
)


def _ts_ms_to_utc(ts_ms: int) -> datetime:
    """Convert a millisecond timestamp to a UTC datetime."""
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)


def _make_instrument_code(venue: str, symbol: str) -> str:
    """Build the canonical instrument code for a perpetual future."""
    return f"crypto.{venue}.perp.{symbol}"


# ---------------------------------------------------------------------------
# Funding rate
# ---------------------------------------------------------------------------

def normalize_funding_rate(raw: dict, venue: str = "binance") -> NormalizedFundingRate:
    """Normalize a single Binance funding-rate record.

    Expected raw shape (from ``/fapi/v1/fundingRate``):
    {
        "symbol": "BTCUSDT",
        "fundingTime": 1698768000000,
        "fundingRate": "0.00010000",
        "markPrice": "34567.80000000"   # optional
    }
    """
    symbol = raw["symbol"]
    funding_time_ts_ms = int(raw["fundingTime"])
    funding_rate = Decimal(raw["fundingRate"])
    mark_price_raw = raw.get("markPrice")
    mark_price: Optional[Decimal] = Decimal(mark_price_raw) if mark_price_raw is not None else None

    return NormalizedFundingRate(
        venue=venue,
        symbol=symbol,
        instrument_code=_make_instrument_code(venue, symbol),
        funding_time_ts_ms=funding_time_ts_ms,
        funding_time_dt_utc=_ts_ms_to_utc(funding_time_ts_ms),
        funding_rate=funding_rate,
        mark_price=mark_price,
        source="exchange",
    )


def normalize_funding_rates_batch(raws: list[dict], venue: str = "binance") -> list[NormalizedFundingRate]:
    """Normalize a batch of funding-rate records."""
    return [normalize_funding_rate(r, venue=venue) for r in raws]


# ---------------------------------------------------------------------------
# Open interest (hist)
# ---------------------------------------------------------------------------

def normalize_open_interest_hist(raw: dict, venue: str = "binance") -> NormalizedOpenInterest:
    """Normalize a single Binance open-interest history record.

    Expected raw shape (from ``/futures/data/openInterestHist``):
    {
        "symbol": "BTCUSDT",
        "sumOpenInterest": "12345.678",
        "sumOpenInterestValue": "427654321.12",
        "timestamp": 1698768000000
    }
    """
    symbol = raw["symbol"]
    event_ts_ms = int(raw["timestamp"])
    open_interest = Decimal(raw["sumOpenInterest"])
    oi_value_raw = raw.get("sumOpenInterestValue")
    open_interest_value: Optional[Decimal] = Decimal(oi_value_raw) if oi_value_raw is not None else None

    return NormalizedOpenInterest(
        venue=venue,
        symbol=symbol,
        instrument_code=_make_instrument_code(venue, symbol),
        event_ts_ms=event_ts_ms,
        event_dt_utc=_ts_ms_to_utc(event_ts_ms),
        open_interest=open_interest,
        open_interest_value=open_interest_value,
        source="exchange",
    )


def normalize_open_interest_hist_batch(raws: list[dict], venue: str = "binance") -> list[NormalizedOpenInterest]:
    """Normalize a batch of open-interest history records."""
    return [normalize_open_interest_hist(r, venue=venue) for r in raws]


# ---------------------------------------------------------------------------
# Premium index  (splits into MarkPrice + IndexPrice)
# ---------------------------------------------------------------------------

def normalize_premium_index(
    raw: dict,
    venue: str = "binance",
) -> tuple[NormalizedMarkPrice, NormalizedIndexPrice]:
    """Normalize a single Binance premium-index / mark-price snapshot.

    Expected raw shape (from ``/fapi/v1/premiumIndex``):
    {
        "symbol": "BTCUSDT",
        "markPrice": "34567.80",
        "indexPrice": "34565.50",
        "lastFundingRate": "0.00010000",
        "nextFundingTime": 1698796800000,
        "time": 1698768001000
    }

    Returns a (NormalizedMarkPrice, NormalizedIndexPrice) tuple.
    """
    symbol = raw["symbol"]
    event_ts_ms = int(raw["time"])
    instrument_code = _make_instrument_code(venue, symbol)

    mark_price = NormalizedMarkPrice(
        venue=venue,
        symbol=symbol,
        instrument_code=instrument_code,
        event_ts_ms=event_ts_ms,
        event_dt_utc=_ts_ms_to_utc(event_ts_ms),
        mark_price=Decimal(raw["markPrice"]),
        funding_rate=Decimal(raw.get("lastFundingRate", "0")),
        next_funding_time_ts_ms=int(raw["nextFundingTime"]) if raw.get("nextFundingTime") else None,
        source="exchange",
    )

    index_price = NormalizedIndexPrice(
        venue=venue,
        symbol=symbol,
        instrument_code=instrument_code,
        event_ts_ms=event_ts_ms,
        event_dt_utc=_ts_ms_to_utc(event_ts_ms),
        index_price=Decimal(raw["indexPrice"]),
        source="exchange",
    )

    return mark_price, index_price


# ---------------------------------------------------------------------------
# Mark-price klines
# ---------------------------------------------------------------------------

def normalize_mark_price_kline(
    raw: list,
    venue: str = "binance",
    symbol: str = "",
) -> NormalizedMarkPrice:
    """Normalize a single Binance mark-price kline array.

    Expected raw shape (from ``/fapi/v1/markPriceKlines``):
    [
        1698764400000,   # 0 open_time
        "34500.00",      # 1 open
        "34600.00",      # 2 high
        "34400.00",      # 3 low
        "34567.80",      # 4 close  -> used as mark_price
        "0",             # 5 volume (always 0 for mark-price klines)
        1698767999999,   # 6 close_time
        "0",             # 7 quote_volume
        0,               # 8 trade_count
        "0",             # 9 taker_buy_base_volume
        "0",             # 10 taker_buy_quote_volume
        "0",             # 11 ignore
    ]
    """
    open_ts_ms = int(raw[0])
    close_price = Decimal(str(raw[4]))

    return NormalizedMarkPrice(
        venue=venue,
        symbol=symbol,
        instrument_code=_make_instrument_code(venue, symbol),
        event_ts_ms=open_ts_ms,
        event_dt_utc=_ts_ms_to_utc(open_ts_ms),
        mark_price=close_price,
        source="exchange",
    )


def normalize_mark_price_klines_batch(
    raws: list[list],
    venue: str = "binance",
    symbol: str = "",
) -> list[NormalizedMarkPrice]:
    """Normalize a batch of mark-price kline arrays."""
    return [normalize_mark_price_kline(r, venue=venue, symbol=symbol) for r in raws]


# ---------------------------------------------------------------------------
# Index-price klines
# ---------------------------------------------------------------------------

def normalize_index_price_kline(
    raw: list,
    venue: str = "binance",
    symbol: str = "",
) -> NormalizedIndexPrice:
    """Normalize a single Binance index-price kline array.

    Expected raw shape from ``/fapi/v1/indexPriceKlines`` mirrors kline arrays;
    the close field is used as the interval's index_price.
    """
    open_ts_ms = int(raw[0])
    close_price = Decimal(str(raw[4]))

    return NormalizedIndexPrice(
        venue=venue,
        symbol=symbol,
        instrument_code=_make_instrument_code(venue, symbol),
        event_ts_ms=open_ts_ms,
        event_dt_utc=_ts_ms_to_utc(open_ts_ms),
        index_price=close_price,
        source="exchange",
    )


def normalize_index_price_klines_batch(
    raws: list[list],
    venue: str = "binance",
    symbol: str = "",
) -> list[NormalizedIndexPrice]:
    """Normalize a batch of index-price kline arrays."""
    return [normalize_index_price_kline(r, venue=venue, symbol=symbol) for r in raws]
