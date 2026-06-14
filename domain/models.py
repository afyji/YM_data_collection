"""Core domain objects and standardized DTOs."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Optional

from pydantic import BaseModel, Field


class InstrumentInfo(BaseModel):
    """Standardized instrument metadata."""
    venue: str
    market_type: str
    symbol: str
    base_asset: str
    quote_asset: str
    instrument_code: str
    is_active: bool = True
    tick_size: Decimal
    step_size: Decimal
    min_qty: Decimal
    min_notional: Decimal
    contract_type: Optional[str] = None


class NormalizedKline(BaseModel):
    """Standardized kline bar."""
    venue: str
    symbol: str
    instrument_code: str
    interval_code: str
    open_ts_ms: int
    close_ts_ms: int
    open_dt_utc: datetime
    close_dt_utc: datetime
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    close_price: Decimal
    volume: Decimal
    quote_volume: Decimal
    trade_count: int
    taker_buy_base_volume: Decimal
    taker_buy_quote_volume: Decimal
    source: str = "exchange"
    market_type: str = ""


class NormalizedFundingRate(BaseModel):
    """Standardized funding rate record."""
    venue: str
    symbol: str
    instrument_code: str
    funding_time_ts_ms: int
    funding_time_dt_utc: datetime
    funding_rate: Decimal
    mark_price: Optional[Decimal] = None
    source: str = "exchange"


class NormalizedOpenInterest(BaseModel):
    """Standardized open interest record."""
    venue: str
    symbol: str
    instrument_code: str
    event_ts_ms: int
    event_dt_utc: datetime
    open_interest: Decimal
    open_interest_value: Optional[Decimal] = None
    source: str = "exchange"


class NormalizedMarkPrice(BaseModel):
    """Standardized mark price record."""
    venue: str
    symbol: str
    instrument_code: str
    event_ts_ms: int
    event_dt_utc: datetime
    mark_price: Decimal
    funding_rate: Optional[Decimal] = None
    next_funding_time_ts_ms: Optional[int] = None
    source: str = "exchange"


class NormalizedIndexPrice(BaseModel):
    """Standardized index price record."""
    venue: str
    symbol: str
    instrument_code: str
    event_ts_ms: int
    event_dt_utc: datetime
    index_price: Decimal
    source: str = "exchange"


class NormalizedDepthSnapshot(BaseModel):
    """Standardized depth snapshot."""
    venue: str
    symbol: str
    instrument_code: str
    event_ts_ms: int
    event_dt_utc: datetime
    best_bid_price: Decimal
    best_bid_qty: Decimal
    best_ask_price: Decimal
    best_ask_qty: Decimal
    mid_price: Decimal
    spread_abs: Decimal
    spread_bps: Decimal
    depth_levels: int
    bid_depth_json: list  # [[price, qty], ...]
    ask_depth_json: list  # [[price, qty], ...]
    source: str = "exchange"
    market_type: str = ""


class IngestCheckpoint(BaseModel):
    """Sync progress checkpoint."""
    venue: str
    market_type: str
    symbol: str
    data_type: str
    interval_code: Optional[str] = None
    last_event_ts_ms: Optional[int] = None
    last_event_dt_utc: Optional[datetime] = None
    last_trade_id: Optional[int] = None
    last_kline_open_ts_ms: Optional[int] = None
    status: str = "ok"
    last_success_at_utc: Optional[datetime] = None
    last_error_message: Optional[str] = None


class DataQualityIssue(BaseModel):
    """Data quality issue record."""
    venue: str
    market_type: str
    symbol: str
    data_type: str
    interval_code: Optional[str] = None
    issue_type: str
    severity: str
    detected_at_utc: datetime
    start_ts_ms: Optional[int] = None
    end_ts_ms: Optional[int] = None
    description: str
    status: str = "open"
    resolution_note: Optional[str] = None


class FileManifest(BaseModel):
    """Exported dataset file manifest."""

    id: Optional[int] = None
    dataset_name: str
    venue: str
    market_type: str
    symbol: str
    data_type: str
    interval_code: Optional[str] = None
    time_boundary_rule: Optional[str] = None
    file_format: str = "parquet"
    file_path: str
    partition_key: Optional[str] = None
    start_ts_ms: int
    end_ts_ms: int
    row_count: int
    file_size_bytes: int
    content_hash: str
    version: int
    generated_by: str
    generated_at_utc: datetime
    status: str = "ready"
