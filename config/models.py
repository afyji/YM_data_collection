"""Pydantic models for runtime configuration."""

from __future__ import annotations

from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, Field


class AppConfig(BaseModel):
    app_name: str
    env: Literal["dev", "prod"]
    timezone: str
    log_level: str
    debug: bool = False


class MySQLConfig(BaseModel):
    host: str
    port: int
    database: str
    username: str
    password_secret_ref: str
    pool_size: int = Field(ge=1)
    max_overflow: int = Field(ge=0)
    connect_timeout_seconds: int = Field(ge=1)
    read_timeout_seconds: int = Field(ge=1)
    write_timeout_seconds: int = Field(ge=1)


class CacheConfig(BaseModel):
    enabled: bool
    backend: Literal["redis"]
    host: str
    port: int
    password_secret_ref: str
    db: int = Field(ge=0)
    ttl_seconds: int = Field(ge=1)
    key_prefix: str = "ym:binance"


class AuthConfig(BaseModel):
    enabled: bool
    http_token_secret_ref: str
    ws_token_secret_ref: str
    internal_service_token_secret_ref: str


class BinanceEndpointConfig(BaseModel):
    rest_base_url: str
    ws_base_url: str


class BinanceProxyConfig(BaseModel):
    enabled: bool = False
    http_proxy: str = ""
    https_proxy: str = ""


class BinanceRateLimitConfig(BaseModel):
    spot_weight_per_minute: int = 1200
    perp_weight_per_minute: int = 2400
    min_request_interval_ms: int = 100
    backoff_on_429_seconds: int = 30
    max_concurrent_requests: int = 5


class WritePolicy(str, Enum):
    mysql_first = "mysql_first"
    redis_first = "redis_first"


class WritePolicyConfig(BaseModel):
    kline: WritePolicy = WritePolicy.mysql_first
    funding_rate: WritePolicy = WritePolicy.mysql_first
    mark_price: WritePolicy = WritePolicy.redis_first
    index_price: WritePolicy = WritePolicy.redis_first
    open_interest: WritePolicy = WritePolicy.redis_first
    depth_snapshot: WritePolicy = WritePolicy.redis_first


class RealtimePersistenceConfig(BaseModel):
    mysql_flush_interval_seconds: int = Field(default=60, ge=5)
    redis_retention_after_flush_seconds: int = Field(default=120, ge=0)
    write_policy: WritePolicyConfig = WritePolicyConfig()


class BinanceConfig(BaseModel):
    spot: BinanceEndpointConfig = BinanceEndpointConfig(
        rest_base_url="https://api.binance.com",
        ws_base_url="wss://stream.binance.com:9443/ws"
    )
    perp: BinanceEndpointConfig = BinanceEndpointConfig(
        rest_base_url="https://fapi.binance.com",
        ws_base_url="wss://fstream.binance.com/ws"
    )
    spot_enabled: bool = True
    perp_enabled: bool = True
    symbols: list[str] = Field(default_factory=lambda: ["BTCUSDT", "ETHUSDT"])
    intervals: list[str] = Field(default_factory=lambda: ["1h", "4h", "12h", "1d"])
    http_timeout_seconds: int = Field(default=15, ge=1)
    http_retry_times: int = Field(default=3, ge=0)
    ws_reconnect_backoff_seconds: int = Field(default=5, ge=1)
    ws_ping_interval_seconds: int = Field(default=20, ge=5)
    proxy: BinanceProxyConfig = BinanceProxyConfig()
    rate_limit: BinanceRateLimitConfig = BinanceRateLimitConfig()


class IngestionConfig(BaseModel):
    historical_batch_size: int = Field(ge=1)
    historical_start_ts_ms: int | None = None
    historical_end_ts_ms: int | None = None
    realtime_enabled: bool
    checkpoint_enabled: bool
    raw_trace_enabled: bool


class ValidationConfig(BaseModel):
    kline_boundary_validation_enabled: bool
    kline_auto_repair_enabled: bool
    trade_quote_quantity_tolerance_bps: int = Field(ge=0)
    mark_index_deviation_warning_bps: int = Field(ge=0)
    depth_order_validation_enabled: bool
    quality_record_enabled: bool


class DepthConfig(BaseModel):
    default_depth_levels: int = Field(ge=1)
    freshness_threshold_ms: int = Field(ge=1)
    max_snapshot_age_ms: int = Field(ge=1)


class SlippageConfig(BaseModel):
    slippage_estimation_enabled: bool
    default_slippage_depth_levels: int = Field(ge=1)
    max_slippage_depth_levels: int = Field(ge=1)
    reference_price_mode: Literal["best_bid_ask", "mid_price"]
    insufficient_depth_policy: Literal["reject", "allow_partial_fill"]
    allow_partial_fill_estimation: bool


class ServiceConfig(BaseModel):
    http_enabled: bool
    http_host: str
    http_port: int = Field(ge=1, le=65535)
    ws_enabled: bool
    ws_host: str
    ws_port: int = Field(ge=1, le=65535)
    default_page_count: int = Field(ge=1)
    max_page_count: int = Field(ge=1)
    download_enabled: bool
    http_read_timeout_seconds: int = Field(ge=1)
    http_write_timeout_seconds: int = Field(ge=1)
    http_keepalive_enabled: bool
    request_id_enabled: bool
    api_docs_enabled: bool


class WebSocketConfig(BaseModel):
    heartbeat_enabled: bool
    heartbeat_interval_seconds: int = Field(ge=1)
    pong_timeout_seconds: int = Field(ge=1)
    client_idle_timeout_seconds: int = Field(ge=1)
    max_subscriptions_per_connection: int = Field(ge=1)
    max_connections: int = Field(ge=1)
    send_queue_size: int = Field(ge=1)
    message_max_bytes: int = Field(ge=1)
    snapshot_push_enabled: bool
    quality_event_push_enabled: bool
    stream_status_push_enabled: bool


class QuerySourceConfig(BaseModel):
    snapshot_cache_first_enabled: bool
    depth_cache_first_enabled: bool
    snapshot_mysql_fallback_enabled: bool
    depth_mysql_fallback_enabled: bool
    cache_backfill_on_fallback_enabled: bool
    allow_http_read_from_parquet: bool


class WindowConfig(BaseModel):
    default_recent_kline_count: int = Field(ge=1)
    max_recent_kline_count: int = Field(ge=1)
    http_window_refill_enabled: bool
    http_window_refill_limit: int = Field(ge=1)


class ExportConfig(BaseModel):
    enabled: bool
    base_dir: str
    default_format: Literal["parquet"]
    compression: str
    partition_rule: str
    manifest_write_enabled: bool
    overwrite_same_version_enabled: bool


class DownloadConfig(BaseModel):
    download_enabled: bool
    download_token_required: bool
    download_url_expire_seconds: int = Field(ge=1)
    max_download_file_size_mb: int = Field(ge=1)
    download_audit_enabled: bool


class QualityConfig(BaseModel):
    enabled: bool
    warning_thresholds: dict[str, Any]
    error_thresholds: dict[str, Any]
    email_alert_enabled: bool
    email_recipients: list[str]


class DataCollectionConfig(BaseModel):
    app: AppConfig
    mysql: MySQLConfig
    cache: CacheConfig
    auth: AuthConfig
    binance: BinanceConfig
    ingestion: IngestionConfig
    validation: ValidationConfig
    depth: DepthConfig
    slippage: SlippageConfig
    service: ServiceConfig
    websocket: WebSocketConfig
    query_source: QuerySourceConfig
    window: WindowConfig
    export: ExportConfig
    download: DownloadConfig
    quality: QualityConfig
    realtime_persistence: RealtimePersistenceConfig = RealtimePersistenceConfig()
