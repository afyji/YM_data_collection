"""Tests for config loading."""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from pydantic import ValidationError

from YM_data_collection.config.loader import load_config, resolve_secret


BASE_CONFIG_PATH = Path(__file__).resolve().parent.parent / "config" / "base.yaml"


def test_load_dev_config_defaults() -> None:
    settings = load_config(config_path=BASE_CONFIG_PATH, env_name="dev")
    assert settings.app.env == "dev"
    assert settings.service.http_port == 18081
    assert settings.query_source.allow_http_read_from_parquet is False
    assert settings.binance.spot.rest_base_url == "https://api.binance.com"
    assert settings.binance.perp.rest_base_url == "https://fapi.binance.com"
    assert settings.realtime_persistence.write_policy.kline.value == "mysql_first"
    assert settings.realtime_persistence.write_policy.mark_price.value == "redis_first"


def test_load_config_uses_base_env_when_env_not_provided() -> None:
    settings = load_config(config_path=BASE_CONFIG_PATH)
    assert settings.app.env == "dev"
    assert settings.mysql.database == "quant_data_dev"


def test_explicit_env_overrides_base_env(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "base.yaml").write_text(
        "\n".join(
            [
                "app:",
                "  app_name: test",
                "  env: dev",
                "  timezone: UTC",
                "  log_level: INFO",
                "  debug: false",
                "mysql:",
                "  host: 127.0.0.1",
                "  port: 3306",
                "  database: base_db",
                "  username: base_user",
                "  password_secret_ref: MYSQL_PASSWORD",
                "  pool_size: 1",
                "  max_overflow: 0",
                "  connect_timeout_seconds: 1",
                "  read_timeout_seconds: 1",
                "  write_timeout_seconds: 1",
                "cache:",
                "  enabled: true",
                "  backend: redis",
                "  host: 127.0.0.1",
                "  port: 6379",
                "  password_secret_ref: REDIS_PASSWORD",
                "  db: 0",
                "  ttl_seconds: 60",
                "auth:",
                "  enabled: false",
                "  http_token_secret_ref: HTTP_TOKEN",
                "  ws_token_secret_ref: WS_TOKEN",
                "  internal_service_token_secret_ref: INTERNAL_TOKEN",
                "binance:",
                "  symbols: [BTCUSDT]",
                "  intervals: [1h]",
                "ingestion:",
                "  historical_batch_size: 100",
                "  realtime_enabled: true",
                "  checkpoint_enabled: true",
                "  raw_trace_enabled: false",
                "validation:",
                "  kline_boundary_validation_enabled: true",
                "  kline_auto_repair_enabled: false",
                "  trade_quote_quantity_tolerance_bps: 5",
                "  mark_index_deviation_warning_bps: 50",
                "  depth_order_validation_enabled: true",
                "  quality_record_enabled: true",
                "depth:",
                "  default_depth_levels: 10",
                "  freshness_threshold_ms: 1000",
                "  max_snapshot_age_ms: 3000",
                "slippage:",
                "  slippage_estimation_enabled: true",
                "  default_slippage_depth_levels: 10",
                "  max_slippage_depth_levels: 20",
                "  reference_price_mode: best_bid_ask",
                "  insufficient_depth_policy: reject",
                "  allow_partial_fill_estimation: false",
                "service:",
                "  http_enabled: true",
                "  http_host: 127.0.0.1",
                "  http_port: 18081",
                "  ws_enabled: true",
                "  ws_host: 127.0.0.1",
                "  ws_port: 18082",
                "  default_page_count: 20",
                "  max_page_count: 100",
                "  download_enabled: true",
                "  http_read_timeout_seconds: 30",
                "  http_write_timeout_seconds: 30",
                "  http_keepalive_enabled: true",
                "  request_id_enabled: true",
                "  api_docs_enabled: true",
                "websocket:",
                "  heartbeat_enabled: true",
                "  heartbeat_interval_seconds: 20",
                "  pong_timeout_seconds: 10",
                "  client_idle_timeout_seconds: 60",
                "  max_subscriptions_per_connection: 10",
                "  max_connections: 10",
                "  send_queue_size: 100",
                "  message_max_bytes: 1024",
                "  snapshot_push_enabled: true",
                "  quality_event_push_enabled: true",
                "  stream_status_push_enabled: true",
                "query_source:",
                "  snapshot_cache_first_enabled: true",
                "  depth_cache_first_enabled: true",
                "  snapshot_mysql_fallback_enabled: true",
                "  depth_mysql_fallback_enabled: true",
                "  cache_backfill_on_fallback_enabled: true",
                "  allow_http_read_from_parquet: false",
                "window:",
                "  default_recent_kline_count: 200",
                "  max_recent_kline_count: 2000",
                "  http_window_refill_enabled: true",
                "  http_window_refill_limit: 5000",
                "export:",
                "  enabled: true",
                "  base_dir: exports",
                "  default_format: parquet",
                "  compression: snappy",
                "  partition_rule: by_symbol_interval_date",
                "  manifest_write_enabled: true",
                "  overwrite_same_version_enabled: true",
                "download:",
                "  download_enabled: true",
                "  download_token_required: false",
                "  download_url_expire_seconds: 600",
                "  max_download_file_size_mb: 100",
                "  download_audit_enabled: false",
                "quality:",
                "  enabled: true",
                "  warning_thresholds: {kline_gap_count: 1}",
                "  error_thresholds: {kline_gap_count: 3}",
                "  email_alert_enabled: false",
                "  email_recipients: []",
            ]
        ),
        encoding="utf-8",
    )
    (config_dir / "dev.yaml").write_text("mysql:\n  database: dev_db\n", encoding="utf-8")
    (config_dir / "prod.yaml").write_text("app:\n  env: prod\nmysql:\n  database: prod_db\n", encoding="utf-8")

    settings = load_config(config_path=config_dir / "base.yaml", env_name="prod")
    assert settings.app.env == "prod"
    assert settings.mysql.database == "prod_db"


def test_env_override_wins(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("YM_DATA_SERVICE__HTTP_PORT", "19081")
    settings = load_config(config_path=BASE_CONFIG_PATH, env_name="dev")
    assert settings.service.http_port == 19081


def test_missing_required_field_raises(tmp_path: Path) -> None:
    broken_dir = tmp_path / "config"
    broken_dir.mkdir()
    (broken_dir / "base.yaml").write_text("app:\n  app_name: test\n", encoding="utf-8")
    (broken_dir / "dev.yaml").write_text("app:\n  env: dev\n", encoding="utf-8")
    with pytest.raises(ValidationError):
        load_config(config_path=broken_dir / "base.yaml", env_name="dev")


def test_resolve_secret_reads_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MYSQL_PASSWORD", "secret")
    assert resolve_secret("MYSQL_PASSWORD", os.environ) == "secret"
