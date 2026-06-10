"""Config loading and secret resolution."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict

from YM_data_collection.config.models import DataCollectionConfig


class EnvironmentOverrides(BaseSettings):
    """Environment-sourced overrides using nested keys."""

    model_config = SettingsConfigDict(
        env_prefix="YM_DATA_",
        env_nested_delimiter="__",
        extra="ignore",
    )

    app: dict[str, Any] | None = None
    mysql: dict[str, Any] | None = None
    cache: dict[str, Any] | None = None
    auth: dict[str, Any] | None = None
    binance: dict[str, Any] | None = None
    ingestion: dict[str, Any] | None = None
    validation: dict[str, Any] | None = None
    depth: dict[str, Any] | None = None
    slippage: dict[str, Any] | None = None
    service: dict[str, Any] | None = None
    websocket: dict[str, Any] | None = None
    query_source: dict[str, Any] | None = None
    window: dict[str, Any] | None = None
    export: dict[str, Any] | None = None
    download: dict[str, Any] | None = None
    quality: dict[str, Any] | None = None


def read_yaml_file(path: str | Path) -> dict[str, Any]:
    """Read a YAML file into a dict."""

    target = Path(path)
    with target.open("r", encoding="utf-8") as handle:
        loaded = yaml.safe_load(handle) or {}
    if not isinstance(loaded, dict):
        raise ValueError(f"Config file must contain a mapping: {target}")
    return loaded


def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Deep merge nested config dictionaries."""

    merged = dict(base)
    for key, value in override.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def load_config(
    *,
    config_path: str | Path,
    env_name: str | None = None,
    explicit_overrides: dict[str, Any] | None = None,
) -> DataCollectionConfig:
    """Load base config, then env overlay, then env var and explicit overrides.

    Precedence:
    1. ``config_path`` base YAML
    2. ``env_name`` overlay if provided, otherwise ``app.env`` from the base YAML
    3. ``YM_DATA_*`` environment variable overrides
    4. ``explicit_overrides``
    """

    config_path = Path(config_path)
    config_dir = config_path.parent
    base_config = read_yaml_file(config_path)

    target_env = env_name or base_config.get("app", {}).get("env", "dev")
    env_path = config_dir / f"{target_env}.yaml"
    merged = deep_merge(base_config, read_yaml_file(env_path))

    env_overrides = EnvironmentOverrides().model_dump(exclude_none=True)
    if env_overrides:
        merged = deep_merge(merged, env_overrides)

    if explicit_overrides:
        merged = deep_merge(merged, explicit_overrides)

    try:
        return DataCollectionConfig.model_validate(merged)
    except ValidationError:
        raise


def resolve_secret(secret_ref: str, environ: dict[str, str] | None = None) -> str:
    """Resolve a secret reference from the environment."""

    env = environ or os.environ
    value = env.get(secret_ref)
    if value is None or value == "":
        raise KeyError(f"Missing secret for ref: {secret_ref}")
    return value
