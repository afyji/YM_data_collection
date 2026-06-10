"""Redis cache client wrapper."""

from __future__ import annotations

import json
from typing import Any

from redis import Redis

from YM_data_collection.cache.keyspace import CacheKeyBuilder
from YM_data_collection.config.loader import resolve_secret
from YM_data_collection.config.models import CacheConfig


class RedisCacheClient:
    """Thin Redis wrapper with key prefix and default TTL support."""

    def __init__(self, client: Redis, key_builder: CacheKeyBuilder, default_ttl_seconds: int) -> None:
        self._client = client
        self._key_builder = key_builder
        self._default_ttl_seconds = default_ttl_seconds

    @property
    def key_builder(self) -> CacheKeyBuilder:
        return self._key_builder

    def build_key(self, *parts: str) -> str:
        return self._key_builder.build(*parts)

    def ping(self) -> bool:
        return bool(self._client.ping())

    def set_json(self, *parts: str, payload: dict[str, Any], ttl_seconds: int | None = None) -> bool:
        key = self.build_key(*parts)
        return bool(
            self._client.set(
                name=key,
                value=json.dumps(payload, ensure_ascii=True, sort_keys=True),
                ex=ttl_seconds or self._default_ttl_seconds,
            )
        )

    def get_json(self, *parts: str) -> dict[str, Any] | None:
        key = self.build_key(*parts)
        raw = self._client.get(key)
        if raw is None:
            return None
        return json.loads(raw)

    def delete(self, *parts: str) -> int:
        key = self.build_key(*parts)
        return int(self._client.delete(key))


def build_redis_client(cache_config: CacheConfig, environ: dict[str, str] | None = None) -> RedisCacheClient:
    """Create the concrete Redis client from config."""

    password = resolve_secret(cache_config.password_secret_ref, environ)
    client = Redis(
        host=cache_config.host,
        port=cache_config.port,
        password=password,
        db=cache_config.db,
        decode_responses=True,
    )
    return RedisCacheClient(
        client=client,
        key_builder=CacheKeyBuilder(cache_config.key_prefix),
        default_ttl_seconds=cache_config.ttl_seconds,
    )
