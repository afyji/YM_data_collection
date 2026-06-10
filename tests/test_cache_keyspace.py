"""Tests for cache key namespace helpers."""

from YM_data_collection.cache.keyspace import CacheKeyBuilder


def test_cache_key_builder_builds_namespaced_keys() -> None:
    builder = CacheKeyBuilder("ym:dev:binance")
    assert builder.build("KLINE", "BTCUSDT", "1H") == "ym:dev:binance:kline:btcusdt:1h"
