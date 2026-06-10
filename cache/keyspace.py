"""Cache key namespace helpers."""

from __future__ import annotations


def normalize_key_part(value: str) -> str:
    """Normalize a cache key part."""

    return value.strip().replace(" ", "_").replace("/", "_").lower()


class CacheKeyBuilder:
    """Build stable namespaced cache keys."""

    def __init__(self, prefix: str) -> None:
        self.prefix = prefix.rstrip(":")

    def build(self, *parts: str) -> str:
        normalized = [normalize_key_part(part) for part in parts if part]
        if not normalized:
            return self.prefix
        return ":".join([self.prefix, *normalized])
