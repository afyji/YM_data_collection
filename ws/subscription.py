"""Per-connection subscription manager."""

from __future__ import annotations

from YM_data_collection.ws.protocol import (
    ErrorCode,
    InvalidTopicError,
    MaxSubscriptionsExceededError,
)

# Phase-1 valid topic names (the first segment of a topic key).
VALID_TOPICS: set[str] = {
    "marketdata.kline",
    "marketdata.mark_price",
    "marketdata.index_price",
    "marketdata.open_interest",
    "marketdata.funding_rate",
    "marketdata.depth_snapshot",
    "system.quality_event",
    "system.stream_status",
}


def _topic_name(topic_key: str) -> str:
    """Extract the topic name (first segment) from a topic key.

    Topic key format: "{topic}:{venue}:{market_type}:{symbol}" (+ ":interval" for kline)
    """
    parts = topic_key.split(":")
    return parts[0] if parts else topic_key


def validate_topic_key(topic_key: str) -> str:
    """Validate a topic key and return it. Raises InvalidTopicError if invalid."""
    name = _topic_name(topic_key)
    if name not in VALID_TOPICS:
        raise InvalidTopicError(f"Invalid topic: {name}")
    return topic_key


class SubscriptionManager:
    """Tracks subscriptions for a single WebSocket connection."""

    def __init__(self, max_subscriptions: int = 20) -> None:
        self._topics: set[str] = set()
        self._max_subscriptions = max_subscriptions

    def add_topics(self, topics: list[str]) -> list[str]:
        """Add topics. Returns list of actually-new topics added.

        Raises InvalidTopicError for any invalid topic.
        Raises MaxSubscriptionsExceededError if adding would exceed max.
        """
        # Validate all topics first
        for t in topics:
            validate_topic_key(t)

        new = [t for t in topics if t not in self._topics]
        if len(self._topics) + len(new) > self._max_subscriptions:
            raise MaxSubscriptionsExceededError(
                f"Max subscriptions ({self._max_subscriptions}) exceeded"
            )
        self._topics.update(new)
        return new

    def remove_topics(self, topics: list[str]) -> list[str]:
        """Remove topics. Returns list of actually-removed topics."""
        removed = [t for t in topics if t in self._topics]
        self._topics.difference_update(removed)
        return removed

    def get_topics(self) -> set[str]:
        """Return a copy of the subscribed topic set."""
        return set(self._topics)

    def has_topic(self, topic_key: str) -> bool:
        return topic_key in self._topics

    @property
    def count(self) -> int:
        return len(self._topics)
