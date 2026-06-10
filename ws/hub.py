"""Central hub managing all active WebSocket connections and broadcasts."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from fastapi import WebSocket

from YM_data_collection.ws.subscription import SubscriptionManager

logger = logging.getLogger(__name__)


class ConnectionHub:
    """Registry of all active WebSocket connections."""

    def __init__(self, max_connections: int = 100) -> None:
        self._connections: dict[WebSocket, SubscriptionManager] = {}
        self._max_connections = max_connections

    def register(self, ws: WebSocket, sub_mgr: SubscriptionManager) -> None:
        self._connections[ws] = sub_mgr

    def unregister(self, ws: WebSocket) -> None:
        self._connections.pop(ws, None)

    def is_full(self) -> bool:
        return len(self._connections) >= self._max_connections

    def get_connection_count(self) -> int:
        return len(self._connections)

    async def broadcast(self, topic_key: str, message_dict: dict[str, Any]) -> None:
        """Send *message_dict* as JSON to every connection subscribed to *topic_key*."""
        import json

        payload = json.dumps(message_dict)
        stale: list[WebSocket] = []
        for ws, sub_mgr in self._connections.items():
            if sub_mgr.has_topic(topic_key):
                try:
                    await ws.send_text(payload)
                except Exception:
                    logger.warning("Failed to send to connection; marking stale")
                    stale.append(ws)
        for ws in stale:
            self.unregister(ws)
