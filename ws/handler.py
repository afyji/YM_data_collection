"""WebSocket connection handler – the FastAPI websocket endpoint."""

from __future__ import annotations

import logging
from typing import Any, Callable, Awaitable

from fastapi import WebSocket, WebSocketDisconnect

from YM_data_collection.config.loader import resolve_secret
from YM_data_collection.config.models import DataCollectionConfig
from YM_data_collection.ws.hub import ConnectionHub
from YM_data_collection.ws.protocol import (
    ErrorCode,
    IncomingMessage,
    InvalidActionError,
    InvalidJsonError,
    InvalidTopicError,
    MaxSubscriptionsExceededError,
    build_error,
    build_pong,
    build_subscribed,
    build_unsubscribed,
    parse_message,
)
from YM_data_collection.ws.subscription import SubscriptionManager

logger = logging.getLogger(__name__)


def _verify_ws_token(auth_config: Any, token: str | None) -> bool:
    """Return True if the token is valid (or auth is disabled)."""
    if not auth_config.enabled:
        return True
    if token is None:
        return False
    try:
        expected = resolve_secret(auth_config.ws_token_secret_ref)
    except KeyError:
        return False
    return token == expected


def create_ws_endpoint(
    config: DataCollectionConfig,
    hub: ConnectionHub,
) -> Callable[[WebSocket], Awaitable[None]]:
    """Return a FastAPI websocket route handler wired to *config* and *hub*."""

    async def websocket_endpoint(websocket: WebSocket) -> None:
        # ── Auth check ────────────────────────────────────────────────
        token: str | None = websocket.query_params.get("token")
        if token is None:
            token = websocket.headers.get("x-api-token")

        if not _verify_ws_token(config.auth, token):
            await websocket.close(code=4001)
            return

        # ── Connection limit ──────────────────────────────────────────
        if hub.is_full():
            await websocket.close(code=4002)
            return

        # ── Accept & register ─────────────────────────────────────────
        await websocket.accept()
        sub_mgr = SubscriptionManager(
            max_subscriptions=config.websocket.max_subscriptions_per_connection,
        )
        hub.register(websocket, sub_mgr)

        try:
            while True:
                raw = await websocket.receive_text()
                await _handle_message(websocket, sub_mgr, raw)
        except WebSocketDisconnect:
            pass
        except Exception:
            logger.exception("Unexpected error on WS connection")
        finally:
            hub.unregister(websocket)

    return websocket_endpoint


async def _handle_message(
    websocket: WebSocket,
    sub_mgr: SubscriptionManager,
    raw: str,
) -> None:
    """Parse *raw* text and dispatch the appropriate action."""
    try:
        msg = parse_message(raw)
    except InvalidJsonError:
        resp = build_error(request_id=0, error_code=ErrorCode.INVALID_JSON, error_message="Invalid JSON")
        await websocket.send_text(resp.to_json())
        return
    except InvalidActionError as exc:
        resp = build_error(
            request_id=0, error_code=ErrorCode.INVALID_ACTION, error_message=str(exc)
        )
        await websocket.send_text(resp.to_json())
        return

    request_id = msg.request_id

    if msg.action == "ping":
        ts_ms = msg.ts_ms if msg.ts_ms is not None else 0
        await websocket.send_text(build_pong(request_id, ts_ms).to_json())
        return

    if msg.action == "subscribe":
        try:
            added = sub_mgr.add_topics(msg.topics)
        except InvalidTopicError as exc:
            resp = build_error(
                request_id=request_id,
                error_code=ErrorCode.INVALID_TOPIC,
                error_message=str(exc),
            )
            await websocket.send_text(resp.to_json())
            return
        except MaxSubscriptionsExceededError as exc:
            resp = build_error(
                request_id=request_id,
                error_code=ErrorCode.MAX_SUBSCRIPTIONS_EXCEEDED,
                error_message=str(exc),
            )
            await websocket.send_text(resp.to_json())
            return
        await websocket.send_text(build_subscribed(request_id, added).to_json())
        return

    if msg.action == "unsubscribe":
        removed = sub_mgr.remove_topics(msg.topics)
        await websocket.send_text(build_unsubscribed(request_id, removed).to_json())
        return
