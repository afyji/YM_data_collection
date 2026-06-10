"""WebSocket protocol: message parsing and response building."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class ErrorCode(str, Enum):
    INVALID_ACTION = "INVALID_ACTION"
    INVALID_TOPIC = "INVALID_TOPIC"
    MAX_SUBSCRIPTIONS_EXCEEDED = "MAX_SUBSCRIPTIONS_EXCEEDED"
    INVALID_JSON = "INVALID_JSON"
    AUTH_FAILED = "AUTH_FAILED"


VALID_ACTIONS = {"subscribe", "unsubscribe", "ping"}


@dataclass
class IncomingMessage:
    action: str
    request_id: str | int
    topics: list[str] = field(default_factory=list)
    ts_ms: int | None = None


@dataclass
class OutgoingMessage:
    type: str
    request_id: str | int
    topics: list[str] | None = None
    ts_ms: int | None = None
    code: str | None = None
    message: str | None = None

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {"type": self.type, "request_id": self.request_id}
        if self.topics is not None:
            d["topics"] = self.topics
        if self.ts_ms is not None:
            d["ts_ms"] = self.ts_ms
        if self.code is not None:
            d["code"] = self.code
        if self.message is not None:
            d["message"] = self.message
        return d

    def to_json(self) -> str:
        return json.dumps(self.to_dict())


def parse_message(raw: str | bytes) -> IncomingMessage:
    """Parse a raw JSON string into an IncomingMessage.

    Raises ValueError with a protocol-appropriate error hint on failure.
    """
    try:
        data = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        raise InvalidJsonError("Invalid JSON")

    if not isinstance(data, dict):
        raise InvalidJsonError("Message must be a JSON object")

    action = data.get("action")
    if not isinstance(action, str) or action not in VALID_ACTIONS:
        raise InvalidActionError(f"Invalid action: {action!r}")

    request_id = data.get("request_id")
    if request_id is None:
        raise InvalidActionError("Missing request_id")

    topics = data.get("topics", [])
    if not isinstance(topics, list):
        raise InvalidActionError("topics must be a list")

    ts_ms = data.get("ts_ms")
    if ts_ms is not None and not isinstance(ts_ms, int):
        raise InvalidActionError("ts_ms must be an integer")

    return IncomingMessage(action=action, request_id=request_id, topics=topics, ts_ms=ts_ms)


# ── Response builders ──────────────────────────────────────────────────

def build_subscribed(request_id: str | int, topics: list[str]) -> OutgoingMessage:
    return OutgoingMessage(type="subscribed", request_id=request_id, topics=topics,
                           ts_ms=int(time.time() * 1000))


def build_unsubscribed(request_id: str | int, topics: list[str]) -> OutgoingMessage:
    return OutgoingMessage(type="unsubscribed", request_id=request_id, topics=topics,
                           ts_ms=int(time.time() * 1000))


def build_pong(request_id: str | int, ts_ms: int) -> OutgoingMessage:
    return OutgoingMessage(type="pong", request_id=request_id, ts_ms=ts_ms)


def build_error(
    request_id: str | int,
    error_code: ErrorCode | str,
    error_message: str,
) -> OutgoingMessage:
    return OutgoingMessage(
        type="error",
        request_id=request_id,
        code=error_code if isinstance(error_code, str) else error_code.value,
        message=error_message,
        ts_ms=int(time.time() * 1000),
    )


# ── Typed parse errors ────────────────────────────────────────────────

class ProtocolError(Exception):
    """Base for protocol-level errors that map to error responses."""


class InvalidJsonError(ProtocolError):
    pass


class InvalidActionError(ProtocolError):
    pass


class InvalidTopicError(ProtocolError):
    pass


class MaxSubscriptionsExceededError(ProtocolError):
    pass
