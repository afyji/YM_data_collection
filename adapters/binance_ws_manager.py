"""Binance WebSocket connection manager with auto-reconnect and subscription tracking."""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Awaitable, Callable

import websockets

from YM_data_collection.config.models import BinanceConfig

logger = logging.getLogger(__name__)

# Reconnect backoff cap (seconds)
_MAX_BACKOFF_SECONDS = 60


class BinanceWSManager:
    """Manages WebSocket connections to Binance spot and perpetual endpoints.

    Features:
    - Separate connections for spot and perp market types
    - Combined stream subscriptions per connection
    - Auto-reconnect with exponential backoff
    - Heartbeat ping/pong
    - Callback-based message dispatch
    - Automatic resubscription after reconnect
    """

    def __init__(self, config: BinanceConfig, logger: Any = None) -> None:
        self._config = config
        self._logger = logger or logging.getLogger(__name__)

        # Per market-type internal state
        self._state: dict[str, _ConnState] = {
            "spot": _ConnState(config.spot.ws_base_url),
            "perp": _ConnState(config.perp.ws_base_url),
        }

        # Registered message callback: callback(stream_name, data)
        self._message_callback: Callable[[str, dict], Awaitable[None]] | None = None

        # Shutdown flag
        self._closing = False

        # Next request id for Binance WS protocol
        self._next_id = 1

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def connect(self, market_type: str) -> None:
        """Connect to spot or perp WS endpoint."""
        if market_type not in self._state:
            raise ValueError(f"Unknown market_type: {market_type!r}. Expected 'spot' or 'perp'.")
        state = self._state[market_type]
        await self._do_connect(state)

    async def subscribe(self, market_type: str, streams: list[str]) -> None:
        """Subscribe to streams on the given market type connection."""
        if market_type not in self._state:
            raise ValueError(f"Unknown market_type: {market_type!r}")
        state = self._state[market_type]
        state.subscriptions.update(streams)
        if state.ws is not None and state.connected:
            await self._send_subscribe(state, streams)

    async def unsubscribe(self, market_type: str, streams: list[str]) -> None:
        """Unsubscribe from streams."""
        if market_type not in self._state:
            raise ValueError(f"Unknown market_type: {market_type!r}")
        state = self._state[market_type]
        state.subscriptions -= set(streams)
        if state.ws is not None and state.connected:
            await self._send_unsubscribe(state, streams)

    def on_message(self, callback: Callable[[str, dict], Awaitable[None]]) -> None:
        """Register a message handler. callback(stream_name, data)"""
        self._message_callback = callback

    async def run_forever(self) -> None:
        """Main event loop: read messages, dispatch to handlers, handle reconnects."""
        self._closing = False
        # Connect all enabled market types
        for mt, state in self._state.items():
            if not state.connected:
                try:
                    await self._do_connect(state)
                except Exception:
                    self._logger.warning("Initial connect failed for %s, will retry in run loop", mt)

        # Spawn a reader task per market type
        tasks = []
        for mt, state in self._state.items():
            tasks.append(asyncio.create_task(self._reader_loop(mt, state)))

        # Spawn heartbeat tasks
        for mt, state in self._state.items():
            tasks.append(asyncio.create_task(self._heartbeat_loop(mt, state)))

        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            pass

    async def close(self) -> None:
        """Graceful shutdown."""
        self._closing = True
        for mt, state in self._state.items():
            state.connected = False
            state.reconnect_task = None
            if state.ws is not None:
                try:
                    await state.ws.close()
                except Exception:
                    pass
                state.ws = None

    @property
    def is_connected(self) -> dict[str, bool]:
        """Return connection status per market_type."""
        return {mt: state.connected for mt, state in self._state.items()}

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    async def _do_connect(self, state: _ConnState) -> None:
        """Open a WebSocket connection."""
        ws = await websockets.connect(state.url)
        state.ws = ws
        state.connected = True
        state.reconnect_attempt = 0
        self._logger.info("Connected to %s", state.url)

        # Force combined payloads on /ws endpoints so every message carries the
        # originating stream name. Without this, raw SUBSCRIBE payloads do not
        # include stream/symbol for depth snapshots, so the ingestion dispatcher
        # cannot route messages correctly.
        await self._send_set_combined(state, True)

        # Resubscribe any prior subscriptions
        if state.subscriptions:
            await self._send_subscribe(state, list(state.subscriptions))

    async def _send_set_combined(self, state: _ConnState, enabled: bool) -> None:
        """Enable/disable Binance combined stream envelopes on a raw /ws connection."""
        if state.ws is None:
            return
        msg = {
            "method": "SET_PROPERTY",
            "params": ["combined", enabled],
            "id": self._next_id,
        }
        self._next_id += 1
        await state.ws.send(json.dumps(msg))
        self._logger.debug("Set combined stream payloads to %s", enabled)

    async def _send_subscribe(self, state: _ConnState, streams: list[str]) -> None:
        """Send SUBSCRIBE message."""
        if not streams or state.ws is None:
            return
        msg = {
            "method": "SUBSCRIBE",
            "params": streams,
            "id": self._next_id,
        }
        self._next_id += 1
        await state.ws.send(json.dumps(msg))
        self._logger.debug("Subscribed to %s", streams)

    async def _send_unsubscribe(self, state: _ConnState, streams: list[str]) -> None:
        """Send UNSUBSCRIBE message."""
        if not streams or state.ws is None:
            return
        msg = {
            "method": "UNSUBSCRIBE",
            "params": streams,
            "id": self._next_id,
        }
        self._next_id += 1
        await state.ws.send(json.dumps(msg))
        self._logger.debug("Unsubscribed from %s", streams)

    async def _reader_loop(self, market_type: str, state: _ConnState) -> None:
        """Read messages from a single connection, handle reconnect."""
        while not self._closing:
            if state.ws is None or not state.connected:
                await self._try_reconnect(state)
                continue

            try:
                raw = await state.ws.recv()
            except websockets.ConnectionClosed:
                self._logger.warning("WS disconnected for %s", market_type)
                state.connected = False
                continue
            except Exception:
                self._logger.exception("WS read error for %s", market_type)
                state.connected = False
                continue

            # Dispatch message
            try:
                data = json.loads(raw)

                # Ignore protocol acknowledgements such as SUBSCRIBE / SET_PROPERTY replies.
                if "result" in data and "id" in data and "stream" not in data and "data" not in data:
                    continue

                stream_name = data.get("stream", market_type)
                payload = data.get("data", data) if isinstance(data, dict) else data
                routed_stream_name = f"{market_type}:{stream_name}"
                if self._message_callback is not None:
                    await self._message_callback(routed_stream_name, payload)
            except json.JSONDecodeError:
                self._logger.warning("Invalid JSON from WS: %s", raw[:200])
            except Exception:
                self._logger.exception("Error dispatching WS message")

    async def _heartbeat_loop(self, market_type: str, state: _ConnState) -> None:
        """Periodically send ping frames to keep connection alive."""
        interval = self._config.ws_ping_interval_seconds
        while not self._closing:
            await asyncio.sleep(interval)
            if state.ws is not None and state.connected:
                try:
                    pong = await state.ws.ping()
                    await asyncio.wait_for(pong, timeout=10)
                except Exception:
                    self._logger.warning("Ping/pong failed for %s", market_type)
                    state.connected = False

    async def _try_reconnect(self, state: _ConnState) -> None:
        """Attempt to reconnect with exponential backoff."""
        state.reconnect_attempt += 1
        base = self._config.ws_reconnect_backoff_seconds
        attempt = state.reconnect_attempt
        backoff = min(base * (2 ** (attempt - 1)), _MAX_BACKOFF_SECONDS)
        self._logger.info("Reconnecting in %ds (attempt %d)", backoff, attempt)
        await asyncio.sleep(backoff)

        try:
            await self._do_connect(state)
        except Exception:
            self._logger.exception("Reconnect attempt %d failed", attempt)
            # state.connected stays False, loop will retry


class _ConnState:
    """Internal per-connection state."""

    __slots__ = ("url", "ws", "subscriptions", "connected", "reconnect_attempt", "reconnect_task")

    def __init__(self, url: str) -> None:
        self.url = url
        self.ws: Any = None
        self.subscriptions: set[str] = set()
        self.connected: bool = False
        self.reconnect_attempt: int = 0
        self.reconnect_task: asyncio.Task | None = None
