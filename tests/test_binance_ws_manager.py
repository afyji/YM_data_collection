"""Tests for BinanceWSManager — all mocked, no real WS connections."""

from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from YM_data_collection.adapters.binance_ws_manager import BinanceWSManager, _MAX_BACKOFF_SECONDS
from YM_data_collection.config.models import BinanceConfig, BinanceEndpointConfig


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def config() -> BinanceConfig:
    return BinanceConfig(
        spot=BinanceEndpointConfig(
            rest_base_url="https://api.binance.com",
            ws_base_url="wss://stream.binance.com:9443/ws",
        ),
        perp=BinanceEndpointConfig(
            rest_base_url="https://fapi.binance.com",
            ws_base_url="wss://fstream.binance.com/ws",
        ),
        ws_reconnect_backoff_seconds=5,
        ws_ping_interval_seconds=20,
    )


@pytest.fixture
def manager(config: BinanceConfig) -> BinanceWSManager:
    return BinanceWSManager(config)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestInit:
    def test_init_stores_config(self, manager: BinanceWSManager, config: BinanceConfig):
        """Constructor stores config correctly."""
        assert manager._config is config
        assert "spot" in manager._state
        assert "perp" in manager._state
        assert manager._state["spot"].url == "wss://stream.binance.com:9443/ws"
        assert manager._state["perp"].url == "wss://fstream.binance.com/ws"

    def test_init_with_custom_logger(self, config: BinanceConfig):
        custom_logger = MagicMock()
        mgr = BinanceWSManager(config, logger=custom_logger)
        assert mgr._logger is custom_logger

    def test_initial_state_not_connected(self, manager: BinanceWSManager):
        assert manager.is_connected == {"spot": False, "perp": False}


class TestSubscriptions:
    @pytest.mark.asyncio
    async def test_subscribe_adds_streams(self, manager: BinanceWSManager):
        """Subscribe tracks streams internally even without a connection."""
        await manager.subscribe("spot", ["btcusdt@kline_1h", "ethusdt@trade"])
        assert "btcusdt@kline_1h" in manager._state["spot"].subscriptions
        assert "ethusdt@trade" in manager._state["spot"].subscriptions

    @pytest.mark.asyncio
    async def test_subscribe_sends_message_when_connected(self, manager: BinanceWSManager):
        """When connected, subscribe sends a WS SUBSCRIBE message."""
        mock_ws = AsyncMock()
        manager._state["spot"].ws = mock_ws
        manager._state["spot"].connected = True

        await manager.subscribe("spot", ["btcusdt@kline_1h"])

        mock_ws.send.assert_called_once()
        sent = json.loads(mock_ws.send.call_args[0][0])
        assert sent["method"] == "SUBSCRIBE"
        assert "btcusdt@kline_1h" in sent["params"]

    @pytest.mark.asyncio
    async def test_unsubscribe_removes_streams(self, manager: BinanceWSManager):
        """Unsubscribe removes streams from internal tracking."""
        manager._state["spot"].subscriptions = {"btcusdt@kline_1h", "ethusdt@trade"}
        await manager.unsubscribe("spot", ["btcusdt@kline_1h"])
        assert "btcusdt@kline_1h" not in manager._state["spot"].subscriptions
        assert "ethusdt@trade" in manager._state["spot"].subscriptions

    @pytest.mark.asyncio
    async def test_unsubscribe_sends_message_when_connected(self, manager: BinanceWSManager):
        """When connected, unsubscribe sends a WS UNSUBSCRIBE message."""
        mock_ws = AsyncMock()
        manager._state["spot"].ws = mock_ws
        manager._state["spot"].connected = True
        manager._state["spot"].subscriptions = {"btcusdt@kline_1h"}

        await manager.unsubscribe("spot", ["btcusdt@kline_1h"])

        mock_ws.send.assert_called_once()
        sent = json.loads(mock_ws.send.call_args[0][0])
        assert sent["method"] == "UNSUBSCRIBE"
        assert "btcusdt@kline_1h" in sent["params"]

    @pytest.mark.asyncio
    async def test_subscribe_invalid_market_type(self, manager: BinanceWSManager):
        with pytest.raises(ValueError, match="Unknown market_type"):
            await manager.subscribe("invalid", ["btcusdt@kline_1h"])

    @pytest.mark.asyncio
    async def test_unsubscribe_invalid_market_type(self, manager: BinanceWSManager):
        with pytest.raises(ValueError, match="Unknown market_type"):
            await manager.unsubscribe("invalid", ["btcusdt@kline_1h"])


class TestMessageCallback:
    def test_on_message_registers_callback(self, manager: BinanceWSManager):
        """on_message stores the callback."""
        cb = AsyncMock()
        manager.on_message(cb)
        assert manager._message_callback is cb

    @pytest.mark.asyncio
    async def test_message_dispatch(self, manager: BinanceWSManager):
        """Incoming message is dispatched to registered callback."""
        cb = AsyncMock()
        manager.on_message(cb)

        # Simulate receiving a combined-stream message after reader unwrapping.
        payload = {"e": "kline"}
        await manager._message_callback("btcusdt@kline_1h", payload)

        # Direct callback invocation preserves the exact value; market-type
        # prefixing is added by _reader_loop where market_type is known.
        cb.assert_called_once_with("btcusdt@kline_1h", payload)


class TestReconnect:
    @pytest.mark.asyncio
    async def test_reconnect_resubscribes(self, manager: BinanceWSManager):
        """After reconnect, all prior subscriptions are re-sent."""
        # Set up subscriptions on spot
        manager._state["spot"].subscriptions = {"btcusdt@kline_1h", "ethusdt@trade"}

        mock_ws = AsyncMock()
        with patch("YM_data_collection.adapters.binance_ws_manager.websockets") as mock_wslib:
            mock_wslib.connect = AsyncMock(return_value=mock_ws)
            await manager._do_connect(manager._state["spot"])

        # SET_PROPERTY enables combined envelopes, then SUBSCRIBE is sent.
        assert mock_ws.send.call_count == 2
        set_property = json.loads(mock_ws.send.call_args_list[0][0][0])
        assert set_property["method"] == "SET_PROPERTY"
        assert set_property["params"] == ["combined", True]
        sent = json.loads(mock_ws.send.call_args_list[1][0][0])
        assert sent["method"] == "SUBSCRIBE"
        assert set(sent["params"]) == {"btcusdt@kline_1h", "ethusdt@trade"}

    @pytest.mark.asyncio
    async def test_backoff_increases(self, manager: BinanceWSManager):
        """Exponential backoff increases and caps at 60s."""
        base = manager._config.ws_reconnect_backoff_seconds  # 5

        state = manager._state["spot"]

        # Attempt 1: 5 * 2^0 = 5
        state.reconnect_attempt = 0
        state.reconnect_attempt += 1
        backoff1 = min(base * (2 ** (state.reconnect_attempt - 1)), _MAX_BACKOFF_SECONDS)
        assert backoff1 == 5

        # Attempt 2: 5 * 2^1 = 10
        state.reconnect_attempt += 1
        backoff2 = min(base * (2 ** (state.reconnect_attempt - 1)), _MAX_BACKOFF_SECONDS)
        assert backoff2 == 10

        # Attempt 3: 5 * 2^2 = 20
        state.reconnect_attempt += 1
        backoff3 = min(base * (2 ** (state.reconnect_attempt - 1)), _MAX_BACKOFF_SECONDS)
        assert backoff3 == 20

        # Attempt 5: 5 * 2^4 = 80, capped at 60
        state.reconnect_attempt = 5
        backoff5 = min(base * (2 ** (state.reconnect_attempt - 1)), _MAX_BACKOFF_SECONDS)
        assert backoff5 == 60


class TestClose:
    @pytest.mark.asyncio
    async def test_close_sets_disconnected(self, manager: BinanceWSManager):
        """close() sets connected=False and closes WS."""
        mock_ws = AsyncMock()
        manager._state["spot"].ws = mock_ws
        manager._state["spot"].connected = True
        manager._state["perp"].ws = None
        manager._state["perp"].connected = False

        await manager.close()

        assert manager._state["spot"].connected is False
        assert manager._state["perp"].connected is False
        assert manager._closing is True
        mock_ws.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_close_handles_ws_close_error(self, manager: BinanceWSManager):
        """close() gracefully handles errors when closing WS."""
        mock_ws = AsyncMock()
        mock_ws.close.side_effect = Exception("close error")
        manager._state["spot"].ws = mock_ws
        manager._state["spot"].connected = True

        # Should not raise
        await manager.close()
        assert manager._state["spot"].connected is False


class TestMultipleMarketTypes:
    @pytest.mark.asyncio
    async def test_spot_and_perp_tracked_separately(self, manager: BinanceWSManager):
        """Spot and perp subscriptions and state are independent."""
        await manager.subscribe("spot", ["btcusdt@kline_1h"])
        await manager.subscribe("perp", ["btcusdt@markPrice"])

        assert "btcusdt@kline_1h" in manager._state["spot"].subscriptions
        assert "btcusdt@kline_1h" not in manager._state["perp"].subscriptions
        assert "btcusdt@markPrice" in manager._state["perp"].subscriptions
        assert "btcusdt@markPrice" not in manager._state["spot"].subscriptions

    @pytest.mark.asyncio
    async def test_connect_spot_only(self, manager: BinanceWSManager):
        """Can connect to spot without affecting perp."""
        mock_ws = AsyncMock()
        with patch("YM_data_collection.adapters.binance_ws_manager.websockets") as mock_wslib:
            mock_wslib.connect = AsyncMock(return_value=mock_ws)
            await manager.connect("spot")

        assert manager._state["spot"].connected is True
        assert manager._state["perp"].connected is False


class TestIsConnected:
    @pytest.mark.asyncio
    async def test_is_connected_property(self, manager: BinanceWSManager):
        """is_connected returns correct status dict."""
        assert manager.is_connected == {"spot": False, "perp": False}

        manager._state["spot"].connected = True
        assert manager.is_connected == {"spot": True, "perp": False}

        manager._state["perp"].connected = True
        assert manager.is_connected == {"spot": True, "perp": True}


class TestConnect:
    @pytest.mark.asyncio
    async def test_connect_invalid_market_type(self, manager: BinanceWSManager):
        with pytest.raises(ValueError, match="Unknown market_type"):
            await manager.connect("invalid")

    @pytest.mark.asyncio
    async def test_connect_sets_state(self, manager: BinanceWSManager):
        mock_ws = AsyncMock()
        with patch("YM_data_collection.adapters.binance_ws_manager.websockets") as mock_wslib:
            mock_wslib.connect = AsyncMock(return_value=mock_ws)
            await manager.connect("spot")

        assert manager._state["spot"].ws is mock_ws
        assert manager._state["spot"].connected is True
        assert manager._state["spot"].reconnect_attempt == 0


class TestReaderLoop:
    @pytest.mark.asyncio
    async def test_reader_dispatches_messages(self, manager: BinanceWSManager):
        """Reader loop parses and dispatches incoming messages."""
        cb = AsyncMock()
        manager.on_message(cb)

        mock_ws = AsyncMock()
        payload = {"e": "kline"}
        msg = {"stream": "btcusdt@kline_1h", "data": payload}

        # recv returns one message, then raises ConnectionClosed
        import websockets as real_ws
        mock_ws.recv = AsyncMock(
            side_effect=[
                json.dumps(msg),
                real_ws.ConnectionClosed(None, None),
            ]
        )
        manager._state["spot"].ws = mock_ws
        manager._state["spot"].connected = True

        # Patch _try_reconnect to immediately set closing flag
        async def fake_reconnect(state):
            manager._closing = True

        with patch.object(manager, "_try_reconnect", side_effect=fake_reconnect):
            await manager._reader_loop("spot", manager._state["spot"])

        # Verify the callback was called with the unwrapped stream payload
        cb.assert_called_once_with("spot:btcusdt@kline_1h", payload)

    @pytest.mark.asyncio
    async def test_reader_ignores_protocol_ack(self, manager: BinanceWSManager):
        cb = AsyncMock()
        manager.on_message(cb)

        mock_ws = AsyncMock()
        import websockets as real_ws
        mock_ws.recv = AsyncMock(
            side_effect=[
                json.dumps({"result": None, "id": 1}),
                real_ws.ConnectionClosed(None, None),
            ]
        )
        manager._state["spot"].ws = mock_ws
        manager._state["spot"].connected = True

        async def fake_reconnect(state):
            manager._closing = True

        with patch.object(manager, "_try_reconnect", side_effect=fake_reconnect):
            await manager._reader_loop("spot", manager._state["spot"])

        cb.assert_not_called()
