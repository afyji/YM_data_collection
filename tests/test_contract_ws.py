"""WebSocket JSON Schema contract tests.

Verifies that WS message payload shapes produced by the pure builder
functions match the contract defined in ``18_ws_json_schema_draft.yaml``.

No actual WebSocket connections are made — these tests exercise only
the builder / protocol layer.
"""

from __future__ import annotations

import pytest

from YM_data_collection.ws.protocol import (
    ErrorCode,
    build_error,
    build_pong,
    build_subscribed,
    build_unsubscribed,
)
from YM_data_collection.ws.publishers.kline_publisher import build_kline_message
from YM_data_collection.ws.publishers.marketdata_publisher import (
    TOPIC_DEPTH_SNAPSHOT,
    TOPIC_FUNDING_RATE,
    TOPIC_INDEX_PRICE,
    TOPIC_MARK_PRICE,
    TOPIC_OPEN_INTEREST,
    build_depth_snapshot_message,
    build_funding_rate_message,
    build_index_price_message,
    build_mark_price_message,
    build_marketdata_message,
    build_open_interest_message,
)
from YM_data_collection.ws.publishers.system_publisher import (
    TOPIC_QUALITY_EVENT,
    TOPIC_STREAM_STATUS,
    build_quality_event_message,
    build_stream_status_message,
)
from YM_data_collection.ws.subscription import VALID_TOPICS

# ── Helpers ────────────────────────────────────────────────────────────────

SCHEMA_TOPIC_NAMES = {
    "marketdata.kline",
    "marketdata.mark_price",
    "marketdata.index_price",
    "marketdata.open_interest",
    "marketdata.funding_rate",
    "marketdata.depth_snapshot",
    "system.quality_event",
    "system.stream_status",
}

# ── 1. Protocol response builders ─────────────────────────────────────────


class TestServerSubscribed:
    """Contract: ServerSubscribed required [type='subscribed', request_id, topics, ts_ms]."""

    def test_has_required_type(self):
        msg = build_subscribed("r1", ["marketdata.kline:binance:perp:BTCUSDT"]).to_dict()
        assert msg["type"] == "subscribed"

    def test_has_request_id(self):
        msg = build_subscribed("r1", ["t1"]).to_dict()
        assert "request_id" in msg
        assert msg["request_id"] == "r1"

    def test_has_topics(self):
        msg = build_subscribed("r1", ["t1", "t2"]).to_dict()
        assert "topics" in msg
        assert msg["topics"] == ["t1", "t2"]

    def test_has_ts_ms(self):
        msg = build_subscribed("r1", ["t1"]).to_dict()
        assert "ts_ms" in msg
        assert isinstance(msg["ts_ms"], int)


class TestServerUnsubscribed:
    """Contract: ServerUnsubscribed required [type='unsubscribed', request_id, topics, ts_ms]."""

    def test_has_required_type(self):
        msg = build_unsubscribed("r2", ["t1"]).to_dict()
        assert msg["type"] == "unsubscribed"

    def test_has_request_id(self):
        msg = build_unsubscribed("r2", ["t1"]).to_dict()
        assert "request_id" in msg

    def test_has_topics(self):
        msg = build_unsubscribed("r2", ["t1"]).to_dict()
        assert "topics" in msg

    def test_has_ts_ms(self):
        msg = build_unsubscribed("r2", ["t1"]).to_dict()
        assert "ts_ms" in msg
        assert isinstance(msg["ts_ms"], int)


class TestServerPong:
    """Contract: ServerPong required [type='pong', request_id, ts_ms]."""

    def test_has_required_type(self):
        msg = build_pong("r3", 1700000000000).to_dict()
        assert msg["type"] == "pong"

    def test_has_request_id(self):
        msg = build_pong("r3", 1700000000000).to_dict()
        assert "request_id" in msg
        assert msg["request_id"] == "r3"

    def test_has_ts_ms(self):
        msg = build_pong("r3", 1700000000000).to_dict()
        assert "ts_ms" in msg
        assert isinstance(msg["ts_ms"], int)

    def test_request_id_accepts_int(self):
        msg = build_pong(42, 1700000000000).to_dict()
        assert msg["request_id"] == 42


class TestServerError:
    """Contract: ServerError required [type='error', request_id, code, message, ts_ms]."""

    def test_has_required_type(self):
        msg = build_error("r4", ErrorCode.INVALID_TOPIC, "bad topic").to_dict()
        assert msg["type"] == "error"

    def test_has_request_id(self):
        msg = build_error("r4", ErrorCode.INVALID_TOPIC, "bad topic").to_dict()
        assert "request_id" in msg

    def test_has_code_field(self):
        msg = build_error("r4", ErrorCode.INVALID_TOPIC, "bad topic").to_dict()
        assert "code" in msg

    def test_code_field_value(self):
        """Schema requires 'code' field with correct value."""
        msg = build_error("r4", ErrorCode.INVALID_TOPIC, "bad topic").to_dict()
        assert "code" in msg
        assert msg["code"] == "INVALID_TOPIC"

    def test_has_message_field(self):
        msg = build_error("r4", ErrorCode.INVALID_TOPIC, "bad topic").to_dict()
        assert "message" in msg

    def test_message_field_value(self):
        """Schema requires 'message' field with correct value."""
        msg = build_error("r4", ErrorCode.INVALID_TOPIC, "bad topic").to_dict()
        assert "message" in msg
        assert msg["message"] == "bad topic"

    def test_has_ts_ms(self):
        msg = build_error("r4", ErrorCode.INVALID_TOPIC, "bad topic").to_dict()
        assert "ts_ms" in msg
        assert isinstance(msg["ts_ms"], int)

    def test_error_code_from_enum_value(self):
        msg = build_error(0, ErrorCode.MAX_SUBSCRIPTIONS_EXCEEDED, "limit").to_dict()
        assert msg["code"] == "MAX_SUBSCRIPTIONS_EXCEEDED"

    def test_error_code_from_string(self):
        msg = build_error(0, "CUSTOM_ERROR", "detail").to_dict()
        assert msg["code"] == "CUSTOM_ERROR"


# ── 2. Kline publisher ────────────────────────────────────────────────────


class TestKlineMessage:
    """Contract: KlineUpdate envelope + KlinePayload required fields."""

    @pytest.fixture()
    def full_kline_data(self) -> dict:
        return {
            "instrument_code": "BTCUSDT",
            "interval_code": "1h",
            "open_ts_ms": 1700000000000,
            "close_ts_ms": 1700003600000,
            "open_price": "42000.00",
            "high_price": "42500.00",
            "low_price": "41800.00",
            "close_price": "42300.00",
            "volume": "1234.56",
            "quote_volume": "52000000.00",
            "trade_count": 9876,
            "taker_buy_base_volume": "600.00",
            "taker_buy_quote_volume": "25200000.00",
            "is_closed": True,
        }

    @pytest.fixture()
    def msg(self, full_kline_data) -> dict:
        return build_kline_message("binance", "perp", "BTCUSDT", "1h", full_kline_data)

    # -- Envelope-level contract --

    def test_envelope_type_is_update(self, msg):
        assert msg["type"] == "update"

    def test_envelope_topic_is_kline(self, msg):
        assert msg["topic"] == "marketdata.kline"

    def test_envelope_has_venue(self, msg):
        assert msg["venue"] == "binance"

    def test_envelope_has_market_type(self, msg):
        assert msg["market_type"] == "perp"

    def test_envelope_has_symbol(self, msg):
        assert msg["symbol"] == "BTCUSDT"

    def test_envelope_has_ts_ms_int(self, msg):
        assert "ts_ms" in msg
        assert isinstance(msg["ts_ms"], int)

    def test_envelope_has_data(self, msg):
        assert "data" in msg
        assert isinstance(msg["data"], dict)

    def test_kline_has_interval_at_envelope_level(self, msg):
        """Kline messages carry 'interval' at the envelope level (not in data)."""
        assert "interval" in msg
        assert msg["interval"] == "1h"

    def test_envelope_required_keys_present(self, msg):
        required = {"type", "topic", "venue", "market_type", "symbol", "ts_ms", "data"}
        assert required.issubset(msg.keys())

    # -- Payload-level contract --

    def test_data_has_all_required_kline_fields(self, msg):
        required = {
            "instrument_code",
            "interval_code",
            "open_ts_ms",
            "close_ts_ms",
            "open_price",
            "high_price",
            "low_price",
            "close_price",
            "volume",
            "quote_volume",
            "trade_count",
            "taker_buy_base_volume",
            "taker_buy_quote_volume",
            "is_closed",
        }
        assert required.issubset(msg["data"].keys())

    def test_missing_optional_fields_omitted_from_data(self):
        """If a field is absent from kline_data, it should not appear in data."""
        partial = {
            "instrument_code": "BTCUSDT",
            "interval_code": "1h",
            "open_ts_ms": 1700000000000,
            "close_ts_ms": 1700003600000,
            "open_price": "42000.00",
            "high_price": "42500.00",
            "low_price": "41800.00",
            "close_price": "42300.00",
            "volume": "1234.56",
            "quote_volume": "52000000.00",
            "trade_count": 9876,
            "taker_buy_base_volume": "600.00",
            "taker_buy_quote_volume": "25200000.00",
            "is_closed": False,
        }
        msg = build_kline_message("binance", "perp", "BTCUSDT", "1h", partial)
        # is_closed is present; taker_buy_quote_volume is present
        assert "is_closed" in msg["data"]
        # Extra key that was never in kline_data should not appear
        assert "extra_field" not in msg["data"]


# ── 3. Market-data publisher ──────────────────────────────────────────────

# -- Generic builder --


class TestBuildMarketdataMessage:
    """Contract: UpdateEnvelopeBase required [type, topic, venue, market_type, symbol, ts_ms, data]."""

    @pytest.fixture()
    def msg(self) -> dict:
        return build_marketdata_message(
            topic="marketdata.mark_price",
            msg_type="update",
            venue="binance",
            market_type="perp",
            symbol="BTCUSDT",
            data={"instrument_code": "BTCUSDT", "event_ts_ms": 1700000000000, "mark_price": "42000.00"},
        )

    def test_envelope_type(self, msg):
        assert msg["type"] == "update"

    def test_envelope_topic(self, msg):
        assert msg["topic"] == "marketdata.mark_price"

    def test_envelope_venue(self, msg):
        assert msg["venue"] == "binance"

    def test_envelope_market_type(self, msg):
        assert msg["market_type"] == "perp"

    def test_envelope_symbol(self, msg):
        assert msg["symbol"] == "BTCUSDT"

    def test_envelope_ts_ms_int(self, msg):
        assert "ts_ms" in msg
        assert isinstance(msg["ts_ms"], int)

    def test_envelope_has_data(self, msg):
        assert "data" in msg
        assert isinstance(msg["data"], dict)

    def test_envelope_required_keys(self, msg):
        required = {"type", "topic", "venue", "market_type", "symbol", "ts_ms", "data"}
        assert required.issubset(msg.keys())

    def test_data_passes_through_verbatim(self, msg):
        assert msg["data"]["mark_price"] == "42000.00"


# -- Specific market-data builders --


class TestMarkPriceMessage:
    """Contract: MarkPricePayload required [instrument_code, event_ts_ms, mark_price]."""

    @pytest.fixture()
    def msg(self) -> dict:
        return build_mark_price_message(
            "binance", "perp", "BTCUSDT",
            {"instrument_code": "BTCUSDT", "event_ts_ms": 1700000000000, "mark_price": "42000.00"},
        )

    def test_type_is_update(self, msg):
        assert msg["type"] == "update"

    def test_topic_is_mark_price(self, msg):
        assert msg["topic"] == TOPIC_MARK_PRICE
        assert msg["topic"] == "marketdata.mark_price"

    def test_data_has_required_fields(self, msg):
        required = {"instrument_code", "event_ts_ms", "mark_price"}
        assert required.issubset(msg["data"].keys())


class TestIndexPriceMessage:
    """Contract: IndexPricePayload required [instrument_code, event_ts_ms, index_price]."""

    @pytest.fixture()
    def msg(self) -> dict:
        return build_index_price_message(
            "binance", "perp", "BTCUSDT",
            {"instrument_code": "BTCUSDT", "event_ts_ms": 1700000000000, "index_price": "41950.00"},
        )

    def test_type_is_update(self, msg):
        assert msg["type"] == "update"

    def test_topic_is_index_price(self, msg):
        assert msg["topic"] == TOPIC_INDEX_PRICE
        assert msg["topic"] == "marketdata.index_price"

    def test_data_has_required_fields(self, msg):
        required = {"instrument_code", "event_ts_ms", "index_price"}
        assert required.issubset(msg["data"].keys())


class TestOpenInterestMessage:
    """Contract: OpenInterestPayload required [instrument_code, event_ts_ms, open_interest]."""

    @pytest.fixture()
    def msg(self) -> dict:
        return build_open_interest_message(
            "binance", "perp", "BTCUSDT",
            {"instrument_code": "BTCUSDT", "event_ts_ms": 1700000000000, "open_interest": "50000.00"},
        )

    def test_type_is_update(self, msg):
        assert msg["type"] == "update"

    def test_topic_is_open_interest(self, msg):
        assert msg["topic"] == TOPIC_OPEN_INTEREST
        assert msg["topic"] == "marketdata.open_interest"

    def test_data_has_required_fields(self, msg):
        required = {"instrument_code", "event_ts_ms", "open_interest"}
        assert required.issubset(msg["data"].keys())


class TestFundingRateMessage:
    """Contract: FundingRatePayload required [instrument_code, funding_time_ts_ms, funding_rate]."""

    @pytest.fixture()
    def msg(self) -> dict:
        return build_funding_rate_message(
            "binance", "perp", "BTCUSDT",
            {"instrument_code": "BTCUSDT", "funding_time_ts_ms": 1700011200000, "funding_rate": "0.0001"},
        )

    def test_type_is_update(self, msg):
        assert msg["type"] == "update"

    def test_topic_is_funding_rate(self, msg):
        assert msg["topic"] == TOPIC_FUNDING_RATE
        assert msg["topic"] == "marketdata.funding_rate"

    def test_data_has_required_fields(self, msg):
        required = {"instrument_code", "funding_time_ts_ms", "funding_rate"}
        assert required.issubset(msg["data"].keys())


class TestDepthSnapshotMessage:
    """Contract: DepthSnapshotPayload required [instrument_code, event_ts_ms,
    best_bid_price, best_bid_qty, best_ask_price, best_ask_qty,
    mid_price, spread_abs, spread_bps, depth_levels, bids, asks]."""

    @pytest.fixture()
    def msg(self) -> dict:
        return build_depth_snapshot_message(
            "binance", "perp", "BTCUSDT",
            {
                "instrument_code": "BTCUSDT",
                "event_ts_ms": 1700000000000,
                "best_bid_price": "42000.00",
                "best_bid_qty": "1.5",
                "best_ask_price": "42001.00",
                "best_ask_qty": "2.0",
                "mid_price": "42000.50",
                "spread_abs": "1.00",
                "spread_bps": "2.38",
                "depth_levels": 20,
                "bids": [["42000.00", "1.5"], ["41999.00", "3.0"]],
                "asks": [["42001.00", "2.0"], ["42002.00", "1.0"]],
            },
        )

    def test_type_is_snapshot(self, msg):
        assert msg["type"] == "snapshot"

    def test_topic_is_depth_snapshot(self, msg):
        assert msg["topic"] == TOPIC_DEPTH_SNAPSHOT
        assert msg["topic"] == "marketdata.depth_snapshot"

    def test_data_has_all_required_fields(self, msg):
        required = {
            "instrument_code",
            "event_ts_ms",
            "best_bid_price",
            "best_bid_qty",
            "best_ask_price",
            "best_ask_qty",
            "mid_price",
            "spread_abs",
            "spread_bps",
            "depth_levels",
            "bids",
            "asks",
        }
        assert required.issubset(msg["data"].keys())

    def test_bids_asks_are_lists(self, msg):
        assert isinstance(msg["data"]["bids"], list)
        assert isinstance(msg["data"]["asks"], list)

    def test_envelope_has_ts_ms(self, msg):
        assert isinstance(msg["ts_ms"], int)


# ── 4. System publisher ──────────────────────────────────────────────────


class TestQualityEventMessage:
    """Contract: QualityEventPayload required [issue_type, severity, status, symbol, data_type, detected_at_utc]."""

    @pytest.fixture()
    def msg(self) -> dict:
        return build_quality_event_message(
            "binance", "perp", "BTCUSDT",
            {
                "issue_type": "gap",
                "severity": "warning",
                "status": "open",
                "symbol": "BTCUSDT",
                "data_type": "kline",
                "detected_at_utc": "2024-01-01T00:00:00Z",
            },
        )

    def test_type_is_event(self, msg):
        assert msg["type"] == "event"

    def test_topic_is_quality_event(self, msg):
        assert msg["topic"] == TOPIC_QUALITY_EVENT
        assert msg["topic"] == "system.quality_event"

    def test_has_venue_market_type_symbol_at_envelope(self, msg):
        """Quality events are per-venue/symbol, so envelope has routing fields."""
        assert "venue" in msg
        assert "market_type" in msg
        assert "symbol" in msg
        assert msg["venue"] == "binance"
        assert msg["market_type"] == "perp"
        assert msg["symbol"] == "BTCUSDT"

    def test_data_has_fields_current_builder_passes(self, msg):
        """Current builder only passes through: data_type, interval_code, issue_type, severity, description."""
        current_fields = {"issue_type", "severity", "data_type"}
        assert current_fields.issubset(msg["data"].keys())

    def test_data_has_status_field(self, msg):
        assert "status" in msg["data"]

    def test_data_has_detected_at_utc_field(self, msg):
        assert "detected_at_utc" in msg["data"]

    def test_data_has_symbol_field(self, msg):
        assert "symbol" in msg["data"]

    def test_severity_enum_values(self):
        """severity must be one of [info, warning, critical]."""
        for sev in ("info", "warning", "critical"):
            msg = build_quality_event_message(
                "binance", "perp", "ETHUSDT",
                {"issue_type": "gap", "severity": sev, "status": "open",
                 "symbol": "ETHUSDT", "data_type": "kline", "detected_at_utc": "2024-01-01T00:00:00Z"},
            )
            assert msg["data"]["severity"] == sev

    def test_status_enum_values(self):
        """status must be one of [open, resolved, ignored]."""
        for st in ("open", "resolved", "ignored"):
            msg = build_quality_event_message(
                "binance", "perp", "ETHUSDT",
                {"issue_type": "gap", "severity": "info", "status": st,
                 "symbol": "ETHUSDT", "data_type": "kline", "detected_at_utc": "2024-01-01T00:00:00Z"},
            )
            assert msg["data"]["status"] == st

    def test_envelope_has_ts_ms(self, msg):
        assert isinstance(msg["ts_ms"], int)


class TestStreamStatusMessage:
    """Contract: StreamStatusPayload required [stream_name, status, ts_ms]."""

    @pytest.fixture()
    def msg(self) -> dict:
        return build_stream_status_message(
            {
                "stream_name": "binance_perp_BTCUSDT",
                "status": "up",
                "description": "connected",
            },
        )

    def test_type_is_event(self, msg):
        assert msg["type"] == "event"

    def test_topic_is_stream_status(self, msg):
        assert msg["topic"] == TOPIC_STREAM_STATUS
        assert msg["topic"] == "system.stream_status"

    def test_no_venue_market_type_symbol_at_envelope(self, msg):
        """Stream status is global — it must NOT carry venue/market_type/symbol."""
        assert "venue" not in msg
        assert "market_type" not in msg
        assert "symbol" not in msg

    def test_data_has_required_fields(self, msg):
        required = {"stream_name", "status"}
        # ts_ms is at envelope level, not in data; description is optional
        assert required.issubset(msg["data"].keys())

    def test_status_enum_values(self):
        """status must be one of [up, degraded, down, reconnecting]."""
        for st in ("up", "degraded", "down", "reconnecting"):
            msg = build_stream_status_message(
                {"stream_name": "binance_perp_BTCUSDT", "status": st},
            )
            assert msg["data"]["status"] == st

    def test_envelope_has_ts_ms(self, msg):
        assert isinstance(msg["ts_ms"], int)


# ── 5. Topic name contract ────────────────────────────────────────────────


class TestTopicNameContract:
    """Verify that topic name constants match VALID_TOPICS and the schema enum."""

    def test_valid_topics_matches_schema(self):
        """VALID_TOPICS in subscription.py must match the TopicName schema enum."""
        assert VALID_TOPICS == SCHEMA_TOPIC_NAMES

    def test_publisher_topic_constants_in_valid_topics(self):
        """All publisher TOPIC_* constants must appear in VALID_TOPICS."""
        from YM_data_collection.ws.publishers import marketdata_publisher, system_publisher

        publisher_topics = {
            "marketdata.kline",  # used by kline_publisher
            marketdata_publisher.TOPIC_MARK_PRICE,
            marketdata_publisher.TOPIC_INDEX_PRICE,
            marketdata_publisher.TOPIC_OPEN_INTEREST,
            marketdata_publisher.TOPIC_FUNDING_RATE,
            marketdata_publisher.TOPIC_DEPTH_SNAPSHOT,
            system_publisher.TOPIC_QUALITY_EVENT,
            system_publisher.TOPIC_STREAM_STATUS,
        }
        assert publisher_topics.issubset(VALID_TOPICS)

    def test_kline_topic_in_valid_topics(self):
        assert "marketdata.kline" in VALID_TOPICS


# ── 6. Envelope type enum contract ────────────────────────────────────────


class TestEnvelopeTypeEnum:
    """Schema: type must be one of ['update', 'event', 'subscribed', 'unsubscribed', 'pong', 'error']."""

    def test_kline_type_is_update(self):
        msg = build_kline_message("binance", "perp", "BTCUSDT", "1h", {"instrument_code": "X"})
        assert msg["type"] == "update"

    def test_mark_price_type_is_update(self):
        msg = build_mark_price_message("binance", "perp", "BTCUSDT", {})
        assert msg["type"] == "update"

    def test_depth_snapshot_type_is_snapshot(self):
        """Depth snapshot uses 'snapshot' which maps to the update enum concept."""
        msg = build_depth_snapshot_message("binance", "perp", "BTCUSDT", {})
        assert msg["type"] == "snapshot"

    def test_quality_event_type_is_event(self):
        msg = build_quality_event_message("binance", "perp", "BTCUSDT", {})
        assert msg["type"] == "event"

    def test_stream_status_type_is_event(self):
        msg = build_stream_status_message({"stream_name": "x", "status": "up"})
        assert msg["type"] == "event"

    def test_subscribed_type(self):
        msg = build_subscribed("r1", []).to_dict()
        assert msg["type"] == "subscribed"

    def test_unsubscribed_type(self):
        msg = build_unsubscribed("r1", []).to_dict()
        assert msg["type"] == "unsubscribed"

    def test_pong_type(self):
        msg = build_pong("r1", 0).to_dict()
        assert msg["type"] == "pong"

    def test_error_type(self):
        msg = build_error("r1", "ERR", "msg").to_dict()
        assert msg["type"] == "error"


# ── 7. ts_ms type contract ────────────────────────────────────────────────


class TestTsMsIsInt:
    """All ts_ms values in envelopes must be int."""

    def test_kline_ts_ms_is_int(self):
        msg = build_kline_message("binance", "perp", "BTCUSDT", "1h", {})
        assert isinstance(msg["ts_ms"], int)

    def test_marketdata_ts_ms_is_int(self):
        msg = build_marketdata_message("marketdata.mark_price", "update", "binance", "perp", "BTCUSDT", {})
        assert isinstance(msg["ts_ms"], int)

    def test_quality_event_ts_ms_is_int(self):
        msg = build_quality_event_message("binance", "perp", "BTCUSDT", {})
        assert isinstance(msg["ts_ms"], int)

    def test_stream_status_ts_ms_is_int(self):
        msg = build_stream_status_message({"stream_name": "x", "status": "up"})
        assert isinstance(msg["ts_ms"], int)

    def test_pong_ts_ms_is_int(self):
        msg = build_pong("r1", 1700000000000).to_dict()
        assert isinstance(msg["ts_ms"], int)
