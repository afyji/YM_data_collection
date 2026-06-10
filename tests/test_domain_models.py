"""Tests for domain models."""

from datetime import datetime, timezone
from decimal import Decimal

from YM_data_collection.domain.models import (
    InstrumentInfo,
    NormalizedKline,
    NormalizedFundingRate,
    NormalizedMarkPrice,
    NormalizedDepthSnapshot,
    IngestCheckpoint,
    DataQualityIssue,
)


def test_instrument_info_creation():
    inst = InstrumentInfo(
        venue="binance", market_type="perp", symbol="BTCUSDT",
        base_asset="BTC", quote_asset="USDT",
        instrument_code="crypto.binance.perp.BTCUSDT",
        tick_size=Decimal("0.1"), step_size=Decimal("0.001"),
        min_qty=Decimal("0.001"), min_notional=Decimal("100"),
        contract_type="perpetual",
    )
    assert inst.venue == "binance"
    assert inst.instrument_code == "crypto.binance.perp.BTCUSDT"


def test_normalized_kline_creation():
    kline = NormalizedKline(
        venue="binance", symbol="BTCUSDT",
        instrument_code="crypto.binance.spot.BTCUSDT",
        interval_code="1h", market_type="spot",
        open_ts_ms=1710000000000, close_ts_ms=1710003599999,
        open_dt_utc=datetime(2024, 3, 9, 16, 0, tzinfo=timezone.utc),
        close_dt_utc=datetime(2024, 3, 9, 16, 59, 59, tzinfo=timezone.utc),
        open_price=Decimal("68000.10"), high_price=Decimal("68120.50"),
        low_price=Decimal("67980.00"), close_price=Decimal("68080.20"),
        volume=Decimal("123.456"), quote_volume=Decimal("8401234.12"),
        trade_count=3201,
        taker_buy_base_volume=Decimal("62.3"),
        taker_buy_quote_volume=Decimal("4240000.23"),
    )
    assert kline.interval_code == "1h"
    assert kline.open_price == Decimal("68000.10")


def test_normalized_funding_rate():
    fr = NormalizedFundingRate(
        venue="binance", symbol="BTCUSDT",
        instrument_code="crypto.binance.perp.BTCUSDT",
        funding_time_ts_ms=1710028800000,
        funding_time_dt_utc=datetime(2024, 3, 10, 0, 0, tzinfo=timezone.utc),
        funding_rate=Decimal("0.0001"),
    )
    assert fr.funding_rate == Decimal("0.0001")


def test_normalized_depth_snapshot():
    ds = NormalizedDepthSnapshot(
        venue="binance", symbol="BTCUSDT",
        instrument_code="crypto.binance.perp.BTCUSDT",
        market_type="perp",
        event_ts_ms=1710000001000,
        event_dt_utc=datetime(2024, 3, 9, 16, 0, 1, tzinfo=timezone.utc),
        best_bid_price=Decimal("68010.40"), best_bid_qty=Decimal("1.25"),
        best_ask_price=Decimal("68010.60"), best_ask_qty=Decimal("0.98"),
        mid_price=Decimal("68010.50"), spread_abs=Decimal("0.20"),
        spread_bps=Decimal("0.029409"), depth_levels=10,
        bid_depth_json=[["68010.40", "1.25"]], ask_depth_json=[["68010.60", "0.98"]],
    )
    assert ds.best_bid_price < ds.best_ask_price


def test_ingest_checkpoint_defaults():
    cp = IngestCheckpoint(
        venue="binance", market_type="perp", symbol="BTCUSDT", data_type="kline",
    )
    assert cp.status == "ok"
    assert cp.interval_code is None


def test_data_quality_issue():
    issue = DataQualityIssue(
        venue="binance", market_type="perp", symbol="BTCUSDT",
        data_type="kline", issue_type="missing_bar", severity="warning",
        detected_at_utc=datetime(2024, 3, 9, 17, 0, tzinfo=timezone.utc),
        description="Missing 1h bar",
    )
    assert issue.status == "open"
