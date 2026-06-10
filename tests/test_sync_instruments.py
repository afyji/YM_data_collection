"""Tests for sync_instruments — parsing, filtering, and end-to-end CLI flow."""

from __future__ import annotations

import argparse
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from YM_data_collection.apps.sync_instruments import (
    _parse_perp_symbol,
    _parse_spot_symbol,
    _parse_symbols,
    build_parser,
    main,
)
from YM_data_collection.domain.models import InstrumentInfo


# ---------------------------------------------------------------------------
# Realistic Binance response fixtures
# ---------------------------------------------------------------------------

def _spot_exchange_info_response() -> dict:
    """Minimal but realistic Binance spot exchangeInfo response."""
    return {
        "timezone": "UTC",
        "symbols": [
            {
                "symbol": "BTCUSDT",
                "baseAsset": "BTC",
                "quoteAsset": "USDT",
                "status": "TRADING",
                "filters": [
                    {"filterType": "PRICE_FILTER", "tickSize": "0.01000000"},
                    {"filterType": "LOT_SIZE", "stepSize": "0.00000100", "minQty": "0.00000100"},
                    {"filterType": "MIN_NOTIONAL", "minNotional": "10.00000000"},
                ],
            },
            {
                "symbol": "ETHUSDT",
                "baseAsset": "ETH",
                "quoteAsset": "USDT",
                "status": "TRADING",
                "filters": [
                    {"filterType": "PRICE_FILTER", "tickSize": "0.00100000"},
                    {"filterType": "LOT_SIZE", "stepSize": "0.00001000", "minQty": "0.00001000"},
                    {"filterType": "NOTIONAL", "minNotional": "5.00000000"},
                ],
            },
            {
                "symbol": "XRPUSDT",
                "baseAsset": "XRP",
                "quoteAsset": "USDT",
                "status": "BREAK",  # not TRADING
                "filters": [
                    {"filterType": "PRICE_FILTER", "tickSize": "0.00010000"},
                    {"filterType": "LOT_SIZE", "stepSize": "0.10000000", "minQty": "0.10000000"},
                    {"filterType": "MIN_NOTIONAL", "minNotional": "1.00000000"},
                ],
            },
        ],
    }


def _perp_exchange_info_response() -> dict:
    """Minimal but realistic Binance perp exchangeInfo response."""
    return {
        "timezone": "UTC",
        "symbols": [
            {
                "symbol": "BTCUSDT",
                "baseAsset": "BTC",
                "quoteAsset": "USDT",
                "status": "TRADING",
                "contractType": "PERPETUAL",
                "filters": [
                    {"filterType": "PRICE_FILTER", "tickSize": "0.10000000"},
                    {"filterType": "LOT_SIZE", "stepSize": "0.00100000", "minQty": "0.00100000"},
                    {"filterType": "MIN_NOTIONAL", "minNotional": "5.00000000"},
                ],
            },
            {
                "symbol": "ETHUSDT",
                "baseAsset": "ETH",
                "quoteAsset": "USDT",
                "status": "TRADING",
                "contractType": "PERPETUAL",
                "filters": [
                    {"filterType": "PRICE_FILTER", "tickSize": "0.01000000"},
                    {"filterType": "LOT_SIZE", "stepSize": "0.01000000", "minQty": "0.01000000"},
                    {"filterType": "NOTIONAL", "minNotional": "5.00000000"},
                ],
            },
        ],
    }


# ---------------------------------------------------------------------------
# Unit tests: spot parsing
# ---------------------------------------------------------------------------

class TestParseSpotSymbol:
    def test_btcusdt_spot(self) -> None:
        symb = _spot_exchange_info_response()["symbols"][0]
        inst = _parse_spot_symbol(symb, "binance")

        assert inst.venue == "binance"
        assert inst.market_type == "spot"
        assert inst.symbol == "BTCUSDT"
        assert inst.base_asset == "BTC"
        assert inst.quote_asset == "USDT"
        assert inst.instrument_code == "binance:spot:BTCUSDT"
        assert inst.is_active is True
        assert inst.tick_size == Decimal("0.01000000")
        assert inst.step_size == Decimal("0.00000100")
        assert inst.min_qty == Decimal("0.00000100")
        assert inst.min_notional == Decimal("10.00000000")
        assert inst.contract_type is None

    def test_inactive_symbol(self) -> None:
        symb = _spot_exchange_info_response()["symbols"][2]  # XRPUSDT, status=BREAK
        inst = _parse_spot_symbol(symb, "binance")
        assert inst.is_active is False

    def test_notional_filter_name_variant(self) -> None:
        """ETHUSDT uses NOTIONAL instead of MIN_NOTIONAL — should still parse."""
        symb = _spot_exchange_info_response()["symbols"][1]
        inst = _parse_spot_symbol(symb, "binance")
        assert inst.min_notional == Decimal("5.00000000")


# ---------------------------------------------------------------------------
# Unit tests: perp parsing
# ---------------------------------------------------------------------------

class TestParsePerpSymbol:
    def test_btcusdt_perp(self) -> None:
        symb = _perp_exchange_info_response()["symbols"][0]
        inst = _parse_perp_symbol(symb, "binance")

        assert inst.venue == "binance"
        assert inst.market_type == "perp"
        assert inst.symbol == "BTCUSDT"
        assert inst.instrument_code == "binance:perp:BTCUSDT"
        assert inst.is_active is True
        assert inst.contract_type == "PERPETUAL"
        assert inst.tick_size == Decimal("0.10000000")
        assert inst.step_size == Decimal("0.00100000")

    def test_ethusdt_perp(self) -> None:
        symb = _perp_exchange_info_response()["symbols"][1]
        inst = _parse_perp_symbol(symb, "binance")
        assert inst.contract_type == "PERPETUAL"
        assert inst.min_notional == Decimal("5.00000000")


# ---------------------------------------------------------------------------
# Unit tests: _parse_symbols with filtering
# ---------------------------------------------------------------------------

class TestParseSymbols:
    def test_spot_no_filter(self) -> None:
        response = _spot_exchange_info_response()
        results = _parse_symbols(response, "spot", "binance")
        assert len(results) == 3

    def test_spot_filter_btcusdt_only(self) -> None:
        response = _spot_exchange_info_response()
        results = _parse_symbols(response, "spot", "binance", allowed_symbols={"BTCUSDT"})
        assert len(results) == 1
        assert results[0].symbol == "BTCUSDT"

    def test_spot_filter_multiple(self) -> None:
        response = _spot_exchange_info_response()
        results = _parse_symbols(response, "spot", "binance", allowed_symbols={"BTCUSDT", "ETHUSDT"})
        assert len(results) == 2

    def test_spot_filter_no_match(self) -> None:
        response = _spot_exchange_info_response()
        results = _parse_symbols(response, "spot", "binance", allowed_symbols={"DOGEUSDT"})
        assert len(results) == 0

    def test_perp_no_filter(self) -> None:
        response = _perp_exchange_info_response()
        results = _parse_symbols(response, "perp", "binance")
        assert len(results) == 2
        assert all(r.market_type == "perp" for r in results)

    def test_perp_filter_btcusdt(self) -> None:
        response = _perp_exchange_info_response()
        results = _parse_symbols(response, "perp", "binance", allowed_symbols={"BTCUSDT"})
        assert len(results) == 1
        assert results[0].contract_type == "PERPETUAL"

    def test_empty_response(self) -> None:
        results = _parse_symbols({"symbols": []}, "spot", "binance")
        assert results == []

    def test_no_symbols_key(self) -> None:
        results = _parse_symbols({}, "spot", "binance")
        assert results == []


# ---------------------------------------------------------------------------
# Integration test: main() with mocked dependencies
# ---------------------------------------------------------------------------

def _make_config_mock(
    spot_enabled: bool = True,
    perp_enabled: bool = True,
    symbols: list[str] | None = None,
) -> MagicMock:
    """Build a minimal config mock that satisfies _sync()."""
    config = MagicMock()
    config.binance.spot_enabled = spot_enabled
    config.binance.perp_enabled = perp_enabled
    config.binance.symbols = symbols or ["BTCUSDT", "ETHUSDT"]
    # Provide enough of BinanceConfig for BinanceGateway
    config.binance.spot = MagicMock(rest_base_url="https://api.binance.com", ws_base_url="wss://stream.binance.com:9443/ws")
    config.binance.perp = MagicMock(rest_base_url="https://fapi.binance.com", ws_base_url="wss://fstream.binance.com/ws")
    config.binance.http_timeout_seconds = 10
    config.binance.rate_limit = MagicMock(
        spot_weight_per_minute=1200,
        perp_weight_per_minute=2400,
        min_request_interval_ms=0,
        backoff_on_429_seconds=1,
    )
    config.mysql = MagicMock()
    return config


class TestMainEndToEnd:
    """Test main() with fully mocked infrastructure."""

    @patch("YM_data_collection.apps.sync_instruments.InstrumentRepository")
    @patch("YM_data_collection.apps.sync_instruments.create_session_factory", return_value=MagicMock())
    @patch("YM_data_collection.apps.sync_instruments.create_mysql_engine", return_value=MagicMock())
    @patch("YM_data_collection.apps.sync_instruments.BinanceGateway")
    @patch("YM_data_collection.apps.sync_instruments.load_config")
    def test_spot_only_sync(
        self,
        mock_load_config: MagicMock,
        mock_gw_cls: MagicMock,
        mock_engine: MagicMock,
        mock_sf: MagicMock,
        mock_repo_cls: MagicMock,
    ) -> None:
        config = _make_config_mock(spot_enabled=True, perp_enabled=False)
        mock_load_config.return_value = config

        gw_instance = AsyncMock()
        gw_instance.fetch_exchange_info = AsyncMock(return_value=_spot_exchange_info_response())
        gw_instance.close = AsyncMock()
        mock_gw_cls.return_value = gw_instance

        repo_instance = MagicMock()
        mock_repo_cls.return_value = repo_instance

        exit_code = main(["--market-type", "spot", "--symbols", "BTCUSDT", "ETHUSDT"])

        assert exit_code == 0
        gw_instance.fetch_exchange_info.assert_called_once_with("spot")
        assert repo_instance.upsert.call_count == 2  # BTCUSDT + ETHUSDT

    @patch("YM_data_collection.apps.sync_instruments.InstrumentRepository")
    @patch("YM_data_collection.apps.sync_instruments.create_session_factory", return_value=MagicMock())
    @patch("YM_data_collection.apps.sync_instruments.create_mysql_engine", return_value=MagicMock())
    @patch("YM_data_collection.apps.sync_instruments.BinanceGateway")
    @patch("YM_data_collection.apps.sync_instruments.load_config")
    def test_perp_only_sync(
        self,
        mock_load_config: MagicMock,
        mock_gw_cls: MagicMock,
        mock_engine: MagicMock,
        mock_sf: MagicMock,
        mock_repo_cls: MagicMock,
    ) -> None:
        config = _make_config_mock(spot_enabled=False, perp_enabled=True)
        mock_load_config.return_value = config

        gw_instance = AsyncMock()
        gw_instance.fetch_exchange_info = AsyncMock(return_value=_perp_exchange_info_response())
        gw_instance.close = AsyncMock()
        mock_gw_cls.return_value = gw_instance

        repo_instance = MagicMock()
        mock_repo_cls.return_value = repo_instance

        exit_code = main(["--market-type", "perp", "--symbols", "BTCUSDT"])

        assert exit_code == 0
        gw_instance.fetch_exchange_info.assert_called_once_with("perp")
        assert repo_instance.upsert.call_count == 1  # Only BTCUSDT

    @patch("YM_data_collection.apps.sync_instruments.InstrumentRepository")
    @patch("YM_data_collection.apps.sync_instruments.create_session_factory", return_value=MagicMock())
    @patch("YM_data_collection.apps.sync_instruments.create_mysql_engine", return_value=MagicMock())
    @patch("YM_data_collection.apps.sync_instruments.BinanceGateway")
    @patch("YM_data_collection.apps.sync_instruments.load_config")
    def test_both_spot_and_perp(
        self,
        mock_load_config: MagicMock,
        mock_gw_cls: MagicMock,
        mock_engine: MagicMock,
        mock_sf: MagicMock,
        mock_repo_cls: MagicMock,
    ) -> None:
        config = _make_config_mock(spot_enabled=True, perp_enabled=True)
        mock_load_config.return_value = config

        gw_instance = AsyncMock()
        gw_instance.fetch_exchange_info = AsyncMock(
            side_effect=[
                _spot_exchange_info_response(),
                _perp_exchange_info_response(),
            ]
        )
        gw_instance.close = AsyncMock()
        mock_gw_cls.return_value = gw_instance

        repo_instance = MagicMock()
        mock_repo_cls.return_value = repo_instance

        exit_code = main(["--market-type", "all", "--symbols", "BTCUSDT"])

        assert exit_code == 0
        assert gw_instance.fetch_exchange_info.call_count == 2
        # spot BTCUSDT + perp BTCUSDT = 2 upserts
        assert repo_instance.upsert.call_count == 2

    @patch("YM_data_collection.apps.sync_instruments.InstrumentRepository")
    @patch("YM_data_collection.apps.sync_instruments.create_session_factory", return_value=MagicMock())
    @patch("YM_data_collection.apps.sync_instruments.create_mysql_engine", return_value=MagicMock())
    @patch("YM_data_collection.apps.sync_instruments.BinanceGateway")
    @patch("YM_data_collection.apps.sync_instruments.load_config")
    def test_symbol_filtering_from_config(
        self,
        mock_load_config: MagicMock,
        mock_gw_cls: MagicMock,
        mock_engine: MagicMock,
        mock_sf: MagicMock,
        mock_repo_cls: MagicMock,
    ) -> None:
        """When --symbols not provided, use config.binance.symbols as filter."""
        config = _make_config_mock(spot_enabled=True, perp_enabled=False, symbols=["BTCUSDT"])
        mock_load_config.return_value = config

        gw_instance = AsyncMock()
        gw_instance.fetch_exchange_info = AsyncMock(return_value=_spot_exchange_info_response())
        gw_instance.close = AsyncMock()
        mock_gw_cls.return_value = gw_instance

        repo_instance = MagicMock()
        mock_repo_cls.return_value = repo_instance

        exit_code = main(["--market-type", "spot"])

        assert exit_code == 0
        # Only BTCUSDT should be upserted (config filter)
        assert repo_instance.upsert.call_count == 1
        upserted = repo_instance.upsert.call_args[0][0]
        assert upserted.symbol == "BTCUSDT"

    @patch("YM_data_collection.apps.sync_instruments.load_config")
    def test_no_market_types_enabled(self, mock_load_config: MagicMock) -> None:
        """Should return config error when no market types match."""
        config = _make_config_mock(spot_enabled=False, perp_enabled=False)
        mock_load_config.return_value = config

        exit_code = main(["--market-type", "all"])
        assert exit_code == 3  # CONFIG_ERROR

    @patch("YM_data_collection.apps.sync_instruments.load_config", side_effect=Exception("config boom"))
    def test_config_load_failure(self, mock_load_config: MagicMock) -> None:
        exit_code = main(["--market-type", "spot"])
        assert exit_code == 3  # CONFIG_ERROR (wrapped by CliConfigError)


# ---------------------------------------------------------------------------
# Parser tests
# ---------------------------------------------------------------------------

class TestBuildParser:
    def test_default_args(self) -> None:
        parser = build_parser()
        args = parser.parse_args([])
        assert args.market_type == "spot"
        assert args.venue == "binance"
        assert args.symbols is None  # defaults to config

    def test_custom_args(self) -> None:
        parser = build_parser()
        args = parser.parse_args([
            "--market-type", "perp",
            "--venue", "binance",
            "--symbols", "SOLUSDT", "ADAUSDT",
            "--env", "prod",
        ])
        assert args.market_type == "perp"
        assert args.venue == "binance"
        assert args.symbols == ["SOLUSDT", "ADAUSDT"]
        assert args.env == "prod"
