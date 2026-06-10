"""Tests for BinanceGateway, BinanceSpotAdapter, BinancePerpAdapter — URL routing, header parsing, market-type dispatch."""

from __future__ import annotations

import json
from typing import Any

import httpx
import pytest
import respx

from YM_data_collection.adapters.binance_gateway import BinanceGateway
from YM_data_collection.config.models import (
    BinanceConfig,
    BinanceEndpointConfig,
    BinanceRateLimitConfig,
)


# ---------------------------------------------------------------------------
# Fixtures & helpers
# ---------------------------------------------------------------------------

SPOT_BASE = "https://api.binance.com"
PERP_BASE = "https://fapi.binance.com"


def _make_config() -> BinanceConfig:
    return BinanceConfig(
        spot=BinanceEndpointConfig(
            rest_base_url=SPOT_BASE,
            ws_base_url="wss://stream.binance.com:9443/ws",
        ),
        perp=BinanceEndpointConfig(
            rest_base_url=PERP_BASE,
            ws_base_url="wss://fstream.binance.com/ws",
        ),
        rate_limit=BinanceRateLimitConfig(
            spot_weight_per_minute=1200,
            perp_weight_per_minute=2400,
            min_request_interval_ms=0,
            backoff_on_429_seconds=1,
        ),
    )


def _spot_kline_response() -> list[list[Any]]:
    """Minimal realistic spot kline response."""
    return [
        [
            1609459200000,   # open time
            "28923.63",      # open
            "28924.00",      # high
            "28900.00",      # low
            "28910.01",      # close
            "12.345",        # volume
            1609462799999,   # close time
            "356789.00",     # quote asset volume
            123,             # number of trades
            "6.789",         # taker buy base asset volume
            "196432.10",     # taker buy quote asset volume
            "0",             # ignore
        ]
    ]


def _perp_kline_response() -> list[list[Any]]:
    """Minimal realistic perp kline response."""
    return [
        [
            1609459200000,
            "28923.63",
            "28924.00",
            "28900.00",
            "28910.01",
            "12.345",
            1609462799999,
            "356789.00",
            123,
            "6.789",
            "196432.10",
            "0",
        ]
    ]


def _funding_rate_response() -> list[dict[str, Any]]:
    return [
        {
            "symbol": "BTCUSDT",
            "fundingRate": "0.00010000",
            "fundingTime": 1609459200000,
        }
    ]


def _mark_price_kline_response() -> list[list[Any]]:
    return [
        [
            1609459200000,
            "28923.63",
            "28924.00",
            "28900.00",
            "28910.01",
            "0",
            1609462799999,
        ]
    ]


def _open_interest_hist_response() -> list[dict[str, Any]]:
    return [
        {
            "symbol": "BTCUSDT",
            "sumOpenInterest": "10000.5",
            "sumOpenInterestValue": "289105000.0",
            "timestamp": 1609459200000,
        }
    ]


def _premium_index_response() -> dict[str, Any]:
    return {
        "symbol": "BTCUSDT",
        "markPrice": "28910.01",
        "indexPrice": "28905.00",
        "estimatedSettlePrice": "28908.50",
        "lastFundingRate": "0.00010000",
        "interestRate": "0.00010000",
        "nextFundingTime": 1609480800000,
        "time": 1609459200000,
    }


def _exchange_info_response() -> dict[str, Any]:
    return {"timezone": "UTC", "symbols": []}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestBinanceGatewaySpotRouting:
    """Verify that spot calls hit the correct URL pattern."""

    @pytest.mark.asyncio
    async def test_spot_klines_url(self) -> None:
        cfg = _make_config()
        with respx.mock:
            route = respx.get(f"{SPOT_BASE}/api/v3/klines").mock(
                return_value=httpx.Response(
                    200,
                    json=_spot_kline_response(),
                    headers={"X-MBX-USED-WEIGHT-1M": "5"},
                )
            )
            async with httpx.AsyncClient() as client:
                gw = BinanceGateway(cfg, client=client)
                result = await gw.fetch_klines(
                    "spot", "BTCUSDT", "1h", 1609459200000, 1609462800000
                )
                await gw.close()
            assert route.called
            assert len(result) == 1
            assert result[0][0] == 1609459200000

    @pytest.mark.asyncio
    async def test_spot_exchange_info_url(self) -> None:
        cfg = _make_config()
        with respx.mock:
            route = respx.get(f"{SPOT_BASE}/api/v3/exchangeInfo").mock(
                return_value=httpx.Response(
                    200,
                    json=_exchange_info_response(),
                    headers={"X-MBX-USED-WEIGHT-1M": "1"},
                )
            )
            async with httpx.AsyncClient() as client:
                gw = BinanceGateway(cfg, client=client)
                result = await gw.fetch_exchange_info("spot")
                await gw.close()
            assert route.called
            assert result["timezone"] == "UTC"


class TestBinanceGatewayPerpRouting:
    """Verify that perp calls hit the correct URL pattern."""

    @pytest.mark.asyncio
    async def test_perp_klines_url(self) -> None:
        cfg = _make_config()
        with respx.mock:
            route = respx.get(f"{PERP_BASE}/fapi/v1/klines").mock(
                return_value=httpx.Response(
                    200,
                    json=_perp_kline_response(),
                    headers={"x-mbx-used-weight-1m": "3"},
                )
            )
            async with httpx.AsyncClient() as client:
                gw = BinanceGateway(cfg, client=client)
                result = await gw.fetch_klines(
                    "perp", "BTCUSDT", "1h", 1609459200000, 1609462800000
                )
                await gw.close()
            assert route.called
            assert len(result) == 1

    @pytest.mark.asyncio
    async def test_perp_funding_rates_url(self) -> None:
        cfg = _make_config()
        with respx.mock:
            route = respx.get(f"{PERP_BASE}/fapi/v1/fundingRate").mock(
                return_value=httpx.Response(
                    200,
                    json=_funding_rate_response(),
                    headers={"x-mbx-used-weight-1m": "2"},
                )
            )
            async with httpx.AsyncClient() as client:
                gw = BinanceGateway(cfg, client=client)
                result = await gw.fetch_funding_rates(
                    "BTCUSDT", 1609459200000, 1609462800000
                )
                await gw.close()
            assert route.called
            assert result[0]["symbol"] == "BTCUSDT"

    @pytest.mark.asyncio
    async def test_perp_mark_price_klines_url(self) -> None:
        cfg = _make_config()
        with respx.mock:
            route = respx.get(f"{PERP_BASE}/fapi/v1/markPriceKlines").mock(
                return_value=httpx.Response(
                    200,
                    json=_mark_price_kline_response(),
                    headers={"x-mbx-used-weight-1m": "2"},
                )
            )
            async with httpx.AsyncClient() as client:
                gw = BinanceGateway(cfg, client=client)
                result = await gw.fetch_mark_price_klines(
                    "BTCUSDT", "1h", 1609459200000, 1609462800000
                )
                await gw.close()
            assert route.called
            assert len(result) == 1

    @pytest.mark.asyncio
    async def test_perp_open_interest_hist_url(self) -> None:
        cfg = _make_config()
        with respx.mock:
            route = respx.get(f"{PERP_BASE}/futures/data/openInterestHist").mock(
                return_value=httpx.Response(
                    200,
                    json=_open_interest_hist_response(),
                    headers={"x-mbx-used-weight-1m": "2"},
                )
            )
            async with httpx.AsyncClient() as client:
                gw = BinanceGateway(cfg, client=client)
                result = await gw.fetch_open_interest_hist(
                    "BTCUSDT", "5m", 1609459200000, 1609462800000
                )
                await gw.close()
            assert route.called
            assert result[0]["symbol"] == "BTCUSDT"

    @pytest.mark.asyncio
    async def test_perp_premium_index_url(self) -> None:
        cfg = _make_config()
        with respx.mock:
            route = respx.get(f"{PERP_BASE}/fapi/v1/premiumIndex").mock(
                return_value=httpx.Response(
                    200,
                    json=_premium_index_response(),
                    headers={"x-mbx-used-weight-1m": "1"},
                )
            )
            async with httpx.AsyncClient() as client:
                gw = BinanceGateway(cfg, client=client)
                result = await gw.fetch_premium_index("BTCUSDT")
                await gw.close()
            assert route.called
            assert result["markPrice"] == "28910.01"

    @pytest.mark.asyncio
    async def test_perp_exchange_info_url(self) -> None:
        cfg = _make_config()
        with respx.mock:
            route = respx.get(f"{PERP_BASE}/fapi/v1/exchangeInfo").mock(
                return_value=httpx.Response(
                    200,
                    json=_exchange_info_response(),
                    headers={"x-mbx-used-weight-1m": "1"},
                )
            )
            async with httpx.AsyncClient() as client:
                gw = BinanceGateway(cfg, client=client)
                result = await gw.fetch_exchange_info("perp")
                await gw.close()
            assert route.called
            assert result["timezone"] == "UTC"


class TestBinanceGatewayMarketTypeDispatch:
    """Verify market_type selects the correct adapter and default limit."""

    @pytest.mark.asyncio
    async def test_spot_default_limit_1000(self) -> None:
        cfg = _make_config()
        with respx.mock:
            route = respx.get(f"{SPOT_BASE}/api/v3/klines").mock(
                return_value=httpx.Response(
                    200,
                    json=_spot_kline_response(),
                    headers={"X-MBX-USED-WEIGHT-1M": "5"},
                )
            )
            async with httpx.AsyncClient() as client:
                gw = BinanceGateway(cfg, client=client)
                await gw.fetch_klines(
                    "spot", "BTCUSDT", "1h", 1609459200000, 1609462800000
                )
                await gw.close()
            # Verify limit=1000 in the request
            request = route.calls[0].request
            assert "limit=1000" in str(request.url)

    @pytest.mark.asyncio
    async def test_perp_default_limit_1500(self) -> None:
        cfg = _make_config()
        with respx.mock:
            route = respx.get(f"{PERP_BASE}/fapi/v1/klines").mock(
                return_value=httpx.Response(
                    200,
                    json=_perp_kline_response(),
                    headers={"x-mbx-used-weight-1m": "3"},
                )
            )
            async with httpx.AsyncClient() as client:
                gw = BinanceGateway(cfg, client=client)
                await gw.fetch_klines(
                    "perp", "BTCUSDT", "1h", 1609459200000, 1609462800000
                )
                await gw.close()
            request = route.calls[0].request
            assert "limit=1500" in str(request.url)


class TestBinanceGatewayRateLimitHeaderParsing:
    """Verify that weight headers calibrate the correct limiter."""

    @pytest.mark.asyncio
    async def test_spot_header_calibrates_spot_limiter(self) -> None:
        cfg = _make_config()
        with respx.mock:
            respx.get(f"{SPOT_BASE}/api/v3/klines").mock(
                return_value=httpx.Response(
                    200,
                    json=_spot_kline_response(),
                    headers={"X-MBX-USED-WEIGHT-1M": "42"},
                )
            )
            async with httpx.AsyncClient() as client:
                gw = BinanceGateway(cfg, client=client)
                await gw.fetch_klines(
                    "spot", "BTCUSDT", "1h", 1609459200000, 1609462800000
                )
                assert gw._spot_limiter.used_weight == 42
                # Perp limiter should be untouched
                assert gw._perp_limiter.used_weight == 0
                await gw.close()

    @pytest.mark.asyncio
    async def test_perp_header_calibrates_perp_limiter(self) -> None:
        cfg = _make_config()
        with respx.mock:
            respx.get(f"{PERP_BASE}/fapi/v1/klines").mock(
                return_value=httpx.Response(
                    200,
                    json=_perp_kline_response(),
                    headers={"x-mbx-used-weight-1m": "99"},
                )
            )
            async with httpx.AsyncClient() as client:
                gw = BinanceGateway(cfg, client=client)
                await gw.fetch_klines(
                    "perp", "BTCUSDT", "1h", 1609459200000, 1609462800000
                )
                assert gw._perp_limiter.used_weight == 99
                assert gw._spot_limiter.used_weight == 0
                await gw.close()


class TestBinanceGatewayQueryParamConstruction:
    """Verify that query parameters are correctly built."""

    @pytest.mark.asyncio
    async def test_spot_klines_params(self) -> None:
        cfg = _make_config()
        with respx.mock:
            route = respx.get(f"{SPOT_BASE}/api/v3/klines").mock(
                return_value=httpx.Response(
                    200,
                    json=_spot_kline_response(),
                    headers={"X-MBX-USED-WEIGHT-1M": "5"},
                )
            )
            async with httpx.AsyncClient() as client:
                gw = BinanceGateway(cfg, client=client)
                await gw.fetch_klines(
                    "spot", "ETHUSDT", "4h", 1609459200000, 1609473600000, limit=500
                )
                await gw.close()
            request = route.calls[0].request
            url_str = str(request.url)
            assert "symbol=ETHUSDT" in url_str
            assert "interval=4h" in url_str
            assert "startTime=1609459200000" in url_str
            assert "endTime=1609473600000" in url_str
            assert "limit=500" in url_str

    @pytest.mark.asyncio
    async def test_perp_funding_rates_params(self) -> None:
        cfg = _make_config()
        with respx.mock:
            route = respx.get(f"{PERP_BASE}/fapi/v1/fundingRate").mock(
                return_value=httpx.Response(
                    200,
                    json=_funding_rate_response(),
                    headers={"x-mbx-used-weight-1m": "2"},
                )
            )
            async with httpx.AsyncClient() as client:
                gw = BinanceGateway(cfg, client=client)
                await gw.fetch_funding_rates(
                    "ETHUSDT", 1609459200000, 1609473600000, limit=500
                )
                await gw.close()
            request = route.calls[0].request
            url_str = str(request.url)
            assert "symbol=ETHUSDT" in url_str
            assert "startTime=1609459200000" in url_str
            assert "endTime=1609473600000" in url_str
            assert "limit=500" in url_str

    @pytest.mark.asyncio
    async def test_perp_open_interest_hist_params(self) -> None:
        cfg = _make_config()
        with respx.mock:
            route = respx.get(f"{PERP_BASE}/futures/data/openInterestHist").mock(
                return_value=httpx.Response(
                    200,
                    json=_open_interest_hist_response(),
                    headers={"x-mbx-used-weight-1m": "2"},
                )
            )
            async with httpx.AsyncClient() as client:
                gw = BinanceGateway(cfg, client=client)
                await gw.fetch_open_interest_hist(
                    "BTCUSDT", "5m", 1609459200000, 1609462800000, limit=200
                )
                await gw.close()
            request = route.calls[0].request
            url_str = str(request.url)
            assert "symbol=BTCUSDT" in url_str
            assert "period=5m" in url_str
            assert "limit=200" in url_str

    @pytest.mark.asyncio
    async def test_premium_index_params(self) -> None:
        cfg = _make_config()
        with respx.mock:
            route = respx.get(f"{PERP_BASE}/fapi/v1/premiumIndex").mock(
                return_value=httpx.Response(
                    200,
                    json=_premium_index_response(),
                    headers={"x-mbx-used-weight-1m": "1"},
                )
            )
            async with httpx.AsyncClient() as client:
                gw = BinanceGateway(cfg, client=client)
                await gw.fetch_premium_index("BTCUSDT")
                await gw.close()
            request = route.calls[0].request
            url_str = str(request.url)
            assert "symbol=BTCUSDT" in url_str


class TestBinanceGatewayClientOwnership:
    """Verify that gateway closes client only when it owns it."""

    @pytest.mark.asyncio
    async def test_close_owned_client(self) -> None:
        cfg = _make_config()
        gw = BinanceGateway(cfg)  # no client provided → owns it
        assert gw._owns_client is True
        await gw.close()  # should not raise

    @pytest.mark.asyncio
    async def test_close_external_client_does_not_aclose(self) -> None:
        cfg = _make_config()
        client = httpx.AsyncClient()
        gw = BinanceGateway(cfg, client=client)
        assert gw._owns_client is False
        await gw.close()
        # Client is still open (caller's responsibility)
        # Just ensure no exception
        await client.aclose()
