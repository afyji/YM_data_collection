"""Tests for the slippage estimation API endpoint (DC-T031)."""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient

from YM_data_collection.api.routes.slippage import router as slippage_router


# ── Fixtures ─────────────────────────────────────────────────────────────────


def _make_app() -> FastAPI:
    """Create a minimal FastAPI app with the slippage router mounted."""
    app = FastAPI()
    app.state.slippage_service = MagicMock()
    app.include_router(slippage_router, prefix="/api/v1")
    return app


@pytest.fixture()
def client():
    app = _make_app()
    with TestClient(app, raise_server_exceptions=False) as c:
        yield c


@pytest.fixture()
def mock_service(client: TestClient):
    """Return the mock SlippageService attached to the app."""
    return client.app.state.slippage_service


# ── Helpers ──────────────────────────────────────────────────────────────────

_ESTIMATE_URL = "/api/v1/marketdata/slippage/estimate"


def _base_params(**overrides) -> dict:
    """Return valid default query params, with optional overrides."""
    params = {
        "venue": "binance",
        "market_type": "spot",
        "symbol": "BTCUSDT",
        "side": "buy",
        "quote_asset_amount": "1000",
    }
    params.update(overrides)
    return params


# ── Tests ────────────────────────────────────────────────────────────────────


class TestNormalEstimation:
    """Successful slippage estimation with quote_asset_amount."""

    def test_returns_200_with_ok_response(self, client: TestClient, mock_service):
        mock_service.estimate.return_value = {
            "reference_price": Decimal("50000.00"),
            "estimated_avg_fill_price": Decimal("50001.50"),
            "slippage_abs": Decimal("1.50"),
            "slippage_bps": Decimal("0.30"),
            "filled_qty": Decimal("0.01999994"),
            "unfilled_qty": Decimal("0"),
            "meta": {"filled_levels": 3, "age_ms": 120},
        }
        resp = client.get(_ESTIMATE_URL, params=_base_params())
        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert body["code"] == "OK"

    def test_passes_correct_args_to_service(self, client: TestClient, mock_service):
        mock_service.estimate.return_value = {
            "reference_price": Decimal("50000.00"),
            "estimated_avg_fill_price": Decimal("50001.50"),
            "slippage_abs": Decimal("1.50"),
            "slippage_bps": Decimal("0.30"),
            "filled_qty": Decimal("0.01999994"),
            "unfilled_qty": Decimal("0"),
            "meta": {"filled_levels": 3, "age_ms": 120},
        }
        client.get(
            _ESTIMATE_URL,
            params=_base_params(
                market_type="perp", symbol="ETHUSDT", side="sell", quote_asset_amount="5000"
            ),
        )
        mock_service.estimate.assert_called_once_with(
            market_type="perp",
            symbol="ETHUSDT",
            side="sell",
            amount_quote=Decimal("5000"),
        )


class TestDecimalSerialization:
    """Decimal values must be serialized as strings in the JSON response."""

    def test_decimal_fields_are_strings(self, client: TestClient, mock_service):
        mock_service.estimate.return_value = {
            "reference_price": Decimal("50000.00"),
            "estimated_avg_fill_price": Decimal("50001.50"),
            "slippage_abs": Decimal("1.50"),
            "slippage_bps": Decimal("0.30"),
            "filled_qty": Decimal("0.01999994"),
            "unfilled_qty": Decimal("0"),
            "meta": {"filled_levels": 3, "age_ms": 120},
        }
        resp = client.get(_ESTIMATE_URL, params=_base_params())
        body = resp.json()
        data = body["data"]

        assert isinstance(data["reference_price"], str)
        assert isinstance(data["estimated_avg_fill_price"], str)
        assert isinstance(data["slippage_abs"], str)
        assert isinstance(data["slippage_bps"], str)
        assert isinstance(data["filled_qty"], str)
        assert isinstance(data["unfilled_qty"], str)

    def test_none_decimal_fields_stay_none(self, client: TestClient, mock_service):
        mock_service.estimate.return_value = {
            "reference_price": None,
            "estimated_avg_fill_price": None,
            "slippage_abs": None,
            "slippage_bps": None,
            "filled_qty": Decimal("0"),
            "unfilled_qty": Decimal("1000"),
            "meta": {"error": "no_depth_data"},
        }
        resp = client.get(_ESTIMATE_URL, params=_base_params())
        data = resp.json()["data"]

        assert data["reference_price"] is None
        assert data["estimated_avg_fill_price"] is None
        assert data["slippage_abs"] is None
        assert data["slippage_bps"] is None
        assert isinstance(data["filled_qty"], str)
        assert isinstance(data["unfilled_qty"], str)


class TestMutualExclusivityValidation:
    """quote_asset_amount and base_asset_amount are mutually exclusive."""

    def test_error_when_both_amounts_given(self, client: TestClient, mock_service):
        resp = client.get(
            _ESTIMATE_URL,
            params=_base_params(base_asset_amount="0.1"),
        )
        assert resp.status_code == 400
        body = resp.json()
        assert body["success"] is False
        assert "mutually exclusive" in body["message"].lower()

    def test_error_when_neither_amount_given(self, client: TestClient, mock_service):
        params = _base_params()
        del params["quote_asset_amount"]
        resp = client.get(_ESTIMATE_URL, params=params)
        assert resp.status_code == 400
        body = resp.json()
        assert body["success"] is False
        assert "required" in body["message"].lower()


class TestBaseAssetAmountNotSupported:
    """base_asset_amount is not yet supported."""

    def test_error_when_base_asset_amount_given(self, client: TestClient, mock_service):
        params = _base_params()
        # Replace quote_asset_amount with base_asset_amount
        del params["quote_asset_amount"]
        params["base_asset_amount"] = "0.5"
        resp = client.get(_ESTIMATE_URL, params=params)
        assert resp.status_code == 400
        body = resp.json()
        assert body["success"] is False
        assert "base_asset_amount not yet supported" in body["message"]
        assert "quote_asset_amount" in body["message"]


class TestServiceErrorMeta:
    """When the service returns an error in meta, it should still be returned."""

    def test_service_error_in_meta_passes_through(self, client: TestClient, mock_service):
        mock_service.estimate.return_value = {
            "reference_price": None,
            "estimated_avg_fill_price": None,
            "slippage_abs": None,
            "slippage_bps": None,
            "filled_qty": Decimal("0"),
            "unfilled_qty": Decimal("1000"),
            "meta": {"error": "no_depth_data"},
        }
        resp = client.get(_ESTIMATE_URL, params=_base_params())
        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        data = body["data"]
        assert data["meta"]["error"] == "no_depth_data"

    def test_service_stale_depth_meta(self, client: TestClient, mock_service):
        mock_service.estimate.return_value = {
            "reference_price": None,
            "estimated_avg_fill_price": None,
            "slippage_abs": None,
            "slippage_bps": None,
            "filled_qty": Decimal("0"),
            "unfilled_qty": Decimal("1000"),
            "meta": {"error": "stale_depth", "age_ms": 120000},
        }
        resp = client.get(_ESTIMATE_URL, params=_base_params())
        assert resp.status_code == 200
        data = resp.json()["data"]
        assert data["meta"]["error"] == "stale_depth"
        assert data["meta"]["age_ms"] == 120000


class TestInvalidQuoteAmount:
    """Invalid quote_asset_amount values."""

    def test_non_numeric_quote_amount(self, client: TestClient, mock_service):
        resp = client.get(
            _ESTIMATE_URL,
            params=_base_params(quote_asset_amount="abc"),
        )
        assert resp.status_code == 400

    def test_zero_quote_amount(self, client: TestClient, mock_service):
        resp = client.get(
            _ESTIMATE_URL,
            params=_base_params(quote_asset_amount="0"),
        )
        assert resp.status_code == 400

    def test_negative_quote_amount(self, client: TestClient, mock_service):
        resp = client.get(
            _ESTIMATE_URL,
            params=_base_params(quote_asset_amount="-100"),
        )
        assert resp.status_code == 400
