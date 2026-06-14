"""Tests for run_data_api runtime dependency wiring."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from YM_data_collection.apps import run_data_api
from YM_data_collection.services.coverage_service import CoverageService
from YM_data_collection.services.query_service import MarketDataQueryService
from YM_data_collection.services.slippage_service import SlippageService
from YM_data_collection.quality.health_checker import ServiceHealthChecker
from YM_data_collection.tests.test_data_api import _make_config


def test_create_runtime_app_wires_repositories_and_services() -> None:
    config = _make_config(auth_enabled=False)
    engine = MagicMock(name="engine")
    session_factory = MagicMock(name="session_factory")
    cache_client = MagicMock(name="cache_client")
    app = MagicMock(name="app")

    with patch.object(run_data_api, "create_mysql_engine", return_value=engine) as create_engine, \
         patch.object(run_data_api, "create_session_factory", return_value=session_factory) as create_sessions, \
         patch.object(run_data_api, "build_redis_client", return_value=cache_client) as create_cache, \
         patch("YM_data_collection.api.app.create_app", return_value=app) as create_app:
        result = run_data_api.create_runtime_app(config)

    assert result is app
    create_engine.assert_called_once_with(config.mysql)
    create_sessions.assert_called_once_with(engine)
    create_cache.assert_called_once_with(config.cache)

    kwargs = create_app.call_args.kwargs
    assert kwargs["instrument_repo"] is not None
    assert kwargs["quality_repo"] is not None
    assert kwargs["manifest_repo"] is not None
    assert kwargs["checkpoint_repo"] is not None
    assert isinstance(kwargs["query_service"], MarketDataQueryService)
    assert isinstance(kwargs["coverage_service"], CoverageService)
    assert isinstance(kwargs["slippage_service"], SlippageService)
    assert isinstance(kwargs["health_checker"], ServiceHealthChecker)

    repos = kwargs["query_service"]._repos
    assert set(repos) == {
        "kline",
        "funding_rate",
        "open_interest",
        "mark_price",
        "index_price",
        "depth_snapshot",
    }


def test_main_uses_runtime_app_for_uvicorn() -> None:
    config = _make_config(auth_enabled=False)
    app = MagicMock(name="app")

    with patch.object(run_data_api, "load_config", return_value=config), \
         patch.object(run_data_api, "create_runtime_app", return_value=app) as create_runtime_app, \
         patch("uvicorn.run") as run:
        code = run_data_api.main(["--http-host", "127.0.0.1", "--http-port", "19081"])

    assert code == 0
    create_runtime_app.assert_called_once_with(config)
    run.assert_called_once_with(app, host="127.0.0.1", port=19081, log_level="info")
