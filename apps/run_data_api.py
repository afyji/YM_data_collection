"""Run HTTP and WebSocket API service."""

from __future__ import annotations

import argparse
import sys

from pathlib import Path

if __package__ is None or __package__ == "":
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from YM_data_collection.apps._cli_common import add_common_arguments
from YM_data_collection.cache.redis_client import build_redis_client
from YM_data_collection.config.loader import load_config
from YM_data_collection.config.models import DataCollectionConfig
from YM_data_collection.persistence.mysql import (
    create_mysql_engine,
    create_session_factory,
)
from YM_data_collection.persistence.repositories.checkpoint_repo import CheckpointRepository
from YM_data_collection.persistence.repositories.instrument_repo import InstrumentRepository
from YM_data_collection.persistence.repositories.manifest_repo import ManifestRepository
from YM_data_collection.persistence.repositories.marketdata_repo import (
    DepthSnapshotRepository,
    FundingRateRepository,
    IndexPriceRepository,
    KlineRepository,
    MarkPriceRepository,
    OpenInterestRepository,
)
from YM_data_collection.persistence.repositories.quality_repo import QualityIssueRepository
from YM_data_collection.quality.health_checker import ServiceHealthChecker
from YM_data_collection.services.coverage_service import CoverageService
from YM_data_collection.services.query_service import MarketDataQueryService
from YM_data_collection.services.slippage_service import SlippageService
from YM_data_collection.utils.logging_utils import configure_logging, get_logger

APP_NAME = "run_data_api"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the data API service.")
    add_common_arguments(parser, include_config=True, include_env=True)
    parser.add_argument("--http-host", default=None, help="HTTP bind host (overrides config).")
    parser.add_argument("--http-port", type=int, default=None, help="HTTP bind port (overrides config).")
    parser.add_argument("--ws-host", default="127.0.0.1", help="WebSocket bind host.")
    parser.add_argument("--ws-port", type=int, default=8001, help="WebSocket bind port.")
    return parser


def create_runtime_app(config: DataCollectionConfig):
    """Create the FastAPI app with concrete runtime dependencies wired in."""

    engine = create_mysql_engine(config.mysql)
    session_factory = create_session_factory(engine)
    cache_client = build_redis_client(config.cache)

    checkpoint_repo = CheckpointRepository(session_factory)
    instrument_repo = InstrumentRepository(session_factory)
    manifest_repo = ManifestRepository(session_factory)
    quality_repo = QualityIssueRepository(session_factory)

    repos = {
        "kline": KlineRepository(),
        "funding_rate": FundingRateRepository(),
        "open_interest": OpenInterestRepository(),
        "mark_price": MarkPriceRepository(),
        "index_price": IndexPriceRepository(),
        "depth_snapshot": DepthSnapshotRepository(),
    }

    query_service = MarketDataQueryService(
        session_factory=session_factory,
        cache_client=cache_client,
        query_source_config=config.query_source,
        repos=repos,
    )
    coverage_service = CoverageService(
        session_factory=session_factory,
        checkpoint_repo=checkpoint_repo,
    )
    slippage_service = SlippageService(
        cache_client=cache_client,
        slippage_config=config.slippage,
        depth_config=config.depth,
    )
    health_checker = ServiceHealthChecker(
        session_factory=session_factory,
        cache_client=cache_client,
    )

    from YM_data_collection.api.app import create_app

    return create_app(
        config,
        query_service=query_service,
        slippage_service=slippage_service,
        coverage_service=coverage_service,
        instrument_repo=instrument_repo,
        quality_repo=quality_repo,
        manifest_repo=manifest_repo,
        checkpoint_repo=checkpoint_repo,
        health_checker=health_checker,
    )


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    configure_logging(args.log_level)
    logger = get_logger(APP_NAME)

    config = load_config(config_path=args.config, env_name=args.env)

    # CLI overrides for host/port
    host = args.http_host or config.service.http_host
    port = args.http_port or config.service.http_port

    if not config.service.http_enabled:
        logger.warning("HTTP service is disabled in config; nothing to start.")
        return 0

    # Import here so the module can be imported without uvicorn at module level
    import uvicorn

    app = create_runtime_app(config)

    logger.info("Starting YM data API on %s:%s", host, port)
    uvicorn.run(app, host=host, port=port, log_level=args.log_level.lower())
    return 0


if __name__ == "__main__":
    sys.exit(main())
