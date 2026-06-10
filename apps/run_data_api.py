"""Run HTTP and WebSocket API service."""

from __future__ import annotations

import argparse
import sys

from pathlib import Path

if __package__ is None or __package__ == "":
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from YM_data_collection.apps._cli_common import add_common_arguments
from YM_data_collection.config.loader import load_config
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

    # Import here so the module can be imported without fastapi at module level
    import uvicorn

    from YM_data_collection.api.app import create_app

    app = create_app(config)

    logger.info("Starting YM data API on %s:%s", host, port)
    uvicorn.run(app, host=host, port=port, log_level=args.log_level.lower())
    return 0


if __name__ == "__main__":
    sys.exit(main())
