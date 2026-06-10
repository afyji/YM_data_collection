"""Run service health checks."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

if __package__ is None or __package__ == "":
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from YM_data_collection.apps._cli_common import (
    CliConfigError,
    add_common_arguments,
    configure_logging,
    emit_final_status,
    get_logger,
)
from YM_data_collection.utils.exit_codes import ExitCode

APP_NAME = "run_service_health_check"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run service health checks.")
    add_common_arguments(parser, include_config=True, include_env=True)
    parser.add_argument("--http-url", default=None, help="HTTP API base URL (skip HTTP check if omitted).")
    parser.add_argument("--ws-url", default=None, help="WebSocket URL (informational, not checked yet).")
    parser.add_argument(
        "--max-data-age-seconds",
        type=int,
        default=600,
        help="Maximum acceptable age (seconds) for latest kline data.",
    )
    return parser


def _format_table(health) -> str:
    """Format SystemHealth as a human-readable table."""
    lines = []
    lines.append(f"{'Component':<18} {'Healthy':<9} {'Latency':<12} {'Detail'}")
    lines.append("-" * 72)
    for s in health.statuses:
        healthy_str = "OK" if s.healthy else "FAIL"
        latency_str = f"{s.latency_ms:.1f}ms" if s.latency_ms is not None else "n/a"
        detail = s.detail
        if s.error:
            detail += f" [{s.error}]"
        lines.append(f"{s.component:<18} {healthy_str:<9} {latency_str:<12} {detail}")
    lines.append("-" * 72)
    overall = "HEALTHY" if health.overall_healthy else "UNHEALTHY"
    lines.append(f"Overall: {overall}  (checked at {health.checked_at_utc})")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    configure_logging(args.log_level)
    logger = get_logger(APP_NAME)

    # --- Load config ---
    try:
        from YM_data_collection.config.loader import load_config
        config = load_config(config_path=args.config, env_name=args.env)
    except Exception as exc:
        logger.error("Failed to load config: %s", exc)
        return emit_final_status(APP_NAME, ExitCode.CONFIG_ERROR, str(exc))

    # --- Build dependencies ---
    session_factory = None
    cache_client = None

    # MySQL
    try:
        from YM_data_collection.persistence.mysql import create_mysql_engine, create_session_factory
        engine = create_mysql_engine(config.mysql)
        session_factory = create_session_factory(engine)
        logger.info("MySQL session factory created")
    except Exception as exc:
        logger.warning("Could not create MySQL session factory: %s", exc)

    # Redis
    try:
        from YM_data_collection.cache.redis_client import build_redis_client
        cache_client = build_redis_client(config.cache)
        logger.info("Redis cache client created")
    except Exception as exc:
        logger.warning("Could not create Redis cache client: %s", exc)

    # --- Run health checks ---
    from YM_data_collection.quality.health_checker import ServiceHealthChecker
    checker = ServiceHealthChecker(session_factory=session_factory, cache_client=cache_client)
    health = checker.run_all(http_url=args.http_url, max_data_age_seconds=args.max_data_age_seconds)

    # --- Report ---
    table = _format_table(health)
    print(table)

    if health.overall_healthy:
        return emit_final_status(APP_NAME, ExitCode.SUCCESS, "All health checks passed")
    else:
        failed = [s.component for s in health.statuses if not s.healthy]
        return emit_final_status(
            APP_NAME,
            ExitCode.DEPENDENCY_ERROR,
            f"Unhealthy components: {', '.join(failed)}",
        )


if __name__ == "__main__":
    sys.exit(main())
