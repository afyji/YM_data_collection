"""Sync instrument metadata from the venue."""

from __future__ import annotations

import argparse
import asyncio
import sys
from decimal import Decimal
from pathlib import Path
from typing import Any

if __package__ is None or __package__ == "":
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from YM_data_collection.apps._cli_common import (
    CliConfigError,
    CliError,
    add_common_arguments,
    emit_final_status,
)
from YM_data_collection.adapters.binance_gateway import BinanceGateway
from YM_data_collection.config.loader import load_config
from YM_data_collection.domain.models import InstrumentInfo
from YM_data_collection.persistence.mysql import create_mysql_engine, create_session_factory
from YM_data_collection.persistence.repositories.instrument_repo import InstrumentRepository
from YM_data_collection.utils.exit_codes import ExitCode
from YM_data_collection.utils.logging_utils import configure_logging, get_logger

APP_NAME = "sync_instruments"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Sync instrument metadata.")
    add_common_arguments(
        parser,
        include_config=True,
        include_env=True,
        include_venue=True,
        include_market_type=True,
        include_symbols=True,
        market_type_choices=("spot", "perp", "all"),
    )
    return parser


# ---------------------------------------------------------------------------
# Binance response parsing helpers
# ---------------------------------------------------------------------------

def _find_filter(symb: dict[str, Any], filter_type: str) -> dict[str, Any]:
    """Return the first filter dict matching *filter_type*, or empty dict."""
    for f in symb.get("filters", []):
        if f.get("filterType") == filter_type:
            return f
    return {}


def _parse_spot_symbol(symb: dict[str, Any], venue: str) -> InstrumentInfo:
    """Build an InstrumentInfo from a Binance spot exchangeInfo symbol entry."""
    price_filter = _find_filter(symb, "PRICE_FILTER")
    lot_filter = _find_filter(symb, "LOT_SIZE")
    min_notional_filter = _find_filter(symb, "MIN_NOTIONAL") or _find_filter(symb, "NOTIONAL")

    tick_size = price_filter.get("tickSize", "0.01")
    step_size = lot_filter.get("stepSize", "0.001")
    min_qty = lot_filter.get("minQty", "0.001")
    min_notional = min_notional_filter.get("minNotional", "0")

    return InstrumentInfo(
        venue=venue,
        market_type="spot",
        symbol=symb["symbol"],
        base_asset=symb["baseAsset"],
        quote_asset=symb["quoteAsset"],
        instrument_code=f"{venue}:spot:{symb['symbol']}",
        is_active=symb.get("status") == "TRADING",
        tick_size=Decimal(tick_size),
        step_size=Decimal(step_size),
        min_qty=Decimal(min_qty),
        min_notional=Decimal(min_notional),
        contract_type=None,
    )


def _parse_perp_symbol(symb: dict[str, Any], venue: str) -> InstrumentInfo:
    """Build an InstrumentInfo from a Binance perp exchangeInfo symbol entry."""
    price_filter = _find_filter(symb, "PRICE_FILTER")
    lot_filter = _find_filter(symb, "LOT_SIZE")
    min_notional_filter = _find_filter(symb, "MIN_NOTIONAL") or _find_filter(symb, "NOTIONAL")

    tick_size = price_filter.get("tickSize", "0.01")
    step_size = lot_filter.get("stepSize", "0.001")
    min_qty = lot_filter.get("minQty", "0.001")
    min_notional = min_notional_filter.get("minNotional", "0")
    contract_type = symb.get("contractType")

    return InstrumentInfo(
        venue=venue,
        market_type="perp",
        symbol=symb["symbol"],
        base_asset=symb["baseAsset"],
        quote_asset=symb["quoteAsset"],
        instrument_code=f"{venue}:perp:{symb['symbol']}",
        is_active=symb.get("status") == "TRADING",
        tick_size=Decimal(tick_size),
        step_size=Decimal(step_size),
        min_qty=Decimal(min_qty),
        min_notional=Decimal(min_notional),
        contract_type=contract_type,
    )


def _parse_symbols(
    response: dict[str, Any],
    market_type: str,
    venue: str,
    allowed_symbols: set[str] | None = None,
) -> list[InstrumentInfo]:
    """Parse exchangeInfo response into InstrumentInfo objects, optionally filtering by symbol."""
    parser = _parse_spot_symbol if market_type == "spot" else _parse_perp_symbol
    results: list[InstrumentInfo] = []
    for symb in response.get("symbols", []):
        symbol_name = symb.get("symbol", "")
        if allowed_symbols is not None and symbol_name not in allowed_symbols:
            continue
        results.append(parser(symb, venue))
    return results


# ---------------------------------------------------------------------------
# Main sync logic
# ---------------------------------------------------------------------------

async def _sync(args: argparse.Namespace) -> int:
    """Core async sync logic. Returns exit code."""
    logger = get_logger(APP_NAME)

    # 1. Load config
    try:
        config = load_config(config_path=args.config, env_name=args.env)
    except Exception as exc:
        raise CliConfigError(f"Failed to load config: {exc}") from exc

    # 2. Determine market types to sync
    market_types: list[str] = []
    mt_filter = args.market_type.lower() if args.market_type else "all"
    if mt_filter in ("spot", "all") and config.binance.spot_enabled:
        market_types.append("spot")
    if mt_filter in ("perp", "all") and config.binance.perp_enabled:
        market_types.append("perp")

    if not market_types:
        raise CliConfigError(
            f"No market types to sync (filter={mt_filter}, "
            f"spot_enabled={config.binance.spot_enabled}, perp_enabled={config.binance.perp_enabled})"
        )

    # 3. Determine symbol filter
    allowed_symbols: set[str] | None = None
    if args.symbols:
        allowed_symbols = set(args.symbols)
    else:
        allowed_symbols = set(config.binance.symbols)

    # 4. Create MySQL engine + session factory + repo
    engine = create_mysql_engine(config.mysql)
    session_factory = create_session_factory(engine)
    repo = InstrumentRepository(session_factory)

    # 5. Create gateway
    gateway = BinanceGateway(config.binance)

    try:
        total_upserted = 0
        summary_parts: list[str] = []

        for mt in market_types:
            logger.info("Fetching exchange info for market_type=%s", mt)
            response = await gateway.fetch_exchange_info(mt)
            instruments = _parse_symbols(response, mt, args.venue, allowed_symbols)

            for inst in instruments:
                repo.upsert(inst)
                total_upserted += 1

            summary_parts.append(f"{mt}={len(instruments)}")
            logger.info("Synced %d instruments for %s", len(instruments), mt)

        msg = f"Synced {total_upserted} instruments ({', '.join(summary_parts)})"
        logger.info(msg)
        return emit_final_status(APP_NAME, ExitCode.SUCCESS, msg)

    except Exception as exc:
        logger.exception("Sync failed: %s", exc)
        raise
    finally:
        await gateway.close()


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    try:
        args = parser.parse_args(argv)
        configure_logging(args.log_level)
        return asyncio.run(_sync(args))
    except CliError as exc:
        return emit_final_status(APP_NAME, exc.exit_code, str(exc))
    except Exception as exc:
        return emit_final_status(APP_NAME, ExitCode.GENERAL_FAILURE, str(exc))


if __name__ == "__main__":
    sys.exit(main())
