"""Slippage estimation service.

Walks the order book (latest depth snapshot from Redis) and estimates the
fill price, slippage, and unfilled quantity for a given trade side and
quote amount.
"""

from __future__ import annotations

import logging
import time
from decimal import Decimal
from typing import Any

from YM_data_collection.cache.redis_client import RedisCacheClient
from YM_data_collection.config.models import DepthConfig, SlippageConfig

logger = logging.getLogger(__name__)


class SlippageService:
    """Estimate slippage from the latest depth snapshot in Redis."""

    def __init__(
        self,
        cache_client: RedisCacheClient,
        slippage_config: SlippageConfig,
        depth_config: DepthConfig,
    ) -> None:
        self._cache = cache_client
        self._slippage_cfg = slippage_config
        self._depth_cfg = depth_config

    def estimate(
        self,
        market_type: str,
        symbol: str,
        side: str,
        amount_quote: Decimal,
    ) -> dict[str, Any]:
        """Estimate slippage for a trade.

        Parameters
        ----------
        market_type : str
            Market type (e.g. "spot", "perp") used as cache key part.
        symbol : str
            Trading symbol, e.g. "BTCUSDT".
        side : str
            "buy" or "sell".
        amount_quote : Decimal
            Notional amount in quote currency to fill.

        Returns
        -------
        dict with keys: reference_price, estimated_avg_fill_price,
        slippage_abs, slippage_bps, filled_qty, unfilled_qty, meta.
        """
        if not self._slippage_cfg.slippage_estimation_enabled:
            return {
                "reference_price": None,
                "estimated_avg_fill_price": None,
                "slippage_abs": None,
                "slippage_bps": None,
                "filled_qty": Decimal("0"),
                "unfilled_qty": amount_quote,
                "meta": {"error": "slippage_estimation_disabled"},
            }

        # Read latest depth from cache
        data_type = "depth_snapshot"
        depth_data = self._cache.get_json(data_type, market_type, symbol)
        if depth_data is None:
            return {
                "reference_price": None,
                "estimated_avg_fill_price": None,
                "slippage_abs": None,
                "slippage_bps": None,
                "filled_qty": Decimal("0"),
                "unfilled_qty": amount_quote,
                "meta": {"error": "no_depth_data"},
            }

        # Freshness check
        now_ms = int(time.time() * 1000)
        event_ts = depth_data.get("event_ts_ms", 0)
        age_ms = now_ms - event_ts
        if age_ms > self._depth_cfg.max_snapshot_age_ms:
            return {
                "reference_price": None,
                "estimated_avg_fill_price": None,
                "slippage_abs": None,
                "slippage_bps": None,
                "filled_qty": Decimal("0"),
                "unfilled_qty": amount_quote,
                "meta": {"error": "stale_depth", "age_ms": age_ms},
            }

        # Build order book levels
        if side == "buy":
            levels = depth_data.get("ask_depth_json", [])
        else:
            levels = depth_data.get("bid_depth_json", [])

        if not levels:
            return {
                "reference_price": None,
                "estimated_avg_fill_price": None,
                "slippage_abs": None,
                "slippage_bps": None,
                "filled_qty": Decimal("0"),
                "unfilled_qty": amount_quote,
                "meta": {"error": "empty_book"},
            }

        # Determine reference price
        reference_price = self._compute_reference_price(depth_data, side)
        if reference_price is None:
            return {
                "reference_price": None,
                "estimated_avg_fill_price": None,
                "slippage_abs": None,
                "slippage_bps": None,
                "filled_qty": Decimal("0"),
                "unfilled_qty": amount_quote,
                "meta": {"error": "no_reference_price"},
            }

        # Walk the book
        remaining_quote = amount_quote
        total_base = Decimal("0")
        total_quote = Decimal("0")
        filled_levels = 0

        for level in levels:
            if remaining_quote <= 0:
                break
            price = Decimal(str(level[0]))
            qty = Decimal(str(level[1]))
            level_quote = price * qty

            if level_quote <= remaining_quote:
                total_base += qty
                total_quote += level_quote
                remaining_quote -= level_quote
                filled_levels += 1
            else:
                # Partial fill at this level
                fill_base = remaining_quote / price
                total_base += fill_base
                total_quote += remaining_quote
                remaining_quote = Decimal("0")
                filled_levels += 1

        filled_qty = total_base
        filled_quote = total_quote
        unfilled_qty = remaining_quote

        # Check insufficient depth
        if unfilled_qty > 0:
            if self._slippage_cfg.insufficient_depth_policy == "reject":
                return {
                    "reference_price": reference_price,
                    "estimated_avg_fill_price": None,
                    "slippage_abs": None,
                    "slippage_bps": None,
                    "filled_qty": Decimal("0"),
                    "unfilled_qty": amount_quote,
                    "meta": {"error": "insufficient_depth", "available_quote": filled_quote},
                }
            # allow_partial_fill
            if not self._slippage_cfg.allow_partial_fill_estimation:
                return {
                    "reference_price": reference_price,
                    "estimated_avg_fill_price": None,
                    "slippage_abs": None,
                    "slippage_bps": None,
                    "filled_qty": Decimal("0"),
                    "unfilled_qty": amount_quote,
                    "meta": {"error": "insufficient_depth", "available_quote": filled_quote},
                }

        # Calculate results
        avg_fill_price = filled_quote / filled_qty if filled_qty > 0 else Decimal("0")
        slippage_abs = avg_fill_price - reference_price
        if side == "sell":
            slippage_abs = reference_price - avg_fill_price

        slippage_bps = (
            (slippage_abs / reference_price * Decimal("10000")).quantize(Decimal("0.01"))
            if reference_price != 0
            else Decimal("0")
        )

        return {
            "reference_price": reference_price,
            "estimated_avg_fill_price": avg_fill_price,
            "slippage_abs": slippage_abs,
            "slippage_bps": slippage_bps,
            "filled_qty": filled_qty,
            "unfilled_qty": unfilled_qty,
            "meta": {"filled_levels": filled_levels, "age_ms": age_ms},
        }

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _compute_reference_price(
        self, depth_data: dict[str, Any], side: str
    ) -> Decimal | None:
        """Compute the reference price based on config mode."""
        mode = self._slippage_cfg.reference_price_mode

        if mode == "best_bid_ask":
            if side == "buy":
                val = depth_data.get("best_ask_price")
            else:
                val = depth_data.get("best_bid_price")
            if val is not None:
                return Decimal(str(val))
            return None

        if mode == "mid_price":
            val = depth_data.get("mid_price")
            if val is not None:
                return Decimal(str(val))
            return None

        return None
