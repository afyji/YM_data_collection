"""Repository for the instruments table using raw SQL."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

from YM_data_collection.domain.models import InstrumentInfo
from YM_data_collection.persistence.mysql import session_scope


class InstrumentRepository:
    """CRUD operations on the ``instruments`` table.

    Uses raw SQL via :func:`sqlalchemy.text` – no ORM mapped classes.
    """

    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        self._session_factory = session_factory

    # ------------------------------------------------------------------
    # Upsert
    # ------------------------------------------------------------------

    def upsert(self, instrument: InstrumentInfo) -> None:
        """Insert or update an instrument row (idempotent).

        On MySQL uses ``INSERT … ON DUPLICATE KEY UPDATE``.
        On SQLite uses ``INSERT … ON CONFLICT … DO UPDATE``.
        """
        # Store naive UTC datetime for cross-dialect compatibility
        now = datetime.now(timezone.utc).replace(tzinfo=None)

        with session_scope(self._session_factory) as session:
            bind = session.get_bind()
            dialect = bind.dialect.name

            if dialect == "sqlite":
                sql = text("""
                    INSERT INTO instruments (
                        venue, market_type, symbol, base_asset, quote_asset,
                        instrument_code, is_active, tick_size, step_size,
                        min_qty, min_notional, contract_type,
                        created_at_utc, updated_at_utc
                    ) VALUES (
                        :venue, :market_type, :symbol, :base_asset, :quote_asset,
                        :instrument_code, :is_active, :tick_size, :step_size,
                        :min_qty, :min_notional, :contract_type,
                        :created_at_utc, :updated_at_utc
                    )
                    ON CONFLICT(venue, market_type, symbol) DO UPDATE SET
                        base_asset      = excluded.base_asset,
                        quote_asset     = excluded.quote_asset,
                        instrument_code = excluded.instrument_code,
                        is_active       = excluded.is_active,
                        tick_size       = excluded.tick_size,
                        step_size       = excluded.step_size,
                        min_qty         = excluded.min_qty,
                        min_notional    = excluded.min_notional,
                        contract_type   = excluded.contract_type,
                        updated_at_utc  = excluded.updated_at_utc
                """)
            else:
                # MySQL
                sql = text("""
                    INSERT INTO instruments (
                        venue, market_type, symbol, base_asset, quote_asset,
                        instrument_code, is_active, tick_size, step_size,
                        min_qty, min_notional, contract_type,
                        created_at_utc, updated_at_utc
                    ) VALUES (
                        :venue, :market_type, :symbol, :base_asset, :quote_asset,
                        :instrument_code, :is_active, :tick_size, :step_size,
                        :min_qty, :min_notional, :contract_type,
                        :created_at_utc, :updated_at_utc
                    )
                    ON DUPLICATE KEY UPDATE
                        base_asset      = VALUES(base_asset),
                        quote_asset     = VALUES(quote_asset),
                        instrument_code = VALUES(instrument_code),
                        is_active       = VALUES(is_active),
                        tick_size       = VALUES(tick_size),
                        step_size       = VALUES(step_size),
                        min_qty         = VALUES(min_qty),
                        min_notional    = VALUES(min_notional),
                        contract_type   = VALUES(contract_type),
                        updated_at_utc  = VALUES(updated_at_utc)
                """)

            params = {
                "venue": instrument.venue,
                "market_type": instrument.market_type,
                "symbol": instrument.symbol,
                "base_asset": instrument.base_asset,
                "quote_asset": instrument.quote_asset,
                "instrument_code": instrument.instrument_code,
                "is_active": 1 if instrument.is_active else 0,
                "tick_size": str(instrument.tick_size),
                "step_size": str(instrument.step_size),
                "min_qty": str(instrument.min_qty),
                "min_notional": str(instrument.min_notional),
                "contract_type": instrument.contract_type,
                "created_at_utc": now,
                "updated_at_utc": now,
            }
            session.execute(sql, params)

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def get_by_code(self, instrument_code: str) -> Optional[InstrumentInfo]:
        """Return a single instrument by its ``instrument_code``, or *None*."""
        with session_scope(self._session_factory) as session:
            row = session.execute(
                text("SELECT * FROM instruments WHERE instrument_code = :code"),
                {"code": instrument_code},
            ).mappings().first()

            if row is None:
                return None
            return self._row_to_instrument(row)

    def list_active(self) -> List[InstrumentInfo]:
        """Return all instruments where ``is_active = 1``."""
        with session_scope(self._session_factory) as session:
            rows = session.execute(
                text("SELECT * FROM instruments WHERE is_active = 1")
            ).mappings().all()
            return [self._row_to_instrument(r) for r in rows]

    def list_all(self) -> List[InstrumentInfo]:
        """Return every instrument row."""
        with session_scope(self._session_factory) as session:
            rows = session.execute(
                text("SELECT * FROM instruments")
            ).mappings().all()
            return [self._row_to_instrument(r) for r in rows]

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _row_to_instrument(row) -> InstrumentInfo:
        """Map a DB row mapping to an :class:`InstrumentInfo`."""
        return InstrumentInfo(
            venue=row["venue"],
            market_type=row["market_type"],
            symbol=row["symbol"],
            base_asset=row["base_asset"],
            quote_asset=row["quote_asset"],
            instrument_code=row["instrument_code"],
            is_active=bool(row["is_active"]),
            tick_size=Decimal(str(row["tick_size"])),
            step_size=Decimal(str(row["step_size"])),
            min_qty=Decimal(str(row["min_qty"])),
            min_notional=Decimal(str(row["min_notional"])),
            contract_type=row["contract_type"],
        )
