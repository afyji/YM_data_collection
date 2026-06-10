"""Tests for InstrumentRepository using SQLite in-memory."""

from __future__ import annotations

from decimal import Decimal

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from YM_data_collection.domain.models import InstrumentInfo
from YM_data_collection.persistence.repositories.instrument_repo import InstrumentRepository

# ---------------------------------------------------------------------------
# SQLite DDL (adapted from the MySQL schema)
# ---------------------------------------------------------------------------

_CREATE_INSTRUMENTS_TABLE = """
CREATE TABLE IF NOT EXISTS instruments (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    venue           VARCHAR(32)   NOT NULL,
    market_type     VARCHAR(16)   NOT NULL,
    symbol          VARCHAR(64)   NOT NULL,
    base_asset      VARCHAR(32)   NOT NULL,
    quote_asset     VARCHAR(32)   NOT NULL,
    instrument_code VARCHAR(128)  NOT NULL,
    is_active       INTEGER       NOT NULL DEFAULT 1,
    tick_size       DECIMAL(20,8) NOT NULL,
    step_size       DECIMAL(20,8) NOT NULL,
    min_qty         DECIMAL(20,8) NOT NULL,
    min_notional    DECIMAL(24,8) NOT NULL,
    contract_type   VARCHAR(32),
    created_at_utc  DATETIME      NOT NULL,
    updated_at_utc  DATETIME      NOT NULL,
    UNIQUE(venue, market_type, symbol),
    UNIQUE(instrument_code)
);
"""


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def session_factory():
    """Create an in-memory SQLite session factory with the instruments table."""
    engine = create_engine("sqlite:///:memory:")
    with engine.connect() as conn:
        conn.execute(text(_CREATE_INSTRUMENTS_TABLE))
        conn.commit()
    factory = sessionmaker(bind=engine, autoflush=False, autocommit=False,
                           expire_on_commit=False)
    return factory


@pytest.fixture()
def repo(session_factory):
    return InstrumentRepository(session_factory)


def _make_instrument(**overrides) -> InstrumentInfo:
    defaults = dict(
        venue="binance",
        market_type="spot",
        symbol="BTCUSDT",
        base_asset="BTC",
        quote_asset="USDT",
        instrument_code="binance-spot-BTCUSDT",
        is_active=True,
        tick_size=Decimal("0.01000000"),
        step_size=Decimal("0.00001000"),
        min_qty=Decimal("0.00001000"),
        min_notional=Decimal("10.00000000"),
        contract_type=None,
    )
    defaults.update(overrides)
    return InstrumentInfo(**defaults)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestInstrumentRepository:
    """InstrumentRepository test suite."""

    def test_upsert_inserts_new_row(self, repo: InstrumentRepository, session_factory):
        inst = _make_instrument()
        repo.upsert(inst)

        with session_factory() as session:
            count = session.execute(text("SELECT COUNT(*) FROM instruments")).scalar()
        assert count == 1

    def test_upsert_is_idempotent(self, repo: InstrumentRepository, session_factory):
        """Inserting the same instrument twice should result in exactly one row."""
        inst = _make_instrument()
        repo.upsert(inst)
        repo.upsert(inst)

        with session_factory() as session:
            count = session.execute(text("SELECT COUNT(*) FROM instruments")).scalar()
        assert count == 1

    def test_upsert_updates_existing(self, repo: InstrumentRepository, session_factory):
        """Second upsert should update mutable fields."""
        inst = _make_instrument()
        repo.upsert(inst)

        updated = _make_instrument(tick_size=Decimal("0.00100000"), is_active=False)
        repo.upsert(updated)

        result = repo.get_by_code("binance-spot-BTCUSDT")
        assert result is not None
        assert result.tick_size == Decimal("0.00100000")
        assert result.is_active is False

    def test_get_by_code_found(self, repo: InstrumentRepository):
        inst = _make_instrument()
        repo.upsert(inst)

        result = repo.get_by_code("binance-spot-BTCUSDT")
        assert result is not None
        assert result.symbol == "BTCUSDT"
        assert result.venue == "binance"

    def test_get_by_code_not_found(self, repo: InstrumentRepository):
        result = repo.get_by_code("nonexistent-code")
        assert result is None

    def test_list_active(self, repo: InstrumentRepository):
        repo.upsert(_make_instrument(instrument_code="a1", symbol="BTCUSDT", is_active=True))
        repo.upsert(_make_instrument(instrument_code="a2", symbol="ETHUSDT", is_active=False))

        active = repo.list_active()
        assert len(active) == 1
        assert active[0].symbol == "BTCUSDT"

    def test_list_all(self, repo: InstrumentRepository):
        repo.upsert(_make_instrument(instrument_code="a1", symbol="BTCUSDT"))
        repo.upsert(_make_instrument(instrument_code="a2", symbol="ETHUSDT", is_active=False))

        all_inst = repo.list_all()
        assert len(all_inst) == 2

    def test_upsert_different_venues_same_symbol(self, repo: InstrumentRepository):
        """Same symbol on different venues/market_types should be separate rows."""
        repo.upsert(_make_instrument(venue="binance", market_type="spot",
                                      instrument_code="binance-spot-BTCUSDT"))
        repo.upsert(_make_instrument(venue="binance", market_type="perp",
                                      instrument_code="binance-perp-BTCUSDT"))

        all_inst = repo.list_all()
        assert len(all_inst) == 2

    def test_contract_type_nullable(self, repo: InstrumentRepository):
        inst = _make_instrument(contract_type=None)
        repo.upsert(inst)

        result = repo.get_by_code("binance-spot-BTCUSDT")
        assert result is not None
        assert result.contract_type is None

    def test_contract_type_set(self, repo: InstrumentRepository):
        inst = _make_instrument(contract_type="perpetual")
        repo.upsert(inst)

        result = repo.get_by_code("binance-spot-BTCUSDT")
        assert result is not None
        assert result.contract_type == "perpetual"
