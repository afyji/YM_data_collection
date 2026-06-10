"""Tests for quality/cache_checker.py."""

from __future__ import annotations

from dataclasses import asdict
from unittest.mock import MagicMock, patch

import pytest

from YM_data_collection.quality.cache_checker import (
    CacheConsistencyChecker,
    ConsistencyResult,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_cache_client():
    """Create a mock RedisCacheClient."""
    client = MagicMock()
    client.get_json = MagicMock(return_value=None)
    return client


@pytest.fixture
def mock_session_factory():
    """Create a mock sessionmaker."""
    return MagicMock()


@pytest.fixture
def checker(mock_session_factory, mock_cache_client):
    """Create a CacheConsistencyChecker with mock dependencies."""
    return CacheConsistencyChecker(mock_session_factory, mock_cache_client)


# ---------------------------------------------------------------------------
# Consistent data tests
# ---------------------------------------------------------------------------

class TestConsistentData:
    """When cache and MySQL have matching data, the result should be consistent."""

    def test_mark_price_consistent(self, checker, mock_cache_client, mock_session_factory):
        cache_data = {"mark_price": "65000.00", "funding_rate": "0.0001"}
        mysql_data = [{"mark_price": "65000.00", "funding_rate": "0.0001"}]

        mock_cache_client.get_json.return_value = cache_data
        with patch.object(checker._mark_price_repo, "query_latest", return_value=mysql_data):
            result = checker.check_symbol("binance", "perp", "BTCUSDT", "mark_price")

        assert result.cache_exists is True
        assert result.mysql_exists is True
        assert result.consistent is True
        assert result.discrepancies == []
        assert "Consistent" in result.summary

    def test_kline_consistent(self, checker, mock_cache_client, mock_session_factory):
        cache_data = {"close_price": "65000.50", "volume": "1234.56"}
        mysql_data = [{"close_price": "65000.50", "volume": "1234.56"}]

        mock_cache_client.get_json.return_value = cache_data
        with patch.object(checker._kline_repo, "query_latest", return_value=mysql_data):
            result = checker.check_symbol("binance", "spot", "BTCUSDT", "kline")

        assert result.consistent is True
        assert result.discrepancies == []

    def test_index_price_consistent(self, checker, mock_cache_client):
        cache_data = {"index_price": "64999.00"}
        mysql_data = [{"index_price": "64999.00"}]

        mock_cache_client.get_json.return_value = cache_data
        with patch.object(checker._index_price_repo, "query_latest", return_value=mysql_data):
            result = checker.check_symbol("binance", "perp", "BTCUSDT", "index_price")

        assert result.consistent is True

    def test_open_interest_consistent(self, checker, mock_cache_client):
        cache_data = {"open_interest": "50000.00"}
        mysql_data = [{"open_interest": "50000.00"}]

        mock_cache_client.get_json.return_value = cache_data
        with patch.object(checker._open_interest_repo, "query_latest", return_value=mysql_data):
            result = checker.check_symbol("binance", "perp", "BTCUSDT", "open_interest")

        assert result.consistent is True

    def test_funding_rate_consistent(self, checker, mock_cache_client):
        cache_data = {"funding_rate": "0.0001", "mark_price": "65000.00"}
        mysql_data = [{"funding_rate": "0.0001", "mark_price": "65000.00"}]

        mock_cache_client.get_json.return_value = cache_data
        with patch.object(checker._funding_rate_repo, "query_latest", return_value=mysql_data):
            result = checker.check_symbol("binance", "perp", "BTCUSDT", "funding_rate")

        assert result.consistent is True

    def test_depth_snapshot_consistent(self, checker, mock_cache_client):
        cache_data = {"best_bid_price": "64999.00", "best_ask_price": "65001.00", "mid_price": "65000.00"}
        mysql_data = [{"best_bid_price": "64999.00", "best_ask_price": "65001.00", "mid_price": "65000.00"}]

        mock_cache_client.get_json.return_value = cache_data
        with patch.object(checker._depth_repo, "query_latest", return_value=mysql_data):
            result = checker.check_symbol("binance", "perp", "BTCUSDT", "depth_snapshot")

        assert result.consistent is True


# ---------------------------------------------------------------------------
# Cache missing
# ---------------------------------------------------------------------------

class TestCacheMissing:
    """When cache has no data but MySQL does."""

    def test_cache_missing(self, checker, mock_cache_client):
        mock_cache_client.get_json.return_value = None
        mysql_data = [{"mark_price": "65000.00", "funding_rate": "0.0001"}]

        with patch.object(checker._mark_price_repo, "query_latest", return_value=mysql_data):
            result = checker.check_symbol("binance", "perp", "BTCUSDT", "mark_price")

        assert result.cache_exists is False
        assert result.mysql_exists is True
        assert result.consistent is False
        assert len(result.discrepancies) > 0
        assert any("missing from cache" in d for d in result.discrepancies)
        assert "Cache missing" in result.summary


# ---------------------------------------------------------------------------
# MySQL missing
# ---------------------------------------------------------------------------

class TestMySQLMissing:
    """When MySQL has no data but cache does."""

    def test_mysql_missing(self, checker, mock_cache_client):
        cache_data = {"mark_price": "65000.00", "funding_rate": "0.0001"}
        mock_cache_client.get_json.return_value = cache_data

        with patch.object(checker._mark_price_repo, "query_latest", return_value=[]):
            result = checker.check_symbol("binance", "perp", "BTCUSDT", "mark_price")

        assert result.cache_exists is True
        assert result.mysql_exists is False
        assert result.consistent is False
        assert len(result.discrepancies) > 0
        assert any("missing from MySQL" in d for d in result.discrepancies)
        assert "MySQL missing" in result.summary


# ---------------------------------------------------------------------------
# Both missing
# ---------------------------------------------------------------------------

class TestBothMissing:
    """When neither cache nor MySQL has data."""

    def test_both_missing(self, checker, mock_cache_client):
        mock_cache_client.get_json.return_value = None

        with patch.object(checker._mark_price_repo, "query_latest", return_value=[]):
            result = checker.check_symbol("binance", "perp", "BTCUSDT", "mark_price")

        assert result.cache_exists is False
        assert result.mysql_exists is False
        assert result.consistent is True  # consistent: both empty
        assert result.discrepancies == []
        assert "Neither" in result.summary


# ---------------------------------------------------------------------------
# Value mismatch
# ---------------------------------------------------------------------------

class TestValueMismatch:
    """When cache and MySQL both have data but values differ."""

    def test_mark_price_mismatch(self, checker, mock_cache_client):
        cache_data = {"mark_price": "65000.00", "funding_rate": "0.0001"}
        mysql_data = [{"mark_price": "64000.00", "funding_rate": "0.0001"}]

        mock_cache_client.get_json.return_value = cache_data
        with patch.object(checker._mark_price_repo, "query_latest", return_value=mysql_data):
            result = checker.check_symbol("binance", "perp", "BTCUSDT", "mark_price")

        assert result.cache_exists is True
        assert result.mysql_exists is True
        assert result.consistent is False
        assert len(result.discrepancies) > 0
        assert any("mark_price" in d for d in result.discrepancies)
        assert "discrepancy" in result.summary.lower()

    def test_kline_mismatch_multiple_fields(self, checker, mock_cache_client):
        cache_data = {"close_price": "65000.00", "volume": "1000.00"}
        mysql_data = [{"close_price": "64000.00", "volume": "2000.00"}]

        mock_cache_client.get_json.return_value = cache_data
        with patch.object(checker._kline_repo, "query_latest", return_value=mysql_data):
            result = checker.check_symbol("binance", "spot", "BTCUSDT", "kline")

        assert result.consistent is False
        assert len(result.discrepancies) == 2
        assert any("close_price" in d for d in result.discrepancies)
        assert any("volume" in d for d in result.discrepancies)

    def test_one_field_mismatch_only_that_reported(self, checker, mock_cache_client):
        cache_data = {"mark_price": "65000.00", "funding_rate": "0.0001"}
        mysql_data = [{"mark_price": "64000.00", "funding_rate": "0.0001"}]

        mock_cache_client.get_json.return_value = cache_data
        with patch.object(checker._mark_price_repo, "query_latest", return_value=mysql_data):
            result = checker.check_symbol("binance", "perp", "BTCUSDT", "mark_price")

        assert result.consistent is False
        assert len(result.discrepancies) == 1
        assert "mark_price" in result.discrepancies[0]


# ---------------------------------------------------------------------------
# check_all
# ---------------------------------------------------------------------------

class TestCheckAll:
    """check_all should iterate over all symbol x data_type combos."""

    def test_check_all_iterates_correctly(self, checker, mock_cache_client):
        symbols = ["BTCUSDT", "ETHUSDT"]
        data_types = ["mark_price", "kline"]

        # Return consistent data for all combos
        cache_data_mp = {"mark_price": "65000.00", "funding_rate": "0.0001"}
        cache_data_kl = {"close_price": "65000.00", "volume": "1000.00"}

        def cache_side_effect(*parts):
            if "mark_price" in parts:
                return cache_data_mp
            if "kline" in parts:
                return cache_data_kl
            return None

        mock_cache_client.get_json.side_effect = cache_side_effect

        mysql_mp = [{"mark_price": "65000.00", "funding_rate": "0.0001"}]
        mysql_kl = [{"close_price": "65000.00", "volume": "1000.00"}]

        with (
            patch.object(checker._mark_price_repo, "query_latest", return_value=mysql_mp),
            patch.object(checker._kline_repo, "query_latest", return_value=mysql_kl),
        ):
            results = checker.check_all("binance", "perp", symbols, data_types)

        # 2 symbols x 2 data_types = 4 results
        assert len(results) == 4

        # Verify all combos are covered
        combos = {(r.symbol, r.data_type) for r in results}
        expected = {
            ("BTCUSDT", "mark_price"),
            ("BTCUSDT", "kline"),
            ("ETHUSDT", "mark_price"),
            ("ETHUSDT", "kline"),
        }
        assert combos == expected

        # All should be consistent
        assert all(r.consistent for r in results)

    def test_check_all_empty_inputs(self, checker, mock_cache_client):
        results = checker.check_all("binance", "perp", [], [])
        assert results == []

    def test_check_all_mixed_results(self, checker, mock_cache_client):
        symbols = ["BTCUSDT"]
        data_types = ["mark_price", "index_price"]

        cache_data_mp = {"mark_price": "65000.00", "funding_rate": "0.0001"}
        # index_price missing from cache
        def cache_side_effect(*parts):
            if "mark_price" in parts:
                return cache_data_mp
            return None

        mock_cache_client.get_json.side_effect = cache_side_effect

        mysql_mp = [{"mark_price": "65000.00", "funding_rate": "0.0001"}]
        mysql_ip = [{"index_price": "64999.00"}]

        with (
            patch.object(checker._mark_price_repo, "query_latest", return_value=mysql_mp),
            patch.object(checker._index_price_repo, "query_latest", return_value=mysql_ip),
        ):
            results = checker.check_all("binance", "perp", symbols, data_types)

        assert len(results) == 2
        # mark_price: consistent
        mp_result = [r for r in results if r.data_type == "mark_price"][0]
        assert mp_result.consistent is True

        # index_price: cache missing
        ip_result = [r for r in results if r.data_type == "index_price"][0]
        assert ip_result.cache_exists is False
        assert ip_result.consistent is False


# ---------------------------------------------------------------------------
# Numeric tolerance
# ---------------------------------------------------------------------------

class TestNumericTolerance:
    """Float/Decimal comparisons should handle minor precision differences."""

    def test_numeric_close_values_consistent(self, checker, mock_cache_client):
        cache_data = {"mark_price": "65000.00000000001", "funding_rate": "0.0001"}
        mysql_data = [{"mark_price": "65000.00", "funding_rate": "0.0001"}]

        mock_cache_client.get_json.return_value = cache_data
        with patch.object(checker._mark_price_repo, "query_latest", return_value=mysql_data):
            result = checker.check_symbol("binance", "perp", "BTCUSDT", "mark_price")

        assert result.consistent is True


# ---------------------------------------------------------------------------
# Unknown data type
# ---------------------------------------------------------------------------

class TestUnknownDataType:
    """Unknown data_type should return mysql_exists=False."""

    def test_unknown_data_type(self, checker, mock_cache_client):
        cache_data = {"some_field": "some_value"}
        mock_cache_client.get_json.return_value = cache_data

        result = checker.check_symbol("binance", "perp", "BTCUSDT", "unknown_type")

        assert result.cache_exists is True
        assert result.mysql_exists is False
        assert result.consistent is False
