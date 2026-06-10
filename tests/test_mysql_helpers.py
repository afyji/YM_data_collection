"""Tests for MySQL helper functions."""

from YM_data_collection.config.models import MySQLConfig
from YM_data_collection.persistence.mysql import build_mysql_url


def _make_config() -> MySQLConfig:
    return MySQLConfig(
        host="127.0.0.1",
        port=3306,
        database="quant_data_dev",
        username="quant_user",
        password_secret_ref="MYSQL_PASSWORD",
        pool_size=5,
        max_overflow=10,
        connect_timeout_seconds=5,
        read_timeout_seconds=10,
        write_timeout_seconds=10,
    )


def test_build_mysql_url_masked() -> None:
    config = _make_config()
    url = build_mysql_url(config, {"MYSQL_PASSWORD": "p@ss word"}, masked=True)
    assert "quant_user" in url
    assert "quant_data_dev" in url
    assert "p%40ss" not in url  # password should be masked


def test_build_mysql_url_real_password() -> None:
    config = _make_config()
    url = build_mysql_url(config, {"MYSQL_PASSWORD": "p@ss word"}, masked=False)
    assert "quant_user" in url
    assert "p%40ss" in url  # password should be url-encoded and present


def test_build_mysql_url_default_not_masked() -> None:
    config = _make_config()
    url = build_mysql_url(config, {"MYSQL_PASSWORD": "secret123"})
    assert "secret123" in url
