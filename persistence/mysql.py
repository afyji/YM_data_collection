"""MySQL engine and session helpers."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator
from urllib.parse import quote_plus

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from YM_data_collection.config.loader import resolve_secret
from YM_data_collection.config.models import MySQLConfig


def build_mysql_url(mysql_config: MySQLConfig, environ: dict[str, str] | None = None, *, masked: bool = False) -> str:
    """Build a SQLAlchemy MySQL URL from config.

    When masked=True the password is replaced with '***' for safe logging.
    """

    password = quote_plus(resolve_secret(mysql_config.password_secret_ref, environ))
    username = quote_plus(mysql_config.username)
    pw_display = "***" if masked else password
    return (
        f"mysql+pymysql://{username}:{pw_display}@{mysql_config.host}:{mysql_config.port}/"
        f"{mysql_config.database}"
    )


def create_mysql_engine(mysql_config: MySQLConfig, environ: dict[str, str] | None = None) -> Engine:
    """Create the SQLAlchemy engine."""

    return create_engine(
        build_mysql_url(mysql_config, environ, masked=False),
        pool_pre_ping=True,
        pool_size=mysql_config.pool_size,
        max_overflow=mysql_config.max_overflow,
        connect_args={
            "connect_timeout": mysql_config.connect_timeout_seconds,
            "read_timeout": mysql_config.read_timeout_seconds,
            "write_timeout": mysql_config.write_timeout_seconds,
        },
        future=True,
    )


def create_session_factory(engine: Engine) -> sessionmaker[Session]:
    """Create the shared session factory."""

    return sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False, future=True)


@contextmanager
def session_scope(session_factory: sessionmaker[Session]) -> Iterator[Session]:
    """Transactional session scope."""

    session = session_factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def ping_mysql(engine: Engine) -> bool:
    """Ping the target database."""

    with engine.connect() as connection:
        connection.execute(text("SELECT 1"))
    return True
