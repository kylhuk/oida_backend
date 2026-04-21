from __future__ import annotations

from functools import lru_cache
from importlib import import_module

import sqlalchemy as sa
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker


class Base(DeclarativeBase):
    pass


@lru_cache(maxsize=1)
def get_settings():
    settings_module = import_module("data_platform.settings")
    return settings_module.get_settings()


@lru_cache(maxsize=1)
def _get_run_migrations():
    migrations_module = import_module("data_platform.migrations")
    return migrations_module.run_migrations


@lru_cache(maxsize=1)
def get_engine() -> Engine:
    settings = get_settings()
    return sa.create_engine(
        settings.sqlalchemy_database_uri,
        pool_pre_ping=True,
        pool_size=20,
        max_overflow=40,
        future=True,
    )


@lru_cache(maxsize=1)
def get_session_factory() -> sessionmaker[Session]:
    return sessionmaker(bind=get_engine(), autoflush=False, autocommit=False, expire_on_commit=False)


def init_db(bind: Engine | Connection | None = None) -> None:
    from data_platform.models import api_client, audit, dataset, ingestion, pipeline  # noqa: F401

    _get_run_migrations()(bind or get_engine())


def __getattr__(name: str):
    if name == "engine":
        return get_engine()
    if name == "SessionLocal":
        return get_session_factory()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "Base",
    "SessionLocal",
    "engine",
    "get_engine",
    "get_session_factory",
    "get_settings",
    "init_db",
]
