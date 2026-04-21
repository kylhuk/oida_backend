from __future__ import annotations

from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from types import SimpleNamespace

import sqlalchemy as sa
from sqlalchemy.pool import StaticPool


def _load_db_module(alias: str):
    db_path = Path(__file__).resolve().parents[1] / "src" / "data_platform" / "db.py"
    spec = spec_from_file_location(alias, db_path)
    assert spec is not None and spec.loader is not None
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module



def test_db_module_import_is_lazy(monkeypatch):
    calls: list[tuple[tuple, dict]] = []

    def fake_create_engine(*args, **kwargs):
        calls.append((args, kwargs))
        raise AssertionError("create_engine should not run during module import")

    monkeypatch.setattr(sa, "create_engine", fake_create_engine)

    module = _load_db_module("test_lazy_db_module")

    assert calls == []
    assert hasattr(module, "get_engine")
    assert hasattr(module, "get_session_factory")



def test_get_engine_creates_engine_once(monkeypatch):
    import data_platform.db as db

    db.get_engine.cache_clear()
    db.get_session_factory.cache_clear()

    calls: list[tuple[tuple, dict]] = []
    sentinel_engine = object()

    monkeypatch.setattr(
        db,
        "get_settings",
        lambda: SimpleNamespace(sqlalchemy_database_uri="postgresql+psycopg://u:p@localhost:5432/platform"),
    )
    monkeypatch.setattr(
        db.sa,
        "create_engine",
        lambda *args, **kwargs: calls.append((args, kwargs)) or sentinel_engine,
    )

    try:
        assert db.get_engine() is sentinel_engine
        assert db.get_engine() is sentinel_engine
        assert len(calls) == 1
    finally:
        db.get_engine.cache_clear()
        db.get_session_factory.cache_clear()



def test_init_db_with_explicit_bind_skips_default_engine_creation(monkeypatch):
    import data_platform.db as db

    db.get_engine.cache_clear()
    db.get_session_factory.cache_clear()

    explicit_engine = sa.create_engine(
        "sqlite+pysqlite:///:memory:",
        future=True,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    def fail_create_engine(*args, **kwargs):
        raise AssertionError("default engine should not be created when bind is provided")

    monkeypatch.setattr(db.sa, "create_engine", fail_create_engine)

    try:
        db.init_db(bind=explicit_engine)
        with explicit_engine.connect() as conn:
            tables = set(sa.inspect(conn).get_table_names())
        assert {"datasets", "schema_migrations"} <= tables
    finally:
        db.get_engine.cache_clear()
        db.get_session_factory.cache_clear()
