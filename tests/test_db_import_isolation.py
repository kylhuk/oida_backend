from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace


def _db_module_path() -> Path:
    return Path(__file__).resolve().parents[1] / "src" / "data_platform" / "db.py"


class _FakeDeclarativeBase:
    pass


class _FakeSession:
    pass


class _FakeSessionMaker:
    @classmethod
    def __class_getitem__(cls, _item):
        return cls

    def __call__(self, **kwargs):
        return ("sessionmaker", kwargs)


class _FakeConnection:
    pass


class _FakeEngine:
    pass


def _install_sqlalchemy_stubs(monkeypatch, *, create_engine_callback):
    sqlalchemy_module = ModuleType("sqlalchemy")
    sqlalchemy_module.create_engine = create_engine_callback

    sqlalchemy_engine_module = ModuleType("sqlalchemy.engine")
    sqlalchemy_engine_module.Connection = _FakeConnection
    sqlalchemy_engine_module.Engine = _FakeEngine

    sqlalchemy_orm_module = ModuleType("sqlalchemy.orm")
    sqlalchemy_orm_module.DeclarativeBase = _FakeDeclarativeBase
    sqlalchemy_orm_module.Session = _FakeSession
    sqlalchemy_orm_module.sessionmaker = _FakeSessionMaker()

    monkeypatch.setitem(sys.modules, "sqlalchemy", sqlalchemy_module)
    monkeypatch.setitem(sys.modules, "sqlalchemy.engine", sqlalchemy_engine_module)
    monkeypatch.setitem(sys.modules, "sqlalchemy.orm", sqlalchemy_orm_module)


def _load_db_module(alias: str):
    spec = importlib.util.spec_from_file_location(alias, _db_module_path())
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[alias] = module
    spec.loader.exec_module(module)
    return module


def test_db_import_does_not_import_settings_or_migrations(monkeypatch):
    _install_sqlalchemy_stubs(
        monkeypatch,
        create_engine_callback=lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("create_engine should not run")),
    )

    imported_names: list[str] = []
    real_import_module = __import__("importlib").import_module

    def tracking_import_module(name: str, package: str | None = None):
        imported_names.append(name)
        return real_import_module(name, package)

    monkeypatch.setattr("importlib.import_module", tracking_import_module)

    module = _load_db_module("test_db_import_isolation_module")

    assert module.get_engine.cache_info().currsize == 0
    assert module.get_settings.cache_info().currsize == 0
    assert module._get_run_migrations.cache_info().currsize == 0
    assert "data_platform.settings" not in imported_names
    assert "data_platform.migrations" not in imported_names


def test_get_engine_imports_settings_only_on_first_use(monkeypatch):
    engine_calls: list[tuple[tuple, dict]] = []

    _install_sqlalchemy_stubs(
        monkeypatch,
        create_engine_callback=lambda *args, **kwargs: engine_calls.append((args, kwargs)) or object(),
    )

    settings_module = ModuleType("data_platform.settings")
    settings_calls: list[str] = []

    def fake_get_settings():
        settings_calls.append("called")
        return SimpleNamespace(sqlalchemy_database_uri="postgresql+psycopg://example")

    settings_module.get_settings = fake_get_settings
    monkeypatch.setitem(sys.modules, "data_platform.settings", settings_module)

    module = _load_db_module("test_db_import_isolation_engine")

    first = module.get_engine()
    second = module.get_engine()

    assert first is second
    assert settings_calls == ["called"]
    assert len(engine_calls) == 1
    assert engine_calls[0][0] == ("postgresql+psycopg://example",)


def test_init_db_with_explicit_bind_imports_migrations_lazily(monkeypatch):
    _install_sqlalchemy_stubs(
        monkeypatch,
        create_engine_callback=lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("default engine should not be created")),
    )

    settings_module = ModuleType("data_platform.settings")
    settings_module.get_settings = lambda: (_ for _ in ()).throw(AssertionError("settings should not load"))
    monkeypatch.setitem(sys.modules, "data_platform.settings", settings_module)

    migrations_calls: list[object] = []
    migrations_module = ModuleType("data_platform.migrations")
    migrations_module.run_migrations = lambda bind: migrations_calls.append(bind)
    monkeypatch.setitem(sys.modules, "data_platform.migrations", migrations_module)

    models_module = ModuleType("data_platform.models")
    models_module.api_client = object()
    models_module.dataset = object()
    models_module.ingestion = object()
    models_module.pipeline = object()
    monkeypatch.setitem(sys.modules, "data_platform.models", models_module)

    module = _load_db_module("test_db_import_isolation_init")
    explicit_bind = object()

    module.init_db(bind=explicit_bind)

    assert migrations_calls == [explicit_bind]
    assert module._get_run_migrations.cache_info().currsize == 1
