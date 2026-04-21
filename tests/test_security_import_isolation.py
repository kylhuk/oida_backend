from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace


def _security_module_path() -> Path:
    return Path(__file__).resolve().parents[1] / "src" / "data_platform" / "security.py"


def _load_security_module(alias: str):
    spec = importlib.util.spec_from_file_location(alias, _security_module_path())
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[alias] = module
    spec.loader.exec_module(module)
    return module


class _FieldProbe:
    def __init__(self, name: str) -> None:
        self.name = name

    def __eq__(self, other):
        return ("eq", self.name, other)

    def is_(self, other):
        return ("is", self.name, other)


class _SelectQuery:
    def __init__(self, model) -> None:
        self.model = model

    def where(self, *conditions):
        return {"model": self.model, "conditions": conditions}


class _FakeSession:
    def __init__(self, scalar_result=None) -> None:
        self.scalar_result = scalar_result
        self.scalar_queries = []
        self.added = []
        self.commit_calls = 0

    def scalar(self, query):
        self.scalar_queries.append(query)
        return self.scalar_result

    def add(self, item) -> None:
        self.added.append(item)

    def commit(self) -> None:
        self.commit_calls += 1


def test_importing_security_module_does_not_require_sqlalchemy_or_settings(monkeypatch) -> None:
    for name in [
        "sqlalchemy",
        "sqlalchemy.orm",
        "data_platform.models.api_client",
        "data_platform.settings",
    ]:
        monkeypatch.delitem(sys.modules, name, raising=False)

    module = _load_security_module("test_security_import_isolation_module")

    assert module.hash_api_key("dev-local-key") == module.hash_api_key("dev-local-key")
    assert module.key_prefix("abcdefghijk") == "abcdefgh"
    assert "sqlalchemy" not in sys.modules
    assert "data_platform.models.api_client" not in sys.modules
    assert "data_platform.settings" not in sys.modules



def test_authenticate_api_key_imports_sqlalchemy_and_model_lazily(monkeypatch) -> None:
    for name in ["sqlalchemy", "data_platform.models.api_client", "data_platform.settings"]:
        monkeypatch.delitem(sys.modules, name, raising=False)

    module = _load_security_module("test_security_import_isolation_auth")

    select_calls: list[object] = []
    sqlalchemy_module = ModuleType("sqlalchemy")
    sqlalchemy_module.select = lambda model: select_calls.append(model) or _SelectQuery(model)

    api_client_module = ModuleType("data_platform.models.api_client")

    class FakeApiClient:
        key_hash = _FieldProbe("key_hash")
        active = _FieldProbe("active")

    api_client_module.ApiClient = FakeApiClient

    monkeypatch.setitem(sys.modules, "sqlalchemy", sqlalchemy_module)
    monkeypatch.setitem(sys.modules, "data_platform.models.api_client", api_client_module)

    session = _FakeSession(scalar_result="client-row")
    result = module.authenticate_api_key(session, "dev-local-key")

    assert result == "client-row"
    assert select_calls == [FakeApiClient]
    assert session.scalar_queries == [
        {
            "model": FakeApiClient,
            "conditions": (
                ("eq", "key_hash", module.hash_api_key("dev-local-key")),
                ("is", "active", True),
            ),
        }
    ]



def test_ensure_seeded_api_key_imports_settings_lazily(monkeypatch) -> None:
    for name in ["sqlalchemy", "data_platform.models.api_client", "data_platform.settings"]:
        monkeypatch.delitem(sys.modules, name, raising=False)

    module = _load_security_module("test_security_import_isolation_seed")

    sqlalchemy_module = ModuleType("sqlalchemy")
    sqlalchemy_module.select = lambda model: _SelectQuery(model)

    api_client_module = ModuleType("data_platform.models.api_client")

    class FakeApiClient:
        key_hash = _FieldProbe("key_hash")
        active = _FieldProbe("active")

        def __init__(self, **kwargs) -> None:
            self.__dict__.update(kwargs)

    api_client_module.ApiClient = FakeApiClient

    settings_calls: list[str] = []
    settings_module = ModuleType("data_platform.settings")
    settings_module.get_settings = lambda: settings_calls.append("called") or SimpleNamespace(seed_dev_api_key="dev-local-key")

    monkeypatch.setitem(sys.modules, "sqlalchemy", sqlalchemy_module)
    monkeypatch.setitem(sys.modules, "data_platform.models.api_client", api_client_module)
    monkeypatch.setitem(sys.modules, "data_platform.settings", settings_module)

    existing = SimpleNamespace(
        scopes=["datasets:read"],
        active=False,
        key_prefix="legacy",
        key_hash=module.hash_api_key("dev-local-key"),
    )
    session = _FakeSession(scalar_result=existing)

    module.ensure_seeded_api_key(session)

    assert settings_calls == ["called"]
    assert existing.active is True
    assert existing.key_prefix == "dev-loca"
    assert set(module.DEFAULT_SEEDED_SCOPES).issubset(set(existing.scopes))
    assert session.added == [existing]
    assert session.commit_calls == 1
