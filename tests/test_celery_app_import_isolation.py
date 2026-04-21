from __future__ import annotations

import importlib
import sys
import types
from types import SimpleNamespace

import pytest


class _CeleryStub:
    def __init__(self, name: str, *, broker: str, backend: str):
        self.name = name
        self.broker = broker
        self.backend = backend
        self.conf: dict[str, object] = {}
        self.autodiscovered: list[list[str]] = []

    def autodiscover_tasks(self, packages: list[str]) -> None:
        self.autodiscovered.append(list(packages))



def _purge_celery_app_modules() -> None:
    for name in list(sys.modules):
        if name == "data_platform.workers.celery_app" or name.startswith("data_platform.workers.celery_app."):
            sys.modules.pop(name, None)



def _install_celery_app_stubs(monkeypatch, *, get_settings_impl=None):
    counters = {"settings_calls": 0, "logging_calls": 0}
    settings_value = SimpleNamespace(redis_url="redis://redis:6379/0", log_level="INFO")

    def default_get_settings():
        counters["settings_calls"] += 1
        return settings_value

    def configure_logging(level: str) -> None:
        counters["logging_calls"] += 1
        counters["last_log_level"] = level

    celery_module = types.ModuleType("celery")
    celery_module.Celery = _CeleryStub

    settings_module = types.ModuleType("data_platform.settings")
    settings_module.get_settings = get_settings_impl or default_get_settings

    log_config_module = types.ModuleType("data_platform.log_config")
    log_config_module.configure_logging = configure_logging

    monkeypatch.setitem(sys.modules, "celery", celery_module)
    monkeypatch.setitem(sys.modules, "data_platform.settings", settings_module)
    monkeypatch.setitem(sys.modules, "data_platform.log_config", log_config_module)
    return counters, settings_value



def test_importing_celery_app_does_not_import_optional_dependencies(monkeypatch):
    _purge_celery_app_modules()
    for name in ["celery", "data_platform.settings", "data_platform.log_config"]:
        monkeypatch.delitem(sys.modules, name, raising=False)

    module = importlib.import_module("data_platform.workers.celery_app")

    assert hasattr(module, "create_celery_app")
    assert "celery" not in sys.modules
    assert "data_platform.settings" not in sys.modules
    assert "data_platform.log_config" not in sys.modules



def test_module_celery_app_is_built_lazily_and_cached(monkeypatch):
    _purge_celery_app_modules()
    counters, settings_value = _install_celery_app_stubs(monkeypatch)

    module = importlib.import_module("data_platform.workers.celery_app")
    app_one = getattr(module, "celery_app")
    app_two = getattr(module, "celery_app")

    assert app_one is app_two
    assert app_one.name == "data_platform"
    assert app_one.broker == settings_value.redis_url
    assert app_one.backend == settings_value.redis_url
    assert app_one.conf == {
        "task_track_started": True,
        "worker_prefetch_multiplier": 1,
        "task_acks_late": True,
        "task_serializer": "json",
        "result_serializer": "json",
        "accept_content": ["json"],
    }
    assert app_one.autodiscovered == [["data_platform.workers"]]
    assert counters["settings_calls"] == 1
    assert counters["logging_calls"] == 1





def test_celery_app_raises_clear_error_when_celery_is_unavailable(monkeypatch):
    _purge_celery_app_modules()

    real_import_module = importlib.import_module

    def fake_import_module(name: str, package: str | None = None):
        if name == "data_platform.settings":
            return types.SimpleNamespace(get_settings=lambda: SimpleNamespace(redis_url="redis://redis:6379/0", log_level="INFO"))
        if name == "data_platform.log_config":
            return types.SimpleNamespace(configure_logging=lambda level: None)
        if name == "celery":
            raise ModuleNotFoundError(name)
        return real_import_module(name, package)

    monkeypatch.setattr(importlib, "import_module", fake_import_module)

    module = importlib.import_module("data_platform.workers.celery_app")
    with pytest.raises(RuntimeError, match="requires celery to be installed"):
        _ = getattr(module, "celery_app")

def test_create_celery_app_with_explicit_settings_skips_settings_loader(monkeypatch):
    _purge_celery_app_modules()

    def exploding_get_settings():
        raise AssertionError("get_settings should not be called")

    counters, _ = _install_celery_app_stubs(monkeypatch, get_settings_impl=exploding_get_settings)
    module = importlib.import_module("data_platform.workers.celery_app")

    app = module.create_celery_app(settings=SimpleNamespace(redis_url="redis://example:6379/1", log_level="DEBUG"))

    assert app.broker == "redis://example:6379/1"
    assert app.backend == "redis://example:6379/1"
    assert counters.get("settings_calls", 0) == 0
    assert counters["logging_calls"] == 1
    assert counters["last_log_level"] == "DEBUG"
