from __future__ import annotations

import importlib
import sys
import types
from types import SimpleNamespace

from data_platform.audit_trail import AuditTrailMiddleware
from data_platform.rate_limit import RateLimitMiddleware
from data_platform.request_context import RequestContextMiddleware


def _purge_api_main_modules() -> None:
    for name in list(sys.modules):
        if name == "data_platform.api.main" or name.startswith("data_platform.api.main."):
            sys.modules.pop(name, None)


class _FastAPIStub:
    def __init__(self, *, title: str, version: str, lifespan):
        self.title = title
        self.version = version
        self.lifespan = lifespan
        self.middlewares = []
        self.routers = []
        self.routes = {}

    def add_middleware(self, middleware_cls, **kwargs):
        self.middlewares.append((middleware_cls, kwargs))

    def include_router(self, router):
        self.routers.append(router)

    def get(self, path: str):
        def decorator(func):
            self.routes[path] = func
            return func

        return decorator


def _install_api_main_stubs(monkeypatch, *, get_settings_impl=None):
    counters = {"settings_calls": 0, "logging_calls": 0}
    settings_value = SimpleNamespace(
        app_name="medallion-backend",
        log_level="INFO",
        cors_allowed_origins=["http://localhost:3000"],
        enable_rate_limit=False,
        enable_audit_trail=True,
        audit_trail_exempt_path_prefixes=["/health", "/docs", "/openapi.json", "/redoc"],
        rate_limit_requests=120,
        rate_limit_window_seconds=60,
        rate_limit_exempt_path_prefixes=["/health", "/docs", "/openapi.json", "/redoc"],
    )

    def default_get_settings():
        counters["settings_calls"] += 1
        return settings_value

    def configure_logging(level: str):
        counters["logging_calls"] += 1
        counters["last_log_level"] = level

    fastapi_module = types.ModuleType("fastapi")
    fastapi_module.FastAPI = _FastAPIStub
    cors_module = types.ModuleType("fastapi.middleware.cors")
    cors_module.CORSMiddleware = object

    settings_module = types.ModuleType("data_platform.settings")
    settings_module.get_settings = get_settings_impl or default_get_settings

    log_config_module = types.ModuleType("data_platform.log_config")
    log_config_module.configure_logging = configure_logging

    db_module = types.ModuleType("data_platform.db")
    db_module.init_db = lambda: None
    db_module.get_session_factory = lambda: (lambda: SimpleNamespace(__enter__=lambda self: self, __exit__=lambda self, exc_type, exc, tb: False))

    class _SessionContext:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    db_module.get_session_factory = lambda: _SessionContext

    security_module = types.ModuleType("data_platform.security")
    security_module.ensure_seeded_api_key = lambda session: None

    for module_name, router_name in [
        ("data_platform.api.routes.audit", "audit"),
        ("data_platform.api.routes.catalog", "catalog"),
        ("data_platform.api.routes.datasets", "datasets"),
        ("data_platform.api.routes.observability", "observability"),
        ("data_platform.api.routes.gold", "gold"),
        ("data_platform.api.routes.health", "health"),
        ("data_platform.api.routes.ingestions", "ingestions"),
        ("data_platform.api.routes.pipelines", "pipelines"),
    ]:
        router_module = types.ModuleType(module_name)
        router_module.router = router_name
        monkeypatch.setitem(sys.modules, module_name, router_module)

    monkeypatch.setitem(sys.modules, "fastapi", fastapi_module)
    monkeypatch.setitem(sys.modules, "fastapi.middleware.cors", cors_module)
    monkeypatch.setitem(sys.modules, "data_platform.settings", settings_module)
    monkeypatch.setitem(sys.modules, "data_platform.log_config", log_config_module)
    monkeypatch.setitem(sys.modules, "data_platform.db", db_module)
    monkeypatch.setitem(sys.modules, "data_platform.security", security_module)

    return counters, settings_value


def test_importing_api_main_does_not_import_optional_dependencies(monkeypatch):
    _purge_api_main_modules()
    for name in [
        "fastapi",
        "fastapi.middleware.cors",
        "data_platform.settings",
        "data_platform.log_config",
        "data_platform.db",
        "data_platform.security",
        "data_platform.api.routes.audit",
        "data_platform.api.routes.catalog",
        "data_platform.api.routes.datasets",
        "data_platform.api.routes.observability",
        "data_platform.api.routes.gold",
        "data_platform.api.routes.health",
        "data_platform.api.routes.ingestions",
        "data_platform.api.routes.pipelines",
    ]:
        monkeypatch.delitem(sys.modules, name, raising=False)

    module = importlib.import_module("data_platform.api.main")

    assert hasattr(module, "create_app")
    assert "fastapi" not in sys.modules
    assert "data_platform.settings" not in sys.modules
    assert "data_platform.api.routes.datasets" not in sys.modules


def test_module_app_is_built_lazily_and_cached(monkeypatch):
    _purge_api_main_modules()
    counters, settings_value = _install_api_main_stubs(monkeypatch)

    module = importlib.import_module("data_platform.api.main")
    app_one = getattr(module, "app")
    app_two = getattr(module, "app")

    assert app_one is app_two
    assert app_one.title == settings_value.app_name
    assert counters["settings_calls"] == 1
    assert counters["logging_calls"] == 1
    assert app_one.routers == ["health", "audit", "observability", "catalog", "datasets", "ingestions", "gold", "pipelines"]
    assert app_one.middlewares[0] == (
        object,
        {
            "allow_origins": settings_value.cors_allowed_origins,
            "allow_credentials": True,
            "allow_methods": ["*"],
            "allow_headers": ["*"],
        },
    )
    assert app_one.middlewares[1] == (
        RateLimitMiddleware,
        {
            "enabled": settings_value.enable_rate_limit,
            "limit": settings_value.rate_limit_requests,
            "window_seconds": settings_value.rate_limit_window_seconds,
            "exempt_path_prefixes": settings_value.rate_limit_exempt_path_prefixes,
        },
    )
    assert app_one.middlewares[2][0] is AuditTrailMiddleware
    assert app_one.middlewares[2][1]["enabled"] is settings_value.enable_audit_trail
    assert app_one.middlewares[2][1]["exempt_path_prefixes"] == settings_value.audit_trail_exempt_path_prefixes
    assert callable(app_one.middlewares[2][1]["session_factory"])
    assert app_one.middlewares[3] == (RequestContextMiddleware, {})
    assert app_one.routes["/"]() == {
        "service": settings_value.app_name,
        "status": "ok",
        "version": "0.3.0",
        "docs": "/docs",
    }


def test_create_app_with_explicit_settings_skips_settings_loader(monkeypatch):
    _purge_api_main_modules()

    def exploding_get_settings():
        raise AssertionError("get_settings should not be called")

    counters, _ = _install_api_main_stubs(monkeypatch, get_settings_impl=exploding_get_settings)
    module = importlib.import_module("data_platform.api.main")

    settings = SimpleNamespace(
        app_name="custom-app",
        log_level="DEBUG",
        cors_allowed_origins=["https://example.com"],
        enable_rate_limit=True,
        enable_audit_trail=True,
        audit_trail_exempt_path_prefixes=["/health"],
        rate_limit_requests=42,
        rate_limit_window_seconds=30,
        rate_limit_exempt_path_prefixes=["/health"],
    )
    app = module.create_app(settings=settings)

    assert app.title == "custom-app"
    assert counters["logging_calls"] == 1
    assert counters.get("settings_calls", 0) == 0
