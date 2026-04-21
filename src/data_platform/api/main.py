from __future__ import annotations

import importlib
from contextlib import asynccontextmanager
from typing import Any

from data_platform import __version__
from data_platform.audit_trail import AuditTrailMiddleware
from data_platform.rate_limit import RateLimitMiddleware
from data_platform.request_context import RequestContextMiddleware

_cached_app: Any | None = None


def _load_fastapi_components() -> tuple[type[Any], Any]:
    try:
        fastapi_module = importlib.import_module("fastapi")
        cors_module = importlib.import_module("fastapi.middleware.cors")
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "FastAPI application bootstrap requires fastapi to be installed. "
            "Install the API dependencies or access data_platform.api.main.create_app only in a provisioned environment."
        ) from exc

    fastapi_class = getattr(fastapi_module, "FastAPI", None)
    cors_middleware = getattr(cors_module, "CORSMiddleware", None)
    if fastapi_class is None or cors_middleware is None:
        raise RuntimeError("FastAPI application bootstrap requires fastapi.FastAPI and CORSMiddleware.")
    return fastapi_class, cors_middleware


def _load_get_settings() -> Any:
    try:
        settings_module = importlib.import_module("data_platform.settings")
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "FastAPI application bootstrap requires data_platform.settings to be importable."
        ) from exc

    get_settings = getattr(settings_module, "get_settings", None)
    if not callable(get_settings):
        raise RuntimeError("FastAPI application bootstrap requires data_platform.settings.get_settings().")
    return get_settings


def _load_configure_logging() -> Any:
    log_config_module = importlib.import_module("data_platform.log_config")
    configure_logging = getattr(log_config_module, "configure_logging", None)
    if not callable(configure_logging):
        raise RuntimeError("FastAPI application bootstrap requires data_platform.log_config.configure_logging().")
    return configure_logging


def _load_router(module_name: str) -> Any:
    module = importlib.import_module(module_name)
    router = getattr(module, "router", None)
    if router is None:
        raise RuntimeError(f"{module_name} must expose a router.")
    return router


def create_app(settings: Any | None = None) -> Any:
    runtime_settings = settings if settings is not None else _load_get_settings()()
    configure_logging = _load_configure_logging()
    configure_logging(runtime_settings.log_level)
    FastAPI, CORSMiddleware = _load_fastapi_components()

    init_db = getattr(importlib.import_module("data_platform.db"), "init_db")
    get_session_factory = getattr(importlib.import_module("data_platform.db"), "get_session_factory")
    ensure_seeded_api_key = getattr(importlib.import_module("data_platform.security"), "ensure_seeded_api_key")

    audit_router = _load_router("data_platform.api.routes.audit")
    catalog_router = _load_router("data_platform.api.routes.catalog")
    datasets_router = _load_router("data_platform.api.routes.datasets")
    observability_router = _load_router("data_platform.api.routes.observability")
    gold_router = _load_router("data_platform.api.routes.gold")
    health_router = _load_router("data_platform.api.routes.health")
    ingestions_router = _load_router("data_platform.api.routes.ingestions")
    pipelines_router = _load_router("data_platform.api.routes.pipelines")

    @asynccontextmanager
    async def lifespan(_: Any):
        init_db()
        with get_session_factory()() as session:
            ensure_seeded_api_key(session)
        yield

    app = FastAPI(
        title=runtime_settings.app_name,
        version=__version__,
        lifespan=lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=runtime_settings.cors_allowed_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.add_middleware(
        RateLimitMiddleware,
        enabled=runtime_settings.enable_rate_limit,
        limit=runtime_settings.rate_limit_requests,
        window_seconds=runtime_settings.rate_limit_window_seconds,
        exempt_path_prefixes=runtime_settings.rate_limit_exempt_path_prefixes,
    )
    app.add_middleware(
        AuditTrailMiddleware,
        enabled=runtime_settings.enable_audit_trail,
        exempt_path_prefixes=runtime_settings.audit_trail_exempt_path_prefixes,
        session_factory=get_session_factory(),
    )
    app.add_middleware(RequestContextMiddleware)

    app.include_router(health_router)
    app.include_router(audit_router)
    app.include_router(observability_router)
    app.include_router(catalog_router)
    app.include_router(datasets_router)
    app.include_router(ingestions_router)
    app.include_router(gold_router)
    app.include_router(pipelines_router)

    @app.get("/")
    def root() -> dict[str, str]:
        return {
            "service": runtime_settings.app_name,
            "status": "ok",
            "version": __version__,
            "docs": "/docs",
        }

    return app


def get_app() -> Any:
    global _cached_app
    if _cached_app is None:
        _cached_app = create_app()
    return _cached_app


def __getattr__(name: str) -> Any:
    if name == "app":
        return get_app()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = ["create_app", "get_app", "app"]
