from __future__ import annotations

import importlib
from typing import Any

_cached_celery_app: Any | None = None


def _load_celery_class() -> type[Any]:
    try:
        celery_module = importlib.import_module("celery")
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Celery worker bootstrap requires celery to be installed. "
            "Install the worker dependencies or access data_platform.workers.celery_app only in a provisioned environment."
        ) from exc

    celery_class = getattr(celery_module, "Celery", None)
    if celery_class is None:
        raise RuntimeError("Celery worker bootstrap requires celery.Celery.")
    return celery_class


def _load_get_settings() -> Any:
    try:
        settings_module = importlib.import_module("data_platform.settings")
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Celery worker bootstrap requires data_platform.settings to be importable."
        ) from exc

    get_settings = getattr(settings_module, "get_settings", None)
    if not callable(get_settings):
        raise RuntimeError("Celery worker bootstrap requires data_platform.settings.get_settings().")
    return get_settings


def _load_configure_logging() -> Any:
    log_config_module = importlib.import_module("data_platform.log_config")
    configure_logging = getattr(log_config_module, "configure_logging", None)
    if not callable(configure_logging):
        raise RuntimeError("Celery worker bootstrap requires data_platform.log_config.configure_logging().")
    return configure_logging


def create_celery_app(*, settings: Any | None = None, celery_class: type[Any] | None = None) -> Any:
    runtime_settings = settings if settings is not None else _load_get_settings()()
    configure_logging = _load_configure_logging()
    configure_logging(runtime_settings.log_level)

    Celery = celery_class or _load_celery_class()
    app = Celery("data_platform", broker=runtime_settings.redis_url, backend=runtime_settings.redis_url)
    app.conf.update(
        task_track_started=True,
        worker_prefetch_multiplier=1,
        task_acks_late=True,
        task_serializer="json",
        result_serializer="json",
        accept_content=["json"],
    )
    app.autodiscover_tasks(["data_platform.workers"])
    return app


def get_celery_app() -> Any:
    global _cached_celery_app
    if _cached_celery_app is None:
        _cached_celery_app = create_celery_app()
    return _cached_celery_app


def __getattr__(name: str) -> Any:
    if name == "celery_app":
        return get_celery_app()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = ["create_celery_app", "get_celery_app", "celery_app"]
