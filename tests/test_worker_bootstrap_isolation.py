from __future__ import annotations

import importlib
import importlib.util
import sys
import types
from pathlib import Path
from types import SimpleNamespace


class _CeleryStub:
    def __init__(self, name: str, broker: str, backend: str) -> None:
        self.name = name
        self.broker = broker
        self.backend = backend
        self.conf: dict[str, object] = {}
        self.autodiscovered: list[list[str]] = []

    def autodiscover_tasks(self, packages: list[str]) -> None:
        self.autodiscovered.append(list(packages))

    def task(self, **kwargs):
        def decorator(func):
            func._celery_task_kwargs = kwargs
            return func

        return decorator


class _CeleryFactory:
    def __init__(self) -> None:
        self.instances: list[_CeleryStub] = []

    def __call__(self, name: str, broker: str, backend: str) -> _CeleryStub:
        instance = _CeleryStub(name, broker, backend)
        self.instances.append(instance)
        return instance


def _purge_worker_modules() -> None:
    for name in list(sys.modules):
        if name in {"data_platform.workers.celery_app", "data_platform.workers.tasks"}:
            sys.modules.pop(name, None)


def _install_celery_bootstrap_stubs(monkeypatch, *, get_settings_impl=None):
    counters = {"settings_calls": 0, "logging_calls": 0}
    settings_value = SimpleNamespace(redis_url="redis://redis:6379/0", log_level="INFO")

    def default_get_settings():
        counters["settings_calls"] += 1
        return settings_value

    def configure_logging(level: str):
        counters["logging_calls"] += 1
        counters["last_log_level"] = level

    celery_factory = _CeleryFactory()
    celery_module = types.ModuleType("celery")
    celery_module.Celery = celery_factory

    settings_module = types.ModuleType("data_platform.settings")
    settings_module.get_settings = get_settings_impl or default_get_settings

    log_config_module = types.ModuleType("data_platform.log_config")
    log_config_module.configure_logging = configure_logging

    monkeypatch.setitem(sys.modules, "celery", celery_module)
    monkeypatch.setitem(sys.modules, "data_platform.settings", settings_module)
    monkeypatch.setitem(sys.modules, "data_platform.log_config", log_config_module)

    return counters, settings_value, celery_factory


def _load_tasks_module(module_name: str):
    module_path = Path(__file__).resolve().parents[1] / "src" / "data_platform" / "workers" / "tasks.py"
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_importing_celery_app_module_does_not_import_optional_dependencies(monkeypatch):
    _purge_worker_modules()
    for name in ["celery", "data_platform.settings", "data_platform.log_config"]:
        monkeypatch.delitem(sys.modules, name, raising=False)

    module = importlib.import_module("data_platform.workers.celery_app")

    assert hasattr(module, "create_celery_app")
    assert "celery" not in sys.modules
    assert "data_platform.settings" not in sys.modules
    assert "data_platform.log_config" not in sys.modules



def test_module_celery_app_is_built_lazily_and_cached(monkeypatch):
    _purge_worker_modules()
    counters, settings_value, celery_factory = _install_celery_bootstrap_stubs(monkeypatch)

    module = importlib.import_module("data_platform.workers.celery_app")
    app_one = getattr(module, "celery_app")
    app_two = getattr(module, "celery_app")

    assert app_one is app_two
    assert len(celery_factory.instances) == 1
    assert app_one.broker == settings_value.redis_url
    assert app_one.backend == settings_value.redis_url
    assert counters["settings_calls"] == 1
    assert counters["logging_calls"] == 1
    assert app_one.autodiscovered == [["data_platform.workers"]]
    assert app_one.conf["task_track_started"] is True
    assert app_one.conf["task_serializer"] == "json"



def test_create_celery_app_with_explicit_settings_skips_settings_loader(monkeypatch):
    _purge_worker_modules()

    def exploding_get_settings():
        raise AssertionError("get_settings should not be called")

    counters, _, celery_factory = _install_celery_bootstrap_stubs(monkeypatch, get_settings_impl=exploding_get_settings)
    module = importlib.import_module("data_platform.workers.celery_app")

    app = module.create_celery_app(settings=SimpleNamespace(redis_url="redis://example/1", log_level="DEBUG"))

    assert app.broker == "redis://example/1"
    assert app.backend == "redis://example/1"
    assert counters["logging_calls"] == 1
    assert counters.get("settings_calls", 0) == 0
    assert len(celery_factory.instances) == 1



def test_importing_tasks_module_does_not_require_sqlalchemy_or_models(monkeypatch):
    _purge_worker_modules()
    celery_stub_module = types.ModuleType("data_platform.workers.celery_app")
    celery_stub_module.celery_app = _CeleryStub("data_platform", broker="redis://example/1", backend="redis://example/1")
    monkeypatch.setitem(sys.modules, "data_platform.workers.celery_app", celery_stub_module)
    for name in [
        "sqlalchemy",
        "sqlalchemy.orm",
        "data_platform.db",
        "data_platform.enums",
        "data_platform.models.ingestion",
        "data_platform.services.processing_service",
        "data_platform.services.pipeline_execution_service",
    ]:
        monkeypatch.delitem(sys.modules, name, raising=False)

    module = _load_tasks_module("_isolated_worker_tasks_test_module")

    assert callable(module.process_ingestion_job)
    assert callable(module.execute_next_pipeline_run)
    assert "data_platform.db" not in sys.modules
    assert "data_platform.models.ingestion" not in sys.modules
    assert "data_platform.services.pipeline_execution_service" not in sys.modules
    assert module.process_ingestion_job._celery_task_kwargs["name"] == "data_platform.process_ingestion_job"
    assert module.execute_next_pipeline_run._celery_task_kwargs == {
        "name": "data_platform.execute_next_pipeline_run",
        "bind": True,
    }


class _JobStub:
    def __init__(self) -> None:
        self.status = "downloading"
        self.error_message = None
        self.finished_at = None
        self.job_metadata = {"processing": {"started_at": "2026-04-14T00:00:00+00:00"}}


class _SessionStub:
    def __init__(self, job: _JobStub | None) -> None:
        self.job = job
        self.rollback_calls = 0
        self.commit_calls = 0
        self.closed = False

    def get(self, _model, _job_id: str):
        return self.job

    def rollback(self) -> None:
        self.rollback_calls += 1

    def commit(self) -> None:
        self.commit_calls += 1

    def close(self) -> None:
        self.closed = True


class _ProcessingServiceFailingStub:
    def __init__(self, session) -> None:
        self.session = session

    def process_job(self, job_id: str, task_id: str | None = None):
        raise RuntimeError(f"boom for {job_id} via {task_id}")


class _NotificationServiceStub:
    calls: list[tuple[str, dict]] = []

    def notify(self, event_type: str, payload: dict):
        self.calls.append((event_type, dict(payload)))
        return []


def _install_task_runtime_stubs(monkeypatch, *, session: _SessionStub):
    db_module = types.ModuleType("data_platform.db")
    db_module.get_session_factory = lambda: (lambda: session)

    enums_module = types.ModuleType("data_platform.enums")
    enums_module.IngestionStatus = SimpleNamespace(FAILED=SimpleNamespace(value="failed"))

    models_module = types.ModuleType("data_platform.models.ingestion")
    models_module.IngestionJob = object

    processing_module = types.ModuleType("data_platform.services.processing_service")
    processing_module.MedallionProcessingService = _ProcessingServiceFailingStub

    notifications_module = types.ModuleType("data_platform.services.notifications")
    notifications_module.WebhookNotificationService = _NotificationServiceStub
    notifications_module.build_ingestion_job_notification_payload = lambda job, dataset, *, task_id=None: {
        "job_id": getattr(job, "id", None),
        "dataset_slug": getattr(dataset, "slug", None),
        "task_id": task_id,
        "status": getattr(job, "status", None),
    }

    monkeypatch.setitem(sys.modules, "data_platform.db", db_module)
    monkeypatch.setitem(sys.modules, "data_platform.enums", enums_module)
    monkeypatch.setitem(sys.modules, "data_platform.models.ingestion", models_module)
    monkeypatch.setitem(sys.modules, "data_platform.services.processing_service", processing_module)
    monkeypatch.setitem(sys.modules, "data_platform.services.notifications", notifications_module)


def test_process_ingestion_job_preserves_non_terminal_retry_state(monkeypatch):
    _purge_worker_modules()
    celery_stub_module = types.ModuleType("data_platform.workers.celery_app")
    celery_stub_module.celery_app = _CeleryStub("data_platform", broker="redis://example/1", backend="redis://example/1")
    monkeypatch.setitem(sys.modules, "data_platform.workers.celery_app", celery_stub_module)

    _NotificationServiceStub.calls = []
    job = _JobStub()
    session = _SessionStub(job)
    _install_task_runtime_stubs(monkeypatch, session=session)

    module = _load_tasks_module("_retrying_worker_tasks_test_module")
    task_self = SimpleNamespace(request=SimpleNamespace(id="task-123", retries=1), max_retries=3)

    try:
        module.process_ingestion_job(task_self, "job-1")
    except RuntimeError:
        pass
    else:
        raise AssertionError("process_ingestion_job should re-raise the worker exception")

    assert session.rollback_calls == 1
    assert session.commit_calls == 1
    assert session.closed is True
    assert job.status == "downloading"
    assert job.finished_at is None
    assert "failed_at" not in job.job_metadata["processing"]
    assert job.job_metadata["processing"]["retry"] == {
        "current_retry": 1,
        "max_retries": 3,
        "will_retry": True,
        "task_id": "task-123",
    }
    assert job.error_message == "boom for job-1 via task-123"



def test_process_ingestion_job_marks_terminal_failure_when_retries_exhausted(monkeypatch):
    _purge_worker_modules()
    celery_stub_module = types.ModuleType("data_platform.workers.celery_app")
    celery_stub_module.celery_app = _CeleryStub("data_platform", broker="redis://example/1", backend="redis://example/1")
    monkeypatch.setitem(sys.modules, "data_platform.workers.celery_app", celery_stub_module)

    _NotificationServiceStub.calls = []
    job = _JobStub()
    session = _SessionStub(job)
    _install_task_runtime_stubs(monkeypatch, session=session)

    module = _load_tasks_module("_terminal_worker_tasks_test_module")
    task_self = SimpleNamespace(request=SimpleNamespace(id="task-999", retries=3), max_retries=3)

    try:
        module.process_ingestion_job(task_self, "job-9")
    except RuntimeError:
        pass
    else:
        raise AssertionError("process_ingestion_job should re-raise the worker exception")

    assert session.rollback_calls == 1
    assert session.commit_calls == 1
    assert session.closed is True
    assert job.status == "failed"
    assert job.finished_at is not None
    assert job.job_metadata["processing"]["retry"] == {
        "current_retry": 3,
        "max_retries": 3,
        "will_retry": False,
        "task_id": "task-999",
    }
    assert job.job_metadata["processing"]["terminal_failure"] is True
    assert job.job_metadata["processing"]["failed_at"] == job.finished_at.isoformat()
    assert job.error_message == "boom for job-9 via task-999"
    assert _NotificationServiceStub.calls == [
        (
            "ingestion_job.failed",
            {
                "job_id": None,
                "dataset_slug": None,
                "task_id": "task-999",
                "status": "failed",
            },
        )
    ]


class _PipelineExecutionRunStub:
    def __init__(self, run_id: str, status: str) -> None:
        self.id = run_id
        self.status = status


class _PipelineExecutionServiceIdleStub:
    def __init__(self, session) -> None:
        self.session = session

    def execute_next_run(self, pipeline_id: str, *, task_id: str | None = None):
        return None


def test_execute_next_pipeline_run_returns_idle_when_no_planned_run_exists(monkeypatch):
    _purge_worker_modules()
    celery_stub_module = types.ModuleType("data_platform.workers.celery_app")
    celery_stub_module.celery_app = _CeleryStub("data_platform", broker="redis://example/1", backend="redis://example/1")
    monkeypatch.setitem(sys.modules, "data_platform.workers.celery_app", celery_stub_module)

    session = _SessionStub(job=None)
    db_module = types.ModuleType("data_platform.db")
    db_module.get_session_factory = lambda: (lambda: session)

    pipeline_execution_module = types.ModuleType("data_platform.services.pipeline_execution_service")
    pipeline_execution_module.SqlPipelineExecutionService = _PipelineExecutionServiceIdleStub

    monkeypatch.setitem(sys.modules, "data_platform.db", db_module)
    monkeypatch.setitem(sys.modules, "data_platform.services.pipeline_execution_service", pipeline_execution_module)

    module = _load_tasks_module("_pipeline_worker_idle_test_module")
    task_self = SimpleNamespace(request=SimpleNamespace(id="task-789"))

    result = module.execute_next_pipeline_run(task_self, "pipeline-1")

    assert result == {"pipeline_id": "pipeline-1", "status": "idle"}
    assert session.closed is True
