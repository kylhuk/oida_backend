from __future__ import annotations

import json
import sys
import types

from pydantic import BaseModel

pydantic_settings_stub = types.ModuleType("pydantic_settings")
pydantic_settings_stub.BaseSettings = BaseModel
pydantic_settings_stub.SettingsConfigDict = dict
sys.modules.setdefault("pydantic_settings", pydantic_settings_stub)
from datetime import datetime, timezone
from types import SimpleNamespace

from data_platform.services.notifications import (
    NotificationDeliveryResult,
    WebhookNotificationService,
    build_ingestion_job_notification_payload,
    build_pipeline_run_notification_payload,
)
from data_platform.settings import Settings


class _FakeResponse:
    def __init__(self, *, status: int = 202) -> None:
        self.status = status

    def __enter__(self) -> "_FakeResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


class _RecordingOpener:
    def __init__(self, *, status: int = 202) -> None:
        self.status = status
        self.requests: list[tuple[object, int]] = []

    def __call__(self, request, *, timeout: int):
        self.requests.append((request, timeout))
        return _FakeResponse(status=self.status)


def test_webhook_notification_service_posts_json_to_every_configured_url() -> None:
    opener = _RecordingOpener(status=204)
    settings = Settings(
        ENABLE_WEBHOOK_NOTIFICATIONS=True,
        NOTIFICATION_WEBHOOK_URLS="https://hooks.example/a,https://hooks.example/b",
        NOTIFICATION_EVENTS="pipeline_run.failed,pipeline_run.succeeded",
        NOTIFICATION_TIMEOUT_SECONDS=9,
        APP_NAME="Platform",
        APP_ENV="test",
    )

    results = WebhookNotificationService(settings=settings, opener=opener).notify(
        "pipeline_run.failed",
        {"run_id": "run-123", "status": "failed"},
    )

    assert results == [
        NotificationDeliveryResult(url="https://hooks.example/a", delivered=True, status_code=204),
        NotificationDeliveryResult(url="https://hooks.example/b", delivered=True, status_code=204),
    ]
    assert len(opener.requests) == 2
    first_request, first_timeout = opener.requests[0]
    assert first_timeout == 9
    assert first_request.full_url == "https://hooks.example/a"
    assert first_request.get_method() == "POST"
    body = json.loads(first_request.data.decode("utf-8"))
    assert body["event_type"] == "pipeline_run.failed"
    assert body["app_name"] == "Platform"
    assert body["app_env"] == "test"
    assert body["payload"] == {"run_id": "run-123", "status": "failed"}


def test_webhook_notification_service_respects_event_filter_and_disable_flag() -> None:
    opener = _RecordingOpener()
    disabled_settings = Settings(ENABLE_WEBHOOK_NOTIFICATIONS=False, NOTIFICATION_WEBHOOK_URLS="https://hooks.example/a")
    enabled_settings = Settings(
        ENABLE_WEBHOOK_NOTIFICATIONS=True,
        NOTIFICATION_WEBHOOK_URLS="https://hooks.example/a",
        NOTIFICATION_EVENTS="pipeline_run.failed",
    )

    disabled_results = WebhookNotificationService(settings=disabled_settings, opener=opener).notify(
        "pipeline_run.failed",
        {"run_id": "run-123"},
    )
    filtered_results = WebhookNotificationService(settings=enabled_settings, opener=opener).notify(
        "ingestion_job.succeeded",
        {"job_id": "job-123"},
    )

    assert disabled_results == []
    assert filtered_results == []
    assert opener.requests == []


def test_webhook_notification_service_returns_failed_delivery_results_without_raising() -> None:
    def failing_opener(request, *, timeout: int):
        raise RuntimeError(f"boom:{request.full_url}:{timeout}")

    settings = Settings(
        ENABLE_WEBHOOK_NOTIFICATIONS=True,
        NOTIFICATION_WEBHOOK_URLS="https://hooks.example/a",
        NOTIFICATION_EVENTS="*",
        NOTIFICATION_TIMEOUT_SECONDS=3,
    )

    results = WebhookNotificationService(settings=settings, opener=failing_opener).notify(
        "ingestion_job.failed",
        {"job_id": "job-123"},
    )

    assert results == [
        NotificationDeliveryResult(
            url="https://hooks.example/a",
            delivered=False,
            error="boom:https://hooks.example/a:3",
        )
    ]


def test_notification_payload_builders_capture_terminal_resource_context() -> None:
    finished_at = datetime(2026, 4, 16, 9, 30, tzinfo=timezone.utc)
    run = SimpleNamespace(
        id="run-123",
        status="failed",
        run_ref="nightly:job-7",
        ingestion_job_id="job-7",
        error_message="sql boom",
        started_at=finished_at,
        finished_at=finished_at,
        metrics_json={
            "execution_plan": {
                "engine": "sql",
                "source_layer": "raw",
                "target_layer": "gold",
                "source_object_uri": "s3://raw/orders/job-7/input.parquet",
            },
            "execution": {
                "executor": "sql_builtin",
                "status": "failed",
                "task_id": "task-7",
                "error_message": "sql boom",
                "source_object_uri": "s3://raw/orders/job-7/input.parquet",
            },
        },
    )
    pipeline = SimpleNamespace(id="pipe-1", name="Orders nightly", dataset_id="dataset-1")
    dataset = SimpleNamespace(id="dataset-1", slug="orders")

    run_payload = build_pipeline_run_notification_payload(run, pipeline, dataset)

    assert run_payload["resource_type"] == "pipeline_run"
    assert run_payload["run_id"] == "run-123"
    assert run_payload["dataset_slug"] == "orders"
    assert run_payload["execution_details"]["task_id"] == "task-7"
    assert run_payload["artifact_manifest"]["source_object_uri"] == "s3://raw/orders/job-7/input.parquet"

    job = SimpleNamespace(
        id="job-7",
        dataset_id="dataset-1",
        status="failed",
        source_type="upload",
        filename="orders.csv",
        error_message="csv boom",
        raw_object_uri="s3://raw/orders/job-7/input.csv",
        silver_object_uri=None,
        gold_object_uri=None,
        content_hash="abc123",
        source_format="csv",
        source_content_type="text/csv",
        size_bytes=42,
        row_count=None,
        started_at=finished_at,
        finished_at=finished_at,
        job_metadata={"processing": {"task_id": "ingest-task-7", "detected_format": "csv"}},
    )

    job_payload = build_ingestion_job_notification_payload(job, dataset)

    assert job_payload["resource_type"] == "ingestion_job"
    assert job_payload["job_id"] == "job-7"
    assert job_payload["dataset_slug"] == "orders"
    assert job_payload["task_id"] == "ingest-task-7"
    assert job_payload["detected_format"] == "csv"
    assert job_payload["raw_object_uri"] == "s3://raw/orders/job-7/input.csv"
