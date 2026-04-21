from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable
from urllib.request import Request, urlopen

from data_platform.settings import Settings, get_settings
from data_platform.utils.pipeline_definitions import extract_pipeline_artifact_manifest, extract_pipeline_run_snapshot

logger = logging.getLogger(__name__)

WebhookOpener = Callable[..., Any]


@dataclass(frozen=True)
class NotificationDeliveryResult:
    url: str
    delivered: bool
    status_code: int | None = None
    error: str | None = None


class WebhookNotificationService:
    def __init__(
        self,
        *,
        settings: Settings | None = None,
        opener: WebhookOpener | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.opener = opener or urlopen

    def is_enabled_for_event(self, event_type: str) -> bool:
        normalized_event = str(event_type).strip().lower()
        if not self.settings.enable_webhook_notifications:
            return False
        if not normalized_event:
            return False
        if not self.settings.notification_webhook_url_values:
            return False
        configured_events = self.settings.notification_event_values
        if not configured_events:
            return False
        return "*" in configured_events or normalized_event in configured_events

    def notify(self, event_type: str, payload: dict[str, Any]) -> list[NotificationDeliveryResult]:
        normalized_event = str(event_type).strip().lower()
        if not self.is_enabled_for_event(normalized_event):
            return []

        encoded_payload = json.dumps(
            {
                "event_type": normalized_event,
                "occurred_at": datetime.now(timezone.utc).isoformat(),
                "app_name": self.settings.app_name,
                "app_env": self.settings.app_env,
                "payload": _json_safe(payload),
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")

        results: list[NotificationDeliveryResult] = []
        for url in self.settings.notification_webhook_url_values:
            request = Request(
                url,
                data=encoded_payload,
                headers={
                    "Content-Type": "application/json",
                    "User-Agent": "medallion-backend-notifier/1",
                },
                method="POST",
            )
            try:
                with self.opener(request, timeout=self.settings.notification_timeout_seconds) as response:
                    status_code = int(getattr(response, "status", 200))
                results.append(NotificationDeliveryResult(url=url, delivered=True, status_code=status_code))
            except Exception as exc:  # pragma: no cover - exercised in tests through injected opener
                logger.warning("Webhook notification delivery failed for %s (%s): %s", normalized_event, url, exc)
                results.append(NotificationDeliveryResult(url=url, delivered=False, error=str(exc)))
        return results


def build_ingestion_job_notification_payload(job: Any, dataset: Any | None = None, *, task_id: str | None = None) -> dict[str, Any]:
    dataset_slug = _strip_optional(getattr(dataset, "slug", None))
    dataset_id = _strip_optional(getattr(dataset, "id", None)) or _strip_optional(getattr(job, "dataset_id", None))
    processing_metadata = {}
    if isinstance(getattr(job, "job_metadata", None), dict):
        processing_metadata = dict(getattr(job, "job_metadata")).get("processing") or {}
        if not isinstance(processing_metadata, dict):
            processing_metadata = {}

    payload: dict[str, Any] = {
        "resource_type": "ingestion_job",
        "job_id": _strip_optional(getattr(job, "id", None)),
        "dataset_id": dataset_id,
        "dataset_slug": dataset_slug,
        "status": _strip_optional(getattr(job, "status", None)),
        "source_type": _strip_optional(getattr(job, "source_type", None)),
        "filename": _strip_optional(getattr(job, "filename", None)),
        "error_message": _strip_optional(getattr(job, "error_message", None)),
        "raw_object_uri": _strip_optional(getattr(job, "raw_object_uri", None)),
        "silver_object_uri": _strip_optional(getattr(job, "silver_object_uri", None)),
        "gold_object_uri": _strip_optional(getattr(job, "gold_object_uri", None)),
        "content_hash": _strip_optional(getattr(job, "content_hash", None)),
        "source_format": _strip_optional(getattr(job, "source_format", None)),
        "source_content_type": _strip_optional(getattr(job, "source_content_type", None)),
        "started_at": _encode_optional_datetime(getattr(job, "started_at", None)),
        "finished_at": _encode_optional_datetime(getattr(job, "finished_at", None)),
        "task_id": _strip_optional(task_id) or _strip_optional(processing_metadata.get("task_id")),
    }

    row_count = getattr(job, "row_count", None)
    if isinstance(row_count, int) and not isinstance(row_count, bool) and row_count >= 0:
        payload["row_count"] = row_count

    size_bytes = getattr(job, "size_bytes", None)
    if isinstance(size_bytes, int) and not isinstance(size_bytes, bool) and size_bytes >= 0:
        payload["size_bytes"] = size_bytes

    detected_format = _strip_optional(processing_metadata.get("detected_format"))
    if detected_format is not None:
        payload["detected_format"] = detected_format

    retry_metadata = processing_metadata.get("retry")
    if isinstance(retry_metadata, dict) and retry_metadata:
        payload["retry"] = _json_safe(retry_metadata)

    return {key: value for key, value in payload.items() if value is not None}


def build_pipeline_run_notification_payload(
    run: Any,
    pipeline: Any,
    dataset: Any | None = None,
    *,
    task_id: str | None = None,
) -> dict[str, Any]:
    dataset_id = _strip_optional(getattr(dataset, "id", None)) or _strip_optional(getattr(pipeline, "dataset_id", None))
    metrics_json = getattr(run, "metrics_json", None)

    payload: dict[str, Any] = {
        "resource_type": "pipeline_run",
        "run_id": _strip_optional(getattr(run, "id", None)),
        "pipeline_id": _strip_optional(getattr(pipeline, "id", None)),
        "pipeline_name": _strip_optional(getattr(pipeline, "name", None)),
        "dataset_id": dataset_id,
        "dataset_slug": _strip_optional(getattr(dataset, "slug", None)),
        "status": _strip_optional(getattr(run, "status", None)),
        "run_ref": _strip_optional(getattr(run, "run_ref", None)),
        "source_ingestion_job_id": _strip_optional(getattr(run, "ingestion_job_id", None)),
        "error_message": _strip_optional(getattr(run, "error_message", None)),
        "started_at": _encode_optional_datetime(getattr(run, "started_at", None)),
        "finished_at": _encode_optional_datetime(getattr(run, "finished_at", None)),
        "task_id": _strip_optional(task_id),
    }

    execution_snapshot = extract_pipeline_run_snapshot(metrics_json)
    execution_details = execution_snapshot.get("execution_details")
    if isinstance(execution_details, dict) and execution_details:
        payload["execution_details"] = _json_safe(execution_details)
        payload.setdefault("task_id", _strip_optional(execution_details.get("task_id")))

    artifact_manifest = extract_pipeline_artifact_manifest(
        metrics_json,
        run_id=payload.get("run_id"),
        pipeline_id=payload.get("pipeline_id"),
        dataset_id=dataset_id,
        source_ingestion_job_id=payload.get("source_ingestion_job_id"),
        run_status=payload.get("status"),
    )
    if artifact_manifest is not None:
        payload["artifact_manifest"] = _json_safe(artifact_manifest)

    return {key: value for key, value in payload.items() if value is not None}


def _encode_optional_datetime(value: Any) -> str | None:
    if not isinstance(value, datetime):
        return None
    normalized = value.astimezone(timezone.utc) if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    return normalized.isoformat()


def _strip_optional(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    return normalized or None


def _json_safe(value: Any) -> Any:
    if isinstance(value, datetime):
        return _encode_optional_datetime(value)
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    return value
