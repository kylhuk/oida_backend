from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from data_platform.workers.celery_app import celery_app

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


def _retry_state(task: Any) -> tuple[int, int | None, bool]:
    request = getattr(task, "request", None)
    current_retry = getattr(request, "retries", 0)
    try:
        current_retry = int(current_retry)
    except (TypeError, ValueError):
        current_retry = 0

    max_retries = getattr(task, "max_retries", None)
    try:
        max_retries = int(max_retries) if max_retries is not None else None
    except (TypeError, ValueError):
        max_retries = None

    will_retry = max_retries is None or current_retry < max_retries
    return current_retry, max_retries, will_retry


def _merge_processing_metadata(job: Any, extra: dict[str, Any]) -> None:
    updated = dict(getattr(job, "job_metadata", None) or {})
    processing = dict(updated.get("processing") or {})
    processing.update(extra)
    updated["processing"] = processing
    job.job_metadata = updated


@celery_app.task(
    name="data_platform.process_ingestion_job",
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_kwargs={"max_retries": 3},
)
def process_ingestion_job(self, job_id: str) -> dict[str, Any]:
    from data_platform.db import get_session_factory
    from data_platform.enums import IngestionStatus
    from data_platform.models.ingestion import IngestionJob
    from data_platform.services.processing_service import MedallionProcessingService

    session: Session = get_session_factory()()
    try:
        return MedallionProcessingService(session).process_job(
            job_id,
            task_id=getattr(self.request, "id", None),
        )
    except Exception as exc:
        logger.exception("Ingestion job failed: %s", job_id)
        session.rollback()
        failed_job = session.get(IngestionJob, job_id)
        if failed_job:
            current_retry, max_retries, will_retry = _retry_state(self)
            error_timestamp = datetime.now(timezone.utc)

            failed_job.error_message = str(exc)
            _merge_processing_metadata(
                failed_job,
                {
                    "last_failed_at": error_timestamp.isoformat(),
                    "error": str(exc),
                    "retry": {
                        "current_retry": current_retry,
                        "max_retries": max_retries,
                        "will_retry": will_retry,
                        "task_id": getattr(self.request, "id", None),
                    },
                },
            )

            if will_retry:
                failed_job.finished_at = None
            else:
                from data_platform.services.notifications import (
                    WebhookNotificationService,
                    build_ingestion_job_notification_payload,
                )

                failed_job.status = IngestionStatus.FAILED.value
                failed_job.finished_at = error_timestamp
                _merge_processing_metadata(
                    failed_job,
                    {
                        "failed_at": error_timestamp.isoformat(),
                        "terminal_failure": True,
                    },
                )
            session.commit()

            from data_platform.audit_trail import build_system_audit_event, persist_audit_event_with_session_factory
            from data_platform.settings import get_settings

            settings = get_settings()
            if settings.enable_audit_trail:
                payload = build_system_audit_event(
                    "ingestion_job.retrying" if will_retry else "ingestion_job.failed",
                    resource_type="ingestion_job",
                    resource_id=failed_job.id,
                    path=f"/worker/ingestion-jobs/{failed_job.id}",
                    status_code=500,
                    details_json={
                        "status": failed_job.status,
                        "dataset_id": getattr(failed_job, "dataset_id", None),
                        "dataset_slug": getattr(getattr(failed_job, "dataset", None), "slug", None),
                        "task_id": getattr(self.request, "id", None),
                        "error_message": str(exc),
                        "retry": dict((failed_job.job_metadata or {}).get("processing", {}).get("retry") or {}),
                    },
                )
                persist_audit_event_with_session_factory(get_session_factory(), payload)

            if not will_retry:
                WebhookNotificationService().notify(
                    "ingestion_job.failed",
                    build_ingestion_job_notification_payload(
                        failed_job,
                        getattr(failed_job, "dataset", None),
                        task_id=getattr(self.request, "id", None),
                    ),
                )
        raise
    finally:
        session.close()


@celery_app.task(
    name="data_platform.execute_next_pipeline_run",
    bind=True,
)
def execute_next_pipeline_run(self, pipeline_id: str) -> dict[str, Any]:
    from data_platform.db import get_session_factory
    from data_platform.services.pipeline_execution_service import SqlPipelineExecutionService

    session: Session = get_session_factory()()
    try:
        run = SqlPipelineExecutionService(session).execute_next_run(
            pipeline_id,
            task_id=getattr(self.request, "id", None),
        )
        if run is None:
            return {"pipeline_id": pipeline_id, "status": "idle"}
        return {"pipeline_id": pipeline_id, "run_id": run.id, "status": run.status}
    finally:
        session.close()
