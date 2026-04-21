from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import and_, func, not_, or_, select
from sqlalchemy.orm import Session

from data_platform.audit_trail import AUDIT_MAINTENANCE_METHOD, AUDIT_WORKER_METHOD
from data_platform.models.audit import AuditEvent
from data_platform.models.dataset import Dataset
from data_platform.models.ingestion import IngestionJob
from data_platform.models.pipeline import PipelineDefinition, PipelineRun, QualityResult
from data_platform.utils.observability import build_observability_activity_report, normalize_observability_bucket


class ObservabilityService:
    @staticmethod
    def _utcnow() -> datetime:
        return datetime.now(timezone.utc)

    @staticmethod
    def _status_counts(
        session: Session,
        *,
        model: Any,
        status_column: Any,
        created_at_column: Any,
        since: datetime,
    ) -> dict[str, int]:
        stmt = (
            select(status_column, func.count(model.id))
            .where(created_at_column >= since)
            .group_by(status_column)
        )
        rows = session.execute(stmt).all()
        return {
            str(status).strip().lower(): int(count)
            for status, count in rows
            if str(status).strip()
        }

    @staticmethod
    def _build_recent_ingestion_failures(session: Session, *, limit: int) -> list[dict[str, Any]]:
        occurred_at = func.coalesce(IngestionJob.finished_at, IngestionJob.created_at)
        stmt = (
            select(
                IngestionJob.id,
                Dataset.slug,
                IngestionJob.status,
                IngestionJob.error_message,
                occurred_at.label("occurred_at"),
            )
            .join(Dataset, Dataset.id == IngestionJob.dataset_id)
            .where(IngestionJob.status == "failed")
            .order_by(occurred_at.desc(), IngestionJob.id.desc())
            .limit(limit)
        )
        rows = session.execute(stmt).all()
        return [
            {
                "kind": "ingestion_job",
                "id": job_id,
                "dataset_slug": dataset_slug,
                "status": status,
                "error_message": error_message,
                "occurred_at": occurred_at_value,
            }
            for job_id, dataset_slug, status, error_message, occurred_at_value in rows
        ]

    @staticmethod
    def _build_recent_pipeline_failures(session: Session, *, limit: int) -> list[dict[str, Any]]:
        occurred_at = func.coalesce(PipelineRun.finished_at, PipelineRun.created_at)
        stmt = (
            select(
                PipelineRun.id,
                Dataset.slug,
                PipelineRun.pipeline_id,
                PipelineDefinition.name,
                PipelineRun.run_ref,
                PipelineRun.status,
                PipelineRun.error_message,
                occurred_at.label("occurred_at"),
            )
            .select_from(PipelineRun)
            .join(Dataset, Dataset.id == PipelineRun.dataset_id, isouter=True)
            .join(PipelineDefinition, PipelineDefinition.id == PipelineRun.pipeline_id, isouter=True)
            .where(PipelineRun.status == "failed")
            .order_by(occurred_at.desc(), PipelineRun.id.desc())
            .limit(limit)
        )
        rows = session.execute(stmt).all()
        return [
            {
                "kind": "pipeline_run",
                "id": run_id,
                "dataset_slug": dataset_slug,
                "pipeline_id": pipeline_id,
                "pipeline_name": pipeline_name,
                "run_ref": run_ref,
                "status": status,
                "error_message": error_message,
                "occurred_at": occurred_at_value,
            }
            for run_id, dataset_slug, pipeline_id, pipeline_name, run_ref, status, error_message, occurred_at_value in rows
        ]

    @staticmethod
    def get_summary(
        session: Session,
        *,
        lookback_hours: int = 24,
        recent_limit: int = 10,
    ) -> dict[str, Any]:
        bounded_lookback = max(1, min(int(lookback_hours), 24 * 30))
        bounded_recent_limit = max(1, min(int(recent_limit), 100))
        generated_at = ObservabilityService._utcnow()
        since = generated_at - timedelta(hours=bounded_lookback)

        datasets_total = int(session.scalar(select(func.count(Dataset.id))) or 0)
        active_datasets = int(
            session.scalar(select(func.count(Dataset.id)).where(Dataset.status == "active")) or 0
        )
        pipelines_total = int(session.scalar(select(func.count(PipelineDefinition.id))) or 0)
        active_pipelines = int(
            session.scalar(select(func.count(PipelineDefinition.id)).where(PipelineDefinition.active.is_(True))) or 0
        )

        ingestion_status_counts = ObservabilityService._status_counts(
            session,
            model=IngestionJob,
            status_column=IngestionJob.status,
            created_at_column=IngestionJob.created_at,
            since=since,
        )
        pipeline_run_status_counts = ObservabilityService._status_counts(
            session,
            model=PipelineRun,
            status_column=PipelineRun.status,
            created_at_column=PipelineRun.created_at,
            since=since,
        )
        quality_result_status_counts = ObservabilityService._status_counts(
            session,
            model=QualityResult,
            status_column=QualityResult.status,
            created_at_column=QualityResult.created_at,
            since=since,
        )

        http_event_count = int(
            session.scalar(
                select(func.count(AuditEvent.id)).where(
                    AuditEvent.created_at >= since,
                    not_(AuditEvent.method.in_([AUDIT_WORKER_METHOD, AUDIT_MAINTENANCE_METHOD])),
                )
            )
            or 0
        )
        worker_event_count = int(
            session.scalar(
                select(func.count(AuditEvent.id)).where(
                    AuditEvent.created_at >= since,
                    AuditEvent.method == AUDIT_WORKER_METHOD,
                )
            )
            or 0
        )
        maintenance_event_count = int(
            session.scalar(
                select(func.count(AuditEvent.id)).where(
                    AuditEvent.created_at >= since,
                    AuditEvent.method == AUDIT_MAINTENANCE_METHOD,
                )
            )
            or 0
        )
        http_error_response_count = int(
            session.scalar(
                select(func.count(AuditEvent.id)).where(
                    AuditEvent.created_at >= since,
                    not_(AuditEvent.method.in_([AUDIT_WORKER_METHOD, AUDIT_MAINTENANCE_METHOD])),
                    AuditEvent.status_code >= 500,
                )
            )
            or 0
        )
        rate_limited_request_count = int(
            session.scalar(
                select(func.count(AuditEvent.id)).where(
                    AuditEvent.created_at >= since,
                    not_(AuditEvent.method.in_([AUDIT_WORKER_METHOD, AUDIT_MAINTENANCE_METHOD])),
                    AuditEvent.status_code == 429,
                )
            )
            or 0
        )

        return {
            "generated_at": generated_at,
            "lookback_hours": bounded_lookback,
            "datasets_total": datasets_total,
            "active_datasets": active_datasets,
            "pipelines_total": pipelines_total,
            "active_pipelines": active_pipelines,
            "ingestion_status_counts": ingestion_status_counts,
            "pipeline_run_status_counts": pipeline_run_status_counts,
            "quality_result_status_counts": quality_result_status_counts,
            "audit_event_counts": {
                "http": http_event_count,
                "worker": worker_event_count,
                "maintenance": maintenance_event_count,
                "http_errors": http_error_response_count,
                "rate_limited": rate_limited_request_count,
            },
            "recent_ingestion_failures": ObservabilityService._build_recent_ingestion_failures(
                session, limit=bounded_recent_limit
            ),
            "recent_pipeline_failures": ObservabilityService._build_recent_pipeline_failures(
                session, limit=bounded_recent_limit
            ),
        }

    @staticmethod
    def get_activity(
        session: Session,
        *,
        bucket: str = "hour",
        lookback_hours: int = 24,
        limit: int = 48,
    ) -> dict[str, Any]:
        normalized_bucket = normalize_observability_bucket(bucket)
        bounded_lookback = max(1, min(int(lookback_hours), 24 * 30))
        bounded_limit = max(1, min(int(limit), 1000))
        generated_at = ObservabilityService._utcnow()
        since = generated_at - timedelta(hours=bounded_lookback)

        rows: list[dict[str, Any]] = []

        audit_rows = session.execute(
            select(AuditEvent.created_at, AuditEvent.method).where(AuditEvent.created_at >= since)
        ).all()
        for created_at, method in audit_rows:
            if method == AUDIT_WORKER_METHOD:
                category = "worker_event"
            elif method == AUDIT_MAINTENANCE_METHOD:
                category = "maintenance_event"
            else:
                category = "http_event"
            rows.append({"created_at": created_at, "category": category})

        ingestion_failure_rows = session.execute(
            select(IngestionJob.created_at, IngestionJob.finished_at)
            .where(IngestionJob.status == "failed")
        ).all()
        for created_at, finished_at in ingestion_failure_rows:
            occurred_at = finished_at or created_at
            if occurred_at is not None and occurred_at >= since:
                rows.append({"created_at": occurred_at, "category": "ingestion_failure"})

        pipeline_failure_rows = session.execute(
            select(PipelineRun.created_at, PipelineRun.finished_at)
            .where(PipelineRun.status == "failed")
        ).all()
        for created_at, finished_at in pipeline_failure_rows:
            occurred_at = finished_at or created_at
            if occurred_at is not None and occurred_at >= since:
                rows.append({"created_at": occurred_at, "category": "pipeline_failure"})

        quality_failure_rows = session.execute(
            select(QualityResult.created_at).where(
                QualityResult.created_at >= since,
                QualityResult.status == "failed",
            )
        ).all()
        for (created_at,) in quality_failure_rows:
            rows.append({"created_at": created_at, "category": "quality_failure"})

        report = build_observability_activity_report(rows, bucket=normalized_bucket, limit=bounded_limit)
        return {
            "generated_at": generated_at,
            "lookback_hours": bounded_lookback,
            "bucket": normalized_bucket,
            **report,
        }
