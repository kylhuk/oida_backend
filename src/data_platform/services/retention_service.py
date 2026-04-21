from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from data_platform.metadata_retention import (
    MetadataCleanupReport,
    MetadataCleanupTableResult,
    MetadataRetentionPolicy,
    retention_cutoff,
    utcnow,
)

if TYPE_CHECKING:  # pragma: no cover
    from sqlalchemy.orm import Session


class MetadataRetentionService:
    @staticmethod
    def plan(
        session: Session,
        policies: tuple[MetadataRetentionPolicy, ...],
        *,
        now: datetime | None = None,
    ) -> MetadataCleanupReport:
        return MetadataRetentionService._collect(session, policies, apply=False, now=now)

    @staticmethod
    def apply(
        session: Session,
        policies: tuple[MetadataRetentionPolicy, ...],
        *,
        now: datetime | None = None,
    ) -> MetadataCleanupReport:
        return MetadataRetentionService._collect(session, policies, apply=True, now=now)

    @staticmethod
    def _collect(
        session: Session,
        policies: tuple[MetadataRetentionPolicy, ...],
        *,
        apply: bool,
        now: datetime | None,
    ) -> MetadataCleanupReport:
        from sqlalchemy import delete, func, select

        reference_now = now or utcnow()
        rows: list[MetadataCleanupTableResult] = []

        for policy in policies:
            model = MetadataRetentionService._model_for_policy(policy)
            cutoff_dt = retention_cutoff(reference_now, policy.retention_days)
            count_query = select(func.count()).select_from(model).where(model.created_at < cutoff_dt)
            matched_rows = int(session.scalar(count_query) or 0)
            deleted_rows = 0
            if apply and matched_rows:
                delete_stmt = delete(model).where(model.created_at < cutoff_dt)
                result = session.execute(delete_stmt)
                deleted_rows = matched_rows if result.rowcount is None else int(result.rowcount)
            rows.append(
                MetadataCleanupTableResult(
                    name=policy.name,
                    table_name=policy.table_name,
                    retention_days=policy.retention_days,
                    cutoff=cutoff_dt.isoformat(),
                    matched_rows=matched_rows,
                    deleted_rows=deleted_rows,
                    description=policy.description,
                )
            )

        if apply:
            session.commit()
        return MetadataCleanupReport(
            applied=apply,
            generated_at=reference_now.isoformat(),
            tables=rows,
        )

    @staticmethod
    def _model_for_policy(policy: MetadataRetentionPolicy):
        from data_platform.models.ingestion import IngestionJob
        from data_platform.models.pipeline import PipelinePreflightAttempt, PipelineRun, QualityResult

        mapping = {
            "pipeline_runs": PipelineRun,
            "pipeline_preflight_attempts": PipelinePreflightAttempt,
            "quality_results": QualityResult,
            "ingestion_jobs": IngestionJob,
        }
        try:
            return mapping[policy.name]
        except KeyError as exc:  # pragma: no cover - defensive branch
            raise ValueError(f"Unsupported metadata retention policy: {policy.name}") from exc
