from __future__ import annotations

from datetime import datetime

import sqlalchemy as sa
from sqlalchemy.orm import Mapped, mapped_column, relationship

from data_platform.db import Base
from data_platform.models.base import TimestampMixin


class PipelineDefinition(TimestampMixin, Base):
    __tablename__ = "pipeline_definitions"
    __table_args__ = (
        sa.Index("ix_pipeline_definitions_dataset_active", "dataset_id", "active"),
        sa.UniqueConstraint("dataset_id", "name", name="uq_pipeline_definitions_dataset_name"),
    )

    dataset_id: Mapped[str] = mapped_column(sa.ForeignKey("datasets.id", ondelete="CASCADE"), index=True, nullable=False)
    name: Mapped[str] = mapped_column(sa.String(255), nullable=False)
    source_layer: Mapped[str] = mapped_column(sa.String(16), nullable=False)
    target_layer: Mapped[str] = mapped_column(sa.String(16), nullable=False)
    engine: Mapped[str] = mapped_column(sa.String(32), nullable=False)
    definition_json: Mapped[dict] = mapped_column(sa.JSON, default=dict, nullable=False)
    active: Mapped[bool] = mapped_column(sa.Boolean, default=True, nullable=False)

    dataset: Mapped["Dataset"] = relationship(back_populates="pipelines")
    runs: Mapped[list["PipelineRun"]] = relationship(back_populates="pipeline", cascade="all, delete-orphan")
    preflight_attempts: Mapped[list["PipelinePreflightAttempt"]] = relationship(
        back_populates="pipeline", cascade="all, delete-orphan"
    )


class PipelineRun(TimestampMixin, Base):
    __tablename__ = "pipeline_runs"
    __table_args__ = (
        sa.Index("ix_pipeline_runs_pipeline_created", "pipeline_id", "created_at"),
        sa.Index("ix_pipeline_runs_pipeline_status_created", "pipeline_id", "status", "created_at"),
        sa.Index("ix_pipeline_runs_pipeline_ingestion_created", "pipeline_id", "ingestion_job_id", "created_at"),
        sa.Index("ix_pipeline_runs_pipeline_run_ref_created", "pipeline_id", "run_ref", "created_at"),
    )

    pipeline_id: Mapped[str | None] = mapped_column(sa.ForeignKey("pipeline_definitions.id", ondelete="CASCADE"), index=True)
    dataset_id: Mapped[str | None] = mapped_column(sa.ForeignKey("datasets.id", ondelete="CASCADE"), index=True)
    ingestion_job_id: Mapped[str | None] = mapped_column(sa.ForeignKey("ingestion_jobs.id", ondelete="SET NULL"), index=True)

    status: Mapped[str] = mapped_column(sa.String(32), index=True, nullable=False)
    run_ref: Mapped[str | None] = mapped_column(sa.String(255))
    metrics_json: Mapped[dict] = mapped_column(sa.JSON, default=dict, nullable=False)
    error_message: Mapped[str | None] = mapped_column(sa.Text)
    started_at: Mapped[datetime | None] = mapped_column(sa.DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(sa.DateTime(timezone=True))

    pipeline: Mapped[PipelineDefinition | None] = relationship(back_populates="runs")


class PipelinePreflightAttempt(TimestampMixin, Base):
    __tablename__ = "pipeline_preflight_attempts"
    __table_args__ = (
        sa.Index("ix_pipeline_preflight_attempts_pipeline_created", "pipeline_id", "created_at"),
        sa.Index(
            "ix_pipeline_preflight_attempts_pipeline_ingestion_created",
            "pipeline_id",
            "ingestion_job_id",
            "created_at",
        ),
        sa.Index(
            "ix_pipeline_preflight_attempts_pipeline_request_kind_created",
            "pipeline_id",
            "request_kind",
            "created_at",
        ),
        sa.Index(
            "ix_pipeline_preflight_attempts_pipeline_run_ref_created",
            "pipeline_id",
            "run_ref",
            "created_at",
        ),
    )

    pipeline_id: Mapped[str] = mapped_column(
        sa.ForeignKey("pipeline_definitions.id", ondelete="CASCADE"), index=True, nullable=False
    )
    dataset_id: Mapped[str] = mapped_column(sa.ForeignKey("datasets.id", ondelete="CASCADE"), index=True, nullable=False)
    ingestion_job_id: Mapped[str | None] = mapped_column(sa.ForeignKey("ingestion_jobs.id", ondelete="SET NULL"), index=True)

    request_kind: Mapped[str] = mapped_column(sa.String(32), nullable=False)
    run_ref: Mapped[str | None] = mapped_column(sa.String(255))
    metrics_json: Mapped[dict] = mapped_column(sa.JSON, default=dict, nullable=False)
    error_message: Mapped[str] = mapped_column(sa.Text, nullable=False)

    pipeline: Mapped[PipelineDefinition] = relationship(back_populates="preflight_attempts")


class QualityCheck(TimestampMixin, Base):
    __tablename__ = "quality_checks"
    __table_args__ = (
        sa.Index("ix_quality_checks_dataset_layer_active", "dataset_id", "layer", "active"),
        sa.UniqueConstraint("dataset_id", "layer", "name", name="uq_quality_checks_dataset_layer_name"),
    )

    dataset_id: Mapped[str] = mapped_column(sa.ForeignKey("datasets.id", ondelete="CASCADE"), index=True, nullable=False)
    name: Mapped[str] = mapped_column(sa.String(255), nullable=False)
    layer: Mapped[str] = mapped_column(sa.String(16), nullable=False)
    severity: Mapped[str] = mapped_column(sa.String(16), nullable=False)
    sql_expression: Mapped[str] = mapped_column(sa.Text, nullable=False)
    active: Mapped[bool] = mapped_column(sa.Boolean, default=True, nullable=False)

    dataset: Mapped["Dataset"] = relationship(back_populates="quality_checks")
    results: Mapped[list["QualityResult"]] = relationship(back_populates="quality_check", cascade="all, delete-orphan")


class QualityResult(TimestampMixin, Base):
    __tablename__ = "quality_results"
    __table_args__ = (
        sa.Index("ix_quality_results_dataset_layer_status_created", "dataset_id", "layer", "status", "created_at"),
    )

    dataset_id: Mapped[str] = mapped_column(sa.ForeignKey("datasets.id", ondelete="CASCADE"), index=True, nullable=False)
    ingestion_job_id: Mapped[str | None] = mapped_column(sa.ForeignKey("ingestion_jobs.id", ondelete="SET NULL"), index=True)
    quality_check_id: Mapped[str] = mapped_column(sa.ForeignKey("quality_checks.id", ondelete="CASCADE"), index=True, nullable=False)

    layer: Mapped[str] = mapped_column(sa.String(16), nullable=False)
    status: Mapped[str] = mapped_column(sa.String(16), nullable=False)
    observed_value: Mapped[str | None] = mapped_column(sa.String(255))
    details_json: Mapped[dict] = mapped_column(sa.JSON, default=dict, nullable=False)

    quality_check: Mapped[QualityCheck] = relationship(back_populates="results")
    ingestion_job: Mapped["IngestionJob | None"] = relationship(back_populates="quality_results")
