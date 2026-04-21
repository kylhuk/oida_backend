from __future__ import annotations

from datetime import datetime

import sqlalchemy as sa
from sqlalchemy.orm import Mapped, mapped_column, relationship

from data_platform.db import Base
from data_platform.models.base import TimestampMixin


class IngestionJob(TimestampMixin, Base):
    __tablename__ = "ingestion_jobs"
    __table_args__ = (
        sa.Index("ix_ingestion_jobs_dataset_created", "dataset_id", "created_at"),
        sa.Index("ix_ingestion_jobs_dataset_status_created", "dataset_id", "status", "created_at"),
        sa.Index("ix_ingestion_jobs_dataset_idempotency", "dataset_id", "idempotency_key"),
        sa.UniqueConstraint("dataset_id", "idempotency_key", name="uq_ingestion_jobs_dataset_idempotency"),
    )

    dataset_id: Mapped[str] = mapped_column(sa.ForeignKey("datasets.id", ondelete="CASCADE"), index=True, nullable=False)

    source_type: Mapped[str] = mapped_column(sa.String(32), nullable=False)
    status: Mapped[str] = mapped_column(sa.String(32), index=True, nullable=False)

    filename: Mapped[str | None] = mapped_column(sa.String(255))
    source_format: Mapped[str | None] = mapped_column(sa.String(32))
    source_content_type: Mapped[str | None] = mapped_column(sa.String(255))
    source_url: Mapped[str | None] = mapped_column(sa.Text)

    raw_object_uri: Mapped[str | None] = mapped_column(sa.Text)
    silver_object_uri: Mapped[str | None] = mapped_column(sa.Text)
    gold_object_uri: Mapped[str | None] = mapped_column(sa.Text)

    content_hash: Mapped[str | None] = mapped_column(sa.String(64), index=True)
    idempotency_key: Mapped[str | None] = mapped_column(sa.String(255), index=True)
    size_bytes: Mapped[int | None] = mapped_column(sa.BigInteger)
    row_count: Mapped[int | None] = mapped_column(sa.BigInteger)

    error_message: Mapped[str | None] = mapped_column(sa.Text)
    job_metadata: Mapped[dict] = mapped_column(sa.JSON, default=dict, nullable=False)

    started_at: Mapped[datetime | None] = mapped_column(sa.DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(sa.DateTime(timezone=True))

    dataset: Mapped["Dataset"] = relationship(back_populates="ingestions")
    quality_results: Mapped[list["QualityResult"]] = relationship(
        back_populates="ingestion_job",
        cascade="all, delete-orphan",
    )
