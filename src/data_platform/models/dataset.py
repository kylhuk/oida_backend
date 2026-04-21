from __future__ import annotations

from datetime import datetime

import sqlalchemy as sa
from sqlalchemy.orm import Mapped, mapped_column, relationship

from data_platform.db import Base
from data_platform.models.base import TimestampMixin


class Dataset(TimestampMixin, Base):
    __tablename__ = "datasets"
    __table_args__ = (
        sa.UniqueConstraint("slug", name="uq_datasets_slug"),
        sa.UniqueConstraint("gold_table_name", name="uq_datasets_gold_table_name"),
    )

    slug: Mapped[str] = mapped_column(sa.String(120), index=True, nullable=False)
    name: Mapped[str] = mapped_column(sa.String(255), nullable=False)
    description: Mapped[str | None] = mapped_column(sa.Text)
    status: Mapped[str] = mapped_column(sa.String(32), default="active", nullable=False)

    schema_mode: Mapped[str] = mapped_column(sa.String(32), default="evolve", nullable=False)
    silver_sql: Mapped[str | None] = mapped_column(sa.Text)
    gold_sql: Mapped[str | None] = mapped_column(sa.Text)

    partitioning: Mapped[dict] = mapped_column(sa.JSON, default=dict, nullable=False)
    serving_config: Mapped[dict] = mapped_column(sa.JSON, default=dict, nullable=False)
    tags: Mapped[list[str]] = mapped_column(sa.JSON, default=list, nullable=False)

    gold_table_name: Mapped[str] = mapped_column(sa.String(255), nullable=False)

    latest_raw_schema_fingerprint: Mapped[str | None] = mapped_column(sa.String(64))
    latest_silver_schema_fingerprint: Mapped[str | None] = mapped_column(sa.String(64))
    latest_gold_schema_fingerprint: Mapped[str | None] = mapped_column(sa.String(64))

    schema_snapshots: Mapped[list["SchemaSnapshot"]] = relationship(
        back_populates="dataset",
        cascade="all, delete-orphan",
    )
    schema_approvals: Mapped[list["SchemaApproval"]] = relationship(
        back_populates="dataset",
        cascade="all, delete-orphan",
    )
    ingestions: Mapped[list["IngestionJob"]] = relationship(
        back_populates="dataset",
        cascade="all, delete-orphan",
    )
    data_products: Mapped[list["DataProduct"]] = relationship(
        back_populates="dataset",
        cascade="all, delete-orphan",
    )
    data_product_versions: Mapped[list["DataProductVersion"]] = relationship(
        back_populates="dataset",
        cascade="all, delete-orphan",
    )
    quality_checks: Mapped[list["QualityCheck"]] = relationship(
        back_populates="dataset",
        cascade="all, delete-orphan",
    )
    pipelines: Mapped[list["PipelineDefinition"]] = relationship(
        back_populates="dataset",
        cascade="all, delete-orphan",
    )


class SchemaSnapshot(TimestampMixin, Base):
    __tablename__ = "schema_snapshots"
    __table_args__ = (
        sa.UniqueConstraint("dataset_id", "layer", "version", name="uq_schema_snapshots_dataset_layer_version"),
        sa.Index("ix_schema_snapshots_dataset_layer_created", "dataset_id", "layer", "created_at"),
    )

    dataset_id: Mapped[str] = mapped_column(sa.ForeignKey("datasets.id", ondelete="CASCADE"), index=True, nullable=False)
    layer: Mapped[str] = mapped_column(sa.String(16), index=True, nullable=False)
    version: Mapped[int] = mapped_column(sa.Integer, nullable=False)
    fingerprint: Mapped[str] = mapped_column(sa.String(64), nullable=False, index=True)
    schema_json: Mapped[list[dict]] = mapped_column(sa.JSON, nullable=False)

    dataset: Mapped[Dataset] = relationship(back_populates="schema_snapshots")
    approval: Mapped["SchemaApproval | None"] = relationship(
        back_populates="schema_snapshot",
        cascade="all, delete-orphan",
        uselist=False,
    )


class SchemaApproval(TimestampMixin, Base):
    __tablename__ = "schema_approvals"
    __table_args__ = (
        sa.UniqueConstraint("schema_snapshot_id", name="uq_schema_approvals_schema_snapshot"),
        sa.Index("ix_schema_approvals_dataset_layer_approved", "dataset_id", "layer", "approved_at"),
    )

    dataset_id: Mapped[str] = mapped_column(sa.ForeignKey("datasets.id", ondelete="CASCADE"), index=True, nullable=False)
    schema_snapshot_id: Mapped[str] = mapped_column(
        sa.ForeignKey("schema_snapshots.id", ondelete="CASCADE"),
        index=True,
        nullable=False,
    )
    layer: Mapped[str] = mapped_column(sa.String(16), nullable=False)
    version: Mapped[int] = mapped_column(sa.Integer, nullable=False)
    approved_by: Mapped[str] = mapped_column(sa.String(255), nullable=False)
    note: Mapped[str | None] = mapped_column(sa.Text)
    approved_at: Mapped[datetime] = mapped_column(sa.DateTime(timezone=True), nullable=False, index=True)

    dataset: Mapped[Dataset] = relationship(back_populates="schema_approvals")
    schema_snapshot: Mapped[SchemaSnapshot] = relationship(back_populates="approval")


class DataProduct(TimestampMixin, Base):
    __tablename__ = "data_products"
    __table_args__ = (
        sa.UniqueConstraint("slug", name="uq_data_products_slug"),
        sa.Index("ix_data_products_dataset_default", "dataset_id", "is_default"),
    )

    dataset_id: Mapped[str] = mapped_column(sa.ForeignKey("datasets.id", ondelete="CASCADE"), index=True, nullable=False)
    slug: Mapped[str] = mapped_column(sa.String(120), index=True, nullable=False)
    name: Mapped[str] = mapped_column(sa.String(255), nullable=False)
    description: Mapped[str | None] = mapped_column(sa.Text)
    table_name: Mapped[str] = mapped_column(sa.String(255), nullable=False)
    config: Mapped[dict] = mapped_column(sa.JSON, default=dict, nullable=False)
    is_default: Mapped[bool] = mapped_column(sa.Boolean, default=False, nullable=False)
    current_version: Mapped[int] = mapped_column(sa.Integer, default=1, nullable=False)

    dataset: Mapped[Dataset] = relationship(back_populates="data_products")
    versions: Mapped[list["DataProductVersion"]] = relationship(
        back_populates="data_product",
        cascade="all, delete-orphan",
    )


class DataProductVersion(TimestampMixin, Base):
    __tablename__ = "data_product_versions"
    __table_args__ = (
        sa.UniqueConstraint("data_product_id", "version", name="uq_data_product_versions_product_version"),
        sa.Index("ix_data_product_versions_product_created", "data_product_id", "created_at"),
        sa.Index("ix_data_product_versions_dataset_product_version", "dataset_id", "data_product_id", "version"),
    )

    data_product_id: Mapped[str] = mapped_column(
        sa.ForeignKey("data_products.id", ondelete="CASCADE"),
        index=True,
        nullable=False,
    )
    dataset_id: Mapped[str] = mapped_column(sa.ForeignKey("datasets.id", ondelete="CASCADE"), index=True, nullable=False)
    version: Mapped[int] = mapped_column(sa.Integer, nullable=False)
    slug: Mapped[str] = mapped_column(sa.String(120), nullable=False)
    name: Mapped[str] = mapped_column(sa.String(255), nullable=False)
    description: Mapped[str | None] = mapped_column(sa.Text)
    table_name: Mapped[str] = mapped_column(sa.String(255), nullable=False)
    config: Mapped[dict] = mapped_column(sa.JSON, default=dict, nullable=False)
    is_default: Mapped[bool] = mapped_column(sa.Boolean, default=False, nullable=False)

    dataset: Mapped[Dataset] = relationship(back_populates="data_product_versions")
    data_product: Mapped[DataProduct] = relationship(back_populates="versions")
