from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import uuid4
from typing import Callable

import sqlalchemy as sa
from sqlalchemy.engine import Connection, Engine


schema_migrations = sa.Table(
    "schema_migrations",
    sa.MetaData(),
    sa.Column("version", sa.Integer, primary_key=True, nullable=False),
    sa.Column("name", sa.String(255), nullable=False),
    sa.Column("applied_at", sa.DateTime(timezone=True), nullable=False),
)

MigrationUpgrade = Callable[[Connection], None]


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    upgrade: MigrationUpgrade
    app_tables: frozenset[str] = frozenset()


# Keep the baseline migration frozen here so future model changes do not silently
# rewrite migration history.
def _baseline_metadata() -> sa.MetaData:
    metadata = sa.MetaData()

    datasets = sa.Table(
        "datasets",
        metadata,
        sa.Column("id", sa.String(36), primary_key=True, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("slug", sa.String(120), nullable=False),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("description", sa.Text),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("schema_mode", sa.String(32), nullable=False),
        sa.Column("silver_sql", sa.Text),
        sa.Column("gold_sql", sa.Text),
        sa.Column("partitioning", sa.JSON, nullable=False),
        sa.Column("serving_config", sa.JSON, nullable=False),
        sa.Column("tags", sa.JSON, nullable=False),
        sa.Column("gold_table_name", sa.String(255), nullable=False),
        sa.Column("latest_raw_schema_fingerprint", sa.String(64)),
        sa.Column("latest_silver_schema_fingerprint", sa.String(64)),
        sa.Column("latest_gold_schema_fingerprint", sa.String(64)),
        sa.UniqueConstraint("slug", name="uq_datasets_slug"),
        sa.UniqueConstraint("gold_table_name", name="uq_datasets_gold_table_name"),
    )
    sa.Index("ix_datasets_slug", datasets.c.slug)

    schema_snapshots = sa.Table(
        "schema_snapshots",
        metadata,
        sa.Column("id", sa.String(36), primary_key=True, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("dataset_id", sa.String(36), sa.ForeignKey("datasets.id", ondelete="CASCADE"), nullable=False),
        sa.Column("layer", sa.String(16), nullable=False),
        sa.Column("version", sa.Integer, nullable=False),
        sa.Column("fingerprint", sa.String(64), nullable=False),
        sa.Column("schema_json", sa.JSON, nullable=False),
        sa.UniqueConstraint("dataset_id", "layer", "version", name="uq_schema_snapshots_dataset_layer_version"),
    )
    sa.Index("ix_schema_snapshots_dataset_id", schema_snapshots.c.dataset_id)
    sa.Index("ix_schema_snapshots_layer", schema_snapshots.c.layer)
    sa.Index("ix_schema_snapshots_fingerprint", schema_snapshots.c.fingerprint)

    data_products = sa.Table(
        "data_products",
        metadata,
        sa.Column("id", sa.String(36), primary_key=True, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("dataset_id", sa.String(36), sa.ForeignKey("datasets.id", ondelete="CASCADE"), nullable=False),
        sa.Column("slug", sa.String(120), nullable=False),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("description", sa.Text),
        sa.Column("table_name", sa.String(255), nullable=False),
        sa.Column("config", sa.JSON, nullable=False),
        sa.Column("is_default", sa.Boolean, nullable=False),
        sa.UniqueConstraint("slug", name="uq_data_products_slug"),
    )
    sa.Index("ix_data_products_dataset_id", data_products.c.dataset_id)
    sa.Index("ix_data_products_slug", data_products.c.slug)
    sa.Index("ix_data_products_dataset_default", data_products.c.dataset_id, data_products.c.is_default)

    ingestion_jobs = sa.Table(
        "ingestion_jobs",
        metadata,
        sa.Column("id", sa.String(36), primary_key=True, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("dataset_id", sa.String(36), sa.ForeignKey("datasets.id", ondelete="CASCADE"), nullable=False),
        sa.Column("source_type", sa.String(32), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("filename", sa.String(255)),
        sa.Column("source_format", sa.String(32)),
        sa.Column("source_content_type", sa.String(255)),
        sa.Column("source_url", sa.Text),
        sa.Column("raw_object_uri", sa.Text),
        sa.Column("silver_object_uri", sa.Text),
        sa.Column("gold_object_uri", sa.Text),
        sa.Column("content_hash", sa.String(64)),
        sa.Column("idempotency_key", sa.String(255)),
        sa.Column("size_bytes", sa.BigInteger),
        sa.Column("row_count", sa.BigInteger),
        sa.Column("error_message", sa.Text),
        sa.Column("job_metadata", sa.JSON, nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True)),
        sa.Column("finished_at", sa.DateTime(timezone=True)),
        sa.UniqueConstraint("dataset_id", "idempotency_key", name="uq_ingestion_jobs_dataset_idempotency"),
    )
    sa.Index("ix_ingestion_jobs_dataset_id", ingestion_jobs.c.dataset_id)
    sa.Index("ix_ingestion_jobs_status", ingestion_jobs.c.status)
    sa.Index("ix_ingestion_jobs_content_hash", ingestion_jobs.c.content_hash)
    sa.Index("ix_ingestion_jobs_idempotency_key", ingestion_jobs.c.idempotency_key)
    sa.Index(
        "ix_ingestion_jobs_dataset_status_created",
        ingestion_jobs.c.dataset_id,
        ingestion_jobs.c.status,
        ingestion_jobs.c.created_at,
    )
    sa.Index("ix_ingestion_jobs_dataset_idempotency", ingestion_jobs.c.dataset_id, ingestion_jobs.c.idempotency_key)

    pipeline_definitions = sa.Table(
        "pipeline_definitions",
        metadata,
        sa.Column("id", sa.String(36), primary_key=True, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("dataset_id", sa.String(36), sa.ForeignKey("datasets.id", ondelete="CASCADE"), nullable=False),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("source_layer", sa.String(16), nullable=False),
        sa.Column("target_layer", sa.String(16), nullable=False),
        sa.Column("engine", sa.String(32), nullable=False),
        sa.Column("definition_json", sa.JSON, nullable=False),
        sa.Column("active", sa.Boolean, nullable=False),
        sa.UniqueConstraint("dataset_id", "name", name="uq_pipeline_definitions_dataset_name"),
    )
    sa.Index("ix_pipeline_definitions_dataset_id", pipeline_definitions.c.dataset_id)
    sa.Index("ix_pipeline_definitions_dataset_active", pipeline_definitions.c.dataset_id, pipeline_definitions.c.active)

    pipeline_runs = sa.Table(
        "pipeline_runs",
        metadata,
        sa.Column("id", sa.String(36), primary_key=True, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("pipeline_id", sa.String(36), sa.ForeignKey("pipeline_definitions.id", ondelete="CASCADE")),
        sa.Column("dataset_id", sa.String(36), sa.ForeignKey("datasets.id", ondelete="CASCADE")),
        sa.Column("ingestion_job_id", sa.String(36), sa.ForeignKey("ingestion_jobs.id", ondelete="SET NULL")),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("run_ref", sa.String(255)),
        sa.Column("metrics_json", sa.JSON, nullable=False),
        sa.Column("error_message", sa.Text),
        sa.Column("started_at", sa.DateTime(timezone=True)),
        sa.Column("finished_at", sa.DateTime(timezone=True)),
    )
    sa.Index("ix_pipeline_runs_pipeline_id", pipeline_runs.c.pipeline_id)
    sa.Index("ix_pipeline_runs_dataset_id", pipeline_runs.c.dataset_id)
    sa.Index("ix_pipeline_runs_ingestion_job_id", pipeline_runs.c.ingestion_job_id)
    sa.Index("ix_pipeline_runs_status", pipeline_runs.c.status)
    sa.Index(
        "ix_pipeline_runs_pipeline_status_created",
        pipeline_runs.c.pipeline_id,
        pipeline_runs.c.status,
        pipeline_runs.c.created_at,
    )

    quality_checks = sa.Table(
        "quality_checks",
        metadata,
        sa.Column("id", sa.String(36), primary_key=True, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("dataset_id", sa.String(36), sa.ForeignKey("datasets.id", ondelete="CASCADE"), nullable=False),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("layer", sa.String(16), nullable=False),
        sa.Column("severity", sa.String(16), nullable=False),
        sa.Column("sql_expression", sa.Text, nullable=False),
        sa.Column("active", sa.Boolean, nullable=False),
        sa.UniqueConstraint("dataset_id", "layer", "name", name="uq_quality_checks_dataset_layer_name"),
    )
    sa.Index("ix_quality_checks_dataset_id", quality_checks.c.dataset_id)
    sa.Index("ix_quality_checks_dataset_layer_active", quality_checks.c.dataset_id, quality_checks.c.layer, quality_checks.c.active)

    quality_results = sa.Table(
        "quality_results",
        metadata,
        sa.Column("id", sa.String(36), primary_key=True, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("dataset_id", sa.String(36), sa.ForeignKey("datasets.id", ondelete="CASCADE"), nullable=False),
        sa.Column("ingestion_job_id", sa.String(36), sa.ForeignKey("ingestion_jobs.id", ondelete="SET NULL")),
        sa.Column("quality_check_id", sa.String(36), sa.ForeignKey("quality_checks.id", ondelete="CASCADE"), nullable=False),
        sa.Column("layer", sa.String(16), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("observed_value", sa.String(255)),
        sa.Column("details_json", sa.JSON, nullable=False),
    )
    sa.Index("ix_quality_results_dataset_id", quality_results.c.dataset_id)
    sa.Index("ix_quality_results_ingestion_job_id", quality_results.c.ingestion_job_id)
    sa.Index("ix_quality_results_quality_check_id", quality_results.c.quality_check_id)
    sa.Index(
        "ix_quality_results_dataset_layer_status_created",
        quality_results.c.dataset_id,
        quality_results.c.layer,
        quality_results.c.status,
        quality_results.c.created_at,
    )

    api_clients = sa.Table(
        "api_clients",
        metadata,
        sa.Column("id", sa.String(36), primary_key=True, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("key_prefix", sa.String(16), nullable=False),
        sa.Column("key_hash", sa.String(64), nullable=False),
        sa.Column("scopes", sa.JSON, nullable=False),
        sa.Column("active", sa.Boolean, nullable=False),
    )
    sa.Index("ix_api_clients_key_prefix", api_clients.c.key_prefix)
    sa.Index("ix_api_clients_key_hash", api_clients.c.key_hash, unique=True)

    return metadata


BASELINE_METADATA = _baseline_metadata()
BASELINE_APP_TABLES = frozenset(
    name for name in BASELINE_METADATA.tables.keys() if name != schema_migrations.name
)


def _apply_baseline(conn: Connection) -> None:
    BASELINE_METADATA.create_all(conn, checkfirst=True)


def _create_index_if_missing(
    conn: Connection,
    *,
    table_name: str,
    index_name: str,
    columns: tuple[str, ...],
) -> None:
    inspector = sa.inspect(conn)
    existing = {index["name"] for index in inspector.get_indexes(table_name)}
    if index_name in existing:
        return

    metadata = sa.MetaData()
    table = sa.Table(table_name, metadata, autoload_with=conn)
    sa.Index(index_name, *(table.c[column] for column in columns)).create(conn)



def _add_pipeline_run_source_lookup_index(conn: Connection) -> None:
    _create_index_if_missing(
        conn,
        table_name="pipeline_runs",
        index_name="ix_pipeline_runs_pipeline_ingestion_created",
        columns=("pipeline_id", "ingestion_job_id", "created_at"),
    )


def _add_pipeline_run_history_lookup_index(conn: Connection) -> None:
    _create_index_if_missing(
        conn,
        table_name="pipeline_runs",
        index_name="ix_pipeline_runs_pipeline_created",
        columns=("pipeline_id", "created_at"),
    )




def _add_ingestion_job_history_lookup_index(conn: Connection) -> None:
    _create_index_if_missing(
        conn,
        table_name="ingestion_jobs",
        index_name="ix_ingestion_jobs_dataset_created",
        columns=("dataset_id", "created_at"),
    )


def _add_schema_snapshot_lookup_index(conn: Connection) -> None:
    _create_index_if_missing(
        conn,
        table_name="schema_snapshots",
        index_name="ix_schema_snapshots_dataset_layer_created",
        columns=("dataset_id", "layer", "created_at"),
    )



def _add_pipeline_preflight_attempts_table(conn: Connection) -> None:
    metadata = sa.MetaData()
    sa.Table(
        "pipeline_definitions",
        metadata,
        sa.Column("id", sa.String(36), primary_key=True, nullable=False),
    )
    sa.Table(
        "datasets",
        metadata,
        sa.Column("id", sa.String(36), primary_key=True, nullable=False),
    )
    sa.Table(
        "ingestion_jobs",
        metadata,
        sa.Column("id", sa.String(36), primary_key=True, nullable=False),
    )
    table = sa.Table(
        "pipeline_preflight_attempts",
        metadata,
        sa.Column("id", sa.String(36), primary_key=True, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "pipeline_id",
            sa.String(36),
            sa.ForeignKey("pipeline_definitions.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "dataset_id",
            sa.String(36),
            sa.ForeignKey("datasets.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "ingestion_job_id",
            sa.String(36),
            sa.ForeignKey("ingestion_jobs.id", ondelete="SET NULL"),
        ),
        sa.Column("request_kind", sa.String(32), nullable=False),
        sa.Column("run_ref", sa.String(255)),
        sa.Column("metrics_json", sa.JSON, nullable=False),
        sa.Column("error_message", sa.Text, nullable=False),
    )
    table.create(conn, checkfirst=True)
    _create_index_if_missing(
        conn,
        table_name="pipeline_preflight_attempts",
        index_name="ix_pipeline_preflight_attempts_pipeline_created",
        columns=("pipeline_id", "created_at"),
    )
    _create_index_if_missing(
        conn,
        table_name="pipeline_preflight_attempts",
        index_name="ix_pipeline_preflight_attempts_pipeline_ingestion_created",
        columns=("pipeline_id", "ingestion_job_id", "created_at"),
    )



def _add_pipeline_preflight_attempt_lookup_indexes(conn: Connection) -> None:
    _create_index_if_missing(
        conn,
        table_name="pipeline_preflight_attempts",
        index_name="ix_pipeline_preflight_attempts_pipeline_request_kind_created",
        columns=("pipeline_id", "request_kind", "created_at"),
    )
    _create_index_if_missing(
        conn,
        table_name="pipeline_preflight_attempts",
        index_name="ix_pipeline_preflight_attempts_pipeline_run_ref_created",
        columns=("pipeline_id", "run_ref", "created_at"),
    )



def _add_pipeline_run_run_ref_lookup_index(conn: Connection) -> None:
    _create_index_if_missing(
        conn,
        table_name="pipeline_runs",
        index_name="ix_pipeline_runs_pipeline_run_ref_created",
        columns=("pipeline_id", "run_ref", "created_at"),
    )



def _add_data_product_versioning(conn: Connection) -> None:
    inspector = sa.inspect(conn)
    existing_columns = {column["name"] for column in inspector.get_columns("data_products")}
    if "current_version" not in existing_columns:
        conn.execute(sa.text("ALTER TABLE data_products ADD COLUMN current_version INTEGER NOT NULL DEFAULT 1"))
    conn.execute(sa.text("UPDATE data_products SET current_version = 1 WHERE current_version IS NULL"))

    metadata = sa.MetaData()
    sa.Table(
        "datasets",
        metadata,
        sa.Column("id", sa.String(36), primary_key=True, nullable=False),
    )
    sa.Table(
        "data_products",
        metadata,
        sa.Column("id", sa.String(36), primary_key=True, nullable=False),
    )
    table = sa.Table(
        "data_product_versions",
        metadata,
        sa.Column("id", sa.String(36), primary_key=True, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("dataset_id", sa.String(36), sa.ForeignKey("datasets.id", ondelete="CASCADE"), nullable=False),
        sa.Column(
            "data_product_id",
            sa.String(36),
            sa.ForeignKey("data_products.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("version", sa.Integer, nullable=False),
        sa.Column("slug", sa.String(120), nullable=False),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("description", sa.Text),
        sa.Column("table_name", sa.String(255), nullable=False),
        sa.Column("config", sa.JSON, nullable=False),
        sa.Column("is_default", sa.Boolean, nullable=False),
        sa.UniqueConstraint("data_product_id", "version", name="uq_data_product_versions_product_version"),
    )
    table.create(conn, checkfirst=True)
    _create_index_if_missing(
        conn,
        table_name="data_product_versions",
        index_name="ix_data_product_versions_product_created",
        columns=("data_product_id", "created_at"),
    )
    _create_index_if_missing(
        conn,
        table_name="data_product_versions",
        index_name="ix_data_product_versions_dataset_product_version",
        columns=("dataset_id", "data_product_id", "version"),
    )

    product_table = sa.Table("data_products", sa.MetaData(), autoload_with=conn)
    existing_versions = {
        row[0]
        for row in conn.execute(sa.select(table.c.data_product_id)).all()
    }
    for row in conn.execute(
        sa.select(
            product_table.c.id,
            product_table.c.dataset_id,
            product_table.c.created_at,
            product_table.c.updated_at,
            product_table.c.slug,
            product_table.c.name,
            product_table.c.description,
            product_table.c.table_name,
            product_table.c.config,
            product_table.c.is_default,
            product_table.c.current_version,
        )
    ).mappings():
        if row["id"] in existing_versions:
            continue
        created_at = row["updated_at"] or row["created_at"] or datetime.now(timezone.utc)
        conn.execute(
            table.insert().values(
                id=str(uuid4()),
                created_at=created_at,
                updated_at=created_at,
                dataset_id=row["dataset_id"],
                data_product_id=row["id"],
                version=int(row["current_version"] or 1),
                slug=row["slug"],
                name=row["name"],
                description=row["description"],
                table_name=row["table_name"],
                config=row["config"] or {},
                is_default=row["is_default"],
            )
        )

    for row in conn.execute(
        sa.select(
            table.c.data_product_id,
            sa.func.max(table.c.version).label("max_version"),
        ).group_by(table.c.data_product_id)
    ).mappings():
        conn.execute(
            product_table.update()
            .where(product_table.c.id == row["data_product_id"])
            .values(current_version=int(row["max_version"] or 1))
        )






def _add_audit_events_table(conn: Connection) -> None:
    metadata = sa.MetaData()
    sa.Table(
        "api_clients",
        metadata,
        sa.Column("id", sa.String(36), primary_key=True, nullable=False),
    )
    table = sa.Table(
        "audit_events",
        metadata,
        sa.Column("id", sa.String(36), primary_key=True, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("request_id", sa.String(128)),
        sa.Column("method", sa.String(16), nullable=False),
        sa.Column("path", sa.String(1024), nullable=False),
        sa.Column("query_string", sa.Text),
        sa.Column("status_code", sa.Integer, nullable=False),
        sa.Column("api_client_id", sa.String(36), sa.ForeignKey("api_clients.id", ondelete="SET NULL")),
        sa.Column("api_client_name", sa.String(255)),
        sa.Column("api_key_prefix", sa.String(16)),
        sa.Column("client_ip", sa.String(64)),
        sa.Column("user_agent", sa.String(512)),
        sa.Column("details_json", sa.JSON, nullable=False),
    )
    table.create(conn, checkfirst=True)
    _create_index_if_missing(
        conn,
        table_name="audit_events",
        index_name="ix_audit_events_created",
        columns=("created_at",),
    )
    _create_index_if_missing(
        conn,
        table_name="audit_events",
        index_name="ix_audit_events_request_id_created",
        columns=("request_id", "created_at"),
    )
    _create_index_if_missing(
        conn,
        table_name="audit_events",
        index_name="ix_audit_events_api_key_prefix_created",
        columns=("api_key_prefix", "created_at"),
    )
    _create_index_if_missing(
        conn,
        table_name="audit_events",
        index_name="ix_audit_events_method_created",
        columns=("method", "created_at"),
    )
    _create_index_if_missing(
        conn,
        table_name="audit_events",
        index_name="ix_audit_events_status_created",
        columns=("status_code", "created_at"),
    )

def _add_schema_approval_workflow(conn: Connection) -> None:
    metadata = sa.MetaData()
    sa.Table(
        "datasets",
        metadata,
        sa.Column("id", sa.String(36), primary_key=True, nullable=False),
    )
    sa.Table(
        "schema_snapshots",
        metadata,
        sa.Column("id", sa.String(36), primary_key=True, nullable=False),
    )
    table = sa.Table(
        "schema_approvals",
        metadata,
        sa.Column("id", sa.String(36), primary_key=True, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("dataset_id", sa.String(36), sa.ForeignKey("datasets.id", ondelete="CASCADE"), nullable=False),
        sa.Column(
            "schema_snapshot_id",
            sa.String(36),
            sa.ForeignKey("schema_snapshots.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("layer", sa.String(16), nullable=False),
        sa.Column("version", sa.Integer, nullable=False),
        sa.Column("approved_by", sa.String(255), nullable=False),
        sa.Column("note", sa.Text),
        sa.Column("approved_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("schema_snapshot_id", name="uq_schema_approvals_schema_snapshot"),
    )
    table.create(conn, checkfirst=True)
    _create_index_if_missing(
        conn,
        table_name="schema_approvals",
        index_name="ix_schema_approvals_dataset_id",
        columns=("dataset_id",),
    )
    _create_index_if_missing(
        conn,
        table_name="schema_approvals",
        index_name="ix_schema_approvals_schema_snapshot_id",
        columns=("schema_snapshot_id",),
    )
    _create_index_if_missing(
        conn,
        table_name="schema_approvals",
        index_name="ix_schema_approvals_approved_at",
        columns=("approved_at",),
    )
    _create_index_if_missing(
        conn,
        table_name="schema_approvals",
        index_name="ix_schema_approvals_dataset_layer_approved",
        columns=("dataset_id", "layer", "approved_at"),
    )


MIGRATIONS: tuple[Migration, ...] = (
    Migration(
        version=1,
        name="0001_initial_metadata_schema",
        upgrade=_apply_baseline,
        app_tables=BASELINE_APP_TABLES,
    ),
    Migration(
        version=2,
        name="0002_pipeline_run_source_lookup_index",
        upgrade=_add_pipeline_run_source_lookup_index,
    ),
    Migration(
        version=3,
        name="0003_schema_snapshot_lookup_index",
        upgrade=_add_schema_snapshot_lookup_index,
    ),
    Migration(
        version=4,
        name="0004_pipeline_preflight_attempts",
        upgrade=_add_pipeline_preflight_attempts_table,
    ),
    Migration(
        version=5,
        name="0005_pipeline_preflight_attempt_lookup_indexes",
        upgrade=_add_pipeline_preflight_attempt_lookup_indexes,
    ),
    Migration(
        version=6,
        name="0006_pipeline_run_run_ref_lookup_index",
        upgrade=_add_pipeline_run_run_ref_lookup_index,
    ),
    Migration(
        version=7,
        name="0007_pipeline_run_history_lookup_index",
        upgrade=_add_pipeline_run_history_lookup_index,
    ),
    Migration(
        version=8,
        name="0008_ingestion_job_history_lookup_index",
        upgrade=_add_ingestion_job_history_lookup_index,
    ),
    Migration(
        version=9,
        name="0009_data_product_versioning",
        upgrade=_add_data_product_versioning,
    ),
    Migration(
        version=10,
        name="0010_schema_approval_workflow",
        upgrade=_add_schema_approval_workflow,
    ),
    Migration(
        version=11,
        name="0011_audit_events",
        upgrade=_add_audit_events_table,
    ),
)


def _ensure_migration_table(conn: Connection) -> None:
    schema_migrations.create(conn, checkfirst=True)


def _load_applied_versions(conn: Connection) -> set[int]:
    return {int(version) for version in conn.execute(sa.select(schema_migrations.c.version)).scalars().all()}


def _record_migration(conn: Connection, migration: Migration) -> None:
    conn.execute(
        schema_migrations.insert().values(
            version=migration.version,
            name=migration.name,
            applied_at=datetime.now(timezone.utc),
        )
    )


def _legacy_state(conn: Connection, expected_tables: frozenset[str]) -> frozenset[str]:
    inspector = sa.inspect(conn)
    existing = set(inspector.get_table_names())
    return frozenset(existing & expected_tables)


def _validate_migration_registry() -> None:
    versions = [migration.version for migration in MIGRATIONS]
    if versions != sorted(versions):
        raise RuntimeError("Schema migrations must be registered in ascending version order.")
    if len(versions) != len(set(versions)):
        raise RuntimeError("Schema migrations contain duplicate version numbers.")


def _apply_pending_migrations(conn: Connection) -> None:
    _validate_migration_registry()
    _ensure_migration_table(conn)

    applied = _load_applied_versions(conn)
    known = {migration.version for migration in MIGRATIONS}
    unknown = applied - known
    if unknown:
        raise RuntimeError(f"Database contains unknown schema migration versions: {sorted(unknown)}")

    if not applied and MIGRATIONS:
        baseline = MIGRATIONS[0]
        legacy_tables = _legacy_state(conn, baseline.app_tables)
        if legacy_tables == baseline.app_tables:
            _record_migration(conn, baseline)
            applied.add(baseline.version)
        elif legacy_tables:
            raise RuntimeError(
                "Database is partially initialized but unversioned. Existing application tables: "
                f"{sorted(legacy_tables)}. Run a manual repair before enabling migrations."
            )

    for migration in MIGRATIONS:
        if migration.version in applied:
            continue
        migration.upgrade(conn)
        _record_migration(conn, migration)
        applied.add(migration.version)


def run_migrations(bind: Engine | Connection) -> None:
    if isinstance(bind, Engine):
        with bind.begin() as conn:
            _apply_pending_migrations(conn)
        return
    if bind.in_transaction():
        _apply_pending_migrations(bind)
        return
    with bind.begin():
        _apply_pending_migrations(bind)


def get_current_schema_version(bind: Engine | Connection) -> int | None:
    def _read(conn: Connection) -> int | None:
        inspector = sa.inspect(conn)
        if schema_migrations.name not in inspector.get_table_names():
            return None
        return conn.execute(sa.select(sa.func.max(schema_migrations.c.version))).scalar_one_or_none()

    if isinstance(bind, Engine):
        with bind.connect() as conn:
            return _read(conn)
    return _read(bind)
