from __future__ import annotations

import pytest
import sqlalchemy as sa
from sqlalchemy.pool import StaticPool

from data_platform.db import Base, init_db
from data_platform.migrations.manager import BASELINE_APP_TABLES, MIGRATIONS, get_current_schema_version, schema_migrations
from data_platform.models import api_client, dataset, ingestion, pipeline  # noqa: F401


def make_engine() -> sa.Engine:
    return sa.create_engine(
        "sqlite+pysqlite:///:memory:",
        future=True,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )


def test_init_db_applies_baseline_migration_to_fresh_database() -> None:
    engine = make_engine()

    init_db(bind=engine)
    init_db(bind=engine)

    with engine.connect() as conn:
        tables = set(sa.inspect(conn).get_table_names())
        assert BASELINE_APP_TABLES <= tables
        assert schema_migrations.name in tables
        rows = conn.execute(sa.select(schema_migrations.c.version, schema_migrations.c.name)).all()

    expected = [(migration.version, migration.name) for migration in MIGRATIONS]
    assert rows == expected
    assert get_current_schema_version(engine) == MIGRATIONS[-1].version


def test_init_db_creates_pipeline_preflight_attempts_table_with_foreign_keys() -> None:
    engine = make_engine()

    init_db(bind=engine)

    inspector = sa.inspect(engine)
    foreign_keys = {
        (item["constrained_columns"][0], item["referred_table"], item["referred_columns"][0])
        for item in inspector.get_foreign_keys("pipeline_preflight_attempts")
    }

    assert (
        "pipeline_id",
        "pipeline_definitions",
        "id",
    ) in foreign_keys
    assert (
        "dataset_id",
        "datasets",
        "id",
    ) in foreign_keys
    assert (
        "ingestion_job_id",
        "ingestion_jobs",
        "id",
    ) in foreign_keys


def test_init_db_creates_schema_approvals_table_with_foreign_keys() -> None:
    engine = make_engine()

    init_db(bind=engine)

    inspector = sa.inspect(engine)
    foreign_keys = {
        (item["constrained_columns"][0], item["referred_table"], item["referred_columns"][0])
        for item in inspector.get_foreign_keys("schema_approvals")
    }

    assert (
        "dataset_id",
        "datasets",
        "id",
    ) in foreign_keys
    assert (
        "schema_snapshot_id",
        "schema_snapshots",
        "id",
    ) in foreign_keys


def test_init_db_stamps_legacy_bootstrapped_database() -> None:
    engine = make_engine()
    Base.metadata.create_all(engine)

    init_db(bind=engine)

    with engine.connect() as conn:
        rows = conn.execute(sa.select(schema_migrations.c.version, schema_migrations.c.name)).all()

    expected = [(migration.version, migration.name) for migration in MIGRATIONS]
    assert rows == expected
    assert get_current_schema_version(engine) == MIGRATIONS[-1].version


def test_init_db_rejects_partial_legacy_schema_without_migration_history() -> None:
    engine = make_engine()
    metadata = sa.MetaData()
    sa.Table(
        "datasets",
        metadata,
        sa.Column("id", sa.String(36), primary_key=True),
    )
    metadata.create_all(engine)

    with pytest.raises(RuntimeError, match="partially initialized"):
        init_db(bind=engine)
