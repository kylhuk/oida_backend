from __future__ import annotations

from pathlib import Path


def test_ingestion_job_model_declares_history_lookup_index() -> None:
    source = Path("src/data_platform/models/ingestion.py").read_text()

    assert "ix_ingestion_jobs_dataset_created" in source
    assert '"dataset_id", "created_at"' in source


def test_migration_registry_includes_ingestion_job_history_lookup_index_upgrade() -> None:
    source = Path("src/data_platform/migrations/manager.py").read_text()

    assert "def _add_ingestion_job_history_lookup_index" in source
    assert 'index_name="ix_ingestion_jobs_dataset_created"' in source
    assert 'columns=("dataset_id", "created_at")' in source
    assert 'version=8' in source
    assert 'name="0008_ingestion_job_history_lookup_index"' in source


def test_ingestion_service_uses_deterministic_newest_first_ordering() -> None:
    source = Path("src/data_platform/services/ingestion_service.py").read_text()

    assert "stmt = stmt.order_by(IngestionJob.created_at.desc(), IngestionJob.id.desc()).limit(limit).offset(offset)" in source


def test_readme_mentions_ingestion_history_lookup_index_and_ordering() -> None:
    source = Path("README.md").read_text()

    assert "dataset-filtered ingestion history scans now use a dedicated `(dataset_id, created_at)` lookup index" in source
    assert "`GET /v1/ingestions` preserves deterministic newest-first ordering" in source
