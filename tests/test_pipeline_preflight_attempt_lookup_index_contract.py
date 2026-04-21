from __future__ import annotations

from pathlib import Path


def test_pipeline_preflight_attempt_model_declares_filtered_lookup_indexes() -> None:
    source = Path("src/data_platform/models/pipeline.py").read_text()

    assert "ix_pipeline_preflight_attempts_pipeline_request_kind_created" in source
    assert '"request_kind",' in source
    assert "ix_pipeline_preflight_attempts_pipeline_run_ref_created" in source
    assert '"run_ref",' in source


def test_migration_registry_includes_preflight_attempt_lookup_index_upgrade() -> None:
    source = Path("src/data_platform/migrations/manager.py").read_text()

    assert "def _add_pipeline_preflight_attempt_lookup_indexes" in source
    assert 'index_name="ix_pipeline_preflight_attempts_pipeline_request_kind_created"' in source
    assert 'columns=("pipeline_id", "request_kind", "created_at")' in source
    assert 'index_name="ix_pipeline_preflight_attempts_pipeline_run_ref_created"' in source
    assert 'columns=("pipeline_id", "run_ref", "created_at")' in source
    assert 'version=5' in source
    assert 'name="0005_pipeline_preflight_attempt_lookup_indexes"' in source


def test_readme_mentions_preflight_attempt_audit_filter_indexes() -> None:
    source = Path("README.md").read_text()

    assert "pipeline preflight-attempt audit filters" in source
    assert "composite lookup indexes for `request_kind` and `run_ref` filtered audit scans" in source
