from __future__ import annotations

from pathlib import Path


def test_pipeline_run_model_declares_run_ref_lookup_index() -> None:
    source = Path("src/data_platform/models/pipeline.py").read_text()

    assert "ix_pipeline_runs_pipeline_run_ref_created" in source
    assert '"run_ref", "created_at"' in source



def test_migration_registry_includes_run_ref_lookup_index_upgrade() -> None:
    source = Path("src/data_platform/migrations/manager.py").read_text()

    assert "def _add_pipeline_run_run_ref_lookup_index" in source
    assert 'index_name="ix_pipeline_runs_pipeline_run_ref_created"' in source
    assert 'columns=("pipeline_id", "run_ref", "created_at")' in source
    assert 'version=6' in source
    assert 'name="0006_pipeline_run_run_ref_lookup_index"' in source



def test_readme_mentions_pipeline_run_run_ref_history_lookup_index() -> None:
    source = Path("README.md").read_text()

    assert "pipeline run `run_ref` history filters" in source
    assert "composite lookup index for `run_ref` filtered pipeline-run history scans" in source
