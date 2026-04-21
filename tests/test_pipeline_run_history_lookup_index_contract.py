from __future__ import annotations

from pathlib import Path


def test_pipeline_run_model_declares_history_lookup_index() -> None:
    source = Path("src/data_platform/models/pipeline.py").read_text()

    assert "ix_pipeline_runs_pipeline_created" in source
    assert '"pipeline_id", "created_at"' in source


def test_migration_registry_includes_pipeline_run_history_lookup_index_upgrade() -> None:
    source = Path("src/data_platform/migrations/manager.py").read_text()

    assert "def _add_pipeline_run_history_lookup_index" in source
    assert 'index_name="ix_pipeline_runs_pipeline_created"' in source
    assert 'columns=("pipeline_id", "created_at")' in source
    assert 'version=7' in source
    assert 'name="0007_pipeline_run_history_lookup_index"' in source


def test_readme_mentions_pipeline_run_history_lookup_index() -> None:
    source = Path("README.md").read_text()

    assert "general pipeline-run history scans" in source
    assert "general pipeline-run history index for unfiltered newest-first history scans" in source
