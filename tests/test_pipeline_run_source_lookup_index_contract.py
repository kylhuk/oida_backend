from __future__ import annotations

import ast
from pathlib import Path


def _parse_module(relative_path: str) -> ast.Module:
    source = Path(relative_path).read_text()
    return ast.parse(source, filename=relative_path)


def test_pipeline_run_model_declares_source_lookup_index() -> None:
    module = _parse_module("src/data_platform/models/pipeline.py")
    source = Path("src/data_platform/models/pipeline.py").read_text()

    assert "ix_pipeline_runs_pipeline_ingestion_created" in source

    for node in module.body:
        if isinstance(node, ast.ClassDef) and node.name == "PipelineRun":
            segment = ast.get_source_segment(source, node) or ""
            assert 'sa.Index("ix_pipeline_runs_pipeline_ingestion_created", "pipeline_id", "ingestion_job_id", "created_at")' in segment
            return

    raise AssertionError("PipelineRun model definition not found.")


def test_migration_registry_includes_source_lookup_index_upgrade() -> None:
    source = Path("src/data_platform/migrations/manager.py").read_text()

    assert "def _add_pipeline_run_source_lookup_index" in source
    assert 'index_name="ix_pipeline_runs_pipeline_ingestion_created"' in source
    assert 'columns=("pipeline_id", "ingestion_job_id", "created_at")' in source
    assert 'version=2' in source
    assert 'name="0002_pipeline_run_source_lookup_index"' in source
