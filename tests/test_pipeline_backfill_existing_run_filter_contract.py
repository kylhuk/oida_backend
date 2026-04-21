from __future__ import annotations

from pathlib import Path



def test_backfill_request_schemas_expose_has_existing_run_filter() -> None:
    text = Path("src/data_platform/schemas/pipeline.py").read_text()

    assert "class CreatePipelineBackfillRunsRequest" in text
    assert "class CreatePipelineBackfillRunsPageRequest" in text
    assert "has_existing_run: bool | None = None" in text
    assert "skip_existing_runs cannot be combined with has_existing_run=true" in text
    assert "class PipelineBackfillRequestResponse" in text



def test_pipeline_service_forwards_has_existing_run_filter_to_backfill_selection() -> None:
    text = Path("src/data_platform/services/pipeline_service.py").read_text()

    assert "def create_pipeline_backfill_runs(" in text
    assert "has_existing_run=payload.has_existing_run" in text
    assert text.count("has_existing_run=payload.has_existing_run") >= 3



def test_backfill_request_snapshot_persists_has_existing_run_filter() -> None:
    text = Path("src/data_platform/utils/pipeline_definitions.py").read_text()

    assert "has_existing_run: bool | None = None" in text
    assert 'snapshot["has_existing_run"] = has_existing_run' in text
    assert 'has_existing_run = value.get("has_existing_run")' in text



def test_readme_documents_has_existing_run_backfill_filter() -> None:
    text = Path("README.md").read_text()

    assert "optional `has_existing_run` filter" in text
    assert "persist that selection inside each run's `backfill_request` snapshot" in text
