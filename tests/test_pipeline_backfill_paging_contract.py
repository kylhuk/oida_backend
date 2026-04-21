from __future__ import annotations

from pathlib import Path


def test_pipeline_backfill_page_request_schema_is_exposed() -> None:
    text = Path("src/data_platform/schemas/pipeline.py").read_text()

    assert "class CreatePipelineBackfillRunsPageRequest" in text
    assert "cursor: str | None = None" in text
    assert "class PipelineBackfillRunsPageResponse" in text
    assert "items: list[PipelineRunResponse] = Field(default_factory=list)" in text


def test_pipeline_backfill_page_route_is_exposed() -> None:
    text = Path("src/data_platform/api/routes/pipelines.py").read_text()

    assert '"/pipelines/{pipeline_id}/runs/backfill/page"' in text
    assert "CreatePipelineBackfillRunsPageRequest" in text
    assert "PipelineBackfillRunsPageResponse" in text
    assert "PipelineService.create_pipeline_backfill_runs_page(" in text


def test_pipeline_service_supports_checkpoint_paged_backfill_creation() -> None:
    text = Path("src/data_platform/services/pipeline_service.py").read_text()

    assert "def create_pipeline_backfill_runs_page" in text
    assert "PipelineService.list_pipeline_source_candidates_page(" in text
    assert "cursor=payload.cursor" in text
    assert "run_ref_prefix=payload.run_ref_prefix" in text
    assert "require_contract_compatible_schema=payload.require_contract_compatible_schema" in text
    assert '"next_cursor": page.get("next_cursor")' in text
    assert "def _materialize_pipeline_backfill_runs" in text


def test_readme_documents_checkpoint_paged_backfill_creation() -> None:
    text = Path("README.md").read_text()

    assert "POST /v1/pipelines/{pipeline_id}/runs/backfill/page" in text
    assert "opaque `next_cursor`" in text
    assert "without relying on offset drift" in text
    assert "requested `run_ref_prefix`" in text
    assert "`require_contract_compatible_schema` mode across pages" in text
    assert "run-ref naming prefixes" in text
