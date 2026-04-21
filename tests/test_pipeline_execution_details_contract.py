from __future__ import annotations

from pathlib import Path



def test_pipeline_run_response_schema_exposes_execution_details() -> None:
    text = Path("src/data_platform/schemas/pipeline.py").read_text()

    assert "class PipelineExecutionDetailsResponse" in text
    assert "execution_details: PipelineExecutionDetailsResponse | None = None" in text
    assert "output_row_count: int | None = None" in text
    assert "output_schema: list[dict[str, str]] = Field(default_factory=list)" in text



def test_pipeline_run_snapshot_extracts_execution_details() -> None:
    text = Path("src/data_platform/utils/pipeline_definitions.py").read_text()

    assert "def _extract_pipeline_execution_details" in text
    assert 'normalized_metrics.get("execution")' in text
    assert 'snapshot["execution_details"] = execution_details' in text
    assert 'details["output_row_count"] = output_row_count' in text
    assert 'details["target_schema_version"] = target_schema_version' in text



def test_readme_documents_first_class_execution_details() -> None:
    text = Path("README.md").read_text()

    assert "first-class `execution_details`" in text
    assert "`metrics_json.execution`" in text
    assert "task id, object URIs, row counts, output schema" in text
