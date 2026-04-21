from __future__ import annotations

from pathlib import Path


def test_pipeline_execution_service_marks_executor_validation_failures_failed() -> None:
    text = Path("src/data_platform/services/pipeline_execution_service.py").read_text()

    assert "execution_started_at = run.started_at or datetime.now(timezone.utc)" in text
    assert "try:" in text
    assert "execution_plan = self._execution_plan_for_run(run)" in text
    assert "Dataset {pipeline.dataset_id!r} not found for pipeline {pipeline.id!r}." in text
    assert "failure_metrics = {" in text
    assert '"status": PipelineStatus.FAILED.value' in text
    assert "if source_object_uri is not None:" in text
    assert "PipelineService.transition_pipeline_run(" in text
    assert "UpdatePipelineRunStatusRequest(status=PipelineStatus.FAILED.value, error_message=str(exc))" in text



def test_pipeline_execution_runtime_test_covers_invalid_execution_plan_failure() -> None:
    text = Path("tests/test_pipeline_execution_service.py").read_text()

    assert "def test_execute_next_run_marks_run_failed_when_persisted_execution_plan_is_invalid" in text
    assert 'planned.metrics_json = {"execution_plan": {"executable": True}}' in text
    assert 'assert planned.status == "failed"' in text
    assert 'assert execution["status"] == "failed"' in text



def test_readme_documents_executor_validation_failures() -> None:
    text = Path("README.md").read_text()

    assert "Executor-side validation failures also transition the claimed run to `failed`" in text
