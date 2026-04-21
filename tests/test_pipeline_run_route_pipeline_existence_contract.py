from __future__ import annotations

import ast
from pathlib import Path


ROUTES_PATH = Path("src/data_platform/api/routes/pipelines.py")
README_PATH = Path("README.md")


def _function_source(name: str) -> str:
    source = ROUTES_PATH.read_text()
    module = ast.parse(source)
    lines = source.splitlines()
    for node in module.body:
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return "\n".join(lines[node.lineno - 1 : node.end_lineno])
    raise AssertionError(f"Function {name!r} not found in {ROUTES_PATH}.")


def test_pipeline_run_read_routes_check_parent_pipeline_existence() -> None:
    for function_name in [
        "count_pipeline_runs",
        "list_pipeline_runs_page",
        "get_pipeline_run",
        "list_pipeline_runs",
    ]:
        function_source = _function_source(function_name)
        assert "PipelineService.get_pipeline(session, pipeline_id)" in function_source
        assert 'raise HTTPException(status_code=404, detail="Pipeline not found.")' in function_source


def test_readme_documents_pipeline_run_history_404_behavior() -> None:
    text = README_PATH.read_text()

    assert "GET /v1/pipelines/{pipeline_id}/runs/count" in text
    assert "GET /v1/pipelines/{pipeline_id}/runs/page" in text
    assert "All three pipeline-run history endpoints now return `404`" in text
