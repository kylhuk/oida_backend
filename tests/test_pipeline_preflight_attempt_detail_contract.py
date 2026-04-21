from __future__ import annotations

import ast
from pathlib import Path


ROUTES_PATH = Path("src/data_platform/api/routes/pipelines.py")
SERVICE_PATH = Path("src/data_platform/services/pipeline_service.py")
README_PATH = Path("README.md")


def _function_source(path: Path, name: str) -> str:
    source = path.read_text()
    module = ast.parse(source)
    lines = source.splitlines()
    for node in module.body:
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return "\n".join(lines[node.lineno - 1 : node.end_lineno])
        if isinstance(node, ast.ClassDef):
            for child in node.body:
                if isinstance(child, ast.FunctionDef) and child.name == name:
                    return "\n".join(lines[child.lineno - 1 : child.end_lineno])
    raise AssertionError(f"Function {name!r} not found in {path}.")


def test_pipeline_service_exposes_single_preflight_attempt_lookup() -> None:
    source = _function_source(SERVICE_PATH, "get_pipeline_preflight_attempt")

    assert "session.get(PipelinePreflightAttempt, preflight_attempt_id)" in source
    assert "attempt.pipeline_id != pipeline_id" in source
    assert "return None" in source


def test_pipeline_preflight_attempt_detail_route_is_exposed() -> None:
    route_source = ROUTES_PATH.read_text()
    function_source = _function_source(ROUTES_PATH, "get_pipeline_preflight_attempt")

    assert '"/pipelines/{pipeline_id}/preflight-attempts/{preflight_attempt_id}"' in route_source
    assert "PipelineService.get_pipeline(session, pipeline_id)" in function_source
    assert 'raise HTTPException(status_code=404, detail="Pipeline not found.")' in function_source
    assert "PipelineService.get_pipeline_preflight_attempt(session, pipeline_id, preflight_attempt_id)" in function_source
    assert 'raise HTTPException(status_code=404, detail="Pipeline preflight attempt not found.")' in function_source
    assert "PipelinePreflightAttemptResponse.model_validate" in function_source


def test_readme_documents_single_preflight_attempt_endpoint() -> None:
    source = README_PATH.read_text()

    assert "GET /v1/pipelines/{pipeline_id}/preflight-attempts/{preflight_attempt_id}" in source
    assert "fetch one directly" in source
    assert "returns one persisted rejected strict-preflight record by id" in source
