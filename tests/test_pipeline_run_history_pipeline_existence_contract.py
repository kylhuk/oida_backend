from __future__ import annotations

from pathlib import Path


def _route_section(text: str, start_marker: str, end_marker: str) -> str:
    return text.split(start_marker, 1)[1].split(end_marker, 1)[0]


def test_pipeline_run_history_routes_require_existing_pipeline() -> None:
    text = Path("src/data_platform/api/routes/pipelines.py").read_text()

    count_section = _route_section(
        text,
        'def count_pipeline_runs(',
        '@router.get("/pipelines/{pipeline_id}/runs/page"',
    )
    page_section = _route_section(
        text,
        'def list_pipeline_runs_page(',
        '@router.get("/pipelines/{pipeline_id}/runs/{run_id}"',
    )
    list_section = text.split('def list_pipeline_runs(', 1)[1]

    for section in (count_section, page_section, list_section):
        assert 'pipeline = PipelineService.get_pipeline(session, pipeline_id)' in section
        assert 'if not pipeline:' in section
        assert 'raise HTTPException(status_code=404, detail="Pipeline not found.")' in section



def test_readme_documents_unknown_pipeline_behavior_for_run_history() -> None:
    text = Path("README.md").read_text()

    assert "All three pipeline-run history endpoints now return `404`" in text
    assert "empty result set" in text
