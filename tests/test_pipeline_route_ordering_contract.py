from __future__ import annotations

from pathlib import Path


ROUTES_PATH = Path("src/data_platform/api/routes/pipelines.py")


def test_preflight_attempt_static_routes_precede_dynamic_id_route() -> None:
    text = ROUTES_PATH.read_text()

    count_index = text.index('@router.get("/pipelines/{pipeline_id}/preflight-attempts/count"')
    page_index = text.index('@router.get("/pipelines/{pipeline_id}/preflight-attempts/page"')
    detail_index = text.index('@router.get("/pipelines/{pipeline_id}/preflight-attempts/{preflight_attempt_id}"')

    assert count_index < detail_index
    assert page_index < detail_index
    assert 'static preflight-attempt routes above this dynamic id route' in text


def test_pipeline_run_static_routes_precede_dynamic_id_route() -> None:
    text = ROUTES_PATH.read_text()

    count_index = text.index('@router.get("/pipelines/{pipeline_id}/runs/count"')
    page_index = text.index('@router.get("/pipelines/{pipeline_id}/runs/page"')
    detail_index = text.index('@router.get("/pipelines/{pipeline_id}/runs/{run_id}"')

    assert count_index < detail_index
    assert page_index < detail_index
    assert 'static pipeline-run routes above this dynamic id route' in text
