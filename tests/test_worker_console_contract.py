from __future__ import annotations

from pathlib import Path

AUDIT_SERVICE_PATH = Path("src/data_platform/services/audit_service.py")
INGESTION_ROUTES_PATH = Path("src/data_platform/api/routes/ingestions.py")
PIPELINE_ROUTES_PATH = Path("src/data_platform/api/routes/pipelines.py")
SCHEMA_PATH = Path("src/data_platform/schemas/console.py")
SERVICE_PATH = Path("src/data_platform/services/worker_console_service.py")
UTILS_PATH = Path("src/data_platform/utils/worker_console.py")
README_PATH = Path("README.md")


def test_worker_console_feature_is_wired_and_documented() -> None:
    audit_service_text = AUDIT_SERVICE_PATH.read_text()
    ingestion_routes_text = INGESTION_ROUTES_PATH.read_text()
    pipeline_routes_text = PIPELINE_ROUTES_PATH.read_text()
    schema_text = SCHEMA_PATH.read_text()
    service_text = SERVICE_PATH.read_text()
    utils_text = UTILS_PATH.read_text()
    readme_text = README_PATH.read_text()

    assert 'class WorkerConsoleEventResponse' in schema_text
    assert 'class WorkerConsolePageResponse' in schema_text

    assert 'def build_worker_console_entry(' in utils_text
    assert 'def build_worker_console_sse_frame(' in utils_text
    assert 'def stream_console(' in service_text
    assert 'def list_console_page(' in service_text

    assert 'def get_latest_event_cursor(' in audit_service_text
    assert 'def list_events_forward(' in audit_service_text

    assert '@router.get("/{job_id}/console", response_model=WorkerConsolePageResponse)' in ingestion_routes_text
    assert '@router.get("/{job_id}/console/tail")' in ingestion_routes_text
    assert '@router.get("/pipelines/{pipeline_id}/runs/{run_id}/console", response_model=WorkerConsolePageResponse)' in pipeline_routes_text
    assert '@router.get("/pipelines/{pipeline_id}/runs/{run_id}/console/tail")' in pipeline_routes_text

    assert 'worker run console and live tailing endpoints' in readme_text
    assert 'GET /v1/ingestions/{job_id}/console' in readme_text
    assert 'GET /v1/pipelines/{pipeline_id}/runs/{run_id}/console/tail' in readme_text
    assert 'tail_cursor' in readme_text
