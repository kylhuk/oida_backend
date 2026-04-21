from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def _read(relative_path: str) -> str:
    return (REPO_ROOT / relative_path).read_text(encoding="utf-8")


def test_observability_schemas_service_and_routes_exist() -> None:
    schema_text = _read("src/data_platform/schemas/observability.py")
    service_text = _read("src/data_platform/services/observability_service.py")
    route_text = _read("src/data_platform/api/routes/observability.py")
    api_main_text = _read("src/data_platform/api/main.py")

    assert "class ObservabilitySummaryResponse(BaseModel):" in schema_text
    assert "class ObservabilityActivityResponse(BaseModel):" in schema_text
    assert "def get_summary(" in service_text
    assert "def get_activity(" in service_text
    assert 'router = APIRouter(prefix="/v1/observability", tags=["observability"])' in route_text
    assert '"/summary"' in route_text
    assert '"/activity"' in route_text
    assert 'require_scopes("audit:read")' in route_text
    assert 'data_platform.api.routes.observability' in api_main_text
    assert 'app.include_router(observability_router)' in api_main_text


def test_api_main_import_isolation_covers_observability_router() -> None:
    text = _read("tests/test_api_main_import_isolation.py")
    assert '"data_platform.api.routes.observability", "observability"' in text
    assert '"health", "audit", "observability", "catalog", "datasets", "ingestions", "gold", "pipelines"' in text


def test_readme_documents_observability_feature_and_endpoints() -> None:
    text = _read("README.md")
    assert "observability summary and activity endpoints" in text
    assert "GET /v1/observability/summary" in text
    assert "GET /v1/observability/activity" in text
