from __future__ import annotations

from pathlib import Path


SCHEMA_PATH = Path("src/data_platform/schemas/catalog.py")
SERVICE_PATH = Path("src/data_platform/services/catalog_service.py")
ROUTES_PATH = Path("src/data_platform/api/routes/catalog.py")
API_MAIN_PATH = Path("src/data_platform/api/main.py")
README_PATH = Path("README.md")


def test_catalog_schema_models_exist() -> None:
    text = SCHEMA_PATH.read_text()

    assert "class CatalogCountResponse(BaseModel):" in text
    assert "class CatalogDatasetEntryResponse(BaseModel):" in text
    assert "CatalogDatasetResponse = CatalogDatasetEntryResponse" in text
    assert "tags: list[str] = Field(default_factory=list)" in text
    assert "latest_ingestion_created_at: datetime | None = None" in text
    assert "class CatalogPipelineEntryResponse(BaseModel):" in text
    assert "CatalogPipelineResponse = CatalogPipelineEntryResponse" in text
    assert "run_count: int = 0" in text
    assert "latest_run_status: str | None = None" in text
    assert "class CatalogSearchResponse(BaseModel):" in text


def test_catalog_service_exposes_searchable_dataset_and_pipeline_lists() -> None:
    text = SERVICE_PATH.read_text()

    assert "class CatalogService:" in text
    assert "def list_catalog_datasets(" in text
    assert "def count_catalog_datasets(" in text
    assert "Dataset.slug.ilike(pattern" in text
    assert "Dataset.gold_table_name.ilike(pattern" in text
    assert "sa.cast(Dataset.tags, sa.Text)" in text
    assert "latest_ingestion_created_at" in text
    assert "def list_catalog_pipelines(" in text
    assert "def count_catalog_pipelines(" in text
    assert "PipelineDefinition.name.ilike(pattern" in text
    assert "dataset_slug: str | None = None" in text
    assert "def _normalize_enum_filter" in text
    assert "run_count" in text
    assert "latest_run_finished_at" in text
    assert "def search_catalog(" in text


def test_catalog_routes_and_app_wiring_exist() -> None:
    route_text = ROUTES_PATH.read_text()
    api_main_text = API_MAIN_PATH.read_text()

    assert 'router = APIRouter(prefix="/v1/catalog", tags=["catalog"])' in route_text
    assert '@router.get("/search", response_model=CatalogSearchResponse)' in route_text
    assert '@router.get("/datasets/count", response_model=CatalogCountResponse)' in route_text
    assert '@router.get("/datasets", response_model=list[CatalogDatasetEntryResponse])' in route_text
    assert '@router.get("/pipelines/count", response_model=CatalogCountResponse)' in route_text
    assert '@router.get("/pipelines", response_model=list[CatalogPipelineEntryResponse])' in route_text
    assert "CatalogService.search_catalog" in route_text
    assert "CatalogService.count_catalog_datasets" in route_text
    assert "CatalogService.list_catalog_pipelines" in route_text
    assert '_load_router("data_platform.api.routes.catalog")' in api_main_text
    assert "app.include_router(catalog_router)" in api_main_text


def test_readme_documents_catalog_feature_and_endpoints() -> None:
    text = README_PATH.read_text()

    assert "searchable dataset and pipeline catalog endpoints" in text
    assert "GET /v1/catalog/search" in text
    assert "GET /v1/catalog/datasets" in text
    assert "GET /v1/catalog/datasets/count" in text
    assert "GET /v1/catalog/pipelines" in text
    assert "GET /v1/catalog/pipelines/count" in text
    assert "latest ingestion timestamp" in text
    assert "latest-run summary fields" in text
