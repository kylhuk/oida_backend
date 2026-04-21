from pathlib import Path

SCHEMA_PATH = Path("src/data_platform/schemas/dataset.py")
ROUTES_PATH = Path("src/data_platform/api/routes/datasets.py")
SERVICE_PATH = Path("src/data_platform/services/dataset_service.py")
README_PATH = Path("README.md")


def test_dataset_import_schema_contract_exists():
    text = SCHEMA_PATH.read_text()
    assert "class DatasetImportRequest(BaseModel):" in text
    assert "class DatasetImportDatasetRequest(BaseModel):" in text
    assert "class SchemaSnapshotImportRequest(BaseModel):" in text
    assert "At most one imported data product can be marked as default." in text
    assert "schema_snapshots for layer '" in text



def test_dataset_import_route_and_service_are_documented_in_source():
    route_text = ROUTES_PATH.read_text()
    service_text = SERVICE_PATH.read_text()
    readme_text = README_PATH.read_text()

    assert '@router.post("/import", response_model=DatasetExportResponse, status_code=status.HTTP_201_CREATED)' in route_text
    assert "def import_dataset_definition(" in route_text
    assert "DatasetService.import_dataset(session, payload)" in route_text
    assert "def import_dataset(session: Session, payload: DatasetImportRequest) -> dict:" in service_text
    assert "POST /v1/datasets/import" in readme_text
    assert "dataset export and import endpoints" in readme_text
