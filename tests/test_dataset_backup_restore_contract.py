from pathlib import Path

SCHEMA_PATH = Path("src/data_platform/schemas/dataset.py")
SERVICE_PATH = Path("src/data_platform/services/dataset_service.py")
ROUTES_PATH = Path("src/data_platform/api/routes/datasets.py")
README_PATH = Path("README.md")


def test_dataset_backup_restore_schema_contract_exists() -> None:
    text = SCHEMA_PATH.read_text()

    assert "class DatasetBackupResponse(BaseModel):" in text
    assert "class DatasetRestoreRequest(BaseModel):" in text
    assert "class DatasetRestoreResponse(BaseModel):" in text
    assert "skip_existing: bool = False" in text
    assert "datasets restore bundle contains duplicate dataset slug" in text



def test_dataset_backup_restore_service_and_routes_exist() -> None:
    service_text = SERVICE_PATH.read_text()
    route_text = ROUTES_PATH.read_text()

    assert "def export_dataset_backup(session: Session, include_schema_snapshots: bool = True) -> dict:" in service_text
    assert "def restore_dataset_backup(session: Session, payload: DatasetRestoreRequest) -> dict:" in service_text
    assert "def _import_dataset_definition(session: Session, payload: DatasetImportRequest) -> Dataset:" in service_text
    assert '@router.get("/backup", response_model=DatasetBackupResponse)' in route_text
    assert '@router.post("/restore", response_model=DatasetRestoreResponse, status_code=status.HTTP_201_CREATED)' in route_text
    assert "DatasetService.export_dataset_backup(" in route_text
    assert "DatasetService.restore_dataset_backup(session, payload)" in route_text
    assert route_text.index('@router.get("/backup", response_model=DatasetBackupResponse)') < route_text.index(
        '@router.get("/{dataset_slug}", response_model=DatasetResponse)'
    )
    assert route_text.index('@router.post("/restore", response_model=DatasetRestoreResponse, status_code=status.HTTP_201_CREATED)') < route_text.index(
        '@router.get("/{dataset_slug}", response_model=DatasetResponse)'
    )



def test_readme_documents_dataset_backup_restore_bundle_endpoints() -> None:
    text = README_PATH.read_text()

    assert "metadata backup and restore endpoints that round-trip all dataset configurations in one bundle" in text
    assert "GET /v1/datasets/backup" in text
    assert "POST /v1/datasets/restore" in text
    assert "skip already-existing dataset slugs" in text
