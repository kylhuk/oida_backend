from __future__ import annotations

from pathlib import Path

MODELS_PATH = Path("src/data_platform/models/dataset.py")
MODELS_INIT_PATH = Path("src/data_platform/models/__init__.py")
MIGRATIONS_PATH = Path("src/data_platform/migrations/manager.py")
SCHEMAS_PATH = Path("src/data_platform/schemas/dataset.py")
SERVICE_PATH = Path("src/data_platform/services/dataset_service.py")
ROUTES_PATH = Path("src/data_platform/api/routes/datasets.py")
UTILS_PATH = Path("src/data_platform/utils/data_product_versions.py")
README_PATH = Path("README.md")


def test_data_product_versioning_model_and_exports_exist() -> None:
    models = MODELS_PATH.read_text()
    models_init = MODELS_INIT_PATH.read_text()

    assert 'current_version: Mapped[int] = mapped_column(sa.Integer, default=1, nullable=False)' in models
    assert 'class DataProductVersion(TimestampMixin, Base):' in models
    assert 'uq_data_product_versions_product_version' in models
    assert 'ix_data_product_versions_product_created' in models
    assert 'ix_data_product_versions_dataset_product_version' in models
    assert 'versions: Mapped[list["DataProductVersion"]] = relationship(' in models
    assert 'DataProductVersion' in models_init


def test_data_product_versioning_migration_replays_column_and_history_table() -> None:
    text = MIGRATIONS_PATH.read_text()

    assert 'def _add_data_product_versioning(conn: Connection) -> None:' in text
    assert 'ALTER TABLE data_products ADD COLUMN current_version INTEGER NOT NULL DEFAULT 1' in text
    assert 'UPDATE data_products SET current_version = 1 WHERE current_version IS NULL' in text
    assert 'table_name="data_product_versions"' in text
    assert 'index_name="ix_data_product_versions_product_created"' in text
    assert 'index_name="ix_data_product_versions_dataset_product_version"' in text
    assert 'product_table.c.current_version' in text
    assert 'version=int(row["current_version"] or 1),' in text
    assert 'version=9' in text
    assert 'name="0009_data_product_versioning"' in text


def test_data_product_versioning_schema_and_import_export_contract_exist() -> None:
    text = SCHEMAS_PATH.read_text()

    assert 'class UpdateDataProductRequest(BaseModel):' in text
    assert 'class DataProductVersionResponse(BaseModel):' in text
    assert 'class DataProductVersionedResponse(DataProductResponse):' in text
    assert 'current_version: int = 1' in text
    assert 'data_products: list[DataProductVersionedResponse] = Field(default_factory=list)' in text
    assert 'class DataProductVersionImportRequest(BaseModel):' in text
    assert 'versions: list[DataProductVersionImportRequest] = Field(default_factory=list)' in text
    assert 'current_version greater than 1 requires imported data product versions.' in text
    assert 'current_version must equal the latest imported data product version.' in text
    assert 'The latest imported data product version must match the imported current data product state.' in text


def test_data_product_versioning_helpers_service_and_routes_exist() -> None:
    utils = UTILS_PATH.read_text()
    service = SERVICE_PATH.read_text()
    routes = ROUTES_PATH.read_text()

    assert 'def build_data_product_version_snapshot(' in utils
    assert 'def data_product_version_matches_current(' in utils
    assert 'def validate_contiguous_data_product_versions(' in utils

    assert 'from data_platform.utils.data_product_versions import build_data_product_version_snapshot' in service
    assert 'def _unset_existing_default_data_products(' in service
    assert 'def _record_data_product_version(' in service
    assert 'def build_data_product_export_response(session: Session, product: DataProduct) -> dict:' in service
    assert 'payload["versions"] = [' in service
    assert 'def _create_data_product_record(' in service
    assert 'versions=[' in service
    assert 'current_version=product.current_version,' in service
    assert 'product.current_version = version_number' in service
    assert '_create_data_product_version' not in service

    assert '@router.get("/{dataset_slug}/data-products/{product_slug}", response_model=DataProductResponse)' in routes
    assert '@router.patch("/{dataset_slug}/data-products/{product_slug}", response_model=DataProductResponse)' in routes
    assert '@router.get("/{dataset_slug}/data-products/{product_slug}/versions", response_model=list[DataProductVersionResponse])' in routes
    assert '@router.get("/{dataset_slug}/data-products/{product_slug}/versions/{version}", response_model=DataProductVersionResponse)' in routes
    assert 'DataProductVersionedResponse.model_validate(item)' in routes
    assert 'DatasetService.build_data_product_response(product)' in routes
    assert 'DatasetService.build_data_product_version_response(item)' in routes


def test_readme_documents_data_product_versioning_feature() -> None:
    text = README_PATH.read_text()

    assert 'data-product version history with current-version tracking' in text
    assert 'Exported data products now include immutable version history' in text
    assert 'Imported data products also preserve their exported `current_version`' in text
    assert 'Data products now also keep first-class version history.' in text
    assert 'GET /v1/datasets/{dataset_slug}/data-products/{product_slug}' in text
    assert 'PATCH /v1/datasets/{dataset_slug}/data-products/{product_slug}' in text
    assert 'GET /v1/datasets/{dataset_slug}/data-products/{product_slug}/versions' in text
    assert 'GET /v1/datasets/{dataset_slug}/data-products/{product_slug}/versions/{version}' in text
