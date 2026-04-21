from __future__ import annotations

from pathlib import Path

SCHEMA_PATH = Path("src/data_platform/schemas/dataset.py")
SERVICE_PATH = Path("src/data_platform/services/dataset_service.py")
ROUTES_PATH = Path("src/data_platform/api/routes/datasets.py")
MODELS_PATH = Path("src/data_platform/models/dataset.py")
MIGRATIONS_PATH = Path("src/data_platform/migrations/manager.py")
README_PATH = Path("README.md")


def test_schema_approval_workflow_model_route_and_migration_exist() -> None:
    model_text = MODELS_PATH.read_text()
    route_text = ROUTES_PATH.read_text()
    migration_text = MIGRATIONS_PATH.read_text()

    assert "class SchemaApproval" in model_text
    assert 'router.get("/{dataset_slug}/schemas/approvals"' in route_text
    assert 'router.get("/{dataset_slug}/schemas/pending"' in route_text
    assert 'router.post("/{dataset_slug}/schemas/{layer}/{version}/approve"' in route_text
    assert 'name="0010_schema_approval_workflow"' in migration_text


def test_schema_approval_workflow_schema_and_service_helpers_exist() -> None:
    schema_text = SCHEMA_PATH.read_text()
    service_text = SERVICE_PATH.read_text()

    assert "class SchemaApprovalResponse" in schema_text
    assert "class ApproveSchemaSnapshotRequest" in schema_text
    assert "approval: SchemaApprovalResponse | None = None" in schema_text
    assert "class SchemaApprovalImportRequest" in schema_text
    assert "def list_schema_approvals(" in service_text
    assert "def list_pending_schema_snapshots(" in service_text
    assert "def approve_schema_snapshot(" in service_text


def test_readme_documents_schema_approval_workflow() -> None:
    text = README_PATH.read_text()

    assert "schema approval workflow" in text
    assert "GET /v1/datasets/{dataset_slug}/schemas/pending" in text
    assert "GET /v1/datasets/{dataset_slug}/schemas/approvals" in text
    assert "POST /v1/datasets/{dataset_slug}/schemas/{layer}/{version}/approve" in text
