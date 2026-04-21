from __future__ import annotations

from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

from data_platform.api.deps import get_db, require_api_key
from data_platform.api.routes.datasets import router as datasets_router
from data_platform.schemas.dataset import ApproveSchemaSnapshotRequest, CreateDatasetRequest, DatasetImportRequest
from data_platform.services.dataset_service import DatasetService


def _build_import_payload(exported: dict, *, target_slug: str, target_name: str) -> dict:
    payload = {
        "dataset": {
            "slug": target_slug,
            "name": target_name,
            "description": exported["dataset"].description,
            "schema_mode": exported["dataset"].schema_mode,
            "silver_sql": exported["dataset"].silver_sql,
            "gold_sql": exported["dataset"].gold_sql,
            "partitioning": exported["dataset"].partitioning,
            "serving_config": exported["dataset"].serving_config,
            "tags": exported["dataset"].tags,
            "gold_table_name": f"dataset_{target_slug}",
            "status": exported["dataset"].status,
        },
        "quality_rules": [],
        "data_products": [
            {
                "slug": target_slug if product["slug"] == exported["dataset"].slug else f"{product['slug']}_{target_slug}",
                "name": product["name"],
                "description": product["description"],
                "table_name": f"dataset_{target_slug}" if product["table_name"] == exported["dataset"].gold_table_name else product["table_name"],
                "config": product["config"],
                "is_default": product["is_default"],
                "current_version": product["current_version"],
                "versions": product["versions"],
            }
            for product in exported["data_products"]
        ],
        "pipelines": exported["pipelines"],
        "schema_snapshots": [
            {
                "layer": snapshot.layer,
                "version": snapshot.version,
                "fingerprint": snapshot.fingerprint,
                "schema_json": snapshot.schema_json,
                "approval": None
                if snapshot.approval is None
                else {
                    "approved_by": snapshot.approval.approved_by,
                    "note": snapshot.approval.note,
                },
            }
            for snapshot in exported["schema_snapshots"]
        ],
    }
    return payload


def _build_api_client(db_session):
    app = FastAPI()
    app.include_router(datasets_router)

    def _get_db():
        yield db_session

    app.dependency_overrides[get_db] = _get_db
    app.dependency_overrides[require_api_key] = lambda: SimpleNamespace(
        scopes=["datasets:read", "datasets:write"]
    )
    return TestClient(app)



def test_schema_approval_routes_list_pending_and_approve(db_session):
    dataset = DatasetService.create_dataset(db_session, CreateDatasetRequest(slug="orders", name="Orders"))
    DatasetService.save_schema_snapshot(db_session, dataset, "gold", [{"name": "id", "type": "BIGINT"}])
    DatasetService.save_schema_snapshot(
        db_session,
        dataset,
        "gold",
        [{"name": "id", "type": "BIGINT"}, {"name": "amount", "type": "DOUBLE"}],
    )

    with _build_api_client(db_session) as api_client:
        listed = api_client.get("/v1/datasets/orders/schemas")
        assert listed.status_code == 200
        assert [item["approval"] for item in listed.json()] == [None, None]

        approved = api_client.post(
            "/v1/datasets/orders/schemas/gold/1/approve",
            json={"approved_by": "schema-admin", "note": "Promoted to production"},
        )
        assert approved.status_code == 201
        assert approved.json()["layer"] == "gold"
        assert approved.json()["version"] == 1
        assert approved.json()["approved_by"] == "schema-admin"
        assert approved.json()["note"] == "Promoted to production"

        duplicate = api_client.post(
            "/v1/datasets/orders/schemas/gold/1/approve",
            json={"approved_by": "schema-admin"},
        )
        assert duplicate.status_code == 409

        pending = api_client.get("/v1/datasets/orders/schemas/pending?layer=gold")
        assert pending.status_code == 200
        assert [(item["layer"], item["version"]) for item in pending.json()] == [("gold", 2)]

        approvals = api_client.get("/v1/datasets/orders/schemas/approvals?layer=gold")
        assert approvals.status_code == 200
        assert [(item["layer"], item["version"], item["approved_by"]) for item in approvals.json()] == [
            ("gold", 1, "schema-admin")
        ]

        refreshed = api_client.get("/v1/datasets/orders/schemas?layer=gold")
        assert refreshed.status_code == 200
        payload = refreshed.json()
        assert payload[0]["approval"] is None
        assert payload[1]["approval"]["approved_by"] == "schema-admin"



def test_dataset_import_preserves_schema_approval_workflow_metadata(db_session):
    dataset = DatasetService.create_dataset(db_session, CreateDatasetRequest(slug="orders", name="Orders"))
    DatasetService.save_schema_snapshot(db_session, dataset, "gold", [{"name": "id", "type": "BIGINT"}])
    approval = DatasetService.approve_schema_snapshot(
        db_session,
        dataset,
        "gold",
        1,
        payload=ApproveSchemaSnapshotRequest(approved_by="schema-admin", note="Approved"),
    )
    assert approval.approved_by == "schema-admin"

    exported = DatasetService.export_dataset(db_session, dataset, include_schema_snapshots=True)
    payload = _build_import_payload(exported, target_slug="orders_copy", target_name="Orders Copy")

    imported = DatasetService.import_dataset(db_session, DatasetImportRequest.model_validate(payload))

    assert imported["dataset"].slug == "orders_copy"
    assert len(imported["schema_snapshots"]) == 1
    imported_snapshot = imported["schema_snapshots"][0]
    assert imported_snapshot.layer == "gold"
    assert imported_snapshot.version == 1
    assert imported_snapshot.approval is not None
    assert imported_snapshot.approval.approved_by == "schema-admin"
    assert imported_snapshot.approval.note == "Approved"
