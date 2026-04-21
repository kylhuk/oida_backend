from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

import pytest

from data_platform.api.deps import get_db, require_api_key
from data_platform.api.routes.datasets import router as datasets_router
from data_platform.schemas.dataset import CreateDataProductRequest, CreateDatasetRequest, QualityRuleCreate
from data_platform.schemas.pipeline import CreatePipelineDefinitionRequest
from data_platform.services.dataset_service import DatasetService
from data_platform.services.pipeline_service import PipelineService


@pytest.fixture()
def dataset_api_client(db_session):
    app = FastAPI()
    app.include_router(datasets_router)

    def _get_db():
        yield db_session

    app.dependency_overrides[get_db] = _get_db
    app.dependency_overrides[require_api_key] = lambda: type(
        "Auth",
        (),
        {"scopes": ["datasets:read", "datasets:write", "pipelines:read", "pipelines:write"]},
    )()

    with TestClient(app) as client:
        yield client



def test_import_dataset_route_recreates_exported_configuration(dataset_api_client, db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(
            slug="orders",
            name="Orders",
            quality_rules=[
                QualityRuleCreate(
                    name="gold_has_rows",
                    layer="gold",
                    severity="error",
                    sql_expression="SELECT TRUE AS passed",
                )
            ],
        ),
    )
    DatasetService.create_data_product(
        db_session,
        dataset,
        CreateDataProductRequest(
            slug="orders_dashboard",
            name="Orders Dashboard",
            table_name="orders_dashboard_gold",
        ),
    )
    DatasetService.save_schema_snapshot(db_session, dataset, "gold", [{"name": "id", "type": "BIGINT"}])
    PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="orders_refresh",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )

    exported = dataset_api_client.get("/v1/datasets/orders/export?include_schema_snapshots=true")
    assert exported.status_code == 200
    payload = exported.json()
    original_dataset_slug = payload["dataset"]["slug"]
    payload["dataset"]["slug"] = "orders_copy"
    payload["dataset"]["name"] = "Orders Copy"
    payload["dataset"]["gold_table_name"] = "dataset_orders_copy"
    for product in payload["data_products"]:
        if product["slug"] == original_dataset_slug:
            product["slug"] = "orders_copy"
        else:
            product["slug"] = f"{product['slug']}_orders_copy"
        if product["table_name"] == "dataset_orders":
            product["table_name"] = "dataset_orders_copy"

    imported = dataset_api_client.post("/v1/datasets/import", json=payload)

    assert imported.status_code == 201
    body = imported.json()
    assert body["dataset"]["slug"] == "orders_copy"
    assert len(body["quality_rules"]) == 1
    assert len(body["data_products"]) == 2
    assert len(body["pipelines"]) == 1
    assert len(body["schema_snapshots"]) == 1



def test_import_dataset_route_returns_conflict_for_existing_slug(dataset_api_client, db_session):
    DatasetService.create_dataset(db_session, CreateDatasetRequest(slug="orders", name="Orders"))

    payload = {
        "dataset": {"slug": "orders", "name": "Orders"},
        "quality_rules": [],
        "data_products": [],
        "pipelines": [],
        "schema_snapshots": [],
    }

    response = dataset_api_client.post("/v1/datasets/import", json=payload)

    assert response.status_code == 409
