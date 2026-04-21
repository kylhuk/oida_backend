from __future__ import annotations

import pytest

from data_platform.schemas.dataset import (
    CreateDataProductRequest,
    CreateDatasetRequest,
    DataProductResponse,
    DatasetExportResponse,
    DatasetImportRequest,
    DatasetResponse,
    QualityRuleCreate,
    QualityRuleResponse,
    SchemaSnapshotResponse,
)
from data_platform.schemas.pipeline import CreatePipelineDefinitionRequest
from data_platform.services.dataset_service import DatasetService
from data_platform.services.pipeline_service import PipelineService


def _build_import_payload(exported: dict, *, target_slug: str, target_name: str) -> dict:
    payload = DatasetExportResponse(
        dataset=DatasetResponse.model_validate(exported["dataset"]),
        quality_rules=[QualityRuleResponse.model_validate(item) for item in exported["quality_rules"]],
        data_products=[DataProductResponse.model_validate(item) for item in exported["data_products"]],
        pipelines=exported["pipelines"],
        schema_snapshots=[SchemaSnapshotResponse.model_validate(item) for item in exported["schema_snapshots"]],
    ).model_dump(mode="python", by_alias=True)

    original_dataset_slug = payload["dataset"]["slug"]
    original_gold_table = payload["dataset"]["gold_table_name"]
    payload["dataset"]["slug"] = target_slug
    payload["dataset"]["name"] = target_name
    payload["dataset"]["gold_table_name"] = f"dataset_{target_slug}"

    for product in payload["data_products"]:
        if product["slug"] == original_dataset_slug:
            product["slug"] = target_slug
        else:
            product["slug"] = f"{product['slug']}_{target_slug}"
        if product["table_name"] == original_gold_table:
            product["table_name"] = payload["dataset"]["gold_table_name"]

    return payload



def test_import_dataset_recreates_exported_configuration(db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(
            slug="orders",
            name="Orders",
            description="Primary orders dataset",
            silver_sql="SELECT * FROM source",
            gold_sql="SELECT id, amount FROM source",
            serving_config={"order_by": ["id"]},
            tags=["Finance", "Ops"],
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
            is_default=False,
        ),
    )
    DatasetService.save_schema_snapshot(db_session, dataset, "raw", [{"name": "id", "type": "BIGINT"}])
    DatasetService.save_schema_snapshot(db_session, dataset, "gold", [{"name": "id", "type": "BIGINT"}])
    DatasetService.save_schema_snapshot(
        db_session,
        dataset,
        "gold",
        [{"name": "id", "type": "BIGINT"}, {"name": "amount", "type": "DOUBLE"}],
    )
    PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="orders_refresh",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
            definition_json={"mode": "custom_sql", "sql": "SELECT id, amount FROM source"},
        ),
    )

    exported = DatasetService.export_dataset(db_session, dataset, include_schema_snapshots=True)
    payload = _build_import_payload(exported, target_slug="orders_copy", target_name="Orders Copy")

    imported = DatasetService.import_dataset(db_session, DatasetImportRequest.model_validate(payload))

    assert imported["dataset"].slug == "orders_copy"
    assert imported["dataset"].gold_table_name == "dataset_orders_copy"
    assert [rule.name for rule in imported["quality_rules"]] == ["gold_has_rows"]
    assert sorted(product.slug for product in imported["data_products"]) == [
        "orders_copy",
        "orders_dashboard_orders_copy",
    ]
    assert [item["name"] for item in imported["pipelines"]] == ["orders_refresh"]
    assert [(item.layer, item.version) for item in imported["schema_snapshots"]] == [
        ("gold", 1),
        ("gold", 2),
        ("raw", 1),
    ]
    assert imported["dataset"].latest_gold_schema_fingerprint == imported["schema_snapshots"][1].fingerprint



def test_dataset_import_request_rejects_multiple_default_products():
    payload = {
        "dataset": {"slug": "orders", "name": "Orders"},
        "data_products": [
            {"slug": "orders", "name": "Orders", "is_default": True},
            {"slug": "orders_dashboard", "name": "Orders Dashboard", "is_default": True},
        ],
    }

    with pytest.raises(ValueError, match="At most one imported data product can be marked as default"):
        DatasetImportRequest.model_validate(payload)



def test_dataset_import_request_rejects_nonconsecutive_snapshot_versions():
    payload = {
        "dataset": {"slug": "orders", "name": "Orders"},
        "schema_snapshots": [
            {"layer": "gold", "version": 1, "schema_json": [{"name": "id", "type": "BIGINT"}]},
            {"layer": "gold", "version": 3, "schema_json": [{"name": "id", "type": "BIGINT"}]},
        ],
    }

    with pytest.raises(ValueError, match="schema_snapshots for layer 'gold' must use consecutive versions starting at 1"):
        DatasetImportRequest.model_validate(payload)
