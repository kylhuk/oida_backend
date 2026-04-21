from __future__ import annotations

from data_platform.schemas.dataset import CreateDatasetRequest, QualityRuleCreate
from data_platform.schemas.pipeline import CreatePipelineDefinitionRequest
from data_platform.services.dataset_service import DatasetService
from data_platform.services.pipeline_service import PipelineService



def test_export_dataset_includes_rules_products_pipelines_and_snapshots(db_session):
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
    DatasetService.save_schema_snapshot(db_session, dataset, "gold", [{"name": "id", "type": "BIGINT"}])
    PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="orders_gold_refresh",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
            definition_json={},
        ),
    )

    exported = DatasetService.export_dataset(db_session, dataset, include_schema_snapshots=True)

    assert exported["dataset"].slug == "orders"
    assert [rule.name for rule in exported["quality_rules"]] == ["gold_has_rows"]
    assert len(exported["data_products"]) == 1
    assert exported["data_products"][0].is_default is True
    assert [item["name"] for item in exported["pipelines"]] == ["orders_gold_refresh"]
    assert len(exported["schema_snapshots"]) == 1
    assert exported["schema_snapshots"][0].layer == "gold"
