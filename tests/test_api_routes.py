from __future__ import annotations

from datetime import datetime, timedelta, timezone

from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from data_platform.api.deps import get_db, require_api_key
from data_platform.api.routes.datasets import router as datasets_router
from data_platform.api.routes.ingestions import router as ingestions_router
from data_platform.api.routes.pipelines import router as pipelines_router
from data_platform.schemas.dataset import CreateDatasetRequest, QualityRuleCreate
from data_platform.models.pipeline import PipelineRun
from data_platform.schemas.pipeline import CreatePipelineDefinitionRequest, CreatePipelineRunRequest
from data_platform.services.dataset_service import DatasetService
from data_platform.services.pipeline_service import PipelineService


@pytest.fixture()
def api_client(db_session):
    app = FastAPI()
    app.include_router(datasets_router)
    app.include_router(ingestions_router)
    app.include_router(pipelines_router)

    def _get_db():
        yield db_session

    app.dependency_overrides[get_db] = _get_db
    app.dependency_overrides[require_api_key] = lambda: SimpleNamespace(
        scopes=[
            "datasets:read",
            "datasets:write",
            "ingestions:read",
            "ingestions:write",
            "pipelines:read",
            "pipelines:write",
            "gold:read",
        ]
    )

    with TestClient(app) as client:
        yield client



def test_quality_rule_routes_create_and_update(api_client, db_session):
    DatasetService.create_dataset(db_session, CreateDatasetRequest(slug="orders", name="Orders"))

    created = api_client.post(
        "/v1/datasets/orders/quality-rules",
        json={
            "name": "gold_has_rows",
            "layer": "gold",
            "severity": "error",
            "sql_expression": "SELECT TRUE AS passed",
        },
    )
    assert created.status_code == 201
    rule_id = created.json()["id"]

    updated = api_client.patch(
        f"/v1/datasets/orders/quality-rules/{rule_id}",
        json={"active": False, "severity": "warn"},
    )
    assert updated.status_code == 200
    assert updated.json()["active"] is False
    assert updated.json()["severity"] == "warn"



def test_export_dataset_route_returns_nested_configuration(api_client, db_session):
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
            name="orders_refresh",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )

    response = api_client.get("/v1/datasets/orders/export?include_schema_snapshots=true")
    assert response.status_code == 200
    payload = response.json()
    assert payload["dataset"]["slug"] == "orders"
    assert len(payload["quality_rules"]) == 1
    assert len(payload["data_products"]) == 1
    assert len(payload["pipelines"]) == 1
    assert len(payload["schema_snapshots"]) == 1



def test_object_uri_ingestion_route_creates_job(api_client, db_session, monkeypatch):
    DatasetService.create_dataset(db_session, CreateDatasetRequest(slug="orders", name="Orders"))
    queued: list[str] = []

    monkeypatch.setattr(
        "data_platform.services.ingestion_service.IngestionService._safe_head_object",
        lambda self, bucket, key: {"ContentLength": 99, "ContentType": "text/csv; charset=utf-8"},
    )
    monkeypatch.setattr(
        "data_platform.services.ingestion_service.IngestionService.queue_job",
        lambda self, job_id: queued.append(job_id),
    )

    response = api_client.post(
        "/v1/ingestions/object-uri",
        json={
            "dataset_slug": "orders",
            "object_uri": "s3://raw/bootstrap/orders.csv",
            "idempotency_key": "route-1",
        },
    )

    assert response.status_code == 202
    payload = response.json()
    assert payload["source_type"] == "object_uri"
    assert payload["filename"] == "orders.csv"
    assert payload["source_content_type"] == "text/csv"
    assert payload["size_bytes"] == 99
    assert queued == [payload["id"]]



def test_pipeline_execution_plan_route_returns_resolved_sql_and_source_artifact(api_client, db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(slug="orders", name="Orders", gold_sql="SELECT * FROM source WHERE gold = TRUE"),
    )
    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="orders_refresh",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )

    from data_platform.enums import IngestionStatus
    from data_platform.models.ingestion import IngestionJob

    db_session.add(
        IngestionJob(
            dataset_id=dataset.id,
            source_type="object_uri",
            status=IngestionStatus.SUCCEEDED.value,
            silver_object_uri="s3://silver/orders/job-1/part-00000.parquet",
        )
    )
    db_session.commit()

    response = api_client.get(f"/v1/pipelines/{pipeline.id}/execution-plan")

    assert response.status_code == 200
    payload = response.json()
    assert payload["pipeline_id"] == pipeline.id
    assert payload["executable"] is True
    assert payload["resolved_query"] == "SELECT * FROM source WHERE gold = TRUE"
    assert payload["source_object_uri"] == "s3://silver/orders/job-1/part-00000.parquet"


def test_pipeline_execution_plan_route_supports_time_window_source_selection(api_client, db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(slug="orders-window", name="Orders Window", gold_sql="SELECT * FROM source WHERE gold = TRUE"),
    )
    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="orders_window_refresh",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )

    from data_platform.enums import IngestionStatus
    from data_platform.models.ingestion import IngestionJob

    db_session.add_all(
        [
            IngestionJob(
                dataset_id=dataset.id,
                source_type="object_uri",
                status=IngestionStatus.SUCCEEDED.value,
                finished_at=datetime(2026, 4, 14, 16, 0, tzinfo=timezone.utc),
                silver_object_uri="s3://silver/orders-window/job-1/part-00000.parquet",
            ),
            IngestionJob(
                dataset_id=dataset.id,
                source_type="object_uri",
                status=IngestionStatus.SUCCEEDED.value,
                finished_at=datetime(2026, 4, 14, 18, 0, tzinfo=timezone.utc),
                silver_object_uri="s3://silver/orders-window/job-2/part-00000.parquet",
            ),
            IngestionJob(
                dataset_id=dataset.id,
                source_type="object_uri",
                status=IngestionStatus.SUCCEEDED.value,
                finished_at=datetime(2026, 4, 14, 20, 0, tzinfo=timezone.utc),
                silver_object_uri="s3://silver/orders-window/job-3/part-00000.parquet",
            ),
        ]
    )
    db_session.commit()

    response = api_client.get(
        f"/v1/pipelines/{pipeline.id}/execution-plan",
        params={
            "source_finished_at_gte": "2026-04-14T17:00:00Z",
            "source_finished_at_lte": "2026-04-14T19:00:00Z",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["source_selection"] == "latest_successful_between"
    assert payload["requested_source_finished_at_gte"] in {"2026-04-14T17:00:00Z", "2026-04-14T17:00:00+00:00"}
    assert payload["requested_source_finished_at_lte"] in {"2026-04-14T19:00:00Z", "2026-04-14T19:00:00+00:00"}
    assert payload["source_object_uri"] == "s3://silver/orders-window/job-2/part-00000.parquet"
    assert payload["executable"] is True


def test_pipeline_run_detail_route_returns_persisted_preflight_snapshot(api_client, db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(slug="orders", name="Orders", gold_sql="SELECT * FROM source WHERE gold = TRUE"),
    )
    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="orders_refresh",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )

    from data_platform.enums import IngestionStatus
    from data_platform.models.ingestion import IngestionJob

    source_job = IngestionJob(
        dataset_id=dataset.id,
        source_type="object_uri",
        status=IngestionStatus.SUCCEEDED.value,
        silver_object_uri="s3://silver/orders/job-1/part-00000.parquet",
    )
    db_session.add(source_job)
    db_session.commit()

    created = api_client.post(f"/v1/pipelines/{pipeline.id}/runs", json={"run_ref": " nightly refresh "})
    assert created.status_code == 201
    run_id = created.json()["id"]

    response = api_client.get(f"/v1/pipelines/{pipeline.id}/runs/{run_id}")

    assert response.status_code == 200
    payload = response.json()
    assert payload["id"] == run_id
    assert payload["run_ref"] == "nightly refresh"
    assert payload["preflighted_at"] is not None
    assert payload["execution_plan"]["source_object_uri"] == source_job.silver_object_uri
    assert payload["execution_plan"]["resolved_query"] == "SELECT * FROM source WHERE gold = TRUE"
    assert payload["backfill_request"] is None


def test_pipeline_run_create_route_returns_first_class_preflight_fields(api_client, db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(
            slug="orders-run-create-fields-api",
            name="Orders Run Create Fields API",
            gold_sql="SELECT * FROM source WHERE gold = TRUE",
        ),
    )
    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="orders_run_create_fields_refresh",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )
    from data_platform.enums import IngestionStatus
    from data_platform.models.ingestion import IngestionJob

    source_job = IngestionJob(
        dataset_id=dataset.id,
        source_type="object_uri",
        status=IngestionStatus.SUCCEEDED.value,
        silver_object_uri="s3://silver/orders-run-create-fields-api/job-1/part-00000.parquet",
    )
    db_session.add(source_job)
    db_session.commit()

    response = api_client.post(f"/v1/pipelines/{pipeline.id}/runs", json={"run_ref": " nightly refresh "})

    assert response.status_code == 201
    payload = response.json()
    assert payload["run_ref"] == "nightly refresh"
    assert payload["preflighted_at"] is not None
    assert payload["execution_plan"]["source_ingestion_job_id"] == source_job.id
    assert payload["backfill_request"] is None



def test_pipeline_run_detail_route_returns_backfill_request_snapshot(api_client, db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(
            slug="orders-run-detail-backfill-api",
            name="Orders Run Detail Backfill API",
            gold_sql="SELECT * FROM source WHERE gold = TRUE",
        ),
    )
    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="orders_run_detail_backfill_api_refresh",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )

    from data_platform.enums import IngestionStatus
    from data_platform.models.ingestion import IngestionJob

    source_job = IngestionJob(
        dataset_id=dataset.id,
        source_type="object_uri",
        status=IngestionStatus.SUCCEEDED.value,
        silver_object_uri="s3://silver/orders-run-detail-backfill-api/job-1/part-00000.parquet",
        finished_at=datetime(2026, 4, 14, 10, 0, tzinfo=timezone.utc),
    )
    db_session.add(source_job)
    db_session.commit()

    created = api_client.post(
        f"/v1/pipelines/{pipeline.id}/runs/backfill",
        json={
            "run_ref_prefix": " nightly backfill ",
            "source_finished_at_gte": "2026-04-14T09:30:00Z",
            "source_finished_at_lte": "2026-04-14T10:30:00Z",
        },
    )
    assert created.status_code == 200
    run_id = created.json()[0]["id"]

    response = api_client.get(f"/v1/pipelines/{pipeline.id}/runs/{run_id}")

    assert response.status_code == 200
    payload = response.json()
    assert payload["backfill_request"] == {
        "run_ref_prefix": "nightly backfill",
        "source_finished_at_gte": "2026-04-14T09:30:00Z",
        "source_finished_at_lte": "2026-04-14T10:30:00Z",
        "skip_existing_runs": False,
        "require_contract_compatible_schema": False,
        "limit": 100,
        "offset": 0,
    }



def test_pipeline_run_detail_route_returns_404_for_wrong_pipeline(api_client, db_session):
    orders = DatasetService.create_dataset(db_session, CreateDatasetRequest(slug="orders", name="Orders"))
    customers = DatasetService.create_dataset(db_session, CreateDatasetRequest(slug="customers", name="Customers"))
    orders_pipeline = PipelineService.create_pipeline(
        db_session,
        orders,
        CreatePipelineDefinitionRequest(
            name="orders_refresh",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )
    customers_pipeline = PipelineService.create_pipeline(
        db_session,
        customers,
        CreatePipelineDefinitionRequest(
            name="customers_refresh",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )
    created = api_client.post(f"/v1/pipelines/{orders_pipeline.id}/runs", json={})
    assert created.status_code == 201
    run_id = created.json()["id"]

    response = api_client.get(f"/v1/pipelines/{customers_pipeline.id}/runs/{run_id}")

    assert response.status_code == 404
    assert response.json()["detail"] == "Pipeline run not found."


def test_pipeline_execution_plan_route_reports_missing_source_artifact(api_client, db_session):
    dataset = DatasetService.create_dataset(db_session, CreateDatasetRequest(slug="orders", name="Orders"))
    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="orders_refresh",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )

    response = api_client.get(f"/v1/pipelines/{pipeline.id}/execution-plan")

    assert response.status_code == 200
    payload = response.json()
    assert payload["executable"] is False
    assert payload["issues"] == [
        "No successful ingestion is available for source layer 'silver'.",
        "No source object URI is available for source layer 'silver'.",
    ]



def test_schema_diff_route_returns_latest_diff(api_client, db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(slug="orders", name="Orders"),
    )
    DatasetService.save_schema_snapshot(db_session, dataset, "gold", [{"name": "id", "type": "BIGINT"}])
    DatasetService.save_schema_snapshot(
        db_session,
        dataset,
        "gold",
        [{"name": "id", "type": "BIGINT"}, {"name": "amount", "type": "DOUBLE"}],
    )

    response = api_client.get("/v1/datasets/orders/schemas/diff?layer=gold")

    assert response.status_code == 200
    payload = response.json()
    assert payload["from_version"] == 1
    assert payload["to_version"] == 2
    assert payload["added_columns"] == [{"name": "amount", "type": "DOUBLE"}]
    assert payload["removed_columns"] == []
    assert payload["changed_columns"] == []
    assert payload["breaking_changes"] is False


def test_schema_diff_route_rejects_unknown_layer(api_client, db_session):
    DatasetService.create_dataset(db_session, CreateDatasetRequest(slug="orders", name="Orders"))

    response = api_client.get("/v1/datasets/orders/schemas/diff?layer=platinum")

    assert response.status_code == 400
    assert "layer must be one of" in response.json()["detail"]


def test_schema_compatibility_route_returns_contract_preview(api_client, db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(slug="orders", name="Orders"),
    )
    DatasetService.save_schema_snapshot(db_session, dataset, "gold", [{"name": "id", "type": "BIGINT"}])

    response = api_client.post(
        "/v1/datasets/orders/schemas/compatibility",
        json={
            "layer": "gold",
            "schema_json": [
                {"name": "id", "type": "BIGINT"},
                {"name": "amount", "type": "DOUBLE"},
            ],
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["against_version"] == 1
    assert payload["contract_compatible"] is True
    assert payload["strict_mode_compatible"] is False
    assert payload["merged_schema"] == [
        {"name": "id", "type": "BIGINT"},
        {"name": "amount", "type": "DOUBLE"},
    ]



def test_schema_compatibility_route_rejects_unknown_snapshot_version(api_client, db_session):
    DatasetService.create_dataset(db_session, CreateDatasetRequest(slug="orders", name="Orders"))

    response = api_client.post(
        "/v1/datasets/orders/schemas/compatibility",
        json={
            "layer": "gold",
            "against_version": 99,
            "schema_json": [{"name": "id", "type": "BIGINT"}],
        },
    )

    assert response.status_code == 400
    assert "Schema snapshot version 99 not found" in response.json()["detail"]


def test_pipeline_execution_plan_route_supports_explicit_source_ingestion_job_id(api_client, db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(slug="orders", name="Orders", gold_sql="SELECT * FROM source WHERE gold = TRUE"),
    )
    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="orders_refresh",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )

    from data_platform.enums import IngestionStatus
    from data_platform.models.ingestion import IngestionJob

    selected_job = IngestionJob(
        dataset_id=dataset.id,
        source_type="object_uri",
        status=IngestionStatus.SUCCEEDED.value,
        silver_object_uri="s3://silver/orders/job-selected/part-00000.parquet",
    )
    latest_job = IngestionJob(
        dataset_id=dataset.id,
        source_type="object_uri",
        status=IngestionStatus.SUCCEEDED.value,
        silver_object_uri="s3://silver/orders/job-latest/part-00000.parquet",
    )
    db_session.add_all([selected_job, latest_job])
    db_session.commit()

    response = api_client.get(
        f"/v1/pipelines/{pipeline.id}/execution-plan",
        params={"source_ingestion_job_id": selected_job.id},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["source_selection"] == "explicit"
    assert payload["requested_source_ingestion_job_id"] == selected_job.id
    assert payload["source_ingestion_job_id"] == selected_job.id
    assert payload["source_object_uri"] == "s3://silver/orders/job-selected/part-00000.parquet"



def test_pipeline_execution_plan_route_rejects_source_ingestion_job_from_other_dataset(api_client, db_session):
    orders = DatasetService.create_dataset(db_session, CreateDatasetRequest(slug="orders", name="Orders"))
    customers = DatasetService.create_dataset(db_session, CreateDatasetRequest(slug="customers", name="Customers"))
    pipeline = PipelineService.create_pipeline(
        db_session,
        orders,
        CreatePipelineDefinitionRequest(
            name="orders_refresh",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )

    from data_platform.enums import IngestionStatus
    from data_platform.models.ingestion import IngestionJob

    foreign_job = IngestionJob(
        dataset_id=customers.id,
        source_type="object_uri",
        status=IngestionStatus.SUCCEEDED.value,
        silver_object_uri="s3://silver/customers/job-1/part-00000.parquet",
    )
    db_session.add(foreign_job)
    db_session.commit()

    response = api_client.get(
        f"/v1/pipelines/{pipeline.id}/execution-plan",
        params={"source_ingestion_job_id": foreign_job.id},
    )

    assert response.status_code == 400
    assert "does not belong to dataset" in response.json()["detail"]


def test_pipeline_execution_plan_route_supports_time_bounded_source_selection(api_client, db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(slug="orders", name="Orders", gold_sql="SELECT * FROM source WHERE gold = TRUE"),
    )
    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="orders_refresh",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )

    from data_platform.enums import IngestionStatus
    from data_platform.models.ingestion import IngestionJob

    older_job = IngestionJob(
        dataset_id=dataset.id,
        source_type="object_uri",
        status=IngestionStatus.SUCCEEDED.value,
        silver_object_uri="s3://silver/orders/job-older/part-00000.parquet",
        finished_at=datetime(2026, 4, 14, 17, 0, tzinfo=timezone.utc),
    )
    newer_job = IngestionJob(
        dataset_id=dataset.id,
        source_type="object_uri",
        status=IngestionStatus.SUCCEEDED.value,
        silver_object_uri="s3://silver/orders/job-newer/part-00000.parquet",
        finished_at=datetime(2026, 4, 14, 19, 0, tzinfo=timezone.utc),
    )
    db_session.add_all([older_job, newer_job])
    db_session.commit()

    response = api_client.get(
        f"/v1/pipelines/{pipeline.id}/execution-plan",
        params={"source_finished_at_lte": "2026-04-14T18:30:00Z"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["source_selection"] == "latest_successful_at_or_before"
    assert payload["requested_source_finished_at_lte"].startswith("2026-04-14T18:30:00")
    assert payload["source_ingestion_job_id"] == older_job.id
    assert payload["source_object_uri"] == older_job.silver_object_uri
    assert payload["executable"] is True



def test_pipeline_execution_plan_route_rejects_combined_source_selection_inputs(api_client, db_session):
    dataset = DatasetService.create_dataset(db_session, CreateDatasetRequest(slug="orders", name="Orders"))
    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="orders_refresh",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )

    from data_platform.enums import IngestionStatus
    from data_platform.models.ingestion import IngestionJob

    source_job = IngestionJob(
        dataset_id=dataset.id,
        source_type="object_uri",
        status=IngestionStatus.SUCCEEDED.value,
        silver_object_uri="s3://silver/orders/job-1/part-00000.parquet",
    )
    db_session.add(source_job)
    db_session.commit()

    response = api_client.get(
        f"/v1/pipelines/{pipeline.id}/execution-plan",
        params={
            "source_ingestion_job_id": source_job.id,
            "source_finished_at_lte": "2026-04-14T18:30:00Z",
        },
    )

    assert response.status_code == 400
    assert "cannot be combined" in response.json()["detail"]


def test_pipeline_run_create_route_persists_planned_preflight(api_client, db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(slug="orders", name="Orders", gold_sql="SELECT * FROM source WHERE gold = TRUE"),
    )
    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="orders_refresh",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )

    from data_platform.enums import IngestionStatus
    from data_platform.models.ingestion import IngestionJob

    source_job = IngestionJob(
        dataset_id=dataset.id,
        source_type="object_uri",
        status=IngestionStatus.SUCCEEDED.value,
        silver_object_uri="s3://silver/orders/job-1/part-00000.parquet",
    )
    db_session.add(source_job)
    db_session.commit()

    response = api_client.post(
        f"/v1/pipelines/{pipeline.id}/runs",
        json={"run_ref": " nightly refresh "},
    )

    assert response.status_code == 201
    payload = response.json()
    assert payload["status"] == "planned"
    assert payload["run_ref"] == "nightly refresh"
    assert payload["ingestion_job_id"] == source_job.id
    assert payload["metrics_json"]["execution_plan"]["source_object_uri"] == source_job.silver_object_uri


def test_pipeline_run_create_route_persists_blocked_preflight(api_client, db_session):
    dataset = DatasetService.create_dataset(db_session, CreateDatasetRequest(slug="orders", name="Orders"))
    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="orders_refresh",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )

    response = api_client.post(f"/v1/pipelines/{pipeline.id}/runs", json={})

    assert response.status_code == 201
    payload = response.json()
    assert payload["status"] == "blocked"
    assert payload["finished_at"] is not None
    assert "No successful ingestion is available" in payload["error_message"]
    assert payload["metrics_json"]["execution_plan"]["issues"] == [
        "No successful ingestion is available for source layer 'silver'.",
        "No source object URI is available for source layer 'silver'.",
    ]




def test_pipeline_run_routes_can_filter_by_created_at_range(api_client, db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(slug="orders-run-created-range-route", name="Orders Run Created Range Route", gold_sql="SELECT * FROM source"),
    )
    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="orders_run_created_range_route",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )

    from data_platform.enums import IngestionStatus
    from data_platform.models.ingestion import IngestionJob

    source_job = IngestionJob(
        dataset_id=dataset.id,
        source_type="object_uri",
        status=IngestionStatus.SUCCEEDED.value,
        silver_object_uri="s3://silver/orders-run-created-range-route/job-1/part-00000.parquet",
    )
    db_session.add(source_job)
    db_session.commit()

    older = PipelineService.create_pipeline_run(db_session, pipeline, CreatePipelineRunRequest(run_ref="older"))
    newer = PipelineService.create_pipeline_run(db_session, pipeline, CreatePipelineRunRequest(run_ref="newer"))
    lower_bound = older.created_at + timedelta(microseconds=1)
    lower_bound_str = lower_bound.isoformat().replace("+00:00", "Z")

    list_response = api_client.get(
        f"/v1/pipelines/{pipeline.id}/runs",
        params={"created_at_gte": lower_bound_str},
    )
    assert list_response.status_code == 200
    assert [item["id"] for item in list_response.json()] == [newer.id]

    count_response = api_client.get(
        f"/v1/pipelines/{pipeline.id}/runs/count",
        params={"created_at_gte": lower_bound_str},
    )
    assert count_response.status_code == 200
    assert count_response.json()["count"] == 1


def test_pipeline_run_routes_reject_inverted_created_at_range(api_client, db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(slug="orders-run-created-range-route-invalid", name="Orders Run Created Range Route Invalid"),
    )
    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="orders_run_created_range_route_invalid",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )

    params = {
        "created_at_gte": "2026-04-15T00:00:00Z",
        "created_at_lte": "2026-04-14T00:00:00Z",
    }
    list_response = api_client.get(f"/v1/pipelines/{pipeline.id}/runs", params=params)
    count_response = api_client.get(f"/v1/pipelines/{pipeline.id}/runs/count", params=params)

    assert list_response.status_code == 400
    assert "created_at_gte cannot be after created_at_lte" in list_response.json()["detail"]
    assert count_response.status_code == 400
    assert "created_at_gte cannot be after created_at_lte" in count_response.json()["detail"]

def test_pipeline_run_count_route_can_filter_by_status(api_client, db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(slug="orders-run-count-status-api", name="Orders Run Count Status API", gold_sql="SELECT * FROM source"),
    )
    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="orders_run_count_status_refresh",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )

    from data_platform.enums import IngestionStatus
    from data_platform.models.ingestion import IngestionJob

    db_session.add(
        IngestionJob(
            dataset_id=dataset.id,
            source_type="object_uri",
            status=IngestionStatus.SUCCEEDED.value,
            silver_object_uri="s3://silver/orders-run-count-status-api/job-1/part-00000.parquet",
        )
    )
    db_session.commit()

    assert api_client.post(f"/v1/pipelines/{pipeline.id}/runs", json={}).status_code == 201
    assert api_client.post(
        f"/v1/pipelines/{pipeline.id}/runs",
        json={"source_finished_at_gte": "2030-01-01T00:00:00Z"},
    ).status_code == 201

    response = api_client.get(f"/v1/pipelines/{pipeline.id}/runs/count", params={"status": "blocked"})

    assert response.status_code == 200
    assert response.json() == {"count": 1}


def test_pipeline_run_list_route_can_filter_by_status(api_client, db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(slug="orders-run-filter", name="Orders Run Filter", gold_sql="SELECT * FROM source WHERE gold = TRUE"),
    )
    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="orders_run_filter_refresh",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )

    from data_platform.enums import IngestionStatus
    from data_platform.models.ingestion import IngestionJob

    db_session.add(
        IngestionJob(
            dataset_id=dataset.id,
            source_type="object_uri",
            status=IngestionStatus.SUCCEEDED.value,
            silver_object_uri="s3://silver/orders-run-filter/job-1/part-00000.parquet",
        )
    )
    db_session.commit()

    planned = api_client.post(f"/v1/pipelines/{pipeline.id}/runs", json={})
    assert planned.status_code == 201

    blocked = api_client.post(
        f"/v1/pipelines/{pipeline.id}/runs",
        json={"source_finished_at_gte": "2030-01-01T00:00:00Z"},
    )
    assert blocked.status_code == 201

    response = api_client.get(f"/v1/pipelines/{pipeline.id}/runs", params={"status": "blocked"})

    assert response.status_code == 200
    payload = response.json()
    assert [run["status"] for run in payload] == ["blocked"]
    assert [run["id"] for run in payload] == [blocked.json()["id"]]



def test_pipeline_run_list_route_can_filter_by_run_ref(api_client, db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(slug="orders-run-ref-filter-api", name="Orders Run Ref Filter API", gold_sql="SELECT * FROM source WHERE gold = TRUE"),
    )
    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="orders_run_ref_filter_refresh",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )

    from data_platform.enums import IngestionStatus
    from data_platform.models.ingestion import IngestionJob

    db_session.add(
        IngestionJob(
            dataset_id=dataset.id,
            source_type="object_uri",
            status=IngestionStatus.SUCCEEDED.value,
            silver_object_uri="s3://silver/orders-run-ref-filter-api/job-1/part-00000.parquet",
        )
    )
    db_session.commit()

    selected = api_client.post(f"/v1/pipelines/{pipeline.id}/runs", json={"run_ref": "nightly refresh"})
    assert selected.status_code == 201
    other = api_client.post(f"/v1/pipelines/{pipeline.id}/runs", json={"run_ref": "hourly refresh"})
    assert other.status_code == 201

    response = api_client.get(
        f"/v1/pipelines/{pipeline.id}/runs",
        params={"run_ref": "  nightly refresh  "},
    )

    assert response.status_code == 200
    payload = response.json()
    assert [run["id"] for run in payload] == [selected.json()["id"]]




def test_pipeline_run_list_route_can_filter_by_source_ingestion_job_id(api_client, db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(
            slug="orders-run-source-filter-api",
            name="Orders Run Source Filter API",
            gold_sql="SELECT * FROM source WHERE gold = TRUE",
        ),
    )
    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="orders_run_source_filter_refresh",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )

    from data_platform.enums import IngestionStatus
    from data_platform.models.ingestion import IngestionJob

    selected_job = IngestionJob(
        dataset_id=dataset.id,
        source_type="object_uri",
        status=IngestionStatus.SUCCEEDED.value,
        silver_object_uri="s3://silver/orders-run-source-filter-api/job-1/part-00000.parquet",
    )
    other_job = IngestionJob(
        dataset_id=dataset.id,
        source_type="object_uri",
        status=IngestionStatus.SUCCEEDED.value,
        silver_object_uri="s3://silver/orders-run-source-filter-api/job-2/part-00000.parquet",
    )
    db_session.add_all([selected_job, other_job])
    db_session.commit()

    selected = api_client.post(
        f"/v1/pipelines/{pipeline.id}/runs",
        json={"source_ingestion_job_id": f"  {selected_job.id}  "},
    )
    assert selected.status_code == 201
    other = api_client.post(
        f"/v1/pipelines/{pipeline.id}/runs",
        json={"source_ingestion_job_id": other_job.id},
    )
    assert other.status_code == 201

    response = api_client.get(
        f"/v1/pipelines/{pipeline.id}/runs",
        params={"source_ingestion_job_id": f"  {selected_job.id}  "},
    )

    assert response.status_code == 200
    payload = response.json()
    assert [run["id"] for run in payload] == [selected.json()["id"]]


def test_pipeline_run_list_route_rejects_blank_source_ingestion_job_id(api_client, db_session):
    dataset = DatasetService.create_dataset(db_session, CreateDatasetRequest(slug="orders-run-source-filter-api-blank", name="Orders Run Source Filter API Blank"))
    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="orders_run_source_filter_blank_refresh",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )

    response = api_client.get(
        f"/v1/pipelines/{pipeline.id}/runs",
        params={"source_ingestion_job_id": "   "},
    )

    assert response.status_code == 400
    assert "source_ingestion_job_id cannot be empty" in response.json()["detail"]



def test_pipeline_source_candidates_route_lists_successful_source_jobs(api_client, db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(
            slug="orders-source-candidates-api",
            name="Orders Source Candidates API",
            gold_sql="SELECT * FROM source WHERE gold = TRUE",
        ),
    )
    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="orders_source_candidates_refresh",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )

    from data_platform.enums import IngestionStatus
    from data_platform.models.ingestion import IngestionJob

    older_job = IngestionJob(
        dataset_id=dataset.id,
        source_type="object_uri",
        status=IngestionStatus.SUCCEEDED.value,
        silver_object_uri="s3://silver/orders-source-candidates-api/job-1/part-00000.parquet",
        finished_at=datetime(2026, 4, 14, 10, 0, tzinfo=timezone.utc),
    )
    newer_job = IngestionJob(
        dataset_id=dataset.id,
        source_type="object_uri",
        status=IngestionStatus.SUCCEEDED.value,
        silver_object_uri="s3://silver/orders-source-candidates-api/job-2/part-00000.parquet",
        finished_at=datetime(2026, 4, 14, 11, 0, tzinfo=timezone.utc),
    )
    db_session.add_all([older_job, newer_job])
    db_session.commit()

    older_run = PipelineRun(
        pipeline_id=pipeline.id,
        dataset_id=dataset.id,
        ingestion_job_id=newer_job.id,
        status="blocked",
        run_ref="older refresh",
        metrics_json={},
        created_at=datetime(2026, 4, 14, 11, 5, tzinfo=timezone.utc),
    )
    existing_run = PipelineRun(
        pipeline_id=pipeline.id,
        dataset_id=dataset.id,
        ingestion_job_id=newer_job.id,
        status="planned",
        run_ref="nightly refresh",
        metrics_json={},
        created_at=datetime(2026, 4, 14, 11, 15, tzinfo=timezone.utc),
    )
    db_session.add_all([older_run, existing_run])
    db_session.commit()

    response = api_client.get(f"/v1/pipelines/{pipeline.id}/source-candidates")

    assert response.status_code == 200
    payload = response.json()
    assert [candidate["ingestion_job_id"] for candidate in payload[:2]] == [newer_job.id, older_job.id]
    assert payload[0]["object_uri"] == newer_job.silver_object_uri
    assert payload[0]["existing_run_count"] == 2
    assert payload[0]["has_existing_run"] is True
    assert payload[0]["latest_run_id"] == existing_run.id
    assert payload[0]["latest_run_status"] == "planned"
    assert payload[0]["latest_run_ref"] == "nightly refresh"
    assert payload[0]["latest_run_created_at"] == "2026-04-14T11:15:00Z"
    assert payload[1]["existing_run_count"] == 0
    assert payload[1]["has_existing_run"] is False
    assert payload[1]["latest_run_id"] is None


def test_pipeline_source_candidates_route_respects_time_window(api_client, db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(
            slug="orders-source-candidates-window-api",
            name="Orders Source Candidates Window API",
            gold_sql="SELECT * FROM source WHERE gold = TRUE",
        ),
    )
    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="orders_source_candidates_window_refresh",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )

    from data_platform.enums import IngestionStatus
    from data_platform.models.ingestion import IngestionJob

    older_job = IngestionJob(
        dataset_id=dataset.id,
        source_type="object_uri",
        status=IngestionStatus.SUCCEEDED.value,
        silver_object_uri="s3://silver/orders-source-candidates-window-api/job-1/part-00000.parquet",
        finished_at=datetime(2026, 4, 14, 10, 0, tzinfo=timezone.utc),
    )
    newer_job = IngestionJob(
        dataset_id=dataset.id,
        source_type="object_uri",
        status=IngestionStatus.SUCCEEDED.value,
        silver_object_uri="s3://silver/orders-source-candidates-window-api/job-2/part-00000.parquet",
        finished_at=datetime(2026, 4, 14, 11, 0, tzinfo=timezone.utc),
    )
    db_session.add_all([older_job, newer_job])
    db_session.commit()

    response = api_client.get(
        f"/v1/pipelines/{pipeline.id}/source-candidates",
        params={
            "source_finished_at_gte": "2026-04-14T10:30:00Z",
            "source_finished_at_lte": "2026-04-14T11:30:00Z",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert [candidate["ingestion_job_id"] for candidate in payload] == [newer_job.id]




def test_pipeline_source_candidates_route_can_filter_on_existing_run_state(api_client, db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(
            slug="orders-source-candidates-existing-filter-api",
            name="Orders Source Candidates Existing Filter API",
            gold_sql="SELECT * FROM source WHERE gold = TRUE",
        ),
    )
    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="orders_source_candidates_existing_filter_refresh",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )

    from data_platform.enums import IngestionStatus
    from data_platform.models.ingestion import IngestionJob

    older_job = IngestionJob(
        dataset_id=dataset.id,
        source_type="object_uri",
        status=IngestionStatus.SUCCEEDED.value,
        silver_object_uri="s3://silver/orders-source-candidates-existing-filter-api/job-1/part-00000.parquet",
        finished_at=datetime(2026, 4, 14, 10, 0, tzinfo=timezone.utc),
    )
    newer_job = IngestionJob(
        dataset_id=dataset.id,
        source_type="object_uri",
        status=IngestionStatus.SUCCEEDED.value,
        silver_object_uri="s3://silver/orders-source-candidates-existing-filter-api/job-2/part-00000.parquet",
        finished_at=datetime(2026, 4, 14, 11, 0, tzinfo=timezone.utc),
    )
    db_session.add_all([older_job, newer_job])
    db_session.commit()

    db_session.add(PipelineRun(
        pipeline_id=pipeline.id,
        dataset_id=dataset.id,
        ingestion_job_id=newer_job.id,
        status="planned",
        run_ref="nightly refresh",
        metrics_json={},
        created_at=datetime(2026, 4, 14, 11, 15, tzinfo=timezone.utc),
    ))
    db_session.commit()

    existing_response = api_client.get(
        f"/v1/pipelines/{pipeline.id}/source-candidates",
        params={"has_existing_run": "true"},
    )
    missing_response = api_client.get(
        f"/v1/pipelines/{pipeline.id}/source-candidates",
        params={"has_existing_run": "false"},
    )

    assert existing_response.status_code == 200
    assert [candidate["ingestion_job_id"] for candidate in existing_response.json()] == [newer_job.id]
    assert missing_response.status_code == 200
    assert [candidate["ingestion_job_id"] for candidate in missing_response.json()] == [older_job.id]


def test_pipeline_source_candidates_route_rejects_conflicting_existing_run_filters(api_client, db_session):
    dataset = DatasetService.create_dataset(db_session, CreateDatasetRequest(slug="orders-source-candidates-existing-invalid-api", name="Orders Source Candidates Existing Invalid API"))
    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="orders_source_candidates_existing_invalid_refresh",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )

    response = api_client.get(
        f"/v1/pipelines/{pipeline.id}/source-candidates",
        params={"exclude_existing_runs": "true", "has_existing_run": "true"},
    )

    assert response.status_code == 400
    assert "exclude_existing_runs cannot be combined with has_existing_run=true" in response.json()["detail"]

    response = api_client.get(
        f"/v1/pipelines/{pipeline.id}/source-candidates/count",
        params={"exclude_existing_runs": "true", "has_existing_run": "true"},
    )

    assert response.status_code == 400
    assert "exclude_existing_runs cannot be combined with has_existing_run=true" in response.json()["detail"]

def test_pipeline_source_candidates_route_rejects_invalid_time_window(api_client, db_session):
    dataset = DatasetService.create_dataset(db_session, CreateDatasetRequest(slug="orders-source-candidates-invalid-api", name="Orders Source Candidates Invalid API"))
    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="orders_source_candidates_invalid_refresh",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )

    response = api_client.get(
        f"/v1/pipelines/{pipeline.id}/source-candidates",
        params={
            "source_finished_at_gte": "2026-04-14T12:00:00Z",
            "source_finished_at_lte": "2026-04-14T11:00:00Z",
        },
    )

    assert response.status_code == 400
    assert "source_finished_at_gte cannot be after source_finished_at_lte" in response.json()["detail"]


def test_pipeline_backfill_runs_route_creates_windowed_preflights(api_client, db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(
            slug="orders-backfill-runs-api",
            name="Orders Backfill Runs API",
            gold_sql="SELECT * FROM source WHERE gold = TRUE",
        ),
    )
    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="orders_backfill_runs_api_refresh",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )

    from data_platform.enums import IngestionStatus
    from data_platform.models.ingestion import IngestionJob

    older_job = IngestionJob(
        dataset_id=dataset.id,
        source_type="object_uri",
        status=IngestionStatus.SUCCEEDED.value,
        silver_object_uri="s3://silver/orders-backfill-runs-api/job-1/part-00000.parquet",
        finished_at=datetime(2026, 4, 14, 10, 0, tzinfo=timezone.utc),
    )
    newer_job = IngestionJob(
        dataset_id=dataset.id,
        source_type="object_uri",
        status=IngestionStatus.SUCCEEDED.value,
        silver_object_uri="s3://silver/orders-backfill-runs-api/job-2/part-00000.parquet",
        finished_at=datetime(2026, 4, 14, 11, 0, tzinfo=timezone.utc),
    )
    db_session.add_all([older_job, newer_job])
    db_session.commit()

    response = api_client.post(
        f"/v1/pipelines/{pipeline.id}/runs/backfill",
        json={
            "run_ref_prefix": " nightly backfill ",
            "source_finished_at_gte": "2026-04-14T09:30:00Z",
            "source_finished_at_lte": "2026-04-14T11:30:00Z",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert [run["ingestion_job_id"] for run in payload] == [newer_job.id, older_job.id]
    assert payload[0]["run_ref"] == f"nightly backfill:{newer_job.id}"
    assert payload[1]["run_ref"] == f"nightly backfill:{older_job.id}"


def test_pipeline_backfill_runs_route_skips_existing_runs_when_requested(api_client, db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(
            slug="orders-backfill-runs-api-dedup",
            name="Orders Backfill Runs API Dedup",
            gold_sql="SELECT * FROM source WHERE gold = TRUE",
        ),
    )
    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="orders_backfill_runs_api_dedup_refresh",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )

    from data_platform.enums import IngestionStatus
    from data_platform.models.ingestion import IngestionJob

    older_job = IngestionJob(
        dataset_id=dataset.id,
        source_type="object_uri",
        status=IngestionStatus.SUCCEEDED.value,
        silver_object_uri="s3://silver/orders-backfill-runs-api-dedup/job-1/part-00000.parquet",
        finished_at=datetime(2026, 4, 14, 10, 0, tzinfo=timezone.utc),
    )
    newer_job = IngestionJob(
        dataset_id=dataset.id,
        source_type="object_uri",
        status=IngestionStatus.SUCCEEDED.value,
        silver_object_uri="s3://silver/orders-backfill-runs-api-dedup/job-2/part-00000.parquet",
        finished_at=datetime(2026, 4, 14, 11, 0, tzinfo=timezone.utc),
    )
    db_session.add_all([older_job, newer_job])
    db_session.commit()

    PipelineService.create_pipeline_run(
        db_session,
        pipeline,
        CreatePipelineRunRequest(source_ingestion_job_id=older_job.id),
    )

    response = api_client.post(
        f"/v1/pipelines/{pipeline.id}/runs/backfill",
        json={
            "run_ref_prefix": "nightly backfill",
            "source_finished_at_gte": "2026-04-14T09:30:00Z",
            "source_finished_at_lte": "2026-04-14T11:30:00Z",
            "skip_existing_runs": True,
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert [run["ingestion_job_id"] for run in payload] == [newer_job.id]
    assert payload[0]["backfill_request"]["skip_existing_runs"] is True


def test_pipeline_backfill_runs_route_rejects_missing_time_bounds(api_client, db_session):
    dataset = DatasetService.create_dataset(db_session, CreateDatasetRequest(slug="orders-backfill-runs-api-invalid", name="Orders Backfill Runs API Invalid"))
    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="orders_backfill_runs_api_invalid_refresh",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )

    response = api_client.post(
        f"/v1/pipelines/{pipeline.id}/runs/backfill",
        json={"run_ref_prefix": "nightly"},
    )

    assert response.status_code == 422


def test_pipeline_source_candidates_route_uses_enriched_response_model():
    from pathlib import Path

    route_source = Path("src/data_platform/api/routes/pipelines.py").read_text()
    schema_source = Path("src/data_platform/schemas/pipeline.py").read_text()

    assert "PipelineSourceCandidateResponse" in route_source
    assert "latest_run_finished_at" in schema_source
    assert "latest_run_error_message" in schema_source
    assert "schema_compatibility_preview" in schema_source
    assert "would_fail_require_contract_compatible_schema" in schema_source


def test_pipeline_run_create_route_rejects_when_contract_compatibility_is_required_but_unavailable(api_client, db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(
            slug="orders-run-compat-required-unavailable-api",
            name="Orders Run Compat Required Unavailable API",
            gold_sql="SELECT * FROM source",
        ),
    )
    DatasetService.save_schema_snapshot(db_session, dataset, "silver", [{"name": "id", "type": "BIGINT"}])
    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="orders_run_compat_required_unavailable_refresh",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
            definition_json={"mode": "custom_sql", "sql": "SELECT * FROM source"},
        ),
    )

    from data_platform.enums import IngestionStatus
    from data_platform.models.ingestion import IngestionJob

    db_session.add(
        IngestionJob(
            dataset_id=dataset.id,
            source_type="object_uri",
            status=IngestionStatus.SUCCEEDED.value,
            silver_object_uri="s3://silver/orders-run-compat-required-unavailable-api/job-1/part-00000.parquet",
        )
    )
    db_session.commit()

    response = api_client.post(
        f"/v1/pipelines/{pipeline.id}/runs",
        json={"require_contract_compatible_schema": True},
    )

    assert response.status_code == 400
    assert response.json()["detail"] == "Compatibility preview is only available for sql dataset-transform pipelines."


def test_pipeline_run_create_route_persists_rejected_preflight_attempt_for_contract_guard(api_client, db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(
            slug="orders-run-compat-required-unavailable-preflight-api",
            name="Orders Run Compat Required Unavailable Preflight API",
            gold_sql="SELECT * FROM source",
        ),
    )
    DatasetService.save_schema_snapshot(db_session, dataset, "silver", [{"name": "id", "type": "BIGINT"}])
    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="orders_run_compat_required_unavailable_preflight_refresh",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
            definition_json={"mode": "custom_sql", "sql": "SELECT * FROM source"},
        ),
    )

    from data_platform.enums import IngestionStatus
    from data_platform.models.ingestion import IngestionJob

    db_session.add(
        IngestionJob(
            dataset_id=dataset.id,
            source_type="object_uri",
            status=IngestionStatus.SUCCEEDED.value,
            silver_object_uri="s3://silver/orders-run-compat-required-unavailable-preflight-api/job-1/part-00000.parquet",
        )
    )
    db_session.commit()

    rejected = api_client.post(
        f"/v1/pipelines/{pipeline.id}/runs",
        json={"require_contract_compatible_schema": True},
    )

    assert rejected.status_code == 400

    response = api_client.get(f"/v1/pipelines/{pipeline.id}/preflight-attempts")

    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["request_kind"] == "run"
    assert payload[0]["contract_compatibility_outcome"] == "required_preview_unavailable"
    assert payload[0]["schema_compatibility_preview_unavailable_reason"] == (
        "Compatibility preview is only available for sql dataset-transform pipelines."
    )
    assert payload[0]["error_message"] == "Compatibility preview is only available for sql dataset-transform pipelines."


def test_pipeline_run_create_route_returns_schema_compatibility_preview_unavailable_reason(api_client, db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(
            slug="orders-run-compat-preview-unavailable-api",
            name="Orders Run Compat Preview Unavailable API",
            gold_sql="SELECT * FROM source",
        ),
    )
    DatasetService.save_schema_snapshot(db_session, dataset, "silver", [{"name": "id", "type": "BIGINT"}])
    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="orders_run_compat_preview_unavailable_refresh",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
            definition_json={"mode": "custom_sql", "sql": "SELECT * FROM source"},
        ),
    )

    from data_platform.enums import IngestionStatus
    from data_platform.models.ingestion import IngestionJob

    db_session.add(
        IngestionJob(
            dataset_id=dataset.id,
            source_type="object_uri",
            status=IngestionStatus.SUCCEEDED.value,
            silver_object_uri="s3://silver/orders-run-compat-preview-unavailable-api/job-1/part-00000.parquet",
        )
    )
    db_session.commit()

    response = api_client.post(f"/v1/pipelines/{pipeline.id}/runs", json={})

    assert response.status_code == 201
    payload = response.json()
    assert payload["schema_compatibility_preview"] is None
    assert payload["schema_compatibility_preview_unavailable_reason"] == (
        "Compatibility preview is only available for sql dataset-transform pipelines."
    )



def test_pipeline_run_create_route_returns_contract_compatibility_requirement_flag(api_client, db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(
            slug="orders-run-compat-required-api",
            name="Orders Run Compat Required API",
            gold_sql="SELECT * FROM source WHERE gold = TRUE",
        ),
    )
    DatasetService.save_schema_snapshot(db_session, dataset, "silver", [{"name": "id", "type": "BIGINT"}])
    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="orders_run_compat_required_refresh",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )

    from data_platform.enums import IngestionStatus
    from data_platform.models.ingestion import IngestionJob

    db_session.add(
        IngestionJob(
            dataset_id=dataset.id,
            source_type="object_uri",
            status=IngestionStatus.SUCCEEDED.value,
            silver_object_uri="s3://silver/orders-run-compat-required-api/job-1/part-00000.parquet",
        )
    )
    db_session.commit()

    response = api_client.post(
        f"/v1/pipelines/{pipeline.id}/runs",
        json={"require_contract_compatible_schema": True},
    )

    assert response.status_code == 201
    assert response.json()["contract_compatibility_required"] is True


def test_pipeline_run_create_route_returns_schema_compatibility_preview(api_client, db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(
            slug="orders-run-compat-preview-api",
            name="Orders Run Compat Preview API",
            gold_sql="SELECT * FROM source WHERE gold = TRUE",
        ),
    )
    DatasetService.save_schema_snapshot(db_session, dataset, "silver", [{"name": "id", "type": "BIGINT"}, {"name": "total", "type": "DOUBLE"}])
    DatasetService.save_schema_snapshot(db_session, dataset, "gold", [{"name": "id", "type": "BIGINT"}])
    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="orders_run_compat_preview_refresh",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )

    from data_platform.enums import IngestionStatus
    from data_platform.models.ingestion import IngestionJob

    db_session.add(
        IngestionJob(
            dataset_id=dataset.id,
            source_type="object_uri",
            status=IngestionStatus.SUCCEEDED.value,
            silver_object_uri="s3://silver/orders-run-compat-preview-api/job-1/part-00000.parquet",
        )
    )
    db_session.commit()

    response = api_client.post(f"/v1/pipelines/{pipeline.id}/runs", json={})

    assert response.status_code == 201
    payload = response.json()
    assert payload["schema_compatibility_preview"] is not None
    assert payload["schema_compatibility_preview"]["against_version"] == 1
    assert payload["schema_compatibility_preview"]["added_columns"] == [{"name": "total", "type": "DOUBLE"}]
    assert payload["schema_compatibility_preview"]["contract_compatible"] is True



def test_pipeline_run_claim_route_promotes_oldest_planned_run_to_pending(api_client, db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(
            slug="orders-run-claim-api",
            name="Orders Run Claim API",
            gold_sql="SELECT * FROM source WHERE gold = TRUE",
        ),
    )
    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="orders_run_claim_api_refresh",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )

    from data_platform.enums import IngestionStatus
    from data_platform.models.ingestion import IngestionJob

    older_job = IngestionJob(
        dataset_id=dataset.id,
        source_type="object_uri",
        status=IngestionStatus.SUCCEEDED.value,
        silver_object_uri="s3://silver/orders-run-claim-api/job-1/part-00000.parquet",
        created_at=datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc),
        updated_at=datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc),
    )
    newer_job = IngestionJob(
        dataset_id=dataset.id,
        source_type="object_uri",
        status=IngestionStatus.SUCCEEDED.value,
        silver_object_uri="s3://silver/orders-run-claim-api/job-2/part-00000.parquet",
        created_at=datetime(2026, 4, 14, 13, 0, tzinfo=timezone.utc),
        updated_at=datetime(2026, 4, 14, 13, 0, tzinfo=timezone.utc),
    )
    db_session.add_all([older_job, newer_job])
    db_session.commit()

    first_run = PipelineService.create_pipeline_run(
        db_session,
        pipeline,
        CreatePipelineRunRequest(source_ingestion_job_id=older_job.id),
    )
    PipelineService.create_pipeline_run(
        db_session,
        pipeline,
        CreatePipelineRunRequest(source_ingestion_job_id=newer_job.id),
    )

    response = api_client.post(f"/v1/pipelines/{pipeline.id}/runs/claim")

    assert response.status_code == 200
    payload = response.json()
    assert payload["id"] == first_run.id
    assert payload["status"] == "pending"
    assert payload["started_at"] is not None
    assert payload["finished_at"] is None



def test_pipeline_run_status_route_advances_claimed_run_to_terminal_state(api_client, db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(
            slug="orders-run-status-api",
            name="Orders Run Status API",
            gold_sql="SELECT * FROM source WHERE gold = TRUE",
        ),
    )
    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="orders_run_status_api_refresh",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )

    from data_platform.enums import IngestionStatus
    from data_platform.models.ingestion import IngestionJob

    db_session.add(
        IngestionJob(
            dataset_id=dataset.id,
            source_type="object_uri",
            status=IngestionStatus.SUCCEEDED.value,
            silver_object_uri="s3://silver/orders-run-status-api/job-1/part-00000.parquet",
        )
    )
    db_session.commit()

    planned = api_client.post(f"/v1/pipelines/{pipeline.id}/runs", json={})
    assert planned.status_code == 201
    run_id = planned.json()["id"]

    claimed = api_client.post(f"/v1/pipelines/{pipeline.id}/runs/claim")
    assert claimed.status_code == 200
    assert claimed.json()["id"] == run_id

    running = api_client.patch(
        f"/v1/pipelines/{pipeline.id}/runs/{run_id}/status",
        json={"status": "running"},
    )
    assert running.status_code == 200
    assert running.json()["status"] == "running"
    assert running.json()["finished_at"] is None

    failed = api_client.patch(
        f"/v1/pipelines/{pipeline.id}/runs/{run_id}/status",
        json={"status": "failed", "error_message": "warehouse timeout"},
    )
    assert failed.status_code == 200
    payload = failed.json()
    assert payload["status"] == "failed"
    assert payload["error_message"] == "warehouse timeout"
    assert payload["finished_at"] is not None



def test_pipeline_run_status_route_rejects_invalid_transition(api_client, db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(
            slug="orders-run-status-invalid-api",
            name="Orders Run Status Invalid API",
            gold_sql="SELECT * FROM source WHERE gold = TRUE",
        ),
    )
    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="orders_run_status_invalid_api_refresh",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )

    from data_platform.enums import IngestionStatus
    from data_platform.models.ingestion import IngestionJob

    db_session.add(
        IngestionJob(
            dataset_id=dataset.id,
            source_type="object_uri",
            status=IngestionStatus.SUCCEEDED.value,
            silver_object_uri="s3://silver/orders-run-status-invalid-api/job-1/part-00000.parquet",
        )
    )
    db_session.commit()

    planned = api_client.post(f"/v1/pipelines/{pipeline.id}/runs", json={})
    assert planned.status_code == 201
    run_id = planned.json()["id"]

    response = api_client.patch(
        f"/v1/pipelines/{pipeline.id}/runs/{run_id}/status",
        json={"status": "running"},
    )

    assert response.status_code == 409
    assert "cannot transition" in response.json()["detail"]
