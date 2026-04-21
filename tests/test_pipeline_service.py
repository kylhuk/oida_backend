from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from data_platform.enums import IngestionStatus
from data_platform.models.ingestion import IngestionJob
from data_platform.models.pipeline import PipelineRun
from data_platform.schemas.dataset import CreateDatasetRequest
from data_platform.schemas.pipeline import (
    CreatePipelineBackfillRunsRequest,
    CreatePipelineDefinitionRequest,
    CreatePipelineRunRequest,
    UpdatePipelineRunStatusRequest,
    UpdatePipelineDefinitionRequest,
)
from data_platform.services.dataset_service import DatasetService
from data_platform.services.pipeline_service import PipelineService


def test_create_and_list_pipeline_definitions_normalize_sql_definition(db_session):
    dataset = DatasetService.create_dataset(db_session, CreateDatasetRequest(slug="orders", name="Orders"))
    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="Orders silver to gold",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
            definition_json={},
        ),
    )

    pipelines = PipelineService.list_pipelines(db_session, dataset.id)
    assert len(pipelines) == 1
    assert pipelines[0].id == pipeline.id
    assert pipelines[0].definition_json == {"mode": "dataset_transform"}


def test_update_pipeline_definition_changes_name_and_active_flag(db_session):
    dataset = DatasetService.create_dataset(db_session, CreateDatasetRequest(slug="orders", name="Orders"))
    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="Orders silver to gold",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )

    updated = PipelineService.update_pipeline(
        db_session,
        pipeline,
        UpdatePipelineDefinitionRequest(name="Orders refresh", active=False),
    )

    assert updated.name == "Orders refresh"
    assert updated.active is False


def test_update_pipeline_definition_rejects_same_source_and_target(db_session):
    dataset = DatasetService.create_dataset(db_session, CreateDatasetRequest(slug="orders", name="Orders"))
    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="Orders silver to gold",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )

    with pytest.raises(ValueError):
        PipelineService.update_pipeline(
            db_session,
            pipeline,
            UpdatePipelineDefinitionRequest(target_layer="silver"),
        )


def test_build_execution_plan_uses_latest_successful_source_layer_artifact(db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(
            slug="orders",
            name="Orders",
            gold_sql="SELECT * FROM source WHERE gold = TRUE",
        ),
    )
    older_job = IngestionJob(
        dataset_id=dataset.id,
        source_type="object_uri",
        status=IngestionStatus.SUCCEEDED.value,
        silver_object_uri="s3://silver/orders/job-1/part-00000.parquet",
    )
    newer_job = IngestionJob(
        dataset_id=dataset.id,
        source_type="object_uri",
        status=IngestionStatus.SUCCEEDED.value,
        silver_object_uri="s3://silver/orders/job-2/part-00000.parquet",
    )
    db_session.add_all([older_job, newer_job])
    db_session.commit()

    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="Orders silver to gold",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )

    plan = PipelineService.build_execution_plan(db_session, pipeline)

    assert plan["executable"] is True
    assert plan["resolved_query"] == "SELECT * FROM source WHERE gold = TRUE"
    assert plan["source_ingestion_job_id"] == newer_job.id
    assert plan["source_object_uri"] == "s3://silver/orders/job-2/part-00000.parquet"


def test_build_execution_plan_uses_latest_successful_source_artifact_between_timestamps(db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(
            slug="orders-window",
            name="Orders Window",
            gold_sql="SELECT * FROM source WHERE gold = TRUE",
        ),
    )
    older_job = IngestionJob(
        dataset_id=dataset.id,
        source_type="object_uri",
        status=IngestionStatus.SUCCEEDED.value,
        finished_at=datetime(2026, 4, 14, 16, 0, tzinfo=timezone.utc),
        silver_object_uri="s3://silver/orders-window/job-1/part-00000.parquet",
    )
    selected_job = IngestionJob(
        dataset_id=dataset.id,
        source_type="object_uri",
        status=IngestionStatus.SUCCEEDED.value,
        finished_at=datetime(2026, 4, 14, 18, 0, tzinfo=timezone.utc),
        silver_object_uri="s3://silver/orders-window/job-2/part-00000.parquet",
    )
    newer_job = IngestionJob(
        dataset_id=dataset.id,
        source_type="object_uri",
        status=IngestionStatus.SUCCEEDED.value,
        finished_at=datetime(2026, 4, 14, 20, 0, tzinfo=timezone.utc),
        silver_object_uri="s3://silver/orders-window/job-3/part-00000.parquet",
    )
    db_session.add_all([older_job, selected_job, newer_job])
    db_session.commit()

    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="Orders silver to gold window",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )

    plan = PipelineService.build_execution_plan(
        db_session,
        pipeline,
        source_finished_at_gte=datetime(2026, 4, 14, 17, 0, tzinfo=timezone.utc),
        source_finished_at_lte=datetime(2026, 4, 14, 19, 0, tzinfo=timezone.utc),
    )

    assert plan["source_selection"] == "latest_successful_between"
    assert plan["requested_source_finished_at_gte"] == datetime(2026, 4, 14, 17, 0, tzinfo=timezone.utc)
    assert plan["requested_source_finished_at_lte"] == datetime(2026, 4, 14, 19, 0, tzinfo=timezone.utc)
    assert plan["source_ingestion_job_id"] == selected_job.id
    assert plan["source_object_uri"] == selected_job.silver_object_uri
    assert plan["executable"] is True


def test_build_execution_plan_reports_missing_source_artifact(db_session):
    dataset = DatasetService.create_dataset(db_session, CreateDatasetRequest(slug="orders", name="Orders"))
    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="Orders silver to gold",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )

    plan = PipelineService.build_execution_plan(db_session, pipeline)

    assert plan["executable"] is False
    assert plan["issues"] == [
        "No successful ingestion is available for source layer 'silver'.",
        "No source object URI is available for source layer 'silver'.",
    ]


def test_build_execution_plan_can_target_explicit_successful_source_job(db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(
            slug="orders",
            name="Orders",
            gold_sql="SELECT * FROM source WHERE gold = TRUE",
        ),
    )
    selected_job = IngestionJob(
        dataset_id=dataset.id,
        source_type="object_uri",
        status=IngestionStatus.SUCCEEDED.value,
        silver_object_uri="s3://silver/orders/job-older/part-00000.parquet",
    )
    latest_job = IngestionJob(
        dataset_id=dataset.id,
        source_type="object_uri",
        status=IngestionStatus.SUCCEEDED.value,
        silver_object_uri="s3://silver/orders/job-latest/part-00000.parquet",
    )
    db_session.add_all([selected_job, latest_job])
    db_session.commit()

    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="Orders silver to gold",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )

    plan = PipelineService.build_execution_plan(db_session, pipeline, source_ingestion_job_id=selected_job.id)

    assert plan["source_selection"] == "explicit"
    assert plan["requested_source_ingestion_job_id"] == selected_job.id
    assert plan["source_ingestion_job_id"] == selected_job.id
    assert plan["source_object_uri"] == "s3://silver/orders/job-older/part-00000.parquet"
    assert plan["executable"] is True



def test_build_execution_plan_reports_explicit_source_job_without_success_or_layer_artifact(db_session):
    dataset = DatasetService.create_dataset(db_session, CreateDatasetRequest(slug="orders", name="Orders"))
    source_job = IngestionJob(
        dataset_id=dataset.id,
        source_type="object_uri",
        status=IngestionStatus.FAILED.value,
        raw_object_uri="s3://raw/orders/job-1/source.csv",
    )
    db_session.add(source_job)
    db_session.commit()

    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="Orders silver to gold",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )

    plan = PipelineService.build_execution_plan(db_session, pipeline, source_ingestion_job_id=source_job.id)

    assert plan["source_selection"] == "explicit"
    assert plan["source_ingestion_job_id"] == source_job.id
    assert plan["source_job_status"] == IngestionStatus.FAILED.value
    assert plan["executable"] is False
    assert plan["issues"] == [
        f"Source ingestion job '{source_job.id}' is in status 'failed', expected 'succeeded'.",
        f"Source ingestion job '{source_job.id}' does not expose a 'silver' object URI.",
    ]



def test_build_execution_plan_rejects_explicit_source_job_for_different_dataset(db_session):
    orders = DatasetService.create_dataset(db_session, CreateDatasetRequest(slug="orders", name="Orders"))
    customers = DatasetService.create_dataset(db_session, CreateDatasetRequest(slug="customers", name="Customers"))
    foreign_job = IngestionJob(
        dataset_id=customers.id,
        source_type="object_uri",
        status=IngestionStatus.SUCCEEDED.value,
        silver_object_uri="s3://silver/customers/job-1/part-00000.parquet",
    )
    db_session.add(foreign_job)
    db_session.commit()

    pipeline = PipelineService.create_pipeline(
        db_session,
        orders,
        CreatePipelineDefinitionRequest(
            name="Orders silver to gold",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )

    with pytest.raises(ValueError, match="does not belong to dataset"):
        PipelineService.build_execution_plan(db_session, pipeline, source_ingestion_job_id=foreign_job.id)


def test_build_execution_plan_uses_latest_successful_source_artifact_at_or_before_timestamp(db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(
            slug="orders",
            name="Orders",
            gold_sql="SELECT * FROM source WHERE gold = TRUE",
        ),
    )
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

    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="Orders silver to gold",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )

    cutoff = datetime(2026, 4, 14, 18, 30, tzinfo=timezone.utc)
    plan = PipelineService.build_execution_plan(db_session, pipeline, source_finished_at_lte=cutoff)

    assert plan["source_selection"] == "latest_successful_at_or_before"
    assert plan["requested_source_finished_at_lte"] == cutoff
    assert plan["source_ingestion_job_id"] == older_job.id
    assert plan["source_object_uri"] == older_job.silver_object_uri
    assert plan["executable"] is True



def test_build_execution_plan_rejects_combined_explicit_and_time_bounded_source_selection(db_session):
    dataset = DatasetService.create_dataset(db_session, CreateDatasetRequest(slug="orders", name="Orders"))
    source_job = IngestionJob(
        dataset_id=dataset.id,
        source_type="object_uri",
        status=IngestionStatus.SUCCEEDED.value,
        silver_object_uri="s3://silver/orders/job-1/part-00000.parquet",
    )
    db_session.add(source_job)
    db_session.commit()

    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="Orders silver to gold",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )

    cutoff = datetime.now(timezone.utc) + timedelta(minutes=1)
    with pytest.raises(ValueError, match="cannot be combined"):
        PipelineService.build_execution_plan(
            db_session,
            pipeline,
            source_ingestion_job_id=source_job.id,
            source_finished_at_lte=cutoff,
        )


def test_get_pipeline_run_returns_only_runs_for_matching_pipeline(db_session):
    dataset = DatasetService.create_dataset(db_session, CreateDatasetRequest(slug="orders", name="Orders"))
    other_dataset = DatasetService.create_dataset(db_session, CreateDatasetRequest(slug="customers", name="Customers"))
    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="Orders silver to gold",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )
    other_pipeline = PipelineService.create_pipeline(
        db_session,
        other_dataset,
        CreatePipelineDefinitionRequest(
            name="Customers silver to gold",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )
    source_job = IngestionJob(
        dataset_id=dataset.id,
        source_type="object_uri",
        status=IngestionStatus.SUCCEEDED.value,
        silver_object_uri="s3://silver/orders/job-1/part-00000.parquet",
    )
    db_session.add(source_job)
    db_session.commit()

    run = PipelineService.create_pipeline_run(db_session, pipeline, CreatePipelineRunRequest())

    assert PipelineService.get_pipeline_run(db_session, pipeline.id, run.id).id == run.id
    assert PipelineService.get_pipeline_run(db_session, other_pipeline.id, run.id) is None



def test_build_pipeline_run_detail_exposes_preflight_fields(db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(
            slug="orders",
            name="Orders",
            gold_sql="SELECT * FROM source WHERE gold = TRUE",
        ),
    )
    source_job = IngestionJob(
        dataset_id=dataset.id,
        source_type="object_uri",
        status=IngestionStatus.SUCCEEDED.value,
        silver_object_uri="s3://silver/orders/job-1/part-00000.parquet",
    )
    db_session.add(source_job)
    db_session.commit()
    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="Orders silver to gold",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )

    run = PipelineService.create_pipeline_run(db_session, pipeline, CreatePipelineRunRequest())
    detail = PipelineService.build_pipeline_run_detail(run)

    assert detail["id"] == run.id
    assert detail["preflighted_at"] is not None
    assert detail["execution_plan"]["source_object_uri"] == source_job.silver_object_uri
    assert detail["execution_plan"]["resolved_query"] == "SELECT * FROM source WHERE gold = TRUE"
    assert detail["backfill_request"] is None


def test_build_pipeline_run_detail_exposes_backfill_request_fields(db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(
            slug="orders-run-detail-backfill",
            name="Orders Run Detail Backfill",
            gold_sql="SELECT * FROM source WHERE gold = TRUE",
        ),
    )
    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="Orders run detail backfill",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )

    older_job = IngestionJob(
        dataset_id=dataset.id,
        source_type="object_uri",
        status=IngestionStatus.SUCCEEDED.value,
        silver_object_uri="s3://silver/orders-run-detail-backfill/job-1/part-00000.parquet",
        finished_at=datetime(2026, 4, 14, 10, 0, tzinfo=timezone.utc),
    )
    db_session.add(older_job)
    db_session.commit()

    runs = PipelineService.create_pipeline_backfill_runs(
        db_session,
        pipeline,
        CreatePipelineBackfillRunsRequest(
            run_ref_prefix=" nightly backfill ",
            source_finished_at_gte=datetime(2026, 4, 14, 9, 30, tzinfo=timezone.utc),
            source_finished_at_lte=datetime(2026, 4, 14, 10, 30, tzinfo=timezone.utc),
        ),
    )

    detail = PipelineService.build_pipeline_run_detail(runs[0])

    assert detail["backfill_request"] == {
        "run_ref_prefix": "nightly backfill",
        "source_finished_at_gte": datetime(2026, 4, 14, 9, 30, tzinfo=timezone.utc),
        "source_finished_at_lte": datetime(2026, 4, 14, 10, 30, tzinfo=timezone.utc),
        "limit": 100,
        "offset": 0,
    }


def test_build_pipeline_run_response_matches_detail_fields(db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(
            slug="orders-run-response",
            name="Orders Run Response",
            gold_sql="SELECT * FROM source WHERE gold = TRUE",
        ),
    )
    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="orders_run_response_refresh",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )
    source_job = IngestionJob(
        dataset_id=dataset.id,
        source_type="object_uri",
        status=IngestionStatus.SUCCEEDED.value,
        silver_object_uri="s3://silver/orders-run-response/job-1/part-00000.parquet",
        finished_at=datetime(2026, 4, 14, 10, 0, tzinfo=timezone.utc),
    )
    db_session.add(source_job)
    db_session.commit()

    run = PipelineService.create_pipeline_backfill_runs(
        db_session,
        pipeline,
        CreatePipelineBackfillRunsRequest(
            run_ref_prefix=" nightly backfill ",
            source_finished_at_gte=datetime(2026, 4, 14, 9, 30, tzinfo=timezone.utc),
            source_finished_at_lte=datetime(2026, 4, 14, 10, 30, tzinfo=timezone.utc),
        ),
    )[0]

    response = PipelineService.build_pipeline_run_response(run)
    detail = PipelineService.build_pipeline_run_detail(run)

    assert response == detail
    assert response["preflighted_at"] is not None
    assert response["backfill_request"] is not None
    assert response["execution_plan"]["source_ingestion_job_id"] == source_job.id


def test_create_pipeline_run_persists_planned_preflight_with_execution_plan_snapshot(db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(
            slug="orders",
            name="Orders",
            gold_sql="SELECT * FROM source WHERE gold = TRUE",
        ),
    )
    source_job = IngestionJob(
        dataset_id=dataset.id,
        source_type="object_uri",
        status=IngestionStatus.SUCCEEDED.value,
        silver_object_uri="s3://silver/orders/job-1/part-00000.parquet",
    )
    db_session.add(source_job)
    db_session.commit()

    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="Orders silver to gold",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )

    run = PipelineService.create_pipeline_run(
        db_session,
        pipeline,
        CreatePipelineRunRequest(run_ref=" nightly refresh "),
    )

    assert run.status == "planned"
    assert run.run_ref == "nightly refresh"
    assert run.ingestion_job_id == source_job.id
    assert run.error_message is None
    assert run.finished_at is None
    assert run.metrics_json["execution_plan"]["source_object_uri"] == source_job.silver_object_uri
    assert run.metrics_json["execution_plan"]["resolved_query"] == "SELECT * FROM source WHERE gold = TRUE"


def test_create_pipeline_run_persists_blocked_preflight_when_execution_plan_has_issues(db_session):
    dataset = DatasetService.create_dataset(db_session, CreateDatasetRequest(slug="orders", name="Orders"))
    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="Orders silver to gold",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )

    run = PipelineService.create_pipeline_run(db_session, pipeline, CreatePipelineRunRequest())

    assert run.status == "blocked"
    assert run.ingestion_job_id is None
    assert run.finished_at is not None
    assert "No successful ingestion is available" in run.error_message
    assert run.metrics_json["execution_plan"]["issues"] == [
        "No successful ingestion is available for source layer 'silver'.",
        "No source object URI is available for source layer 'silver'.",
    ]


def test_build_execution_plan_rejects_invalid_time_window(db_session):
    dataset = DatasetService.create_dataset(db_session, CreateDatasetRequest(slug="orders-invalid-window", name="Orders Invalid Window"))
    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="Orders silver to gold",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )

    with pytest.raises(ValueError, match="cannot be after"):
        PipelineService.build_execution_plan(
            db_session,
            pipeline,
            source_finished_at_gte=datetime(2026, 4, 14, 19, 0, tzinfo=timezone.utc),
            source_finished_at_lte=datetime(2026, 4, 14, 18, 30, tzinfo=timezone.utc),
        )




def test_count_pipeline_runs_can_filter_by_status(db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(slug="orders-run-count-status", name="Orders Run Count Status", gold_sql="SELECT * FROM source")
    )
    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="Orders run count status",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )

    runnable_job = IngestionJob(
        dataset_id=dataset.id,
        source_type="object_uri",
        status=IngestionStatus.SUCCEEDED.value,
        silver_object_uri="s3://silver/orders-run-count-status/job-1/part-00000.parquet",
    )
    db_session.add(runnable_job)
    db_session.commit()

    PipelineService.create_pipeline_run(db_session, pipeline, CreatePipelineRunRequest(run_ref="planned"))
    PipelineService.create_pipeline_run(
        db_session,
        pipeline,
        CreatePipelineRunRequest(source_finished_at_gte=datetime(2030, 1, 1, tzinfo=timezone.utc)),
    )

    assert PipelineService.count_pipeline_runs(db_session, pipeline.id, run_status="planned") == 1
    assert PipelineService.count_pipeline_runs(db_session, pipeline.id, run_status="blocked") == 1


def test_list_pipeline_runs_can_filter_by_status(db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(slug="orders-run-filter", name="Orders Run Filter", gold_sql="SELECT * FROM source")
    )
    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="Orders run filter",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )

    runnable_job = IngestionJob(
        dataset_id=dataset.id,
        source_type="object_uri",
        status=IngestionStatus.SUCCEEDED.value,
        silver_object_uri="s3://silver/orders-run-filter/job-1/part-00000.parquet",
    )
    db_session.add(runnable_job)
    db_session.commit()

    planned_run = PipelineService.create_pipeline_run(db_session, pipeline, CreatePipelineRunRequest(run_ref="planned"))
    blocked_run = PipelineService.create_pipeline_run(
        db_session,
        pipeline,
        CreatePipelineRunRequest(
            run_ref="blocked",
            source_finished_at_gte=datetime(2030, 1, 1, tzinfo=timezone.utc),
        ),
    )

    planned_runs = PipelineService.list_pipeline_runs(db_session, pipeline.id, run_status="planned")
    blocked_runs = PipelineService.list_pipeline_runs(db_session, pipeline.id, run_status="blocked")

    assert [run.id for run in planned_runs] == [planned_run.id]
    assert [run.id for run in blocked_runs] == [blocked_run.id]



def test_list_pipeline_runs_can_filter_by_run_ref(db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(slug="orders-run-ref-filter", name="Orders Run Ref Filter", gold_sql="SELECT * FROM source")
    )
    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="Orders run ref filter",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )

    runnable_job = IngestionJob(
        dataset_id=dataset.id,
        source_type="object_uri",
        status=IngestionStatus.SUCCEEDED.value,
        silver_object_uri="s3://silver/orders-run-ref-filter/job-1/part-00000.parquet",
    )
    db_session.add(runnable_job)
    db_session.commit()

    selected_run = PipelineService.create_pipeline_run(db_session, pipeline, CreatePipelineRunRequest(run_ref="nightly refresh"))
    PipelineService.create_pipeline_run(db_session, pipeline, CreatePipelineRunRequest(run_ref="hourly refresh"))

    filtered_runs = PipelineService.list_pipeline_runs(db_session, pipeline.id, run_ref="  nightly refresh  ")

    assert [run.id for run in filtered_runs] == [selected_run.id]


def test_list_pipeline_runs_can_filter_by_source_ingestion_job_id(db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(slug="orders-run-source-filter", name="Orders Run Source Filter", gold_sql="SELECT * FROM source")
    )
    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="Orders run source filter",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )

    selected_job = IngestionJob(
        dataset_id=dataset.id,
        source_type="object_uri",
        status=IngestionStatus.SUCCEEDED.value,
        silver_object_uri="s3://silver/orders-run-source-filter/job-1/part-00000.parquet",
    )
    other_job = IngestionJob(
        dataset_id=dataset.id,
        source_type="object_uri",
        status=IngestionStatus.SUCCEEDED.value,
        silver_object_uri="s3://silver/orders-run-source-filter/job-2/part-00000.parquet",
    )
    db_session.add_all([selected_job, other_job])
    db_session.commit()

    selected_run = PipelineService.create_pipeline_run(
        db_session,
        pipeline,
        CreatePipelineRunRequest(source_ingestion_job_id=f"  {selected_job.id}  "),
    )
    PipelineService.create_pipeline_run(
        db_session,
        pipeline,
        CreatePipelineRunRequest(source_ingestion_job_id=other_job.id),
    )

    filtered_runs = PipelineService.list_pipeline_runs(
        db_session,
        pipeline.id,
        source_ingestion_job_id=f"  {selected_job.id}  ",
    )

    assert [run.id for run in filtered_runs] == [selected_run.id]


def test_list_pipeline_runs_rejects_blank_source_ingestion_job_id(db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(slug="orders-run-source-filter-blank", name="Orders Run Source Filter Blank")
    )
    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="Orders run source filter blank",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )

    with pytest.raises(ValueError, match="source_ingestion_job_id cannot be empty"):
        PipelineService.list_pipeline_runs(db_session, pipeline.id, source_ingestion_job_id="   ")


def test_list_pipeline_runs_can_filter_by_created_at_range(db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(slug="orders-run-created-filter", name="Orders Run Created Filter", gold_sql="SELECT * FROM source")
    )
    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="Orders run created filter",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )

    runnable_job = IngestionJob(
        dataset_id=dataset.id,
        source_type="object_uri",
        status=IngestionStatus.SUCCEEDED.value,
        silver_object_uri="s3://silver/orders-run-created-filter/job-1/part-00000.parquet",
    )
    db_session.add(runnable_job)
    db_session.commit()

    older_run = PipelineService.create_pipeline_run(db_session, pipeline, CreatePipelineRunRequest(run_ref="older"))
    newer_run = PipelineService.create_pipeline_run(db_session, pipeline, CreatePipelineRunRequest(run_ref="newer"))

    lower_bound = older_run.created_at + timedelta(microseconds=1)
    filtered_runs = PipelineService.list_pipeline_runs(
        db_session,
        pipeline.id,
        created_at_gte=lower_bound,
    )
    assert [run.id for run in filtered_runs] == [newer_run.id]

    count = PipelineService.count_pipeline_runs(
        db_session,
        pipeline.id,
        created_at_gte=lower_bound,
    )
    assert count == 1


def test_pipeline_run_filters_reject_inverted_created_at_range(db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(slug="orders-run-created-range-invalid", name="Orders Run Created Range Invalid")
    )
    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="Orders run created range invalid",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )

    start = datetime(2026, 4, 15, 0, 0, tzinfo=timezone.utc)
    end = datetime(2026, 4, 14, 0, 0, tzinfo=timezone.utc)

    with pytest.raises(ValueError, match="created_at_gte cannot be after created_at_lte"):
        PipelineService.list_pipeline_runs(db_session, pipeline.id, created_at_gte=start, created_at_lte=end)

    with pytest.raises(ValueError, match="created_at_gte cannot be after created_at_lte"):
        PipelineService.count_pipeline_runs(db_session, pipeline.id, created_at_gte=start, created_at_lte=end)



def test_list_pipeline_source_candidates_returns_latest_successful_source_jobs(db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(slug="orders-source-candidates", name="Orders Source Candidates", gold_sql="SELECT * FROM source")
    )
    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="Orders source candidates",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )

    older_job = IngestionJob(
        dataset_id=dataset.id,
        source_type="object_uri",
        status=IngestionStatus.SUCCEEDED.value,
        silver_object_uri="s3://silver/orders-source-candidates/job-1/part-00000.parquet",
        finished_at=datetime(2026, 4, 14, 10, 0, tzinfo=timezone.utc),
    )
    newer_job = IngestionJob(
        dataset_id=dataset.id,
        source_type="object_uri",
        status=IngestionStatus.SUCCEEDED.value,
        silver_object_uri="s3://silver/orders-source-candidates/job-2/part-00000.parquet",
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

    candidates = PipelineService.list_pipeline_source_candidates(db_session, pipeline)

    assert [candidate["ingestion_job_id"] for candidate in candidates[:2]] == [newer_job.id, older_job.id]
    assert candidates[0]["object_uri"] == newer_job.silver_object_uri
    assert candidates[0]["existing_run_count"] == 2
    assert candidates[0]["has_existing_run"] is True
    assert candidates[0]["latest_run_id"] == existing_run.id
    assert candidates[0]["latest_run_status"] == "planned"
    assert candidates[0]["latest_run_ref"] == "nightly refresh"
    assert candidates[0]["latest_run_created_at"] == existing_run.created_at
    assert candidates[1]["existing_run_count"] == 0
    assert candidates[1]["has_existing_run"] is False
    assert candidates[1]["latest_run_id"] is None


def test_list_pipeline_source_candidates_respects_time_window(db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(slug="orders-source-candidates-window", name="Orders Source Candidates Window", gold_sql="SELECT * FROM source")
    )
    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="Orders source candidates window",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )

    older_job = IngestionJob(
        dataset_id=dataset.id,
        source_type="object_uri",
        status=IngestionStatus.SUCCEEDED.value,
        silver_object_uri="s3://silver/orders-source-candidates-window/job-1/part-00000.parquet",
        finished_at=datetime(2026, 4, 14, 10, 0, tzinfo=timezone.utc),
    )
    newer_job = IngestionJob(
        dataset_id=dataset.id,
        source_type="object_uri",
        status=IngestionStatus.SUCCEEDED.value,
        silver_object_uri="s3://silver/orders-source-candidates-window/job-2/part-00000.parquet",
        finished_at=datetime(2026, 4, 14, 11, 0, tzinfo=timezone.utc),
    )
    db_session.add_all([older_job, newer_job])
    db_session.commit()

    candidates = PipelineService.list_pipeline_source_candidates(
        db_session,
        pipeline,
        source_finished_at_gte=datetime(2026, 4, 14, 10, 30, tzinfo=timezone.utc),
        source_finished_at_lte=datetime(2026, 4, 14, 11, 30, tzinfo=timezone.utc),
    )

    assert [candidate["ingestion_job_id"] for candidate in candidates] == [newer_job.id]




def test_list_pipeline_source_candidates_can_filter_on_existing_run_state(db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(slug="orders-source-candidates-existing-filter", name="Orders Source Candidates Existing Filter", gold_sql="SELECT * FROM source")
    )
    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="Orders source candidates existing filter",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )

    older_job = IngestionJob(
        dataset_id=dataset.id,
        source_type="object_uri",
        status=IngestionStatus.SUCCEEDED.value,
        silver_object_uri="s3://silver/orders-source-candidates-existing-filter/job-1/part-00000.parquet",
        finished_at=datetime(2026, 4, 14, 10, 0, tzinfo=timezone.utc),
    )
    newer_job = IngestionJob(
        dataset_id=dataset.id,
        source_type="object_uri",
        status=IngestionStatus.SUCCEEDED.value,
        silver_object_uri="s3://silver/orders-source-candidates-existing-filter/job-2/part-00000.parquet",
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

    existing_only = PipelineService.list_pipeline_source_candidates(db_session, pipeline, has_existing_run=True)
    missing_only = PipelineService.list_pipeline_source_candidates(db_session, pipeline, has_existing_run=False)

    assert [candidate["ingestion_job_id"] for candidate in existing_only] == [newer_job.id]
    assert [candidate["ingestion_job_id"] for candidate in missing_only] == [older_job.id]


def test_list_pipeline_source_candidates_rejects_conflicting_existing_run_filters(db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(slug="orders-source-candidates-existing-invalid", name="Orders Source Candidates Existing Invalid")
    )
    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="Orders source candidates existing invalid",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )

    with pytest.raises(ValueError, match="exclude_existing_runs cannot be combined with has_existing_run=true"):
        PipelineService.list_pipeline_source_candidates(
            db_session,
            pipeline,
            exclude_existing_runs=True,
            has_existing_run=True,
        )

    with pytest.raises(ValueError, match="exclude_existing_runs cannot be combined with has_existing_run=true"):
        PipelineService.count_pipeline_source_candidates(
            db_session,
            pipeline,
            exclude_existing_runs=True,
            has_existing_run=True,
        )

def test_list_pipeline_source_candidates_rejects_invalid_time_window(db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(slug="orders-source-candidates-invalid", name="Orders Source Candidates Invalid")
    )
    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="Orders source candidates invalid",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )

    with pytest.raises(ValueError, match="source_finished_at_gte cannot be after source_finished_at_lte"):
        PipelineService.list_pipeline_source_candidates(
            db_session,
            pipeline,
            source_finished_at_gte=datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc),
            source_finished_at_lte=datetime(2026, 4, 14, 11, 0, tzinfo=timezone.utc),
        )


def test_create_pipeline_backfill_runs_materializes_windowed_preflights(db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(
            slug="orders-backfill-runs",
            name="Orders Backfill Runs",
            gold_sql="SELECT * FROM source WHERE gold = TRUE",
        ),
    )
    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="orders_backfill_refresh",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )

    older_job = IngestionJob(
        dataset_id=dataset.id,
        source_type="object_uri",
        status=IngestionStatus.SUCCEEDED.value,
        silver_object_uri="s3://silver/orders-backfill-runs/job-1/part-00000.parquet",
        finished_at=datetime(2026, 4, 14, 10, 0, tzinfo=timezone.utc),
    )
    newer_job = IngestionJob(
        dataset_id=dataset.id,
        source_type="object_uri",
        status=IngestionStatus.SUCCEEDED.value,
        silver_object_uri="s3://silver/orders-backfill-runs/job-2/part-00000.parquet",
        finished_at=datetime(2026, 4, 14, 11, 0, tzinfo=timezone.utc),
    )
    db_session.add_all([older_job, newer_job])
    db_session.commit()

    runs = PipelineService.create_pipeline_backfill_runs(
        db_session,
        pipeline,
        CreatePipelineBackfillRunsRequest(
            run_ref_prefix=" nightly backfill ",
            source_finished_at_gte=datetime(2026, 4, 14, 9, 30, tzinfo=timezone.utc),
            source_finished_at_lte=datetime(2026, 4, 14, 11, 30, tzinfo=timezone.utc),
        ),
    )

    assert [run.ingestion_job_id for run in runs] == [newer_job.id, older_job.id]
    assert [run.run_ref for run in runs] == [
        f"nightly backfill:{newer_job.id}",
        f"nightly backfill:{older_job.id}",
    ]
    assert all(run.status == "planned" for run in runs)
    assert runs[0].metrics_json["backfill_request"]["run_ref_prefix"] == "nightly backfill"
    assert runs[0].metrics_json["backfill_request"]["source_finished_at_gte"] == "2026-04-14T09:30:00+00:00"


def test_create_pipeline_backfill_runs_skips_existing_runs_when_requested(db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(
            slug="orders-backfill-dedup",
            name="Orders Backfill Dedup",
            gold_sql="SELECT * FROM source WHERE gold = TRUE",
        ),
    )
    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="orders_backfill_dedup_refresh",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )

    older_job = IngestionJob(
        dataset_id=dataset.id,
        source_type="object_uri",
        status=IngestionStatus.SUCCEEDED.value,
        silver_object_uri="s3://silver/orders-backfill-dedup/job-1/part-00000.parquet",
        finished_at=datetime(2026, 4, 14, 10, 0, tzinfo=timezone.utc),
    )
    newer_job = IngestionJob(
        dataset_id=dataset.id,
        source_type="object_uri",
        status=IngestionStatus.SUCCEEDED.value,
        silver_object_uri="s3://silver/orders-backfill-dedup/job-2/part-00000.parquet",
        finished_at=datetime(2026, 4, 14, 11, 0, tzinfo=timezone.utc),
    )
    db_session.add_all([older_job, newer_job])
    db_session.commit()

    existing_run = PipelineService.create_pipeline_run(
        db_session,
        pipeline,
        CreatePipelineRunRequest(source_ingestion_job_id=older_job.id),
    )

    runs = PipelineService.create_pipeline_backfill_runs(
        db_session,
        pipeline,
        CreatePipelineBackfillRunsRequest(
            run_ref_prefix="nightly backfill",
            source_finished_at_gte=datetime(2026, 4, 14, 9, 30, tzinfo=timezone.utc),
            source_finished_at_lte=datetime(2026, 4, 14, 11, 30, tzinfo=timezone.utc),
            skip_existing_runs=True,
        ),
    )

    assert existing_run.ingestion_job_id == older_job.id
    assert [run.ingestion_job_id for run in runs] == [newer_job.id]
    assert runs[0].metrics_json["backfill_request"]["skip_existing_runs"] is True


def test_create_pipeline_backfill_runs_returns_empty_list_when_no_candidates_match(db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(slug="orders-backfill-empty", name="Orders Backfill Empty", gold_sql="SELECT * FROM source"),
    )
    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="orders_backfill_empty_refresh",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )

    runs = PipelineService.create_pipeline_backfill_runs(
        db_session,
        pipeline,
        CreatePipelineBackfillRunsRequest(
            source_finished_at_gte=datetime(2026, 4, 14, 9, 30, tzinfo=timezone.utc),
            source_finished_at_lte=datetime(2026, 4, 14, 11, 30, tzinfo=timezone.utc),
        ),
    )

    assert runs == []


def test_pipeline_service_source_candidate_query_tracks_latest_run_terminal_context():
    from pathlib import Path

    service_source = Path("src/data_platform/services/pipeline_service.py").read_text()
    assert "PipelineRun.finished_at" in service_source
    assert "PipelineRun.error_message" in service_source
    assert '"latest_run_finished_at": finished_at' in service_source
    assert '"latest_run_error_message": error_message' in service_source


def test_create_pipeline_run_rejects_when_contract_compatibility_is_required_but_unavailable(db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(
            slug="orders-compat-required-unavailable",
            name="Orders Compat Required Unavailable",
            gold_sql="SELECT * FROM source",
        ),
    )
    DatasetService.save_schema_snapshot(db_session, dataset, "silver", [{"name": "id", "type": "BIGINT"}])
    source_job = IngestionJob(
        dataset_id=dataset.id,
        source_type="object_uri",
        status=IngestionStatus.SUCCEEDED.value,
        silver_object_uri="s3://silver/orders-compat-required-unavailable/job-1/part-00000.parquet",
    )
    db_session.add(source_job)
    db_session.commit()

    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="Orders compat required unavailable run",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
            definition_json={"mode": "custom_sql", "sql": "SELECT * FROM source"},
        ),
    )

    with pytest.raises(ValueError, match="Compatibility preview is only available for sql dataset-transform pipelines."):
        PipelineService.create_pipeline_run(
            db_session,
            pipeline,
            CreatePipelineRunRequest(require_contract_compatible_schema=True),
        )


def test_create_pipeline_run_persists_rejected_preflight_attempt_when_contract_compatibility_is_required_but_unavailable(db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(
            slug="orders-compat-required-unavailable-preflight",
            name="Orders Compat Required Unavailable Preflight",
            gold_sql="SELECT * FROM source",
        ),
    )
    DatasetService.save_schema_snapshot(db_session, dataset, "silver", [{"name": "id", "type": "BIGINT"}])
    source_job = IngestionJob(
        dataset_id=dataset.id,
        source_type="object_uri",
        status=IngestionStatus.SUCCEEDED.value,
        silver_object_uri="s3://silver/orders-compat-required-unavailable-preflight/job-1/part-00000.parquet",
    )
    db_session.add(source_job)
    db_session.commit()

    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="Orders compat required unavailable preflight run",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
            definition_json={"mode": "custom_sql", "sql": "SELECT * FROM source"},
        ),
    )

    with pytest.raises(ValueError, match="Compatibility preview is only available for sql dataset-transform pipelines."):
        PipelineService.create_pipeline_run(
            db_session,
            pipeline,
            CreatePipelineRunRequest(require_contract_compatible_schema=True),
        )

    attempts = PipelineService.list_pipeline_preflight_attempts(db_session, pipeline.id)

    assert len(attempts) == 1
    attempt = attempts[0]
    assert attempt.request_kind == "run"
    assert attempt.ingestion_job_id == source_job.id
    assert attempt.error_message == "Compatibility preview is only available for sql dataset-transform pipelines."
    assert attempt.metrics_json["schema_context"]["contract_compatibility_outcome"] == "required_preview_unavailable"
    assert attempt.metrics_json["schema_context"]["contract_compatibility_required"] is True
    assert PipelineService.list_pipeline_runs(db_session, pipeline.id) == []



def test_create_pipeline_run_persists_rejected_preflight_attempt_when_contract_compatibility_is_incompatible(db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(
            slug="orders-compat-required-incompatible-preflight",
            name="Orders Compat Required Incompatible Preflight",
            gold_sql="SELECT * FROM source WHERE gold = TRUE",
        ),
    )
    DatasetService.save_schema_snapshot(db_session, dataset, "silver", [{"name": "id", "type": "BIGINT"}])
    DatasetService.save_schema_snapshot(
        db_session,
        dataset,
        "gold",
        [{"name": "id", "type": "BIGINT"}, {"name": "legacy_total", "type": "DOUBLE"}],
    )
    source_job = IngestionJob(
        dataset_id=dataset.id,
        source_type="object_uri",
        status=IngestionStatus.SUCCEEDED.value,
        silver_object_uri="s3://silver/orders-compat-required-incompatible-preflight/job-1/part-00000.parquet",
    )
    db_session.add(source_job)
    db_session.commit()

    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="Orders compat required incompatible preflight run",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )

    with pytest.raises(ValueError, match="Schema compatibility preview indicates contract-incompatible schema changes."):
        PipelineService.create_pipeline_run(
            db_session,
            pipeline,
            CreatePipelineRunRequest(require_contract_compatible_schema=True),
        )

    attempts = PipelineService.list_pipeline_preflight_attempts(db_session, pipeline.id)

    assert len(attempts) == 1
    attempt = attempts[0]
    assert attempt.request_kind == "run"
    assert attempt.metrics_json["schema_context"]["contract_compatibility_outcome"] == "required_incompatible"
    assert attempt.metrics_json["schema_context"]["schema_compatibility_preview"]["contract_compatible"] is False
    assert attempt.error_message == "Schema compatibility preview indicates contract-incompatible schema changes."
    assert PipelineService.list_pipeline_runs(db_session, pipeline.id) == []



def test_create_pipeline_backfill_runs_persists_rejected_preflight_attempt_when_contract_compatibility_is_required(db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(
            slug="orders-backfill-compat-required-preflight",
            name="Orders Backfill Compat Required Preflight",
            gold_sql="SELECT * FROM source",
        ),
    )
    DatasetService.save_schema_snapshot(db_session, dataset, "silver", [{"name": "id", "type": "BIGINT"}])
    source_job = IngestionJob(
        dataset_id=dataset.id,
        source_type="object_uri",
        status=IngestionStatus.SUCCEEDED.value,
        created_at=datetime(2026, 4, 14, 10, 0, tzinfo=timezone.utc),
        finished_at=datetime(2026, 4, 14, 10, 5, tzinfo=timezone.utc),
        silver_object_uri="s3://silver/orders-backfill-compat-required-preflight/job-1/part-00000.parquet",
    )
    db_session.add(source_job)
    db_session.commit()

    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="Orders backfill compat required preflight run",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
            definition_json={"mode": "custom_sql", "sql": "SELECT * FROM source"},
        ),
    )

    with pytest.raises(ValueError, match="Compatibility preview is only available for sql dataset-transform pipelines."):
        PipelineService.create_pipeline_backfill_runs(
            db_session,
            pipeline,
            CreatePipelineBackfillRunsRequest(
                run_ref_prefix="nightly",
                source_finished_at_gte=datetime(2026, 4, 14, 9, 0, tzinfo=timezone.utc),
                source_finished_at_lte=datetime(2026, 4, 14, 11, 0, tzinfo=timezone.utc),
                require_contract_compatible_schema=True,
            ),
        )

    attempts = PipelineService.list_pipeline_preflight_attempts(db_session, pipeline.id)

    assert len(attempts) == 1
    attempt = attempts[0]
    assert attempt.request_kind == "backfill"
    assert attempt.run_ref == f"nightly:{source_job.id}"
    assert attempt.metrics_json["backfill_request"]["require_contract_compatible_schema"] is True
    assert attempt.metrics_json["schema_context"]["contract_compatibility_outcome"] == "required_preview_unavailable"
    assert PipelineService.list_pipeline_runs(db_session, pipeline.id) == []


def test_create_pipeline_run_persists_schema_compatibility_preview_unavailable_reason_for_custom_sql(db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(
            slug="orders-compat-preview-unavailable",
            name="Orders Compat Preview Unavailable",
            gold_sql="SELECT * FROM source",
        ),
    )
    DatasetService.save_schema_snapshot(db_session, dataset, "silver", [{"name": "id", "type": "BIGINT"}])
    source_job = IngestionJob(
        dataset_id=dataset.id,
        source_type="object_uri",
        status=IngestionStatus.SUCCEEDED.value,
        silver_object_uri="s3://silver/orders-compat-preview-unavailable/job-1/part-00000.parquet",
    )
    db_session.add(source_job)
    db_session.commit()

    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="Orders compat preview unavailable run",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
            definition_json={"mode": "custom_sql", "sql": "SELECT * FROM source"},
        ),
    )

    run = PipelineService.create_pipeline_run(db_session, pipeline, CreatePipelineRunRequest())

    assert run.metrics_json["schema_context"]["schema_compatibility_preview_unavailable_reason"] == (
        "Compatibility preview is only available for sql dataset-transform pipelines."
    )
    assert "schema_compatibility_preview" not in run.metrics_json["schema_context"]



def test_create_pipeline_run_persists_contract_compatibility_requirement_flag(db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(
            slug="orders-compat-required-preview",
            name="Orders Compat Required Preview",
            gold_sql="SELECT * FROM source WHERE gold = TRUE",
        ),
    )
    DatasetService.save_schema_snapshot(db_session, dataset, "silver", [{"name": "id", "type": "BIGINT"}])
    source_job = IngestionJob(
        dataset_id=dataset.id,
        source_type="object_uri",
        status=IngestionStatus.SUCCEEDED.value,
        silver_object_uri="s3://silver/orders-compat-required-preview/job-1/part-00000.parquet",
    )
    db_session.add(source_job)
    db_session.commit()

    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="Orders compat required preview run",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )

    run = PipelineService.create_pipeline_run(
        db_session,
        pipeline,
        CreatePipelineRunRequest(require_contract_compatible_schema=True),
    )

    assert run.metrics_json["schema_context"]["contract_compatibility_required"] is True


def test_create_pipeline_run_persists_schema_compatibility_preview_for_dataset_transform(db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(
            slug="orders-compat-preview",
            name="Orders Compat Preview",
            gold_sql="SELECT * FROM source WHERE gold = TRUE",
        ),
    )
    DatasetService.save_schema_snapshot(db_session, dataset, "silver", [{"name": "id", "type": "BIGINT"}, {"name": "total", "type": "DOUBLE"}])
    DatasetService.save_schema_snapshot(db_session, dataset, "gold", [{"name": "id", "type": "BIGINT"}])
    source_job = IngestionJob(
        dataset_id=dataset.id,
        source_type="object_uri",
        status=IngestionStatus.SUCCEEDED.value,
        silver_object_uri="s3://silver/orders-compat-preview/job-1/part-00000.parquet",
    )
    db_session.add(source_job)
    db_session.commit()

    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="Orders compat preview run",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )

    run = PipelineService.create_pipeline_run(db_session, pipeline, CreatePipelineRunRequest())

    preview = run.metrics_json["schema_context"]["schema_compatibility_preview"]
    assert preview["against_version"] == 1
    assert preview["added_columns"] == [{"name": "total", "type": "DOUBLE"}]
    assert preview["contract_compatible"] is True



def test_claim_next_pipeline_run_promotes_oldest_planned_run_to_pending(db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(
            slug="orders-run-claim",
            name="Orders Run Claim",
            gold_sql="SELECT * FROM source WHERE gold = TRUE",
        ),
    )
    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="orders_run_claim_refresh",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )
    older_job = IngestionJob(
        dataset_id=dataset.id,
        source_type="object_uri",
        status=IngestionStatus.SUCCEEDED.value,
        silver_object_uri="s3://silver/orders-run-claim/job-1/part-00000.parquet",
        created_at=datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc),
        updated_at=datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc),
    )
    newer_job = IngestionJob(
        dataset_id=dataset.id,
        source_type="object_uri",
        status=IngestionStatus.SUCCEEDED.value,
        silver_object_uri="s3://silver/orders-run-claim/job-2/part-00000.parquet",
        created_at=datetime(2026, 4, 14, 13, 0, tzinfo=timezone.utc),
        updated_at=datetime(2026, 4, 14, 13, 0, tzinfo=timezone.utc),
    )
    db_session.add_all([older_job, newer_job])
    db_session.commit()

    older_run = PipelineService.create_pipeline_run(
        db_session,
        pipeline,
        CreatePipelineRunRequest(source_ingestion_job_id=older_job.id, run_ref="older"),
    )
    newer_run = PipelineService.create_pipeline_run(
        db_session,
        pipeline,
        CreatePipelineRunRequest(source_ingestion_job_id=newer_job.id, run_ref="newer"),
    )

    claimed = PipelineService.claim_next_pipeline_run(db_session, pipeline)

    assert claimed is not None
    assert claimed.id == older_run.id
    assert claimed.status == "pending"
    assert claimed.started_at is not None
    assert claimed.finished_at is None
    assert claimed.error_message is None
    db_session.refresh(newer_run)
    assert newer_run.status == "planned"



def test_transition_pipeline_run_advances_pending_run_to_terminal_state(db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(
            slug="orders-run-transition",
            name="Orders Run Transition",
            gold_sql="SELECT * FROM source WHERE gold = TRUE",
        ),
    )
    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="orders_run_transition_refresh",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )
    db_session.add(
        IngestionJob(
            dataset_id=dataset.id,
            source_type="object_uri",
            status=IngestionStatus.SUCCEEDED.value,
            silver_object_uri="s3://silver/orders-run-transition/job-1/part-00000.parquet",
        )
    )
    db_session.commit()

    run = PipelineService.create_pipeline_run(db_session, pipeline, CreatePipelineRunRequest())
    claimed = PipelineService.claim_next_pipeline_run(db_session, pipeline)
    assert claimed is not None
    assert claimed.id == run.id

    running = PipelineService.transition_pipeline_run(
        db_session,
        pipeline,
        claimed,
        UpdatePipelineRunStatusRequest(status="running"),
    )
    assert running.status == "running"
    assert running.started_at is not None
    assert running.finished_at is None
    assert running.error_message is None

    finished = PipelineService.transition_pipeline_run(
        db_session,
        pipeline,
        running,
        UpdatePipelineRunStatusRequest(status="succeeded"),
    )
    assert finished.status == "succeeded"
    assert finished.started_at is not None
    assert finished.finished_at is not None
    assert finished.error_message is None



def test_transition_pipeline_run_rejects_unclaimed_run(db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(
            slug="orders-run-invalid-transition",
            name="Orders Run Invalid Transition",
            gold_sql="SELECT * FROM source WHERE gold = TRUE",
        ),
    )
    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="orders_run_invalid_transition_refresh",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        ),
    )
    db_session.add(
        IngestionJob(
            dataset_id=dataset.id,
            source_type="object_uri",
            status=IngestionStatus.SUCCEEDED.value,
            silver_object_uri="s3://silver/orders-run-invalid-transition/job-1/part-00000.parquet",
        )
    )
    db_session.commit()

    run = PipelineService.create_pipeline_run(db_session, pipeline, CreatePipelineRunRequest())

    with pytest.raises(ValueError, match="cannot transition"):
        PipelineService.transition_pipeline_run(
            db_session,
            pipeline,
            run,
            UpdatePipelineRunStatusRequest(status="running"),
        )
