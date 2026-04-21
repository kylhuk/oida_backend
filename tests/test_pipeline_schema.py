from __future__ import annotations

from datetime import datetime, timezone

import pytest

from data_platform.schemas.pipeline import (
    CreatePipelineBackfillRunsPageRequest,
    CreatePipelineBackfillRunsRequest,
    CreatePipelineDefinitionRequest,
    CreatePipelineRunRequest,
    PipelineBackfillRunsPageResponse,
    PipelineRunDetailResponse,
    PipelineRunResponse,
    UpdatePipelineDefinitionRequest,
    UpdatePipelineRunStatusRequest,
)
from data_platform.utils.pipeline_definitions import (
    build_backfill_request_snapshot,
    build_backfill_run_ref,
    build_pipeline_execution_plan,
    build_pipeline_run_payload,
    build_pipeline_schema_compatibility_preview,
    build_pipeline_schema_compatibility_preview_unavailable_reason,
    build_pipeline_schema_context,
    build_pipeline_schema_snapshot,
    build_pipeline_source_candidate,
    extract_pipeline_run_snapshot,
    normalize_pipeline_definition,
    resolve_sql_pipeline_query,
)


def test_create_pipeline_definition_request_trims_name() -> None:
    payload = CreatePipelineDefinitionRequest(
        name="  Orders silver to gold  ",
        source_layer="silver",
        target_layer="gold",
        engine="sql",
    )

    assert payload.name == "Orders silver to gold"


def test_create_pipeline_definition_request_rejects_blank_name() -> None:
    with pytest.raises(ValueError, match="name cannot be empty"):
        CreatePipelineDefinitionRequest(
            name="   ",
            source_layer="silver",
            target_layer="gold",
            engine="sql",
        )


def test_update_pipeline_definition_request_trims_name() -> None:
    payload = UpdatePipelineDefinitionRequest(name="  Orders refresh  ")

    assert payload.name == "Orders refresh"


def test_update_pipeline_definition_request_rejects_blank_name() -> None:
    with pytest.raises(ValueError, match="name cannot be empty"):
        UpdatePipelineDefinitionRequest(name="   ")


def test_normalize_sql_pipeline_definition_defaults_to_dataset_transform() -> None:
    assert normalize_pipeline_definition("sql", "gold", {}) == {"mode": "dataset_transform"}


def test_normalize_sql_pipeline_definition_upgrades_legacy_sql_payload() -> None:
    assert normalize_pipeline_definition("sql", "gold", {"sql": " SELECT * FROM source; "}) == {
        "mode": "custom_sql",
        "sql": "SELECT * FROM source",
    }


def test_normalize_sql_pipeline_definition_rejects_mutating_sql() -> None:
    with pytest.raises(ValueError, match=r"definition_json\.sql"):
        normalize_pipeline_definition("sql", "gold", {"mode": "custom_sql", "sql": "DELETE FROM source"})


def test_normalize_sql_pipeline_definition_rejects_dataset_transform_to_raw() -> None:
    with pytest.raises(ValueError, match="raw layer"):
        normalize_pipeline_definition("sql", "raw", {})


def test_resolve_sql_pipeline_query_uses_dataset_gold_sql_for_dataset_transform_mode() -> None:
    resolved = resolve_sql_pipeline_query(
        target_layer="gold",
        definition_json={"mode": "dataset_transform"},
        dataset_silver_sql="SELECT * FROM source WHERE silver = TRUE",
        dataset_gold_sql="SELECT * FROM source WHERE gold = TRUE",
    )

    assert resolved == "SELECT * FROM source WHERE gold = TRUE"


def test_resolve_sql_pipeline_query_falls_back_to_passthrough() -> None:
    resolved = resolve_sql_pipeline_query(
        target_layer="silver",
        definition_json={},
        dataset_silver_sql=None,
        dataset_gold_sql=None,
    )

    assert resolved == "SELECT * FROM source"





def test_create_pipeline_backfill_runs_request_accepts_contract_compatibility_requirement() -> None:
    payload = CreatePipelineBackfillRunsRequest(
        source_finished_at_gte=datetime(2026, 4, 14, 17, 0, tzinfo=timezone.utc),
        require_contract_compatible_schema=True,
    )

    assert payload.require_contract_compatible_schema is True


def test_build_backfill_request_snapshot_round_trips_contract_compatibility_requirement() -> None:
    snapshot = build_backfill_request_snapshot(
        source_finished_at_gte=datetime(2026, 4, 14, 17, 0, tzinfo=timezone.utc),
        source_finished_at_lte=datetime(2026, 4, 14, 18, 0, tzinfo=timezone.utc),
        limit=25,
        offset=5,
        run_ref_prefix=" nightly " ,
        skip_existing_runs=True,
        require_contract_compatible_schema=True,
    )

    extracted = extract_pipeline_run_snapshot({"backfill_request": snapshot})["backfill_request"]

    assert extracted is not None
    assert extracted["require_contract_compatible_schema"] is True
    assert extracted["skip_existing_runs"] is True
    assert extracted["run_ref_prefix"] == "nightly"


def test_create_pipeline_backfill_runs_request_accepts_has_existing_run_filter() -> None:
    payload = CreatePipelineBackfillRunsRequest(
        source_finished_at_gte=datetime(2026, 4, 14, 17, 0, tzinfo=timezone.utc),
        has_existing_run=False,
    )

    assert payload.has_existing_run is False


def test_create_pipeline_backfill_runs_request_rejects_skip_existing_runs_with_has_existing_run_true() -> None:
    with pytest.raises(ValueError, match="skip_existing_runs cannot be combined with has_existing_run=true"):
        CreatePipelineBackfillRunsRequest(
            source_finished_at_gte=datetime(2026, 4, 14, 17, 0, tzinfo=timezone.utc),
            skip_existing_runs=True,
            has_existing_run=True,
        )


def test_create_pipeline_backfill_runs_page_request_rejects_skip_existing_runs_with_has_existing_run_true() -> None:
    with pytest.raises(ValueError, match="skip_existing_runs cannot be combined with has_existing_run=true"):
        CreatePipelineBackfillRunsPageRequest(
            source_finished_at_gte=datetime(2026, 4, 14, 17, 0, tzinfo=timezone.utc),
            skip_existing_runs=True,
            has_existing_run=True,
        )


def test_build_backfill_request_snapshot_round_trips_has_existing_run_filter() -> None:
    snapshot = build_backfill_request_snapshot(
        source_finished_at_gte=datetime(2026, 4, 14, 17, 0, tzinfo=timezone.utc),
        source_finished_at_lte=datetime(2026, 4, 14, 18, 0, tzinfo=timezone.utc),
        limit=25,
        offset=0,
        has_existing_run=False,
    )

    extracted = extract_pipeline_run_snapshot({"backfill_request": snapshot})["backfill_request"]

    assert extracted is not None
    assert extracted["has_existing_run"] is False

def test_build_pipeline_execution_plan_for_sql_pipeline_is_executable_when_source_artifact_exists() -> None:
    plan = build_pipeline_execution_plan(
        pipeline_id="pipe-1",
        dataset_id="dataset-1",
        source_layer="silver",
        target_layer="gold",
        engine="sql",
        definition_json={},
        dataset_silver_sql="SELECT * FROM source WHERE silver = TRUE",
        dataset_gold_sql="SELECT * FROM source WHERE gold = TRUE",
        source_ingestion_job_id="job-1",
        source_object_uri="s3://silver/orders/job-1/part-00000.parquet",
    )

    assert plan["executable"] is True
    assert plan["definition_json"] == {"mode": "dataset_transform"}
    assert plan["resolved_query"] == "SELECT * FROM source WHERE gold = TRUE"
    assert plan["issues"] == []



def test_build_pipeline_execution_plan_allows_selected_source_without_status() -> None:
    plan = build_pipeline_execution_plan(
        pipeline_id="pipe-1",
        dataset_id="dataset-1",
        source_layer="silver",
        target_layer="gold",
        engine="sql",
        definition_json={"mode": "custom_sql", "sql": "SELECT * FROM source"},
        dataset_silver_sql=None,
        dataset_gold_sql=None,
        source_selection="explicit",
        requested_source_ingestion_job_id="job-8",
        source_ingestion_job_id="job-8",
        source_job_status=None,
        source_object_uri="s3://silver/orders/job-8/part-00000.parquet",
    )

    assert plan["executable"] is True
    assert plan["issues"] == []


def test_build_pipeline_execution_plan_supports_explicit_source_selection() -> None:
    plan = build_pipeline_execution_plan(
        pipeline_id="pipe-1",
        dataset_id="dataset-1",
        source_layer="silver",
        target_layer="gold",
        engine="sql",
        definition_json={"mode": "custom_sql", "sql": "SELECT * FROM source"},
        dataset_silver_sql=None,
        dataset_gold_sql=None,
        source_selection="explicit",
        requested_source_ingestion_job_id="job-7",
        source_ingestion_job_id="job-7",
        source_job_status="succeeded",
        source_object_uri="s3://silver/orders/job-7/part-00000.parquet",
    )

    assert plan["source_selection"] == "explicit"
    assert plan["requested_source_ingestion_job_id"] == "job-7"
    assert plan["source_job_status"] == "succeeded"
    assert plan["executable"] is True
    assert plan["issues"] == []



def test_build_pipeline_execution_plan_reports_explicit_source_without_required_artifact() -> None:
    plan = build_pipeline_execution_plan(
        pipeline_id="pipe-1",
        dataset_id="dataset-1",
        source_layer="silver",
        target_layer="gold",
        engine="sql",
        definition_json={"mode": "custom_sql", "sql": "SELECT * FROM source"},
        dataset_silver_sql=None,
        dataset_gold_sql=None,
        source_selection="explicit",
        requested_source_ingestion_job_id="job-9",
        source_ingestion_job_id="job-9",
        source_job_status="failed",
        source_object_uri=None,
    )

    assert plan["executable"] is False
    assert plan["issues"] == [
        "Source ingestion job 'job-9' is in status 'failed', expected 'succeeded'.",
        "Source ingestion job 'job-9' does not expose a 'silver' object URI.",
    ]


def test_build_pipeline_execution_plan_reports_missing_source_artifact() -> None:
    plan = build_pipeline_execution_plan(
        pipeline_id="pipe-1",
        dataset_id="dataset-1",
        source_layer="silver",
        target_layer="gold",
        engine="sql",
        definition_json={"mode": "custom_sql", "sql": "SELECT * FROM source"},
        dataset_silver_sql=None,
        dataset_gold_sql=None,
    )

    assert plan["executable"] is False
    assert plan["resolved_query"] == "SELECT * FROM source"
    assert plan["issues"] == [
        "No successful ingestion is available for source layer 'silver'.",
        "No source object URI is available for source layer 'silver'.",
    ]


def test_build_pipeline_execution_plan_reports_unsupported_engine() -> None:
    plan = build_pipeline_execution_plan(
        pipeline_id="pipe-1",
        dataset_id="dataset-1",
        source_layer="silver",
        target_layer="gold",
        engine="python",
        definition_json={"entrypoint": "jobs.orders:main"},
        dataset_silver_sql=None,
        dataset_gold_sql=None,
        source_ingestion_job_id="job-1",
        source_object_uri="s3://silver/orders/job-1/part-00000.parquet",
    )

    assert plan["executable"] is False
    assert plan["resolved_query"] is None
    assert plan["issues"] == ["Pipeline engine 'python' is not yet executable."]


def test_build_pipeline_execution_plan_supports_time_bounded_source_selection() -> None:
    cutoff = datetime(2026, 4, 14, 18, 30, tzinfo=timezone.utc)
    selected_finished_at = datetime(2026, 4, 14, 18, 0, tzinfo=timezone.utc)
    plan = build_pipeline_execution_plan(
        pipeline_id="pipe-1",
        dataset_id="dataset-1",
        source_layer="silver",
        target_layer="gold",
        engine="sql",
        definition_json={"mode": "custom_sql", "sql": "SELECT * FROM source"},
        dataset_silver_sql=None,
        dataset_gold_sql=None,
        source_selection="latest_successful_at_or_before",
        requested_source_finished_at_lte=cutoff,
        source_ingestion_job_id="job-5",
        source_job_status="succeeded",
        source_finished_at=selected_finished_at,
        source_object_uri="s3://silver/orders/job-5/part-00000.parquet",
    )

    assert plan["source_selection"] == "latest_successful_at_or_before"
    assert plan["requested_source_finished_at_lte"] == cutoff
    assert plan["source_finished_at"] == selected_finished_at
    assert plan["executable"] is True
    assert plan["issues"] == []



def test_build_pipeline_execution_plan_supports_time_window_source_selection() -> None:
    started = datetime(2026, 4, 14, 17, 0, tzinfo=timezone.utc)
    cutoff = datetime(2026, 4, 14, 18, 30, tzinfo=timezone.utc)
    selected_finished_at = datetime(2026, 4, 14, 18, 0, tzinfo=timezone.utc)
    plan = build_pipeline_execution_plan(
        pipeline_id="pipe-1",
        dataset_id="dataset-1",
        source_layer="silver",
        target_layer="gold",
        engine="sql",
        definition_json={"mode": "custom_sql", "sql": "SELECT * FROM source"},
        dataset_silver_sql=None,
        dataset_gold_sql=None,
        source_selection="latest_successful_between",
        requested_source_finished_at_gte=started,
        requested_source_finished_at_lte=cutoff,
        source_ingestion_job_id="job-6",
        source_job_status="succeeded",
        source_finished_at=selected_finished_at,
        source_object_uri="s3://silver/orders/job-6/part-00000.parquet",
    )

    assert plan["source_selection"] == "latest_successful_between"
    assert plan["requested_source_finished_at_gte"] == started
    assert plan["requested_source_finished_at_lte"] == cutoff
    assert plan["source_finished_at"] == selected_finished_at
    assert plan["executable"] is True
    assert plan["issues"] == []



def test_build_pipeline_execution_plan_reports_missing_time_window_source_artifact() -> None:
    started = datetime(2026, 4, 14, 17, 0, tzinfo=timezone.utc)
    cutoff = datetime(2026, 4, 14, 18, 30, tzinfo=timezone.utc)
    plan = build_pipeline_execution_plan(
        pipeline_id="pipe-1",
        dataset_id="dataset-1",
        source_layer="silver",
        target_layer="gold",
        engine="sql",
        definition_json={"mode": "custom_sql", "sql": "SELECT * FROM source"},
        dataset_silver_sql=None,
        dataset_gold_sql=None,
        source_selection="latest_successful_between",
        requested_source_finished_at_gte=started,
        requested_source_finished_at_lte=cutoff,
    )

    assert plan["executable"] is False
    assert plan["issues"] == [
        "No successful ingestion between '2026-04-14T17:00:00+00:00' and '2026-04-14T18:30:00+00:00' is available for source layer 'silver'.",
        "No source object URI is available for source layer 'silver' between '2026-04-14T17:00:00+00:00' and '2026-04-14T18:30:00+00:00'.",
    ]



def test_build_pipeline_execution_plan_reports_missing_time_bounded_source_artifact() -> None:
    cutoff = datetime(2026, 4, 14, 18, 30, tzinfo=timezone.utc)
    plan = build_pipeline_execution_plan(
        pipeline_id="pipe-1",
        dataset_id="dataset-1",
        source_layer="silver",
        target_layer="gold",
        engine="sql",
        definition_json={"mode": "custom_sql", "sql": "SELECT * FROM source"},
        dataset_silver_sql=None,
        dataset_gold_sql=None,
        source_selection="latest_successful_at_or_before",
        requested_source_finished_at_lte=cutoff,
    )

    assert plan["executable"] is False
    assert plan["issues"] == [
        "No successful ingestion at or before '2026-04-14T18:30:00+00:00' is available for source layer 'silver'.",
        "No source object URI is available for source layer 'silver' at or before '2026-04-14T18:30:00+00:00'.",
    ]


def test_create_pipeline_run_request_trims_run_ref() -> None:
    payload = CreatePipelineRunRequest(run_ref="  nightly backfill  ")

    assert payload.run_ref == "nightly backfill"


def test_normalize_optional_run_ref_rejects_blank_values() -> None:
    from data_platform.utils.pipeline_definitions import normalize_optional_run_ref

    with pytest.raises(ValueError, match="run_ref cannot be empty"):
        normalize_optional_run_ref("   ")


def test_create_pipeline_run_request_rejects_combined_source_selection() -> None:
    with pytest.raises(ValueError, match="cannot be combined"):
        CreatePipelineRunRequest(
            source_ingestion_job_id="job-1",
            source_finished_at_lte=datetime(2026, 4, 14, 18, 30, tzinfo=timezone.utc),
        )


def test_create_pipeline_run_request_rejects_invalid_time_window() -> None:
    with pytest.raises(ValueError, match="cannot be after"):
        CreatePipelineRunRequest(
            source_finished_at_gte=datetime(2026, 4, 14, 19, 0, tzinfo=timezone.utc),
            source_finished_at_lte=datetime(2026, 4, 14, 18, 30, tzinfo=timezone.utc),
        )



def test_create_pipeline_run_request_accepts_contract_compatibility_guard() -> None:
    payload = CreatePipelineRunRequest(require_contract_compatible_schema=True)

    assert payload.require_contract_compatible_schema is True


def test_update_pipeline_run_status_request_requires_error_message_for_failed_status() -> None:
    with pytest.raises(ValueError, match="error_message is required"):
        UpdatePipelineRunStatusRequest(status="failed")


def test_update_pipeline_run_status_request_rejects_error_message_for_non_failed_status() -> None:
    with pytest.raises(ValueError, match="only allowed"):
        UpdatePipelineRunStatusRequest(status="running", error_message="  timeout  ")


def test_update_pipeline_run_status_request_trims_failed_error_message() -> None:
    payload = UpdatePipelineRunStatusRequest(status="failed", error_message="  timeout  ")

    assert payload.error_message == "timeout"


def test_build_pipeline_run_payload_serializes_plan_and_marks_planned_status() -> None:
    selected_finished_at = datetime(2026, 4, 14, 18, 0, tzinfo=timezone.utc)
    preflighted_at = datetime(2026, 4, 14, 18, 30, tzinfo=timezone.utc)
    execution_plan = build_pipeline_execution_plan(
        pipeline_id="pipe-1",
        dataset_id="dataset-1",
        source_layer="silver",
        target_layer="gold",
        engine="sql",
        definition_json={},
        dataset_silver_sql=None,
        dataset_gold_sql="SELECT * FROM source WHERE gold = TRUE",
        source_selection="latest_successful_at_or_before",
        requested_source_finished_at_lte=preflighted_at,
        source_ingestion_job_id="job-5",
        source_job_status="succeeded",
        source_finished_at=selected_finished_at,
        source_object_uri="s3://silver/orders/job-5/part-00000.parquet",
    )

    payload = build_pipeline_run_payload(
        pipeline_id="pipe-1",
        dataset_id="dataset-1",
        execution_plan=execution_plan,
        run_ref=" nightly ",
        preflighted_at=preflighted_at,
    )

    assert payload["status"] == "planned"
    assert payload["run_ref"] == "nightly"
    assert payload["finished_at"] is None
    assert payload["metrics_json"]["preflighted_at"] == "2026-04-14T18:30:00+00:00"
    assert payload["metrics_json"]["execution_plan"]["requested_source_finished_at_lte"] == "2026-04-14T18:30:00+00:00"
    assert payload["metrics_json"]["execution_plan"]["source_finished_at"] == "2026-04-14T18:00:00+00:00"


def test_extract_pipeline_run_snapshot_returns_first_class_preflight_fields() -> None:
    preflighted_at = datetime(2026, 4, 14, 18, 30, tzinfo=timezone.utc)
    execution_plan = build_pipeline_execution_plan(
        pipeline_id="pipe-1",
        dataset_id="dataset-1",
        source_layer="silver",
        target_layer="gold",
        engine="sql",
        definition_json={},
        dataset_silver_sql=None,
        dataset_gold_sql="SELECT * FROM source WHERE gold = TRUE",
        source_ingestion_job_id="job-5",
        source_job_status="succeeded",
        source_object_uri="s3://silver/orders/job-5/part-00000.parquet",
    )
    payload = build_pipeline_run_payload(
        pipeline_id="pipe-1",
        dataset_id="dataset-1",
        execution_plan=execution_plan,
        preflighted_at=preflighted_at,
    )

    snapshot = extract_pipeline_run_snapshot(payload["metrics_json"])

    assert snapshot["preflighted_at"] == preflighted_at
    assert snapshot["execution_plan"] == payload["metrics_json"]["execution_plan"]
    assert snapshot["backfill_request"] is None



def test_extract_pipeline_run_snapshot_returns_execution_details_when_present() -> None:
    snapshot = extract_pipeline_run_snapshot(
        {
            "execution": {
                "executor": " sql_builtin ",
                "status": "succeeded",
                "started_at": "2026-04-14T18:30:00+00:00",
                "finished_at": "2026-04-14T18:31:00+00:00",
                "task_id": " task-42 ",
                "source_object_uri": " s3://silver/orders/job-5/part-00000.parquet ",
                "target_object_uri": " s3://gold/orders/run-1/part-00000.parquet ",
                "output_row_count": 25,
                "output_schema": [{"name": " id ", "type": " BIGINT "}],
                "target_schema_version": 3,
                "target_schema_fingerprint": " gold-fp ",
            }
        }
    )

    assert snapshot["execution_details"] == {
        "executor": "sql_builtin",
        "status": "succeeded",
        "started_at": datetime(2026, 4, 14, 18, 30, tzinfo=timezone.utc),
        "finished_at": datetime(2026, 4, 14, 18, 31, tzinfo=timezone.utc),
        "task_id": "task-42",
        "source_object_uri": "s3://silver/orders/job-5/part-00000.parquet",
        "target_object_uri": "s3://gold/orders/run-1/part-00000.parquet",
        "output_row_count": 25,
        "output_schema": [{"name": "id", "type": "BIGINT"}],
        "target_schema_version": 3,
        "target_schema_fingerprint": "gold-fp",
    }



def test_pipeline_run_response_model_exposes_execution_details() -> None:
    executed_at = datetime(2026, 4, 14, 18, 30, tzinfo=timezone.utc)
    response = PipelineRunResponse.model_validate(
        {
            "id": "run-1",
            "pipeline_id": "pipe-1",
            "dataset_id": "dataset-1",
            "ingestion_job_id": "job-5",
            "status": "succeeded",
            "run_ref": "executor-run",
            "metrics_json": {
                "execution": {
                    "executor": "sql_builtin",
                    "status": "succeeded",
                    "started_at": executed_at.isoformat(),
                    "finished_at": executed_at.isoformat(),
                    "task_id": "task-42",
                    "output_row_count": 1,
                    "output_schema": [{"name": "id", "type": "BIGINT"}],
                }
            },
            "error_message": None,
            "started_at": executed_at,
            "finished_at": executed_at,
            "created_at": executed_at,
            "updated_at": executed_at,
            **extract_pipeline_run_snapshot(
                {
                    "execution": {
                        "executor": "sql_builtin",
                        "status": "succeeded",
                        "started_at": executed_at.isoformat(),
                        "finished_at": executed_at.isoformat(),
                        "task_id": "task-42",
                        "output_row_count": 1,
                        "output_schema": [{"name": "id", "type": "BIGINT"}],
                    }
                }
            ),
        }
    )

    assert response.execution_details is not None
    assert response.execution_details.executor == "sql_builtin"
    assert response.execution_details.task_id == "task-42"
    assert response.execution_details.output_row_count == 1
    assert response.execution_details.output_schema == [{"name": "id", "type": "BIGINT"}]



def test_extract_pipeline_run_snapshot_returns_backfill_request_when_present() -> None:
    preflighted_at = datetime(2026, 4, 14, 18, 30, tzinfo=timezone.utc)
    execution_plan = build_pipeline_execution_plan(
        pipeline_id="pipe-1",
        dataset_id="dataset-1",
        source_layer="silver",
        target_layer="gold",
        engine="sql",
        definition_json={},
        dataset_silver_sql=None,
        dataset_gold_sql="SELECT * FROM source WHERE gold = TRUE",
        source_ingestion_job_id="job-5",
        source_job_status="succeeded",
        source_object_uri="s3://silver/orders/job-5/part-00000.parquet",
    )
    backfill_request = build_backfill_request_snapshot(
        source_finished_at_gte=datetime(2026, 4, 14, 9, 30, tzinfo=timezone.utc),
        source_finished_at_lte=datetime(2026, 4, 14, 11, 30, tzinfo=timezone.utc),
        limit=25,
        offset=5,
        run_ref_prefix=" nightly backfill ",
    )
    payload = build_pipeline_run_payload(
        pipeline_id="pipe-1",
        dataset_id="dataset-1",
        execution_plan=execution_plan,
        preflighted_at=preflighted_at,
        backfill_request=backfill_request,
    )

    snapshot = extract_pipeline_run_snapshot(payload["metrics_json"])

    assert snapshot["backfill_request"] == {
        "run_ref_prefix": "nightly backfill",
        "source_finished_at_gte": datetime(2026, 4, 14, 9, 30, tzinfo=timezone.utc),
        "source_finished_at_lte": datetime(2026, 4, 14, 11, 30, tzinfo=timezone.utc),
        "limit": 25,
        "offset": 5,
    }
    detail = PipelineRunDetailResponse.model_validate(
        {
            "id": "run-1",
            "pipeline_id": "pipe-1",
            "dataset_id": "dataset-1",
            "ingestion_job_id": "job-5",
            "status": "planned",
            "run_ref": None,
            "metrics_json": payload["metrics_json"],
            "error_message": None,
            "started_at": None,
            "finished_at": None,
            "created_at": preflighted_at,
            "updated_at": preflighted_at,
            **snapshot,
        }
    )
    assert detail.backfill_request is not None
    assert detail.backfill_request.run_ref_prefix == "nightly backfill"
    assert detail.contract_compatibility_required is False


def test_extract_pipeline_run_snapshot_returns_backfill_request_cursor() -> None:
    preflighted_at = datetime(2026, 4, 14, 18, 30, tzinfo=timezone.utc)
    execution_plan = build_pipeline_execution_plan(
        pipeline_id="pipe-1",
        dataset_id="dataset-1",
        source_layer="silver",
        target_layer="gold",
        engine="sql",
        definition_json={},
        dataset_silver_sql=None,
        dataset_gold_sql="SELECT * FROM source WHERE gold = TRUE",
        source_ingestion_job_id="job-6",
        source_job_status="succeeded",
        source_object_uri="s3://silver/orders/job-6/part-00000.parquet",
    )
    backfill_request = build_backfill_request_snapshot(
        source_finished_at_gte=datetime(2026, 4, 14, 9, 30, tzinfo=timezone.utc),
        source_finished_at_lte=datetime(2026, 4, 14, 11, 30, tzinfo=timezone.utc),
        limit=25,
        offset=0,
        cursor=" cursor-token ",
        run_ref_prefix=" nightly backfill ",
    )
    payload = build_pipeline_run_payload(
        pipeline_id="pipe-1",
        dataset_id="dataset-1",
        execution_plan=execution_plan,
        preflighted_at=preflighted_at,
        backfill_request=backfill_request,
    )

    snapshot = extract_pipeline_run_snapshot(payload["metrics_json"])

    assert payload["metrics_json"]["backfill_request"]["cursor"] == "cursor-token"
    assert snapshot["backfill_request"] == {
        "run_ref_prefix": "nightly backfill",
        "source_finished_at_gte": datetime(2026, 4, 14, 9, 30, tzinfo=timezone.utc),
        "source_finished_at_lte": datetime(2026, 4, 14, 11, 30, tzinfo=timezone.utc),
        "limit": 25,
        "offset": 0,
        "cursor": "cursor-token",
    }


def test_extract_pipeline_run_snapshot_returns_contract_compatibility_requirement() -> None:
    preflighted_at = datetime(2026, 4, 14, 18, 30, tzinfo=timezone.utc)
    execution_plan = build_pipeline_execution_plan(
        pipeline_id="pipe-1",
        dataset_id="dataset-1",
        source_layer="silver",
        target_layer="gold",
        engine="sql",
        definition_json={},
        dataset_silver_sql=None,
        dataset_gold_sql="SELECT * FROM source WHERE gold = TRUE",
        source_ingestion_job_id="job-5",
        source_job_status="succeeded",
        source_object_uri="s3://silver/orders/job-5/part-00000.parquet",
    )
    schema_context = build_pipeline_schema_context(contract_compatibility_required=True)
    payload = build_pipeline_run_payload(
        pipeline_id="pipe-1",
        dataset_id="dataset-1",
        execution_plan=execution_plan,
        preflighted_at=preflighted_at,
        schema_context=schema_context,
    )

    snapshot = extract_pipeline_run_snapshot(payload["metrics_json"])

    assert snapshot["contract_compatibility_required"] is True


def test_pipeline_run_response_model_exposes_first_class_preflight_fields() -> None:
    preflighted_at = datetime(2026, 4, 14, 18, 30, tzinfo=timezone.utc)
    execution_plan = build_pipeline_execution_plan(
        pipeline_id="pipe-1",
        dataset_id="dataset-1",
        source_layer="silver",
        target_layer="gold",
        engine="sql",
        definition_json={"mode": "custom_sql", "sql": "SELECT * FROM source"},
        dataset_silver_sql=None,
        dataset_gold_sql=None,
        source_ingestion_job_id="job-5",
        source_job_status="succeeded",
        source_object_uri="s3://silver/orders/job-5/part-00000.parquet",
    )
    backfill_request = build_backfill_request_snapshot(
        source_finished_at_gte=datetime(2026, 4, 14, 9, 30, tzinfo=timezone.utc),
        source_finished_at_lte=datetime(2026, 4, 14, 11, 30, tzinfo=timezone.utc),
        limit=25,
        offset=5,
        run_ref_prefix=" nightly backfill ",
    )
    payload = build_pipeline_run_payload(
        pipeline_id="pipe-1",
        dataset_id="dataset-1",
        execution_plan=execution_plan,
        preflighted_at=preflighted_at,
        backfill_request=backfill_request,
    )
    snapshot = extract_pipeline_run_snapshot(payload["metrics_json"])

    response = PipelineRunResponse.model_validate(
        {
            "id": "run-1",
            "pipeline_id": "pipe-1",
            "dataset_id": "dataset-1",
            "ingestion_job_id": "job-5",
            "status": "planned",
            "run_ref": None,
            "metrics_json": payload["metrics_json"],
            "error_message": None,
            "started_at": None,
            "finished_at": None,
            "created_at": preflighted_at,
            "updated_at": preflighted_at,
            **snapshot,
        }
    )

    assert response.preflighted_at == preflighted_at
    assert response.execution_plan is not None
    assert response.execution_plan["source_ingestion_job_id"] == "job-5"
    assert response.backfill_request is not None
    assert response.backfill_request.limit == 25



def test_build_pipeline_schema_snapshot_normalizes_schema_items() -> None:
    snapshot = build_pipeline_schema_snapshot(
        layer="silver",
        version=3,
        fingerprint="  abc123  ",
        schema_json=[{"name": " id ", "type": " BIGINT "}],
    )

    assert snapshot == {
        "layer": "silver",
        "version": 3,
        "fingerprint": "abc123",
        "schema_json": [{"name": "id", "type": "BIGINT"}],
    }



def test_extract_pipeline_run_snapshot_returns_schema_context_when_present() -> None:
    preflighted_at = datetime(2026, 4, 14, 18, 30, tzinfo=timezone.utc)
    execution_plan = build_pipeline_execution_plan(
        pipeline_id="pipe-1",
        dataset_id="dataset-1",
        source_layer="silver",
        target_layer="gold",
        engine="sql",
        definition_json={},
        dataset_silver_sql=None,
        dataset_gold_sql="SELECT * FROM source WHERE gold = TRUE",
        source_ingestion_job_id="job-5",
        source_job_status="succeeded",
        source_object_uri="s3://silver/orders/job-5/part-00000.parquet",
    )
    schema_context = build_pipeline_schema_context(
        source_schema_snapshot=build_pipeline_schema_snapshot(
            layer="silver",
            version=2,
            fingerprint="silver-fp",
            schema_json=[{"name": "id", "type": "BIGINT"}],
        ),
        target_schema_snapshot=build_pipeline_schema_snapshot(
            layer="gold",
            version=4,
            fingerprint="gold-fp",
            schema_json=[{"name": "id", "type": "BIGINT"}, {"name": "amount", "type": "DOUBLE"}],
        ),
    )
    payload = build_pipeline_run_payload(
        pipeline_id="pipe-1",
        dataset_id="dataset-1",
        execution_plan=execution_plan,
        preflighted_at=preflighted_at,
        schema_context=schema_context,
    )

    snapshot = extract_pipeline_run_snapshot(payload["metrics_json"])

    assert snapshot["source_schema_snapshot"] == {
        "layer": "silver",
        "version": 2,
        "fingerprint": "silver-fp",
        "schema_json": [{"name": "id", "type": "BIGINT"}],
    }
    assert snapshot["target_schema_snapshot"] == {
        "layer": "gold",
        "version": 4,
        "fingerprint": "gold-fp",
        "schema_json": [
            {"name": "id", "type": "BIGINT"},
            {"name": "amount", "type": "DOUBLE"},
        ],
    }



def test_pipeline_run_response_model_exposes_schema_context_fields() -> None:
    preflighted_at = datetime(2026, 4, 14, 18, 30, tzinfo=timezone.utc)
    execution_plan = build_pipeline_execution_plan(
        pipeline_id="pipe-1",
        dataset_id="dataset-1",
        source_layer="silver",
        target_layer="gold",
        engine="sql",
        definition_json={"mode": "custom_sql", "sql": "SELECT * FROM source"},
        dataset_silver_sql=None,
        dataset_gold_sql=None,
        source_ingestion_job_id="job-5",
        source_job_status="succeeded",
        source_object_uri="s3://silver/orders/job-5/part-00000.parquet",
    )
    payload = build_pipeline_run_payload(
        pipeline_id="pipe-1",
        dataset_id="dataset-1",
        execution_plan=execution_plan,
        preflighted_at=preflighted_at,
        schema_context=build_pipeline_schema_context(
            source_schema_snapshot=build_pipeline_schema_snapshot(
                layer="silver",
                version=2,
                fingerprint="silver-fp",
                schema_json=[{"name": "id", "type": "BIGINT"}],
            ),
            target_schema_snapshot=build_pipeline_schema_snapshot(
                layer="gold",
                version=4,
                fingerprint="gold-fp",
                schema_json=[{"name": "id", "type": "BIGINT"}],
            ),
        ),
    )
    snapshot = extract_pipeline_run_snapshot(payload["metrics_json"])

    response = PipelineRunResponse.model_validate(
        {
            "id": "run-1",
            "pipeline_id": "pipe-1",
            "dataset_id": "dataset-1",
            "ingestion_job_id": "job-5",
            "status": "planned",
            "run_ref": None,
            "metrics_json": payload["metrics_json"],
            "error_message": None,
            "started_at": None,
            "finished_at": None,
            "created_at": preflighted_at,
            "updated_at": preflighted_at,
            **snapshot,
        }
    )

    assert response.source_schema_snapshot is not None
    assert response.source_schema_snapshot.version == 2
    assert response.target_schema_snapshot is not None
    assert response.target_schema_snapshot.fingerprint == "gold-fp"



def test_extract_pipeline_run_snapshot_ignores_invalid_or_missing_fields() -> None:
    snapshot = extract_pipeline_run_snapshot(
        {
            "preflighted_at": "not-a-datetime",
            "execution_plan": "not-an-object",
            "schema_context": {"source_schema_snapshot": {"layer": "silver", "version": 0}},
        }
    )

    assert snapshot == {
        "preflighted_at": None,
        "execution_plan": None,
        "backfill_request": None,
        "source_schema_snapshot": None,
        "target_schema_snapshot": None,
    }


def test_build_pipeline_run_payload_marks_blocked_runs_terminal() -> None:
    preflighted_at = datetime(2026, 4, 14, 18, 30, tzinfo=timezone.utc)
    execution_plan = build_pipeline_execution_plan(
        pipeline_id="pipe-1",
        dataset_id="dataset-1",
        source_layer="silver",
        target_layer="gold",
        engine="sql",
        definition_json={"mode": "custom_sql", "sql": "SELECT * FROM source"},
        dataset_silver_sql=None,
        dataset_gold_sql=None,
    )

    payload = build_pipeline_run_payload(
        pipeline_id="pipe-1",
        dataset_id="dataset-1",
        execution_plan=execution_plan,
        preflighted_at=preflighted_at,
    )

    assert payload["status"] == "blocked"
    assert payload["finished_at"] == preflighted_at
    assert payload["error_message"] == (
        "No successful ingestion is available for source layer 'silver'.; "
        "No source object URI is available for source layer 'silver'."
    )


def test_pipeline_status_enum_includes_preflight_states() -> None:
    from data_platform.enums import PipelineStatus

    assert PipelineStatus.PLANNED.value == "planned"
    assert PipelineStatus.BLOCKED.value == "blocked"


def test_create_pipeline_run_request_trims_source_ingestion_job_id() -> None:
    payload = CreatePipelineRunRequest(source_ingestion_job_id="  job-123  ")

    assert payload.source_ingestion_job_id == "job-123"


def test_create_pipeline_run_request_rejects_blank_source_ingestion_job_id() -> None:
    with pytest.raises(ValueError, match="source_ingestion_job_id cannot be empty"):
        CreatePipelineRunRequest(source_ingestion_job_id="   ")



def test_build_pipeline_source_candidate_prefers_finished_at_over_created_at() -> None:
    created_at = datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc)
    finished_at = datetime(2026, 4, 14, 13, 0, tzinfo=timezone.utc)

    latest_run_created_at = datetime(2026, 4, 14, 13, 30, tzinfo=timezone.utc)
    candidate = build_pipeline_source_candidate(
        ingestion_job_id="job-1",
        dataset_id="dataset-1",
        source_layer="silver",
        status="succeeded",
        created_at=created_at,
        finished_at=finished_at,
        object_uri="s3://silver/dataset-1/job-1/part-00000.parquet",
        existing_run_count=2,
        latest_run_id="run-2",
        latest_run_status="planned",
        latest_run_ref="nightly refresh",
        latest_run_created_at=latest_run_created_at,
    )

    assert candidate["effective_finished_at"] == finished_at
    assert candidate["object_uri"] == "s3://silver/dataset-1/job-1/part-00000.parquet"
    assert candidate["existing_run_count"] == 2
    assert candidate["has_existing_run"] is True
    assert candidate["latest_run_id"] == "run-2"
    assert candidate["latest_run_status"] == "planned"
    assert candidate["latest_run_ref"] == "nightly refresh"
    assert candidate["latest_run_created_at"] == latest_run_created_at


def test_build_pipeline_source_candidate_falls_back_to_created_at_when_finished_at_missing() -> None:
    created_at = datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc)

    candidate = build_pipeline_source_candidate(
        ingestion_job_id="job-2",
        dataset_id="dataset-1",
        source_layer="gold",
        status="succeeded",
        created_at=created_at,
        finished_at=None,
        object_uri="s3://gold/dataset-1/job-2/part-00000.parquet",
    )

    assert candidate["effective_finished_at"] == created_at
    assert candidate["source_layer"] == "gold"
    assert candidate["existing_run_count"] == 0
    assert candidate["has_existing_run"] is False
    assert candidate["latest_run_id"] is None
    assert candidate["latest_run_status"] is None
    assert candidate["latest_run_ref"] is None
    assert candidate["latest_run_created_at"] is None




def test_build_pipeline_source_candidate_rejects_negative_existing_run_count() -> None:
    with pytest.raises(ValueError, match="existing_run_count cannot be negative"):
        build_pipeline_source_candidate(
            ingestion_job_id="job-3",
            dataset_id="dataset-1",
            source_layer="silver",
            status="succeeded",
            created_at=datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc),
            finished_at=None,
            object_uri="s3://silver/dataset-1/job-3/part-00000.parquet",
            existing_run_count=-1,
        )

def test_create_pipeline_backfill_runs_request_requires_time_bound() -> None:
    with pytest.raises(ValueError, match="At least one of source_finished_at_gte or source_finished_at_lte is required"):
        CreatePipelineBackfillRunsRequest()


def test_create_pipeline_backfill_runs_request_trims_run_ref_prefix() -> None:
    payload = CreatePipelineBackfillRunsRequest(
        run_ref_prefix="  nightly backfill  ",
        source_finished_at_lte=datetime(2026, 4, 14, 18, 30, tzinfo=timezone.utc),
    )

    assert payload.run_ref_prefix == "nightly backfill"


def test_create_pipeline_backfill_runs_request_defaults_skip_existing_runs_to_false() -> None:
    payload = CreatePipelineBackfillRunsRequest(
        source_finished_at_lte=datetime(2026, 4, 14, 18, 30, tzinfo=timezone.utc),
    )

    assert payload.skip_existing_runs is False


def test_create_pipeline_backfill_runs_page_request_normalizes_cursor_and_run_ref_prefix() -> None:
    payload = CreatePipelineBackfillRunsPageRequest(
        run_ref_prefix="  nightly backfill  ",
        cursor="  cursor-token  ",
        source_finished_at_lte=datetime(2026, 4, 14, 18, 30, tzinfo=timezone.utc),
    )

    assert payload.run_ref_prefix == "nightly backfill"
    assert payload.cursor == "cursor-token"


def test_build_backfill_run_ref_appends_source_ingestion_job_id() -> None:
    assert build_backfill_run_ref(" nightly ", " job-123 ") == "nightly:job-123"


def test_build_backfill_request_snapshot_serializes_selection_context() -> None:
    snapshot = build_backfill_request_snapshot(
        source_finished_at_gte=datetime(2026, 4, 14, 17, 0, tzinfo=timezone.utc),
        source_finished_at_lte=datetime(2026, 4, 14, 18, 30, tzinfo=timezone.utc),
        limit=25,
        offset=5,
        run_ref_prefix=" nightly ",
    )

    assert snapshot == {
        "source_finished_at_gte": datetime(2026, 4, 14, 17, 0, tzinfo=timezone.utc),
        "source_finished_at_lte": datetime(2026, 4, 14, 18, 30, tzinfo=timezone.utc),
        "limit": 25,
        "offset": 5,
        "run_ref_prefix": "nightly",
        "skip_existing_runs": False,
    }


def test_build_backfill_request_snapshot_serializes_cursor_selection_context() -> None:
    snapshot = build_backfill_request_snapshot(
        source_finished_at_gte=datetime(2026, 4, 14, 17, 0, tzinfo=timezone.utc),
        source_finished_at_lte=datetime(2026, 4, 14, 18, 30, tzinfo=timezone.utc),
        limit=25,
        offset=0,
        cursor=" cursor-token ",
        run_ref_prefix=" nightly ",
    )

    assert snapshot == {
        "source_finished_at_gte": datetime(2026, 4, 14, 17, 0, tzinfo=timezone.utc),
        "source_finished_at_lte": datetime(2026, 4, 14, 18, 30, tzinfo=timezone.utc),
        "limit": 25,
        "offset": 0,
        "cursor": "cursor-token",
        "run_ref_prefix": "nightly",
        "skip_existing_runs": False,
    }


def test_pipeline_backfill_runs_page_response_model_exposes_next_cursor() -> None:
    response = PipelineBackfillRunsPageResponse.model_validate(
        {
            "items": [],
            "next_cursor": "cursor-token",
        }
    )

    assert response.next_cursor == "cursor-token"
    assert response.items == []


def test_pipeline_source_candidate_response_accepts_latest_run_terminal_context():
    from datetime import datetime, timezone

    from data_platform.schemas.pipeline import PipelineSourceCandidateResponse
    from data_platform.utils.pipeline_definitions import build_pipeline_source_candidate

    candidate = build_pipeline_source_candidate(
        ingestion_job_id="ing-1",
        dataset_id="ds-1",
        source_layer="raw",
        status="succeeded",
        created_at=datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc),
        finished_at=datetime(2026, 1, 2, 3, 5, 0, tzinfo=timezone.utc),
        object_uri="s3://bucket/raw/file.parquet",
        existing_run_count=2,
        latest_run_id="run-2",
        latest_run_status="blocked",
        latest_run_ref="backfill:ing-1",
        latest_run_created_at=datetime(2026, 1, 2, 4, 0, 0, tzinfo=timezone.utc),
        latest_run_finished_at=datetime(2026, 1, 2, 4, 1, 0, tzinfo=timezone.utc),
        latest_run_error_message="Source object URI missing.",
    )

    response = PipelineSourceCandidateResponse.model_validate(candidate)
    assert response.latest_run_id == "run-2"
    assert response.latest_run_status == "blocked"
    assert response.latest_run_ref == "backfill:ing-1"
    assert response.latest_run_finished_at == datetime(2026, 1, 2, 4, 1, 0, tzinfo=timezone.utc)
    assert response.latest_run_error_message == "Source object URI missing."



def test_pipeline_source_candidate_includes_backfill_preview_fields():
    from datetime import datetime, timezone
    from data_platform.utils.pipeline_definitions import build_pipeline_source_candidate
    from data_platform.schemas.pipeline import PipelineSourceCandidateResponse

    candidate = build_pipeline_source_candidate(
        ingestion_job_id="job-123",
        dataset_id="dataset-1",
        source_layer="silver",
        status="succeeded",
        created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        finished_at=None,
        object_uri="s3://bucket/silver.parquet",
        existing_run_count=2,
        run_ref_prefix="backfill",
    )

    parsed = PipelineSourceCandidateResponse.model_validate(candidate)
    assert parsed.suggested_run_ref == "backfill:job-123"
    assert parsed.would_skip_with_skip_existing_runs is True


def test_build_pipeline_schema_compatibility_preview_for_dataset_transform_uses_source_schema_as_candidate() -> None:
    preview = build_pipeline_schema_compatibility_preview(
        engine="sql",
        target_layer="gold",
        definition_json={"mode": "dataset_transform"},
        source_schema_snapshot=build_pipeline_schema_snapshot(
            layer="silver",
            version=2,
            fingerprint="silver-fp",
            schema_json=[{"name": "id", "type": "BIGINT"}, {"name": "total", "type": "DOUBLE"}],
        ),
        target_schema_snapshot=build_pipeline_schema_snapshot(
            layer="gold",
            version=4,
            fingerprint="gold-fp",
            schema_json=[{"name": "id", "type": "BIGINT"}],
        ),
    )

    assert preview is not None
    assert preview["layer"] == "gold"
    assert preview["against_version"] == 4
    assert preview["against_fingerprint"] == "gold-fp"
    assert preview["candidate_fingerprint"]
    assert preview["added_columns"] == [{"name": "total", "type": "DOUBLE"}]
    assert preview["contract_compatible"] is True
    assert preview["strict_mode_compatible"] is False


def test_build_pipeline_schema_compatibility_preview_unavailable_reason_for_custom_sql() -> None:
    reason = build_pipeline_schema_compatibility_preview_unavailable_reason(
        engine="sql",
        target_layer="gold",
        definition_json={"mode": "custom_sql", "sql": "SELECT * FROM source"},
        source_schema_snapshot=build_pipeline_schema_snapshot(
            layer="silver",
            version=2,
            fingerprint="silver-fp",
            schema_json=[{"name": "id", "type": "BIGINT"}],
        ),
    )

    assert reason == "Compatibility preview is only available for sql dataset-transform pipelines."



def test_build_pipeline_schema_compatibility_preview_unavailable_reason_for_non_sql_engine() -> None:
    reason = build_pipeline_schema_compatibility_preview_unavailable_reason(
        engine="python",
        target_layer="gold",
        definition_json={},
        source_schema_snapshot=build_pipeline_schema_snapshot(
            layer="silver",
            version=2,
            fingerprint="silver-fp",
            schema_json=[{"name": "id", "type": "BIGINT"}],
        ),
    )

    assert reason == "Compatibility preview is only available for sql pipelines."



def test_build_pipeline_schema_compatibility_preview_unavailable_reason_for_missing_source_snapshot() -> None:
    reason = build_pipeline_schema_compatibility_preview_unavailable_reason(
        engine="sql",
        target_layer="gold",
        definition_json={"mode": "dataset_transform"},
        source_schema_snapshot=None,
    )

    assert reason == "Compatibility preview requires a source schema snapshot for the pipeline source layer."



def test_build_pipeline_schema_compatibility_preview_is_none_for_custom_sql() -> None:
    preview = build_pipeline_schema_compatibility_preview(
        engine="sql",
        target_layer="gold",
        definition_json={"mode": "custom_sql", "sql": "SELECT * FROM source"},
        source_schema_snapshot=build_pipeline_schema_snapshot(
            layer="silver",
            version=2,
            fingerprint="silver-fp",
            schema_json=[{"name": "id", "type": "BIGINT"}],
        ),
        target_schema_snapshot=None,
    )

    assert preview is None


def test_extract_pipeline_run_snapshot_returns_schema_compatibility_preview_unavailable_reason_when_present() -> None:
    execution_plan = build_pipeline_execution_plan(
        pipeline_id="pipe-1",
        dataset_id="dataset-1",
        source_layer="silver",
        target_layer="gold",
        engine="sql",
        definition_json={"mode": "custom_sql", "sql": "SELECT * FROM source"},
        dataset_silver_sql=None,
        dataset_gold_sql="SELECT * FROM source",
        source_ingestion_job_id="job-5",
        source_object_uri="s3://silver/orders/job-5/part-00000.parquet",
    )

    payload = build_pipeline_run_payload(
        pipeline_id="pipe-1",
        dataset_id="dataset-1",
        execution_plan=execution_plan,
        schema_context=build_pipeline_schema_context(
            source_schema_snapshot=build_pipeline_schema_snapshot(
                layer="silver",
                version=2,
                fingerprint="silver-fp",
                schema_json=[{"name": "id", "type": "BIGINT"}],
            ),
            schema_compatibility_preview_unavailable_reason="Compatibility preview is only available for sql dataset-transform pipelines.",
        ),
    )

    snapshot = extract_pipeline_run_snapshot(payload["metrics_json"])

    assert snapshot["schema_compatibility_preview"] is None
    assert snapshot["schema_compatibility_preview_unavailable_reason"] == "Compatibility preview is only available for sql dataset-transform pipelines."



def test_extract_pipeline_run_snapshot_returns_schema_compatibility_preview_when_present() -> None:
    execution_plan = build_pipeline_execution_plan(
        pipeline_id="pipe-1",
        dataset_id="dataset-1",
        source_layer="silver",
        target_layer="gold",
        engine="sql",
        definition_json={},
        dataset_silver_sql=None,
        dataset_gold_sql="SELECT * FROM source",
        source_ingestion_job_id="job-5",
        source_object_uri="s3://silver/orders/job-5/part-00000.parquet",
    )
    compatibility_preview = build_pipeline_schema_compatibility_preview(
        engine="sql",
        target_layer="gold",
        definition_json={"mode": "dataset_transform"},
        source_schema_snapshot=build_pipeline_schema_snapshot(
            layer="silver",
            version=2,
            fingerprint="silver-fp",
            schema_json=[{"name": "id", "type": "BIGINT"}],
        ),
        target_schema_snapshot=None,
    )

    payload = build_pipeline_run_payload(
        pipeline_id="pipe-1",
        dataset_id="dataset-1",
        execution_plan=execution_plan,
        schema_context=build_pipeline_schema_context(
            source_schema_snapshot=build_pipeline_schema_snapshot(
                layer="silver",
                version=2,
                fingerprint="silver-fp",
                schema_json=[{"name": "id", "type": "BIGINT"}],
            ),
            schema_compatibility_preview=compatibility_preview,
        ),
    )

    snapshot = extract_pipeline_run_snapshot(payload["metrics_json"])

    assert snapshot["schema_compatibility_preview"] is not None
    assert snapshot["schema_compatibility_preview"]["against_version"] == 0
    assert snapshot["schema_compatibility_preview"]["candidate_schema"] == [{"name": "id", "type": "BIGINT"}]


def test_pipeline_run_response_exposes_schema_compatibility_preview_unavailable_reason() -> None:
    execution_plan = build_pipeline_execution_plan(
        pipeline_id="pipe-1",
        dataset_id="dataset-1",
        source_layer="silver",
        target_layer="gold",
        engine="sql",
        definition_json={"mode": "custom_sql", "sql": "SELECT * FROM source"},
        dataset_silver_sql=None,
        dataset_gold_sql="SELECT * FROM source",
        source_ingestion_job_id="job-5",
        source_object_uri="s3://silver/orders/job-5/part-00000.parquet",
    )

    payload = build_pipeline_run_payload(
        pipeline_id="pipe-1",
        dataset_id="dataset-1",
        execution_plan=execution_plan,
        schema_context=build_pipeline_schema_context(
            source_schema_snapshot=build_pipeline_schema_snapshot(
                layer="silver",
                version=2,
                fingerprint="silver-fp",
                schema_json=[{"name": "id", "type": "BIGINT"}],
            ),
            schema_compatibility_preview_unavailable_reason="Compatibility preview is only available for sql dataset-transform pipelines.",
        ),
    )

    response = PipelineRunResponse.model_validate({
        "id": "run-1",
        "pipeline_id": "pipe-1",
        "dataset_id": "dataset-1",
        "ingestion_job_id": "job-5",
        "status": payload["status"],
        "run_ref": payload["run_ref"],
        "metrics_json": payload["metrics_json"],
        "error_message": payload["error_message"],
        "started_at": payload["started_at"],
        "finished_at": payload["finished_at"],
        "created_at": datetime(2026, 4, 15, 19, 0, tzinfo=timezone.utc),
        "updated_at": datetime(2026, 4, 15, 19, 0, tzinfo=timezone.utc),
        **extract_pipeline_run_snapshot(payload["metrics_json"]),
    })

    assert response.schema_compatibility_preview is None
    assert response.schema_compatibility_preview_unavailable_reason == "Compatibility preview is only available for sql dataset-transform pipelines."



def test_pipeline_run_response_exposes_schema_compatibility_preview() -> None:
    execution_plan = build_pipeline_execution_plan(
        pipeline_id="pipe-1",
        dataset_id="dataset-1",
        source_layer="silver",
        target_layer="gold",
        engine="sql",
        definition_json={},
        dataset_silver_sql=None,
        dataset_gold_sql="SELECT * FROM source",
        source_ingestion_job_id="job-5",
        source_object_uri="s3://silver/orders/job-5/part-00000.parquet",
    )
    compatibility_preview = build_pipeline_schema_compatibility_preview(
        engine="sql",
        target_layer="gold",
        definition_json={"mode": "dataset_transform"},
        source_schema_snapshot=build_pipeline_schema_snapshot(
            layer="silver",
            version=2,
            fingerprint="silver-fp",
            schema_json=[{"name": "id", "type": "BIGINT"}, {"name": "total", "type": "DOUBLE"}],
        ),
        target_schema_snapshot=build_pipeline_schema_snapshot(
            layer="gold",
            version=4,
            fingerprint="gold-fp",
            schema_json=[{"name": "id", "type": "BIGINT"}],
        ),
    )

    payload = build_pipeline_run_payload(
        pipeline_id="pipe-1",
        dataset_id="dataset-1",
        execution_plan=execution_plan,
        schema_context=build_pipeline_schema_context(
            source_schema_snapshot=build_pipeline_schema_snapshot(
                layer="silver",
                version=2,
                fingerprint="silver-fp",
                schema_json=[{"name": "id", "type": "BIGINT"}, {"name": "total", "type": "DOUBLE"}],
            ),
            target_schema_snapshot=build_pipeline_schema_snapshot(
                layer="gold",
                version=4,
                fingerprint="gold-fp",
                schema_json=[{"name": "id", "type": "BIGINT"}],
            ),
            schema_compatibility_preview=compatibility_preview,
        ),
    )

    response = PipelineRunResponse.model_validate({
        "id": "run-1",
        "pipeline_id": "pipe-1",
        "dataset_id": "dataset-1",
        "ingestion_job_id": "job-5",
        "status": payload["status"],
        "run_ref": payload["run_ref"],
        "metrics_json": payload["metrics_json"],
        "error_message": payload["error_message"],
        "started_at": payload["started_at"],
        "finished_at": payload["finished_at"],
        "created_at": datetime(2026, 4, 15, 19, 0, tzinfo=timezone.utc),
        "updated_at": datetime(2026, 4, 15, 19, 0, tzinfo=timezone.utc),
        **extract_pipeline_run_snapshot(payload["metrics_json"]),
    })

    assert response.schema_compatibility_preview is not None
    assert response.schema_compatibility_preview.against_version == 4
    assert response.schema_compatibility_preview.contract_compatible is True
    assert response.schema_compatibility_preview.added_columns == [{"name": "total", "type": "DOUBLE"}]



def test_create_pipeline_backfill_runs_page_request_requires_time_bound() -> None:
    with pytest.raises(ValueError, match="At least one of source_finished_at_gte or source_finished_at_lte is required"):
        CreatePipelineBackfillRunsPageRequest()



def test_create_pipeline_backfill_runs_page_request_trims_run_ref_prefix_and_cursor() -> None:
    payload = CreatePipelineBackfillRunsPageRequest(
        run_ref_prefix="  nightly backfill  ",
        cursor="  cursor-token  ",
        source_finished_at_lte=datetime(2026, 4, 14, 18, 30, tzinfo=timezone.utc),
    )

    assert payload.run_ref_prefix == "nightly backfill"
    assert payload.cursor == "cursor-token"



def test_build_backfill_request_snapshot_serializes_cursor_context() -> None:
    snapshot = build_backfill_request_snapshot(
        source_finished_at_gte=datetime(2026, 4, 14, 17, 0, tzinfo=timezone.utc),
        source_finished_at_lte=datetime(2026, 4, 14, 18, 30, tzinfo=timezone.utc),
        limit=25,
        offset=0,
        cursor="  cursor-token  ",
        run_ref_prefix=" nightly ",
        skip_existing_runs=True,
    )

    extracted = extract_pipeline_run_snapshot({"backfill_request": snapshot})["backfill_request"]

    assert extracted == {
        "run_ref_prefix": "nightly",
        "source_finished_at_gte": datetime(2026, 4, 14, 17, 0, tzinfo=timezone.utc),
        "source_finished_at_lte": datetime(2026, 4, 14, 18, 30, tzinfo=timezone.utc),
        "limit": 25,
        "offset": 0,
        "cursor": "cursor-token",
        "skip_existing_runs": True,
    }



def test_pipeline_backfill_runs_page_response_accepts_items_and_next_cursor() -> None:
    now = datetime(2026, 4, 15, 12, 0, tzinfo=timezone.utc)
    response = PipelineBackfillRunsPageResponse.model_validate(
        {
            "items": [
                {
                    "id": "run-1",
                    "pipeline_id": "pipe-1",
                    "dataset_id": "dataset-1",
                    "ingestion_job_id": "job-1",
                    "status": "planned",
                    "run_ref": "nightly:job-1",
                    "metrics_json": {},
                    "error_message": None,
                    "started_at": None,
                    "finished_at": None,
                    "created_at": now,
                    "updated_at": now,
                    "preflighted_at": None,
                    "execution_plan": None,
                    "backfill_request": None,
                    "source_schema_snapshot": None,
                    "target_schema_snapshot": None,
                    "schema_compatibility_preview": None,
                    "schema_compatibility_preview_unavailable_reason": None,
                    "contract_compatibility_required": False,
                    "contract_compatibility_outcome": None,
                }
            ],
            "next_cursor": "cursor-token",
        }
    )

    assert response.next_cursor == "cursor-token"
    assert response.items[0].run_ref == "nightly:job-1"
