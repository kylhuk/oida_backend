from __future__ import annotations

from datetime import datetime, timezone

from data_platform.utils.pipeline_definitions import extract_pipeline_run_snapshot



def test_extract_pipeline_run_snapshot_exposes_execution_details() -> None:
    snapshot = extract_pipeline_run_snapshot(
        {
            "execution": {
                "executor": " sql_builtin ",
                "status": " succeeded ",
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



def test_extract_pipeline_run_snapshot_ignores_invalid_execution_details() -> None:
    snapshot = extract_pipeline_run_snapshot(
        {
            "execution": {
                "executor": "   ",
                "status": "not-a-status",
                "started_at": "not-a-datetime",
                "output_row_count": -1,
                "output_schema": "not-a-schema",
                "target_schema_version": 0,
            }
        }
    )

    assert "execution_details" not in snapshot
