from __future__ import annotations

from data_platform.utils.pipeline_definitions import extract_pipeline_artifact_manifest


def test_extract_pipeline_artifact_manifest_exposes_preflight_and_execution_artifacts() -> None:
    manifest = extract_pipeline_artifact_manifest(
        {
            "execution_plan": {
                "engine": "sql",
                "source_layer": "silver",
                "target_layer": "gold",
                "source_ingestion_job_id": "job-5",
                "source_object_uri": "s3://silver/orders/job-5/part-00000.parquet",
            },
            "schema_context": {
                "source_schema_snapshot": {
                    "layer": "silver",
                    "version": 2,
                    "fingerprint": "silver-fp",
                    "schema_json": [{"name": "id", "type": "BIGINT"}],
                },
                "target_schema_snapshot": {
                    "layer": "gold",
                    "version": 3,
                    "fingerprint": "gold-fp",
                    "schema_json": [{"name": "id", "type": "BIGINT"}],
                },
            },
            "execution": {
                "executor": " sql_builtin ",
                "status": " succeeded ",
                "task_id": " task-42 ",
                "target_object_uri": " s3://gold/orders/run-1/part-00000.parquet ",
                "output_row_count": 25,
                "output_schema": [{"name": " id ", "type": " BIGINT "}],
                "target_schema_version": 3,
                "target_schema_fingerprint": " gold-fp ",
            },
        },
        run_id=" run-1 ",
        pipeline_id=" pipeline-1 ",
        dataset_id=" dataset-1 ",
        source_ingestion_job_id=" job-5 ",
        run_status=" succeeded ",
    )

    assert manifest == {
        "run_id": "run-1",
        "pipeline_id": "pipeline-1",
        "dataset_id": "dataset-1",
        "source_ingestion_job_id": "job-5",
        "run_status": "succeeded",
        "execution_status": "succeeded",
        "engine": "sql",
        "source_layer": "silver",
        "target_layer": "gold",
        "executor": "sql_builtin",
        "task_id": "task-42",
        "source_object_uri": "s3://silver/orders/job-5/part-00000.parquet",
        "target_object_uri": "s3://gold/orders/run-1/part-00000.parquet",
        "output_row_count": 25,
        "output_schema": [{"name": "id", "type": "BIGINT"}],
        "target_schema_version": 3,
        "target_schema_fingerprint": "gold-fp",
        "source_schema_snapshot": {
            "layer": "silver",
            "version": 2,
            "fingerprint": "silver-fp",
            "schema_json": [{"name": "id", "type": "BIGINT"}],
        },
        "target_schema_snapshot": {
            "layer": "gold",
            "version": 3,
            "fingerprint": "gold-fp",
            "schema_json": [{"name": "id", "type": "BIGINT"}],
        },
    }


def test_extract_pipeline_artifact_manifest_returns_none_without_artifact_metadata() -> None:
    assert extract_pipeline_artifact_manifest({"execution_plan": {"engine": "sql"}}) is None
