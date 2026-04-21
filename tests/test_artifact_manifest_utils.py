from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from data_platform.utils.artifacts import (
    build_ingestion_artifact_manifest,
    build_ingestion_artifact_manifest_item,
    build_ingestion_artifact_manifest_payload,
    build_pipeline_run_artifact_manifest,
)


def test_build_ingestion_artifact_manifest_item_normalizes_snapshot() -> None:
    item = build_ingestion_artifact_manifest_item(
        layer=" Silver ",
        object_uri="s3://silver/orders/job-1/part-00000.parquet",
        schema_snapshot={
            "layer": "SILVER",
            "version": "2",
            "fingerprint": "fp-2",
            "schema_json": [{"name": "id", "type": "BIGINT"}],
        },
    )

    assert item == {
        "layer": "silver",
        "object_uri": "s3://silver/orders/job-1/part-00000.parquet",
        "schema_snapshot": {
            "layer": "silver",
            "version": 2,
            "fingerprint": "fp-2",
            "schema_json": [{"name": "id", "type": "BIGINT"}],
        },
    }


def test_build_ingestion_artifact_manifest_payload_filters_missing_items() -> None:
    now = datetime(2026, 4, 16, tzinfo=timezone.utc)
    payload = build_ingestion_artifact_manifest_payload(
        ingestion_job_id="job-1",
        dataset_id="dataset-1",
        dataset_slug="orders",
        status="succeeded",
        source_type="upload",
        filename="orders.csv",
        source_format="csv",
        source_content_type="text/csv",
        content_hash="abc",
        size_bytes=123,
        row_count=10,
        created_at=now,
        started_at=now,
        finished_at=now,
        effective_at=now,
        artifacts=[None, {"layer": "raw", "object_uri": "s3://raw/orders/job-1/orders.csv"}],
    )

    assert payload["dataset_slug"] == "orders"
    assert payload["artifacts"] == [{"layer": "raw", "object_uri": "s3://raw/orders/job-1/orders.csv"}]


def test_build_ingestion_artifact_manifest_returns_bucket_and_key_details() -> None:
    job = SimpleNamespace(
        id="job-1",
        dataset_id="dataset-1",
        status="succeeded",
        source_format="csv",
        source_content_type="text/csv",
        raw_object_uri="s3://raw/orders/job-1/orders.csv",
        silver_object_uri="s3://silver/orders/job-1/part-00000.parquet",
        gold_object_uri="s3://gold/orders/job-1/part-00000.parquet",
        row_count=25,
    )

    manifest = build_ingestion_artifact_manifest(job)

    assert manifest["resource_type"] == "ingestion"
    assert manifest["items"][0]["bucket"] == "raw"
    assert manifest["items"][1]["format"] == "parquet"
    assert manifest["items"][2]["row_count"] == 25


def test_build_pipeline_run_artifact_manifest_returns_source_and_target_items() -> None:
    run = SimpleNamespace(
        id="run-1",
        pipeline_id="pipeline-1",
        dataset_id="dataset-1",
        ingestion_job_id="job-5",
        status="succeeded",
        metrics_json={
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
            },
            "execution": {
                "executor": "sql_builtin",
                "status": "succeeded",
                "target_object_uri": "s3://gold/orders/run-1/part-00000.parquet",
                "output_row_count": 25,
                "target_schema_version": 3,
                "target_schema_fingerprint": "gold-fp",
            },
        },
    )

    manifest = build_pipeline_run_artifact_manifest(run)

    assert manifest["resource_type"] == "pipeline_run"
    assert manifest["items"][0]["name"] == "source"
    assert manifest["items"][0]["schema_version"] == 2
    assert manifest["items"][1]["name"] == "target"
    assert manifest["items"][1]["row_count"] == 25
