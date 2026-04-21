from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from data_platform.models.ingestion import IngestionJob
from data_platform.schemas.dataset import CreateDatasetRequest
from data_platform.schemas.pipeline import CreatePipelineDefinitionRequest, CreatePipelineRunRequest
from data_platform.services.dataset_service import DatasetService
from data_platform.services.pipeline_execution_service import SqlPipelineExecutionService
from data_platform.services.pipeline_service import PipelineService
from data_platform.utils.paths import object_uri, parse_object_uri


class LocalStorageStub:
    def __init__(self, raw_objects: dict[tuple[str, str], Path], root: Path):
        self.raw_objects = raw_objects
        self.root = root
        self.uploads: list[tuple[str, str]] = []

    def download_file(self, bucket: str, key: str, local_path: str | Path) -> None:
        source = self.raw_objects[(bucket, key)]
        Path(local_path).parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, local_path)

    def upload_file(self, bucket: str, key: str, local_path: str | Path, content_type: str | None = None) -> str:
        destination = self.root / bucket / key
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(local_path, destination)
        self.uploads.append((bucket, key))
        return object_uri(bucket, key)



def test_execute_next_run_claims_and_executes_planned_sql_run(db_session, tmp_path: Path):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(
            slug="orders-pipeline-executor",
            name="Orders Pipeline Executor",
            gold_sql="SELECT id, amount FROM source WHERE amount >= 20",
        ),
    )
    raw_csv = tmp_path / "orders.csv"
    raw_csv.write_text("id,amount\n1,10\n2,25\n", encoding="utf-8")

    raw_bucket = "raw"
    raw_key = "bootstrap/orders.csv"
    source_job = IngestionJob(
        dataset_id=dataset.id,
        source_type="object_uri",
        status="succeeded",
        filename="orders.csv",
        source_content_type="text/csv; charset=utf-8",
        raw_object_uri=object_uri(raw_bucket, raw_key),
        job_metadata={},
    )
    db_session.add(source_job)
    db_session.commit()

    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="orders_pipeline_executor_refresh",
            source_layer="raw",
            target_layer="gold",
            engine="sql",
        ),
    )
    planned = PipelineService.create_pipeline_run(
        db_session,
        pipeline,
        CreatePipelineRunRequest(source_ingestion_job_id=source_job.id, run_ref="executor-run"),
    )

    storage = LocalStorageStub({(raw_bucket, raw_key): raw_csv}, tmp_path / "object-store")
    executed = SqlPipelineExecutionService(db_session, storage=storage).execute_next_run(
        pipeline.id,
        task_id="task-42",
    )

    assert executed is not None
    db_session.refresh(planned)
    assert planned.id == executed.id
    assert planned.status == "succeeded"
    assert planned.started_at is not None
    assert planned.finished_at is not None
    assert planned.error_message is None

    execution = planned.metrics_json["execution"]
    assert execution["executor"] == "sql_builtin"
    assert execution["status"] == "succeeded"
    assert execution["task_id"] == "task-42"
    assert execution["output_row_count"] == 1
    assert execution["output_schema"] == [
        {"name": "id", "type": "BIGINT"},
        {"name": "amount", "type": "BIGINT"},
    ]
    assert execution["target_schema_version"] == 1

    target_bucket, target_key = parse_object_uri(execution["target_object_uri"])
    assert target_bucket == "gold"
    assert storage.uploads == [(target_bucket, target_key)]
    assert (tmp_path / "object-store" / target_bucket / target_key).exists()

    snapshot = DatasetService.latest_schema_snapshot(db_session, dataset.id, "gold")
    assert snapshot is not None
    assert snapshot.version == 1
    assert snapshot.schema_json == execution["output_schema"]



def test_execute_next_run_marks_run_failed_when_sql_execution_errors(db_session, tmp_path: Path):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(
            slug="orders-pipeline-executor-failure",
            name="Orders Pipeline Executor Failure",
            gold_sql="SELECT missing_column FROM source",
        ),
    )
    raw_csv = tmp_path / "orders.csv"
    raw_csv.write_text("id,amount\n1,10\n2,25\n", encoding="utf-8")

    raw_bucket = "raw"
    raw_key = "bootstrap/orders.csv"
    source_job = IngestionJob(
        dataset_id=dataset.id,
        source_type="object_uri",
        status="succeeded",
        filename="orders.csv",
        source_content_type="text/csv; charset=utf-8",
        raw_object_uri=object_uri(raw_bucket, raw_key),
        job_metadata={},
    )
    db_session.add(source_job)
    db_session.commit()

    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="orders_pipeline_executor_failure_refresh",
            source_layer="raw",
            target_layer="gold",
            engine="sql",
        ),
    )
    planned = PipelineService.create_pipeline_run(
        db_session,
        pipeline,
        CreatePipelineRunRequest(source_ingestion_job_id=source_job.id),
    )

    storage = LocalStorageStub({(raw_bucket, raw_key): raw_csv}, tmp_path / "object-store")
    service = SqlPipelineExecutionService(db_session, storage=storage)

    with pytest.raises(Exception, match="missing_column"):
        service.execute_next_run(pipeline.id, task_id="task-99")

    db_session.refresh(planned)
    assert planned.status == "failed"
    assert planned.finished_at is not None
    assert planned.error_message is not None
    assert "missing_column" in planned.error_message

    execution = planned.metrics_json["execution"]
    assert execution["executor"] == "sql_builtin"
    assert execution["status"] == "failed"
    assert execution["task_id"] == "task-99"
    assert "missing_column" in execution["error_message"]
    assert storage.uploads == []



def test_execute_next_run_returns_none_when_no_planned_run_is_available(db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(slug="orders-pipeline-executor-idle", name="Orders Pipeline Executor Idle"),
    )
    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="orders_pipeline_executor_idle_refresh",
            source_layer="raw",
            target_layer="gold",
            engine="sql",
        ),
    )

    assert SqlPipelineExecutionService(db_session).execute_next_run(pipeline.id) is None



def test_execute_next_run_marks_run_failed_when_persisted_execution_plan_is_invalid(db_session):
    dataset = DatasetService.create_dataset(
        db_session,
        CreateDatasetRequest(slug="orders-pipeline-executor-invalid-plan", name="Orders Pipeline Executor Invalid Plan"),
    )
    source_job = IngestionJob(
        dataset_id=dataset.id,
        source_type="object_uri",
        status="succeeded",
        filename="orders.csv",
        raw_object_uri="s3://raw/orders.csv",
        job_metadata={},
    )
    db_session.add(source_job)
    db_session.commit()

    pipeline = PipelineService.create_pipeline(
        db_session,
        dataset,
        CreatePipelineDefinitionRequest(
            name="orders_pipeline_executor_invalid_plan_refresh",
            source_layer="raw",
            target_layer="gold",
            engine="sql",
        ),
    )
    planned = PipelineService.create_pipeline_run(
        db_session,
        pipeline,
        CreatePipelineRunRequest(source_ingestion_job_id=source_job.id),
    )
    planned.metrics_json = {"execution_plan": {"executable": True}}
    db_session.add(planned)
    db_session.commit()

    service = SqlPipelineExecutionService(db_session)

    with pytest.raises(ValueError, match="missing a resolved SQL query"):
        service.execute_next_run(pipeline.id, task_id="task-invalid-plan")

    db_session.refresh(planned)
    assert planned.status == "failed"
    assert planned.finished_at is not None
    assert planned.error_message is not None
    assert "missing a resolved SQL query" in planned.error_message

    execution = planned.metrics_json["execution"]
    assert execution["executor"] == "sql_builtin"
    assert execution["status"] == "failed"
    assert execution["task_id"] == "task-invalid-plan"
    assert "missing a resolved SQL query" in execution["error_message"]
