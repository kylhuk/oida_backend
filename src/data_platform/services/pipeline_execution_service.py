from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from sqlalchemy.orm import Session

from data_platform.audit_trail import build_system_audit_event, persist_audit_event_with_session_factory
from data_platform.enums import DatasetLayer, PipelineEngine, PipelineStatus
from data_platform.models.dataset import Dataset
from data_platform.models.ingestion import IngestionJob
from data_platform.models.pipeline import PipelineDefinition, PipelineRun
from data_platform.schemas.pipeline import UpdatePipelineRunStatusRequest
from data_platform.services.dataset_service import DatasetService
from data_platform.services.duckdb_service import DuckDBService
from data_platform.services.notifications import WebhookNotificationService, build_pipeline_run_notification_payload
from data_platform.services.pipeline_service import PipelineService
from data_platform.services.storage import ObjectStorageService
from data_platform.settings import Settings, get_settings
from data_platform.utils.formats import detect_file_format
from data_platform.utils.paths import build_layer_object_key, parse_object_uri, sanitize_filename


class SqlPipelineExecutionService:
    def __init__(
        self,
        session: Session,
        *,
        settings: Settings | None = None,
        storage: ObjectStorageService | None = None,
        duckdb_service: DuckDBService | None = None,
    ) -> None:
        self.session = session
        self.settings = settings or get_settings()
        self.storage = storage or ObjectStorageService(settings=self.settings)
        self.duckdb_service = duckdb_service or DuckDBService(settings=self.settings)

    def _notify_pipeline_run(
        self,
        event_type: str,
        pipeline: PipelineDefinition,
        run: PipelineRun,
        *,
        dataset: Dataset | None = None,
        task_id: str | None = None,
    ) -> None:
        WebhookNotificationService(settings=self.settings).notify(
            event_type,
            build_pipeline_run_notification_payload(run, pipeline, dataset, task_id=task_id),
        )

    def _audit_pipeline_run(
        self,
        event_type: str,
        pipeline: PipelineDefinition,
        run: PipelineRun,
        *,
        dataset: Dataset | None = None,
        task_id: str | None = None,
    ) -> None:
        if not self.settings.enable_audit_trail:
            return

        from data_platform.db import get_session_factory

        details_json = {
            "status": run.status,
            "pipeline_id": pipeline.id,
            "pipeline_name": pipeline.name,
            "dataset_id": run.dataset_id,
            "dataset_slug": getattr(dataset, "slug", None),
            "run_ref": run.run_ref,
            "error_message": run.error_message,
            "execution_details": PipelineService.build_pipeline_run_response(run).get("execution_details"),
            "artifact_manifest": PipelineService.build_pipeline_run_artifact_manifest(run),
        }
        if task_id is not None:
            details_json["task_id"] = task_id
        payload = build_system_audit_event(
            event_type,
            resource_type="pipeline_run",
            resource_id=run.id,
            path=f"/worker/pipeline-runs/{run.id}",
            status_code=200 if event_type.endswith("started") or event_type.endswith("succeeded") else 500,
            details_json=details_json,
        )
        persist_audit_event_with_session_factory(get_session_factory(), payload)

    @staticmethod
    def _target_bucket_for_layer(settings: Settings, layer: DatasetLayer | str) -> str:
        layer_value = layer if isinstance(layer, DatasetLayer) else DatasetLayer(str(layer).strip().lower())
        if layer_value == DatasetLayer.RAW:
            return settings.s3_raw_bucket
        if layer_value == DatasetLayer.SILVER:
            return settings.s3_silver_bucket
        if layer_value == DatasetLayer.GOLD:
            return settings.s3_gold_bucket
        raise ValueError(f"Unsupported pipeline target layer: {layer!r}")

    @staticmethod
    def _source_filename(source_job: IngestionJob | None, source_key: str) -> str:
        preferred = source_job.filename if source_job and source_job.filename else Path(source_key).name
        return sanitize_filename(preferred or "source.bin")

    @staticmethod
    def _merge_execution_metrics(run: PipelineRun, extra: dict[str, Any]) -> None:
        metrics = dict(run.metrics_json or {})
        execution = dict(metrics.get("execution") or {})
        execution.update(extra)
        metrics["execution"] = execution
        run.metrics_json = metrics

    @staticmethod
    def _execution_plan_for_run(run: PipelineRun) -> dict[str, Any]:
        metrics = run.metrics_json if isinstance(run.metrics_json, dict) else {}
        execution_plan = metrics.get("execution_plan")
        if not isinstance(execution_plan, dict):
            raise ValueError(f"Pipeline run {run.id!r} is missing a persisted execution plan.")
        if not bool(execution_plan.get("executable")):
            raise ValueError(f"Pipeline run {run.id!r} is not executable.")
        resolved_query = execution_plan.get("resolved_query")
        if not isinstance(resolved_query, str) or not resolved_query.strip():
            raise ValueError(f"Pipeline run {run.id!r} is missing a resolved SQL query.")
        source_object_uri = execution_plan.get("source_object_uri")
        if not isinstance(source_object_uri, str) or not source_object_uri.strip():
            raise ValueError(f"Pipeline run {run.id!r} is missing a source object URI.")
        return execution_plan

    def execute_next_run(self, pipeline_id: str, *, task_id: str | None = None) -> PipelineRun | None:
        pipeline = PipelineService.get_pipeline(self.session, pipeline_id)
        if pipeline is None:
            raise ValueError(f"Pipeline {pipeline_id!r} not found.")
        run = PipelineService.claim_next_pipeline_run(self.session, pipeline)
        if run is None:
            return None
        return self.execute_claimed_run(pipeline, run, task_id=task_id)

    def execute_claimed_run(
        self,
        pipeline: PipelineDefinition,
        run: PipelineRun,
        *,
        task_id: str | None = None,
    ) -> PipelineRun:
        if pipeline.engine != PipelineEngine.SQL.value:
            raise ValueError("Only sql pipelines can be executed by the built-in SQL executor.")
        if run.pipeline_id != pipeline.id:
            raise ValueError(f"Pipeline run {run.id!r} does not belong to pipeline {pipeline.id!r}.")
        if run.status not in {PipelineStatus.PENDING.value, PipelineStatus.RUNNING.value}:
            raise ValueError(
                f"Pipeline run {run.id!r} must be in 'pending' or 'running' status before execution, got {run.status!r}."
            )

        if run.status == PipelineStatus.PENDING.value:
            run = PipelineService.transition_pipeline_run(
                self.session,
                pipeline,
                run,
                UpdatePipelineRunStatusRequest(status=PipelineStatus.RUNNING.value),
            )

        execution_started_at = run.started_at or datetime.now(timezone.utc)
        self._merge_execution_metrics(
            run,
            {
                "executor": "sql_builtin",
                "status": PipelineStatus.RUNNING.value,
                "started_at": execution_started_at.isoformat(),
                **({"task_id": task_id} if task_id is not None else {}),
            },
        )
        self.session.add(run)
        self.session.commit()
        self.session.refresh(run)
        self._audit_pipeline_run("pipeline_run.started", pipeline, run, task_id=task_id)

        source_object_uri: str | None = None
        dataset: Dataset | None = None

        try:
            execution_plan = self._execution_plan_for_run(run)
            dataset = pipeline.dataset or self.session.get(Dataset, pipeline.dataset_id)
            if dataset is None:
                raise ValueError(f"Dataset {pipeline.dataset_id!r} not found for pipeline {pipeline.id!r}.")

            source_object_uri = str(execution_plan["source_object_uri"])
            resolved_query = str(execution_plan["resolved_query"])
            target_layer = DatasetLayer(str(execution_plan.get("target_layer") or pipeline.target_layer).strip().lower())
            source_job = self.session.get(IngestionJob, run.ingestion_job_id) if run.ingestion_job_id is not None else None

            with TemporaryDirectory() as tmpdir:
                tmpdir_path = Path(tmpdir)
                source_bucket, source_key = parse_object_uri(source_object_uri)
                local_source_path = tmpdir_path / self._source_filename(source_job, source_key)
                self.storage.download_file(source_bucket, source_key, local_source_path)

                detected_format = detect_file_format(
                    filename=source_job.filename if source_job and source_job.filename else Path(source_key).name,
                    content_type=source_job.source_content_type if source_job else None,
                    explicit_format=source_job.source_format if source_job else None,
                    local_path=local_source_path,
                )
                source_sql = self.duckdb_service.source_sql_for_file(local_source_path, detected_format, tmpdir_path)

                output_schema = self.duckdb_service.describe_query(resolved_query, views={"source": source_sql})
                output_local_path = tmpdir_path / "pipeline-output.parquet"
                self.duckdb_service.copy_query_to_parquet(resolved_query, output_local_path, views={"source": source_sql})
                row_count = self.duckdb_service.count_parquet_rows(output_local_path)

                target_bucket = self._target_bucket_for_layer(self.settings, target_layer)
                target_key = build_layer_object_key(
                    dataset.slug,
                    run.id,
                    "part-00000.parquet",
                    event_time=execution_started_at,
                )
                target_object_uri = self.storage.upload_file(
                    target_bucket,
                    target_key,
                    output_local_path,
                    content_type="application/octet-stream",
                )
                snapshot = DatasetService.save_schema_snapshot(self.session, dataset, target_layer.value, output_schema)

            finished_at = datetime.now(timezone.utc)
            self._merge_execution_metrics(
                run,
                {
                    "executor": "sql_builtin",
                    "status": PipelineStatus.SUCCEEDED.value,
                    "started_at": execution_started_at.isoformat(),
                    "finished_at": finished_at.isoformat(),
                    "source_object_uri": source_object_uri,
                    "target_object_uri": target_object_uri,
                    "output_row_count": int(row_count),
                    "output_schema": output_schema,
                    "target_schema_version": int(snapshot.version),
                    "target_schema_fingerprint": snapshot.fingerprint,
                    **({"task_id": task_id} if task_id is not None else {}),
                },
            )
            completed_run = PipelineService.transition_pipeline_run(
                self.session,
                pipeline,
                run,
                UpdatePipelineRunStatusRequest(status=PipelineStatus.SUCCEEDED.value),
            )
            self._notify_pipeline_run(
                "pipeline_run.succeeded",
                pipeline,
                completed_run,
                dataset=dataset,
                task_id=task_id,
            )
            self._audit_pipeline_run(
                "pipeline_run.succeeded",
                pipeline,
                completed_run,
                dataset=dataset,
                task_id=task_id,
            )
            return completed_run
        except Exception as exc:
            finished_at = datetime.now(timezone.utc)
            failure_metrics = {
                "executor": "sql_builtin",
                "status": PipelineStatus.FAILED.value,
                "started_at": execution_started_at.isoformat(),
                "finished_at": finished_at.isoformat(),
                "error_message": str(exc),
                **({"task_id": task_id} if task_id is not None else {}),
            }
            if source_object_uri is not None:
                failure_metrics["source_object_uri"] = source_object_uri
            self._merge_execution_metrics(run, failure_metrics)
            failed_run = PipelineService.transition_pipeline_run(
                self.session,
                pipeline,
                run,
                UpdatePipelineRunStatusRequest(status=PipelineStatus.FAILED.value, error_message=str(exc)),
            )
            self._notify_pipeline_run(
                "pipeline_run.failed",
                pipeline,
                failed_run,
                dataset=dataset,
                task_id=task_id,
            )
            self._audit_pipeline_run(
                "pipeline_run.failed",
                pipeline,
                failed_run,
                dataset=dataset,
                task_id=task_id,
            )
            raise
