from __future__ import annotations

import hashlib
import logging
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from time import perf_counter

from sqlalchemy.orm import Session

from data_platform.audit_trail import build_system_audit_event, persist_audit_event_with_session_factory
from data_platform.enums import IngestionStatus, SchemaMode, SourceType
from data_platform.models.ingestion import IngestionJob
from data_platform.services.clickhouse_service import ClickHouseService
from data_platform.services.dataset_service import DatasetService
from data_platform.services.duckdb_service import DuckDBService
from data_platform.services.ingestion_service import IngestionService
from data_platform.services.notifications import WebhookNotificationService, build_ingestion_job_notification_payload
from data_platform.services.quality_service import QualityService
from data_platform.services.storage import ObjectStorageService
from data_platform.settings import Settings, get_settings
from data_platform.utils.formats import detect_file_format
from data_platform.utils.paths import build_layer_object_key, object_uri, parse_object_uri, sanitize_filename
from data_platform.utils.schemas import merge_schemas

logger = logging.getLogger(__name__)


class MedallionProcessingService:
    def __init__(
        self,
        session: Session,
        *,
        settings: Settings | None = None,
        storage: ObjectStorageService | None = None,
        duckdb_service: DuckDBService | None = None,
        clickhouse: ClickHouseService | None = None,
    ) -> None:
        self.session = session
        self.settings = settings or get_settings()
        self.storage = storage or ObjectStorageService()
        self.duckdb_service = duckdb_service or DuckDBService()
        self.clickhouse = clickhouse or ClickHouseService()

    def _notify_ingestion_job(self, event_type: str, job: IngestionJob, dataset: object, *, task_id: str | None = None) -> None:
        WebhookNotificationService(settings=self.settings).notify(
            event_type,
            build_ingestion_job_notification_payload(job, dataset, task_id=task_id),
        )

    def _audit_ingestion_job(self, event_type: str, job: IngestionJob, dataset: object, *, task_id: str | None = None) -> None:
        if not self.settings.enable_audit_trail:
            return

        from data_platform.db import get_session_factory

        details_json = {
            "status": job.status,
            "dataset_id": getattr(job, "dataset_id", None),
            "dataset_slug": getattr(dataset, "slug", None),
            "row_count": getattr(job, "row_count", None),
            "error_message": getattr(job, "error_message", None),
            "raw_object_uri": getattr(job, "raw_object_uri", None),
            "silver_object_uri": getattr(job, "silver_object_uri", None),
            "gold_object_uri": getattr(job, "gold_object_uri", None),
        }
        if task_id is not None:
            details_json["task_id"] = task_id
        payload = build_system_audit_event(
            event_type,
            resource_type="ingestion_job",
            resource_id=job.id,
            path=f"/worker/ingestion-jobs/{job.id}",
            status_code=200 if event_type.endswith("started") or event_type.endswith("succeeded") else 500,
            details_json=details_json,
        )
        persist_audit_event_with_session_factory(get_session_factory(), payload)

    @staticmethod
    def _merge_job_metadata(job: IngestionJob, extra: dict) -> None:
        updated = dict(job.job_metadata or {})
        processing = dict(updated.get("processing") or {})
        processing.update(extra)
        updated["processing"] = processing
        job.job_metadata = updated

    @staticmethod
    def _compute_local_file_digest_and_size(local_path: str | Path) -> tuple[str, int]:
        digest = hashlib.sha256()
        total_size = 0
        with Path(local_path).open("rb") as source:
            while True:
                chunk = source.read(1024 * 1024)
                if not chunk:
                    break
                digest.update(chunk)
                total_size += len(chunk)
        return digest.hexdigest(), total_size

    def process_job(self, job_id: str, task_id: str | None = None) -> dict:
        job = self.session.get(IngestionJob, job_id)
        if not job:
            raise ValueError(f"Ingestion job {job_id!r} not found.")

        if job.status == IngestionStatus.SUCCEEDED.value and job.finished_at is not None:
            logger.info("Skipping already completed ingestion job=%s", job.id)
            return {"job_id": job.id, "status": job.status, "row_count": job.row_count or 0}

        dataset = job.dataset
        logger.info("Starting ingestion job=%s dataset=%s", job.id, dataset.slug)
        started = perf_counter()

        job.status = IngestionStatus.DOWNLOADING.value
        job.started_at = datetime.now(timezone.utc)
        self._merge_job_metadata(
            job,
            {
                "task_id": task_id,
                "started_at": job.started_at.isoformat(),
                "dataset_slug": dataset.slug,
            },
        )
        self.session.commit()
        self._audit_ingestion_job("ingestion_job.started", job, dataset, task_id=task_id)

        with TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            local_raw_path = tmpdir_path / sanitize_filename(job.filename or "payload.bin")

            if job.source_type == SourceType.URL.value:
                ingestion_service = IngestionService(self.session)
                raw_uri, size_bytes, content_hash, resolved_content_type = ingestion_service.download_source_url_to_raw(job)
                job.raw_object_uri = raw_uri
                job.size_bytes = size_bytes
                job.content_hash = content_hash
                if resolved_content_type and not job.source_content_type:
                    job.source_content_type = resolved_content_type
                self.session.commit()

            if not job.raw_object_uri:
                raise ValueError("raw_object_uri must be present before processing.")

            raw_bucket, raw_key = parse_object_uri(job.raw_object_uri)
            self.storage.download_file(raw_bucket, raw_key, local_raw_path)

            if not job.content_hash or not job.size_bytes:
                content_hash, size_bytes = self._compute_local_file_digest_and_size(local_raw_path)
                if not job.content_hash:
                    job.content_hash = content_hash
                if not job.size_bytes:
                    job.size_bytes = size_bytes
                self._merge_job_metadata(
                    job,
                    {
                        "resolved_content_hash": job.content_hash,
                        "resolved_size_bytes": int(job.size_bytes or 0),
                    },
                )
                self.session.commit()

            detected_format = detect_file_format(
                filename=job.filename or local_raw_path.name,
                content_type=job.source_content_type,
                explicit_format=job.source_format,
                local_path=local_raw_path,
            )
            self._merge_job_metadata(job, {"detected_format": detected_format.value})
            self.session.commit()

            source_sql = self.duckdb_service.source_sql_for_file(local_raw_path, detected_format, tmpdir_path)

            raw_schema = self.duckdb_service.describe_query("SELECT * FROM source", views={"source": source_sql})
            DatasetService.save_schema_snapshot(self.session, dataset, "raw", raw_schema)
            job.status = IngestionStatus.RAW_REGISTERED.value
            self._merge_job_metadata(job, {"raw_schema_columns": len(raw_schema)})
            self.session.commit()

            silver_key = build_layer_object_key(dataset.slug, job.id, "part-00000.parquet")
            silver_uri = object_uri(self.settings.s3_silver_bucket, silver_key)

            gold_key = build_layer_object_key(dataset.slug, job.id, "part-00000.parquet")
            gold_uri = object_uri(self.settings.s3_gold_bucket, gold_key)

            silver_query = dataset.silver_sql or "SELECT * FROM source"
            silver_schema = self.duckdb_service.describe_query(silver_query, views={"source": source_sql})

            job.status = IngestionStatus.SILVERIZING.value
            self._merge_job_metadata(job, {"silver_schema_columns": len(silver_schema)})
            self.session.commit()

            silver_local_path = tmpdir_path / "silver.parquet"
            self.duckdb_service.copy_query_to_parquet(silver_query, silver_local_path, views={"source": source_sql})
            self.storage.upload_file(
                self.settings.s3_silver_bucket,
                silver_key,
                silver_local_path,
                content_type="application/octet-stream",
            )

            DatasetService.save_schema_snapshot(self.session, dataset, "silver", silver_schema)
            job.silver_object_uri = silver_uri
            self.session.commit()

            silver_source_query = f"SELECT * FROM parquet_scan('{silver_local_path.as_posix()}')"
            QualityService(self.session).run_checks(dataset.id, job.id, "silver", silver_source_query)

            gold_base_query = dataset.gold_sql or "SELECT * FROM source"
            current_gold_schema = self.duckdb_service.describe_query(
                gold_base_query,
                views={"source": silver_source_query},
            )

            latest_gold_snapshot = DatasetService.latest_schema_snapshot(self.session, dataset.id, "gold")
            canonical_gold_schema = merge_schemas(
                latest_gold_snapshot.schema_json if latest_gold_snapshot else None,
                current_gold_schema,
            )

            if (
                dataset.schema_mode == SchemaMode.STRICT.value
                and latest_gold_snapshot
                and canonical_gold_schema != latest_gold_snapshot.schema_json
            ):
                raise ValueError(
                    "Schema drift detected while dataset is in strict mode. "
                    "Switch schema_mode to evolve or update the transform."
                )

            metadata_columns = [
                {"name": "_ingestion_id", "type": "VARCHAR", "value": job.id},
                {"name": "_dataset_slug", "type": "VARCHAR", "value": dataset.slug},
                {"name": "_source_filename", "type": "VARCHAR", "value": job.filename},
                {"name": "_ingested_at", "type": "TIMESTAMP", "value": datetime.now(timezone.utc).isoformat()},
                {"name": "_raw_object_uri", "type": "VARCHAR", "value": job.raw_object_uri},
                {"name": "_silver_object_uri", "type": "VARCHAR", "value": silver_uri},
                {"name": "_gold_object_uri", "type": "VARCHAR", "value": gold_uri},
            ]

            final_gold_query = self.duckdb_service.build_aligned_query(
                base_query=gold_base_query,
                current_schema=current_gold_schema,
                canonical_schema=canonical_gold_schema,
                metadata_columns=metadata_columns,
            )

            job.status = IngestionStatus.GOLDIZING.value
            self._merge_job_metadata(job, {"gold_schema_columns": len(canonical_gold_schema)})
            self.session.commit()

            gold_local_path = tmpdir_path / "gold.parquet"
            self.duckdb_service.copy_query_to_parquet(
                final_gold_query,
                gold_local_path,
                views={"source": silver_source_query},
            )

            self.storage.upload_file(
                self.settings.s3_gold_bucket,
                gold_key,
                gold_local_path,
                content_type="application/octet-stream",
            )
            job.gold_object_uri = gold_uri
            job.row_count = self.duckdb_service.count_parquet_rows(gold_local_path)
            self._merge_job_metadata(job, {"gold_row_count": int(job.row_count or 0)})
            self.session.commit()

            DatasetService.save_schema_snapshot(self.session, dataset, "gold", canonical_gold_schema)
            gold_source_query = f"SELECT * FROM parquet_scan('{gold_local_path.as_posix()}')"
            QualityService(self.session).run_checks(dataset.id, job.id, "gold", gold_source_query)

            job.status = IngestionStatus.LOADING.value
            self.session.commit()

            self.clickhouse.ensure_gold_table(dataset.gold_table_name, canonical_gold_schema, dataset.serving_config)
            self.clickhouse.delete_rows_for_ingestion(dataset.gold_table_name, job.id)
            self.clickhouse.insert_parquet_from_object_uri(dataset.gold_table_name, canonical_gold_schema, gold_uri)

            elapsed = perf_counter() - started
            job.status = IngestionStatus.SUCCEEDED.value
            job.finished_at = datetime.now(timezone.utc)
            self._merge_job_metadata(
                job,
                {
                    "finished_at": job.finished_at.isoformat(),
                    "processing_seconds": round(elapsed, 3),
                    "bytes_processed": int(job.size_bytes or 0),
                    "rows_processed": int(job.row_count or 0),
                    "rows_per_second": round((job.row_count or 0) / elapsed, 3) if elapsed > 0 else None,
                },
            )
            self.session.commit()

        logger.info("Finished ingestion job=%s dataset=%s", job.id, dataset.slug)
        self._notify_ingestion_job("ingestion_job.succeeded", job, dataset, task_id=task_id)
        self._audit_ingestion_job("ingestion_job.succeeded", job, dataset, task_id=task_id)
        return {"job_id": job.id, "status": job.status, "row_count": job.row_count}
