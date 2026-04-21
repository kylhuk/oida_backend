from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.parse import urlparse

import requests
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from data_platform.enums import IngestionStatus, SourceType
from data_platform.models.dataset import Dataset
from data_platform.models.ingestion import IngestionJob
from data_platform.schemas.ingestion import (
    CompleteUploadRequest,
    InlineJsonIngestionRequest,
    ObjectUriIngestionRequest,
    ReprocessIngestionRequest,
    UrlIngestionRequest,
)
from data_platform.services.storage import ObjectStorageService
from data_platform.services.dataset_service import DatasetService
from data_platform.settings import get_settings
from data_platform.utils.artifacts import (
    build_ingestion_artifact_manifest,
    build_ingestion_artifact_manifest_item,
    build_ingestion_artifact_manifest_payload,
)
from data_platform.utils.formats import normalize_content_type
from data_platform.utils.paths import build_layer_object_key, object_uri, parse_object_uri, sanitize_filename
from data_platform.workers.celery_app import get_celery_app


class IngestionService:
    def __init__(self, session: Session) -> None:
        self.session = session
        self.settings = get_settings()
        self.storage = ObjectStorageService()

    def _existing_for_idempotency(self, dataset_id: str, idempotency_key: str | None) -> IngestionJob | None:
        if not idempotency_key:
            return None
        return self.session.scalar(
            select(IngestionJob)
            .where(
                IngestionJob.dataset_id == dataset_id,
                IngestionJob.idempotency_key == idempotency_key,
            )
            .order_by(IngestionJob.created_at.desc())
        )

    def _commit_job(self, job: IngestionJob, dataset_id: str, idempotency_key: str | None) -> IngestionJob:
        self.session.add(job)
        try:
            self.session.commit()
        except IntegrityError:
            self.session.rollback()
            existing = self._existing_for_idempotency(dataset_id, idempotency_key)
            if existing is None:
                raise
            return existing
        self.session.refresh(job)
        return job

    def _safe_head_object(self, bucket: str, key: str) -> dict:
        try:
            return self.storage.head_object(bucket, key)
        except Exception:
            return {}

    def queue_job(self, job_id: str) -> None:
        get_celery_app().send_task("data_platform.process_ingestion_job", args=[job_id])


    def build_artifacts(self, job: IngestionJob) -> dict:
        return build_ingestion_artifact_manifest(job)

    def list_jobs(
        self,
        dataset_id: str | None = None,
        status: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[IngestionJob]:
        limit = max(1, min(limit, 1000))
        offset = max(0, offset)

        stmt = select(IngestionJob)
        if dataset_id:
            stmt = stmt.where(IngestionJob.dataset_id == dataset_id)
        if status:
            stmt = stmt.where(IngestionJob.status == status)

        stmt = stmt.order_by(IngestionJob.created_at.desc(), IngestionJob.id.desc()).limit(limit).offset(offset)
        return list(self.session.scalars(stmt).all())

    def complete_presigned_upload(self, dataset: Dataset, payload: CompleteUploadRequest) -> IngestionJob:
        existing = self._existing_for_idempotency(dataset.id, payload.idempotency_key)
        if existing:
            return existing

        head = self.storage.head_object(self.settings.s3_raw_bucket, payload.object_key)

        job = IngestionJob(
            dataset_id=dataset.id,
            source_type=SourceType.PRESIGNED_UPLOAD.value,
            status=IngestionStatus.RECEIVED.value,
            filename=sanitize_filename(payload.filename),
            source_format=payload.source_format,
            source_content_type=normalize_content_type(payload.content_type or head.get("ContentType")),
            raw_object_uri=object_uri(self.settings.s3_raw_bucket, payload.object_key),
            idempotency_key=payload.idempotency_key,
            size_bytes=payload.size_bytes or head.get("ContentLength"),
            job_metadata=payload.metadata,
        )
        job = self._commit_job(job, dataset.id, payload.idempotency_key)
        self.queue_job(job.id)
        return job

    def create_object_uri_ingestion(self, dataset: Dataset, payload: ObjectUriIngestionRequest) -> IngestionJob:
        existing = self._existing_for_idempotency(dataset.id, payload.idempotency_key)
        if existing:
            return existing

        bucket, key = parse_object_uri(payload.object_uri)
        head = self._safe_head_object(bucket, key)
        inferred_filename = payload.filename or Path(key).name or "payload.bin"

        job = IngestionJob(
            dataset_id=dataset.id,
            source_type=SourceType.OBJECT_URI.value,
            status=IngestionStatus.RECEIVED.value,
            filename=sanitize_filename(inferred_filename),
            source_format=payload.source_format,
            source_content_type=normalize_content_type(payload.content_type or head.get("ContentType")),
            raw_object_uri=payload.object_uri,
            idempotency_key=payload.idempotency_key,
            size_bytes=payload.size_bytes or head.get("ContentLength"),
            job_metadata=payload.metadata,
        )
        job = self._commit_job(job, dataset.id, payload.idempotency_key)
        self.queue_job(job.id)
        return job

    def create_inline_json_ingestion(self, dataset: Dataset, payload: InlineJsonIngestionRequest) -> IngestionJob:
        existing = self._existing_for_idempotency(dataset.id, payload.idempotency_key)
        if existing:
            return existing

        ingestion_id = str(uuid.uuid4())
        records = payload.records if isinstance(payload.records, list) else [payload.records]
        raw_lines = b"".join(json.dumps(record).encode("utf-8") + b"\n" for record in records)
        raw_key = build_layer_object_key(dataset.slug, ingestion_id, sanitize_filename(payload.filename))
        raw_uri = self.storage.upload_bytes(
            self.settings.s3_raw_bucket,
            raw_key,
            raw_lines,
            content_type="application/x-ndjson",
        )

        job = IngestionJob(
            id=ingestion_id,
            dataset_id=dataset.id,
            source_type=SourceType.INLINE_JSON.value,
            status=IngestionStatus.RECEIVED.value,
            filename=sanitize_filename(payload.filename),
            source_format="ndjson",
            source_content_type="application/x-ndjson",
            raw_object_uri=raw_uri,
            idempotency_key=payload.idempotency_key,
            size_bytes=len(raw_lines),
            content_hash=hashlib.sha256(raw_lines).hexdigest(),
            job_metadata=payload.metadata,
        )
        job = self._commit_job(job, dataset.id, payload.idempotency_key)
        self.queue_job(job.id)
        return job


    @staticmethod
    def build_detailed_artifact_manifest(session: Session, job: IngestionJob) -> dict:
        dataset = job.dataset
        effective_at = job.finished_at or job.created_at
        artifacts = []
        for layer, object_uri in (("raw", job.raw_object_uri), ("silver", job.silver_object_uri), ("gold", job.gold_object_uri)):
            if not object_uri:
                continue
            snapshot = DatasetService.latest_schema_snapshot_at_or_before(
                session,
                job.dataset_id,
                layer,
                effective_at,
            )
            artifacts.append(
                build_ingestion_artifact_manifest_item(
                    layer=layer,
                    object_uri=object_uri,
                    schema_snapshot=snapshot,
                )
            )

        return build_ingestion_artifact_manifest_payload(
            ingestion_job_id=job.id,
            dataset_id=job.dataset_id,
            dataset_slug=getattr(dataset, "slug", None),
            status=job.status,
            source_type=job.source_type,
            filename=job.filename,
            source_format=job.source_format,
            source_content_type=job.source_content_type,
            content_hash=job.content_hash,
            size_bytes=job.size_bytes,
            row_count=job.row_count,
            created_at=job.created_at,
            started_at=job.started_at,
            finished_at=job.finished_at,
            effective_at=effective_at,
            artifacts=artifacts,
        )

    def create_url_ingestion(self, dataset: Dataset, payload: UrlIngestionRequest) -> IngestionJob:
        existing = self._existing_for_idempotency(dataset.id, payload.idempotency_key)
        if existing:
            return existing

        inferred_filename = payload.filename or Path(urlparse(str(payload.url)).path).name or "download.bin"
        job = IngestionJob(
            dataset_id=dataset.id,
            source_type=SourceType.URL.value,
            status=IngestionStatus.RECEIVED.value,
            filename=sanitize_filename(inferred_filename),
            source_format=payload.source_format,
            source_url=str(payload.url),
            idempotency_key=payload.idempotency_key,
            job_metadata=payload.metadata,
        )
        job = self._commit_job(job, dataset.id, payload.idempotency_key)
        self.queue_job(job.id)
        return job

    async def create_direct_upload_ingestion(
        self,
        dataset: Dataset,
        filename: str,
        file_bytes_stream,
        content_type: str | None = None,
        source_format: str | None = None,
        idempotency_key: str | None = None,
        metadata: dict | None = None,
    ) -> IngestionJob:
        existing = self._existing_for_idempotency(dataset.id, idempotency_key)
        if existing:
            return existing

        ingestion_id = str(uuid.uuid4())
        safe_filename = sanitize_filename(filename)
        raw_key = build_layer_object_key(dataset.slug, ingestion_id, safe_filename)
        digest = hashlib.sha256()
        total_size = 0

        with TemporaryDirectory() as tmpdir:
            local_path = Path(tmpdir) / safe_filename
            with local_path.open("wb") as target:
                while True:
                    chunk = await file_bytes_stream.read(1024 * 1024)
                    if not chunk:
                        break
                    target.write(chunk)
                    digest.update(chunk)
                    total_size += len(chunk)

            raw_uri = self.storage.upload_file(
                self.settings.s3_raw_bucket,
                raw_key,
                local_path,
                content_type=normalize_content_type(content_type),
            )

        job = IngestionJob(
            id=ingestion_id,
            dataset_id=dataset.id,
            source_type=SourceType.UPLOAD.value,
            status=IngestionStatus.RECEIVED.value,
            filename=safe_filename,
            source_format=source_format,
            source_content_type=normalize_content_type(content_type),
            raw_object_uri=raw_uri,
            idempotency_key=idempotency_key,
            size_bytes=total_size,
            content_hash=digest.hexdigest(),
            job_metadata=metadata or {},
        )
        job = self._commit_job(job, dataset.id, idempotency_key)
        self.queue_job(job.id)
        return job

    def reprocess_job(self, source_job: IngestionJob, payload: ReprocessIngestionRequest | None = None) -> IngestionJob:
        payload = payload or ReprocessIngestionRequest()
        existing = self._existing_for_idempotency(source_job.dataset_id, payload.idempotency_key)
        if existing:
            return existing
        if not source_job.raw_object_uri:
            raise ValueError("Cannot reprocess a job without a raw_object_uri.")

        reprocess_metadata = dict(source_job.job_metadata or {})
        reprocess_metadata.update(payload.metadata)
        reprocess_metadata["reprocess_of"] = source_job.id
        reprocess_metadata["reprocessed_at"] = datetime.now(timezone.utc).isoformat()

        job = IngestionJob(
            dataset_id=source_job.dataset_id,
            source_type=SourceType.REPROCESS.value,
            status=IngestionStatus.RECEIVED.value,
            filename=source_job.filename,
            source_format=source_job.source_format,
            source_content_type=source_job.source_content_type,
            source_url=source_job.source_url,
            raw_object_uri=source_job.raw_object_uri,
            idempotency_key=payload.idempotency_key,
            size_bytes=source_job.size_bytes,
            content_hash=source_job.content_hash,
            job_metadata=reprocess_metadata,
        )
        job = self._commit_job(job, source_job.dataset_id, payload.idempotency_key)
        self.queue_job(job.id)
        return job

    def build_presigned_upload(self, dataset: Dataset, filename: str, content_type: str | None) -> dict:
        upload_id = str(uuid.uuid4())
        key = build_layer_object_key(dataset.slug, upload_id, sanitize_filename(filename))
        upload_url = self.storage.generate_presigned_put_url(
            bucket=self.settings.s3_raw_bucket,
            key=key,
            expires_in=self.settings.presign_expiration_seconds,
            content_type=normalize_content_type(content_type),
        )
        return {
            "upload_id": upload_id,
            "bucket": self.settings.s3_raw_bucket,
            "object_key": key,
            "upload_url": upload_url,
            "expires_in": self.settings.presign_expiration_seconds,
        }

    def download_source_url_to_raw(self, job: IngestionJob) -> tuple[str, int, str, str | None]:
        if not job.source_url:
            raise ValueError("source_url is required for URL jobs.")

        response = requests.get(job.source_url, timeout=60, stream=True)
        response.raise_for_status()

        digest = hashlib.sha256()
        total_size = 0

        safe_filename = sanitize_filename(job.filename or "download.bin")
        raw_key = build_layer_object_key(job.dataset.slug, job.id, safe_filename)

        with TemporaryDirectory() as tmpdir:
            local_path = Path(tmpdir) / safe_filename
            with local_path.open("wb") as target:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if not chunk:
                        continue
                    target.write(chunk)
                    digest.update(chunk)
                    total_size += len(chunk)

            resolved_content_type = normalize_content_type(job.source_content_type or response.headers.get("Content-Type"))
            raw_uri = self.storage.upload_file(
                self.settings.s3_raw_bucket,
                raw_key,
                local_path,
                content_type=resolved_content_type,
            )

        return raw_uri, total_size, digest.hexdigest(), normalize_content_type(
            job.source_content_type or response.headers.get("Content-Type")
        )
