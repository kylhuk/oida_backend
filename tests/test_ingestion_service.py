from __future__ import annotations

from data_platform.models.ingestion import IngestionJob
from data_platform.schemas.dataset import CreateDatasetRequest
from data_platform.schemas.ingestion import CompleteUploadRequest, ObjectUriIngestionRequest, ReprocessIngestionRequest
from data_platform.services.dataset_service import DatasetService
from data_platform.services.ingestion_service import IngestionService


class DummyStorage:
    def head_object(self, bucket: str, key: str) -> dict:
        return {"ContentLength": 42, "ContentType": "text/csv; charset=utf-8"}


class RecordingIngestionService(IngestionService):
    def __init__(self, session):
        super().__init__(session)
        self.storage = DummyStorage()
        self.queued: list[str] = []

    def queue_job(self, job_id: str) -> None:
        self.queued.append(job_id)



def test_complete_presigned_upload_uses_head_object_metadata(db_session):
    dataset = DatasetService.create_dataset(db_session, CreateDatasetRequest(slug="orders", name="Orders"))
    service = RecordingIngestionService(db_session)

    job = service.complete_presigned_upload(
        dataset,
        CompleteUploadRequest(
            dataset_slug="orders",
            object_key="orders/year=2026/month=04/day=12/job/file.csv",
            filename="file.csv",
        ),
    )

    assert job.size_bytes == 42
    assert job.source_content_type == "text/csv"
    assert service.queued == [job.id]



def test_create_object_uri_ingestion_reuses_existing_object_metadata(db_session):
    dataset = DatasetService.create_dataset(db_session, CreateDatasetRequest(slug="orders", name="Orders"))
    service = RecordingIngestionService(db_session)

    job = service.create_object_uri_ingestion(
        dataset,
        ObjectUriIngestionRequest(
            dataset_slug="orders",
            object_uri="s3://raw/bootstrap/orders.csv",
            idempotency_key="obj-1",
        ),
    )

    assert job.source_type == "object_uri"
    assert job.raw_object_uri == "s3://raw/bootstrap/orders.csv"
    assert job.filename == "orders.csv"
    assert job.source_content_type == "text/csv"
    assert job.size_bytes == 42
    assert service.queued == [job.id]



def test_reprocess_job_reuses_raw_object_and_marks_parent(db_session):
    dataset = DatasetService.create_dataset(db_session, CreateDatasetRequest(slug="orders", name="Orders"))
    source_job = IngestionJob(
        dataset_id=dataset.id,
        source_type="upload",
        status="succeeded",
        filename="orders.csv",
        raw_object_uri="s3://raw/orders/job/orders.csv",
        job_metadata={"origin": "test"},
    )
    db_session.add(source_job)
    db_session.commit()
    db_session.refresh(source_job)

    service = RecordingIngestionService(db_session)
    reprocessed = service.reprocess_job(
        source_job,
        ReprocessIngestionRequest(metadata={"requested_by": "pytest"}),
    )

    assert reprocessed.source_type == "reprocess"
    assert reprocessed.raw_object_uri == source_job.raw_object_uri
    assert reprocessed.job_metadata["reprocess_of"] == source_job.id
    assert reprocessed.job_metadata["requested_by"] == "pytest"
    assert service.queued == [reprocessed.id]
