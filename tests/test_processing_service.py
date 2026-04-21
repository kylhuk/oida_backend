from __future__ import annotations

import shutil
from pathlib import Path

from data_platform.models.ingestion import IngestionJob
from data_platform.services.dataset_service import DatasetService
from data_platform.services.processing_service import MedallionProcessingService
from data_platform.schemas.dataset import CreateDatasetRequest
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


class ClickHouseStub:
    def __init__(self):
        self.ensured: list[tuple[str, list[dict[str, str]]]] = []
        self.deleted: list[tuple[str, str]] = []
        self.inserted: list[tuple[str, str]] = []

    def ensure_gold_table(self, table_name: str, canonical_schema: list[dict[str, str]], serving_config: dict | None = None) -> None:
        self.ensured.append((table_name, canonical_schema))

    def delete_rows_for_ingestion(self, table_name: str, ingestion_id: str) -> None:
        self.deleted.append((table_name, ingestion_id))

    def insert_parquet_from_object_uri(self, table_name: str, canonical_schema: list[dict[str, str]], object_uri_value: str) -> None:
        self.inserted.append((table_name, object_uri_value))



def test_process_job_end_to_end_for_existing_object_uri(db_session, tmp_path: Path):
    dataset = DatasetService.create_dataset(db_session, CreateDatasetRequest(slug="orders", name="Orders"))

    raw_csv = tmp_path / "orders.csv"
    raw_csv.write_text("id,name\n1,Alice\n2,Bob\n", encoding="utf-8")
    raw_bucket = "raw"
    raw_key = "bootstrap/orders.csv"

    job = IngestionJob(
        dataset_id=dataset.id,
        source_type="object_uri",
        status="received",
        filename="orders.csv",
        source_content_type="text/csv; charset=utf-8",
        raw_object_uri=object_uri(raw_bucket, raw_key),
        job_metadata={},
    )
    db_session.add(job)
    db_session.commit()
    db_session.refresh(job)

    storage = LocalStorageStub({(raw_bucket, raw_key): raw_csv}, tmp_path / "object-store")
    clickhouse = ClickHouseStub()

    result = MedallionProcessingService(
        db_session,
        storage=storage,
        clickhouse=clickhouse,
    ).process_job(job.id, task_id="task-1")

    db_session.refresh(job)

    assert result["status"] == "succeeded"
    assert job.status == "succeeded"
    assert job.row_count == 2
    assert job.content_hash is not None
    assert job.size_bytes == raw_csv.stat().st_size
    assert job.silver_object_uri is not None
    assert job.gold_object_uri is not None
    assert job.job_metadata["processing"]["detected_format"] == "csv"
    assert job.job_metadata["processing"]["rows_processed"] == 2
    assert len(storage.uploads) == 2
    assert clickhouse.ensured[0][0] == dataset.gold_table_name
    assert clickhouse.deleted == [(dataset.gold_table_name, job.id)]
    assert clickhouse.inserted == [(dataset.gold_table_name, job.gold_object_uri)]

    gold_bucket, gold_key = parse_object_uri(job.gold_object_uri)
    assert (tmp_path / "object-store" / gold_bucket / gold_key).exists()
