from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import AnyHttpUrl, BaseModel, ConfigDict, Field

from data_platform.schemas.dataset import SchemaSnapshotResponse


class PresignUploadRequest(BaseModel):
    dataset_slug: str
    filename: str
    content_type: str | None = None
    source_format: str | None = None
    size_bytes: int | None = None
    idempotency_key: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class PresignUploadResponse(BaseModel):
    upload_id: str
    bucket: str
    object_key: str
    upload_url: str
    expires_in: int


class CompleteUploadRequest(BaseModel):
    dataset_slug: str
    object_key: str
    filename: str
    content_type: str | None = None
    source_format: str | None = None
    size_bytes: int | None = None
    idempotency_key: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class ObjectUriIngestionRequest(BaseModel):
    dataset_slug: str
    object_uri: str
    filename: str | None = None
    content_type: str | None = None
    source_format: str | None = None
    size_bytes: int | None = None
    idempotency_key: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class UrlIngestionRequest(BaseModel):
    dataset_slug: str
    url: AnyHttpUrl
    filename: str | None = None
    source_format: str | None = None
    idempotency_key: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class InlineJsonIngestionRequest(BaseModel):
    dataset_slug: str
    records: list[dict[str, Any]] | dict[str, Any]
    filename: str = "payload.jsonl"
    idempotency_key: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class ReprocessIngestionRequest(BaseModel):
    idempotency_key: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class IngestionJobResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    dataset_id: str
    source_type: str
    status: str
    filename: str | None = None
    source_format: str | None = None
    source_content_type: str | None = None
    source_url: str | None = None
    raw_object_uri: str | None = None
    silver_object_uri: str | None = None
    gold_object_uri: str | None = None
    content_hash: str | None = None
    idempotency_key: str | None = None
    size_bytes: int | None = None
    row_count: int | None = None
    error_message: str | None = None
    job_metadata: dict[str, Any]
    started_at: datetime | None = None
    finished_at: datetime | None = None
    created_at: datetime
    updated_at: datetime


class GoldPreviewResponse(BaseModel):
    dataset_slug: str
    data_product_slug: str | None = None
    columns: list[str]
    rows: list[dict[str, Any]]
    total_rows: int
    total_rows_is_estimate: bool = False


class GoldSchemaResponse(BaseModel):
    dataset_slug: str
    data_product_slug: str | None = None
    columns: list[dict[str, str]]


class IngestionArtifactManifestItemResponse(BaseModel):
    layer: str
    object_uri: str
    schema_snapshot: SchemaSnapshotResponse | None = None


class IngestionArtifactManifestResponse(BaseModel):
    ingestion_job_id: str
    dataset_id: str
    dataset_slug: str | None = None
    status: str
    source_type: str
    filename: str | None = None
    source_format: str | None = None
    source_content_type: str | None = None
    content_hash: str | None = None
    size_bytes: int | None = None
    row_count: int | None = None
    created_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None
    effective_at: datetime
    artifacts: list[IngestionArtifactManifestItemResponse] = Field(default_factory=list)
