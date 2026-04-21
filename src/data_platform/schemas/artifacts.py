from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class ArtifactManifestEntryResponse(BaseModel):
    name: str
    role: str
    object_uri: str
    bucket: str | None = None
    object_key: str | None = None
    layer: str | None = None
    format: str | None = None
    content_type: str | None = None
    row_count: int | None = None
    schema_version: int | None = None
    schema_fingerprint: str | None = None


class ArtifactManifestResponse(BaseModel):
    resource_type: str
    resource_id: str | None = None
    dataset_id: str | None = None
    ingestion_job_id: str | None = None
    pipeline_id: str | None = None
    run_id: str | None = None
    status: str | None = None
    items: list[ArtifactManifestEntryResponse] = Field(default_factory=list)
