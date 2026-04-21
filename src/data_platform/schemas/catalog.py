from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class CatalogCountResponse(BaseModel):
    count: int


class CatalogDatasetEntryResponse(BaseModel):
    id: str
    slug: str
    name: str
    description: str | None = None
    status: str
    schema_mode: str
    gold_table_name: str
    tags: list[str] = Field(default_factory=list)
    latest_raw_schema_fingerprint: str | None = None
    latest_silver_schema_fingerprint: str | None = None
    latest_gold_schema_fingerprint: str | None = None
    latest_ingestion_created_at: datetime | None = None
    pipeline_count: int = 0
    data_product_count: int = 0
    created_at: datetime
    updated_at: datetime


CatalogDatasetResponse = CatalogDatasetEntryResponse


class CatalogPipelineEntryResponse(BaseModel):
    id: str
    dataset_id: str
    dataset_slug: str
    dataset_name: str
    name: str
    source_layer: str
    target_layer: str
    engine: str
    active: bool
    run_count: int = 0
    latest_run_id: str | None = None
    latest_run_status: str | None = None
    latest_run_ref: str | None = None
    latest_run_created_at: datetime | None = None
    latest_run_finished_at: datetime | None = None
    latest_run_error_message: str | None = None
    created_at: datetime
    updated_at: datetime


CatalogPipelineResponse = CatalogPipelineEntryResponse


class CatalogSearchResponse(BaseModel):
    query: str | None = None
    dataset_count: int = 0
    pipeline_count: int = 0
    datasets: list[CatalogDatasetEntryResponse] = Field(default_factory=list)
    pipelines: list[CatalogPipelineEntryResponse] = Field(default_factory=list)
