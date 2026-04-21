from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel


class ObservabilityRecentFailureResponse(BaseModel):
    kind: str
    id: str
    dataset_slug: str | None = None
    pipeline_id: str | None = None
    pipeline_name: str | None = None
    run_ref: str | None = None
    status: str
    error_message: str | None = None
    occurred_at: datetime | None = None


class ObservabilitySummaryResponse(BaseModel):
    generated_at: datetime
    lookback_hours: int
    datasets_total: int
    active_datasets: int
    pipelines_total: int
    active_pipelines: int
    ingestion_status_counts: dict[str, int]
    pipeline_run_status_counts: dict[str, int]
    quality_result_status_counts: dict[str, int]
    audit_event_counts: dict[str, int]
    recent_ingestion_failures: list[ObservabilityRecentFailureResponse]
    recent_pipeline_failures: list[ObservabilityRecentFailureResponse]


class ObservabilityActivityBucketResponse(BaseModel):
    bucket_start: datetime
    bucket_end: datetime
    http_events: int
    worker_events: int
    maintenance_events: int
    ingestion_failures: int
    pipeline_failures: int
    quality_failures: int
    total_events: int
    total_failures: int
    latest_created_at: datetime | None = None


class ObservabilityActivityResponse(BaseModel):
    generated_at: datetime
    lookback_hours: int
    bucket: str
    series: list[ObservabilityActivityBucketResponse]
