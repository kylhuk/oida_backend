from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Mapping

_SUPPORTED_OBSERVABILITY_BUCKETS = {"hour", "day"}


def _ensure_aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def normalize_observability_bucket(bucket: str | None) -> str:
    normalized = (bucket or "hour").strip().lower()
    if normalized not in _SUPPORTED_OBSERVABILITY_BUCKETS:
        raise ValueError("bucket must be one of: day, hour")
    return normalized


def floor_observability_bucket(value: datetime, *, bucket: str) -> datetime:
    normalized = normalize_observability_bucket(bucket)
    aware = _ensure_aware(value)
    if normalized == "hour":
        return aware.replace(minute=0, second=0, microsecond=0)
    return aware.replace(hour=0, minute=0, second=0, microsecond=0)


def _bucket_delta(bucket: str) -> timedelta:
    normalized = normalize_observability_bucket(bucket)
    if normalized == "hour":
        return timedelta(hours=1)
    return timedelta(days=1)


def build_observability_activity_report(
    rows: Iterable[Mapping[str, Any]],
    *,
    bucket: str = "hour",
    limit: int = 48,
) -> dict[str, list[dict[str, Any]]]:
    normalized_bucket = normalize_observability_bucket(bucket)
    bounded_limit = max(1, min(int(limit), 1000))
    bucket_delta = _bucket_delta(normalized_bucket)

    series_map: dict[datetime, dict[str, Any]] = {}
    for row in rows:
        created_at = row.get("created_at")
        if not isinstance(created_at, datetime):
            continue
        category = str(row.get("category") or "").strip().lower()
        if not category:
            continue
        count = int(row.get("count") or 1)
        if count <= 0:
            continue

        bucket_start = floor_observability_bucket(created_at, bucket=normalized_bucket)
        bucket_end = bucket_start + bucket_delta
        entry = series_map.setdefault(
            bucket_start,
            {
                "bucket_start": bucket_start,
                "bucket_end": bucket_end,
                "http_events": 0,
                "worker_events": 0,
                "maintenance_events": 0,
                "ingestion_failures": 0,
                "pipeline_failures": 0,
                "quality_failures": 0,
                "latest_created_at": None,
            },
        )
        if category == "http_event":
            entry["http_events"] += count
        elif category == "worker_event":
            entry["worker_events"] += count
        elif category == "maintenance_event":
            entry["maintenance_events"] += count
        elif category == "ingestion_failure":
            entry["ingestion_failures"] += count
        elif category == "pipeline_failure":
            entry["pipeline_failures"] += count
        elif category == "quality_failure":
            entry["quality_failures"] += count
        aware_created_at = _ensure_aware(created_at)
        latest_created_at = entry["latest_created_at"]
        if latest_created_at is None or aware_created_at > latest_created_at:
            entry["latest_created_at"] = aware_created_at

    ordered_series = sorted(series_map.values(), key=lambda item: item["bucket_start"], reverse=True)[:bounded_limit]
    series = []
    for item in ordered_series:
        http_events = int(item["http_events"])
        worker_events = int(item["worker_events"])
        maintenance_events = int(item["maintenance_events"])
        ingestion_failures = int(item["ingestion_failures"])
        pipeline_failures = int(item["pipeline_failures"])
        quality_failures = int(item["quality_failures"])
        series.append(
            {
                "bucket_start": item["bucket_start"],
                "bucket_end": item["bucket_end"],
                "http_events": http_events,
                "worker_events": worker_events,
                "maintenance_events": maintenance_events,
                "ingestion_failures": ingestion_failures,
                "pipeline_failures": pipeline_failures,
                "quality_failures": quality_failures,
                "total_events": http_events + worker_events + maintenance_events,
                "total_failures": ingestion_failures + pipeline_failures + quality_failures,
                "latest_created_at": item["latest_created_at"],
            }
        )
    return {"series": series}
