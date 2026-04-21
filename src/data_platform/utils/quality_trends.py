from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Mapping


_SUPPORTED_QUALITY_TREND_BUCKETS = {"hour", "day"}


def _ensure_aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def normalize_quality_trend_bucket(bucket: str | None) -> str:
    normalized = (bucket or "day").strip().lower()
    if normalized not in _SUPPORTED_QUALITY_TREND_BUCKETS:
        raise ValueError("bucket must be one of: day, hour")
    return normalized


def floor_quality_trend_bucket(value: datetime, *, bucket: str) -> datetime:
    normalized = normalize_quality_trend_bucket(bucket)
    aware = _ensure_aware(value)
    if normalized == "hour":
        return aware.replace(minute=0, second=0, microsecond=0)
    return aware.replace(hour=0, minute=0, second=0, microsecond=0)


def _bucket_delta(bucket: str) -> timedelta:
    normalized = normalize_quality_trend_bucket(bucket)
    if normalized == "hour":
        return timedelta(hours=1)
    return timedelta(days=1)


def _coerce_status_counts(value: dict[str, int]) -> dict[str, int]:
    return {key: int(count) for key, count in sorted(value.items()) if int(count) > 0}


def _pass_rate(*, passed_results: int, total_results: int) -> float | None:
    if total_results <= 0:
        return None
    return round(passed_results / total_results, 6)


def build_quality_result_trend_report(
    rows: Iterable[Mapping[str, Any]],
    *,
    bucket: str = "day",
    limit: int = 30,
) -> dict[str, list[dict[str, Any]]]:
    normalized_bucket = normalize_quality_trend_bucket(bucket)
    bounded_limit = max(1, min(int(limit), 1000))
    bucket_delta = _bucket_delta(normalized_bucket)

    series_map: dict[datetime, dict[str, Any]] = {}
    check_map: dict[str, dict[str, Any]] = {}

    for row in rows:
        created_at = row.get("created_at")
        if not isinstance(created_at, datetime):
            continue
        bucket_start = floor_quality_trend_bucket(created_at, bucket=normalized_bucket)
        bucket_end = bucket_start + bucket_delta
        status = str(row.get("status") or "unknown").strip().lower() or "unknown"
        quality_check_id = str(row.get("quality_check_id") or "").strip()
        quality_check_name = str(row.get("quality_check_name") or "").strip()
        severity = str(row.get("severity") or "").strip().lower()

        bucket_entry = series_map.setdefault(
            bucket_start,
            {
                "bucket_start": bucket_start,
                "bucket_end": bucket_end,
                "total_results": 0,
                "passed_results": 0,
                "failed_results": 0,
                "status_counts": defaultdict(int),
                "latest_created_at": None,
            },
        )
        bucket_entry["total_results"] += 1
        bucket_entry["status_counts"][status] += 1
        if status == "passed":
            bucket_entry["passed_results"] += 1
        elif status == "failed":
            bucket_entry["failed_results"] += 1
        latest_created_at = bucket_entry["latest_created_at"]
        aware_created_at = _ensure_aware(created_at)
        if latest_created_at is None or aware_created_at > latest_created_at:
            bucket_entry["latest_created_at"] = aware_created_at

        if quality_check_id:
            check_entry = check_map.setdefault(
                quality_check_id,
                {
                    "quality_check_id": quality_check_id,
                    "quality_check_name": quality_check_name,
                    "severity": severity,
                    "total_results": 0,
                    "passed_results": 0,
                    "failed_results": 0,
                    "status_counts": defaultdict(int),
                    "latest_status": None,
                    "latest_created_at": None,
                },
            )
            check_entry["total_results"] += 1
            check_entry["status_counts"][status] += 1
            if status == "passed":
                check_entry["passed_results"] += 1
            elif status == "failed":
                check_entry["failed_results"] += 1
            check_latest = check_entry["latest_created_at"]
            if check_latest is None or aware_created_at > check_latest:
                check_entry["latest_created_at"] = aware_created_at
                check_entry["latest_status"] = status
            if quality_check_name and not check_entry["quality_check_name"]:
                check_entry["quality_check_name"] = quality_check_name
            if severity and not check_entry["severity"]:
                check_entry["severity"] = severity

    ordered_series = sorted(series_map.values(), key=lambda item: item["bucket_start"], reverse=True)[:bounded_limit]
    ordered_checks = sorted(
        check_map.values(),
        key=lambda item: (
            -(item["latest_created_at"].timestamp() if item["latest_created_at"] is not None else 0.0),
            item["quality_check_name"],
            item["quality_check_id"],
        ),
    )

    series = []
    for item in ordered_series:
        total_results = int(item["total_results"])
        passed_results = int(item["passed_results"])
        failed_results = int(item["failed_results"])
        series.append(
            {
                "bucket_start": item["bucket_start"],
                "bucket_end": item["bucket_end"],
                "total_results": total_results,
                "passed_results": passed_results,
                "failed_results": failed_results,
                "status_counts": _coerce_status_counts(item["status_counts"]),
                "latest_created_at": item["latest_created_at"],
                "pass_rate": _pass_rate(passed_results=passed_results, total_results=total_results),
            }
        )

    quality_checks = []
    for item in ordered_checks:
        total_results = int(item["total_results"])
        passed_results = int(item["passed_results"])
        failed_results = int(item["failed_results"])
        quality_checks.append(
            {
                "quality_check_id": item["quality_check_id"],
                "quality_check_name": item["quality_check_name"],
                "severity": item["severity"],
                "total_results": total_results,
                "passed_results": passed_results,
                "failed_results": failed_results,
                "status_counts": _coerce_status_counts(item["status_counts"]),
                "latest_status": item["latest_status"],
                "latest_created_at": item["latest_created_at"],
                "pass_rate": _pass_rate(passed_results=passed_results, total_results=total_results),
            }
        )

    return {"series": series, "quality_checks": quality_checks}
