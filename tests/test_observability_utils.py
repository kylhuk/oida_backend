from __future__ import annotations

from datetime import datetime, timezone

import pytest

from data_platform.utils.observability import (
    build_observability_activity_report,
    floor_observability_bucket,
    normalize_observability_bucket,
)


def test_build_observability_activity_report_groups_counts_by_bucket() -> None:
    rows = [
        {"created_at": datetime(2026, 4, 16, 9, 15, tzinfo=timezone.utc), "category": "http_event"},
        {"created_at": datetime(2026, 4, 16, 9, 30, tzinfo=timezone.utc), "category": "worker_event", "count": 2},
        {"created_at": datetime(2026, 4, 16, 9, 45, tzinfo=timezone.utc), "category": "pipeline_failure"},
        {"created_at": datetime(2026, 4, 16, 10, 5, tzinfo=timezone.utc), "category": "maintenance_event"},
        {"created_at": datetime(2026, 4, 16, 10, 20, tzinfo=timezone.utc), "category": "quality_failure"},
    ]

    report = build_observability_activity_report(rows, bucket="hour", limit=10)

    assert report["series"] == [
        {
            "bucket_start": datetime(2026, 4, 16, 10, 0, tzinfo=timezone.utc),
            "bucket_end": datetime(2026, 4, 16, 11, 0, tzinfo=timezone.utc),
            "http_events": 0,
            "worker_events": 0,
            "maintenance_events": 1,
            "ingestion_failures": 0,
            "pipeline_failures": 0,
            "quality_failures": 1,
            "total_events": 1,
            "total_failures": 1,
            "latest_created_at": datetime(2026, 4, 16, 10, 20, tzinfo=timezone.utc),
        },
        {
            "bucket_start": datetime(2026, 4, 16, 9, 0, tzinfo=timezone.utc),
            "bucket_end": datetime(2026, 4, 16, 10, 0, tzinfo=timezone.utc),
            "http_events": 1,
            "worker_events": 2,
            "maintenance_events": 0,
            "ingestion_failures": 0,
            "pipeline_failures": 1,
            "quality_failures": 0,
            "total_events": 3,
            "total_failures": 1,
            "latest_created_at": datetime(2026, 4, 16, 9, 45, tzinfo=timezone.utc),
        },
    ]


@pytest.mark.parametrize(
    ("bucket", "value", "expected"),
    [
        (
            "hour",
            datetime(2026, 4, 16, 8, 37, 59, tzinfo=timezone.utc),
            datetime(2026, 4, 16, 8, 0, tzinfo=timezone.utc),
        ),
        (
            "day",
            datetime(2026, 4, 16, 8, 37, 59, tzinfo=timezone.utc),
            datetime(2026, 4, 16, 0, 0, tzinfo=timezone.utc),
        ),
    ],
)
def test_floor_observability_bucket_normalizes_supported_bucket_sizes(bucket: str, value: datetime, expected: datetime) -> None:
    assert floor_observability_bucket(value, bucket=bucket) == expected
    assert normalize_observability_bucket(bucket.upper()) == bucket


def test_observability_bucket_validation_rejects_unknown_values() -> None:
    with pytest.raises(ValueError, match="bucket must be one of: day, hour"):
        build_observability_activity_report([], bucket="week")
