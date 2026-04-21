from __future__ import annotations

from datetime import datetime, timezone

import pytest

from data_platform.utils.quality_trends import (
    build_quality_result_trend_report,
    floor_quality_trend_bucket,
    normalize_quality_trend_bucket,
)


def test_build_quality_result_trend_report_groups_results_by_bucket_and_quality_check() -> None:
    rows = [
        {
            "created_at": datetime(2026, 4, 15, 9, 30, tzinfo=timezone.utc),
            "status": "passed",
            "quality_check_id": "check-1",
            "quality_check_name": "row_count_positive",
            "severity": "error",
        },
        {
            "created_at": datetime(2026, 4, 15, 12, 0, tzinfo=timezone.utc),
            "status": "failed",
            "quality_check_id": "check-1",
            "quality_check_name": "row_count_positive",
            "severity": "error",
        },
        {
            "created_at": datetime(2026, 4, 16, 8, 15, tzinfo=timezone.utc),
            "status": "passed",
            "quality_check_id": "check-2",
            "quality_check_name": "null_rate_ok",
            "severity": "warn",
        },
    ]

    report = build_quality_result_trend_report(rows, bucket="day", limit=10)

    assert report["series"] == [
        {
            "bucket_start": datetime(2026, 4, 16, 0, 0, tzinfo=timezone.utc),
            "bucket_end": datetime(2026, 4, 17, 0, 0, tzinfo=timezone.utc),
            "total_results": 1,
            "passed_results": 1,
            "failed_results": 0,
            "status_counts": {"passed": 1},
            "latest_created_at": datetime(2026, 4, 16, 8, 15, tzinfo=timezone.utc),
            "pass_rate": 1.0,
        },
        {
            "bucket_start": datetime(2026, 4, 15, 0, 0, tzinfo=timezone.utc),
            "bucket_end": datetime(2026, 4, 16, 0, 0, tzinfo=timezone.utc),
            "total_results": 2,
            "passed_results": 1,
            "failed_results": 1,
            "status_counts": {"failed": 1, "passed": 1},
            "latest_created_at": datetime(2026, 4, 15, 12, 0, tzinfo=timezone.utc),
            "pass_rate": 0.5,
        },
    ]
    assert report["quality_checks"] == [
        {
            "quality_check_id": "check-2",
            "quality_check_name": "null_rate_ok",
            "severity": "warn",
            "total_results": 1,
            "passed_results": 1,
            "failed_results": 0,
            "status_counts": {"passed": 1},
            "latest_status": "passed",
            "latest_created_at": datetime(2026, 4, 16, 8, 15, tzinfo=timezone.utc),
            "pass_rate": 1.0,
        },
        {
            "quality_check_id": "check-1",
            "quality_check_name": "row_count_positive",
            "severity": "error",
            "total_results": 2,
            "passed_results": 1,
            "failed_results": 1,
            "status_counts": {"failed": 1, "passed": 1},
            "latest_status": "failed",
            "latest_created_at": datetime(2026, 4, 15, 12, 0, tzinfo=timezone.utc),
            "pass_rate": 0.5,
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
def test_floor_quality_trend_bucket_normalizes_supported_bucket_sizes(bucket: str, value: datetime, expected: datetime) -> None:
    assert floor_quality_trend_bucket(value, bucket=bucket) == expected
    assert normalize_quality_trend_bucket(bucket.upper()) == bucket


def test_quality_trend_bucket_validation_rejects_unknown_values() -> None:
    with pytest.raises(ValueError, match="bucket must be one of: day, hour"):
        build_quality_result_trend_report([], bucket="week")
