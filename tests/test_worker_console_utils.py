from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from data_platform.utils.worker_console import (
    build_worker_console_entry,
    build_worker_console_heartbeat_sse,
    build_worker_console_message,
    build_worker_console_sse_frame,
    floor_console_bucket_start,
)


def test_build_worker_console_entry_uses_event_type_and_retry_details() -> None:
    event = SimpleNamespace(
        id="audit-1",
        created_at=datetime(2026, 4, 16, 12, 0, tzinfo=timezone.utc),
        method="WORKER",
        path="/worker/ingestion-jobs/job-1",
        status_code=500,
        details_json={
            "event_type": "ingestion_job.retrying",
            "resource_type": "ingestion_job",
            "resource_id": "job-1",
            "retry": {"current_retry": 2, "max_retries": 3},
            "error_message": "temporary network failure",
        },
    )

    payload = build_worker_console_entry(event)

    assert payload["event_type"] == "ingestion_job.retrying"
    assert payload["resource_type"] == "ingestion_job"
    assert payload["resource_id"] == "job-1"
    assert "attempt 2 of 3" in payload["message"]
    assert "temporary network failure" in payload["message"]



def test_console_message_defaults_for_success_and_failure() -> None:
    assert build_worker_console_message({"event_type": "pipeline_run.succeeded", "row_count": 12}, status_code=200) == (
        "Worker run succeeded with 12 rows."
    )
    assert build_worker_console_message({"event_type": "pipeline_run.failed", "error_message": "boom"}, status_code=500) == (
        "Worker run failed: boom"
    )



def test_console_sse_helpers_emit_expected_frames() -> None:
    frame = build_worker_console_sse_frame({"id": "evt-1", "message": "ok"})
    heartbeat = build_worker_console_heartbeat_sse(cursor="cursor-1")

    assert frame.startswith("event: message\n")
    assert '"id": "evt-1"' in frame
    assert heartbeat.startswith("event: heartbeat\n")
    assert '"cursor": "cursor-1"' in heartbeat



def test_floor_console_bucket_start_normalizes_hour_and_day() -> None:
    value = datetime(2026, 4, 16, 12, 34, 56, tzinfo=timezone.utc)

    assert floor_console_bucket_start(value, bucket="hour") == datetime(2026, 4, 16, 12, 0, tzinfo=timezone.utc)
    assert floor_console_bucket_start(value, bucket="day") == datetime(2026, 4, 16, 0, 0, tzinfo=timezone.utc)
