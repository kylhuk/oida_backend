from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Mapping


def _coerce_mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _event_type(details_json: Mapping[str, Any]) -> str:
    value = details_json.get("event_type")
    if isinstance(value, str) and value.strip():
        return value.strip()
    return "worker.event"


def build_worker_console_message(details_json: Mapping[str, Any], *, status_code: int) -> str:
    event_type = _event_type(details_json)
    if event_type.endswith(".started"):
        return "Worker run started."

    if event_type.endswith(".retrying"):
        retry = _coerce_mapping(details_json.get("retry"))
        current_retry = retry.get("current_retry")
        max_retries = retry.get("max_retries")
        error_message = details_json.get("error_message") or details_json.get("error")
        attempt_suffix = ""
        if current_retry is not None:
            if max_retries is None:
                attempt_suffix = f" (attempt {current_retry})"
            else:
                attempt_suffix = f" (attempt {current_retry} of {max_retries})"
        if isinstance(error_message, str) and error_message.strip():
            return f"Worker retry scheduled{attempt_suffix}: {error_message.strip()}"
        return f"Worker retry scheduled{attempt_suffix}."

    if event_type.endswith(".succeeded"):
        row_count = details_json.get("row_count")
        if isinstance(row_count, int):
            return f"Worker run succeeded with {row_count} rows."
        return "Worker run succeeded."

    if event_type.endswith(".failed"):
        error_message = details_json.get("error_message") or details_json.get("error")
        if isinstance(error_message, str) and error_message.strip():
            return f"Worker run failed: {error_message.strip()}"
        return "Worker run failed."

    status = details_json.get("status")
    if isinstance(status, str) and status.strip():
        return f"Worker event recorded with status {status.strip()}."

    if status_code >= 500:
        return "Worker event failed."
    return "Worker event recorded."


def build_worker_console_entry(event: Any) -> dict[str, Any]:
    details_json = _coerce_mapping(getattr(event, "details_json", None))
    event_type = _event_type(details_json)
    return {
        "id": getattr(event, "id"),
        "created_at": getattr(event, "created_at"),
        "event_type": event_type,
        "method": getattr(event, "method"),
        "path": getattr(event, "path"),
        "status_code": getattr(event, "status_code"),
        "resource_type": details_json.get("resource_type"),
        "resource_id": details_json.get("resource_id"),
        "message": build_worker_console_message(details_json, status_code=int(getattr(event, "status_code", 200))),
        "details_json": details_json,
    }


def build_worker_console_sse_frame(entry: Mapping[str, Any]) -> str:
    payload = json.dumps(dict(entry), sort_keys=True, default=str)
    return f"event: message\ndata: {payload}\n\n"


def build_worker_console_heartbeat_sse(*, cursor: str | None = None) -> str:
    payload = json.dumps({"cursor": cursor}, sort_keys=True)
    return f"event: heartbeat\ndata: {payload}\n\n"


def floor_console_bucket_start(value: datetime, *, bucket: str) -> datetime:
    normalized_bucket = bucket.strip().lower()
    if normalized_bucket == "hour":
        return value.replace(minute=0, second=0, microsecond=0)
    if normalized_bucket == "day":
        return value.replace(hour=0, minute=0, second=0, microsecond=0)
    raise ValueError("bucket must be 'hour' or 'day'.")
