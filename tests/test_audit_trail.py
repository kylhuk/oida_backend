from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from data_platform.audit_trail import (
    AUDIT_MAINTENANCE_METHOD,
    AUDIT_WORKER_METHOD,
    AuditTrailMiddleware,
    build_http_audit_event,
    build_operational_audit_event,
    build_system_audit_event,
)


class _RouteStub:
    path = "/v1/datasets/{dataset_slug}"
    path_format = "/v1/datasets/{dataset_slug}"
    name = "get_dataset"


async def _receive() -> dict:
    return {"type": "http.request", "body": b"", "more_body": False}


async def _send(message: dict) -> None:
    return None


def test_build_http_audit_event_prefers_authenticated_scope_state() -> None:
    occurred_at = datetime(2026, 4, 16, 9, 30, tzinfo=timezone.utc)
    scope = {
        "type": "http",
        "method": "get",
        "path": "/v1/datasets/orders",
        "query_string": b"verbose=true",
        "headers": [
            (b"x-request-id", b"req-header"),
            (b"x-api-key", b"external-key-value"),
            (b"user-agent", b"pytest-agent"),
        ],
        "client": ("127.0.0.1", 5150),
        "endpoint": lambda: None,
        "route": _RouteStub(),
        "path_params": {"dataset_slug": "orders"},
        "state": {
            "request_id": "req-state",
            "api_client_id": "client-1",
            "api_client_name": "qa-client",
            "api_key_prefix": "seeded01",
        },
    }

    payload = build_http_audit_event(scope, status_code=201, occurred_at=occurred_at)

    assert payload["occurred_at"] == occurred_at
    assert payload["request_id"] == "req-state"
    assert payload["method"] == "GET"
    assert payload["path"] == "/v1/datasets/orders"
    assert payload["query_string"] == "verbose=true"
    assert payload["status_code"] == 201
    assert payload["api_client_id"] == "client-1"
    assert payload["api_client_name"] == "qa-client"
    assert payload["api_key_prefix"] == "seeded01"
    assert payload["client_ip"] == "127.0.0.1"
    assert payload["user_agent"] == "pytest-agent"
    assert payload["details_json"] == {
        "event_type": "http_request",
        "route_name": "<lambda>",
        "path_template": "/v1/datasets/{dataset_slug}",
        "path_params": {"dataset_slug": "orders"},
    }


def test_audit_trail_middleware_records_success_events() -> None:
    recorded: list[dict] = []
    occurred_at = datetime(2026, 4, 16, 10, 0, tzinfo=timezone.utc)

    async def app(scope, receive, send):
        await send({"type": "http.response.start", "status": 204, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    middleware = AuditTrailMiddleware(
        app,
        enabled=True,
        exempt_path_prefixes=[],
        record_event=recorded.append,
        now_fn=lambda: occurred_at,
    )
    scope = {
        "type": "http",
        "method": "POST",
        "path": "/v1/catalog/search",
        "query_string": b"q=orders",
        "headers": [(b"x-request-id", b"req-42")],
        "state": {},
        "client": ("127.0.0.1", 9000),
    }

    asyncio.run(middleware(scope, _receive, _send))

    assert len(recorded) == 1
    assert recorded[0]["status_code"] == 204
    assert recorded[0]["request_id"] == "req-42"
    assert recorded[0]["method"] == "POST"
    assert recorded[0]["occurred_at"] == occurred_at


def test_audit_trail_middleware_skips_exempt_paths_and_records_500_on_exceptions() -> None:
    recorded: list[dict] = []
    occurred_at = datetime(2026, 4, 16, 10, 5, tzinfo=timezone.utc)

    async def failing_app(scope, receive, send):
        raise RuntimeError("boom")

    exempt_middleware = AuditTrailMiddleware(
        failing_app,
        enabled=True,
        exempt_path_prefixes=["/health"],
        record_event=recorded.append,
        now_fn=lambda: occurred_at,
    )
    exempt_scope = {
        "type": "http",
        "method": "GET",
        "path": "/health/ready",
        "query_string": b"",
        "headers": [],
        "state": {},
    }
    try:
        asyncio.run(exempt_middleware(exempt_scope, _receive, _send))
    except RuntimeError:
        pass

    assert recorded == []

    active_middleware = AuditTrailMiddleware(
        failing_app,
        enabled=True,
        exempt_path_prefixes=[],
        record_event=recorded.append,
        now_fn=lambda: occurred_at,
    )
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/v1/catalog/search",
        "query_string": b"",
        "headers": [],
        "state": {},
    }

    try:
        asyncio.run(active_middleware(scope, _receive, _send))
    except RuntimeError:
        pass

    assert len(recorded) == 1
    assert recorded[0]["status_code"] == 500
    assert recorded[0]["occurred_at"] == occurred_at


def test_build_operational_and_system_audit_events_capture_non_request_activity() -> None:
    occurred_at = datetime(2026, 4, 16, 10, 10, tzinfo=timezone.utc)

    maintenance = build_operational_audit_event(
        "metadata_cleanup",
        method=AUDIT_MAINTENANCE_METHOD,
        path="/cli/metadata-cleanup",
        status_code=200,
        details_json={"matched_rows": 3},
        occurred_at=occurred_at,
    )
    assert maintenance["occurred_at"] == occurred_at
    assert maintenance["method"] == "CLI"
    assert maintenance["path"] == "/cli/metadata-cleanup"
    assert maintenance["details_json"]["event_type"] == "metadata_cleanup"
    assert maintenance["details_json"]["matched_rows"] == 3

    worker = build_system_audit_event(
        "pipeline_run.failed",
        resource_type="pipeline_run",
        resource_id="run-1",
        method=AUDIT_WORKER_METHOD,
        path="/worker/pipeline-runs/run-1",
        status_code=500,
        details_json={"task_id": "task-1"},
        occurred_at=occurred_at,
    )
    assert worker["method"] == "WORKER"
    assert worker["details_json"]["event_type"] == "pipeline_run.failed"
    assert worker["details_json"]["resource_type"] == "pipeline_run"
    assert worker["details_json"]["resource_id"] == "run-1"
    assert worker["details_json"]["task_id"] == "task-1"
