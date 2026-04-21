from __future__ import annotations

from datetime import datetime, timezone

import pytest

from data_platform.utils.audit_events import (
    assert_audit_event_page_cursor_matches_scope,
    decode_audit_event_page_cursor,
    encode_audit_event_page_cursor,
)


def test_audit_event_page_cursor_round_trips_and_binds_scope() -> None:
    created_at = datetime(2026, 4, 16, 11, 0, tzinfo=timezone.utc)
    cursor = encode_audit_event_page_cursor(
        created_at=created_at,
        audit_event_id="audit-1",
        request_id="req-1",
        event_type="http_request",
        method="get",
        path_prefix="/v1/datasets",
        status_code=200,
        api_client_id="client-1",
        api_key_prefix="seeded01",
        client_ip="127.0.0.1",
    )

    assert decode_audit_event_page_cursor(cursor) == (created_at, "audit-1")
    assert_audit_event_page_cursor_matches_scope(
        cursor,
        request_id="req-1",
        event_type="http_request",
        method="GET",
        path_prefix="/v1/datasets",
        status_code=200,
        api_client_id="client-1",
        api_key_prefix="seeded01",
        client_ip="127.0.0.1",
    )


def test_audit_event_page_cursor_rejects_scope_mismatches() -> None:
    cursor = encode_audit_event_page_cursor(
        created_at=datetime(2026, 4, 16, 11, 5, tzinfo=timezone.utc),
        audit_event_id="audit-2",
        event_type="worker_event",
        method="POST",
    )

    with pytest.raises(ValueError, match="current audit-event selection"):
        assert_audit_event_page_cursor_matches_scope(cursor, method="GET")

    with pytest.raises(ValueError, match="cursor is invalid"):
        decode_audit_event_page_cursor("not-a-cursor")
