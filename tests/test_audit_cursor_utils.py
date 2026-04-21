from __future__ import annotations

from datetime import datetime, timezone

import pytest

from data_platform.utils.audit_events import (
    assert_audit_event_page_cursor_matches_scope,
    decode_audit_event_page_cursor,
    encode_audit_event_page_cursor,
)



def test_audit_event_page_cursor_round_trips_with_scope() -> None:
    created_at = datetime(2026, 4, 16, 13, 0, tzinfo=timezone.utc)
    cursor = encode_audit_event_page_cursor(
        created_at=created_at,
        audit_event_id="event-1",
        request_id="req-1",
        event_type="http_request",
        method="get",
        path_prefix="/v1/datasets",
        status_code=200,
        api_client_id="client-1",
        api_key_prefix="seeded01",
        client_ip="10.0.0.5",
        created_at_after=datetime(2026, 4, 15, 0, 0, tzinfo=timezone.utc),
        created_at_before=datetime(2026, 4, 17, 0, 0, tzinfo=timezone.utc),
    )

    assert decode_audit_event_page_cursor(cursor) == (created_at, "event-1")
    assert_audit_event_page_cursor_matches_scope(
        cursor,
        request_id="req-1",
        event_type="http_request",
        method="GET",
        path_prefix="/v1/datasets",
        status_code=200,
        api_client_id="client-1",
        api_key_prefix="seeded01",
        client_ip="10.0.0.5",
        created_at_after=datetime(2026, 4, 15, 0, 0, tzinfo=timezone.utc),
        created_at_before=datetime(2026, 4, 17, 0, 0, tzinfo=timezone.utc),
    )



def test_audit_event_page_cursor_rejects_scope_mismatch() -> None:
    cursor = encode_audit_event_page_cursor(
        created_at=datetime(2026, 4, 16, 13, 5, tzinfo=timezone.utc),
        audit_event_id="event-2",
        event_type="worker_event",
        method="POST",
        path_prefix="/v1/pipelines",
    )

    with pytest.raises(ValueError, match="does not match"):
        assert_audit_event_page_cursor_matches_scope(cursor, method="GET", path_prefix="/v1/pipelines")
