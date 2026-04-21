from __future__ import annotations

import base64
import binascii
import json
from datetime import datetime, timezone
from typing import Any


def _coerce_utc_datetime(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _encode_optional_datetime(value: datetime | None) -> str | None:
    normalized = _coerce_utc_datetime(value)
    if normalized is None:
        return None
    return normalized.isoformat()


def _normalize_filter(value: str | None, *, field_name: str) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} cannot be empty.")
    return normalized


def _build_audit_event_page_scope(
    *,
    request_id: str | None = None,
    event_type: str | None = None,
    method: str | None = None,
    path_prefix: str | None = None,
    status_code: int | None = None,
    api_client_id: str | None = None,
    api_key_prefix: str | None = None,
    client_ip: str | None = None,
    created_at_after: datetime | None = None,
    created_at_before: datetime | None = None,
) -> dict[str, Any]:
    if status_code is not None and not (100 <= int(status_code) <= 599):
        raise ValueError("status_code must be between 100 and 599.")
    return {
        "request_id": _normalize_filter(request_id, field_name="request_id") if request_id is not None else None,
        "event_type": _normalize_filter(event_type, field_name="event_type") if event_type is not None else None,
        "method": _normalize_filter(method, field_name="method").upper() if method is not None else None,
        "path_prefix": _normalize_filter(path_prefix, field_name="path_prefix") if path_prefix is not None else None,
        "status_code": int(status_code) if status_code is not None else None,
        "api_client_id": _normalize_filter(api_client_id, field_name="api_client_id") if api_client_id is not None else None,
        "api_key_prefix": _normalize_filter(api_key_prefix, field_name="api_key_prefix") if api_key_prefix is not None else None,
        "client_ip": _normalize_filter(client_ip, field_name="client_ip") if client_ip is not None else None,
        "created_at_after": _encode_optional_datetime(created_at_after),
        "created_at_before": _encode_optional_datetime(created_at_before),
    }


def _decode_page_cursor_payload(cursor: str) -> dict[str, Any]:
    normalized_cursor = _normalize_filter(cursor, field_name="cursor")
    padded_cursor = normalized_cursor + ("=" * (-len(normalized_cursor) % 4))
    try:
        payload = json.loads(base64.urlsafe_b64decode(padded_cursor.encode("ascii")).decode("utf-8"))
    except (ValueError, TypeError, json.JSONDecodeError, UnicodeDecodeError, binascii.Error) as exc:
        raise ValueError("cursor is invalid.") from exc
    if not isinstance(payload, dict) or payload.get("v") != 1:
        raise ValueError("cursor is invalid.")
    return payload


def encode_audit_event_page_cursor(
    *,
    created_at: datetime,
    audit_event_id: str,
    request_id: str | None = None,
    event_type: str | None = None,
    method: str | None = None,
    path_prefix: str | None = None,
    status_code: int | None = None,
    api_client_id: str | None = None,
    api_key_prefix: str | None = None,
    client_ip: str | None = None,
    created_at_after: datetime | None = None,
    created_at_before: datetime | None = None,
) -> str:
    normalized_created_at = _coerce_utc_datetime(created_at)
    if normalized_created_at is None:
        raise ValueError("created_at is required.")
    normalized_audit_event_id = _normalize_filter(audit_event_id, field_name="audit_event_id")
    payload = {
        "v": 1,
        "created_at": normalized_created_at.isoformat(),
        "audit_event_id": normalized_audit_event_id,
        "scope": _build_audit_event_page_scope(
            request_id=request_id,
            event_type=event_type,
            method=method,
            path_prefix=path_prefix,
            status_code=status_code,
            api_client_id=api_client_id,
            api_key_prefix=api_key_prefix,
            client_ip=client_ip,
            created_at_after=created_at_after,
            created_at_before=created_at_before,
        ),
    }
    encoded = base64.urlsafe_b64encode(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).decode("ascii")
    return encoded.rstrip("=")


def assert_audit_event_page_cursor_matches_scope(
    cursor: str,
    *,
    request_id: str | None = None,
    event_type: str | None = None,
    method: str | None = None,
    path_prefix: str | None = None,
    status_code: int | None = None,
    api_client_id: str | None = None,
    api_key_prefix: str | None = None,
    client_ip: str | None = None,
    created_at_after: datetime | None = None,
    created_at_before: datetime | None = None,
) -> None:
    payload = _decode_page_cursor_payload(cursor)
    expected_scope = _build_audit_event_page_scope(
        request_id=request_id,
        event_type=event_type,
        method=method,
        path_prefix=path_prefix,
        status_code=status_code,
        api_client_id=api_client_id,
        api_key_prefix=api_key_prefix,
        client_ip=client_ip,
        created_at_after=created_at_after,
        created_at_before=created_at_before,
    )
    if payload.get("scope") != expected_scope:
        raise ValueError("cursor does not match the current audit-event selection.")


def decode_audit_event_page_cursor(cursor: str) -> tuple[datetime, str]:
    payload = _decode_page_cursor_payload(cursor)
    created_at_raw = payload.get("created_at")
    audit_event_id = payload.get("audit_event_id")
    if not isinstance(created_at_raw, str) or not isinstance(audit_event_id, str):
        raise ValueError("cursor is invalid.")
    try:
        created_at = datetime.fromisoformat(created_at_raw)
    except ValueError as exc:
        raise ValueError("cursor is invalid.") from exc
    normalized_created_at = _coerce_utc_datetime(created_at)
    normalized_audit_event_id = _normalize_filter(audit_event_id, field_name="cursor.audit_event_id")
    return normalized_created_at, normalized_audit_event_id
