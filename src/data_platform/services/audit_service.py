from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import func, or_, and_, select
from sqlalchemy.orm import Session

from data_platform.models.audit import AuditEvent
from data_platform.utils.audit_events import (
    assert_audit_event_page_cursor_matches_scope,
    decode_audit_event_page_cursor,
    encode_audit_event_page_cursor,
)


class AuditService:
    @staticmethod
    def _normalize_filter(value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        return normalized or None

    @staticmethod
    def _apply_filters(
        stmt: Any,
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
    ) -> Any:
        normalized_request_id = AuditService._normalize_filter(request_id)
        if normalized_request_id is not None:
            stmt = stmt.where(AuditEvent.request_id == normalized_request_id)

        normalized_event_type = AuditService._normalize_filter(event_type)
        if normalized_event_type is not None:
            stmt = stmt.where(AuditEvent.details_json["event_type"].as_string() == normalized_event_type)

        normalized_method = AuditService._normalize_filter(method)
        if normalized_method is not None:
            stmt = stmt.where(AuditEvent.method == normalized_method.upper())

        normalized_path_prefix = AuditService._normalize_filter(path_prefix)
        if normalized_path_prefix is not None:
            stmt = stmt.where(AuditEvent.path.like(f"{normalized_path_prefix}%"))

        if status_code is not None:
            stmt = stmt.where(AuditEvent.status_code == status_code)

        normalized_api_client_id = AuditService._normalize_filter(api_client_id)
        if normalized_api_client_id is not None:
            stmt = stmt.where(AuditEvent.api_client_id == normalized_api_client_id)

        normalized_api_key_prefix = AuditService._normalize_filter(api_key_prefix)
        if normalized_api_key_prefix is not None:
            stmt = stmt.where(AuditEvent.api_key_prefix == normalized_api_key_prefix)

        normalized_client_ip = AuditService._normalize_filter(client_ip)
        if normalized_client_ip is not None:
            stmt = stmt.where(AuditEvent.client_ip == normalized_client_ip)

        if created_at_after is not None:
            stmt = stmt.where(AuditEvent.created_at >= created_at_after)
        if created_at_before is not None:
            stmt = stmt.where(AuditEvent.created_at <= created_at_before)
        return stmt

    @staticmethod
    def count_events(
        session: Session,
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
    ) -> int:
        stmt = select(func.count(AuditEvent.id))
        stmt = AuditService._apply_filters(
            stmt,
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
        return int(session.scalar(stmt) or 0)

    @staticmethod
    def list_events(
        session: Session,
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
        limit: int = 100,
        offset: int = 0,
    ) -> list[AuditEvent]:
        bounded_limit = max(1, min(limit, 1000))
        bounded_offset = max(0, offset)
        stmt = select(AuditEvent).order_by(AuditEvent.created_at.desc(), AuditEvent.id.desc())
        stmt = AuditService._apply_filters(
            stmt,
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
        stmt = stmt.limit(bounded_limit).offset(bounded_offset)
        return list(session.scalars(stmt).all())

    @staticmethod
    def list_events_page(
        session: Session,
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
        cursor: str | None = None,
        limit: int = 100,
    ) -> dict[str, object]:
        bounded_limit = max(1, min(limit, 1000))
        stmt = select(AuditEvent)
        stmt = AuditService._apply_filters(
            stmt,
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
        if cursor is not None:
            assert_audit_event_page_cursor_matches_scope(
                cursor,
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
            cursor_created_at, cursor_event_id = decode_audit_event_page_cursor(cursor)
            stmt = stmt.where(
                or_(
                    AuditEvent.created_at < cursor_created_at,
                    and_(AuditEvent.created_at == cursor_created_at, AuditEvent.id < cursor_event_id),
                )
            )
        stmt = stmt.order_by(AuditEvent.created_at.desc(), AuditEvent.id.desc()).limit(bounded_limit + 1)
        rows = list(session.scalars(stmt).all())
        has_more = len(rows) > bounded_limit
        items = rows[:bounded_limit]
        next_cursor = None
        if has_more and items:
            last = items[-1]
            next_cursor = encode_audit_event_page_cursor(
                created_at=last.created_at,
                audit_event_id=last.id,
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
        return {"items": items, "next_cursor": next_cursor}

    @staticmethod
    def build_event_cursor(
        event: AuditEvent,
        *,
        request_id: str | None = None,
        method: str | None = None,
        path_prefix: str | None = None,
        status_code: int | None = None,
        api_client_id: str | None = None,
        api_key_prefix: str | None = None,
        client_ip: str | None = None,
        created_at_after: datetime | None = None,
        created_at_before: datetime | None = None,
    ) -> str:
        return encode_audit_event_page_cursor(
            created_at=event.created_at,
            audit_event_id=event.id,
            request_id=request_id,
            method=method,
            path_prefix=path_prefix,
            status_code=status_code,
            api_client_id=api_client_id,
            api_key_prefix=api_key_prefix,
            client_ip=client_ip,
            created_at_after=created_at_after,
            created_at_before=created_at_before,
        )

    @staticmethod
    def get_latest_event_cursor(
        session: Session,
        *,
        request_id: str | None = None,
        method: str | None = None,
        path_prefix: str | None = None,
        status_code: int | None = None,
        api_client_id: str | None = None,
        api_key_prefix: str | None = None,
        client_ip: str | None = None,
        created_at_after: datetime | None = None,
        created_at_before: datetime | None = None,
    ) -> str | None:
        stmt = select(AuditEvent).order_by(AuditEvent.created_at.desc(), AuditEvent.id.desc()).limit(1)
        stmt = AuditService._apply_filters(
            stmt,
            request_id=request_id,
            method=method,
            path_prefix=path_prefix,
            status_code=status_code,
            api_client_id=api_client_id,
            api_key_prefix=api_key_prefix,
            client_ip=client_ip,
            created_at_after=created_at_after,
            created_at_before=created_at_before,
        )
        latest = session.scalar(stmt)
        if latest is None:
            return None
        return AuditService.build_event_cursor(
            latest,
            request_id=request_id,
            method=method,
            path_prefix=path_prefix,
            status_code=status_code,
            api_client_id=api_client_id,
            api_key_prefix=api_key_prefix,
            client_ip=client_ip,
            created_at_after=created_at_after,
            created_at_before=created_at_before,
        )

    @staticmethod
    def list_events_forward(
        session: Session,
        *,
        cursor: str,
        request_id: str | None = None,
        method: str | None = None,
        path_prefix: str | None = None,
        status_code: int | None = None,
        api_client_id: str | None = None,
        api_key_prefix: str | None = None,
        client_ip: str | None = None,
        created_at_after: datetime | None = None,
        created_at_before: datetime | None = None,
        limit: int = 100,
    ) -> dict[str, object]:
        bounded_limit = max(1, min(limit, 1000))
        assert_audit_event_page_cursor_matches_scope(
            cursor,
            request_id=request_id,
            method=method,
            path_prefix=path_prefix,
            status_code=status_code,
            api_client_id=api_client_id,
            api_key_prefix=api_key_prefix,
            client_ip=client_ip,
            created_at_after=created_at_after,
            created_at_before=created_at_before,
        )
        cursor_created_at, cursor_event_id = decode_audit_event_page_cursor(cursor)
        stmt = select(AuditEvent)
        stmt = AuditService._apply_filters(
            stmt,
            request_id=request_id,
            method=method,
            path_prefix=path_prefix,
            status_code=status_code,
            api_client_id=api_client_id,
            api_key_prefix=api_key_prefix,
            client_ip=client_ip,
            created_at_after=created_at_after,
            created_at_before=created_at_before,
        )
        stmt = stmt.where(
            or_(
                AuditEvent.created_at > cursor_created_at,
                and_(AuditEvent.created_at == cursor_created_at, AuditEvent.id > cursor_event_id),
            )
        )
        stmt = stmt.order_by(AuditEvent.created_at.asc(), AuditEvent.id.asc()).limit(bounded_limit)
        items = list(session.scalars(stmt).all())
        next_cursor = cursor
        if items:
            next_cursor = AuditService.build_event_cursor(
                items[-1],
                request_id=request_id,
                method=method,
                path_prefix=path_prefix,
                status_code=status_code,
                api_client_id=api_client_id,
                api_key_prefix=api_key_prefix,
                client_ip=client_ip,
                created_at_after=created_at_after,
                created_at_before=created_at_before,
            )
        return {"items": items, "next_cursor": next_cursor}

    @staticmethod
    def get_event(session: Session, audit_event_id: str) -> AuditEvent | None:
        normalized_id = AuditService._normalize_filter(audit_event_id)
        if normalized_id is None:
            return None
        return session.scalar(select(AuditEvent).where(AuditEvent.id == normalized_id))

    # Backward-compatible aliases for earlier internal callers.
    count_audit_events = count_events
    list_audit_events = list_events
    get_audit_event = get_event
