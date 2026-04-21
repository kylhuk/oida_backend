from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from data_platform.api.deps import get_db, require_scopes
from data_platform.schemas.audit import AuditEventCountResponse, AuditEventPageResponse, AuditEventResponse
from data_platform.services.audit_service import AuditService

router = APIRouter(prefix="/v1", tags=["audit"])


@router.get("/audit/events", response_model=list[AuditEventResponse])
@router.get("/audit-events", response_model=list[AuditEventResponse], include_in_schema=False)
def list_audit_events(
    request_id: str | None = Query(default=None),
    event_type: str | None = Query(default=None),
    method: str | None = Query(default=None),
    path_prefix: str | None = Query(default=None),
    status_code: int | None = Query(default=None, ge=100, le=599),
    api_client_id: str | None = Query(default=None),
    api_key_prefix: str | None = Query(default=None),
    client_ip: str | None = Query(default=None),
    created_at_after: datetime | None = Query(default=None),
    created_at_before: datetime | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("audit:read")),
) -> list[AuditEventResponse]:
    return [
        AuditEventResponse.model_validate(item)
        for item in AuditService.list_events(
            session,
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
            limit=limit,
            offset=offset,
        )
    ]


@router.get("/audit/events/count", response_model=AuditEventCountResponse)
@router.get("/audit-events/count", response_model=AuditEventCountResponse, include_in_schema=False)
def count_audit_events(
    request_id: str | None = Query(default=None),
    event_type: str | None = Query(default=None),
    method: str | None = Query(default=None),
    path_prefix: str | None = Query(default=None),
    status_code: int | None = Query(default=None, ge=100, le=599),
    api_client_id: str | None = Query(default=None),
    api_key_prefix: str | None = Query(default=None),
    client_ip: str | None = Query(default=None),
    created_at_after: datetime | None = Query(default=None),
    created_at_before: datetime | None = Query(default=None),
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("audit:read")),
) -> AuditEventCountResponse:
    return AuditEventCountResponse(
        count=AuditService.count_events(
            session,
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
    )


@router.get("/audit/events/page", response_model=AuditEventPageResponse)
@router.get("/audit-events/page", response_model=AuditEventPageResponse, include_in_schema=False)
def list_audit_events_page(
    request_id: str | None = Query(default=None),
    event_type: str | None = Query(default=None),
    method: str | None = Query(default=None),
    path_prefix: str | None = Query(default=None),
    status_code: int | None = Query(default=None, ge=100, le=599),
    api_client_id: str | None = Query(default=None),
    api_key_prefix: str | None = Query(default=None),
    client_ip: str | None = Query(default=None),
    created_at_after: datetime | None = Query(default=None),
    created_at_before: datetime | None = Query(default=None),
    cursor: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=1000),
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("audit:read")),
) -> AuditEventPageResponse:
    try:
        page = AuditService.list_events_page(
            session,
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
            cursor=cursor,
            limit=limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return AuditEventPageResponse(
        items=[AuditEventResponse.model_validate(item) for item in page["items"]],
        next_cursor=page["next_cursor"],
    )


@router.get("/audit/events/{event_id}", response_model=AuditEventResponse)
@router.get("/audit-events/{event_id}", response_model=AuditEventResponse, include_in_schema=False)
def get_audit_event(
    event_id: str,
    session: Session = Depends(get_db),
    _: object = Depends(require_scopes("audit:read")),
) -> AuditEventResponse:
    event = AuditService.get_event(session, event_id)
    if event is None:
        raise HTTPException(status_code=404, detail="Audit event not found.")
    return AuditEventResponse.model_validate(event)
