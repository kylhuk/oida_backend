from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from data_platform.api.deps import get_db, require_scopes
from data_platform.schemas.observability import ObservabilityActivityResponse, ObservabilitySummaryResponse
from data_platform.services.observability_service import ObservabilityService

router = APIRouter(prefix="/v1/observability", tags=["observability"])


@router.get(
    "/summary",
    response_model=ObservabilitySummaryResponse,
    dependencies=[Depends(require_scopes("audit:read"))],
)
def get_observability_summary(
    lookback_hours: int = Query(default=24, ge=1, le=24 * 30),
    recent_limit: int = Query(default=10, ge=1, le=100),
    session: Session = Depends(get_db),
) -> ObservabilitySummaryResponse:
    summary = ObservabilityService.get_summary(
        session,
        lookback_hours=lookback_hours,
        recent_limit=recent_limit,
    )
    return ObservabilitySummaryResponse(**summary)


@router.get(
    "/activity",
    response_model=ObservabilityActivityResponse,
    dependencies=[Depends(require_scopes("audit:read"))],
)
def get_observability_activity(
    bucket: str = Query(default="hour"),
    lookback_hours: int = Query(default=24, ge=1, le=24 * 30),
    limit: int = Query(default=48, ge=1, le=1000),
    session: Session = Depends(get_db),
) -> ObservabilityActivityResponse:
    try:
        activity = ObservabilityService.get_activity(
            session,
            bucket=bucket,
            lookback_hours=lookback_hours,
            limit=limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return ObservabilityActivityResponse(**activity)
