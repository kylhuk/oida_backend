from __future__ import annotations

import sqlalchemy as sa
from fastapi import APIRouter, Depends, HTTPException
from redis import Redis
from sqlalchemy.orm import Session

from data_platform.api.deps import get_db
from data_platform.services.clickhouse_service import ClickHouseService
from data_platform.services.storage import ObjectStorageService
from data_platform.settings import get_settings

router = APIRouter(tags=["health"])


@router.get("/health/live")
def live() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/health/ready")
def ready(session: Session = Depends(get_db)) -> dict:
    settings = get_settings()
    checks: dict[str, str] = {}
    try:
        session.execute(sa.text("SELECT 1"))
        checks["postgres"] = "ok"

        Redis.from_url(settings.redis_url).ping()
        checks["redis"] = "ok"

        if not ObjectStorageService().ready():
            raise RuntimeError("Object storage buckets are not ready.")
        checks["object_storage"] = "ok"

        ClickHouseService().ready()
        checks["clickhouse"] = "ok"
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail={"status": "degraded", "checks": checks, "error": str(exc)},
        ) from exc
    return {"status": "ready", "checks": checks}
