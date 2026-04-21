from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class WorkerConsoleEventResponse(BaseModel):
    id: str
    created_at: datetime
    event_type: str
    method: str
    path: str
    status_code: int
    resource_type: str | None = None
    resource_id: str | None = None
    message: str
    details_json: dict[str, Any] = Field(default_factory=dict)


class WorkerConsolePageResponse(BaseModel):
    items: list[WorkerConsoleEventResponse]
    next_cursor: str | None = None
    tail_cursor: str | None = None
