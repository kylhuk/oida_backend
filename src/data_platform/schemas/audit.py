from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class AuditEventResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    created_at: datetime
    updated_at: datetime
    request_id: str | None = None
    method: str
    path: str
    query_string: str | None = None
    status_code: int
    api_client_id: str | None = None
    api_client_name: str | None = None
    api_key_prefix: str | None = None
    client_ip: str | None = None
    user_agent: str | None = None
    details_json: dict[str, Any] = Field(default_factory=dict)


class AuditEventCountResponse(BaseModel):
    count: int


class AuditEventPageResponse(BaseModel):
    items: list[AuditEventResponse]
    next_cursor: str | None = None
