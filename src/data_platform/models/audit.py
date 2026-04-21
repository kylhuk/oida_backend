from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.orm import Mapped, mapped_column

from data_platform.db import Base
from data_platform.models.base import TimestampMixin


class AuditEvent(TimestampMixin, Base):
    __tablename__ = "audit_events"
    __table_args__ = (
        sa.Index("ix_audit_events_created", "created_at"),
        sa.Index("ix_audit_events_request_id_created", "request_id", "created_at"),
        sa.Index("ix_audit_events_api_key_prefix_created", "api_key_prefix", "created_at"),
        sa.Index("ix_audit_events_method_created", "method", "created_at"),
        sa.Index("ix_audit_events_status_created", "status_code", "created_at"),
    )

    request_id: Mapped[str | None] = mapped_column(sa.String(128), index=True)
    method: Mapped[str] = mapped_column(sa.String(16), nullable=False)
    path: Mapped[str] = mapped_column(sa.String(1024), nullable=False)
    query_string: Mapped[str | None] = mapped_column(sa.Text)
    status_code: Mapped[int] = mapped_column(sa.Integer, nullable=False, index=True)

    api_client_id: Mapped[str | None] = mapped_column(sa.ForeignKey("api_clients.id", ondelete="SET NULL"), index=True)
    api_client_name: Mapped[str | None] = mapped_column(sa.String(255))
    api_key_prefix: Mapped[str | None] = mapped_column(sa.String(16), index=True)
    client_ip: Mapped[str | None] = mapped_column(sa.String(64))
    user_agent: Mapped[str | None] = mapped_column(sa.String(512))
    details_json: Mapped[dict] = mapped_column(sa.JSON, default=dict, nullable=False)
