from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.orm import Mapped, mapped_column

from data_platform.db import Base
from data_platform.models.base import TimestampMixin


class ApiClient(TimestampMixin, Base):
    __tablename__ = "api_clients"

    name: Mapped[str] = mapped_column(sa.String(255), nullable=False)
    key_prefix: Mapped[str] = mapped_column(sa.String(16), nullable=False, index=True)
    key_hash: Mapped[str] = mapped_column(sa.String(64), nullable=False, unique=True, index=True)
    scopes: Mapped[list[str]] = mapped_column(sa.JSON, default=list, nullable=False)
    active: Mapped[bool] = mapped_column(sa.Boolean, default=True, nullable=False)
