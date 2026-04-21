from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from sqlalchemy.orm import Session

    from data_platform.models.api_client import ApiClient


DEFAULT_SEEDED_SCOPES = [
    "audit:read",
    "datasets:read",
    "datasets:write",
    "ingestions:read",
    "ingestions:write",
    "gold:read",
    "pipelines:read",
    "pipelines:write",
]


def hash_api_key(raw_key: str) -> str:
    return hashlib.sha256(raw_key.encode("utf-8")).hexdigest()


def key_prefix(raw_key: str) -> str:
    return raw_key[:8]


def authenticate_api_key(session: Session, raw_key: str) -> ApiClient | None:
    from sqlalchemy import select

    from data_platform.models.api_client import ApiClient

    hashed = hash_api_key(raw_key)
    return session.scalar(
        select(ApiClient).where(ApiClient.key_hash == hashed, ApiClient.active.is_(True))
    )


def ensure_seeded_api_key(session: Session) -> None:
    from sqlalchemy import select

    from data_platform.models.api_client import ApiClient
    from data_platform.settings import get_settings

    settings = get_settings()
    raw_key = settings.seed_dev_api_key
    hashed = hash_api_key(raw_key)
    existing = session.scalar(select(ApiClient).where(ApiClient.key_hash == hashed))
    if existing:
        desired_scopes = sorted(set(existing.scopes or []).union(DEFAULT_SEEDED_SCOPES))
        if existing.scopes != desired_scopes or not existing.active or existing.key_prefix != key_prefix(raw_key):
            existing.key_prefix = key_prefix(raw_key)
            existing.scopes = desired_scopes
            existing.active = True
            session.add(existing)
            session.commit()
        return

    client = ApiClient(
        name="development-seeded-client",
        key_prefix=key_prefix(raw_key),
        key_hash=hashed,
        scopes=DEFAULT_SEEDED_SCOPES,
        active=True,
    )
    session.add(client)
    session.commit()
