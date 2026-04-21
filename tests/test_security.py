from __future__ import annotations

from data_platform.models.api_client import ApiClient
from data_platform.security import DEFAULT_SEEDED_SCOPES, ensure_seeded_api_key, hash_api_key
from data_platform.settings import get_settings



def test_ensure_seeded_api_key_upgrades_existing_scopes(db_session):
    settings = get_settings()
    existing = ApiClient(
        name="legacy-client",
        key_prefix="legacy",
        key_hash=hash_api_key(settings.seed_dev_api_key),
        scopes=["datasets:read"],
        active=False,
    )
    db_session.add(existing)
    db_session.commit()

    ensure_seeded_api_key(db_session)
    db_session.refresh(existing)

    assert existing.active is True
    assert existing.key_prefix == settings.seed_dev_api_key[:8]
    assert set(DEFAULT_SEEDED_SCOPES).issubset(set(existing.scopes))
