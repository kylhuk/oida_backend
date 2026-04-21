from __future__ import annotations

from collections.abc import Callable

from fastapi import Depends, Header, HTTPException, Request, status
from sqlalchemy.orm import Session

from data_platform.db import get_session_factory
from data_platform.models.api_client import ApiClient
from data_platform.request_context import get_request_id
from data_platform.security import authenticate_api_key
from data_platform.settings import get_settings


def _set_request_api_client_state(request: Request | None, client: ApiClient | None) -> None:
    if request is None:
        return
    state = request.scope.setdefault("state", {})
    if not isinstance(state, dict):
        return
    if client is None:
        state.pop("api_client_id", None)
        state.pop("api_client_name", None)
        state.pop("api_key_prefix", None)
        return
    state["api_client_id"] = client.id
    state["api_client_name"] = client.name
    state["api_key_prefix"] = client.key_prefix


def get_db():
    session = get_session_factory()()
    session.info.setdefault("request_id", get_request_id())
    try:
        yield session
    finally:
        session.close()


def require_api_key(
    request: Request,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
    session: Session = Depends(get_db),
) -> ApiClient | None:
    settings = get_settings()
    if not settings.enable_api_key_auth:
        _set_request_api_client_state(request, None)
        return None

    if not x_api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing X-API-Key header.",
        )

    client = authenticate_api_key(session, x_api_key)
    if client is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key.",
        )
    _set_request_api_client_state(request, client)
    return client


def require_scopes(*required_scopes: str) -> Callable[..., ApiClient | None]:
    def dependency(client: ApiClient | None = Depends(require_api_key)) -> ApiClient | None:
        if client is None:
            return None

        client_scopes = set(client.scopes or [])
        missing = [scope for scope in required_scopes if scope not in client_scopes]
        if missing:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"API key lacks required scopes: {', '.join(missing)}",
            )
        return client

    return dependency
