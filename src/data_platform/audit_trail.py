from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

AUDIT_REQUEST_ID_HEADER = "X-Request-ID"
AUDIT_EVENT_TYPE = "http_request"
AUDIT_WORKER_EVENT_TYPE = "worker_event"
AUDIT_MAINTENANCE_EVENT_TYPE = "maintenance_event"
AUDIT_WORKER_METHOD = "WORKER"
AUDIT_MAINTENANCE_METHOD = "CLI"
_AUDIT_API_KEY_HEADER = b"x-api-key"
_USER_AGENT_HEADER = b"user-agent"



def utcnow() -> datetime:
    return datetime.now(timezone.utc)



def _extract_header(scope: dict[str, Any], header_name: bytes) -> str | None:
    for key, value in scope.get("headers") or []:
        if key.lower() == header_name:
            return value.decode("latin-1")
    return None



def _extract_client_ip(scope: dict[str, Any]) -> str | None:
    client = scope.get("client")
    if isinstance(client, tuple) and client:
        host = client[0]
        if isinstance(host, str) and host.strip():
            return host.strip()
    return None



def _normalize_api_key_prefix(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    if not stripped:
        return None
    return stripped[:8]



def _extract_request_id(scope: dict[str, Any]) -> str | None:
    state = scope.get("state")
    if isinstance(state, dict):
        request_id = state.get("request_id")
        if isinstance(request_id, str) and request_id.strip():
            return request_id.strip()
    return _extract_header(scope, AUDIT_REQUEST_ID_HEADER.lower().encode("latin-1"))



def _extract_route_name(scope: dict[str, Any]) -> str | None:
    endpoint = scope.get("endpoint")
    route_name = getattr(endpoint, "__name__", None)
    if isinstance(route_name, str) and route_name.strip():
        return route_name
    route = scope.get("route")
    route_name = getattr(route, "name", None)
    if isinstance(route_name, str) and route_name.strip():
        return route_name
    return None



def _extract_path_template(scope: dict[str, Any]) -> str | None:
    route = scope.get("route")
    for attribute in ("path", "path_format"):
        value = getattr(route, attribute, None)
        if isinstance(value, str) and value.strip():
            return value
    return None



def _extract_path_params(scope: dict[str, Any]) -> dict[str, str]:
    raw_params = scope.get("path_params")
    if not isinstance(raw_params, dict):
        return {}
    return {str(key): str(value) for key, value in raw_params.items()}



def _coerce_state_dict(scope: dict[str, Any]) -> dict[str, Any]:
    state = scope.get("state")
    return state if isinstance(state, dict) else {}



def build_http_audit_event(
    scope: dict[str, Any],
    *,
    status_code: int,
    occurred_at: datetime | None = None,
) -> dict[str, Any]:
    query_string = scope.get("query_string") or b""
    if isinstance(query_string, bytes):
        decoded_query_string = query_string.decode("latin-1")
    else:
        decoded_query_string = str(query_string)

    state = _coerce_state_dict(scope)
    raw_api_key = _extract_header(scope, _AUDIT_API_KEY_HEADER)
    authenticated_key_prefix = state.get("api_key_prefix") if isinstance(state.get("api_key_prefix"), str) else None
    details_json = {
        "event_type": AUDIT_EVENT_TYPE,
        "route_name": _extract_route_name(scope),
        "path_template": _extract_path_template(scope),
        "path_params": _extract_path_params(scope),
    }
    return {
        "occurred_at": occurred_at or utcnow(),
        "request_id": _extract_request_id(scope),
        "method": str(scope.get("method") or "GET").upper(),
        "path": str(scope.get("path") or "/"),
        "query_string": decoded_query_string,
        "status_code": int(status_code),
        "api_client_id": state.get("api_client_id") if isinstance(state.get("api_client_id"), str) else None,
        "api_client_name": state.get("api_client_name") if isinstance(state.get("api_client_name"), str) else None,
        "api_key_prefix": authenticated_key_prefix or _normalize_api_key_prefix(raw_api_key),
        "client_ip": _extract_client_ip(scope),
        "user_agent": _extract_header(scope, _USER_AGENT_HEADER),
        "details_json": details_json,
    }



def build_operational_audit_event(
    event_type: str,
    *,
    method: str,
    path: str,
    status_code: int,
    details_json: dict[str, Any] | None = None,
    occurred_at: datetime | None = None,
) -> dict[str, Any]:
    normalized_method = method.strip().upper()
    if not normalized_method:
        raise ValueError("method is required.")

    normalized_path = path.strip()
    if not normalized_path:
        raise ValueError("path is required.")

    normalized_event_type = event_type.strip()
    if not normalized_event_type:
        raise ValueError("event_type is required.")

    resolved_details = dict(details_json or {})
    resolved_details.setdefault("event_type", normalized_event_type)
    return {
        "occurred_at": occurred_at or utcnow(),
        "request_id": None,
        "method": normalized_method,
        "path": normalized_path,
        "query_string": None,
        "status_code": int(status_code),
        "api_client_id": None,
        "api_client_name": None,
        "api_key_prefix": None,
        "client_ip": None,
        "user_agent": None,
        "details_json": resolved_details,
    }



def build_system_audit_event(
    event_type: str,
    *,
    resource_type: str,
    resource_id: str,
    path: str,
    status_code: int,
    details_json: dict[str, Any] | None = None,
    method: str = AUDIT_WORKER_METHOD,
    occurred_at: datetime | None = None,
) -> dict[str, Any]:
    resolved_details = dict(details_json or {})
    resolved_details.setdefault("resource_type", resource_type)
    resolved_details.setdefault("resource_id", resource_id)
    return build_operational_audit_event(
        event_type,
        method=method,
        path=path,
        status_code=status_code,
        details_json=resolved_details,
        occurred_at=occurred_at,
    )



def _persist_event_with_session_factory(session_factory: Callable[[], Any], payload: dict[str, Any]) -> None:
    from data_platform.models.audit import AuditEvent

    session = session_factory()
    try:
        event = AuditEvent(
            created_at=payload.get("occurred_at") or utcnow(),
            updated_at=payload.get("occurred_at") or utcnow(),
            request_id=payload.get("request_id"),
            method=payload["method"],
            path=payload["path"],
            query_string=payload.get("query_string"),
            status_code=payload["status_code"],
            api_client_id=payload.get("api_client_id"),
            api_client_name=payload.get("api_client_name"),
            api_key_prefix=payload.get("api_key_prefix"),
            client_ip=payload.get("client_ip"),
            user_agent=payload.get("user_agent"),
            details_json=payload.get("details_json") or {},
        )
        session.add(event)
        session.commit()
    except Exception:  # pragma: no cover - best effort persistence
        rollback = getattr(session, "rollback", None)
        if callable(rollback):
            rollback()
    finally:
        close = getattr(session, "close", None)
        if callable(close):
            close()



def persist_audit_event_with_session_factory(session_factory: Callable[[], Any], payload: dict[str, Any]) -> None:
    _persist_event_with_session_factory(session_factory, payload)



def record_operational_audit_event(
    session_factory: Callable[[], Any],
    event_type: str,
    *,
    method: str,
    path: str,
    status_code: int,
    details_json: dict[str, Any] | None = None,
    occurred_at: datetime | None = None,
) -> None:
    payload = build_operational_audit_event(
        event_type,
        method=method,
        path=path,
        status_code=status_code,
        details_json=details_json,
        occurred_at=occurred_at,
    )
    persist_audit_event_with_session_factory(session_factory, payload)



class AuditTrailMiddleware:
    def __init__(
        self,
        app: Callable[..., Any],
        *,
        enabled: bool = True,
        exempt_path_prefixes: list[str] | None = None,
        session_factory: Callable[[], Any] | None = None,
        record_event: Callable[[dict[str, Any]], None] | None = None,
        now_fn: Callable[[], datetime] = utcnow,
    ):
        self.app = app
        self.enabled = enabled
        self.exempt_path_prefixes = list(exempt_path_prefixes or [])
        self.session_factory = session_factory
        self.record_event = record_event
        self.now_fn = now_fn

    async def __call__(self, scope: dict[str, Any], receive: Callable[..., Any], send: Callable[..., Any]) -> None:
        if scope.get("type") != "http" or not self.enabled or self._is_exempt(scope):
            await self.app(scope, receive, send)
            return

        status_code = 500

        async def send_wrapper(message: dict[str, Any]) -> None:
            nonlocal status_code
            if message.get("type") == "http.response.start":
                status_code = int(message.get("status") or 500)
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        except Exception:
            self._record(scope, status_code=500)
            raise
        else:
            self._record(scope, status_code=status_code)

    def _is_exempt(self, scope: dict[str, Any]) -> bool:
        method = str(scope.get("method") or "GET").upper()
        if method == "OPTIONS":
            return True
        path = str(scope.get("path") or "/")
        return any(path.startswith(prefix) for prefix in self.exempt_path_prefixes)

    def _record(self, scope: dict[str, Any], *, status_code: int) -> None:
        payload = build_http_audit_event(scope, status_code=status_code, occurred_at=self.now_fn())
        if self.record_event is not None:
            self.record_event(payload)
            return
        if self.session_factory is None:
            return
        _persist_event_with_session_factory(self.session_factory, payload)
