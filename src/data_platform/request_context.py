from __future__ import annotations

import logging
import re
from contextvars import ContextVar, Token
from typing import Any, Callable
from uuid import uuid4

REQUEST_ID_HEADER = "X-Request-ID"
_DEFAULT_REQUEST_ID = "-"
_REQUEST_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_request_id: ContextVar[str] = ContextVar("request_id", default=_DEFAULT_REQUEST_ID)


def get_request_id() -> str:
    return _request_id.get()


def set_request_id(request_id: str) -> Token[str]:
    return _request_id.set(request_id)


def reset_request_id(token: Token[str]) -> None:
    _request_id.reset(token)


def normalize_request_id(value: str | None) -> str:
    if value is None:
        return uuid4().hex
    candidate = value.strip()
    if not candidate or not _REQUEST_ID_RE.fullmatch(candidate):
        return uuid4().hex
    return candidate


class CorrelationIdFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.request_id = get_request_id()
        return True


class RequestContextMiddleware:
    def __init__(self, app: Callable[..., Any]):
        self.app = app

    async def __call__(self, scope: dict[str, Any], receive: Callable[..., Any], send: Callable[..., Any]) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return

        request_id = normalize_request_id(_get_header(scope, REQUEST_ID_HEADER))
        _set_scope_request_id(scope, request_id)
        token = set_request_id(request_id)

        async def send_wrapper(message: dict[str, Any]) -> None:
            if message.get("type") == "http.response.start":
                headers = list(message.get("headers") or [])
                message["headers"] = _upsert_header(
                    headers,
                    REQUEST_ID_HEADER.lower().encode("latin-1"),
                    request_id.encode("latin-1"),
                )
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            reset_request_id(token)


def _get_header(scope: dict[str, Any], name: str) -> str | None:
    header_name = name.lower().encode("latin-1")
    for key, value in scope.get("headers") or []:
        if key.lower() == header_name:
            return value.decode("latin-1")
    return None


def _set_scope_request_id(scope: dict[str, Any], request_id: str) -> None:
    state = scope.setdefault("state", {})
    if isinstance(state, dict):
        state["request_id"] = request_id


def _upsert_header(headers: list[tuple[bytes, bytes]], name: bytes, value: bytes) -> list[tuple[bytes, bytes]]:
    filtered_headers = [header for header in headers if header[0].lower() != name]
    filtered_headers.append((name, value))
    return filtered_headers
