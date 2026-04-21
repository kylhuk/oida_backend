from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Callable

from data_platform.log_config import configure_logging
from data_platform.request_context import (
    CorrelationIdFilter,
    REQUEST_ID_HEADER,
    RequestContextMiddleware,
    get_request_id,
    normalize_request_id,
    reset_request_id,
    set_request_id,
)


async def _invoke_middleware(
    app: Callable[..., object],
    *,
    request_headers: list[tuple[bytes, bytes]] | None = None,
) -> tuple[list[dict[str, object]], dict[str, object]]:
    scope: dict[str, object] = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": "/request-id",
        "raw_path": b"/request-id",
        "query_string": b"",
        "root_path": "",
        "client": ("testclient", 123),
        "server": ("testserver", 80),
        "headers": request_headers or [],
        "state": {},
    }
    messages: list[dict[str, object]] = []

    request_sent = False

    async def receive() -> dict[str, object]:
        nonlocal request_sent
        if request_sent:
            return {"type": "http.disconnect"}
        request_sent = True
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message: dict[str, object]) -> None:
        messages.append(message)

    await RequestContextMiddleware(app)(scope, receive, send)
    return messages, scope


async def _json_response_app(
    scope: dict[str, object],
    receive: Callable[..., object],
    send: Callable[..., object],
    *,
    response_headers: list[tuple[bytes, bytes]] | None = None,
) -> None:
    del receive
    payload = json.dumps(
        {
            "request_id": get_request_id(),
            "state_request_id": scope["state"]["request_id"],
        }
    ).encode("utf-8")
    await send(
        {
            "type": "http.response.start",
            "status": 200,
            "headers": response_headers or [(b"content-type", b"application/json")],
        }
    )
    await send({"type": "http.response.body", "body": payload, "more_body": False})


def _get_response_headers(messages: list[dict[str, object]]) -> list[tuple[bytes, bytes]]:
    response_start = next(message for message in messages if message["type"] == "http.response.start")
    return list(response_start.get("headers") or [])


def _get_response_body(messages: list[dict[str, object]]) -> bytes:
    body_parts = [message.get("body", b"") for message in messages if message["type"] == "http.response.body"]
    return b"".join(body_parts)


def test_request_context_middleware_generates_response_header_and_request_state() -> None:
    messages, scope = asyncio.run(_invoke_middleware(_json_response_app))

    headers = _get_response_headers(messages)
    request_id_headers = [value.decode("latin-1") for key, value in headers if key.lower() == b"x-request-id"]
    assert len(request_id_headers) == 1
    header_request_id = request_id_headers[0]
    payload = json.loads(_get_response_body(messages))

    assert payload["request_id"] == header_request_id
    assert payload["state_request_id"] == header_request_id
    assert scope["state"]["request_id"] == header_request_id
    assert len(header_request_id) == 32


def test_request_context_middleware_replaces_existing_response_request_id_header() -> None:
    async def app(scope, receive, send) -> None:
        await _json_response_app(
            scope,
            receive,
            send,
            response_headers=[
                (b"content-type", b"application/json"),
                (REQUEST_ID_HEADER.lower().encode("latin-1"), b"stale-response-id"),
            ],
        )

    messages, _ = asyncio.run(
        _invoke_middleware(
            app,
            request_headers=[(REQUEST_ID_HEADER.lower().encode("latin-1"), b"edge-proxy-req-42")],
        )
    )

    headers = _get_response_headers(messages)
    request_id_headers = [value.decode("latin-1") for key, value in headers if key.lower() == b"x-request-id"]
    payload = json.loads(_get_response_body(messages))

    assert request_id_headers == ["edge-proxy-req-42"]
    assert payload["request_id"] == "edge-proxy-req-42"
    assert payload["state_request_id"] == "edge-proxy-req-42"


def test_normalize_request_id_rejects_unsafe_values() -> None:
    normalized = normalize_request_id("bad\nvalue")

    assert normalized != "bad\nvalue"
    assert len(normalized) == 32


def test_correlation_id_filter_attaches_request_id_to_log_records() -> None:
    token = set_request_id("req-123")
    try:
        record = logging.LogRecord(
            name="test.logger",
            level=logging.INFO,
            pathname=__file__,
            lineno=1,
            msg="hello",
            args=(),
            exc_info=None,
        )
        assert CorrelationIdFilter().filter(record) is True
        assert record.request_id == "req-123"
    finally:
        reset_request_id(token)


def test_configure_logging_installs_correlation_id_filter() -> None:
    configure_logging("INFO")
    root_logger = logging.getLogger()

    assert root_logger.handlers
    assert any(
        isinstance(handler_filter, CorrelationIdFilter)
        for handler in root_logger.handlers
        for handler_filter in handler.filters
    )
