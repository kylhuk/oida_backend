from __future__ import annotations

import asyncio
import json
from collections.abc import Callable

from data_platform.rate_limit import InMemoryRateLimiter, RATE_LIMIT_EXCEEDED_DETAIL, RateLimitMiddleware


class _Clock:
    def __init__(self, now: float = 0.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


async def _invoke(
    middleware: RateLimitMiddleware,
    *,
    path: str = "/limited",
    client: tuple[str, int] = ("127.0.0.1", 1234),
    headers: list[tuple[bytes, bytes]] | None = None,
) -> list[dict[str, object]]:
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": path,
        "raw_path": path.encode("utf-8"),
        "query_string": b"",
        "root_path": "",
        "client": client,
        "server": ("testserver", 80),
        "headers": headers or [],
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

    await middleware(scope, receive, send)
    return messages


async def _json_app(scope, receive: Callable[..., object], send: Callable[..., object]) -> None:
    del scope, receive
    await send(
        {
            "type": "http.response.start",
            "status": 200,
            "headers": [
                (b"content-type", b"application/json"),
                (b"x-ratelimit-limit", b"stale"),
                (b"x-ratelimit-remaining", b"stale"),
            ],
        }
    )
    await send({"type": "http.response.body", "body": b'{"ok": true}', "more_body": False})


def _response(messages: list[dict[str, object]]) -> tuple[int, dict[str, str], dict[str, object]]:
    start = next(message for message in messages if message["type"] == "http.response.start")
    status = int(start["status"])
    headers = {
        key.decode("latin-1").lower(): value.decode("latin-1")
        for key, value in list(start.get("headers") or [])
    }
    body = b"".join(message.get("body", b"") for message in messages if message["type"] == "http.response.body")
    return status, headers, json.loads(body.decode("utf-8"))


def test_rate_limit_middleware_sets_canonical_headers_on_allowed_requests() -> None:
    clock = _Clock(now=0.0)
    middleware = RateLimitMiddleware(
        _json_app,
        enabled=True,
        limiter=InMemoryRateLimiter(limit=2, window_seconds=60, time_fn=clock),
    )

    status, headers, payload = _response(asyncio.run(_invoke(middleware)))

    assert status == 200
    assert payload == {"ok": True}
    assert headers["x-ratelimit-limit"] == "2"
    assert headers["x-ratelimit-remaining"] == "1"
    assert headers["x-ratelimit-reset"] == "60"


def test_rate_limit_middleware_rejects_requests_after_budget_is_exhausted() -> None:
    clock = _Clock(now=5.0)
    middleware = RateLimitMiddleware(
        _json_app,
        enabled=True,
        limiter=InMemoryRateLimiter(limit=2, window_seconds=60, time_fn=clock),
    )
    headers = [(b"x-api-key", b"client-a")]

    asyncio.run(_invoke(middleware, headers=headers))
    asyncio.run(_invoke(middleware, headers=headers))
    status, response_headers, payload = _response(asyncio.run(_invoke(middleware, headers=headers)))

    assert status == 429
    assert payload == {"detail": RATE_LIMIT_EXCEEDED_DETAIL}
    assert response_headers["retry-after"] == response_headers["x-ratelimit-reset"]
    assert response_headers["x-ratelimit-limit"] == "2"
    assert response_headers["x-ratelimit-remaining"] == "0"


def test_rate_limit_uses_api_key_before_client_ip_for_identity() -> None:
    clock = _Clock(now=0.0)
    middleware = RateLimitMiddleware(
        _json_app,
        enabled=True,
        limiter=InMemoryRateLimiter(limit=1, window_seconds=60, time_fn=clock),
    )

    first = _response(
        asyncio.run(_invoke(middleware, headers=[(b"x-api-key", b"client-a")], client=("10.0.0.1", 1)))
    )
    second = _response(
        asyncio.run(_invoke(middleware, headers=[(b"x-api-key", b"client-b")], client=("10.0.0.1", 1)))
    )
    third = _response(
        asyncio.run(_invoke(middleware, headers=[(b"x-api-key", b"client-a")], client=("10.0.0.1", 1)))
    )

    assert first[0] == 200
    assert second[0] == 200
    assert third[0] == 429


def test_rate_limit_resets_after_window_and_skips_exempt_paths() -> None:
    clock = _Clock(now=0.0)
    middleware = RateLimitMiddleware(
        _json_app,
        enabled=True,
        limiter=InMemoryRateLimiter(limit=1, window_seconds=60, time_fn=clock),
    )

    exempt_one = _response(asyncio.run(_invoke(middleware, path="/health/ready")))
    exempt_two = _response(asyncio.run(_invoke(middleware, path="/health/ready")))
    limited_one = _response(asyncio.run(_invoke(middleware, path="/limited")))
    blocked = _response(asyncio.run(_invoke(middleware, path="/limited")))
    clock.advance(61)
    reset = _response(asyncio.run(_invoke(middleware, path="/limited")))

    assert exempt_one[0] == 200
    assert exempt_two[0] == 200
    assert limited_one[0] == 200
    assert blocked[0] == 429
    assert reset[0] == 200
