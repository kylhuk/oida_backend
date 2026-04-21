from __future__ import annotations

import hashlib
import json
import math
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from threading import Lock
from typing import Any

RATE_LIMIT_EXCEEDED_DETAIL = "Rate limit exceeded."
RATE_LIMIT_LIMIT_HEADER = b"x-ratelimit-limit"
RATE_LIMIT_REMAINING_HEADER = b"x-ratelimit-remaining"
RATE_LIMIT_RESET_HEADER = b"x-ratelimit-reset"
RETRY_AFTER_HEADER = b"retry-after"
_API_KEY_HEADER = b"x-api-key"

_DEFAULT_EXEMPT_PATH_PREFIXES = ("/health", "/docs", "/openapi.json", "/redoc")


@dataclass(frozen=True)
class RateLimitDecision:
    allowed: bool
    limit: int
    remaining: int
    reset_after_seconds: int
    retry_after_seconds: int | None = None


class InMemoryRateLimiter:
    def __init__(
        self,
        limit: int,
        window_seconds: int,
        *,
        time_fn: Callable[[], float] | None = None,
    ) -> None:
        if limit <= 0:
            raise ValueError("limit must be greater than zero.")
        if window_seconds <= 0:
            raise ValueError("window_seconds must be greater than zero.")
        self.limit = int(limit)
        self.window_seconds = int(window_seconds)
        self._time_fn = time_fn or time.monotonic
        self._buckets: dict[str, deque[float]] = {}
        self._lock = Lock()

    def check(self, key: str) -> RateLimitDecision:
        now = float(self._time_fn())
        cutoff = now - self.window_seconds

        with self._lock:
            bucket = self._buckets.get(key)
            if bucket is None:
                bucket = deque()
                self._buckets[key] = bucket

            while bucket and bucket[0] <= cutoff:
                bucket.popleft()

            if len(bucket) >= self.limit:
                retry_after = max(1, math.ceil((bucket[0] + self.window_seconds) - now))
                return RateLimitDecision(
                    allowed=False,
                    limit=self.limit,
                    remaining=0,
                    reset_after_seconds=retry_after,
                    retry_after_seconds=retry_after,
                )

            bucket.append(now)
            remaining = max(0, self.limit - len(bucket))
            reset_after = max(1, math.ceil((bucket[0] + self.window_seconds) - now))
            return RateLimitDecision(
                allowed=True,
                limit=self.limit,
                remaining=remaining,
                reset_after_seconds=reset_after,
            )


class RateLimitMiddleware:
    def __init__(
        self,
        app: Callable[..., Any],
        *,
        enabled: bool = False,
        limit: int = 120,
        window_seconds: int = 60,
        exempt_path_prefixes: list[str] | tuple[str, ...] | None = None,
        limiter: InMemoryRateLimiter | None = None,
    ) -> None:
        self.app = app
        self.enabled = enabled
        self.exempt_path_prefixes = tuple(
            item.strip()
            for item in (exempt_path_prefixes or _DEFAULT_EXEMPT_PATH_PREFIXES)
            if item and item.strip()
        )
        self.limiter = limiter or InMemoryRateLimiter(limit=limit, window_seconds=window_seconds)

    async def __call__(self, scope: dict[str, Any], receive: Callable[..., Any], send: Callable[..., Any]) -> None:
        if scope.get("type") != "http" or not self.enabled or _is_exempt_request(scope, self.exempt_path_prefixes):
            await self.app(scope, receive, send)
            return

        decision = self.limiter.check(_build_identity(scope))
        if not decision.allowed:
            await _send_rate_limited_response(send, decision)
            return

        async def send_wrapper(message: dict[str, Any]) -> None:
            if message.get("type") == "http.response.start":
                headers = list(message.get("headers") or [])
                headers = _upsert_header(headers, RATE_LIMIT_LIMIT_HEADER, str(decision.limit).encode("latin-1"))
                headers = _upsert_header(
                    headers,
                    RATE_LIMIT_REMAINING_HEADER,
                    str(decision.remaining).encode("latin-1"),
                )
                headers = _upsert_header(
                    headers,
                    RATE_LIMIT_RESET_HEADER,
                    str(decision.reset_after_seconds).encode("latin-1"),
                )
                message["headers"] = headers
            await send(message)

        await self.app(scope, receive, send_wrapper)


async def _send_rate_limited_response(send: Callable[..., Any], decision: RateLimitDecision) -> None:
    payload = json.dumps({"detail": RATE_LIMIT_EXCEEDED_DETAIL}).encode("utf-8")
    headers = [
        (b"content-type", b"application/json"),
        (RATE_LIMIT_LIMIT_HEADER, str(decision.limit).encode("latin-1")),
        (RATE_LIMIT_REMAINING_HEADER, b"0"),
        (RATE_LIMIT_RESET_HEADER, str(decision.reset_after_seconds).encode("latin-1")),
    ]
    if decision.retry_after_seconds is not None:
        headers.append((RETRY_AFTER_HEADER, str(decision.retry_after_seconds).encode("latin-1")))

    await send({"type": "http.response.start", "status": 429, "headers": headers})
    await send({"type": "http.response.body", "body": payload, "more_body": False})


def _build_identity(scope: dict[str, Any]) -> str:
    api_key = _get_header(scope, _API_KEY_HEADER)
    if api_key:
        return f"api-key:{hashlib.sha256(api_key.encode('utf-8')).hexdigest()}"

    client = scope.get("client")
    if isinstance(client, tuple) and client and client[0]:
        return f"ip:{client[0]}"

    return "anonymous"


def _get_header(scope: dict[str, Any], header_name: bytes) -> str | None:
    for key, value in scope.get("headers") or []:
        if key.lower() == header_name:
            return value.decode("latin-1")
    return None


def _is_exempt_request(scope: dict[str, Any], exempt_path_prefixes: tuple[str, ...]) -> bool:
    method = str(scope.get("method") or "").upper()
    if method == "OPTIONS":
        return True
    path = str(scope.get("path") or "")
    return any(path.startswith(prefix) for prefix in exempt_path_prefixes)


def _upsert_header(headers: list[tuple[bytes, bytes]], name: bytes, value: bytes) -> list[tuple[bytes, bytes]]:
    filtered_headers = [header for header in headers if header[0].lower() != name]
    filtered_headers.append((name, value))
    return filtered_headers
