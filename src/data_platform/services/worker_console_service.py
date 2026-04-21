from __future__ import annotations

import asyncio
from datetime import datetime
from typing import AsyncIterator

from sqlalchemy.orm import Session

from data_platform.services.audit_service import AuditService
from data_platform.utils.worker_console import (
    build_worker_console_entry,
    build_worker_console_heartbeat_sse,
    build_worker_console_sse_frame,
)


class WorkerConsoleService:
    def __init__(self, session: Session):
        self.session = session

    @staticmethod
    def _worker_path_prefix(resource_path: str) -> str:
        normalized = resource_path.strip()
        if not normalized:
            raise ValueError("resource_path is required.")
        return normalized

    def get_tail_cursor(self, *, resource_path: str) -> str | None:
        return AuditService.get_latest_event_cursor(
            self.session,
            method="WORKER",
            path_prefix=self._worker_path_prefix(resource_path),
        )

    def list_console_page(
        self,
        *,
        resource_path: str,
        cursor: str | None = None,
        limit: int = 100,
    ) -> dict[str, object]:
        page = AuditService.list_events_page(
            self.session,
            method="WORKER",
            path_prefix=self._worker_path_prefix(resource_path),
            cursor=cursor,
            limit=limit,
        )
        return {
            "items": [build_worker_console_entry(item) for item in page["items"]],
            "next_cursor": page.get("next_cursor"),
            "tail_cursor": self.get_tail_cursor(resource_path=resource_path),
        }

    async def stream_console(
        self,
        *,
        resource_path: str,
        cursor: str | None = None,
        poll_interval_seconds: int = 1,
        heartbeat_seconds: int = 10,
    ) -> AsyncIterator[str]:
        current_cursor = cursor or self.get_tail_cursor(resource_path=resource_path)
        last_activity = asyncio.get_running_loop().time()
        while True:
            if current_cursor is not None:
                forward = AuditService.list_events_forward(
                    self.session,
                    method="WORKER",
                    path_prefix=self._worker_path_prefix(resource_path),
                    cursor=current_cursor,
                    limit=100,
                )
                items = forward["items"]
                next_cursor = forward.get("next_cursor")
                if items:
                    for item in items:
                        yield build_worker_console_sse_frame(build_worker_console_entry(item))
                    if isinstance(next_cursor, str):
                        current_cursor = next_cursor
                    last_activity = asyncio.get_running_loop().time()
                elif asyncio.get_running_loop().time() - last_activity >= heartbeat_seconds:
                    yield build_worker_console_heartbeat_sse(cursor=current_cursor)
                    last_activity = asyncio.get_running_loop().time()
            else:
                current_cursor = self.get_tail_cursor(resource_path=resource_path)
                if current_cursor is None and asyncio.get_running_loop().time() - last_activity >= heartbeat_seconds:
                    yield build_worker_console_heartbeat_sse(cursor=None)
                    last_activity = asyncio.get_running_loop().time()
            await asyncio.sleep(max(1, poll_interval_seconds))
