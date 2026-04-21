from __future__ import annotations

import base64
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from data_platform.utils.pipeline_definitions import (
    assert_pipeline_preflight_attempt_page_cursor_matches_scope,
    decode_pipeline_preflight_attempt_page_cursor,
    encode_pipeline_preflight_attempt_page_cursor,
)


def test_pipeline_preflight_attempt_page_cursor_round_trips() -> None:
    created_at = datetime(2026, 4, 15, 10, 45, tzinfo=timezone.utc)
    created_at_gte = datetime(2026, 4, 1, 0, 0, tzinfo=timezone.utc)
    created_at_lte = datetime(2026, 4, 30, 23, 59, tzinfo=timezone.utc)

    cursor = encode_pipeline_preflight_attempt_page_cursor(
        created_at=created_at,
        preflight_attempt_id="attempt-123",
        pipeline_id="pipe-123",
        request_kind="run",
        run_ref="nightly",
        source_ingestion_job_id="job-123",
        created_at_gte=created_at_gte,
        created_at_lte=created_at_lte,
    )
    decoded_created_at, decoded_preflight_attempt_id = decode_pipeline_preflight_attempt_page_cursor(cursor)

    assert decoded_created_at == created_at
    assert decoded_preflight_attempt_id == "attempt-123"
    assert "=" not in cursor
    assert_pipeline_preflight_attempt_page_cursor_matches_scope(
        cursor,
        pipeline_id="pipe-123",
        request_kind="run",
        run_ref="nightly",
        source_ingestion_job_id="job-123",
        created_at_gte=created_at_gte,
        created_at_lte=created_at_lte,
    )


def test_pipeline_preflight_attempt_page_cursor_preserves_legacy_cursor_compatibility() -> None:
    created_at = datetime(2026, 4, 15, 10, 45, tzinfo=timezone.utc)
    legacy_payload = {
        "v": 1,
        "created_at": created_at.isoformat(),
        "preflight_attempt_id": "attempt-legacy",
    }
    cursor = base64.urlsafe_b64encode(
        json.dumps(legacy_payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).decode("ascii").rstrip("=")

    decoded_created_at, decoded_preflight_attempt_id = decode_pipeline_preflight_attempt_page_cursor(cursor)

    assert decoded_created_at == created_at
    assert decoded_preflight_attempt_id == "attempt-legacy"
    assert_pipeline_preflight_attempt_page_cursor_matches_scope(cursor, pipeline_id="pipe-123")


def test_pipeline_preflight_attempt_page_cursor_rejects_invalid_payload() -> None:
    with pytest.raises(ValueError, match="cursor is invalid."):
        decode_pipeline_preflight_attempt_page_cursor("not-a-valid-cursor")


def test_pipeline_preflight_attempt_page_cursor_rejects_scope_mismatch() -> None:
    created_at = datetime(2026, 4, 15, 10, 45, tzinfo=timezone.utc)

    cursor = encode_pipeline_preflight_attempt_page_cursor(
        created_at=created_at,
        preflight_attempt_id="attempt-123",
        pipeline_id="pipe-123",
        request_kind="run",
        run_ref="nightly",
        source_ingestion_job_id="job-123",
    )

    with pytest.raises(ValueError, match="cursor does not match the current pipeline preflight-attempt selection."):
        assert_pipeline_preflight_attempt_page_cursor_matches_scope(
            cursor,
            pipeline_id="pipe-456",
            request_kind="run",
            run_ref="nightly",
            source_ingestion_job_id="job-123",
        )

    with pytest.raises(ValueError, match="cursor does not match the current pipeline preflight-attempt selection."):
        assert_pipeline_preflight_attempt_page_cursor_matches_scope(
            cursor,
            pipeline_id="pipe-123",
            request_kind="backfill",
            run_ref="nightly",
            source_ingestion_job_id="job-123",
        )


def test_pipeline_preflight_attempt_page_response_schema_is_exposed() -> None:
    text = Path("src/data_platform/schemas/pipeline.py").read_text()

    assert "class PipelinePreflightAttemptPageResponse" in text
    assert "items: list[PipelinePreflightAttemptResponse] = Field(default_factory=list)" in text
    assert "next_cursor: str | None = None" in text


def test_pipeline_preflight_attempts_page_route_is_exposed() -> None:
    text = Path("src/data_platform/api/routes/pipelines.py").read_text()

    assert '"/pipelines/{pipeline_id}/preflight-attempts/page"' in text
    assert "PipelinePreflightAttemptPageResponse" in text
    assert "request_kind: str | None = Query(default=None)" in text
    assert "source_ingestion_job_id: str | None = Query(default=None)" in text
    assert "cursor: str | None = Query(default=None)" in text
    assert "PipelineService.list_pipeline_preflight_attempts_page(" in text


def test_pipeline_service_supports_checkpoint_paged_preflight_attempts() -> None:
    text = Path("src/data_platform/services/pipeline_service.py").read_text()

    assert "def _apply_pipeline_preflight_attempt_filters" in text
    assert "def list_pipeline_preflight_attempts_page" in text
    assert "request_kind=request_kind" in text
    assert "run_ref=run_ref" in text
    assert "source_ingestion_job_id=source_ingestion_job_id" in text
    assert "created_at_gte=created_at_gte" in text
    assert "created_at_lte=created_at_lte" in text
    assert "assert_pipeline_preflight_attempt_page_cursor_matches_scope(" in text
    assert "decode_pipeline_preflight_attempt_page_cursor(cursor)" in text
    assert "PipelinePreflightAttempt.created_at < cursor_created_at" in text
    assert "PipelinePreflightAttempt.id < cursor_preflight_attempt_id" in text
    assert ").limit(limit + 1)" in text
    assert "encode_pipeline_preflight_attempt_page_cursor(" in text
    assert "pipeline_id=pipeline_id" in text


def test_readme_documents_checkpoint_paged_preflight_attempts() -> None:
    text = Path("README.md").read_text()

    assert "GET /v1/pipelines/{pipeline_id}/preflight-attempts/page" in text
    assert "GET /v1/pipelines/{pipeline_id}/preflight-attempts/count" in text
    assert "source_ingestion_job_id" in text
    assert "opaque `next_cursor`" in text
    assert "without offset drift" in text
    assert "bound to the same pipeline and preflight-attempt filters" in text
