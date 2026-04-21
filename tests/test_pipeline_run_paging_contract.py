from __future__ import annotations

import base64
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from data_platform.enums import PipelineStatus
from data_platform.utils.pipeline_definitions import (
    assert_pipeline_run_page_cursor_matches_scope,
    decode_pipeline_run_page_cursor,
    encode_pipeline_run_page_cursor,
    normalize_optional_pipeline_status,
)


def test_pipeline_run_page_cursor_round_trips_with_scope() -> None:
    created_at = datetime(2026, 4, 15, 14, 20, tzinfo=timezone.utc)
    created_at_gte = datetime(2026, 4, 1, 0, 0, tzinfo=timezone.utc)
    created_at_lte = datetime(2026, 4, 30, 23, 59, tzinfo=timezone.utc)

    cursor = encode_pipeline_run_page_cursor(
        created_at=created_at,
        run_id="run-123",
        pipeline_id="pipe-123",
        run_status=PipelineStatus.PLANNED,
        run_ref="nightly",
        source_ingestion_job_id="job-123",
        created_at_gte=created_at_gte,
        created_at_lte=created_at_lte,
    )
    decoded_created_at, decoded_run_id = decode_pipeline_run_page_cursor(cursor)

    assert decoded_created_at == created_at
    assert decoded_run_id == "run-123"
    assert "=" not in cursor
    assert_pipeline_run_page_cursor_matches_scope(
        cursor,
        pipeline_id="pipe-123",
        run_status="planned",
        run_ref="nightly",
        source_ingestion_job_id="job-123",
        created_at_gte=created_at_gte,
        created_at_lte=created_at_lte,
    )


def test_pipeline_run_page_cursor_preserves_legacy_cursor_compatibility() -> None:
    created_at = datetime(2026, 4, 15, 14, 20, tzinfo=timezone.utc)
    legacy_payload = {
        "v": 1,
        "created_at": created_at.isoformat(),
        "run_id": "run-legacy",
    }
    cursor = base64.urlsafe_b64encode(
        json.dumps(legacy_payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).decode("ascii").rstrip("=")

    decoded_created_at, decoded_run_id = decode_pipeline_run_page_cursor(cursor)

    assert decoded_created_at == created_at
    assert decoded_run_id == "run-legacy"
    assert_pipeline_run_page_cursor_matches_scope(cursor, pipeline_id="pipe-123")


def test_pipeline_run_page_cursor_rejects_invalid_payload() -> None:
    with pytest.raises(ValueError, match="cursor is invalid."):
        decode_pipeline_run_page_cursor("not-a-valid-cursor")


def test_pipeline_run_page_cursor_rejects_scope_mismatch() -> None:
    created_at = datetime(2026, 4, 15, 14, 20, tzinfo=timezone.utc)

    cursor = encode_pipeline_run_page_cursor(
        created_at=created_at,
        run_id="run-123",
        pipeline_id="pipe-123",
        run_status=PipelineStatus.PLANNED,
        run_ref="nightly",
        source_ingestion_job_id="job-123",
    )

    with pytest.raises(ValueError, match="cursor does not match the current pipeline run selection."):
        assert_pipeline_run_page_cursor_matches_scope(
            cursor,
            pipeline_id="pipe-456",
            run_status=PipelineStatus.PLANNED,
            run_ref="nightly",
            source_ingestion_job_id="job-123",
        )

    with pytest.raises(ValueError, match="cursor does not match the current pipeline run selection."):
        assert_pipeline_run_page_cursor_matches_scope(
            cursor,
            pipeline_id="pipe-123",
            run_status=PipelineStatus.RUNNING,
            run_ref="nightly",
            source_ingestion_job_id="job-123",
        )


def test_pipeline_run_status_normalization_rejects_invalid_values() -> None:
    with pytest.raises(ValueError, match="status must be one of: planned, blocked, pending, running, succeeded, failed."):
        normalize_optional_pipeline_status("bogus")

    with pytest.raises(ValueError, match="status cannot be empty."):
        normalize_optional_pipeline_status("   ")


def test_pipeline_run_page_response_schema_is_exposed() -> None:
    text = Path("src/data_platform/schemas/pipeline.py").read_text()

    assert "class PipelineRunPageResponse" in text
    assert "items: list[PipelineRunResponse] = Field(default_factory=list)" in text
    assert "next_cursor: str | None = None" in text


def test_pipeline_runs_page_route_is_exposed() -> None:
    text = Path("src/data_platform/api/routes/pipelines.py").read_text()

    assert '"/pipelines/{pipeline_id}/runs/page"' in text
    assert "PipelineRunPageResponse" in text
    assert 'run_status: PipelineStatus | None = Query(default=None, alias="status")' in text
    assert "cursor: str | None = Query(default=None)" in text
    assert "PipelineService.list_pipeline_runs_page(" in text


def test_pipeline_service_supports_checkpoint_paged_run_listing() -> None:
    text = Path("src/data_platform/services/pipeline_service.py").read_text()

    assert "def _apply_pipeline_run_filters" in text
    assert "def list_pipeline_runs_page" in text
    assert "run_status=run_status" in text
    assert "run_ref=run_ref" in text
    assert "source_ingestion_job_id=source_ingestion_job_id" in text
    assert "created_at_gte=created_at_gte" in text
    assert "created_at_lte=created_at_lte" in text
    assert "normalize_optional_pipeline_status(run_status)" in text
    assert "assert_pipeline_run_page_cursor_matches_scope(" in text
    assert "decode_pipeline_run_page_cursor(cursor)" in text
    assert "PipelineRun.created_at < cursor_created_at" in text
    assert "PipelineRun.id < cursor_run_id" in text
    assert "query.order_by(PipelineRun.created_at.desc(), PipelineRun.id.desc())" in text
    assert ").limit(limit + 1)" in text
    assert "encode_pipeline_run_page_cursor(" in text
    assert "pipeline_id=pipeline_id" in text


def test_readme_documents_checkpoint_paged_pipeline_runs() -> None:
    text = Path("README.md").read_text()

    assert "GET /v1/pipelines/{pipeline_id}/runs/count" in text
    assert "GET /v1/pipelines/{pipeline_id}/runs/page" in text
    assert "opaque `next_cursor`" in text
    assert "without offset drift" in text
    assert "bound to the same pipeline and pipeline-run filters" in text
