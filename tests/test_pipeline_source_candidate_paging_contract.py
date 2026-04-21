from __future__ import annotations

import base64
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from data_platform.utils.pipeline_definitions import (
    assert_pipeline_source_candidate_page_cursor_matches_scope,
    decode_pipeline_source_candidate_page_cursor,
    decode_pipeline_source_candidate_page_cursor_position,
    encode_pipeline_source_candidate_page_cursor,
)


def _decode_payload(cursor: str) -> dict[str, object]:
    padded = cursor + ("=" * (-len(cursor) % 4))
    return json.loads(base64.urlsafe_b64decode(padded).decode("utf-8"))


def test_pipeline_source_candidate_page_cursor_round_trips() -> None:
    effective_finished_at = datetime(2026, 4, 14, 12, 30, tzinfo=timezone.utc)
    created_at = datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc)
    source_finished_at_gte = datetime(2026, 4, 1, 0, 0, tzinfo=timezone.utc)
    source_finished_at_lte = datetime(2026, 4, 30, 23, 59, tzinfo=timezone.utc)

    cursor = encode_pipeline_source_candidate_page_cursor(
        effective_finished_at=effective_finished_at,
        created_at=created_at,
        ingestion_job_id="job-123",
        pipeline_id="pipe-123",
        source_finished_at_gte=source_finished_at_gte,
        source_finished_at_lte=source_finished_at_lte,
        run_ref_prefix="nightly",
        require_contract_compatible_schema=True,
        exclude_existing_runs=True,
        has_existing_run=None,
    )
    decoded_effective_finished_at, decoded_ingestion_job_id = decode_pipeline_source_candidate_page_cursor(cursor)
    position_effective_finished_at, position_created_at, position_ingestion_job_id = (
        decode_pipeline_source_candidate_page_cursor_position(cursor)
    )
    payload = _decode_payload(cursor)

    assert decoded_effective_finished_at == effective_finished_at
    assert decoded_ingestion_job_id == "job-123"
    assert position_effective_finished_at == effective_finished_at
    assert position_created_at == created_at
    assert position_ingestion_job_id == "job-123"
    assert payload["v"] == 5
    assert payload["scope"]["run_ref_prefix"] == "nightly"
    assert payload["scope"]["require_contract_compatible_schema"] is True
    assert "=" not in cursor
    assert_pipeline_source_candidate_page_cursor_matches_scope(
        cursor,
        pipeline_id="pipe-123",
        source_finished_at_gte=source_finished_at_gte,
        source_finished_at_lte=source_finished_at_lte,
        run_ref_prefix="nightly",
        require_contract_compatible_schema=True,
        exclude_existing_runs=True,
        has_existing_run=None,
    )


def test_pipeline_source_candidate_page_cursor_preserves_legacy_v2_cursor_compatibility() -> None:
    effective_finished_at = datetime(2026, 4, 14, 12, 30, tzinfo=timezone.utc)
    legacy_payload = {
        "v": 2,
        "effective_finished_at": effective_finished_at.isoformat(),
        "ingestion_job_id": "job-legacy",
        "scope": {
            "pipeline_id": "pipe-123",
            "source_finished_at_gte": None,
            "source_finished_at_lte": None,
            "exclude_existing_runs": False,
            "has_existing_run": None,
        },
    }
    cursor = base64.urlsafe_b64encode(
        json.dumps(legacy_payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).decode("ascii").rstrip("=")

    position_effective_finished_at, position_created_at, position_ingestion_job_id = (
        decode_pipeline_source_candidate_page_cursor_position(cursor)
    )

    assert position_effective_finished_at == effective_finished_at
    assert position_created_at is None
    assert position_ingestion_job_id == "job-legacy"
    assert_pipeline_source_candidate_page_cursor_matches_scope(cursor, pipeline_id="pipe-123")


def test_pipeline_source_candidate_page_cursor_preserves_legacy_v4_cursor_compatibility_without_strict_scope() -> None:
    effective_finished_at = datetime(2026, 4, 14, 12, 30, tzinfo=timezone.utc)
    created_at = datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc)
    legacy_payload = {
        "v": 4,
        "effective_finished_at": effective_finished_at.isoformat(),
        "created_at": created_at.isoformat(),
        "ingestion_job_id": "job-legacy-v4",
        "scope": {
            "pipeline_id": "pipe-123",
            "source_finished_at_gte": None,
            "source_finished_at_lte": None,
            "run_ref_prefix": "nightly",
            "exclude_existing_runs": False,
            "has_existing_run": None,
        },
    }
    cursor = base64.urlsafe_b64encode(
        json.dumps(legacy_payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).decode("ascii").rstrip("=")

    position_effective_finished_at, position_created_at, position_ingestion_job_id = (
        decode_pipeline_source_candidate_page_cursor_position(cursor)
    )

    assert position_effective_finished_at == effective_finished_at
    assert position_created_at == created_at
    assert position_ingestion_job_id == "job-legacy-v4"
    assert_pipeline_source_candidate_page_cursor_matches_scope(
        cursor,
        pipeline_id="pipe-123",
        run_ref_prefix="nightly",
        require_contract_compatible_schema=False,
    )


def test_pipeline_source_candidate_page_cursor_rejects_invalid_payload() -> None:
    with pytest.raises(ValueError, match="cursor is invalid."):
        decode_pipeline_source_candidate_page_cursor("not-a-valid-cursor")


def test_pipeline_source_candidate_page_cursor_rejects_scope_mismatch() -> None:
    effective_finished_at = datetime(2026, 4, 14, 12, 30, tzinfo=timezone.utc)

    cursor = encode_pipeline_source_candidate_page_cursor(
        effective_finished_at=effective_finished_at,
        ingestion_job_id="job-123",
        pipeline_id="pipe-123",
        run_ref_prefix="nightly",
        require_contract_compatible_schema=True,
        exclude_existing_runs=True,
    )

    with pytest.raises(ValueError, match="cursor does not match the current pipeline source-candidate selection."):
        assert_pipeline_source_candidate_page_cursor_matches_scope(
            cursor,
            pipeline_id="pipe-456",
            run_ref_prefix="nightly",
            require_contract_compatible_schema=True,
            exclude_existing_runs=True,
        )

    with pytest.raises(ValueError, match="cursor does not match the current pipeline source-candidate selection."):
        assert_pipeline_source_candidate_page_cursor_matches_scope(
            cursor,
            pipeline_id="pipe-123",
            run_ref_prefix="nightly",
            require_contract_compatible_schema=True,
            exclude_existing_runs=False,
        )

    with pytest.raises(ValueError, match="cursor does not match the current pipeline source-candidate selection."):
        assert_pipeline_source_candidate_page_cursor_matches_scope(
            cursor,
            pipeline_id="pipe-123",
            run_ref_prefix="adhoc",
            require_contract_compatible_schema=True,
            exclude_existing_runs=True,
        )

    with pytest.raises(ValueError, match="cursor does not match the current pipeline source-candidate selection."):
        assert_pipeline_source_candidate_page_cursor_matches_scope(
            cursor,
            pipeline_id="pipe-123",
            run_ref_prefix="nightly",
            require_contract_compatible_schema=False,
            exclude_existing_runs=True,
        )


def test_pipeline_source_candidate_page_response_schema_is_exposed() -> None:
    text = Path("src/data_platform/schemas/pipeline.py").read_text()

    assert "class PipelineSourceCandidatePageResponse" in text
    assert "items: list[PipelineSourceCandidateResponse] = Field(default_factory=list)" in text
    assert "next_cursor: str | None = None" in text


def test_pipeline_source_candidates_page_route_is_exposed() -> None:
    text = Path("src/data_platform/api/routes/pipelines.py").read_text()

    assert '"/pipelines/{pipeline_id}/source-candidates/page"' in text
    assert "PipelineSourceCandidatePageResponse" in text
    assert "cursor: str | None = Query(default=None)" in text
    assert "PipelineService.list_pipeline_source_candidates_page(" in text


def test_pipeline_service_supports_checkpoint_paged_source_candidates() -> None:
    text = Path("src/data_platform/services/pipeline_service.py").read_text()

    assert "def list_pipeline_source_candidates_page" in text
    assert "assert_pipeline_source_candidate_page_cursor_matches_scope(" in text
    assert "decode_pipeline_source_candidate_page_cursor_position(cursor)" in text
    assert "cursor_job = session.get(IngestionJob, cursor_ingestion_job_id)" in text
    assert "IngestionJob.created_at < cursor_created_at" in text
    assert "IngestionJob.created_at == cursor_created_at" in text
    assert ".order_by(desc(effective_finished_at), desc(IngestionJob.created_at), desc(IngestionJob.id))" in text
    assert ".limit(limit + 1)" in text
    assert "encode_pipeline_source_candidate_page_cursor(" in text
    assert "run_ref_prefix=normalized_run_ref_prefix" in text
    assert "require_contract_compatible_schema=require_contract_compatible_schema" in text
    assert "created_at=last_job.created_at" in text
    assert "pipeline_id=pipeline.id" in text
    assert "exclude_existing_runs=exclude_existing_runs" in text


def test_readme_documents_checkpoint_paged_source_candidates() -> None:
    text = Path("README.md").read_text()

    assert "GET /v1/pipelines/{pipeline_id}/source-candidates/page" in text
    assert "opaque `next_cursor`" in text
    assert "without offset drift" in text
    assert "bound to the same pipeline, source-candidate selection filters, and requested `run_ref_prefix`" in text
    assert "same newest-first ordering" in text
