from __future__ import annotations

import base64
import json
from datetime import datetime, timezone
from pathlib import Path

from data_platform.utils.pipeline_definitions import (
    assert_pipeline_source_candidate_page_cursor_matches_scope,
    decode_pipeline_source_candidate_page_cursor_position,
)


def test_pipeline_source_candidate_cursor_position_decoder_accepts_legacy_cursor_without_created_at() -> None:
    payload = {
        "v": 2,
        "effective_finished_at": datetime(2026, 4, 14, 12, 30, tzinfo=timezone.utc).isoformat(),
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
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).decode("ascii").rstrip("=")

    effective_finished_at, created_at, ingestion_job_id = decode_pipeline_source_candidate_page_cursor_position(cursor)

    assert effective_finished_at == datetime(2026, 4, 14, 12, 30, tzinfo=timezone.utc)
    assert created_at is None
    assert ingestion_job_id == "job-legacy"


def test_legacy_source_candidate_cursor_without_strict_scope_remains_readable() -> None:
    payload = {
        "v": 4,
        "effective_finished_at": datetime(2026, 4, 14, 12, 30, tzinfo=timezone.utc).isoformat(),
        "created_at": datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc).isoformat(),
        "ingestion_job_id": "job-legacy",
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
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).decode("ascii").rstrip("=")

    assert_pipeline_source_candidate_page_cursor_matches_scope(
        cursor,
        pipeline_id="pipe-123",
        run_ref_prefix="nightly",
        require_contract_compatible_schema=False,
    )


def test_source_candidate_selection_and_paging_share_stable_ordering_contract() -> None:
    text = Path("src/data_platform/services/pipeline_service.py").read_text()

    assert ".order_by(desc(effective_finished_at), desc(IngestionJob.created_at), desc(IngestionJob.id))" in text
    assert "cursor_created_at, cursor_ingestion_job_id" in text
    assert "IngestionJob.created_at < cursor_created_at" in text
    assert "IngestionJob.created_at == cursor_created_at" in text
    assert "require_contract_compatible_schema=require_contract_compatible_schema" in text


def test_source_candidate_cursor_encoder_uses_v5_scope_when_pipeline_context_is_present() -> None:
    text = Path("src/data_platform/utils/pipeline_definitions.py").read_text()

    assert 'payload["v"] = 5' in text
    assert 'payload["created_at"] = normalized_created_at.isoformat()' in text
    assert 'run_ref_prefix=run_ref_prefix' in text
    assert 'require_contract_compatible_schema=require_contract_compatible_schema' in text
    assert 'expected_versions=(1, 2, 3, 4, 5)' in text
