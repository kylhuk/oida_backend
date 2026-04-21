from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from data_platform.utils.pipeline_definitions import build_backfill_request_snapshot, extract_pipeline_run_snapshot


def test_extract_pipeline_run_snapshot_preserves_backfill_request_cursor() -> None:
    snapshot = build_backfill_request_snapshot(
        source_finished_at_gte=datetime(2026, 4, 14, 17, 0, tzinfo=timezone.utc),
        source_finished_at_lte=datetime(2026, 4, 14, 18, 30, tzinfo=timezone.utc),
        limit=25,
        offset=0,
        cursor="  cursor-token  ",
        run_ref_prefix=" nightly ",
        skip_existing_runs=True,
    )

    extracted = extract_pipeline_run_snapshot({"backfill_request": snapshot})["backfill_request"]

    assert extracted == {
        "run_ref_prefix": "nightly",
        "source_finished_at_gte": datetime(2026, 4, 14, 17, 0, tzinfo=timezone.utc),
        "source_finished_at_lte": datetime(2026, 4, 14, 18, 30, tzinfo=timezone.utc),
        "limit": 25,
        "offset": 0,
        "cursor": "cursor-token",
        "skip_existing_runs": True,
    }



def test_pipeline_backfill_request_response_schema_exposes_cursor_field() -> None:
    text = Path("src/data_platform/schemas/pipeline.py").read_text()

    assert "cursor: str | None = None" in text



def test_readme_documents_first_class_backfill_request_cursor() -> None:
    text = Path("README.md").read_text()

    assert "including any checkpoint cursor used for paged selection" in text
