from __future__ import annotations

import json
from datetime import datetime, timezone

from data_platform.metadata_cleanup import main
from data_platform.metadata_retention import (
    MetadataCleanupReport,
    MetadataCleanupTableResult,
    build_metadata_retention_policies,
    render_metadata_cleanup_report,
    retention_cutoff,
)


def test_build_metadata_retention_policies_skips_disabled_tables() -> None:
    policies = build_metadata_retention_policies(
        pipeline_run_days=90,
        preflight_attempt_days=0,
        quality_result_days=30,
        ingestion_job_days=-1,
    )

    assert [policy.name for policy in policies] == ["pipeline_runs", "quality_results"]


def test_retention_cutoff_uses_utc_for_naive_datetimes() -> None:
    now = datetime(2026, 4, 16, 12, 0, 0)
    cutoff = retention_cutoff(now, 10)

    assert cutoff == datetime(2026, 4, 6, 12, 0, 0, tzinfo=timezone.utc)


def test_render_metadata_cleanup_report_includes_mode_and_counts() -> None:
    report = MetadataCleanupReport(
        applied=False,
        generated_at="2026-04-16T12:00:00+00:00",
        tables=[
            MetadataCleanupTableResult(
                name="pipeline_runs",
                table_name="pipeline_runs",
                retention_days=90,
                cutoff="2026-01-17T12:00:00+00:00",
                matched_rows=12,
                deleted_rows=0,
                description="Pipeline run history older than the configured TTL.",
            )
        ],
    )

    rendered = render_metadata_cleanup_report(report)

    assert "Metadata retention preview generated at" in rendered
    assert "pipeline_runs" in rendered
    assert "Matched rows: 12" in rendered
    assert "Deleted rows: 0" in rendered


def test_metadata_cleanup_cli_emits_json_with_monkeypatched_executor(monkeypatch, capsys) -> None:
    expected = MetadataCleanupReport(
        applied=False,
        generated_at="2026-04-16T12:00:00+00:00",
        tables=[],
    )

    monkeypatch.setattr(
        "data_platform.metadata_cleanup.execute_metadata_cleanup",
        lambda *, apply, now=None: expected,
    )

    exit_code = main(["--json"])

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["applied"] is False
    assert payload["generated_at"] == "2026-04-16T12:00:00+00:00"
    assert payload["tables"] == []
