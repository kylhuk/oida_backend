from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Sequence


@dataclass(frozen=True)
class MetadataRetentionPolicy:
    name: str
    table_name: str
    retention_days: int
    description: str


@dataclass(frozen=True)
class MetadataCleanupTableResult:
    name: str
    table_name: str
    retention_days: int
    cutoff: str
    matched_rows: int
    deleted_rows: int
    description: str


@dataclass(frozen=True)
class MetadataCleanupReport:
    applied: bool
    generated_at: str
    tables: list[MetadataCleanupTableResult]


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _ensure_aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def retention_cutoff(now: datetime, retention_days: int) -> datetime:
    return _ensure_aware(now) - timedelta(days=retention_days)


def build_metadata_retention_policies(
    *,
    pipeline_run_days: int,
    preflight_attempt_days: int,
    quality_result_days: int,
    ingestion_job_days: int,
) -> tuple[MetadataRetentionPolicy, ...]:
    policies: list[MetadataRetentionPolicy] = []

    def add(name: str, table_name: str, retention_days: int, description: str) -> None:
        if retention_days <= 0:
            return
        policies.append(
            MetadataRetentionPolicy(
                name=name,
                table_name=table_name,
                retention_days=retention_days,
                description=description,
            )
        )

    add(
        name="pipeline_runs",
        table_name="pipeline_runs",
        retention_days=pipeline_run_days,
        description="Pipeline run history older than the configured TTL.",
    )
    add(
        name="pipeline_preflight_attempts",
        table_name="pipeline_preflight_attempts",
        retention_days=preflight_attempt_days,
        description="Rejected and preview-only pipeline preflight attempts older than the configured TTL.",
    )
    add(
        name="quality_results",
        table_name="quality_results",
        retention_days=quality_result_days,
        description="Persisted quality-check outcomes older than the configured TTL.",
    )
    add(
        name="ingestion_jobs",
        table_name="ingestion_jobs",
        retention_days=ingestion_job_days,
        description="Historical ingestion metadata older than the configured TTL.",
    )
    return tuple(policies)


def render_metadata_cleanup_report(report: MetadataCleanupReport) -> str:
    mode = "apply" if report.applied else "preview"
    lines = [f"Metadata retention {mode} generated at: {report.generated_at}"]
    if not report.tables:
        lines.append("No enabled metadata retention policies.")
        return "\n".join(lines)

    for table in report.tables:
        lines.append(f"- {table.table_name}: retention={table.retention_days}d cutoff={table.cutoff}")
        lines.append(f"  Description: {table.description}")
        lines.append(f"  Matched rows: {table.matched_rows}")
        lines.append(f"  Deleted rows: {table.deleted_rows}")
    return "\n".join(lines)


@dataclass(frozen=True)
class MetadataCleanupCommandArgs:
    apply: bool
    json: bool
    now: datetime | None


def parse_metadata_cleanup_args(argv: Sequence[str] | None = None) -> MetadataCleanupCommandArgs:
    parser = argparse.ArgumentParser(description="Preview or apply metadata retention policies.")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Delete matching metadata instead of running a dry preview.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit JSON instead of a human-readable summary.",
    )
    parser.add_argument(
        "--now",
        default=None,
        help="Optional ISO-8601 timestamp to use as the reference time for cutoffs.",
    )
    parsed = parser.parse_args(argv)

    now: datetime | None = None
    if parsed.now:
        value = parsed.now.strip()
        if value.endswith("Z"):
            value = f"{value[:-1]}+00:00"
        now = datetime.fromisoformat(value)

    return MetadataCleanupCommandArgs(apply=parsed.apply, json=parsed.json, now=now)


def serialize_metadata_cleanup_report(report: MetadataCleanupReport) -> str:
    return json.dumps(asdict(report), indent=2)
