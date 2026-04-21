from __future__ import annotations

from dataclasses import asdict
from typing import Sequence

from data_platform.audit_trail import (
    AUDIT_MAINTENANCE_EVENT_TYPE,
    AUDIT_MAINTENANCE_METHOD,
    build_operational_audit_event,
    persist_audit_event_with_session_factory,
)
from data_platform.metadata_retention import (
    MetadataCleanupReport,
    build_metadata_retention_policies,
    parse_metadata_cleanup_args,
    render_metadata_cleanup_report,
    serialize_metadata_cleanup_report,
)


def _audit_metadata_cleanup(report: MetadataCleanupReport) -> None:
    from data_platform.db import get_session_factory
    from data_platform.settings import get_settings

    settings = get_settings()
    if not settings.enable_audit_trail:
        return

    payload = build_operational_audit_event(
        AUDIT_MAINTENANCE_EVENT_TYPE,
        method=AUDIT_MAINTENANCE_METHOD,
        path="/cli/metadata-cleanup",
        status_code=200,
        details_json={
            "applied": report.applied,
            "generated_at": report.generated_at,
            "table_count": len(report.tables),
            "matched_rows": sum(item.matched_rows for item in report.tables),
            "deleted_rows": sum(item.deleted_rows for item in report.tables),
            "tables": [asdict(item) for item in report.tables],
        },
    )
    persist_audit_event_with_session_factory(get_session_factory(), payload)


def execute_metadata_cleanup(*, apply: bool, now=None) -> MetadataCleanupReport:
    from data_platform.db import get_session_factory
    from data_platform.services.retention_service import MetadataRetentionService
    from data_platform.settings import get_settings

    settings = get_settings()
    policies = build_metadata_retention_policies(
        pipeline_run_days=settings.retention_pipeline_run_days,
        preflight_attempt_days=settings.retention_preflight_attempt_days,
        quality_result_days=settings.retention_quality_result_days,
        ingestion_job_days=settings.retention_ingestion_job_days,
    )

    session = get_session_factory()()
    try:
        if apply:
            report = MetadataRetentionService.apply(session, policies, now=now)
        else:
            report = MetadataRetentionService.plan(session, policies, now=now)
    finally:
        session.close()
    _audit_metadata_cleanup(report)
    return report


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_metadata_cleanup_args(argv)
    report = execute_metadata_cleanup(apply=args.apply, now=args.now)
    if args.json:
        print(serialize_metadata_cleanup_report(report))
    else:
        print(render_metadata_cleanup_report(report))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
