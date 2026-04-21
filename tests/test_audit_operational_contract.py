from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _read(relative_path: str) -> str:
    return (ROOT / relative_path).read_text(encoding="utf-8")



def test_operational_audit_helpers_and_wiring_are_present() -> None:
    audit_source = _read("src/data_platform/audit_trail.py")
    processing_source = _read("src/data_platform/services/processing_service.py")
    pipeline_execution_source = _read("src/data_platform/services/pipeline_execution_service.py")
    metadata_cleanup_source = _read("src/data_platform/metadata_cleanup.py")
    worker_source = _read("src/data_platform/workers/tasks.py")

    assert 'AUDIT_WORKER_EVENT_TYPE = "worker_event"' in audit_source
    assert 'AUDIT_MAINTENANCE_EVENT_TYPE = "maintenance_event"' in audit_source
    assert 'AUDIT_WORKER_METHOD = "WORKER"' in audit_source
    assert 'AUDIT_MAINTENANCE_METHOD = "CLI"' in audit_source
    assert 'def build_operational_audit_event(' in audit_source
    assert 'def build_system_audit_event(' in audit_source
    assert 'path=f"/worker/ingestion-jobs/{job.id}"' in processing_source
    assert 'path=f"/worker/pipeline-runs/{run.id}"' in pipeline_execution_source
    assert 'path="/cli/metadata-cleanup"' in metadata_cleanup_source
    assert '"ingestion_job.retrying" if will_retry else "ingestion_job.failed"' in worker_source



def test_readme_documents_operational_audit_review() -> None:
    readme_source = _read("README.md")

    assert 'control-plane and operational audit trail' in readme_source
    assert '`WORKER` and `CLI` methods' in readme_source
    assert '/worker/ingestion-jobs/{job_id}' in readme_source
    assert '/worker/pipeline-runs/{run_id}' in readme_source
    assert '/cli/metadata-cleanup' in readme_source
