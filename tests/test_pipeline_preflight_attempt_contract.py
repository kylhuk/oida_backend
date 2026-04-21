from __future__ import annotations

from pathlib import Path


def test_pipeline_preflight_attempt_model_and_migration_are_declared() -> None:
    model_source = Path("src/data_platform/models/pipeline.py").read_text()
    migration_source = Path("src/data_platform/migrations/manager.py").read_text()

    assert 'preflight_attempts: Mapped[list["PipelinePreflightAttempt"]]' in model_source
    assert "class PipelinePreflightAttempt(TimestampMixin, Base):" in model_source
    assert "ix_pipeline_preflight_attempts_pipeline_created" in model_source
    assert "ix_pipeline_preflight_attempts_pipeline_ingestion_created" in model_source
    assert "def _add_pipeline_preflight_attempts_table" in migration_source
    assert 'name="0004_pipeline_preflight_attempts"' in migration_source
    assert "version=4" in migration_source


def test_pipeline_service_persists_counts_and_filters_rejected_preflight_attempts() -> None:
    service_source = Path("src/data_platform/services/pipeline_service.py").read_text()
    utils_source = Path("src/data_platform/utils/pipeline_definitions.py").read_text()

    assert "def persist_rejected_preflight_attempt" in service_source
    assert "def _apply_pipeline_preflight_attempt_filters" in service_source
    assert "def count_pipeline_preflight_attempts" in service_source
    assert "def list_pipeline_preflight_attempts" in service_source
    assert "def list_pipeline_preflight_attempts_page" in service_source
    assert "def build_pipeline_preflight_attempt_response" in service_source
    assert 'request_kind="run"' in service_source
    assert 'request_kind="backfill"' in service_source
    assert 'request_kind must be one of: run, backfill.' in service_source
    assert 'normalize_optional_run_ref(source_ingestion_job_id, field_name="source_ingestion_job_id")' in service_source
    assert 'build_pipeline_preflight_metrics(' in service_source
    assert 'def build_pipeline_preflight_metrics(' in utils_source


def test_pipeline_preflight_attempt_route_and_schema_are_exposed() -> None:
    route_source = Path("src/data_platform/api/routes/pipelines.py").read_text()
    schema_source = Path("src/data_platform/schemas/pipeline.py").read_text()
    readme_source = Path("README.md").read_text()

    assert '"/pipelines/{pipeline_id}/preflight-attempts/count"' in route_source
    assert '"/pipelines/{pipeline_id}/preflight-attempts"' in route_source
    assert '"/pipelines/{pipeline_id}/preflight-attempts/page"' in route_source
    assert "PipelinePreflightAttemptCountResponse" in route_source
    assert "PipelinePreflightAttemptPageResponse" in route_source
    assert "PipelinePreflightAttemptResponse" in route_source
    assert "request_kind: str | None = Query(default=None)" in route_source
    assert "source_ingestion_job_id: str | None = Query(default=None)" in route_source
    assert "class PipelinePreflightAttemptCountResponse(BaseModel):" in schema_source
    assert "class PipelinePreflightAttemptResponse(BaseModel):" in schema_source
    assert "request_kind: str" in schema_source
    assert "contract_compatibility_outcome: str | None = None" in schema_source
    assert "GET /v1/pipelines/{pipeline_id}/preflight-attempts" in readme_source
    assert "GET /v1/pipelines/{pipeline_id}/preflight-attempts/count" in readme_source
    assert "GET /v1/pipelines/{pipeline_id}/preflight-attempts/page" in readme_source
