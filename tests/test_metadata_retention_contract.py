from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_metadata_retention_is_wired_into_settings_cli_makefile_and_docs() -> None:
    settings_text = (REPO_ROOT / "src" / "data_platform" / "settings.py").read_text(encoding="utf-8")
    service_text = (REPO_ROOT / "src" / "data_platform" / "services" / "retention_service.py").read_text(
        encoding="utf-8"
    )
    cli_text = (REPO_ROOT / "src" / "data_platform" / "metadata_cleanup.py").read_text(encoding="utf-8")
    makefile_text = (REPO_ROOT / "Makefile").read_text(encoding="utf-8")
    env_text = (REPO_ROOT / ".env.example").read_text(encoding="utf-8")
    readme_text = (REPO_ROOT / "README.md").read_text(encoding="utf-8")

    assert 'retention_pipeline_run_days: int = Field(default=90, alias="RETENTION_PIPELINE_RUN_DAYS")' in settings_text
    assert 'retention_preflight_attempt_days: int = Field(default=30, alias="RETENTION_PREFLIGHT_ATTEMPT_DAYS")' in settings_text
    assert 'retention_quality_result_days: int = Field(default=90, alias="RETENTION_QUALITY_RESULT_DAYS")' in settings_text
    assert 'retention_ingestion_job_days: int = Field(default=180, alias="RETENTION_INGESTION_JOB_DAYS")' in settings_text

    assert "class MetadataRetentionService:" in service_text
    assert "def plan(" in service_text
    assert "def apply(" in service_text
    assert '"pipeline_runs": PipelineRun' in service_text
    assert '"pipeline_preflight_attempts": PipelinePreflightAttempt' in service_text
    assert '"quality_results": QualityResult' in service_text
    assert '"ingestion_jobs": IngestionJob' in service_text

    assert "def execute_metadata_cleanup(*, apply: bool, now=None)" in cli_text
    assert "build_metadata_retention_policies(" in cli_text
    assert "MetadataRetentionService.apply" in cli_text
    assert "MetadataRetentionService.plan" in cli_text

    assert "cleanup-metadata:" in makefile_text
    assert "python -m data_platform.metadata_cleanup" in makefile_text
    assert "cleanup-metadata-apply:" in makefile_text
    assert "python -m data_platform.metadata_cleanup --apply" in makefile_text

    assert "RETENTION_PIPELINE_RUN_DAYS=90" in env_text
    assert "RETENTION_PREFLIGHT_ATTEMPT_DAYS=30" in env_text
    assert "RETENTION_QUALITY_RESULT_DAYS=90" in env_text
    assert "RETENTION_INGESTION_JOB_DAYS=180" in env_text

    assert "configurable metadata retention and cleanup policies" in readme_text
    assert "## Metadata retention and cleanup" in readme_text
    assert "make cleanup-metadata" in readme_text
    assert "make cleanup-metadata-apply" in readme_text
