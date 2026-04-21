from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from data_platform.secrets import (
    SecretConfigurationError,
    collect_secret_configuration_report,
    collect_secret_settings_overrides,
    secret_configuration_exit_code,
)


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_collect_secret_settings_overrides_reads_file_and_secrets_dir(tmp_path: Path) -> None:
    postgres_secret = tmp_path / "postgres-password.txt"
    postgres_secret.write_text("hunter2\n", encoding="utf-8")

    secrets_dir = tmp_path / "secrets"
    secrets_dir.mkdir()
    (secrets_dir / "S3_SECRET_KEY").write_text("minio-secret\n", encoding="utf-8")

    overrides = collect_secret_settings_overrides(
        {
            "POSTGRES_PASSWORD_FILE": str(postgres_secret),
            "SECRETS_DIR": str(secrets_dir),
        }
    )

    assert overrides == {
        "POSTGRES_PASSWORD": "hunter2",
        "S3_SECRET_KEY": "minio-secret",
    }



def test_collect_secret_settings_overrides_rejects_ambiguous_sources() -> None:
    with pytest.raises(SecretConfigurationError) as excinfo:
        collect_secret_settings_overrides(
            {
                "POSTGRES_PASSWORD": "direct-value",
                "POSTGRES_PASSWORD_FILE": "/tmp/secret.txt",
            }
        )

    assert "Both POSTGRES_PASSWORD and POSTGRES_PASSWORD_FILE are set" in str(excinfo.value)



def test_collect_secret_configuration_report_flags_invalid_secrets_dir_and_production_defaults(tmp_path: Path) -> None:
    missing_dir = tmp_path / "missing-secrets"

    report = collect_secret_configuration_report(
        {
            "APP_ENV": "production",
            "SECRETS_DIR": str(missing_dir),
        }
    )

    assert report.app_env == "production"
    assert report.structural_errors == [
        f"SECRETS_DIR does not exist: {missing_dir.resolve()}"
    ]
    assert any(
        item == "POSTGRES_PASSWORD falls back to the repository default secret in APP_ENV=production."
        for item in report.policy_errors
    )
    assert secret_configuration_exit_code(report) == 1



def test_secrets_cli_json_output_with_file_backed_secret(tmp_path: Path) -> None:
    clickhouse_secret = tmp_path / "clickhouse-password.txt"
    clickhouse_secret.write_text("supersecret\n", encoding="utf-8")

    completed = subprocess.run(
        [sys.executable, "-m", "data_platform.secrets", "--json"],
        check=False,
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "src"),
            "APP_ENV": "development",
            "CLICKHOUSE_PASSWORD_FILE": str(clickhouse_secret),
        },
    )

    payload = json.loads(completed.stdout)
    assert completed.returncode == 0
    assert payload["structural_errors"] == []
    assert payload["policy_errors"] == []
    clickhouse_entry = next(item for item in payload["secrets"] if item["name"] == "CLICKHOUSE_PASSWORD")
    assert clickhouse_entry["source"] == "file"
    assert clickhouse_entry["resolved"] is True
    assert clickhouse_entry["path"] == str(clickhouse_secret.resolve())
