from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

from data_platform.dependency_locks import collect_dependency_lock_report, dependency_lock_exit_code


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_collect_dependency_lock_report_accepts_current_repository_files() -> None:
    report = collect_dependency_lock_report(
        pyproject_path=REPO_ROOT / "pyproject.toml",
        runtime_lock_path=REPO_ROOT / "requirements-runtime.lock",
        dev_lock_path=REPO_ROOT / "requirements-dev.lock",
    )

    assert report.errors == []
    assert len(report.runtime_pins) == 23
    assert report.dev_pins == ["httpx==0.28.0", "pytest==8.3.0"]
    assert dependency_lock_exit_code(report) == 0



def test_collect_dependency_lock_report_rejects_missing_runtime_pin(tmp_path: Path) -> None:
    (tmp_path / "pyproject.toml").write_text(
        """
[project]
name = "demo"
version = "0.1.0"
dependencies = ["requests>=2.32", "orjson>=3.10"]

[project.optional-dependencies]
dev = ["pytest>=8.3"]
""".strip()
        + "\n",
        encoding="utf-8",
    )
    (tmp_path / "requirements-runtime.lock").write_text("requests==2.32.0\n", encoding="utf-8")
    (tmp_path / "requirements-dev.lock").write_text(
        "-r requirements-runtime.lock\npytest==8.3.0\n", encoding="utf-8"
    )

    report = collect_dependency_lock_report(
        pyproject_path=tmp_path / "pyproject.toml",
        runtime_lock_path=tmp_path / "requirements-runtime.lock",
        dev_lock_path=tmp_path / "requirements-dev.lock",
    )

    assert "Missing runtime pin for 'orjson' in the lock files." in report.errors
    assert dependency_lock_exit_code(report) == 1



def test_collect_dependency_lock_report_requires_runtime_include_and_exact_pins(tmp_path: Path) -> None:
    (tmp_path / "pyproject.toml").write_text(
        """
[project]
name = "demo"
version = "0.1.0"
dependencies = ["requests>=2.32"]

[project.optional-dependencies]
dev = ["pytest>=8.3"]
""".strip()
        + "\n",
        encoding="utf-8",
    )
    (tmp_path / "requirements-runtime.lock").write_text("requests>=2.32\n", encoding="utf-8")
    (tmp_path / "requirements-dev.lock").write_text("pytest==8.3.0\n", encoding="utf-8")

    report = collect_dependency_lock_report(
        pyproject_path=tmp_path / "pyproject.toml",
        runtime_lock_path=tmp_path / "requirements-runtime.lock",
        dev_lock_path=tmp_path / "requirements-dev.lock",
    )

    assert "requirements-runtime.lock:1 must use an exact '==' pin: requests>=2.32" in report.errors
    assert "requirements-dev.lock must include exactly one runtime lock reference: requirements-runtime.lock" in report.errors



def test_dependency_lock_cli_json_output() -> None:
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "data_platform.dependency_locks",
            "--pyproject",
            str(REPO_ROOT / "pyproject.toml"),
            "--runtime-lock",
            str(REPO_ROOT / "requirements-runtime.lock"),
            "--dev-lock",
            str(REPO_ROOT / "requirements-dev.lock"),
            "--json",
        ],
        check=False,
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
        env={**os.environ, "PYTHONPATH": str(REPO_ROOT / "src")},
    )

    payload = json.loads(completed.stdout)
    assert completed.returncode == 0
    assert payload["errors"] == []
    assert len(payload["runtime_pins"]) == 23
