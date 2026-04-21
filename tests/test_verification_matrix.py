from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from data_platform.baseline_health import CheckResult
from types import SimpleNamespace

from data_platform.verification_matrix import (
    UnknownCheckError,
    VerificationMatrix,
    collect_verification_matrix,
    main,
    render_verification_matrix,
    verification_matrix_exit_code,
)


class FakeChecker:
    def __init__(self, checks: list[CheckResult]) -> None:
        self._checks = checks

    def collect(self, checks):
        class Report:
            def __init__(self, items):
                self.path = "/tmp/repo"
                self.checks = items

        return Report(self._checks)


class FakeBaselineHealthChecker:
    def __init__(self, repo_path: str | Path) -> None:
        self.repo_path = str(repo_path)

    def collect(self, checks):
        return type(
            "Report",
            (),
            {
                "path": self.repo_path,
                "checks": [
                    CheckResult(
                        name=check.name,
                        description=check.description,
                        status="passed",
                        command=list(check.command),
                        returncode=0,
                        stdout="",
                        stderr="",
                        reason=None,
                    )
                    for check in checks
                ],
            },
        )()


def test_collect_verification_matrix_runs_selected_checks(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        "data_platform.verification_matrix.BaselineHealthChecker",
        FakeBaselineHealthChecker,
    )

    matrix = collect_verification_matrix(tmp_path, check_names=["build", "test"])

    assert matrix.path == str(tmp_path)
    assert matrix.selected_checks == ["build", "test"]
    assert [item.name for item in matrix.checks] == ["build", "test"]


def test_collect_verification_matrix_rejects_unknown_checks(tmp_path: Path) -> None:
    with pytest.raises(UnknownCheckError):
        collect_verification_matrix(tmp_path, check_names=["bogus"])


def test_render_verification_matrix_human_output() -> None:
    matrix = type(
        "Matrix",
        (),
        {
            "path": "/tmp/repo",
            "selected_checks": ["build"],
            "checks": [
                CheckResult(
                    name="build",
                    description="build",
                    status="passed",
                    command=["python", "-m", "compileall", "src", "tests"],
                    returncode=0,
                    stdout="",
                    stderr="",
                    reason=None,
                )
            ],
        },
    )()

    rendered = render_verification_matrix(matrix)

    assert "Verification matrix:" in rendered
    assert "build" in rendered
    assert "passed" in rendered
    assert "python -m compileall src tests" in rendered


def test_verification_matrix_cli_json_output(tmp_path: Path) -> None:
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "data_platform.verification_matrix",
            "--path",
            str(tmp_path),
            "--checks",
            "build,test",
            "--json",
        ],
        check=False,
        capture_output=True,
        text=True,
        cwd=tmp_path,
        env={**__import__("os").environ, "PYTHONPATH": str(Path(__file__).resolve().parents[1] / "src")},
    )

    payload = json.loads(completed.stdout)
    assert completed.returncode == 1
    assert payload["path"] == str(tmp_path.resolve())
    assert payload["selected_checks"] == ["build", "test"]
    assert [entry["name"] for entry in payload["checks"]] == ["build", "test"]



def test_verification_matrix_cli_rejects_unknown_checks_without_traceback(tmp_path: Path) -> None:
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "data_platform.verification_matrix",
            "--path",
            str(tmp_path),
            "--checks",
            "bogus",
        ],
        check=False,
        capture_output=True,
        text=True,
        cwd=tmp_path,
        env={**__import__("os").environ, "PYTHONPATH": str(Path(__file__).resolve().parents[1] / "src")},
    )

    assert completed.returncode == 2
    assert "Unknown verification matrix checks: bogus." in completed.stderr
    assert "Traceback" not in completed.stderr
    assert completed.stdout == ""


def test_collect_verification_matrix_marks_missing_repo_path_as_failed(tmp_path: Path) -> None:
    missing_path = tmp_path / "missing-repo"

    matrix = collect_verification_matrix(missing_path, check_names=["build", "test"])

    assert [check.status for check in matrix.checks] == ["failed", "failed"]
    assert all(check.returncode is None for check in matrix.checks)
    assert all(check.reason == f"Repository path does not exist: {missing_path.resolve()}" for check in matrix.checks)



def test_verification_matrix_cli_json_output_for_missing_path(tmp_path: Path) -> None:
    missing_path = tmp_path / "missing-repo"

    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "data_platform.verification_matrix",
            "--path",
            str(missing_path),
            "--checks",
            "build",
            "--json",
        ],
        check=False,
        capture_output=True,
        text=True,
        cwd=tmp_path,
        env={**__import__("os").environ, "PYTHONPATH": str(Path(__file__).resolve().parents[1] / "src")},
    )

    payload = json.loads(completed.stdout)
    assert completed.returncode == 1
    assert payload["checks"][0]["status"] == "failed"
    assert payload["checks"][0]["reason"] == f"Repository path does not exist: {missing_path.resolve()}"



def test_verification_matrix_exit_code_is_nonzero_when_any_check_fails() -> None:
    matrix = VerificationMatrix(
        path="/tmp/repo",
        selected_checks=["build", "test"],
        checks=[
            CheckResult(
                name="build",
                description="build",
                status="passed",
                command=["python", "-m", "compileall", "src", "tests"],
                returncode=0,
                stdout="",
                stderr="",
                reason=None,
            ),
            CheckResult(
                name="test",
                description="test",
                status="failed",
                command=["pytest", "-q"],
                returncode=1,
                stdout="",
                stderr="boom",
                reason=None,
            ),
        ],
    )

    assert verification_matrix_exit_code(matrix) == 1



def test_verification_matrix_exit_code_ignores_skipped_checks() -> None:
    matrix = VerificationMatrix(
        path="/tmp/repo",
        selected_checks=["lint"],
        checks=[
            CheckResult(
                name="lint",
                description="lint",
                status="skipped",
                command=["ruff", "check", "src", "tests"],
                returncode=None,
                stdout="",
                stderr="",
                reason="Required tool not available: ruff",
            )
        ],
    )

    assert verification_matrix_exit_code(matrix) == 0



def test_verification_matrix_main_returns_nonzero_after_printing_json_for_failed_matrix(monkeypatch, capsys) -> None:
    matrix = VerificationMatrix(
        path="/tmp/repo",
        selected_checks=["test"],
        checks=[
            CheckResult(
                name="test",
                description="test",
                status="failed",
                command=["pytest", "-q"],
                returncode=1,
                stdout="",
                stderr="failure",
                reason=None,
            )
        ],
    )
    monkeypatch.setattr(
        "data_platform.verification_matrix._parse_args",
        lambda: SimpleNamespace(path="/tmp/repo", checks="test", json=True),
    )
    monkeypatch.setattr(
        "data_platform.verification_matrix.collect_verification_matrix",
        lambda repo_path, check_names=None: matrix,
    )

    exit_code = main()
    output = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert output["checks"][0]["status"] == "failed"
