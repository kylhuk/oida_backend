from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from types import SimpleNamespace

from data_platform.baseline_health import (
    BaselineHealthChecker,
    BaselineHealthReport,
    CheckResult,
    CheckSpec,
    UnknownCheckError,
    baseline_health_exit_code,
    collect_baseline_health,
    main,
    render_baseline_health,
)


class FakeRunner:
    def __init__(self, completed_by_name: dict[str, subprocess.CompletedProcess[str]]) -> None:
        self.completed_by_name = completed_by_name

    def run(self, command: tuple[str, ...] | list[str]) -> subprocess.CompletedProcess[str]:
        return self.completed_by_name[command[0]]


def _completed(command: list[str], returncode: int, stdout: str = "", stderr: str = "") -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(command, returncode=returncode, stdout=stdout, stderr=stderr)


def test_collect_baseline_health_records_passed_and_failed_checks(tmp_path: Path) -> None:
    checks = (
        CheckSpec(name="build", description="build", command=("build",)),
        CheckSpec(name="test", description="test", command=("test",)),
    )
    runner = FakeRunner(
        {
            "build": _completed(["build"], 0, stdout="compiled"),
            "test": _completed(["test"], 2, stderr="boom"),
        }
    )

    report = BaselineHealthChecker(tmp_path, runner=runner).collect(checks)

    assert [result.status for result in report.checks] == ["passed", "failed"]
    assert report.checks[0].stdout == "compiled"
    assert report.checks[1].stderr == "boom"


def test_collect_baseline_health_marks_missing_optional_tool_as_skipped(tmp_path: Path, monkeypatch) -> None:
    checks = (
        CheckSpec(
            name="lint",
            description="lint",
            command=("ruff", "check", "src"),
            optional_programs=("ruff",),
        ),
    )
    monkeypatch.setattr("shutil.which", lambda name: None)

    report = BaselineHealthChecker(tmp_path, runner=FakeRunner({})).collect(checks)

    assert report.checks == [
        CheckResult(
            name="lint",
            description="lint",
            status="skipped",
            command=["ruff", "check", "src"],
            returncode=None,
            stdout="",
            stderr="",
            reason="Required tool not available: ruff",
        )
    ]


def test_render_baseline_health_human_output(tmp_path: Path) -> None:
    report = BaselineHealthChecker(
        tmp_path,
        runner=FakeRunner({"build": _completed(["build"], 0, stdout="ok")}),
    ).collect((CheckSpec(name="build", description="build", command=("build",)),))

    rendered = render_baseline_health(report)

    assert "Baseline health checks:" in rendered
    assert "- build: passed" in rendered
    assert "Stdout:" in rendered




def test_collect_baseline_health_runs_selected_checks(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        "data_platform.baseline_health.BaselineHealthChecker",
        lambda repo_path: type(
            "Checker",
            (),
            {
                "collect": lambda self, checks: type(
                    "Report",
                    (),
                    {
                        "path": str(tmp_path.resolve()),
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
                )(),
            },
        )(),
    )

    report = collect_baseline_health(tmp_path, check_names=["build", "test"])

    assert [item.name for item in report.checks] == ["build", "test"]


def test_collect_baseline_health_rejects_unknown_checks(tmp_path: Path) -> None:
    try:
        collect_baseline_health(tmp_path, check_names=["bogus"])
    except UnknownCheckError:
        return
    raise AssertionError("Expected UnknownCheckError")

def test_collect_baseline_health_marks_missing_repo_path_as_failed(tmp_path: Path) -> None:
    missing_path = tmp_path / "missing-repo"

    report = collect_baseline_health(missing_path, check_names=["build", "test"])

    assert [check.status for check in report.checks] == ["failed", "failed"]
    assert all(check.returncode is None for check in report.checks)
    assert all(check.reason == f"Repository path does not exist: {missing_path.resolve()}" for check in report.checks)



def test_collect_baseline_health_marks_non_directory_repo_path_as_failed(tmp_path: Path) -> None:
    file_path = tmp_path / "repo.txt"
    file_path.write_text("not a directory\n", encoding="utf-8")

    report = collect_baseline_health(file_path, check_names=["build"])

    assert report.checks == [
        CheckResult(
            name="build",
            description="Bytecode compilation for source and tests.",
            status="failed",
            command=[sys.executable, "-m", "compileall", "src", "tests"],
            returncode=None,
            stdout="",
            stderr="",
            reason=f"Repository path is not a directory: {file_path.resolve()}",
        )
    ]



def test_baseline_health_cli_json_output_for_missing_path(tmp_path: Path) -> None:
    missing_path = tmp_path / "missing-repo"

    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "data_platform.baseline_health",
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



def test_baseline_health_cli_json_output(tmp_path: Path) -> None:
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "data_platform.baseline_health",
            "--path",
            str(tmp_path),
            "--checks",
            "build",
            "--json",
        ],
        check=True,
        capture_output=True,
        text=True,
        cwd=tmp_path,
        env={**__import__("os").environ, "PYTHONPATH": str(Path(__file__).resolve().parents[1] / "src")},
    )

    payload = json.loads(completed.stdout)
    assert payload["path"] == str(tmp_path.resolve())
    assert [entry["name"] for entry in payload["checks"]] == ["build"]



def test_baseline_health_cli_rejects_unknown_checks_without_traceback(tmp_path: Path) -> None:
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "data_platform.baseline_health",
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
    assert "Unknown baseline health checks: bogus." in completed.stderr
    assert "Traceback" not in completed.stderr
    assert completed.stdout == ""



def test_baseline_health_exit_code_is_nonzero_when_any_check_fails() -> None:
    report = BaselineHealthReport(
        path="/tmp/repo",
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

    assert baseline_health_exit_code(report) == 1



def test_baseline_health_exit_code_ignores_skipped_checks() -> None:
    report = BaselineHealthReport(
        path="/tmp/repo",
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

    assert baseline_health_exit_code(report) == 0



def test_baseline_health_main_returns_nonzero_after_printing_json_for_failed_report(monkeypatch, capsys) -> None:
    report = BaselineHealthReport(
        path="/tmp/repo",
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
        "data_platform.baseline_health._parse_args",
        lambda: SimpleNamespace(path="/tmp/repo", checks="test", json=True),
    )
    monkeypatch.setattr(
        "data_platform.baseline_health.collect_baseline_health",
        lambda repo_path, check_names=None: report,
    )

    exit_code = main()
    output = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert output["checks"][0]["status"] == "failed"
