from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable, Sequence


@dataclass(frozen=True)
class CheckSpec:
    name: str
    description: str
    command: tuple[str, ...]
    optional_programs: tuple[str, ...] = ()


@dataclass(frozen=True)
class CheckResult:
    name: str
    description: str
    status: str
    command: list[str]
    returncode: int | None
    stdout: str
    stderr: str
    reason: str | None = None


@dataclass(frozen=True)
class BaselineHealthReport:
    path: str
    checks: list[CheckResult]


class CommandRunner:
    def __init__(self, repo_path: str | Path) -> None:
        self.repo_path = Path(repo_path).resolve()

    def run(self, command: Sequence[str]) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            list(command),
            cwd=self.repo_path,
            capture_output=True,
            text=True,
            check=False,
        )


class UnknownCheckError(ValueError):
    """Raised when a requested baseline health check name is unknown."""


def select_checks(
    check_names: Iterable[str] | None,
    available_checks: Iterable[CheckSpec] = (),
    *,
    check_collection_name: str = "baseline health",
) -> tuple[CheckSpec, ...]:
    checks = tuple(available_checks) or DEFAULT_CHECKS
    if check_names is None:
        return checks

    selected_names = [name.strip() for name in check_names if name.strip()]
    if not selected_names:
        return checks

    checks_by_name = {check.name: check for check in checks}
    unknown = [name for name in selected_names if name not in checks_by_name]
    if unknown:
        available = ", ".join(sorted(checks_by_name))
        raise UnknownCheckError(
            f"Unknown {check_collection_name} checks: {', '.join(unknown)}. Available checks: {available}."
        )

    return tuple(checks_by_name[name] for name in selected_names)


DEFAULT_CHECKS: tuple[CheckSpec, ...] = (
    CheckSpec(
        name="secrets",
        description="Secret file configuration and non-development secret hygiene.",
        command=(sys.executable, "-m", "data_platform.secrets"),
    ),
    CheckSpec(
        name="deps",
        description="Dependency lock files stay aligned with pyproject.toml.",
        command=(sys.executable, "-m", "data_platform.dependency_locks"),
    ),
    CheckSpec(
        name="build",
        description="Bytecode compilation for source and tests.",
        command=(sys.executable, "-m", "compileall", "src", "tests"),
    ),
    CheckSpec(
        name="test",
        description="Repository test suite.",
        command=("pytest", "-q"),
        optional_programs=("pytest",),
    ),
    CheckSpec(
        name="lint",
        description="Static linting with Ruff when available.",
        command=("ruff", "check", "src", "tests"),
        optional_programs=("ruff",),
    ),
    CheckSpec(
        name="typecheck",
        description="Type checking with mypy when available.",
        command=("mypy", "src"),
        optional_programs=("mypy",),
    ),
)


class BaselineHealthChecker:
    def __init__(self, repo_path: str | Path = ".", runner: CommandRunner | None = None) -> None:
        self.repo_path = Path(repo_path).resolve()
        self.runner = runner or CommandRunner(self.repo_path)

    def collect(self, checks: Iterable[CheckSpec] = DEFAULT_CHECKS) -> BaselineHealthReport:
        selected_checks = tuple(checks)
        path_error_reason = get_repo_path_error_reason(self.repo_path)
        if path_error_reason is not None:
            return BaselineHealthReport(
                path=str(self.repo_path),
                checks=[self._build_path_error_result(check, path_error_reason) for check in selected_checks],
            )

        results = [self._run_check(check) for check in selected_checks]
        return BaselineHealthReport(path=str(self.repo_path), checks=results)

    @staticmethod
    def _build_path_error_result(check: CheckSpec, reason: str) -> CheckResult:
        return CheckResult(
            name=check.name,
            description=check.description,
            status="failed",
            command=list(check.command),
            returncode=None,
            stdout="",
            stderr="",
            reason=reason,
        )

    def _run_check(self, check: CheckSpec) -> CheckResult:
        missing = [program for program in check.optional_programs if shutil.which(program) is None]
        if missing:
            return CheckResult(
                name=check.name,
                description=check.description,
                status="skipped",
                command=list(check.command),
                returncode=None,
                stdout="",
                stderr="",
                reason=f"Required tool not available: {', '.join(missing)}",
            )

        completed = self.runner.run(check.command)
        return CheckResult(
            name=check.name,
            description=check.description,
            status="passed" if completed.returncode == 0 else "failed",
            command=list(check.command),
            returncode=completed.returncode,
            stdout=(completed.stdout or "").strip(),
            stderr=(completed.stderr or "").strip(),
            reason=None,
        )


def collect_baseline_health(
    repo_path: str | Path = ".",
    check_names: Iterable[str] | None = None,
) -> BaselineHealthReport:
    checker = BaselineHealthChecker(repo_path)
    return checker.collect(select_checks(check_names))


def get_repo_path_error_reason(repo_path: str | Path) -> str | None:
    resolved_path = Path(repo_path).resolve()
    if not resolved_path.exists():
        return f"Repository path does not exist: {resolved_path}"
    if not resolved_path.is_dir():
        return f"Repository path is not a directory: {resolved_path}"
    return None


def checks_have_failures(checks: Iterable[CheckResult]) -> bool:
    return any(check.status == "failed" for check in checks)


def baseline_health_exit_code(report: BaselineHealthReport) -> int:
    return 1 if checks_have_failures(report.checks) else 0


def render_baseline_health(report: BaselineHealthReport) -> str:
    lines = [f"Repository path: {report.path}", "Baseline health checks:"]
    for result in report.checks:
        lines.append(f"- {result.name}: {result.status}")
        lines.append(f"  Description: {result.description}")
        lines.append(f"  Command: {' '.join(result.command)}")
        if result.reason:
            lines.append(f"  Reason: {result.reason}")
        elif result.returncode is not None:
            lines.append(f"  Exit code: {result.returncode}")
        if result.stdout:
            lines.append("  Stdout:")
            lines.extend(f"    {line}" for line in result.stdout.splitlines())
        if result.stderr:
            lines.append("  Stderr:")
            lines.extend(f"    {line}" for line in result.stderr.splitlines())
    return "\n".join(lines)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run baseline repository health checks.")
    parser.add_argument(
        "--path",
        default=".",
        help="Repository path to inspect. Defaults to the current directory.",
    )
    parser.add_argument(
        "--checks",
        default="",
        help=(
            "Comma-separated check names to run. Defaults to all known checks. "
            f"Available: {', '.join(sorted(check.name for check in DEFAULT_CHECKS))}."
        ),
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit JSON instead of a human-readable summary.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    check_names = [item for item in args.checks.split(",")] if args.checks else None
    try:
        report = collect_baseline_health(args.path, check_names=check_names)
    except UnknownCheckError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    if args.json:
        print(json.dumps(asdict(report), indent=2))
    else:
        print(render_baseline_health(report))
    return baseline_health_exit_code(report)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
