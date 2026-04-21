from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable

from data_platform.baseline_health import (
    BaselineHealthChecker,
    CheckResult,
    CheckSpec,
    DEFAULT_CHECKS,
    UnknownCheckError,
    checks_have_failures,
    select_checks,
)


@dataclass(frozen=True)
class VerificationMatrix:
    path: str
    selected_checks: list[str]
    checks: list[CheckResult]


def collect_verification_matrix(
    repo_path: str | Path = ".",
    check_names: Iterable[str] | None = None,
) -> VerificationMatrix:
    checks = select_checks(
        check_names, DEFAULT_CHECKS, check_collection_name="verification matrix"
    )
    report = BaselineHealthChecker(repo_path).collect(checks)
    return VerificationMatrix(
        path=report.path,
        selected_checks=[check.name for check in checks],
        checks=report.checks,
    )


def verification_matrix_exit_code(matrix: VerificationMatrix) -> int:
    return 1 if checks_have_failures(matrix.checks) else 0


def render_verification_matrix(matrix: VerificationMatrix) -> str:
    lines = [
        f"Repository path: {matrix.path}",
        "Verification matrix:",
    ]

    name_width = max(len("Check"), *(len(check.name) for check in matrix.checks))
    status_width = max(len("Status"), *(len(check.status) for check in matrix.checks))
    command_width = max(
        len("Command"), *(len(" ".join(check.command)) for check in matrix.checks)
    )

    header = f"{'Check':<{name_width}}  {'Status':<{status_width}}  {'Command':<{command_width}}"
    divider = f"{'-' * name_width}  {'-' * status_width}  {'-' * command_width}"
    lines.append(header)
    lines.append(divider)

    for check in matrix.checks:
        lines.append(
            f"{check.name:<{name_width}}  {check.status:<{status_width}}  {' '.join(check.command):<{command_width}}"
        )
        if check.reason:
            lines.append(f"  reason: {check.reason}")
        elif check.returncode is not None:
            lines.append(f"  exit code: {check.returncode}")

    return "\n".join(lines)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a concise repository verification matrix.")
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
        help="Emit JSON instead of a human-readable matrix.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    check_names = [item for item in args.checks.split(",")] if args.checks else None
    try:
        matrix = collect_verification_matrix(args.path, check_names=check_names)
    except UnknownCheckError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    if args.json:
        print(json.dumps(asdict(matrix), indent=2))
    else:
        print(render_verification_matrix(matrix))
    return verification_matrix_exit_code(matrix)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
