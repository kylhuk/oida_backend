from __future__ import annotations

import argparse
import json
import sys
import tomllib
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable


@dataclass(frozen=True)
class LockedRequirement:
    name: str
    subject: str
    version: str
    source_line: str


@dataclass(frozen=True)
class DependencyLockReport:
    pyproject_path: str
    runtime_lock_path: str
    dev_lock_path: str
    runtime_pins: list[str]
    dev_pins: list[str]
    errors: list[str]


_VERSION_OPERATORS: tuple[str, ...] = ("==", ">=", "<=", "~=", "!=", ">", "<")


def _normalize_requirement_name(value: str) -> str:
    subject = value.strip()
    if not subject:
        raise ValueError("Dependency subject cannot be empty.")
    name_chars: list[str] = []
    for char in subject:
        if char.isalnum() or char in {"-", "_", "."}:
            name_chars.append(char)
            continue
        break
    if not name_chars:
        raise ValueError(f"Unable to parse dependency name from {value!r}.")
    return "".join(name_chars).lower().replace("_", "-")


def _split_requirement_subject_and_spec(requirement: str) -> tuple[str, str | None]:
    text = requirement.split("#", 1)[0].strip()
    if not text:
        raise ValueError("Requirement line is empty.")
    if ";" in text:
        text = text.split(";", 1)[0].strip()
    for operator in _VERSION_OPERATORS:
        index = text.find(operator)
        if index != -1:
            return text[:index].strip(), text[index:].strip()
    return text.strip(), None


def _canonicalize_subject(subject: str) -> str:
    return subject.strip().lower().replace(" ", "")


def _load_pyproject_dependency_subjects(pyproject_path: str | Path) -> tuple[dict[str, str], dict[str, str]]:
    with Path(pyproject_path).open("rb") as handle:
        pyproject = tomllib.load(handle)

    project = pyproject.get("project", {})
    runtime_dependencies = project.get("dependencies", []) or []
    optional_dependencies = project.get("optional-dependencies", {}) or {}
    dev_dependencies = optional_dependencies.get("dev", []) or []

    def _collect_subjects(requirements: Iterable[str], label: str) -> dict[str, str]:
        subjects: dict[str, str] = {}
        for requirement in requirements:
            subject, _ = _split_requirement_subject_and_spec(requirement)
            name = _normalize_requirement_name(subject)
            if name in subjects:
                raise ValueError(f"Duplicate {label} dependency for {name!r} in pyproject.toml.")
            subjects[name] = subject
        return subjects

    return _collect_subjects(runtime_dependencies, "runtime"), _collect_subjects(dev_dependencies, "dev")


def _parse_lock_file(lock_path: str | Path) -> tuple[dict[str, LockedRequirement], list[str], list[str]]:
    path = Path(lock_path)
    pins: dict[str, LockedRequirement] = {}
    includes: list[str] = []
    errors: list[str] = []

    for line_number, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        stripped = raw_line.split("#", 1)[0].strip()
        if not stripped:
            continue
        if stripped.startswith("-r ") or stripped.startswith("--requirement "):
            includes.append(stripped.split(None, 1)[1].strip())
            continue

        subject, specifier = _split_requirement_subject_and_spec(stripped)
        if specifier is None or not specifier.startswith("==") or specifier == "==":
            errors.append(f"{path.name}:{line_number} must use an exact '==' pin: {raw_line.strip()}")
            continue

        name = _normalize_requirement_name(subject)
        if name in pins:
            errors.append(f"{path.name}:{line_number} duplicates dependency {name!r}.")
            continue

        pins[name] = LockedRequirement(
            name=name,
            subject=subject,
            version=specifier[2:].strip(),
            source_line=stripped,
        )

    return pins, includes, errors


def collect_dependency_lock_report(
    pyproject_path: str | Path = "pyproject.toml",
    runtime_lock_path: str | Path = "requirements-runtime.lock",
    dev_lock_path: str | Path = "requirements-dev.lock",
) -> DependencyLockReport:
    pyproject_path = Path(pyproject_path).resolve()
    runtime_lock_path = Path(runtime_lock_path).resolve()
    dev_lock_path = Path(dev_lock_path).resolve()

    runtime_subjects, dev_subjects = _load_pyproject_dependency_subjects(pyproject_path)
    runtime_pins, runtime_includes, runtime_errors = _parse_lock_file(runtime_lock_path)
    dev_pins, dev_includes, dev_errors = _parse_lock_file(dev_lock_path)

    errors = [*runtime_errors, *dev_errors]

    if runtime_includes:
        errors.append(
            f"{runtime_lock_path.name} must not include other requirement files: {', '.join(runtime_includes)}"
        )

    expected_dev_include = Path(runtime_lock_path).name
    normalized_dev_includes = [Path(include).name for include in dev_includes]
    if normalized_dev_includes != [expected_dev_include]:
        errors.append(
            f"{dev_lock_path.name} must include exactly one runtime lock reference: {expected_dev_include}"
        )

    def _compare(expected: dict[str, str], actual: dict[str, LockedRequirement], label: str) -> None:
        missing = sorted(name for name in expected if name not in actual)
        extra = sorted(name for name in actual if name not in expected)
        for name in missing:
            errors.append(f"Missing {label} pin for {expected[name]!r} in the lock files.")
        for name in extra:
            errors.append(f"Unexpected {label} pin for {actual[name].subject!r} in the lock files.")
        for name in sorted(name for name in expected if name in actual):
            expected_subject = _canonicalize_subject(expected[name])
            actual_subject = _canonicalize_subject(actual[name].subject)
            if expected_subject != actual_subject:
                errors.append(
                    f"Pinned {label} dependency {actual[name].subject!r} does not match pyproject subject {expected[name]!r}."
                )

    _compare(runtime_subjects, runtime_pins, "runtime")
    _compare(dev_subjects, dev_pins, "dev")

    duplicated_dev_names = sorted(name for name in dev_pins if name in runtime_subjects)
    for name in duplicated_dev_names:
        errors.append(
            f"{dev_lock_path.name} should not repeat runtime dependency {dev_pins[name].subject!r}; keep it in {runtime_lock_path.name}."
        )

    return DependencyLockReport(
        pyproject_path=str(pyproject_path),
        runtime_lock_path=str(runtime_lock_path),
        dev_lock_path=str(dev_lock_path),
        runtime_pins=[runtime_pins[name].source_line for name in sorted(runtime_pins)],
        dev_pins=[dev_pins[name].source_line for name in sorted(dev_pins)],
        errors=errors,
    )


def dependency_lock_exit_code(report: DependencyLockReport) -> int:
    return 1 if report.errors else 0


def render_dependency_lock_report(report: DependencyLockReport) -> str:
    lines = [
        f"pyproject: {report.pyproject_path}",
        f"runtime lock: {report.runtime_lock_path}",
        f"dev lock: {report.dev_lock_path}",
        f"runtime pins: {len(report.runtime_pins)}",
        f"dev pins: {len(report.dev_pins)}",
    ]
    if report.errors:
        lines.append("errors:")
        lines.extend(f"- {error}" for error in report.errors)
    else:
        lines.append("status: ok")
    return "\n".join(lines)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify repository dependency lock files.")
    parser.add_argument("--pyproject", default="pyproject.toml", help="Path to pyproject.toml.")
    parser.add_argument(
        "--runtime-lock",
        default="requirements-runtime.lock",
        help="Path to the runtime dependency lock file.",
    )
    parser.add_argument(
        "--dev-lock",
        default="requirements-dev.lock",
        help="Path to the development dependency lock file.",
    )
    parser.add_argument("--json", action="store_true", help="Emit JSON instead of a human-readable summary.")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    report = collect_dependency_lock_report(
        pyproject_path=args.pyproject,
        runtime_lock_path=args.runtime_lock,
        dev_lock_path=args.dev_lock,
    )
    if args.json:
        print(json.dumps(asdict(report), indent=2))
    else:
        print(render_dependency_lock_report(report))
    return dependency_lock_exit_code(report)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
