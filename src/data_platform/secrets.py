from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Mapping, Sequence

DEFAULT_MANAGED_SECRET_DEFAULTS: dict[str, str] = {
    "POSTGRES_PASSWORD": "platform",
    "S3_ACCESS_KEY": "minioadmin",
    "S3_SECRET_KEY": "minioadmin",
    "CLICKHOUSE_PASSWORD": "clickhouse",
    "SEED_DEV_API_KEY": "dev-local-key",
}
DEFAULT_MANAGED_SECRET_NAMES: tuple[str, ...] = tuple(DEFAULT_MANAGED_SECRET_DEFAULTS)
DEFAULT_SECRETS_DIR_ENV = "SECRETS_DIR"
SAFE_DEFAULT_SECRET_ENVS = {"development", "dev", "test", "local"}


@dataclass(frozen=True)
class ManagedSecretStatus:
    name: str
    source: str
    resolved: bool
    path: str | None = None
    uses_default: bool = False
    reason: str | None = None


@dataclass(frozen=True)
class SecretConfigurationReport:
    app_env: str
    secrets_dir: str | None
    secrets: list[ManagedSecretStatus]
    structural_errors: list[str]
    policy_errors: list[str]


@dataclass(frozen=True)
class _SecretResolution:
    name: str
    source: str
    resolved: bool
    value: str | None = None
    path: Path | None = None
    uses_default: bool = False
    reason: str | None = None


class SecretConfigurationError(ValueError):
    """Raised when secret file configuration is structurally invalid."""



def _normalize_app_env(environ: Mapping[str, str]) -> str:
    value = (environ.get("APP_ENV") or "development").strip().lower()
    return value or "development"



def _trim_secret_value(raw_value: str) -> str:
    return raw_value.rstrip("\r\n")



def _read_secret_file(path: Path) -> str:
    resolved = path.expanduser().resolve()
    if not resolved.exists():
        raise SecretConfigurationError(f"Secret file does not exist: {resolved}")
    if not resolved.is_file():
        raise SecretConfigurationError(f"Secret path is not a file: {resolved}")

    try:
        value = resolved.read_text(encoding="utf-8")
    except UnicodeDecodeError as exc:  # pragma: no cover - defensive, hard to trigger portably
        raise SecretConfigurationError(f"Secret file is not valid UTF-8: {resolved}") from exc

    trimmed = _trim_secret_value(value)
    if not trimmed:
        raise SecretConfigurationError(f"Secret file is empty: {resolved}")
    return trimmed



def _resolve_secrets_dir(environ: Mapping[str, str]) -> tuple[Path | None, str | None]:
    raw_value = (environ.get(DEFAULT_SECRETS_DIR_ENV) or "").strip()
    if not raw_value:
        return None, None

    resolved = Path(raw_value).expanduser().resolve()
    if not resolved.exists():
        return None, f"{DEFAULT_SECRETS_DIR_ENV} does not exist: {resolved}"
    if not resolved.is_dir():
        return None, f"{DEFAULT_SECRETS_DIR_ENV} is not a directory: {resolved}"
    return resolved, None



def _evaluate_secret_resolutions(
    environ: Mapping[str, str],
    managed_secret_defaults: Mapping[str, str] = DEFAULT_MANAGED_SECRET_DEFAULTS,
) -> tuple[list[_SecretResolution], list[str], list[str], Path | None]:
    structural_errors: list[str] = []
    policy_errors: list[str] = []
    app_env = _normalize_app_env(environ)
    secrets_dir, secrets_dir_error = _resolve_secrets_dir(environ)
    if secrets_dir_error is not None:
        structural_errors.append(secrets_dir_error)

    resolutions: list[_SecretResolution] = []
    for name, default_value in managed_secret_defaults.items():
        raw_value = environ.get(name)
        raw_file_ref = (environ.get(f"{name}_FILE") or "").strip()

        if raw_value is not None and raw_file_ref:
            message = f"Both {name} and {name}_FILE are set; choose only one secret source."
            structural_errors.append(message)
            resolutions.append(
                _SecretResolution(
                    name=name,
                    source="ambiguous",
                    resolved=False,
                    reason=message,
                )
            )
            continue

        if raw_value is not None:
            if not raw_value:
                message = f"{name} is set but empty."
                structural_errors.append(message)
                resolutions.append(
                    _SecretResolution(
                        name=name,
                        source="env",
                        resolved=False,
                        reason=message,
                    )
                )
                continue

            uses_default = raw_value == default_value
            resolutions.append(
                _SecretResolution(
                    name=name,
                    source="env",
                    resolved=True,
                    value=raw_value,
                    uses_default=uses_default,
                )
            )
            if app_env not in SAFE_DEFAULT_SECRET_ENVS and uses_default:
                policy_errors.append(
                    f"{name} uses the repository default secret in APP_ENV={app_env}."
                )
            continue

        if raw_file_ref:
            path = Path(raw_file_ref).expanduser()
            try:
                resolved_value = _read_secret_file(path)
            except SecretConfigurationError as exc:
                message = str(exc)
                structural_errors.append(message)
                resolutions.append(
                    _SecretResolution(
                        name=name,
                        source="file",
                        resolved=False,
                        path=path.expanduser().resolve(),
                        reason=message,
                    )
                )
                continue

            resolved_path = path.expanduser().resolve()
            uses_default = resolved_value == default_value
            resolutions.append(
                _SecretResolution(
                    name=name,
                    source="file",
                    resolved=True,
                    value=resolved_value,
                    path=resolved_path,
                    uses_default=uses_default,
                )
            )
            if app_env not in SAFE_DEFAULT_SECRET_ENVS and uses_default:
                policy_errors.append(
                    f"{name} resolves from {resolved_path} but still uses the repository default secret in APP_ENV={app_env}."
                )
            continue

        if secrets_dir is not None:
            candidate = (secrets_dir / name).resolve()
            if candidate.exists():
                if not candidate.is_file():
                    message = f"Secret path is not a file: {candidate}"
                    structural_errors.append(message)
                    resolutions.append(
                        _SecretResolution(
                            name=name,
                            source="secrets_dir",
                            resolved=False,
                            path=candidate,
                            reason=message,
                        )
                    )
                    continue
                try:
                    resolved_value = _read_secret_file(candidate)
                except SecretConfigurationError as exc:
                    message = str(exc)
                    structural_errors.append(message)
                    resolutions.append(
                        _SecretResolution(
                            name=name,
                            source="secrets_dir",
                            resolved=False,
                            path=candidate,
                            reason=message,
                        )
                    )
                    continue

                uses_default = resolved_value == default_value
                resolutions.append(
                    _SecretResolution(
                        name=name,
                        source="secrets_dir",
                        resolved=True,
                        value=resolved_value,
                        path=candidate,
                        uses_default=uses_default,
                    )
                )
                if app_env not in SAFE_DEFAULT_SECRET_ENVS and uses_default:
                    policy_errors.append(
                        f"{name} resolves from {candidate} but still uses the repository default secret in APP_ENV={app_env}."
                    )
                continue

        resolutions.append(
            _SecretResolution(
                name=name,
                source="default",
                resolved=True,
                value=default_value,
                uses_default=True,
            )
        )
        if app_env not in SAFE_DEFAULT_SECRET_ENVS:
            policy_errors.append(
                f"{name} falls back to the repository default secret in APP_ENV={app_env}."
            )

    return resolutions, structural_errors, policy_errors, secrets_dir



def collect_secret_configuration_report(
    environ: Mapping[str, str] | None = None,
    managed_secret_defaults: Mapping[str, str] = DEFAULT_MANAGED_SECRET_DEFAULTS,
) -> SecretConfigurationReport:
    resolved_environ = dict(os.environ if environ is None else environ)
    app_env = _normalize_app_env(resolved_environ)
    resolutions, structural_errors, policy_errors, secrets_dir = _evaluate_secret_resolutions(
        resolved_environ,
        managed_secret_defaults=managed_secret_defaults,
    )
    return SecretConfigurationReport(
        app_env=app_env,
        secrets_dir=str(secrets_dir) if secrets_dir is not None else None,
        secrets=[
            ManagedSecretStatus(
                name=item.name,
                source=item.source,
                resolved=item.resolved,
                path=str(item.path) if item.path is not None else None,
                uses_default=item.uses_default,
                reason=item.reason,
            )
            for item in resolutions
        ],
        structural_errors=structural_errors,
        policy_errors=policy_errors,
    )



def collect_secret_settings_overrides(
    environ: Mapping[str, str] | None = None,
    managed_secret_defaults: Mapping[str, str] = DEFAULT_MANAGED_SECRET_DEFAULTS,
) -> dict[str, str]:
    resolved_environ = dict(os.environ if environ is None else environ)
    resolutions, structural_errors, _, _ = _evaluate_secret_resolutions(
        resolved_environ,
        managed_secret_defaults=managed_secret_defaults,
    )
    if structural_errors:
        raise SecretConfigurationError("\n".join(structural_errors))

    return {
        item.name: item.value
        for item in resolutions
        if item.resolved and item.source in {"file", "secrets_dir"} and item.value is not None
    }



def secret_configuration_exit_code(report: SecretConfigurationReport) -> int:
    return 1 if report.structural_errors or report.policy_errors else 0



def render_secret_configuration(report: SecretConfigurationReport) -> str:
    lines = [
        f"APP_ENV: {report.app_env}",
        f"SECRETS_DIR: {report.secrets_dir or '(unset)'}",
        "Managed secrets:",
    ]
    for secret in report.secrets:
        detail = secret.source
        if secret.path:
            detail = f"{detail} ({secret.path})"
        if secret.uses_default:
            detail = f"{detail}, repository default"
        lines.append(f"- {secret.name}: {'resolved' if secret.resolved else 'invalid'} via {detail}")
        if secret.reason:
            lines.append(f"  Reason: {secret.reason}")

    if report.structural_errors:
        lines.append("Structural errors:")
        lines.extend(f"- {item}" for item in report.structural_errors)
    if report.policy_errors:
        lines.append("Policy errors:")
        lines.extend(f"- {item}" for item in report.policy_errors)
    if not report.structural_errors and not report.policy_errors:
        lines.append("Status: ok")
    return "\n".join(lines)



def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify file-backed secret configuration.")
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit JSON instead of a human-readable summary.",
    )
    return parser.parse_args(list(argv) if argv is not None else None)



def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    report = collect_secret_configuration_report()
    if args.json:
        print(json.dumps(asdict(report), indent=2))
    else:
        print(render_secret_configuration(report))
    return secret_configuration_exit_code(report)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
