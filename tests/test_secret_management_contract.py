from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_secret_management_is_wired_into_repo_tooling_and_docs() -> None:
    makefile_text = (REPO_ROOT / "Makefile").read_text(encoding="utf-8")
    baseline_text = (REPO_ROOT / "src" / "data_platform" / "baseline_health.py").read_text(encoding="utf-8")
    settings_text = (REPO_ROOT / "src" / "data_platform" / "settings.py").read_text(encoding="utf-8")
    env_example_text = (REPO_ROOT / ".env.example").read_text(encoding="utf-8")
    readme_text = (REPO_ROOT / "README.md").read_text(encoding="utf-8")

    assert 'name="secrets"' in baseline_text
    assert 'data_platform.secrets' in baseline_text
    assert 'collect_secret_settings_overrides' in settings_text
    assert 'DEFAULT_MANAGED_SECRET_DEFAULTS' in settings_text
    assert 'verify-secrets:' in makefile_text
    assert 'PYTHONPATH=src python -m data_platform.secrets' in makefile_text
    assert 'SECRETS_DIR=./infra/secrets' in env_example_text
    assert 'POSTGRES_PASSWORD_FILE=./infra/secrets/POSTGRES_PASSWORD' in env_example_text
    assert '## Secrets management' in readme_text
    assert 'make verify-secrets' in readme_text
    assert '`<NAME>_FILE` and `SECRETS_DIR`' in readme_text
