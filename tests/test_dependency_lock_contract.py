from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_makefile_and_readme_document_dependency_lock_strategy() -> None:
    makefile_text = (REPO_ROOT / "Makefile").read_text(encoding="utf-8")
    readme_text = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
    baseline_text = (REPO_ROOT / "src" / "data_platform" / "baseline_health.py").read_text(encoding="utf-8")

    assert "verify-locks:" in makefile_text
    assert "python -m data_platform.dependency_locks" in makefile_text
    assert "## Dependency lock strategy" in readme_text
    assert "requirements-runtime.lock" in readme_text
    assert "requirements-dev.lock" in readme_text
    assert 'name="deps"' in baseline_text
