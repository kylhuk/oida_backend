from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from data_platform.repo_snapshot import (
    GitCommandError,
    InvalidRecentLimitError,
    RepositorySnapshotCollector,
    collect_repository_snapshot,
    render_repository_snapshot,
    repository_snapshot_clean_failure_reason,
    repository_snapshot_clean_requirement_satisfied,
    repository_snapshot_exit_code,
    snapshot_has_worktree_changes,
)


def _run_git(repo: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", *args],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def _commit_file(repo: Path, name: str, content: str, message: str) -> None:
    target = repo / name
    target.write_text(content, encoding="utf-8")
    _run_git(repo, "add", name)
    _run_git(repo, "commit", "-m", message)


def _init_repo(tmp_path: Path, name: str = "repo") -> Path:
    repo = tmp_path / name
    repo.mkdir()
    _run_git(repo, "init")
    _run_git(repo, "config", "user.name", "Test User")
    _run_git(repo, "config", "user.email", "test@example.com")
    _commit_file(repo, "README.md", "hello\n", "initial commit")
    _commit_file(repo, "tracked.txt", "tracked\n", "add tracked file")
    return repo


def _build_env() -> dict[str, str]:
    env = dict(os.environ)
    src_path = str(Path(__file__).resolve().parents[1] / "src")
    existing_pythonpath = env.get("PYTHONPATH")
    env["PYTHONPATH"] = src_path if not existing_pythonpath else f"{src_path}:{existing_pythonpath}"
    return env


class _MissingGitRunner:
    def run(self, *args: str) -> str:
        raise GitCommandError("Git executable is not available on PATH.")

    def run_bytes(self, *args: str) -> bytes:
        raise GitCommandError("Git executable is not available on PATH.")


class _StatusFailureRunner:
    def __init__(self) -> None:
        self._commit_line = "a" * 40 + "	aaaaaaa	seed commit"

    def run(self, *args: str) -> str:
        if args == ("rev-parse", "--is-inside-work-tree"):
            return "true"
        if args == ("rev-parse", "--abbrev-ref", "HEAD"):
            return "main"
        if args == ("log", "-1", "--format=%H%x09%h%x09%s"):
            return self._commit_line
        if len(args) == 3 and args[0] == "log" and args[1].startswith("-n") and args[2] == "--format=%H%x09%h%x09%s":
            return self._commit_line
        raise AssertionError(f"Unexpected git command: {args!r}")

    def run_bytes(self, *args: str) -> bytes:
        if args == ("status", "--porcelain=v1", "-z", "--untracked-files=all"):
            raise GitCommandError("fatal: unable to read index file")
        raise AssertionError(f"Unexpected git command: {args!r}")


def test_collect_repository_snapshot_for_git_repo(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path)
    (repo / "tracked.txt").write_text("changed\n", encoding="utf-8")
    (repo / "untracked.txt").write_text("new\n", encoding="utf-8")

    snapshot = collect_repository_snapshot(repo, recent_limit=2)

    assert snapshot.is_git_repo is True
    assert snapshot.reason is None
    assert snapshot.path_error is None
    assert snapshot.path_error_reason is None
    assert snapshot.current_branch is not None
    assert snapshot.latest_commit is not None
    assert snapshot.latest_commit.subject == "add tracked file"
    assert snapshot.clean_failure_reason == "dirty tracked files present and untracked files present"
    assert snapshot.clean_requirement_satisfied is False
    assert snapshot.dirty_files == ["tracked.txt"]
    assert snapshot.untracked_files == ["untracked.txt"]
    assert [commit.subject for commit in snapshot.recent_commits] == [
        "add tracked file",
        "initial commit",
    ]


def test_collect_repository_snapshot_for_non_git_directory_reports_reason(tmp_path: Path) -> None:
    plain_dir = tmp_path / "plain"
    plain_dir.mkdir()

    snapshot = collect_repository_snapshot(plain_dir)

    assert snapshot.is_git_repo is False
    assert snapshot.reason == f"Repository path is not a Git checkout: {plain_dir.resolve()}"
    assert snapshot.path_error is None
    assert snapshot.path_error_reason is None
    assert snapshot.clean_failure_reason == "path is not a Git checkout"
    assert snapshot.clean_requirement_satisfied is False
    assert snapshot.current_branch is None
    assert snapshot.latest_commit is None
    assert snapshot.dirty_files == []
    assert snapshot.untracked_files == []
    assert snapshot.recent_commits == []


def test_collect_repository_snapshot_for_missing_path_reports_path_error(tmp_path: Path) -> None:
    missing_path = tmp_path / "missing-repo"

    snapshot = collect_repository_snapshot(missing_path)

    assert snapshot.is_git_repo is False
    assert snapshot.reason == f"Repository path does not exist: {missing_path.resolve()}"
    assert snapshot.path_error == f"Repository path does not exist: {missing_path.resolve()}"
    assert snapshot.path_error_reason == f"Repository path does not exist: {missing_path.resolve()}"
    assert snapshot.clean_failure_reason is None
    assert snapshot.clean_requirement_satisfied is None
    assert snapshot.current_branch is None
    assert snapshot.latest_commit is None
    assert snapshot.recent_commits == []


def test_collect_repository_snapshot_for_non_directory_path_reports_path_error(tmp_path: Path) -> None:
    file_path = tmp_path / "repo.txt"
    file_path.write_text("not a repo\n", encoding="utf-8")

    snapshot = collect_repository_snapshot(file_path)

    assert snapshot.is_git_repo is False
    assert snapshot.reason == f"Repository path is not a directory: {file_path.resolve()}"
    assert snapshot.path_error == f"Repository path is not a directory: {file_path.resolve()}"
    assert snapshot.path_error_reason == f"Repository path is not a directory: {file_path.resolve()}"
    assert snapshot.clean_failure_reason is None
    assert snapshot.clean_requirement_satisfied is None
    assert snapshot.current_branch is None
    assert snapshot.latest_commit is None
    assert snapshot.recent_commits == []


def test_collect_repository_snapshot_rejects_negative_recent_limit(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path)

    with pytest.raises(InvalidRecentLimitError, match="recent_limit must be non-negative"):
        collect_repository_snapshot(repo, recent_limit=-1)


def test_render_repository_snapshot_human_output_for_non_git_directory(tmp_path: Path) -> None:
    plain_dir = tmp_path / "plain"
    plain_dir.mkdir()

    rendered = render_repository_snapshot(collect_repository_snapshot(plain_dir))

    assert "Git repository: no" in rendered
    assert f"Reason: Repository path is not a Git checkout: {plain_dir.resolve()}" in rendered


def test_render_repository_snapshot_human_output_for_path_error(tmp_path: Path) -> None:
    file_path = tmp_path / "repo.txt"
    file_path.write_text("not a repo\n", encoding="utf-8")

    rendered = render_repository_snapshot(collect_repository_snapshot(file_path))

    assert "Git repository: no" in rendered
    assert f"Path issue: Repository path is not a directory: {file_path.resolve()}" in rendered


def test_repo_snapshot_cli_json_output_for_git_repo(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path)

    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "data_platform.repo_snapshot",
            "--path",
            str(repo),
            "--recent-limit",
            "1",
            "--json",
        ],
        check=True,
        capture_output=True,
        text=True,
        env=_build_env(),
    )

    payload = json.loads(completed.stdout)
    assert payload["is_git_repo"] is True
    assert payload["reason"] is None
    assert payload["path_error"] is None
    assert payload["path_error_reason"] is None
    assert payload["clean_failure_reason"] is None
    assert payload["clean_requirement_satisfied"] is True
    assert payload["latest_commit"]["subject"] == "add tracked file"
    assert len(payload["recent_commits"]) == 1


def test_repository_snapshot_exit_code_defaults_and_require_clean_modes(tmp_path: Path) -> None:
    clean_repo = _init_repo(tmp_path, name="clean")
    dirty_repo = _init_repo(tmp_path, name="dirty")
    (dirty_repo / "tracked.txt").write_text("changed\n", encoding="utf-8")
    plain_dir = tmp_path / "plain"
    plain_dir.mkdir()
    file_path = tmp_path / "repo.txt"
    file_path.write_text("not a repo\n", encoding="utf-8")

    clean_snapshot = collect_repository_snapshot(clean_repo)
    dirty_snapshot = collect_repository_snapshot(dirty_repo)
    plain_snapshot = collect_repository_snapshot(plain_dir)
    file_snapshot = collect_repository_snapshot(file_path)

    assert snapshot_has_worktree_changes(clean_snapshot) is False
    assert snapshot_has_worktree_changes(dirty_snapshot) is True
    assert repository_snapshot_clean_failure_reason(clean_snapshot) is None
    assert repository_snapshot_clean_requirement_satisfied(clean_snapshot) is True
    assert repository_snapshot_clean_failure_reason(dirty_snapshot) == "dirty tracked files present"
    assert repository_snapshot_clean_requirement_satisfied(dirty_snapshot) is False
    assert repository_snapshot_clean_failure_reason(plain_snapshot) == "path is not a Git checkout"
    assert repository_snapshot_clean_requirement_satisfied(plain_snapshot) is False
    assert repository_snapshot_clean_requirement_satisfied(file_snapshot) is None
    assert repository_snapshot_exit_code(clean_snapshot) == 0
    assert repository_snapshot_exit_code(dirty_snapshot) == 0
    assert repository_snapshot_exit_code(clean_snapshot, require_clean=True) == 0
    assert repository_snapshot_exit_code(dirty_snapshot, require_clean=True) == 1
    assert repository_snapshot_exit_code(plain_snapshot, require_clean=True) == 1
    assert repository_snapshot_exit_code(file_snapshot, require_clean=True) == 2


def test_repo_snapshot_cli_json_output_for_non_directory_path(tmp_path: Path) -> None:
    file_path = tmp_path / "repo.txt"
    file_path.write_text("not a repo\n", encoding="utf-8")

    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "data_platform.repo_snapshot",
            "--path",
            str(file_path),
            "--json",
        ],
        check=False,
        capture_output=True,
        text=True,
        env=_build_env(),
    )

    payload = json.loads(completed.stdout)
    assert completed.returncode == 2
    assert payload["is_git_repo"] is False
    assert payload["reason"] == f"Repository path is not a directory: {file_path.resolve()}"
    assert payload["path_error"] == f"Repository path is not a directory: {file_path.resolve()}"
    assert payload["path_error_reason"] == f"Repository path is not a directory: {file_path.resolve()}"
    assert payload["clean_failure_reason"] is None
    assert payload["clean_requirement_satisfied"] is None


def test_repo_snapshot_cli_rejects_negative_recent_limit_without_traceback(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path)

    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "data_platform.repo_snapshot",
            "--path",
            str(repo),
            "--recent-limit",
            "-1",
            "--json",
        ],
        check=False,
        capture_output=True,
        text=True,
        env=_build_env(),
    )

    assert completed.returncode == 2
    assert completed.stdout == ""
    assert "recent_limit must be non-negative." in completed.stderr
    assert "Traceback" not in completed.stderr


def test_collect_repository_snapshot_preserves_untracked_paths_with_spaces(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path)
    spaced_file = repo / "folder with spaces"
    spaced_file.mkdir()
    target = spaced_file / "new file.txt"
    target.write_text("new\n", encoding="utf-8")

    snapshot = collect_repository_snapshot(repo)

    assert snapshot.untracked_files == ["folder with spaces/new file.txt"]


def test_collect_repository_snapshot_reports_renamed_paths_without_arrow_markup(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path)

    _run_git(repo, "mv", "tracked.txt", "renamed file.txt")
    snapshot = collect_repository_snapshot(repo)

    assert snapshot.dirty_files == ["renamed file.txt"]


def test_render_repository_snapshot_escapes_control_characters_in_paths(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path)
    weird_name = "folder\\nname.txt".encode("utf-8").decode("unicode_escape")
    target = repo / weird_name
    target.write_text("new\n", encoding="utf-8")

    rendered = render_repository_snapshot(collect_repository_snapshot(repo))

    assert "- folder\\nname.txt" in rendered


def test_repo_snapshot_distinguishes_git_metadata_unavailable_from_non_git_directory(
    tmp_path: Path,
) -> None:
    snapshot = RepositorySnapshotCollector(tmp_path, runner=_MissingGitRunner()).collect()

    assert snapshot.is_git_repo is False
    assert snapshot.reason == "Git executable is not available on PATH."
    assert snapshot.clean_failure_reason == "Git executable is not available on PATH."
    assert snapshot.clean_requirement_satisfied is False
    assert repository_snapshot_exit_code(snapshot, require_clean=True) == 1

    rendered = render_repository_snapshot(snapshot, require_clean=True)
    assert "Reason: Git executable is not available on PATH." in rendered
    assert "Clean requirement: failed (Git executable is not available on PATH.)" in rendered


def test_render_repository_snapshot_marks_require_clean_status(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path)
    (repo / "tracked.txt").write_text("changed\n", encoding="utf-8")

    dirty_rendered = render_repository_snapshot(collect_repository_snapshot(repo), require_clean=True)
    assert "Clean requirement: failed (dirty tracked files present)" in dirty_rendered

    plain_dir = tmp_path / "plain"
    plain_dir.mkdir()
    plain_rendered = render_repository_snapshot(collect_repository_snapshot(plain_dir), require_clean=True)
    assert "Clean requirement: failed (path is not a Git checkout)" in plain_rendered


def test_repo_snapshot_cli_require_clean_returns_nonzero_for_dirty_repo(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path)
    (repo / "tracked.txt").write_text("changed\n", encoding="utf-8")

    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "data_platform.repo_snapshot",
            "--path",
            str(repo),
            "--require-clean",
            "--json",
        ],
        check=False,
        capture_output=True,
        text=True,
        env=_build_env(),
    )

    payload = json.loads(completed.stdout)
    assert completed.returncode == 1
    assert payload["dirty_files"] == ["tracked.txt"]
    assert payload["path_error_reason"] is None
    assert payload["clean_failure_reason"] == "dirty tracked files present"
    assert payload["clean_requirement_satisfied"] is False


def test_repo_snapshot_cli_require_clean_returns_nonzero_for_non_git_directory(tmp_path: Path) -> None:
    plain_dir = tmp_path / "plain"
    plain_dir.mkdir()

    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "data_platform.repo_snapshot",
            "--path",
            str(plain_dir),
            "--require-clean",
            "--json",
        ],
        check=False,
        capture_output=True,
        text=True,
        env=_build_env(),
    )

    payload = json.loads(completed.stdout)
    assert completed.returncode == 1
    assert payload["is_git_repo"] is False
    assert payload["reason"] == f"Repository path is not a Git checkout: {plain_dir.resolve()}"
    assert payload["clean_failure_reason"] == "path is not a Git checkout"
    assert payload["clean_requirement_satisfied"] is False


def test_repo_snapshot_cli_require_clean_preserves_invalid_path_exit_code(tmp_path: Path) -> None:
    missing_path = tmp_path / "missing-repo"

    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "data_platform.repo_snapshot",
            "--path",
            str(missing_path),
            "--require-clean",
            "--json",
        ],
        check=False,
        capture_output=True,
        text=True,
        env=_build_env(),
    )

    payload = json.loads(completed.stdout)
    assert completed.returncode == 2
    assert payload["path_error_reason"] == f"Repository path does not exist: {missing_path.resolve()}"
    assert payload["clean_requirement_satisfied"] is None


def test_collect_repository_snapshot_records_worktree_status_failures_without_reporting_clean(
    tmp_path: Path,
) -> None:
    snapshot = RepositorySnapshotCollector(tmp_path, runner=_StatusFailureRunner()).collect()

    assert snapshot.is_git_repo is True
    assert snapshot.current_branch == "main"
    assert snapshot.latest_commit is not None
    assert snapshot.dirty_files == []
    assert snapshot.untracked_files == []
    assert snapshot.worktree_status_reason == "fatal: unable to read index file"
    assert snapshot.clean_failure_reason == "fatal: unable to read index file"
    assert snapshot.clean_requirement_satisfied is False
    assert repository_snapshot_exit_code(snapshot, require_clean=True) == 1


def test_render_repository_snapshot_surfaces_worktree_status_failures(tmp_path: Path) -> None:
    snapshot = RepositorySnapshotCollector(tmp_path, runner=_StatusFailureRunner()).collect()

    rendered = render_repository_snapshot(snapshot, require_clean=True)

    assert "Worktree status: unavailable (fatal: unable to read index file)" in rendered
    assert "Dirty files: unavailable" in rendered
    assert "Untracked files: unavailable" in rendered
    assert "Clean requirement: failed (fatal: unable to read index file)" in rendered


def test_repo_snapshot_readme_documents_require_clean_and_path_error_exit_codes() -> None:
    readme_text = (Path(__file__).resolve().parents[1] / "README.md").read_text(encoding="utf-8")

    assert "path_error_reason" in readme_text
    assert "clean_failure_reason" in readme_text
    assert "clean_requirement_satisfied" in readme_text
    assert "worktree_status_reason" in readme_text
    assert "Negative `--recent-limit` values" in readme_text
    assert "exit code `2`" in readme_text
    assert "--require-clean" in readme_text
    assert "exit code `1`" in readme_text
