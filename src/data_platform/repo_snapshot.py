from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import Iterable


@dataclass(frozen=True)
class CommitSummary:
    full_hash: str
    short_hash: str
    subject: str


@dataclass(frozen=True)
class RepositorySnapshot:
    path: str
    is_git_repo: bool
    current_branch: str | None
    latest_commit: CommitSummary | None
    dirty_files: list[str]
    untracked_files: list[str]
    recent_commits: list[CommitSummary]
    reason: str | None = None
    path_error: str | None = None
    path_error_reason: str | None = None
    clean_failure_reason: str | None = None
    clean_requirement_satisfied: bool | None = None
    worktree_status_reason: str | None = None


class GitCommandError(RuntimeError):
    """Raised when git cannot provide requested repository information."""


class InvalidRecentLimitError(ValueError):
    """Raised when a repository snapshot request uses an invalid recent-commit limit."""


def validate_recent_limit(recent_limit: int) -> int:
    if recent_limit < 0:
        raise InvalidRecentLimitError("recent_limit must be non-negative.")
    return recent_limit


class GitRunner:
    def __init__(self, repo_path: str | Path) -> None:
        self.repo_path = Path(repo_path).resolve()

    def run(self, *args: str) -> str:
        try:
            completed = subprocess.run(
                ["git", *args],
                cwd=self.repo_path,
                check=True,
                capture_output=True,
                text=True,
            )
        except FileNotFoundError as exc:
            if exc.filename == "git":
                raise GitCommandError("Git executable is not available on PATH.") from exc
            raise GitCommandError(f"Unable to access repository path: {self.repo_path}") from exc
        except subprocess.CalledProcessError as exc:
            message = (exc.stderr or exc.stdout or str(exc)).strip()
            raise GitCommandError(message or str(exc)) from exc
        return completed.stdout.rstrip("\n")

    def run_bytes(self, *args: str) -> bytes:
        try:
            completed = subprocess.run(
                ["git", *args],
                cwd=self.repo_path,
                check=True,
                capture_output=True,
            )
        except FileNotFoundError as exc:
            if exc.filename == "git":
                raise GitCommandError("Git executable is not available on PATH.") from exc
            raise GitCommandError(f"Unable to access repository path: {self.repo_path}") from exc
        except subprocess.CalledProcessError as exc:
            raw_message = ((exc.stderr or b"") + (exc.stdout or b"")).decode(
                errors="replace"
            ).strip()
            raise GitCommandError(raw_message or str(exc)) from exc
        return completed.stdout


class RepositorySnapshotCollector:
    def __init__(self, repo_path: str | Path = ".", runner: GitRunner | None = None) -> None:
        self.repo_path = Path(repo_path).resolve()
        self.runner = runner or GitRunner(self.repo_path)

    def collect(self, recent_limit: int = 5) -> RepositorySnapshot:
        recent_limit = validate_recent_limit(recent_limit)
        path_error = get_repo_path_error_reason(self.repo_path)
        if path_error is not None:
            return self._empty_snapshot(reason=path_error, path_error=path_error)

        is_git_repo, reason = self._probe_git_repo()
        if not is_git_repo:
            return self._empty_snapshot(reason=reason, path_error=None)

        current_branch = self.runner.run("rev-parse", "--abbrev-ref", "HEAD")
        latest_commit = self._latest_commit()
        dirty_files, untracked_files, worktree_status_reason = self._status_lists()
        recent_commits = self._recent_commits(recent_limit)

        snapshot = RepositorySnapshot(
            path=str(self.repo_path),
            is_git_repo=True,
            current_branch=current_branch,
            latest_commit=latest_commit,
            dirty_files=dirty_files,
            untracked_files=untracked_files,
            recent_commits=recent_commits,
            reason=None,
            path_error=None,
            path_error_reason=None,
            clean_failure_reason=None,
            clean_requirement_satisfied=None,
            worktree_status_reason=worktree_status_reason,
        )
        return _with_clean_repository_state(snapshot)

    def _empty_snapshot(self, *, reason: str | None, path_error: str | None) -> RepositorySnapshot:
        snapshot = RepositorySnapshot(
            path=str(self.repo_path),
            is_git_repo=False,
            current_branch=None,
            latest_commit=None,
            dirty_files=[],
            untracked_files=[],
            recent_commits=[],
            reason=reason,
            path_error=path_error,
            path_error_reason=path_error,
            clean_failure_reason=None,
            clean_requirement_satisfied=None,
            worktree_status_reason=None,
        )
        return _with_clean_repository_state(snapshot)

    def _probe_git_repo(self) -> tuple[bool, str | None]:
        try:
            result = self.runner.run("rev-parse", "--is-inside-work-tree")
        except GitCommandError as exc:
            return False, self._normalize_git_probe_reason(exc)
        if result != "true":
            return False, f"Repository path is not a Git checkout: {self.repo_path}"
        return True, None

    def _normalize_git_probe_reason(self, exc: GitCommandError) -> str:
        message = str(exc).strip()
        lowered = message.lower()
        if "not a git repository" in lowered or "not in a git directory" in lowered:
            return f"Repository path is not a Git checkout: {self.repo_path}"
        return message or f"Git metadata is unavailable for repository path: {self.repo_path}"

    def _latest_commit(self) -> CommitSummary | None:
        try:
            output = self.runner.run("log", "-1", "--format=%H%x09%h%x09%s")
        except GitCommandError:
            return None
        if not output:
            return None
        return self._parse_commit_line(output)

    def _recent_commits(self, recent_limit: int) -> list[CommitSummary]:
        if recent_limit <= 0:
            return []
        try:
            output = self.runner.run(
                "log",
                f"-n{recent_limit}",
                "--format=%H%x09%h%x09%s",
            )
        except GitCommandError:
            return []
        if not output:
            return []
        return [self._parse_commit_line(line) for line in output.splitlines() if line.strip()]

    def _status_lists(self) -> tuple[list[str], list[str], str | None]:
        try:
            output = self.runner.run_bytes(
                "status",
                "--porcelain=v1",
                "-z",
                "--untracked-files=all",
            )
        except GitCommandError as exc:
            return [], [], str(exc).strip() or "Git worktree status is unavailable."
        if not output:
            return [], [], None
        dirty_files, untracked_files = _parse_porcelain_status_lists(output)
        return dirty_files, untracked_files, None

    @staticmethod
    def _parse_commit_line(line: str) -> CommitSummary:
        full_hash, short_hash, subject = line.split("\t", 2)
        return CommitSummary(full_hash=full_hash, short_hash=short_hash, subject=subject)


def get_repo_path_error_reason(repo_path: str | Path) -> str | None:
    resolved_path = Path(repo_path).resolve()
    if not resolved_path.exists():
        return f"Repository path does not exist: {resolved_path}"
    if not resolved_path.is_dir():
        return f"Repository path is not a directory: {resolved_path}"
    return None


def collect_repository_snapshot(repo_path: str | Path = ".", recent_limit: int = 5) -> RepositorySnapshot:
    collector = RepositorySnapshotCollector(repo_path)
    return collector.collect(recent_limit=recent_limit)


def render_repository_snapshot(
    snapshot: RepositorySnapshot, *, require_clean: bool = False
) -> str:
    lines = [
        f"Repository path: {snapshot.path}",
        f"Git repository: {'yes' if snapshot.is_git_repo else 'no'}",
    ]

    if not snapshot.is_git_repo:
        if snapshot.path_error_reason or snapshot.path_error:
            lines.append(f"Path issue: {snapshot.path_error_reason or snapshot.path_error}")
        elif snapshot.reason:
            lines.append(f"Reason: {snapshot.reason}")
        else:
            lines.append("No git metadata is available for this path.")
        if require_clean:
            if snapshot.path_error_reason or snapshot.path_error:
                lines.append("Clean requirement: unavailable (repository path is invalid)")
            else:
                clean_failure_reason = repository_snapshot_clean_failure_reason(snapshot)
                lines.append(f"Clean requirement: failed ({clean_failure_reason})")
        return "\n".join(lines)

    lines.append(f"Current branch: {snapshot.current_branch}")
    if snapshot.latest_commit is None:
        lines.append("Latest commit: unavailable")
    else:
        lines.append(
            f"Latest commit: {snapshot.latest_commit.short_hash} {snapshot.latest_commit.subject}"
        )

    if snapshot.worktree_status_reason:
        lines.append(f"Worktree status: unavailable ({snapshot.worktree_status_reason})")
        lines.append("Dirty files: unavailable")
        lines.append("Untracked files: unavailable")
    else:
        lines.append(f"Dirty files ({len(snapshot.dirty_files)}):")
        lines.extend(_render_list(snapshot.dirty_files))
        lines.append(f"Untracked files ({len(snapshot.untracked_files)}):")
        lines.extend(_render_list(snapshot.untracked_files))
    lines.append(f"Recent commits ({len(snapshot.recent_commits)}):")
    lines.extend(f"- {commit.short_hash} {commit.subject}" for commit in snapshot.recent_commits)
    if require_clean:
        if repository_snapshot_clean_requirement_satisfied(snapshot):
            lines.append("Clean requirement: satisfied")
        else:
            clean_failure_reason = repository_snapshot_clean_failure_reason(snapshot)
            lines.append(f"Clean requirement: failed ({clean_failure_reason})")
    return "\n".join(lines)


def _render_list(items: Iterable[str]) -> list[str]:
    rendered = [f"- {_render_path_for_display(item)}" for item in items]
    return rendered or ["- none"]


def _render_path_for_display(path_text: str) -> str:
    return path_text.encode("unicode_escape").decode("ascii")


def _status_entry_includes_original_path(status: str) -> bool:
    return "R" in status or "C" in status


def _parse_porcelain_status_lists(output: bytes) -> tuple[list[str], list[str]]:
    dirty_files: list[str] = []
    untracked_files: list[str] = []
    entries = [entry for entry in output.split(b"\0") if entry]
    index = 0
    while index < len(entries):
        entry = entries[index]
        if len(entry) < 3:
            index += 1
            continue
        status = entry[:2].decode("ascii", errors="replace")
        path_text = os.fsdecode(entry[3:])
        if status == "??":
            untracked_files.append(path_text)
            index += 1
            continue
        dirty_files.append(path_text)
        if _status_entry_includes_original_path(status):
            index += 2
        else:
            index += 1
    return dirty_files, untracked_files


def _with_clean_repository_state(snapshot: RepositorySnapshot) -> RepositorySnapshot:
    clean_failure_reason = repository_snapshot_clean_failure_reason(snapshot)
    return replace(
        snapshot,
        clean_failure_reason=clean_failure_reason,
        clean_requirement_satisfied=repository_snapshot_clean_requirement_satisfied(
            snapshot,
            clean_failure_reason=clean_failure_reason,
        ),
    )


def snapshot_has_worktree_changes(snapshot: RepositorySnapshot) -> bool:
    return bool(snapshot.dirty_files or snapshot.untracked_files)


def repository_snapshot_clean_failure_reason(snapshot: RepositorySnapshot) -> str | None:
    if (snapshot.path_error_reason or snapshot.path_error) is not None:
        return None
    if not snapshot.is_git_repo:
        if snapshot.reason and snapshot.reason.startswith("Repository path is not a Git checkout:"):
            return "path is not a Git checkout"
        if snapshot.reason:
            return snapshot.reason
        return "Git metadata is unavailable."

    reasons: list[str] = []
    if snapshot.worktree_status_reason:
        return snapshot.worktree_status_reason

    if snapshot.dirty_files:
        reasons.append("dirty tracked files present")
    if snapshot.untracked_files:
        reasons.append("untracked files present")
    if not reasons:
        return None
    return " and ".join(reasons)


def repository_snapshot_clean_requirement_satisfied(
    snapshot: RepositorySnapshot,
    *,
    clean_failure_reason: str | None = None,
) -> bool | None:
    if (snapshot.path_error_reason or snapshot.path_error) is not None:
        return None
    reason = clean_failure_reason
    if reason is None:
        reason = repository_snapshot_clean_failure_reason(snapshot)
    return reason is None


def repository_snapshot_exit_code(
    snapshot: RepositorySnapshot, *, require_clean: bool = False
) -> int:
    if (snapshot.path_error_reason or snapshot.path_error) is not None:
        return 2
    if require_clean and not repository_snapshot_clean_requirement_satisfied(snapshot):
        return 1
    return 0


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Show a git repository snapshot.")
    parser.add_argument(
        "--path",
        default=".",
        help="Repository path to inspect. Defaults to the current directory.",
    )
    parser.add_argument(
        "--recent-limit",
        type=int,
        default=5,
        help="Number of recent commits to include.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit JSON instead of a human-readable summary.",
    )
    parser.add_argument(
        "--require-clean",
        action="store_true",
        help=(
            "Return exit code 1 when the path is not a clean Git checkout. "
            "Invalid paths continue to return exit code 2."
        ),
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    try:
        snapshot = collect_repository_snapshot(args.path, recent_limit=args.recent_limit)
    except InvalidRecentLimitError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    if args.json:
        print(json.dumps(asdict(snapshot), indent=2))
    else:
        print(render_repository_snapshot(snapshot, require_clean=args.require_clean))
    return repository_snapshot_exit_code(snapshot, require_clean=args.require_clean)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
