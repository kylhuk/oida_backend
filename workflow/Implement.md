# Implement

## Runbook

1. Work only on the production-readiness branch or worktree.
2. Keep migrations append-only under `migrations/clickhouse/`.
3. Use TDD for API behavior and bootstrap behavior.
4. Update docs in the same milestone as behavior changes.
5. Record every verification command in `workflow/Completion.md`.

## Validation Commands

```bash
docker compose config
./scripts/verify.sh
FULL=1 ./scripts/verify.sh
```
