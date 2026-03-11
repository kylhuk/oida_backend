# TESTS

## OVERVIEW
Top-level integration and end-to-end test area. Package-local unit tests stay next to source; this subtree is for broader workflow verification.

## WHERE TO LOOK

| Task | Location | Notes |
|------|----------|-------|
| Full pipeline E2E | `test/e2e/pipeline_test.go` | Requires stack and `e2e` build tag |

## CONVENTIONS

- Keep wide-scope tests here; keep package unit tests beside implementation files.
- E2E tests should use concrete user-facing commands and HTTP endpoints, not internal helper shortcuts.
- Environment defaults should come from env vars with safe local fallbacks.

## GOTCHAS

- `test/e2e` assumes bootstrap + API readiness and shells out to `go run ./cmd/control-plane`.
- These tests validate operator-visible contracts like `run-once --help`, not just data rows.

## COMMANDS

```bash
go test ./test/e2e/... -tags=e2e
```
