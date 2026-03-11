# BINARIES

## OVERVIEW
`cmd/` holds the repo's executable entrypoints. Each child directory is a separately built binary with its own runtime contract.

## WHERE TO LOOK

| Task | Location | Notes |
|------|----------|-------|
| Add API behavior | `cmd/api/` | Public HTTP surface |
| Change install/verify lifecycle | `cmd/bootstrap/` | One-shot bootstrap binary |
| Add orchestration job | `cmd/control-plane/` | `run-once` jobs and operator flows |
| Change fetch worker | `cmd/worker-fetch/` | Crawl / retention path |
| Change parse worker | `cmd/worker-parse/` | Parser worker CLI |
| Change renderer | `cmd/renderer/` | Currently health-only |

## CONVENTIONS

- Each service keeps its own `main.go` entrypoint and local tests.
- Environment handling usually stays in the service package via `getenv()` helpers.
- Cross-service contracts belong in shared packages under `internal/`, not duplicated across binaries.

## GOTCHAS

- `bootstrap` is a startup dependency for the rest of the stack under `docker-compose.yml`.
- `control-plane` doubles as a long-running service and a `run-once` CLI.
- `renderer`, `worker-fetch`, and `worker-parse` are smaller surfaces today, but they still build as first-class binaries.

## ANTI-PATTERNS

- Do not put shared library code directly under `cmd/`.
- Do not create a new binary when an existing service boundary already owns the workflow.
