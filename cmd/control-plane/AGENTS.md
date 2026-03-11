# CONTROL PLANE

## OVERVIEW
Deterministic `run-once` job runner. Jobs register themselves in `init()` and write execution state into `ops.job_run`.

## WHERE TO LOOK

| Task | Location | Notes |
|------|----------|-------|
| CLI contract | `cmd/control-plane/main.go` | `run-once` parsing and help text |
| Add job | `cmd/control-plane/jobs_*.go` | Register in `init()` via `jobRegistry` |
| Place build | `cmd/control-plane/jobs_place_build.go` | Materializes place graph + dictionary |
| Promote pipeline | `cmd/control-plane/jobs_promote.go` | Canonical fixtures -> silver/gold |
| Domain ingest | `cmd/control-plane/jobs_geopolitical.go`, `jobs_maritime.go`, `jobs_aviation.go`, `jobs_space.go`, `jobs_safety.go` | Pack-specific orchestration |

## CONVENTIONS

- Every job has a stable string name and a short operator-facing description.
- `run-once --job ...` is the public contract; keep help output accurate because tests assert it.
- Jobs should record success and failure via `recordJobRun()` with useful `stats` payloads.
- ClickHouse access in jobs goes through `migrate.NewHTTPRunner(controlPlaneClickHouseURL())`.
- `jobOptions` are passed through context; use `currentJobOptions(ctx)` for optional source scoping.

## GOTCHAS

- Missing job registration means the CLI and E2E tests fail even if the job function exists.
- `jobs_place_build.go` deliberately truncates and recreates reverse-geocode artifacts; treat it as rebuild-oriented, not incremental.
- Some jobs are pack wrappers around SQL generation; others are operational workflows with logging and verification.

## COMMANDS

```bash
go test ./cmd/control-plane/...
go run ./cmd/control-plane run-once --help
go run ./cmd/control-plane run-once --job place-build
```

## ANTI-PATTERNS

- Do not add hidden side-effect jobs outside `jobRegistry`.
- Do not change help text casually; tests and operators rely on it.
- Do not skip job-run logging on failure paths.
