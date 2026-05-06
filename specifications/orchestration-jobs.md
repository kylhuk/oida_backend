# Orchestration Jobs

## Source of Truth

- Job registry and CLI contract: `cmd/control-plane/main.go`
- Job implementations: `cmd/control-plane/jobs_*.go`
- Worker loops: `cmd/worker-fetch/main.go`, `cmd/worker-parse/main.go`
- Job logging: `cmd/control-plane/jobs_ops.go`, `ops.job_run`, `gold.api_v1_jobs`
- Stored pipelines: `cmd/control-plane/jobs_pipeline_execute.go`, `migrations/clickhouse/0030_pipeline_execution_engine.sql`

## Runtime Behavior

`control-plane` has two modes:

- With no arguments, it runs a service loop that performs automatic discovery-candidate and HTTP sync ticks.
- With `run-once`, it executes exactly one registered job and exits non-zero on unknown job names or job failure.

Jobs self-register in `init()` by adding entries to `jobRegistry`. Current registered jobs include `noop`, `promote`, `pipeline-execute`, `place-build`, `geo-boundaries-sync`, `geo-names-sync`, `bulk-dump`, `backup-clickhouse`, `restore-clickhouse`, `retention-materialize`, aviation, maritime, geopolitical ingest, safety/security ingest, and space fixture jobs.

`run-once` supports shared options:

- `--job`
- `--source-id`
- `--pipeline-id`
- `--window-start`
- `--window-end`
- `--delta-only`

Worker services are continuous loops. `worker-fetch` starts source worker groups based on automatic source records and suggested rate/burst settings. `worker-parse` starts one parse loop per automatic source and adapts interval/batch from source settings and not-modified ratios.

## Stored Pipelines

Stored pipelines live in `meta.pipeline_registry`; runs live in `ops.pipeline_run`. `pipeline-execute` requires `--pipeline-id`, computes a run key from definition checksum, skips already pending/running/succeeded runs for the same key, and persists planned, pending, running, and terminal snapshots.

## Operational Evidence

Control-plane jobs write `ops.job_run` through `recordJobRun()`. The API exposes job status through `GET /v1/jobs` and `GET /v1/jobs/{jobId}` over `gold.api_v1_jobs`.

## Deferred Or Catalog-Only Behavior

Presence of a source family template or discovery probe does not create a running worker by itself. Runtime workers consume materialized, approved, enabled source registry rows.

## Extension Knobs

- Add a run-once job in `cmd/control-plane/jobs_<topic>.go` and register it in `init()`.
- Add durable job evidence with `recordJobRun()` when a job has observable state.
- Add shared CLI options only in `cmd/control-plane/main.go`.
- Add stored pipeline kinds in `jobs_pipeline_execute.go` and migrations if the durable schema changes.
- Add worker loop behavior in the worker binary, not in control-plane, unless it is deterministic run-once orchestration.
