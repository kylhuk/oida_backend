# Capability Matrix and Contract

## Defaults
- **Job surface**: `control-plane run-once --job <job-name>` is the single canonical orchestration command; the API exposes only GET job telemetry (no POSTs).
- **Migration ledger**: `meta.schema_migrations` is created and owned by `internal/migrate/http_runner.go` during bootstrap; SQL migrations never recreate it.
- **Runtime mode**: a single-node baseline is the declared anchor, with clustering documented as optional scale-out.
- **API auth scope**: protected `/v1/*` routes require hashed, scoped API clients seeded into `meta.api_clients`.
- **Advanced ClickHouse**: `s3()` ingestion for staged bulk dumps and HTTP `async_insert` telemetry writes are implemented; projections, skip indexes, S3Queue, `url()`, and `file()` remain on the roadmap until runtime evidence arrives.

## Status legend
- **implemented** – code and runtime paths exist (even if the payload is still stubbed behind the manifest contract).
- **partial** – the binary/schema exists but the manifest claim lists more identities/metrics than currently registered.
- **roadmap** – the manifest claim describes behavior that has no runtime evidence yet (e.g., POST job surface, missing metrics, advanced CH features).

## Summary
| Status | Claim count |
|---|---:|
| implemented | 33 |
| partial | 5 |
| roadmap | 6 |

See `docs/capability-matrix.json` for the detailed machine-readable table.

## Contract highlights
- **Job execution** stays with `control-plane run-once` (verified in `cmd/control-plane/main.go`), and `run-once --help` now lists place, promote, domain ingest, pipeline, backup, restore, and retention jobs.
- **Renderer** is not part of the single-node production Compose topology; browser-rendered collection remains deferred until a concrete consumer requires it.
- **Migration ledger** is bootstrap-owned, includes only the documented `version`, `applied_at`, `checksum`, `success`, and `notes` columns, and the registry uses checksum-based validation (`internal/migrate/http_runner.go`).
- **API contract** keeps the GET-only endpoints the manifest advertises, and the `/v1/*` surface routes to `gold.api_v1_*` views (see `cmd/api/handlers.go`).
- **Metrics**: all 18 core metrics are now implemented in `internal/metrics/registry.go` with contribution and snapshot support.
- **API observability**: `/metrics` exposes Prometheus text protected by `METRICS_SHARED_KEY`, with Compose wiring for Prometheus, Alertmanager, and Grafana.
- **Domain packs**: all 5 domain packs (geopolitical, maritime, aviation, space, safety) have complete metric sets with exact manifest IDs; legacy aliases preserved for compatibility.
- **Bulk ingest + telemetry**: the `bulk-dump` job stages `stage/bulk_dump.csv` into `ops.bulk_dump` via `s3()`, and worker fetch logs write `ops.fetch_log` with `SETTINGS async_insert=1`; projections, skip indexes, `S3Queue`, `url()`, and `file()` remain on the roadmap.
- **Local HTTP fixtures**: `docker-compose.yml` includes an `http-fixture` service on `:8079` with deterministic GDELT, ReliefWeb, OpenSanctions, NASA FIRMS, NOAA hazards, KEV, and ACLED stubs; maritime, aviation, and space remain non-HTTP fixture packs until concrete source registry entries exist.

## Evidence
- `.sisyphus/evidence/task-1-delta-matrix.txt` captures the grep outputs for `run-once` and the consistent contract messaging.
- `.sisyphus/evidence/task-1-delta-drift.txt` records the drift check outputs for stale `POST /v1/jobs` and `schema_migrations` claims.
- `.sisyphus/evidence/task-3-job-contract.txt` captures the supported run-once jobs and the updated E2E contract surface.
- `.sisyphus/evidence/task-3-job-contract-drift.txt` captures the stale `/v1/jobs/` cleanup grep and the doc/job drift reconciliation.
- `.sisyphus/evidence/task-9-clickhouse-features.txt` documents the `bulk-dump` s3() ingestion path and async insert telemetry.
