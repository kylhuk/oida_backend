# System Architecture

## Source of Truth

- Service entrypoints: `cmd/api/main.go`, `cmd/bootstrap/main.go`, `cmd/control-plane/main.go`, `cmd/worker-fetch/main.go`, `cmd/worker-parse/main.go`
- Storage and schema: `migrations/clickhouse/`, `internal/migrate/http_runner.go`
- Deployment topology: `docker-compose.yml`, `docker/go.Dockerfile`
- Operational docs: `README.md`, `docs/runbooks/fresh-bootstrap.md`, `docs/runbooks/backup-restore.md`

## Runtime Behavior

The repository is a Go 1.23 multi-binary backend. The active binaries are:

- `api`: read-only REST API over `gold.api_v1_*` ClickHouse views.
- `bootstrap`: installs or verifies buckets, databases, RBAC, migrations, source governance seed data, API clients, staged assets, backup assets, and the ready marker.
- `control-plane`: long-running automatic sync service by default, and deterministic `run-once` jobs when invoked with `control-plane run-once --job`.
- `worker-fetch`: leases frontier rows, performs HTTP fetches, retains raw bodies in ClickHouse/MinIO according to retention policy, and updates fetch/frontier ledgers.
- `worker-parse`: replays retained raw documents, resolves parser policy, emits source bronze rows, and records parse checkpoints.
- `worker-aisstream`: maintains a persistent WebSocket connection to `wss://stream.aisstream.io/v0/stream`, batches AIS frames in 5-second windows, and lands each batch as a `bronze.raw_document` for `parser:aisstream-json`. Requires `SOURCE_AISSTREAM_API_KEY`. This is the first `websocket` transport source in the system.

ClickHouse is the application database. Application code uses HTTP on port `8123`; the native protocol is not part of the app contract. MinIO provides S3-compatible buckets for raw/stage/backup artifacts.

## Boot Sequence

1. Compose starts ClickHouse and MinIO.
2. `minio-init` creates base buckets.
3. `bootstrap install` waits for dependencies, creates logical databases, ensures roles/users, applies migrations, loads source governance, loads API clients, uploads stage and backup assets, then writes `/ready/bootstrap.ready`.
4. `api` waits on bootstrap completion through Compose, reads the ready marker for `/v1/ready`, queries ClickHouse through `svc_api`, and exposes `/metrics`.
5. Workers and `control-plane` can run after ClickHouse and MinIO are healthy.

## Trust Boundaries

- External API callers cross into `api` on port `8080`. Data routes require `X-API-Key`.
- Prometheus crosses into `api` at `/metrics` with `Authorization: Bearer $METRICS_SHARED_KEY`.
- Workers and control-plane write to ClickHouse through scoped service users.
- Raw HTTP source credentials are referenced by environment variable names in source auth config; raw production secrets must not be committed.
- MinIO stores object bodies and backup artifacts. ClickHouse stores metadata, rows, ledgers, and API views.

## Deferred Or Catalog-Only Behavior

Kubernetes deployment is not a runtime product in this repository. Browser-rendered collection is runtime-backed only for the opt-in VesselFinder worker profile; AISstream WebSocket ingestion is runtime-backed via `worker-aisstream` but starts in `blocked_missing_credential` lifecycle until `SOURCE_AISSTREAM_API_KEY` is set; many other source catalog entries are cataloged for future onboarding but are not currently runnable ingestion paths.

## Extension Knobs

- Add new binaries under `cmd/<name>` only when a distinct runtime boundary is required.
- Add new service wiring in `docker-compose.yml` and `docker/go.Dockerfile` together.
- Keep service configuration behind `getenv()` helpers in each entrypoint.
- Use `internal/migrate.HTTPRunner` for ClickHouse access from app code.
- Update this page when a new runtime service, storage dependency, or trust boundary is introduced.
