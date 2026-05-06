# Completion Evidence

## Requirements

| Requirement | Evidence |
| --- | --- |
| Python medallion stack removed | Satisfied. Removed `src/data_platform/`, `tests/`, Python lock/config files, Python Dockerfile, Postgres/Dagster infra, root runtime binaries, and stale ready/session artifacts. README and production docs now identify the Go backend as the product. |
| Go verification gate added | Satisfied. Added `scripts/verify.sh`; `make verify` and `make verify-full` call it with Dockerized Go 1.23. |
| Hashed API-key auth implemented | Satisfied. Added `cmd/api/auth.go`, `cmd/bootstrap/api_clients.go`, `migrations/clickhouse/0035_api_clients.sql`, and `seed/api_clients.json`. Tests cover hash lookup, scope checks, missing/invalid keys, and bootstrap seed validation. |
| Compose hardened and image pins removed from `:latest` | Satisfied. `docker compose config` exits 0; third-party images and Go builder/runtime images are pinned by digest; services run read-only with tmpfs and `no-new-privileges`. |
| Metrics and monitoring added | Satisfied. Added `/metrics`, Prometheus alerts, Alertmanager config, Grafana provisioning, and dashboard JSON. Prometheus receives the same `METRICS_SHARED_KEY` through Compose config interpolation. |
| Backup/restore/retention jobs added | Satisfied. Added `backup-clickhouse`, `restore-clickhouse`, and `retention-materialize` run-once jobs with tests for registry, SQL generation, explicit restore URL, and job logging. |
| `./scripts/verify.sh` passed | Satisfied. Passed on 2026-05-05. |
| `FULL=1 ./scripts/verify.sh` passed or documented blocker | Satisfied. After stopping the conflicting `medallion-minio-1` and `medallion-http-fixture-1` containers, `FULL=1 ./scripts/verify.sh` passed on 2026-05-06 with fresh Compose volumes, bootstrap verify, and E2E. |

## Command Log

- `docker compose config >/tmp/oida-compose-config.txt && echo ok` passed.
- `docker run --rm -v "$PWD":/app -w /app golang:1.23@sha256:60deed95d3888cc5e4d9ff8a10c54e5edc008c6ae3fba6187be6fb592e19e8c0 go test ./cmd/api` passed.
- `docker run --rm -v "$PWD":/app -w /app golang:1.23@sha256:60deed95d3888cc5e4d9ff8a10c54e5edc008c6ae3fba6187be6fb592e19e8c0 go test ./cmd/control-plane` passed.
- `docker run --rm -v "$PWD":/app -w /app golang:1.23@sha256:60deed95d3888cc5e4d9ff8a10c54e5edc008c6ae3fba6187be6fb592e19e8c0 go test ./cmd/bootstrap ./internal/migrate` passed.
- `./scripts/verify.sh` passed.
- `FULL=1 ./scripts/verify.sh` initially exposed production-packaging gaps: the image was missing `migrations/`, `seed/`, backup assets, and source catalog markdown; those runtime assets are now copied into `docker/go.Dockerfile`.
- `FULL=1 ./scripts/verify.sh` then exposed a control-plane RBAC gap for generated discovery candidates; `osint_promote` now has explicit `INSERT` and `OPTIMIZE` privileges on `meta.discovery_candidate`.
- `FULL=1 ./scripts/verify.sh` also exposed a ClickHouse HTTP streaming edge case for generated discovery candidates; that path now emits `INSERT ... SELECT` and uses an explicit POST-body runner method.
- `FULL=1 ./scripts/verify.sh` passed on 2026-05-06. The gate ran `docker compose config`, `go test ./...`, static `CGO_ENABLED=0 go build ./...`, generated API docs checks, a fresh-volume Compose stack, `docker compose run --rm bootstrap verify`, and `go test ./test/e2e/... -tags=e2e -v -timeout=10m`.
