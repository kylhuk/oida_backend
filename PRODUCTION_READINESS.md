# Production Readiness

This checklist tracks the Go-only OIDA backend after the production-readiness cleanup.

## Current Status

Status: implementation in progress, final gate pending.

The production target is a single-node Docker Compose deployment with ClickHouse, MinIO, Go services, and the monitoring stack. Optional cluster scale-out remains documented separately.

## Readiness Matrix

| Area | Status | Evidence |
| --- | --- | --- |
| Runtime ownership | implemented | Legacy Python runtime, tests, dependencies, and services removed from the production path. |
| Compose topology | implemented | Single-node Compose includes ClickHouse, MinIO, bootstrap, API, control-plane, fetch, parse, HTTP fixtures, Prometheus, Alertmanager, and Grafana. |
| Image pinning | implemented | Third-party images and Go builder/runtime images are pinned by digest. |
| Non-root runtime | implemented | Go services build into a distroless non-root image. |
| Verification gate | implemented | `scripts/verify.sh` runs Compose config, Go tests, static builds, and generated API contract checks. |
| API authentication | implemented | Protected routes use `X-API-Key`, SHA-256 hashes in `meta.api_clients`, and route scopes. |
| API rate limiting | implemented | `API_RATE_LIMIT_RPS` and `API_RATE_LIMIT_BURST` enforce per-key/IP token buckets. |
| Observability | implemented | `/metrics` exposes Prometheus text behind `METRICS_SHARED_KEY`; Prometheus, Alertmanager, and Grafana are wired in Compose. |
| Backup and restore | implemented | `backup-clickhouse` and `restore-clickhouse` use native ClickHouse S3-compatible backup paths. |
| Retention | implemented | Table TTLs exist in migrations; `retention-materialize` forces TTL materialization. |
| Runbooks | implemented | Fresh bootstrap, migration, backup/restore, kill switch, and triage runbooks are present. |
| Final verification | pending | Run `./scripts/verify.sh`, then `FULL=1 ./scripts/verify.sh` when Docker services are available. |

## Required Commands

```sh
./scripts/verify.sh
FULL=1 ./scripts/verify.sh
```

## Operator Notes

- Replace `seed/api_clients.json` with environment-specific hashed clients before production bootstrap.
- Keep raw API keys out of seed files and source control.
- Prometheus and the API read the same `METRICS_SHARED_KEY` through Compose config interpolation.
- Restore requires an explicit `CLICKHOUSE_RESTORE_URL`; there is no implicit backup selection.

## Quick Reference

```sh
docker compose up -d --build
docker compose run --rm bootstrap verify
docker compose run --rm control-plane run-once --job backup-clickhouse
docker compose run --rm control-plane run-once --job retention-materialize
curl -fsS http://localhost:8080/v1/ready
```

Services:
- API: `http://localhost:8080`
- ClickHouse HTTP: `http://localhost:8124`
- MinIO console: `http://localhost:9001`
- Grafana: `http://localhost:3001`
