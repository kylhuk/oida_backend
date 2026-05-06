# Operations And Deployment

## Source of Truth

- Compose topology: `docker-compose.yml`
- Runtime image: `docker/go.Dockerfile`
- Bootstrap lifecycle and RBAC: `cmd/bootstrap/main.go`
- Verification gate: `scripts/verify.sh`, `Makefile`
- Runbooks: `docs/runbooks/fresh-bootstrap.md`, `backup-restore.md`, `kill-switch.md`, `upgrade-migration.md`, `cluster-scale-out.md`, `unresolved-triage.md`
- Monitoring: `infra/prometheus/`, `infra/alertmanager/`, `infra/grafana/`

## Runtime Behavior

The default production target is the single-node Docker Compose topology. Compose runs ClickHouse, MinIO, bootstrap, API, control-plane, worker-fetch, worker-parse, HTTP fixtures for tests, Prometheus, Alertmanager, and Grafana.

Go service containers are built from `docker/go.Dockerfile`, run read-only with `/tmp` tmpfs, use `no-new-privileges`, and restart unless stopped except for one-shot services. Third-party images are pinned by digest.

Bootstrap owns:

- MinIO bucket checks.
- Logical databases `meta`, `ops`, `bronze`, `silver`, `gold`.
- ClickHouse roles and users: reader/API, ingest/fetch/parse, promote/control-plane, admin/bootstrap.
- Migration application and checksum validation.
- Source governance seed load.
- API client seed load.
- Stage and backup asset registration.
- Ready marker creation.

## Monitoring And Backup

`api` exposes `/metrics` for Prometheus with a shared bearer key. Prometheus loads alert rules from `infra/prometheus/alerts.yml`; Grafana provisioning and dashboards live under `infra/grafana/`.

Backups use native ClickHouse `BACKUP` to S3-compatible MinIO paths under the backup bucket. Restores require explicit `CLICKHOUSE_RESTORE_URL` and the `restore-clickhouse` job. Retention materialization can be forced by `retention-materialize`.

## Verification

The default gate is `./scripts/verify.sh`. It runs:

- `docker compose config`
- Dockerized `go test ./...`
- Dockerized static `CGO_ENABLED=0 go build -buildvcs=false ./...`
- API generated docs and route contract tests

With `FULL=1`, it resets Compose volumes, starts the stack, waits for `/v1/ready`, runs `bootstrap verify`, and executes `go test ./test/e2e/... -tags=e2e -v -timeout=10m` with host networking.

## Deferred Or Out Of Scope

Kubernetes manifests, OIDC/JWT identity provider integration, and a custom frontend are not part of the current deployment contract. Cluster scale-out is documented separately under `infra/clickhouse/cluster/` and the cluster runbook, not the default target.

## Extension Knobs

- Add deployment services in `docker-compose.yml` and ensure `docker compose config` remains valid.
- Add bootstrap-owned runtime assets to `docker/go.Dockerfile` when services need files at runtime.
- Add RBAC grants in `cmd/bootstrap/main.go` with matching verification.
- Add monitoring rules or dashboards under `infra/prometheus` and `infra/grafana`.
- Add operator instructions under `docs/runbooks`, not in `specifications/`, when the content is procedural.
