# PROJECT KNOWLEDGE BASE

**Generated:** 2026-03-10
**Commit:** 4a230ab
**Branch:** main

## OVERVIEW
Production-oriented Go 1.23 OSINT backend. Multi-binary repo with ClickHouse HTTP, MinIO-backed bootstrap, contract-tested API, run-once control-plane jobs, and domain packs that materialize metrics into `gold` views.

## STRUCTURE
```
./
├── cmd/
│   ├── api/                  # REST surface over gold.api_v1_* views
│   ├── bootstrap/            # install/verify lifecycle, RBAC, buckets, seed load
│   ├── control-plane/        # run-once jobs: place-build, promote, ingest-*
│   ├── renderer/             # health-only stub on :8090
│   ├── worker-fetch/         # crawler + retention pipeline
│   └── worker-parse/         # parser worker CLI
├── internal/
│   ├── discovery/            # frontier, feed, sitemap, robots discovery flow
│   ├── metrics/              # shared metric registry, contributions, rollups
│   ├── migrate/              # ClickHouse HTTP runner + migration invariants
│   ├── packs/                # aviation / geopolitical / maritime / safety / space
│   ├── parser/               # format-specific extraction + parser registry
│   ├── place/                # place graph fixtures, polygons, reverse geocoder
│   └── promote/              # canonical stage -> silver / ops SQL generation
├── migrations/clickhouse/    # ordered schema history `0001_*.sql`
├── infra/clickhouse/         # single-node + optional cluster config
├── seed/                     # source registry seed and staged assets
├── test/e2e/                 # compose-backed end-to-end tests (`-tags=e2e`)
└── docs/                     # runbooks + schema standards
```

## WHERE TO LOOK

| Task | Location | Notes |
|------|----------|-------|
| Add or change API list/detail behavior | `cmd/api/handlers.go` | Resource surface is driven by `resourceSpec` + `gold.api_v1_*` views |
| Add API route wiring | `cmd/api/main.go` | Register route in `newAPIMuxWithServer()` |
| Change bootstrap lifecycle | `cmd/bootstrap/main.go` | `install()` and `verify()` stay symmetric |
| Evolve source governance seed | `cmd/bootstrap/source_registry.go` | Preserve kill-switch state and `seed_checksum` behavior |
| Add run-once job | `cmd/control-plane/main.go` + `cmd/control-plane/jobs_*.go` | Jobs self-register in `init()` into `jobRegistry` |
| Change discovery / frontier behavior | `internal/discovery/` | Frontier, robots, feed, and sitemap logic stay together |
| Change shared metrics plumbing | `internal/metrics/` | Registry, contributions, rollups, materialization SQL |
| Change migration mechanics | `internal/migrate/http_runner.go` | HTTP-only ClickHouse runner; checksums are immutable |
| Change parser selection / format extraction | `internal/parser/` | Registry-driven parser composition |
| Add SQL migration | `migrations/clickhouse/` | Keep numeric ordering and idempotent DDL |
| Change place graph / reverse geocoder | `internal/place/materialize.go` + `cmd/control-plane/jobs_place_build.go` | Bundle build is fixture-driven, then H3 coverage is added |
| Change canonical promotion | `internal/promote/pipeline.go` | `Prepare()` builds rows, `SQLStatements()` emits insert SQL |
| Add domain ingest logic | `internal/packs/` + `cmd/control-plane/jobs_*.go` | Packs emit registry, facts, and metrics together |
| Run E2E pipeline | `test/e2e/pipeline_test.go` | Requires compose stack and `go test ./test/e2e/... -tags=e2e` |

## CODE MAP

| Symbol | Type | Location | Role |
|--------|------|----------|------|
| `newAPIMuxWithServer()` | func | `cmd/api/main.go:28` | Route registry for the HTTP API surface |
| `newResourceSpec()` | func | `cmd/api/handlers.go:260` | Normalizes allowed fields and filter metadata |
| `install()` | func | `cmd/bootstrap/main.go:246` | Bootstraps buckets, RBAC, migrations, seed, assets, ready marker |
| `loadSourceSeed()` | func | `cmd/bootstrap/source_registry.go:126` | Governance-aware source seed loader |
| `runOnce()` | func | `cmd/control-plane/main.go:68` | Deterministic one-job CLI contract |
| `BuildBundle()` | func | `internal/place/materialize.go:142` | Materializes place rows, polygons, hierarchy, fixtures |
| `Prepare()` | method | `internal/promote/pipeline.go:186` | Canonical input -> row plan |
| `HTTPRunner` | struct | `internal/migrate/http_runner.go:12` | ClickHouse HTTP execution layer |

## CONVENTIONS

- Use `getenv()` helpers with defaults in service entrypoints instead of hardcoding env handling.
- API responses always use the envelope shape from `respondStatus()` with `api_version`, `schema_version`, `generated_at`, and `data`.
- API list/detail endpoints query ClickHouse `gold.api_v1_*` views and decode `JSONEachRow`; filterable fields stay typed, not buried in JSON.
- Schema-bearing ClickHouse tables use `schema_version`, `record_version`, `api_contract_version`, `updated_at`, `attrs`, and `evidence`; see `docs/schema-standards.md`.
- Migrations are append-only, ordered `000N_name.sql`, and recorded in `meta.schema_migrations` with checksum validation.
- Run-once jobs live under `cmd/control-plane/jobs_*.go` and register themselves in `init()`.
- End-to-end tests live in `test/e2e/` behind the `e2e` build tag; package tests otherwise stay beside source.

## ANTI-PATTERNS (THIS PROJECT)

- Do not add dependencies to `go.mod` without clear need.
- Do not use CGO; Dockerfiles assume static Go builds.
- Do not use the ClickHouse native protocol for app logic; use HTTP `:8123`.
- Do not change applied migration SQL in place; checksum drift is treated as an error.
- Do not hide hot filter fields inside `attrs` or `evidence`.
- Do not write secrets into code or seed data; auth config points to env vars.

## CHILD FILES

- `cmd/AGENTS.md`
- `cmd/api/AGENTS.md`
- `cmd/bootstrap/AGENTS.md`
- `cmd/control-plane/AGENTS.md`
- `docs/runbooks/AGENTS.md`
- `infra/AGENTS.md`
- `test/AGENTS.md`
- `internal/discovery/AGENTS.md`
- `internal/migrate/AGENTS.md`
- `internal/metrics/AGENTS.md`
- `internal/parser/AGENTS.md`
- `internal/place/AGENTS.md`
- `internal/promote/AGENTS.md`
- `internal/packs/AGENTS.md`
- `migrations/clickhouse/AGENTS.md`

## COMMANDS

```bash
docker compose up -d --build
docker compose run --rm bootstrap verify
go test ./...
go test ./test/e2e/... -tags=e2e
go run ./cmd/control-plane run-once --help
CGO_ENABLED=0 go build ./...
```

## NOTES

- `docker-compose.yml` is the default single-node topology; cluster scale-out lives under `infra/clickhouse/cluster/` and `docker-compose.cluster.yml`.
- `cmd/renderer` is still intentionally minimal; most real application behavior sits in API, bootstrap, control-plane, and internal packages.
- `seed/source_registry.json` and `cmd/bootstrap/source_registry.go` are coupled; change both conventions together.
- The old root AGENTS description of a Phase A scaffold is stale; prefer README + package code over that older framing.
