# System Specifications

This folder is the agent-facing specification for the current Go OSINT backend. Use it to answer "where do I change X?" before editing code. Existing `docs/` remains the place for API reference, schema standards, and operator runbooks.

## Reading Order

1. [system-architecture.md](system-architecture.md) for binaries, storage, boot sequence, and trust boundaries.
2. [data-lifecycle.md](data-lifecycle.md) for source catalog to API lifecycle.
3. [source-governance-and-catalog.md](source-governance-and-catalog.md) for source states, compiled catalog rules, runtime gates, and bronze routing.
4. [clickhouse-schema-contract.md](clickhouse-schema-contract.md) for database families, migrations, standard columns, and API views.
5. [parsers-and-promotion.md](parsers-and-promotion.md) for parser routing, canonical envelopes, promotion, and unresolved locations.
6. [domain-packs-and-metrics.md](domain-packs-and-metrics.md) for domain packs and metric materialization.
7. [orchestration-jobs.md](orchestration-jobs.md) for `control-plane run-once`, workers, job logs, and stored pipelines.
8. [api-and-auth.md](api-and-auth.md) for routes, response envelopes, scoped API keys, CORS, rate limits, and `/metrics`.
9. [operations-and-deployment.md](operations-and-deployment.md) for Compose, bootstrap, RBAC, backup/restore, monitoring, and verification.
10. [extension-playbooks.md](extension-playbooks.md) for concrete implementation recipes.

## Knobs To Twist

| Need | Start Here | Source of Truth |
| --- | --- | --- |
| Add or change source catalog entries | [source-governance-and-catalog.md](source-governance-and-catalog.md) | `seed/source_catalog.json`, `cmd/bootstrap/source_catalog.go` |
| Add runnable HTTP source seed behavior | [data-lifecycle.md](data-lifecycle.md) | `seed/source_registry.json`, `seed/source_catalog_compiled.json`, `cmd/bootstrap/source_registry.go` |
| Add source bronze tables | [clickhouse-schema-contract.md](clickhouse-schema-contract.md) | `cmd/bootstrap/source_bronze_migration.go`, `migrations/clickhouse/0015_*`, `0025_*` |
| Add parser behavior | [parsers-and-promotion.md](parsers-and-promotion.md) | `internal/parser/registry.go`, parser files in `internal/parser/` |
| Change canonical promotion | [parsers-and-promotion.md](parsers-and-promotion.md) | `internal/promote/pipeline.go`, `cmd/control-plane/jobs_promote.go` |
| Add or change metrics | [domain-packs-and-metrics.md](domain-packs-and-metrics.md) | `internal/metrics/`, `internal/packs/`, pack jobs |
| Add a control-plane job | [orchestration-jobs.md](orchestration-jobs.md) | `cmd/control-plane/main.go`, `cmd/control-plane/jobs_*.go` |
| Add an API route or field | [api-and-auth.md](api-and-auth.md) | `cmd/api/route_contracts.go`, `cmd/api/handlers.go`, `cmd/api/handlers_expanded.go`, `gold.api_v1_*` views |
| Change auth or API clients | [api-and-auth.md](api-and-auth.md) | `cmd/api/auth.go`, `cmd/bootstrap/api_clients.go`, `seed/api_clients.json`, `migrations/clickhouse/0035_api_clients.sql` |
| Change deployment or verification | [operations-and-deployment.md](operations-and-deployment.md) | `docker-compose.yml`, `cmd/bootstrap/main.go`, `scripts/verify.sh` |

## Runtime Versus Catalog

Runtime-backed behavior means code exists in service binaries or jobs and is exercised by tests or `./scripts/verify.sh`. Cataloged behavior means the source catalog names a capability or source, but no production worker path necessarily runs it. The current source catalog has 309 entries: 267 concrete, 16 fingerprint, and 26 family entries. Only 7 catalog entries are mapped to runnable seeds in the compiled catalog; many other entries are intentionally deferred and should not be described as live ingestion.

## Source of Truth

- Runtime services: `cmd/api`, `cmd/bootstrap`, `cmd/control-plane`, `cmd/worker-fetch`, `cmd/worker-parse`
- Shared behavior: `internal/discovery`, `internal/fetch`, `internal/parser`, `internal/promote`, `internal/metrics`, `internal/packs`, `internal/place`, `internal/migrate`
- Schema: `migrations/clickhouse`, `docs/schema-standards.md`
- Seeds and catalog: `seed/source_catalog.json`, `seed/source_catalog_compiled.json`, `seed/source_registry.json`, `seed/api_clients.json`
- Deployment and verification: `docker-compose.yml`, `scripts/verify.sh`, `docs/runbooks`

## Runtime Behavior

The product is a Go-only backend. Bootstrap applies ClickHouse migrations over HTTP, seeds source governance and API clients, uploads staged/bootstrap assets to MinIO, and writes a ready marker. Workers and control-plane jobs move data through discovery, fetch, parse, promotion, metrics, and API-facing `gold.api_v1_*` views. The API is read-only and protects `/v1/*` data routes with scoped hashed API keys.

## Extension Knobs

- Prefer the playbooks in [extension-playbooks.md](extension-playbooks.md) before changing code.
- Keep migrations append-only and ClickHouse HTTP-only.
- Keep filterable API fields typed in views and handlers.
- Keep source seed evolution governance-aware so kill switches and `seed_checksum` behavior survive reseeding.
- Keep documentation changes in this folder aligned with source files in the same change.
