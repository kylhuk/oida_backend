# ClickHouse Schema Contract

## Source of Truth

- Migrations: `migrations/clickhouse/`
- Migration runner: `internal/migrate/http_runner.go`, `internal/migrate/split.go`
- Schema standards: `docs/schema-standards.md`, `internal/migrate/schema_standards_test.go`
- API views: `migrations/clickhouse/gold_api_views.sql`, `migrations/clickhouse/0007_api_expansion_views.sql`, later view migrations
- Bootstrap verification: `cmd/bootstrap/main.go`

## Runtime Behavior

The application owns five logical ClickHouse databases: `meta`, `ops`, `bronze`, `silver`, and `gold`.

- `meta` stores source governance, parser registry, metric registry, API schema registry, schema migration/change ledgers, API clients, source templates, discovery probes, and pipeline definitions.
- `ops` stores operational ledgers: jobs, frontier, fetch, parse, unresolved locations, quality incidents, bulk stage, checkpoints, pipeline runs, and observability tail data.
- `bronze` stores raw document metadata and source-specific parsed bronze rows.
- `silver` stores canonical facts, dimensions, bridges, place graph tables, and metric contributions.
- `gold` stores materialized metric state/snapshots, cross-domain/hotspot outputs, and `gold.api_v1_*` API views.

Migrations are append-only files under `migrations/clickhouse/`. `HTTPRunner.CheckAppliedMigration()` rejects checksum drift for already-applied versions. `meta.schema_migrations` is bootstrap-owned.

Schema-bearing tables follow the standards in `docs/schema-standards.md`: `schema_version`, `record_version`, `api_contract_version`, `updated_at`, `attrs`, and `evidence` for registries and contract-bearing tables. Hot filter fields stay typed and should not be buried in JSON.

## API View Contract

The API handlers query `gold.api_v1_*` views using `FORMAT JSONEachRow`. Route specs in `cmd/api/handlers.go` and `cmd/api/handlers_expanded.go` list selectable fields, allowed filters, id columns, and search columns. A field exposed by the API must exist in the matching gold view.

Current API view families include sources, source coverage, places, entities, entity events/places, observations, events, jobs, metrics, metric rollups, time series, hotspots, tracks, and cross-domain analytics.

## Runtime-Backed Schema Evolution

Bootstrap verifies table engines, required columns, governance row counts, API client rows, source catalog rows, bronze manifest/live table parity, and source silver coverage contract. `./scripts/verify.sh` also runs schema tests and builds all binaries.

## Deferred Or Catalog-Only Behavior

Bronze tables can exist for catalog manifest parity even when a source is not actively fetched or promoted. Their presence is not proof of runtime ingestion.

## Extension Knobs

- Add schema changes as a new ordered migration; do not modify applied migrations.
- Update `docs/schema-standards.md` and schema tests when introducing a deliberate convention change.
- For API changes, update the `gold.api_v1_*` view and the matching `resourceSpec`.
- For new source bronze table generation, update the catalog compiler and generated migration path.
- For new registry-like tables, use `ReplacingMergeTree(record_version)` and standard metadata columns.
