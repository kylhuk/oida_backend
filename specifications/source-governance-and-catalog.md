# Source Governance And Catalog

## Source of Truth

- Catalog artifact: `seed/source_catalog.json`
- Compiled artifact: `seed/source_catalog_compiled.json`
- Legacy/runtime seed: `seed/source_registry.json`
- Catalog compiler and validators: `cmd/bootstrap/source_catalog.go`
- Registry loader and governance behavior: `cmd/bootstrap/source_registry.go`
- Source bronze migration generation: `cmd/bootstrap/source_bronze_migration.go`
- Governance migrations: `migrations/clickhouse/0002_core_tables.sql`, `0006_source_governance.sql`, `0012_source_registry_http_contract.sql`, `0016_source_catalog_contract.sql`, `0017_source_generation_governance.sql`, `0019_source_family_template_contract.sql`

## Runtime Behavior

`bootstrap compile-catalog` combines `seed/source_catalog.json` and `seed/source_registry.json` into `seed/source_catalog_compiled.json` and generated bronze DDL. At install time bootstrap loads the compiled catalog into governance tables and loads runnable source seeds into `meta.source_registry`.

Catalog entry kinds:

- `concrete`: a named source. It is runtime-runnable only when it has a valid runtime mapping or synthesized seed.
- `fingerprint`: a probe pattern used to detect source or data-platform candidates.
- `family`: a template for generated child sources that require review before runtime materialization.

The current catalog has 309 entries: 267 concrete, 16 fingerprint, 26 family. The compiled artifact contains 7 runnable seeds, 16 fingerprint probes, 26 family templates, and 267 bronze manifest rows. Entries with a non-empty `deferred_reason` or no `runtime_source_id` must be treated as catalog-only until onboarding work makes them runnable.

`loadSourceSeed()` preserves operational governance. When a seed changes, the loader writes a new version and stores a stable `seed_checksum` in `attrs`; unchanged seeds do not churn versions, and kill-switch state remains authoritative.

## Runtime Gates

A source is expected to run only when:

- `catalog_kind='concrete'`
- `enabled=1`
- `crawl_enabled=1`
- `review_status='approved'`
- `transport_type='http'`
- `bronze_table` is set for parsed source-specific bronze landing
- `parser_id` resolves in the parser registry
- Required credential environment variables are present at runtime

Worker and discovery code also enforce allowed hosts, robots policy, request rate/burst, retention class, and supported auth mode.

## Bronze Routing

Source-specific bronze tables are generated from the compiled catalog manifest and migrations. `meta.source_silver_coverage` reports whether each in-scope source lands in silver, is view-only, lacks credentials, produces only unresolved rows, has no promotable rows, or uses an unsupported profile.

## Deferred Or Catalog-Only Behavior

Catalog rows with `integration_archetype` values such as `html_profile`, `bulk_file`, `catalog_ckan`, `catalog_socrata`, `arcgis_rest`, `ogc_records`, `ogc_features`, `stac_api`, or `discovery_web` may be present without a runtime fetch/parse/promotion path. Do not describe them as active ingestion unless `meta.source_registry` runtime policy and worker support both exist.

## Extension Knobs

- Add governance-only source knowledge in `seed/source_catalog.json`.
- Add runnable source behavior by creating a valid runtime mapping and registry seed.
- Add new catalog kind or archetype validation in `cmd/bootstrap/source_catalog.go`.
- Add source governance columns through append-only migrations and mirror loader changes in `cmd/bootstrap/source_registry.go`.
- Add bronze table generation changes in `cmd/bootstrap/source_bronze_migration.go` and validate with bootstrap tests.
