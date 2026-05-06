# Data Lifecycle

## Source of Truth

- Source catalog and seed: `seed/source_catalog.json`, `seed/source_catalog_compiled.json`, `seed/source_registry.json`
- Bootstrap loaders: `cmd/bootstrap/source_catalog.go`, `cmd/bootstrap/source_registry.go`, `cmd/bootstrap/source_bronze_migration.go`
- Discovery: `internal/discovery/`, `cmd/control-plane/jobs_discovery_candidates.go`, `cmd/control-plane/jobs_http_sources.go`
- Fetch and retention: `cmd/worker-fetch/main.go`, `internal/fetch/retention.go`
- Parse: `cmd/worker-parse/main.go`, `internal/parser/`
- Promotion: `cmd/control-plane/jobs_promote.go`, `internal/promote/pipeline.go`
- Metrics and API: `internal/metrics/`, `migrations/clickhouse/gold_api_views.sql`, `cmd/api/handlers.go`

## Runtime Behavior

The runtime lifecycle is:

1. Catalog and registry seed data are compiled or loaded by bootstrap.
2. Approved, enabled, HTTP-capable source rows land in `meta.source_registry`.
3. Discovery candidates and source templates can create frontier rows for crawlable sources.
4. `worker-fetch` leases `ops.crawl_frontier`, executes HTTP requests, records `ops.fetch_log`, and writes successful raw document metadata to `bronze.raw_document`; retained bodies are inline metadata or MinIO objects depending on retention class and size.
5. `worker-parse` selects raw documents, resolves the parser from source policy, parses JSON/CSV/XML/RSS/Atom/HTML-profile inputs, writes source-specific `bronze.src_*` rows, and records parse checkpoints/dead letters.
6. `control-plane run-once --job promote` selects parsed bronze windows, builds canonical promotion inputs, resolves location, and inserts silver facts/dimensions or unresolved-location queue rows.
7. Metrics are emitted as `silver.metric_contribution` rows and materialized into `gold.metric_state` and `gold.metric_snapshot`.
8. API routes query `gold.api_v1_*` views and return a stable envelope.

## Runtime Tables By Stage

- Governance: `meta.source_catalog`, `meta.source_registry`, `meta.source_family_template`, `meta.discovery_probe`, `meta.discovery_candidate`, `meta.source_generation_log`
- Work ledgers: `ops.job_run`, `ops.crawl_frontier`, `ops.fetch_log`, `ops.parse_log`, `ops.parse_checkpoint`, `ops.pipeline_run`
- Raw and bronze: `bronze.raw_document`, `bronze.raw_structured_row`, generated `bronze.src_*` tables
- Silver: `silver.dim_place`, `silver.place_polygon`, `silver.place_hierarchy`, `silver.dim_entity`, `silver.fact_observation`, `silver.fact_event`, `silver.fact_track_point`, `silver.fact_track_segment`, bridge tables, `silver.metric_contribution`
- Gold: `gold.metric_state`, `gold.metric_snapshot`, `gold.hotspot_snapshot`, `gold.cross_domain_snapshot`, `gold.api_v1_*`

## Deferred Or Catalog-Only Behavior

The source catalog includes concrete entries without `runtime_source_id`, fingerprint probes, and family templates. Those records support governance and discovery planning but do not imply that `worker-fetch` or `worker-parse` will run them today. A source is runtime-runnable only when it is materialized into `meta.source_registry` with enabled crawl policy, HTTP transport, parser routing, and any required credentials.

## Extension Knobs

- New data source: start in `seed/source_catalog.json`, then decide whether it should map to a runtime seed.
- New runtime source: add or synthesize a registry seed with `transport_type`, `entrypoints`, `allowed_hosts`, `auth_config_json`, `bronze_table`, `parser_id`, and `promote_profile`.
- New fetch behavior: change `cmd/worker-fetch/main.go` and `internal/fetch/retention.go`.
- New parser behavior: change `internal/parser/` and `cmd/worker-parse/main.go` routing if policy shape changes.
- New promotion behavior: change `internal/promote/pipeline.go` and `cmd/control-plane/jobs_promote.go`.
- New API exposure: add or change a `gold.api_v1_*` view and then update API resource specs.
