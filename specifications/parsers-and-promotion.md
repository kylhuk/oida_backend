# Parsers And Promotion

## Source of Truth

- Parser registry and descriptors: `internal/parser/registry.go`
- Parser implementations: `internal/parser/json.go`, `csv.go`, `xml.go`, `structured_test.go`
- Worker parse loop: `cmd/worker-parse/main.go`
- Canonical envelope: `internal/canonical/envelope.go`, `internal/canonical/evidence.go`, `internal/canonical/id.go`
- Promotion pipeline: `internal/promote/pipeline.go`
- Promotion job: `cmd/control-plane/jobs_promote.go`
- Location resolution and unresolved queue: `internal/location/`, `internal/place/`, `migrations/clickhouse/0005_baseline_tables.sql`

## Runtime Behavior

`parser.DefaultRegistry()` registers JSON, CSV, XML, RSS, Atom, and HTML-profile parsers. Parser resolution prefers an explicit `ParserID`; otherwise it uses `format_hint` and `Content-Type` candidates. Each parser emits canonical `RecordEnvelope` candidates with parser evidence and raw-document evidence.

`worker-parse` reads source parse policy from `meta.source_registry`, replays raw bodies from inline metadata or MinIO, invokes the parser registry, lands rows into the source's configured bronze table, and records parse checkpoint state. Failed parses can retry or dead-letter according to retry policy.

`promote.NewPipeline().Prepare()` normalizes candidate inputs, requires `record_kind`, builds deterministic observation/event/entity identifiers, and requires a resolved location with confidence at or above `DefaultMinLocationConfidence` unless the row should enter the unresolved queue. `Plan.SQLStatements()` emits SQL for silver entities, observations, events, and unresolved locations.

The `promote` run-once job can use explicit input overrides through `PROMOTE_PIPELINE_INPUT` or `PROMOTE_PIPELINE_INPUT_JSON`, or it can select bronze windows using checkpoint state, `--source-id`, `--window-start`, `--window-end`, and `--delta-only`.

## Canonical Record Shapes

Supported promotion record kinds are:

- `observation`: lands in `silver.fact_observation` and may emit entities.
- `event`: lands in `silver.fact_event` and may emit entities/bridges.
- `entity`: lands in `silver.dim_entity`.

Rows with unresolved or low-confidence location data are written to `ops.unresolved_location_queue` rather than forced into silver facts.

## Deferred Or Catalog-Only Behavior

A parser descriptor can exist before every source using that parser is runtime-enabled. HTML-profile parsing exists, but catalog entries that reference it remain deferred unless their source is materialized with runtime policy and parse config.

## Extension Knobs

- Add a parser by implementing `parser.Parser`, registering it in `DefaultRegistry()`, and adding registry/tests.
- Add source-specific parse routing by setting `parser_id`, `format_hint`, `parse_config_json`, `bronze_table`, and `promote_profile` in source governance.
- Add canonical fields in `internal/canonical` and update parser output tests.
- Change promotion row construction in `internal/promote/pipeline.go` and its tests.
- Change promote orchestration or checkpoint selection in `cmd/control-plane/jobs_promote.go`.
- Add location handling in `internal/location` or place graph materialization in `internal/place`.
