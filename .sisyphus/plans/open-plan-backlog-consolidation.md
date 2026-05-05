# Open Plan Backlog Consolidation

## TL;DR
> **Summary**: This file is the post-consolidation source of truth for all previously open plan work. It preserves every unchecked task, keeps source provenance, and adds execution-ready context so the deleted source plan files are no longer required for task start-up.
> **Deliverables**:
> - one current backlog file containing all `110` numbered carryover tasks and `40` legacy final-verification tasks
> - source-plan manifest documenting the ten superseded plan files and their carried work counts
> - per-task execution context: scope note, starting refs, verification command, and first QA scenario
> - no remaining dependency on deleted `.sisyphus/plans/*.md` files for task execution
> **Effort**: Large
> **Parallel**: YES - by source-plan section
> **Critical Path**: use the carried task blocks directly; the consolidation itself is already complete

## Context
### Original Request
Check all the plans inside `@.sisyphus/plans/` and gather all open tasks into a new plan. Afterwards, delete all old plans.

### Current State
- The consolidation is already complete and `.sisyphus/plans/` now contains one file: `open-plan-backlog-consolidation.md`.
- Carried work totals: `110` numbered tasks and `40` legacy final-verification tasks across `10` superseded source plans.
- The source plan files were intentionally removed from `.sisyphus/plans/` after carryover, so this file now carries the execution context needed to start each task.

### Metis Review (gaps addressed)
- Legacy final-verification gates remain distinct from normal numbered work items.
- Source provenance survives through the superseded-plan manifest and per-section source headings.
- The consolidated backlog now carries task-ready context instead of relying on deleted source plan files.
- File-parity checks remain, but they are treated as historical carryover proof rather than a future execution workflow.

## Work Objectives
### Core Objective
Keep one execution-ready backlog file that preserves every previously open planning task and all source-plan provenance after retiring the ten superseded plan files.

### Deliverables
- One carryover backlog section per superseded source plan.
- For each numbered task: a carried `What to do` note, starting references, verification command, and first QA scenario summary.
- One superseded-plan manifest with original filenames and open-task counts.
- A final directory inventory where `.sisyphus/plans/` contains only this consolidated plan.

### Definition of Done (verifiable conditions with commands)
- `python3 - <<'PY'
from pathlib import Path
plans = sorted(p.name for p in Path('.sisyphus/plans').glob('*.md'))
print('\n'.join(plans))
PY` prints only `open-plan-backlog-consolidation.md`.
- `grep -c '^- \[ \] [0-9]\+\.' .sisyphus/plans/open-plan-backlog-consolidation.md` prints `110`.
- `grep -c '^- \[ \] F[0-9]\+\.' .sisyphus/plans/open-plan-backlog-consolidation.md` prints `40`.
- `grep -c '^  - What to do:' .sisyphus/plans/open-plan-backlog-consolidation.md` prints `110`.
- `grep -c '^  - QA:' .sisyphus/plans/open-plan-backlog-consolidation.md` prints `110`.

### Must Have
- Preserve every unchecked top-level task label and title from the superseded plans.
- Preserve original source-plan grouping and filename provenance.
- Preserve legacy final-verification tasks as separate approval gates.
- Carry enough execution context that deleted source plan files are not required to start numbered tasks.

### Must NOT Have
- No deduplication, reprioritization, or semantic narrowing of carried tasks.
- No new open-task checkboxes beyond the carried source-plan tasks.
- No dependence on deleted `.sisyphus/plans/*.md` files inside carried task execution notes.
- No edits outside `.sisyphus/`.

## Verification Strategy
> ZERO HUMAN INTERVENTION - all verification is agent-executed.
- Test decision: exact file-count parity plus carried task-level verification commands and QA summaries.
- QA policy: every numbered carried task includes one concrete verification command and one first-scenario QA summary.
- Evidence: the consolidated plan file itself plus the final `.sisyphus/plans/` directory inventory.

## Superseded Plan Manifest
- `evidence-generation-automation.md` - `Evidence Generation Automation` - `6` numbered tasks + `4` legacy final-verification tasks
- `urgent-telemetry-source-expansion.md` - `Urgent Telemetry Source Expansion` - `12` numbered tasks + `4` legacy final-verification tasks
- `rest-api-frontend-contract-hardening.md` - `REST API Frontend Contract Hardening` - `6` numbered tasks + `4` legacy final-verification tasks
- `frontend-rest-readiness-remediation.md` - `Frontend REST Readiness Remediation` - `9` numbered tasks + `4` legacy final-verification tasks
- `silver-coverage-rollout.md` - `Silver Coverage Rollout` - `8` numbered tasks + `4` legacy final-verification tasks
- `sources-rollout.md` - `Full Source Catalog Rollout and Incremental Runtime Sync` - `13` numbered tasks + `4` legacy final-verification tasks
- `stats-dashboard-ui.md` - `Internal Stats Dashboard UI` - `7` numbered tasks + `4` legacy final-verification tasks
- `http-source-bronze-elt.md` - `HTTP Source Adapter and Source-Specific Bronze ELT` - `14` numbered tasks + `4` legacy final-verification tasks
- `global-osint-backend-delta-closure.md` - `Global OSINT Backend Delta Closure Plan` - `9` numbered tasks + `4` legacy final-verification tasks
- `global-osint-backend-completion.md` - `Global OSINT Backend Completion Plan` - `26` numbered tasks + `4` legacy final-verification tasks

## TODOs
> Carryover numbering intentionally resets inside each source-plan section so each task remains traceable to its original plan.

## Source Plan: Evidence Generation Automation (`evidence-generation-automation.md`)
- Original scope summary: Automate task-scoped verification artifacts for the urgent telemetry rollout by emitting deterministic-suite and live-smoke evidence directly from the host-run E2E test flow, with stable filenames, explicit pass/fail/skip status, and secret-safe contents.
- Carried numbered tasks: `6`
- Carried legacy final-verification tasks: `4`

### Numbered Tasks
- [x] 1. Freeze the task-12 evidence contract in `test/e2e`
  - What to do: Add exact task-12 evidence filename constants and a deterministic evidence schema in `test/e2e` for deterministic and live-smoke verification output.
  - Start here: `test/e2e/pipeline_test.go:25`; `test/e2e/pipeline_test.go:99`; `internal/parser/structured_test.go:11`
  - Verify: `go test ./test/e2e/... -tags=e2e -run TestTask12EvidenceContract -count=1`
  - QA: `Tool`: Bash | `Steps`: Run the contract test, then grep `test/e2e` for the two exact task-12 evidence filenames. | `Expected`: The contract test passes and the exact filenames are frozen without colliding with parser `task-12-parse*` artifacts.

- [x] 2. Add an E2E-local evidence writer with cleanup-based persistence
  - What to do: Create a shared E2E-local evidence writer that uses repo-root discovery and cleanup/deferred finalization to persist host-visible artifacts under `.sisyphus/evidence/`.
  - Start here: `test/e2e/pipeline_test.go:1024`; `internal/promote/pipeline_test.go:168`; `.github/workflows/e2e.yml:50`
  - Verify: `go test ./test/e2e/... -tags=e2e -run TestEvidenceWriter -count=1`
  - QA: `Tool`: Bash | `Steps`: Delete the helper artifact, run the helper test, and inspect the written file under `.sisyphus/evidence/`. | `Expected`: The helper writes a deterministic host-visible artifact and remains scoped to `test/e2e` only.

- [x] 3. Add helper-level tests for pass/fail/skip-safe finalization and redaction
  - What to do: Add helper-level coverage for pass/fail/skip-safe finalization, exact filename targeting, deterministic field order, and secret-safe output.
  - Start here: `internal/parser/structured_test.go:11`; `internal/promote/pipeline_test.go:68`; `internal/place/coverage_test.go:61`
  - Verify: `go test ./test/e2e/... -tags=e2e -run 'TestEvidenceWriter|TestTask12EvidenceContract' -count=1`
  - QA: `Tool`: Bash | `Steps`: Run helper-focused tests and grep the generated helper artifact for secret-like values. | `Expected`: Tests pass, exact filenames are asserted, and no bearer token or client-secret markers appear in the artifact.

- [x] 4. Emit deterministic-suite evidence from `TestHTTPSourcePipeline`
  - What to do: Instrument `TestHTTPSourcePipeline` so deterministic-suite evidence is always emitted from cleanup/deferred logic, including failed paths.
  - Start here: `test/e2e/pipeline_test.go:25`; `test/e2e/pipeline_test.go:38`; `.github/workflows/e2e.yml:47`
  - Verify: `rm -f .sisyphus/evidence/task-12-deterministic-suite.txt && go test ./test/e2e/... -tags=e2e -run TestHTTPSourcePipeline -count=1`
  - QA: `Tool`: Bash | `Steps`: Remove the deterministic evidence file, run the targeted deterministic suite, and inspect the emitted artifact. | `Expected`: The file is recreated with `suite=deterministic` and survives both success and controlled failure paths.

- [x] 5. Emit sanitized live-smoke evidence from `TestOptionalLiveSmokeOpenSky`
  - What to do: Instrument `TestOptionalLiveSmokeOpenSky` so live-smoke evidence is always written with explicit skipped/pass/fail status and no secret leakage.
  - Start here: `test/e2e/pipeline_test.go:99`; `test/e2e/pipeline_test.go:103`; `test/e2e/pipeline_test.go:108`
  - Verify: `rm -f .sisyphus/evidence/task-12-live-smoke.txt && go test ./test/e2e/... -tags=e2e -run TestOptionalLiveSmokeOpenSky -count=1`
  - QA: `Tool`: Bash | `Steps`: Run the live-smoke test once with env unset and inspect the emitted artifact for status and redaction. | `Expected`: The file is created even when skipped and it contains no raw token, bearer header, or secret value.

- [x] 6. Verify end-to-end command compatibility and preserve CI artifact collection
  - What to do: Confirm local and CI verification commands produce both task-12 evidence files and keep `.github/workflows/e2e.yml` aligned with `.sisyphus/evidence/` artifact upload.
  - Start here: `.github/workflows/e2e.yml:47`; `.github/workflows/e2e.yml:50`; `test/e2e/pipeline_test.go:95`
  - Verify: `go test ./test/e2e/... -tags=e2e -run TestHTTPSourcePipeline -count=1`
  - QA: `Tool`: Bash | `Steps`: Run the deterministic and live-smoke commands, then verify both exact files exist and the workflow still uploads `.sisyphus/evidence/`. | `Expected`: Both artifacts are produced locally and CI collection still points at the correct evidence directory.

### Legacy Final Verification
- [x] F1. Plan Compliance Audit — oracle
- [x] F2. Code Quality Review — unspecified-high
- [x] F3. Real Manual QA — unspecified-high (+ playwright if UI)
- [x] F4. Scope Fidelity Check — deep

## Source Plan: Urgent Telemetry Source Expansion (`urgent-telemetry-source-expansion.md`)
- Original scope summary: Replace doc-page crawls with real aviation and maritime machine endpoints, onboard additional overlapping sources, and land each source through its own bronze table and source-specific materialized view into shared `silver` tables.
- Carried numbered tasks: `12`
- Carried legacy final-verification tasks: `4`

### Numbered Tasks
- [x] 1. Freeze canonical source set, IDs, landing targets, and explicit deferrals
  - What to do: Freeze the phase-1 source contract: canonical IDs, shared silver targets, and explicit source deferrals for anything not executable in this rollout.
  - Start here: `seed/source_catalog.json:2446`; `seed/source_catalog.json:2786`; `migrations/clickhouse/0021_source_silver_coverage_lineage.sql:126`
  - Verify: `go test ./cmd/bootstrap/...`
  - QA: `Tool`: Bash | `Steps`: Run bootstrap catalog tests and inspect the compiled source inventory for the exact phase-1 list and target tables. | `Expected`: Each approved phase-1 source has one canonical ID, one target shared silver table, and deferred sources are explicitly blocked from accidental runtime use.

- [x] 2. Replace doc-derived runtime entrypoints with explicit endpoint templates and corrected auth contracts
  - What to do: Remove docs-page runtime fallbacks and compile explicit machine endpoints plus provider-correct auth metadata for the approved sources.
  - Start here: `cmd/bootstrap/source_catalog.go:603`; `cmd/bootstrap/source_catalog.go:624`; `internal/packs/aviation/opensky.go:17`
  - Verify: `go test ./cmd/bootstrap/... ./cmd/worker-fetch/...`
  - QA: `Tool`: Bash | `Steps`: Run bootstrap and fetch package tests, then inspect runtime entrypoints and auth config for the phase-1 source IDs. | `Expected`: No approved source compiles to a docs URL and auth placement matches the provider contract.

- [x] 3. Extend worker-fetch auth delivery, request templating, and provider-specific request building
  - What to do: Extend `worker-fetch` with provider-aware token/query/header auth and request builders for OpenSky, AISHub, Airplanes.live, ADSB.lol, and OpenAIP.
  - Start here: `cmd/worker-fetch/main.go:1161`; `cmd/worker-fetch/main.go:1173`; `internal/packs/aviation/opensky.go:39`
  - Verify: `go test ./cmd/worker-fetch/...`
  - QA: `Tool`: Bash | `Steps`: Run provider-specific request-builder tests and the full fetch suite. | `Expected`: Per-provider URLs, headers, and query params are correct and existing generic fetch behavior does not regress.

- [x] 4. Add or verify per-source bronze DDL and create one MV per approved source into shared silver tables
  - What to do: Add append-only bronze DDL and one materialized view per approved phase-1 source into the assigned shared silver destination.
  - Start here: `migrations/clickhouse/0015_source_bronze_tables.sql:1`; `migrations/clickhouse/0025_source_bronze_tables_expanded.sql:141`; `cmd/bootstrap/source_catalog_test.go:185`
  - Verify: `docker compose run --rm bootstrap verify`
  - QA: `Tool`: Bash | `Steps`: Apply bootstrap verify, then query `system.tables` for the expected bronze tables and `silver.mv_source_%` views. | `Expected`: Every approved source has one bronze table and one MV targeting the correct shared silver table without migration checksum drift.

- [x] 5. Land OpenSky as the primary broad-coverage aircraft telemetry source
  - What to do: Activate OpenSky state-vector ingest and land decoded state rows through the OpenSky-specific MV into `silver.fact_track_point`.
  - Start here: `internal/packs/aviation/opensky.go:17`; `internal/packs/aviation/opensky.go:39`; `internal/packs/aviation/opensky.go:91`
  - Verify: `go test ./internal/packs/aviation/... ./cmd/worker-fetch/...`
  - QA: `Tool`: Bash | `Steps`: Run OpenSky decode/request tests and verify deterministic fixture ingestion lands rows for the OpenSky source ID. | `Expected`: OpenSky compiles to `/api/states/all?extended=1`, uses client-credentials auth, and lands track points with preserved `source_id` lineage.

- [x] 6. Land Airplanes.live and ADSB.lol as overlapping aircraft supplement feeds
  - What to do: Activate Airplanes.live and ADSB.lol as overlapping ADS-B supplement feeds with separate source IDs and fixed endpoint sets.
  - Start here: `seed/source_catalog.json:2824`; `seed/source_catalog.json:7574`; `cmd/control-plane/jobs_http_sources.go:267`
  - Verify: `go test ./cmd/worker-fetch/... ./internal/parser/...`
  - QA: `Tool`: Bash | `Steps`: Run source-specific tests and inspect registry entrypoints for the approved Airplanes.live and ADSB.lol endpoint inventory. | `Expected`: Both providers land rows independently into `silver.fact_track_point` and preserve overlap instead of deduplicating across providers.

- [x] 7. Land AISHub as the primary HTTP-poll maritime telemetry source
  - What to do: Activate AISHub using query-param auth and once-per-minute pacing, landing maritime telemetry into `silver.fact_track_point`.
  - Start here: `seed/source_catalog.json:2446`; `internal/packs/maritime/adapters.go:56`; `internal/packs/maritime/adapters.go:66`
  - Verify: `go test ./internal/packs/maritime/... ./cmd/worker-fetch/...`
  - QA: `Tool`: Bash | `Steps`: Run AISHub fixture/request tests and inspect registry throttling values after bootstrap verify. | `Expected`: AISHub uses query-param auth, `1 RPM / 1 burst`, and lands telemetry rows with correct source lineage.

- [x] 8. Land OpenAIP Core as the first structured aviation enrichment source
  - What to do: Activate OpenAIP Core list endpoints and land structured aviation reference records into `silver.dim_entity` with source-aware lineage.
  - Start here: `seed/source_catalog.json:3117`; `migrations/clickhouse/0005_baseline_tables.sql:230`
  - Verify: `go test ./cmd/worker-fetch/... ./internal/parser/...`
  - QA: `Tool`: Bash | `Steps`: Run OpenAIP fixture tests and inspect runtime metadata plus `silver.v_entity_source_lineage` for the OpenAIP source ID. | `Expected`: Only the four approved list endpoints are active and OpenAIP entities are visible in `silver.dim_entity` with source lineage.

- [x] 9. Register secondary sources as explicit deferred contracts instead of implicit doc crawls
  - What to do: Convert secondary sources from implicit doc-page crawls into explicit deferred/runtime metadata with disabled or review-gated status.
  - Start here: `cmd/bootstrap/source_catalog.go:624`; `seed/source_catalog.json:3024`; `seed/source_catalog.json:3087`
  - Verify: `go test ./cmd/bootstrap/...`
  - QA: `Tool`: Bash | `Steps`: Inspect runtime metadata for all listed secondary sources and query for docs-page entrypoints. | `Expected`: Every secondary source is either explicitly deferred or explicitly modeled with machine endpoints, and none remain runnable on docs URLs.

- [x] 10. Update orchestration for source-specific polling, backfill separation, and MV-safe freshness flow
  - What to do: Tune control-plane orchestration for source-safe polling, source-specific frontier rows, and freshness-only initial rollout pacing.
  - Start here: `cmd/control-plane/jobs_http_sources.go:267`; `internal/packs/maritime/adapters.go:66`
  - Verify: `docker compose run --rm bootstrap verify`
  - QA: `Tool`: Bash | `Steps`: After bootstrap/runtime sync, inspect `meta.source_registry` and `ops.crawl_frontier` for planned RPM/burst settings and one frontier row per endpoint. | `Expected`: Runtime pacing matches the rollout contract exactly and no duplicate doc-page frontier rows are seeded.

- [x] 11. Update silver coverage, lineage, and API reporting for MV-backed source landings
  - What to do: Override coverage and lineage views so MV-backed source-specific landings are counted as terminal silver success instead of doc-crawl or frontier activity.
  - Start here: `cmd/bootstrap/main.go:1146`; `migrations/clickhouse/0020_source_silver_coverage.sql`; `migrations/clickhouse/gold_api_views.sql:1`
  - Verify: `docker compose run --rm bootstrap verify`
  - QA: `Tool`: Bash | `Steps`: Apply the override file and query `meta.source_silver_coverage` plus `gold.api_v1_source_coverage` after deterministic fixture ingestion. | `Expected`: Coverage surfaces report landed rows and correct terminal destinations without editing checksum-locked numeric migrations.

- [x] 12. Add deterministic fixtures, contract tests, E2E coverage, and optional live smoke validation
  - What to do: Add deterministic fixtures, contract tests, compose-backed E2E coverage, and env-gated live smoke validation for the phase-1 sources.
  - Start here: `internal/packs/aviation/aviation_test.go`; `cmd/bootstrap/source_catalog_test.go:185`; `test/e2e/pipeline_test.go`
  - Verify: `go test ./cmd/bootstrap/... ./cmd/control-plane/... ./cmd/worker-fetch/... ./internal/parser/... ./internal/promote/... ./internal/packs/...`
  - QA: `Tool`: Bash | `Steps`: Run the full targeted Go suite, bootstrap verify, and compose-backed E2E pipeline in the local stack. | `Expected`: Deterministic tests prove bronze write, MV landing, silver rows, and coverage reporting, while live smoke stays skipped unless credentials are set.

### Legacy Final Verification
- [x] F1. Plan Compliance Audit — oracle
- [x] F2. Code Quality Review — unspecified-high
- [x] F3. Real Manual QA — unspecified-high (+ playwright if UI)
- [x] F4. Scope Fidelity Check — deep

## Source Plan: REST API Frontend Contract Hardening (`rest-api-frontend-contract-hardening.md`)
- Original scope summary: Harden the read-only REST API so every frontend-consumed route has explicit machine-readable contract metadata, detailed human-readable documentation, and API-key protection on data routes while keeping operational probes public.
- Carried numbered tasks: `6`
- Carried legacy final-verification tasks: `4`

### Numbered Tasks
- [x] 1. Create the route contract source of truth
  - What to do: Create one route metadata source of truth covering all 34 routes, their auth requirements, params, fields, and handler kinds.
  - Start here: `cmd/api/main.go:15`; `cmd/api/main.go:39`; `cmd/api/handlers.go:36`
  - Verify: `go test ./cmd/api/...`
  - QA: `Tool`: Bash | `Steps`: Run API tests, then query `/v1/schema` and verify the endpoint count plus metadata for a bespoke route like `/v1/search`. | `Expected`: All 34 routes are described from one model and the public/protected matrix is explicit in metadata.

- [x] 2. Add shared-key auth middleware for protected data routes
  - What to do: Add inbound `X-API-Key` middleware for protected data routes while keeping probes/schema public and OPTIONS preflight unauthenticated.
  - Start here: `cmd/api/main.go:45`; `cmd/api/main.go:160`; `cmd/api/handlers.go:698`
  - Verify: `go test ./cmd/api/...`
  - QA: `Tool`: Bash | `Steps`: Call `/v1/metrics` with and without `X-API-Key`, then run an OPTIONS preflight requesting `X-API-Key`. | `Expected`: Protected routes return `401` without a key, succeed with the shared key, and preflight stays unauthenticated with the right allow headers.

- [x] 3. Enrich `/v1/schema` into a frontend-usable contract endpoint
  - What to do: Replace the path-only `/v1/schema` payload with a frontend-usable endpoint contract derived from the centralized route metadata.
  - Start here: `cmd/api/main.go:88`; `cmd/api/handlers.go:49`; `cmd/api/contract_test.go:20`
  - Verify: `go test ./cmd/api/... -run 'TestSchemaContract|TestAPIExpandedContracts'`
  - QA: `Tool`: Bash | `Steps`: Query `/v1/schema` with the shared key and inspect route metadata for `/v1/metrics` and `/v1/search`. | `Expected`: Schema exposes auth, params, selectable fields, pagination defaults, and bespoke combined-search behavior without inventing unsupported fields.

- [x] 4. Publish detailed markdown API reference from the same route metadata
  - What to do: Publish a detailed `docs/api-reference.md` from the same route metadata used for auth and schema generation.
  - Start here: `docs/api-reference.md:1`; `cmd/api/main.go:49`; `cmd/api/handlers_expanded.go:109`
  - Verify: `grep -q '## GET /v1/metrics' docs/api-reference.md`
  - QA: `Tool`: Bash | `Steps`: Grep the API reference for metrics, auth guidance, selectable fields, and the server-side BFF note. | `Expected`: The markdown reference is detailed, in route order, and explicitly documents the BFF + `X-API-Key` model.

- [x] 5. Upgrade package and contract tests for auth, schema, and docs parity
  - What to do: Expand package and contract tests so auth behavior, preflight, HEAD parity, schema richness, and docs parity cannot drift silently.
  - Start here: `cmd/api/main_test.go:35`; `cmd/api/handlers_test.go:25`; `cmd/api/contract_test.go:20`
  - Verify: `go test ./cmd/api/...`
  - QA: `Tool`: Bash | `Steps`: Run the full API test suite and the focused schema/preflight contract subset. | `Expected`: Unit and contract coverage enforces protected/public route behavior, detailed schema fixtures, and docs parity assertions.

- [x] 6. Align runtime wiring, E2E verification, and entry-point docs
  - What to do: Align compose defaults, E2E requests, and README guidance with the shared-key API contract and BFF integration model.
  - Start here: `docker-compose.yml`; `test/e2e/pipeline_test.go:20`; `README.md`
  - Verify: `go test ./test/e2e/... -tags=e2e`
  - QA: `Tool`: Bash | `Steps`: Run the E2E suite against the compose stack and grep runtime docs for `API_SHARED_KEY` and `X-API-Key` guidance. | `Expected`: Protected routes are exercised with the shared key, public probes stay open, and docs match compose runtime behavior.

### Legacy Final Verification
- [x] F1. Plan Compliance Audit — oracle
- [x] F2. Code Quality Review — unspecified-high
- [x] F3. Real Manual QA — unspecified-high
- [x] F4. Scope Fidelity Check — deep

## Source Plan: Frontend REST Readiness Remediation (`frontend-rest-readiness-remediation.md`)
- Original scope summary: Make the existing REST API truthful, browser-consumable, and data-complete for frontend use by first repairing live route/view failures and contract drift, then fixing synthetic or thin analytics payloads, and finally reconciling the source/bronze substrate inconsistencies that currently undermine data availability.
- Carried numbered tasks: `9`
- Carried legacy final-verification tasks: `4`

### Numbered Tasks
- [x] 1. Freeze frontend route inventory, schema discoverability, and API reference
  - What to do: Freeze one authoritative route inventory shared by router registration, `/v1/schema`, README, docs, and fixtures.
  - Start here: `cmd/api/main.go:30`; `cmd/api/main.go:76`; `testdata/fixtures/contracts/api_v1_schema.json:30`
  - Verify: `go test ./cmd/api/...`
  - QA: `Tool`: Bash | `Steps`: Query `/v1/schema`, confirm `/v1/internal/stats` is present, and verify `docs/api-reference.md` exists. | `Expected`: Schema/docs fixtures align on the same 34-route inventory and speculative routes remain out of scope.

- [x] 2. Repair nested entity relation views and live route correctness
  - What to do: Append a migration that repairs nested entity relation views so live ClickHouse columns match API handler expectations.
  - Start here: `migrations/clickhouse/0007_api_expansion_views.sql:214`; `migrations/clickhouse/0007_api_expansion_views.sql:230`; `cmd/api/handlers_expanded.go:52`
  - Verify: `entity=$(curl -fsS --data-binary "SELECT entity_id FROM gold.api_v1_entity_events LIMIT 1 FORMAT TabSeparated" "$CH_URL") && curl -s "http://localhost:8080/v1/entities/$entity/events"`
  - QA: `Tool`: Bash | `Steps`: DESCRIBE the repaired ClickHouse views, fetch sample entity IDs, and hit both nested entity routes. | `Expected`: View columns use plain aliases like `event_id` and `place_id`, and both nested routes return `200` with array payloads instead of query failures.

- [x] 3. Add browser preflight/CORS support for frontend REST access
  - What to do: Add allowlisted browser preflight/CORS support for frontend GET/HEAD/OPTIONS access from configured dev origins.
  - Start here: `cmd/api/main.go:41`; `README.md:86`; `test/e2e/pipeline_test.go:150`
  - Verify: `curl -si -X OPTIONS http://localhost:8080/v1/metrics -H 'Origin: http://localhost:3000' -H 'Access-Control-Request-Method: GET'`
  - QA: `Tool`: Bash | `Steps`: Run allowed and disallowed preflight requests, then issue a normal GET with an allowed origin header. | `Expected`: Allowed origins receive explicit CORS headers for GET/HEAD/OPTIONS, while disallowed origins are rejected with an explicit denial.

- [x] 4. Normalize frontend-facing scalar typing
  - What to do: Normalize frontend-facing boolean flags at the API boundary without coercing numeric analytics fields or timestamps.
  - Start here: `cmd/api/handlers.go:80`; `cmd/api/handlers_expanded.go:92`; `cmd/api/handlers.go:565`
  - Verify: `curl -s 'http://localhost:8080/v1/sources?limit=1' | jq -r '.data.items[0].enabled | type'`
  - QA: `Tool`: Bash | `Steps`: Type-check `enabled` and related flags on sources/metrics, then type-check `rank` and `metric_value` on analytics rollups. | `Expected`: Boolean fields serialize as JSON booleans and analytics numbers remain numeric.

- [x] 5. Replace synthetic source coverage with observed coverage state
  - What to do: Rebuild source coverage around observed runtime/promoted data and explicit coverage state instead of synthetic placeholder counts.
  - Start here: `migrations/clickhouse/0007_api_expansion_views.sql:125`; `cmd/api/handlers.go:102`; `migrations/clickhouse/0021_source_silver_coverage_lineage.sql:26`
  - Verify: `go test ./cmd/api/...`
  - QA: `Tool`: Bash | `Steps`: Compare promoted event counts from ClickHouse with `/v1/sources/{sourceId}/coverage` and inspect a blocked source payload. | `Expected`: Coverage endpoints report real counts plus `coverage_state` and `reason` instead of synthetic zeros or healthy-looking placeholders.

- [x] 6. Restore explainability and evidence on REST analytics surfaces
  - What to do: Restore explainability and evidence on existing analytics surfaces so the frontend can understand why metrics exist without a new explain endpoint.
  - Start here: `internal/metrics/registry.go:287`; `internal/metrics/materialization_sql.go:182`; `cmd/api/handlers.go:239`
  - Verify: `curl -s 'http://localhost:8080/v1/places/plc:fr-idf-paris/metrics?limit=1' | jq -e '.data.items[0].attrs.explainability and (.data.items[0].evidence | length > 0)'`
  - QA: `Tool`: Bash | `Steps`: Query `meta.metric_registry` and `gold.metric_snapshot` for explainability/evidence presence, then hit the metric detail and place-metrics endpoints. | `Expected`: Snapshot-backed analytics payloads expose explainability metadata and non-empty evidence references instead of thin placeholder attrs.

- [x] 7. Rebuild authoritative source catalog artifacts and collision-safe bronze naming
  - What to do: Make the compiled catalog authoritative again with collision-safe bronze table naming and manifest parity tests.
  - Start here: `cmd/bootstrap/source_catalog.go:106`; `cmd/bootstrap/source_catalog.go:652`; `seed/source_catalog_compiled.json`
  - Verify: `go test ./cmd/bootstrap/... ./internal/migrate/...`
  - QA: `Tool`: Bash | `Steps`: Run bootstrap/migration tests and a python check over `seed/source_catalog_compiled.json` for one-to-one manifest/seed/table uniqueness. | `Expected`: Compiled catalog rows, runnable seeds, and bronze DDL manifest entries stay consistent with zero duplicate bronze table names.

- [x] 8. Materialize one bronze table per source and remove hardcoded 7-table assumptions
  - What to do: Append a generated migration that materializes every missing bronze table and remove hardcoded seven-table assumptions from dashboard/bootstrap verification.
  - Start here: `migrations/clickhouse/0015_source_bronze_tables.sql:1`; `internal/dashboardstats/source_bronze_tables.go:3`; `internal/dashboardstats/service.go:221`
  - Verify: `docker compose run --rm bootstrap verify`
  - QA: `Tool`: Bash | `Steps`: Compare manifest, registry, and live `bronze.src_*` table counts, then query for registry bronze references missing from `system.tables`. | `Expected`: Manifest, registry, and ClickHouse all agree on one bronze table per concrete source and no stale seven-table assumptions remain.

- [x] 9. Close runtime blind spots in e2e/contract coverage and finalize frontend-facing docs
  - What to do: Close live frontend blind spots by extending E2E and contract coverage for nested entity routes, readiness semantics, CORS, analytics payloads, and docs existence.
  - Start here: `test/e2e/pipeline_test.go:180`; `test/e2e/pipeline_test.go:400`; `cmd/api/handlers_test.go:210`
  - Verify: `go test ./test/e2e/... -tags=e2e`
  - QA: `Tool`: Bash | `Steps`: Run the API package suite and E2E suite, then verify `/v1/ready` asserts `.data.ready == true` only when the stack is actually ready. | `Expected`: Live regressions around readiness, nested routes, rollup payload shape, and preflight behavior are caught automatically.

### Legacy Final Verification
- [x] F1. Plan Compliance Audit — oracle
- [x] F2. Code Quality Review — unspecified-high
- [x] F3. Real Manual QA — unspecified-high (+ playwright if UI)
- [x] F4. Scope Fidelity Check — deep

## Source Plan: Silver Coverage Rollout (`silver-coverage-rollout.md`)
- Original scope summary: Make runtime orchestration registry-driven end to end, then guarantee every in-scope HTTP concrete source lands in a real `silver.*` destination and is provable through a per-source coverage contract.
- Carried numbered tasks: `8`
- Carried legacy final-verification tasks: `4`

### Numbered Tasks
- [x] 1. Freeze the denominator and silver coverage contract
  - What to do: Add one authoritative in-scope source definition based on `meta.source_registry`, then introduce a coverage artifact contract named `meta.source_silver_coverage` keyed by `source_id` with required fields: `coverage_state`, `routing_mode`, `promote_profile`, `terminal_kind`, `terminal_destination`, `last_bronze_at`, `last_parse_at`, `last_promote_at`, `last_silver_at`, `reason`, `attrs`, and `updated_at`. States must include exactly `silver_landed`, `silver_view_only`, `blocked_missing_credential`, `parsed_no_promotable_rows`, `unresolved_only`, and `unsupported_profile`.
  - Start here: `cmd/bootstrap/source_registry.go:413`; `internal/dashboardstats/service.go:136`; `migrations/clickhouse/0005_baseline_tables.sql:165`
  - Verify: `go test ./cmd/bootstrap/... ./internal/dashboardstats/...` passes with the new coverage artifact contract
  - QA: `Tool`: Bash | `Steps`: Run `curl -fsS "http://localhost:8123/?query=SELECT%20count()%20FROM%20meta.source_registry%20FINAL%20WHERE%20catalog_kind%3D'concrete'%20AND%20transport_type%3D'http'%20AND%20bronze_table%20IS%20NOT%20NULL%20FORMAT%20TabSeparated"`; then run `curl -fsS "http://localhost:8123/?query=SELECT%20count()%20FROM%20meta.source_silver_coverage%20FORMAT%20TabSeparated"`. | `Expected`: The two counts match exactly for the in-scope source set.

- [x] 2. Replace hard-coded auto-sync source selection with registry-driven enumeration
  - What to do: Remove the automatic reliance on the fixed `geopoliticalConcreteSources` and `safetyConcreteSources` lists for long-running sync. Build one registry-driven source enumerator that selects every in-scope HTTP concrete source, preserves bundle-alias handling only when explicitly requested, respects due-time logic, and seeds frontier for every eligible source.
  - Start here: `cmd/control-plane/jobs_http_sources.go:18`; `cmd/control-plane/main.go:183`; `cmd/control-plane/jobs_http_sources.go:205`
  - Verify: `go test ./cmd/control-plane/...` passes with automatic sync tests updated to the registry-driven behavior
  - QA: `Tool`: Bash | `Steps`: Start the stack, wait one control-plane tick, then query `ops.crawl_frontier` distinct `source_id` count. | `Expected`: Frontier coverage matches the denominator for eligible HTTP concrete sources in the test dataset.

- [x] 3. Execute real automatic promote runs after bronze parsing
  - What to do: Extend long-running orchestration so automatic sync executes the real promote job or shared promote function after due fetch/parse windows, rather than only recording a `promote` stage marker. Preserve idempotent bronze checkpoint semantics and keep the promote path registry-driven.
  - Start here: `cmd/control-plane/jobs_http_sources.go:93`; `cmd/control-plane/jobs_promote.go:35`; `internal/promote/pipeline.go:186`
  - Verify: `go test ./cmd/control-plane/... ./internal/promote/...` passes
  - QA: `Tool`: Bash | `Steps`: Wait for a control-plane tick, then query `ops.job_run` for recent successful `promote` runs. | `Expected`: At least one real promote job run exists; stage markers are no longer the only promote evidence.

- [x] 4. Define profile routing so every source has one terminal silver strategy
  - What to do: Build an explicit routing matrix keyed by `promote_profile` and source shape. For `promote:geopolitical`, `promote:safety_security`, and `promote:catalog`, declare whether the terminal route is shared canonical silver tables, pack-specific silver writers, or a source-specific silver destination. For every in-scope source, persist exactly one routing mode into the coverage artifact.
  - Start here: `cmd/bootstrap/source_catalog.go:589`; `cmd/bootstrap/source_registry.go:417`; `cmd/control-plane/jobs_promote.go:138`
  - Verify: all in-scope sources in `meta.source_silver_coverage` have non-null `routing_mode`, `promote_profile`, and `terminal_destination`
  - QA: `Tool`: Bash | `Steps`: Query `meta.source_silver_coverage` for null/empty routing fields. | `Expected`: Zero rows with missing routing metadata.

- [x] 5. Complete the shared canonical silver route for promotable generic sources
  - What to do: Ensure the generic promote pipeline can emit terminal proof for every source that can be represented as canonical entities, observations, events, or tracks. Add any missing source-preserving lineage needed so canonical silver outputs can be tied back to `source_id`, especially for entity-heavy flows where `silver.dim_entity` alone is insufficient for source-scoped coverage.
  - Start here: `internal/promote/pipeline.go:186`; `internal/promote/pipeline.go:824`; `migrations/clickhouse/0005_baseline_tables.sql:165`
  - Verify: `go test ./internal/promote/... ./cmd/control-plane/...` passes with source-preserving lineage assertions
  - QA: `Tool`: Bash | `Steps`: Run promote, then query the chosen silver proof surface for a sample canonical-routed source. | `Expected`: Rows are returned with the correct `source_id` and terminal silver destination.

- [x] 6. Add non-canonical source-specific silver destinations only where canonical mapping is lossy
  - What to do: For sources or profiles that cannot be represented safely in the shared canonical silver model, add a source-specific or profile-specific `silver` table/view and register that destination in the routing matrix and coverage artifact. Prefer one profile-level destination over many per-source variants when the shape is shared.
  - Start here: `cmd/control-plane/jobs_aviation.go:233`; `cmd/control-plane/jobs_maritime.go:371`; `internal/packs/safety/safety.go:902`
  - Verify: every source marked `routing_mode='source_specific'` or `routing_mode='profile_specific'` has a non-empty `terminal_destination` in `silver.*`
  - QA: `Tool`: Bash | `Steps`: Promote a source routed to a source/profile-specific silver destination and query that destination by `source_id`. | `Expected`: At least one landed row exists in `silver.*`, and coverage records the same destination.

- [x] 7. Materialize and maintain per-source silver coverage state during runtime
  - What to do: Update runtime jobs so coverage state is refreshed from real pipeline outcomes: frontier, fetch, parse, promote, and terminal silver landing. Populate blocked/partial states for credentialed, empty, unresolved-only, and unsupported cases. Make the coverage artifact queryable from tests and operator tooling.
  - Start here: `cmd/control-plane/jobs_http_sources.go:85`; `cmd/control-plane/jobs_promote.go:35`; `internal/dashboardstats/service.go:136`
  - Verify: `curl -fsS "http://localhost:8123/?query=SELECT%20count()%20FROM%20meta.source_silver_coverage%20WHERE%20last_promote_at%20IS%20NULL%20AND%20coverage_state%20IN%20('silver_landed'%2C'silver_view_only')%20FORMAT%20TabSeparated"` returns `0`
  - QA: `Tool`: Bash | `Steps`: Run the pipeline for a known good source, then query `meta.source_silver_coverage` for timestamps and state. | `Expected`: `coverage_state` is `silver_landed` or `silver_view_only`, with non-null bronze/parse/promote/silver timestamps.

- [x] 8. Harden verification, repeat-run parity, and source-to-silver audits
  - What to do: Add tests and runtime audits that prove end-to-end silver coverage, including denominator freeze, per-state counts, idempotent reruns, and direct silver row presence by `source_id`. Update stale tests and plan/docs that still assume the old 7-source runtime subset.
  - Start here: `internal/dashboardstats/service_test.go:21`; `test/e2e/pipeline_test.go:264`; `cmd/control-plane/jobs_promote.go:35`
  - Verify: `go test ./cmd/control-plane/... ./internal/promote/... ./internal/dashboardstats/... ./test/e2e/... -tags=e2e` passes in the intended environment
  - QA: `Tool`: Bash | `Steps`: Run `curl -fsS "http://localhost:8123/?query=SELECT%20source_id%2Crouting_mode%2Ccoverage_state%2Cterminal_destination%20FROM%20meta.source_silver_coverage%20ORDER%20BY%20source_id%20FORMAT%20TSV"`; then rerun the stack-level test suite without injecting new bronze rows. | `Expected`: Every in-scope source appears exactly once with a terminal destination or blocked state, and reruns keep per-source silver counts stable.

### Legacy Final Verification
- [x] F1. Plan Compliance Audit - oracle
- [x] F2. Code Quality Review - unspecified-high
- [x] F3. Real Manual QA - unspecified-high (+ playwright if UI)
- [x] F4. Scope Fidelity Check - deep

## Source Plan: Full Source Catalog Rollout and Incremental Runtime Sync (`sources-rollout.md`)
- Original scope summary: The repo now fully represents and governs the full `sources.md` universe in machine-readable form, but only a small approved concrete subset is runnable end to end today. Concrete sources are either runtime-linked with bronze/sync coverage or explicitly deferred with reasons; fingerprints and families are review-gated generators, not direct runnable sources.
- Carried numbered tasks: `13`
- Carried legacy final-verification tasks: `4`

### Numbered Tasks
- [x] 1. Freeze the source taxonomy, lifecycle states, and env-var auth contract
  - What to do: Extend the source-governance model so every entry from `sources.md` is classified as exactly one of `catalog_kind in ('concrete','fingerprint','family')`. Freeze the lifecycle states to `draft`, `review_required`, `approved_disabled`, `approved_enabled`, and `blocked_missing_credential`. Freeze credential metadata to the existing `auth_config_json` env-ref shape: `{"env_var":"...","placement":"header|query|cookie","name":"...","prefix":"..."}`. Decision: concrete sources are the only entries allowed in runnable `meta.source_registry`; fingerprints and families live in catalog/template tables and only emit reviewed child sources.
  - Start here: `seed/source_registry.json:1`; `cmd/bootstrap/source_registry.go:388`; `migrations/clickhouse/0012_source_registry_http_contract.sql:1`
  - Verify: `go test ./cmd/bootstrap/... -run 'TestSourceCatalogKinds|TestAuthConfigEnvContract' -count=1` exits `0`.
  - QA: `Tool`: Bash | `Steps`: go test ./cmd/bootstrap/... -run TestSourceCatalogKinds -count=1 | `Expected`: exit 0 and tests prove concrete/fingerprint/family separation plus valid lifecycle states

- [x] 2. Create a machine-readable source catalog compiler from `sources.md`
  - What to do: Introduce a canonical machine-readable catalog file set under `seed/` that represents every row in `sources.md`, including metadata for category, tags, archetype, credential requirements, and generator relationships. Build a compiler that transforms that catalog into:
  - Start here: `cmd/bootstrap/source_registry.go:293`; `seed/source_registry.json:1`; `sources.md:52`
  - Verify: `go test ./cmd/bootstrap/... -run 'TestCompileSourceCatalog|TestCompiledSourceCounts' -count=1` exits `0`.
  - QA: `Tool`: Bash | `Steps`: go test ./cmd/bootstrap/... -run TestCompiledSourceCounts -count=1 | `Expected`: exit 0 and test asserts 222 concrete + 18 family + full fingerprint/standard coverage

- [x] 3. Extend governance tables for fingerprints, families, and reviewed child sources
  - What to do: Add catalog/template tables under `meta` for fingerprints/families and child-source generation results. Recommended tables:
  - Start here: `migrations/clickhouse/0006_source_governance.sql:1`; `docs/runbooks/kill-switch.md:5`; `cmd/bootstrap/source_registry.go:672`
  - Verify: `go test ./cmd/bootstrap/... ./internal/migrate/... -run 'TestSourceGenerationGovernance|TestSchemaStandards' -count=1` exits `0`.
  - QA: `Tool`: Bash | `Steps`: go test ./cmd/bootstrap/... -run TestSourceGenerationGovernance -count=1 | `Expected`: exit 0 and tests prove review_required child sources are not inserted into runnable registry until approved

- [x] 4. Generate static bronze DDL for every runnable concrete source
  - What to do: Keep the “one bronze table per concrete source” rule, but stop hand-maintaining it. Generate append-only static DDL from the source catalog into new migration files, following the existing bronze contract in `0015_source_bronze_tables.sql`. Fingerprints/families do not get bronze tables; their approved emitted child sources do. The generator must also update any curated observability enumerations that depend on bronze table names.
  - Start here: `migrations/clickhouse/0015_source_bronze_tables.sql:1`; `internal/migrate/schema_standards_test.go:164`; `internal/dashboardstats/service.go:161`
  - Verify: `go test ./internal/migrate/... -run 'TestSourceBronzeTablesMigrationDefinesAllStaticTables|TestSourceBronzeTables' -count=1` exits `0`.
  - QA: `Tool`: Bash | `Steps`: go test ./internal/migrate/... -run TestSourceBronzeTables -count=1 | `Expected`: exit 0 and tests verify full concrete-source bronze coverage from the compiled catalog

- [x] 5. Introduce adapter archetypes and parser compatibility matrix
  - What to do: Define a small fixed adapter matrix for the source universe and map every concrete source to exactly one first-wave archetype: `http_json`, `http_csv`, `http_xml`, `rss_atom`, `html_profile`, `bulk_file`, `stac_api`, `catalog_ckan`, `catalog_socrata`, `catalog_opendatasoft`, `arcgis_rest`, `ogc_features`, `ogc_records`, `discovery_web`, or `deferred_transport`. Freeze parser compatibility against the existing registry (`json/csv/xml/rss/html-profile`) and require catalog rows to declare a compatible parser or deferred status.
  - Start here: `internal/parser/registry.go:119`; `internal/parser/json.go:13`; `sources.md:33`
  - Verify: `go test ./cmd/bootstrap/... ./internal/parser/... -run 'TestCatalogArchetypeCoverage|TestArchetypeParserCompatibility' -count=1` exits `0`.
  - QA: `Tool`: Bash | `Steps`: go test ./cmd/bootstrap/... -run TestCatalogArchetypeCoverage -count=1 | `Expected`: exit 0 and test proves 100% archetype assignment for concrete rows

- [x] 6. Make frontier seeding idempotent and automatic
  - What to do: Replace blind frontier inserts with canonical-url upsert/dedupe semantics and add an automated sync planner that periodically seeds eligible sources based on `refresh_strategy`, `crawl_strategy`, and source state. Keep using `ops.crawl_frontier` as the single queue. The automated loop must run inside existing service boundaries (control-plane or a new control-plane mode), not a parallel unmanaged scheduler.
  - Start here: `cmd/control-plane/jobs_http_sources.go:180`; `internal/discovery/frontier.go:233`; `migrations/clickhouse/0013_crawl_frontier_leases.sql:1`
  - Verify: `go test ./internal/discovery/... ./cmd/control-plane/... -run 'TestFrontierDedupe|TestAutomaticSyncPlanner' -count=1` exits `0`.
  - QA: `Tool`: Bash | `Steps`: go test ./cmd/control-plane/... -run TestAutomaticSyncPlanner -count=1 | `Expected`: exit 0 and test proves repeat planner runs do not duplicate `(source_id, canonical_url)` frontier entries

- [x] 7. Add a processed-document ledger for incremental parse
  - What to do: Introduce a parse checkpoint/ledger so `worker-parse parse-source` only emits new bronze rows for raw documents that have not yet been successfully parsed for the relevant parser/version/source-record schema. The second parse run over the same raw inputs must emit zero new rows unless content or parser version changed.
  - Start here: `cmd/worker-parse/main.go:425`; `migrations/clickhouse/0005_baseline_tables.sql:1`; `migrations/clickhouse/0015_source_bronze_tables.sql:1`
  - Verify: `go test ./cmd/worker-parse/... -run 'TestParseCheckpointPreventsDuplicateBronzeWrites|TestParserVersionBumpReprocessesRawDocs' -count=1` exits `0`.
  - QA: `Tool`: Bash | `Steps`: go test ./cmd/worker-parse/... -run TestParseCheckpointPreventsDuplicateBronzeWrites -count=1 | `Expected`: exit 0 and second parse run inserts zero new bronze rows

- [x] 8. Scope promote incrementally by source and change window
  - What to do: Stop scanning every bronze table on each promote run. Add source/time-window scoped promote input selection driven by the parse/fetch ledgers so only changed sources/slices are promoted. Keep bronze-driven promote semantics and canonical IDs stable.
  - Start here: `cmd/control-plane/jobs_promote.go:131`; `internal/promote/pipeline.go:475`; `internal/promote/pipeline_test.go:1`
  - Verify: `go test ./cmd/control-plane/... ./internal/promote/... -run 'TestIncrementalPromoteSelection|TestReplayDoesNotDuplicateCanonicalRows' -count=1` exits `0`.
  - QA: `Tool`: Bash | `Steps`: go test ./cmd/control-plane/... -run TestIncrementalPromoteSelection -count=1 | `Expected`: exit 0 and test proves unchanged sources are skipped by promote selection

- [x] 9. Implement platform fingerprints as discovery generators
  - What to do: Add discovery probes for the fingerprint stack in `sources.md`: CKAN, Socrata, ArcGIS Hub/REST, Opendatasoft, GeoNetwork, GeoNode, OGC APIs, STAC, sitemap/robots, RSS/Atom, and Wayback/CDX. These probes discover candidate child sources and write them into `meta.discovery_candidate` with classifier metadata, not directly into enabled runnable registry state.
  - Start here: `sources.md:33`; `internal/discovery/frontier.go:51`; `cmd/worker-fetch/main.go:278`
  - Verify: `go test ./internal/discovery/... ./cmd/control-plane/... -run 'TestFingerprintProbeGeneration|TestDiscoveryCandidatesStayReviewRequired' -count=1` exits `0`.
  - QA: `Tool`: Bash | `Steps`: go test ./internal/discovery/... -run TestFingerprintProbeGeneration -count=1 | `Expected`: exit 0 and tests prove CKAN/Socrata/ArcGIS/STAC/etc. candidates are emitted with deterministic metadata

- [x] 10. Implement recurring family templates as child-source generators
  - What to do: Add template-driven generation for the 18 recurring source families in `sources.md`. Each family template must encode expected transport/archetype, scope level, review defaults, and emitted child-source shape. Family runs generate reviewed candidates/child sources parameterized by geography/admin level, then feed the same approval pipeline as fingerprint discovery.
  - Start here: `sources.md:367`; `sources.md:392`; `seed/source_registry.json:156`
  - Verify: `go test ./cmd/bootstrap/... ./cmd/control-plane/... -run 'TestFamilyTemplateGeneration|TestGeneratedChildSourcesRequireApproval' -count=1` exits `0`.
  - QA: `Tool`: Bash | `Steps`: go test ./cmd/bootstrap/... -run TestFamilyTemplateGeneration -count=1 | `Expected`: exit 0 and tests prove family templates emit stable child-source definitions by geography/admin scope

- [x] 11. Keep public pull-based concrete source coverage explicit by archetype wave status
  - What to do: Keep every public pull-based concrete source in `sources.md` explicitly classified as either currently runtime-linked or explicitly deferred with `deferred_reason`. Preserve the current approved runtime-linked subset (`7` sources) as structurally complete, and treat remaining public concrete onboarding as future runtime expansion rather than silently implying it is already complete. Group future expansion by archetype, not by category.
  - Start here: `sources.md:70`; `cmd/control-plane/jobs_http_sources.go:15`; `seed/source_registry.json:1`
  - Verify: `go test ./cmd/bootstrap/... -run 'TestConcreteSourceCoverage|TestApprovedRunnableSourceCoverage' -count=1` exits `0`.
  - QA: `Tool`: Bash | `Steps`: go test ./cmd/bootstrap/... -run TestConcreteSourceCoverage -count=1 | `Expected`: exit 0 and tests prove no concrete public pull-based source is unclassified, even when deferred

- [x] 12. Keep credential-gated concrete sources policy-complete and disabled by default
  - What to do: For every concrete source in `sources.md` that is tagged as registration/approval/commercial/noncommercial/restricted or otherwise requires credentials, keep a deterministic env var name in `auth_config_json.env_var`. Preserve disabled/deferred-by-default behavior until env vars are present and review state is satisfied. Apply exact naming convention: `SOURCE_<UPPER_SNAKE_SLUG>_API_KEY` for key-based auth, `SOURCE_<UPPER_SNAKE_SLUG>_TOKEN` for bearer-like auth, unless the upstream API has an already-established env name in current repo conventions (for example `ACLED_API_KEY`).
  - Start here: `seed/source_registry.json:107`; `cmd/worker-fetch/main.go:939`; `cmd/control-plane/jobs_http_sources.go:171`
  - Verify: `go test ./cmd/bootstrap/... ./cmd/worker-fetch/... ./cmd/control-plane/... -run 'TestCredentialedSourcesAreDisabledByDefault|TestMissingCredentialBlocksFetch' -count=1` exits `0`.
  - QA: `Tool`: Bash | `Steps`: go test ./cmd/bootstrap/... -run TestCredentialedSourcesAreDisabledByDefault -count=1 | `Expected`: exit 0 and tests prove all credential-gated sources compile into disabled/blocking or deferred states by default

- [x] 13. Add operator-visible automation, docs, and rollout verification for the full catalog and current runtime subset
  - What to do: Update control-plane/operator flows, dashboard observability, docs, and E2E coverage so operators can verify rollout status across the full catalog and the current runtime-linked subset. Add tests such as `TestSourceCatalogRollout` and `TestAutomaticSourceSync`, extend stats/dashboard visibility for source-catalog coverage and deferred counts, and document exactly how approved/deferred/credential-gated sources behave.
  - Start here: `test/e2e/pipeline_test.go:1`; `README.md:225`; `docs/runbooks/kill-switch.md:1`
  - Verify: `go test ./test/e2e/... -tags=e2e -run 'TestSourceCatalogRollout|TestAutomaticSourceSync' -count=1` exits `0`.
  - QA: `Tool`: Bash | `Steps`: go test ./test/e2e/... -tags=e2e -run TestSourceCatalogRollout -count=1 | `Expected`: exit 0 and tests verify catalog counts, runnable/deferred/gated relationships, and operator-visible sync contracts for the current runtime-linked subset

### Legacy Final Verification
- [x] F1. Plan Compliance Audit — oracle
- [x] F2. Code Quality Review — unspecified-high
- [x] F3. Agent-Executed API/UI QA — unspecified-high (+ playwright if UI surfaces change)
- [x] F4. Scope Fidelity Check — deep

## Source Plan: Internal Stats Dashboard UI (`stats-dashboard-ui.md`)
- Original scope summary: Add a minimal internal operations dashboard served by `cmd/renderer` and styled with Tailwind CSS v4. Back it with a dedicated `GET /v1/internal/stats` contract in `cmd/api` so the UI can show real operational and dataset statistics without inventing new backend semantics.
- Carried numbered tasks: `7`
- Carried legacy final-verification tasks: `4`

### Numbered Tasks
- [x] 1. Freeze the dashboard stats contract and route ownership
  - What to do: Add a dedicated composed stats route in `cmd/api` at `GET /v1/internal/stats`, and keep the dashboard page itself in `cmd/renderer` at `/`. Freeze the JSON contract to four top-level sections: `summary`, `storage`, `quality`, and `outputs`, plus `generated_at` and `warnings`. Fix the concrete fields now: `summary` includes `sources_total`, `sources_enabled`, `sources_disabled`, `jobs_running`, `frontier_pending`, `frontier_retry`, `unresolved_open`, and `quality_open`; `storage` includes curated `table_rows` and `source_bronze_rows`; `quality` includes `freshness` and `parser_success`; `outputs` includes `metrics_total`, `latest_snapshot_at`, `hotspots_total`, and `cross_domain_total`.
  - Start here: `cmd/api/main.go:28`; `cmd/api/handlers_expanded.go:210`; `cmd/api/AGENTS.md:18`
  - Verify: `go test ./cmd/api/... -run TestInternalStatsContract -count=1` exits `0`.
  - QA: `Tool`: Bash | `Steps`: go test ./cmd/api/... -run TestInternalStatsContract -count=1 && curl -fsS http://localhost:8080/v1/internal/stats | `Expected`: test exits 0; JSON contains `summary`, `storage`, `quality`, `outputs`, `generated_at`, and `warnings`

- [x] 2. Implement the internal stats query layer over curated repo tables
  - What to do: Add reusable read-only query code under `internal/` that composes dashboard metrics from existing tables/views. Use exact counts only for bounded operational tables (`ops.crawl_frontier`, `ops.unresolved_location_queue`, `ops.quality_incident`, active `ops.job_run` slices) and use curated approximate counts from ClickHouse metadata for large tables (`bronze.raw_document`, each `bronze.src_*_v1`, `silver.fact_event`, `silver.fact_observation`, `gold.metric_snapshot`, `gold.cross_domain_snapshot`). Derive freshness from latest successful fetch per source and parser success from rolling 15-minute `ops.parse_log` windows. Return explicit warnings when a subquery fails or a section is partial.
  - Start here: `migrations/clickhouse/0003_ops_bronze.sql:1`; `migrations/clickhouse/0005_baseline_tables.sql:1`; `migrations/clickhouse/0006_source_governance.sql:36`
  - Verify: `go test ./cmd/api/... -run TestStatsAggregator -count=1` exits `0`.
  - QA: `Tool`: Bash | `Steps`: go test ./cmd/api/... -run TestStatsAggregator -count=1 && curl -fsS http://localhost:8080/v1/internal/stats | `Expected`: test exits 0; queue/job counts are exact, curated storage counts are present and labeled `approximate`

- [x] 3. Expose the stats contract through `cmd/api`
  - What to do: Add the concrete route handler for `GET /v1/internal/stats`, wire it in `newAPIMuxWithServer()`, use the shared query layer from Task 2, apply a dashboard-specific timeout, and return the standard API envelope via `respond()`/`respondError()`. Add package tests for success, unsupported query params, and upstream query failure.
  - Start here: `cmd/api/main.go:28`; `cmd/api/handlers_expanded.go:210`; `cmd/api/handlers_test.go:117`
  - Verify: `go test ./cmd/api/... -run 'TestInternalStatsContract|TestInternalStatsQueryFailure|TestInternalStatsRejectsUnsupportedParams' -count=1` exits `0`.
  - QA: `Tool`: Bash | `Steps`: go test ./cmd/api/... -run TestInternalStatsContract -count=1 && curl -fsS http://localhost:8080/v1/internal/stats | `Expected`: test exits 0; response contains `api_version`, `generated_at`, and `data`

- [x] 4. Add a minimal Tailwind CSS v4 asset pipeline for renderer-hosted UI
  - What to do: Extend `cmd/renderer` from a health stub into a tiny static-site host. Add a minimal asset pipeline for Tailwind CSS v4 using the smallest possible build path: dedicated renderer UI source files and compiled CSS committed or generated in the renderer build flow without introducing a full SPA stack. Update `build/renderer.Dockerfile` so renderer images include the compiled dashboard assets. Keep the HTML shell intentionally small and server-hosted.
  - Start here: `cmd/renderer/main.go:8`; `build/renderer.Dockerfile:1`; `docker-compose.yml:107`
  - Verify: `go test ./cmd/renderer/... -count=1` exits `0`.
  - QA: `Tool`: Bash | `Steps`: go test ./cmd/renderer/... -count=1 && docker compose build renderer && curl -fsS http://localhost:8090/ | `Expected`: tests/build exit 0; renderer root serves HTML shell instead of only a health stub

- [x] 5. Build the dashboard UI shell and curated visual panels
  - What to do: Implement a minimal, beautiful dashboard page in `cmd/renderer` that fetches `GET /v1/internal/stats` and renders five concrete sections only: hero summary cards, storage table, freshness table with threshold highlighting, parser/fetch quality cards plus a compact bar/sparkline treatment, and output readiness cards. Use restrained typography, whitespace, subtle borders, and a calm palette; prefer simple SVG/CSS bars over extra chart libraries. Ensure the layout works on mobile and desktop and gracefully handles loading, empty, partial-warning, and hard-error states.
  - Start here: `docs/dashboards/quality-dashboards.md:7`; `docs/dashboards/quality-dashboards.md:11`; `cmd/renderer/main.go:8`
  - Verify: `curl -fsS http://localhost:8090/ | grep -q 'Pipeline overview'` exits `0`.
  - QA: `Tool`: Playwright | `Steps`: Open `http://localhost:8090/`; wait for stats load; verify visible headings `Pipeline overview`, `Storage`, `Freshness`, `Parser success`, and `Outputs` | `Expected`: all five sections render with non-empty numeric/text content

- [x] 6. Add contract, package, browser, and E2E verification for the dashboard flow
  - What to do: Add deterministic tests for the stats endpoint payload, renderer page rendering, and a browser/E2E path that exercises the dashboard against the local stack. Reuse the documented quality fixtures under `testdata/fixtures/quality` for exact freshness/parser-success copy and sample states. Add a dedicated E2E test entry such as `TestStatsDashboard` that verifies API stats JSON and renderer HTML together.
  - Start here: `cmd/api/handlers_test.go:117`; `test/e2e/pipeline_test.go:156`; `docs/dashboards/quality-dashboards.md:3`
  - Verify: `go test ./cmd/api/... ./cmd/renderer/... -count=1` exits `0`.
  - QA: `Tool`: Bash | `Steps`: go test ./test/e2e/... -tags=e2e -run TestStatsDashboard -count=1 | `Expected`: exit 0 and test verifies both `http://localhost:8080/v1/internal/stats` and `http://localhost:8090/`

- [x] 7. Wire docs, compose expectations, and operator guidance for the dashboard
  - What to do: Update repo documentation so operators know where the dashboard lives, what it shows, and how to verify it. Keep documentation tight: renderer root URL, internal stats API path, which metrics are exact vs approximate, and what is intentionally excluded from MVP. Update any compose/runtime notes needed for renderer/api coordination, but do not add a new service.
  - Start here: `README.md:225`; `docs/dashboards/quality-dashboards.md:1`; `docker-compose.yml:107`
  - Verify: `grep -n "/v1/internal/stats\|8090" README.md docs/dashboards/quality-dashboards.md` returns the new operator guidance.
  - QA: `Tool`: Bash | `Steps`: grep -n "/v1/internal/stats\|8090" README.md docs/dashboards/quality-dashboards.md | `Expected`: output documents the renderer UI URL and internal stats API path

### Legacy Final Verification
- [x] F1. Plan Compliance Audit — oracle
- [x] F2. Code Quality Review — unspecified-high
- [x] F3. Real Manual QA — unspecified-high (+ playwright if UI)
- [x] F4. Scope Fidelity Check — deep

## Source Plan: HTTP Source Adapter and Source-Specific Bronze ELT (`http-source-bronze-elt.md`)
- Original scope summary: Replace fixture-backed HTTP ingestion with a production path that uses `worker-fetch` as the only generic HTTP transport, persists immutable fetch artifacts in `bronze.raw_document` + MinIO, lands parsed rows into one typed bronze table per concrete source, and promotes bronze to canonical silver with deterministic ClickHouse ELT.
- Carried numbered tasks: `14`
- Carried legacy final-verification tasks: `4`

### Numbered Tasks
- [x] 1. Freeze source registry contract for HTTP crawl, bronze routing, and source aliases
  - What to do: Extend `meta.source_registry` and bootstrap seed handling so every concrete HTTP source declares: `transport_type`, `crawl_enabled`, `allowed_hosts`, `crawl_strategy`, `crawl_config_json`, `parse_config_json`, `bronze_table`, `bronze_schema_version`, and `promote_profile`. Freeze `auth_config_json` to the exact env-ref contract `{"env_var":"...","placement":"header|query|cookie","name":"...","prefix":"..."}`. Preserve existing `source_id` values for compatibility. Set `fixture:safety` to `transport_type='bundle_alias'`, `crawl_enabled=0`, no bronze table, and require domain orchestration to fan out to `fixture:opensanctions`, `fixture:nasa-firms`, `fixture:noaa-hazards`, and `fixture:kev`. Set concrete bronze targets exactly to `bronze.src_seed_gdelt_v1`, `bronze.src_fixture_reliefweb_v1`, `bronze.src_fixture_acled_v1`, `bronze.src_fixture_opensanctions_v1`, `bronze.src_fixture_nasa_firms_v1`, `bronze.src_fixture_noaa_hazards_v1`, and `bronze.src_fixture_kev_v1`. Keep `fixture:acled` disabled by default in this plan and do not require live activation in acceptance until its official auth flow is remapped into the frozen contract.
  - Start here: `cmd/bootstrap/main.go:105`; `cmd/bootstrap/source_registry.go:324`; `seed/source_registry.json:1`
  - Verify: `go test ./cmd/bootstrap -run TestSourceRegistry -count=1` exits `0`.
  - QA: `Tool`: Bash | `Steps`: docker compose up -d --build && go test ./cmd/bootstrap -run TestSourceRegistry -count=1 && curl -fsS "http://localhost:8123/?query=SELECT%20source_id,transport_type,crawl_enabled,bronze_table%20FROM%20meta.source_registry%20ORDER%20BY%20source_id%20FORMAT%20TabSeparated" | `Expected`: test exits 0; every concrete source has the exact bronze table name above; `fixture:safety` shows `bundle_alias` and `crawl_enabled=0`

- [x] 2. Extend `ops.crawl_frontier` into the single leased fetch queue
  - What to do: Upgrade `ops.crawl_frontier` to support production fetch orchestration. Add exactly these new columns: `lease_owner Nullable(String)`, `lease_expires_at Nullable(DateTime64(3, 'UTC'))`, `attempt_count UInt16`, `last_attempt_at Nullable(DateTime64(3, 'UTC'))`, `last_fetch_id Nullable(String)`, `last_status_code Nullable(UInt16)`, `last_error_code Nullable(String)`, `last_error_message Nullable(String)`, `etag Nullable(String)`, `last_modified Nullable(String)`, and `discovery_kind LowCardinality(String)`. Freeze the fetch state machine to: `pending`, `leased`, `fetched`, `not_modified`, `retry`, `dead`, `blocked`. Map outcomes exactly as follows: `200/204 -> fetched`; `304 -> not_modified`; `404/410 -> dead`; `429/5xx/network timeout -> retry`; `disabled/missing-auth/unsupported-auth -> blocked`; `body-too-large -> dead`. Parse failures are not frontier states; they are tracked only in `ops.parse_log`.
  - Start here: `migrations/clickhouse/0003_ops_bronze.sql:15`; `internal/discovery/frontier.go:51`; `internal/discovery/frontier.go:187`
  - Verify: `go test ./internal/discovery ./internal/migrate -count=1` exits `0`.
  - QA: `Tool`: Bash | `Steps`: go test ./internal/discovery -run TestFrontierStateMachine -count=1 && go test ./cmd/worker-fetch -run TestClaimFrontierLease -count=1 | `Expected`: both tests exit 0 and prove the exact state mapping plus single-worker lease ownership

- [x] 3. Preserve immutable raw fetch ledger and force object-store retention for migrated HTTP sources
  - What to do: Keep `bronze.raw_document` as the immutable fetch ledger, but add typed fetch columns used by runtime filtering and replay: `fetch_id`, `final_url`, `etag`, `last_modified`, `not_modified`, and `storage_class`. Keep raw body bytes in MinIO/object-store only for migrated HTTP sources by setting their retention policy to object-store-backed behavior and updating retention code/tests accordingly. Also add fetch attempt visibility to `ops.fetch_log` with `attempt_count` and `retry_count`. `worker-fetch` must remain fetch-only and never write typed bronze source tables.
  - Start here: `migrations/clickhouse/0003_ops_bronze.sql:32`; `cmd/worker-fetch/main.go:361`; `internal/fetch/retention.go:74`
  - Verify: `go test ./internal/fetch ./cmd/worker-fetch -count=1` exits `0`.
  - QA: `Tool`: Bash | `Steps`: go test ./cmd/worker-fetch -run TestFetchOncePersistsObjectStoreOnlyForMigratedSources -count=1 && curl -fsS "http://localhost:8123/?query=SELECT%20count()%20FROM%20bronze.raw_document%20WHERE%20storage_class='inline'%20FORMAT%20TabSeparated" | `Expected`: test exits 0 and ClickHouse query prints `0`

- [x] 4. Tighten RBAC and stage ownership boundaries
  - What to do: Update bootstrap RBAC so `osint_ingest` can read `meta/ops/bronze` and insert only into `ops/bronze`. Add a new `osint_promote` role that can read `meta/ops/bronze/silver` and insert into `silver/gold`. Ensure `worker-fetch` and `worker-parse` use ingest-only privileges, while bronze-driven promote/control-plane jobs use promote privileges. Keep `osint_reader` unchanged and `osint_admin` as bootstrap/admin only.
  - Start here: `cmd/bootstrap/main.go:42`; `cmd/bootstrap/source_registry_test.go`; `PRODUCTION_READINESS.md`
  - Verify: `go test ./cmd/bootstrap -count=1` exits `0`.
  - QA: `Tool`: Bash | `Steps`: go test ./cmd/bootstrap -run TestRoleContracts -count=1 && curl -fsS "http://localhost:8123/?query=SHOW%20GRANTS%20FOR%20osint_ingest" | `Expected`: exit 0 and grant output proves ingest writes only to `ops.*` and `bronze.*`

- [x] 5. Freeze replay-stable canonical ID rules and parity harness before cutover
  - What to do: Remove `raw_id` from canonical event/observation identity generation in `internal/promote/pipeline.go`. Use exact deterministic ID formulas: `event = stableID('event', source_id, native_id_or_contenthash, event_type, starts_at)` and `observation = stableID('observation', source_id, native_id_or_contenthash, observation_type, subject_natural_key, observed_at)`. Keep entity IDs on natural-key logic. Add parity tests that run old fixture semantics and new bronze-driven semantics over the same deterministic local payloads and assert identical silver IDs plus stable gold rollups across reruns.
  - Start here: `internal/promote/pipeline.go:475`; `cmd/control-plane/jobs_promote.go:42`; `internal/promote/pipeline_test.go`
  - Verify: `go test ./internal/promote ./cmd/control-plane -run 'TestPromoteFromBronzePreservesCanonicalIDs|TestPromoteParity' -count=1` exits `0`.
  - QA: `Tool`: Bash | `Steps`: go test ./internal/promote -run TestCanonicalIDsIgnoreRawID -count=1 && go test ./cmd/control-plane -run TestPromoteFromBronzePreservesCanonicalIDs -count=1 | `Expected`: both tests exit 0 and prove identical IDs across refetch/reparse/replay

- [x] 6. Rewire control-plane jobs to seed frontier and orchestrate source pipeline stages
  - What to do: Keep the existing public job names (`ingest-geopolitical`, `ingest-safety-security`) and repurpose them into orchestration wrappers. Each job must: load concrete source metadata from `meta.source_registry`, seed `ops.crawl_frontier` from `entrypoints` when needed, invoke fetch stage, invoke parse stage, invoke promote stage, and record per-source stats. The exact source fan-out is fixed to: geopolitical -> `seed:gdelt`, `fixture:reliefweb`, `fixture:acled`; safety/security -> `fixture:opensanctions`, `fixture:nasa-firms`, `fixture:noaa-hazards`, `fixture:kev`. `fixture:safety` is only a selector alias that expands to those four safety sources and never fetches itself. ACLED is skipped cleanly when credentials are absent and reported in job stats under `disabled_sources`.
  - Start here: `cmd/control-plane/jobs_geopolitical.go:23`; `cmd/control-plane/jobs_safety.go:22`; `cmd/control-plane/main.go`
  - Verify: `go test ./cmd/control-plane -run 'TestRunOnceHelp|TestIngestDomainJobOrchestratesSources' -count=1` exits `0`.
  - QA: `Tool`: Bash | `Steps`: go test ./cmd/control-plane -run TestIngestDomainJobOrchestratesSources -count=1 && docker compose exec control-plane /control-plane run-once --help | `Expected`: test exits 0 and help output still includes the same public job names

- [x] 7. Implement frontier-driven fetch execution in `worker-fetch`
  - What to do: Extend `worker-fetch` so it can claim frontier work for one source at a time, honor source rate limits, fetch entrypoint/request URLs via the existing HTTP client, persist only `ops.fetch_log` + `bronze.raw_document`, and update frontier state/lease/result columns deterministically. Keep existing `fetch-once` and `replay-once` commands, and add one new runtime command `fetch-source --source-id <id> --limit <n>` for deterministic stage execution. Use `allowed_hosts` + `NormalizeURL` validation before fetch. Auth support must resolve the frozen env-ref contract from Task 1; if the referenced env value is absent, the fetch is blocked and frontier state becomes `blocked` without an HTTP request. In this plan, task-level verification uses `httptest.NewServer`-style local HTTP stubs; the compose fixture service is introduced later for full-stack E2E only.
  - Start here: `cmd/worker-fetch/main.go:315`; `internal/fetch/client.go:309`; `internal/discovery/frontier.go:150`
  - Verify: `go test ./internal/fetch ./cmd/worker-fetch -count=1` exits `0`.
  - QA: `Tool`: Bash | `Steps`: go test ./cmd/worker-fetch -run TestFetchSourceClaimsFrontierAndPersistsLedger -count=1 | `Expected`: exit 0 and test proves frontier state updates plus writes only to `ops.fetch_log` and `bronze.raw_document`

- [x] 8. Implement parse-to-bronze runtime in `worker-parse`
  - What to do: Extend `worker-parse` to read stored raw payloads via `raw_id`, resolve parser and parse config from `meta.source_registry`, run the registered parser, write one `ops.parse_log` row per parse attempt, and persist one or more typed bronze rows into the configured `bronze.src_<slug>_v1` table. Add a new command `parse-source --source-id <id> --limit <n>`. Freeze the parsed bronze row identity to `source_record_key + source_record_index`, where `source_record_key = firstNonEmpty(candidate.NativeID, candidate.ContentHash)` and `source_record_index` is the zero-based candidate index within one parse result. Parse failures remain in `ops.parse_log` and never mutate frontier fetch state.
  - Start here: `cmd/worker-parse/main.go:22`; `internal/parser/registry.go:60`; `migrations/clickhouse/0005_baseline_tables.sql:71`
  - Verify: `go test ./internal/parser ./cmd/worker-parse -count=1` exits `0`.
  - QA: `Tool`: Bash | `Steps`: go test ./cmd/worker-parse -run TestParseSourceWritesTypedBronzeRows -count=1 | `Expected`: exit 0 and test proves multiple candidates map to unique `(source_record_key, source_record_index)` rows in the source bronze table

- [x] 9. Add static per-source bronze tables and retire `bronze.raw_structured_row` from migrated flows
  - What to do: Create migration-defined typed bronze tables for the seven concrete sources listed in Task 1. Every table must share this mandatory column contract exactly: `raw_id String`, `fetch_id String`, `source_id LowCardinality(String)`, `parser_id LowCardinality(String)`, `parser_version String`, `source_record_key String`, `source_record_index UInt32`, `record_kind LowCardinality(String)`, `native_id Nullable(String)`, `source_url String`, `canonical_url Nullable(String)`, `fetched_at DateTime64(3, 'UTC')`, `parsed_at DateTime64(3, 'UTC')`, `occurred_at Nullable(DateTime64(3, 'UTC'))`, `published_at Nullable(DateTime64(3, 'UTC'))`, `title Nullable(String)`, `summary Nullable(String)`, `status Nullable(String)`, `place_hint Nullable(String)`, `lat Nullable(Float64)`, `lon Nullable(Float64)`, `severity Nullable(String)`, `content_hash String`, `schema_version UInt32`, `record_version UInt64`, `attrs String`, `evidence String`, `payload_json String`. Use `ReplacingMergeTree(record_version)`, `PARTITION BY toYYYYMM(parsed_at)`, and `ORDER BY (source_record_key, parsed_at, raw_id, source_record_index)`. Keep `bronze.raw_structured_row` in schema, but migrated sources must not write to it.
  - Start here: `migrations/clickhouse/0003_ops_bronze.sql:48`; `migrations/clickhouse/0005_baseline_tables.sql:71`; `migrations/clickhouse/0005_baseline_tables.sql:90`
  - Verify: `go test ./internal/migrate -count=1` exits `0`.
  - QA: `Tool`: Bash | `Steps`: go test ./internal/migrate -run TestSourceBronzeTables -count=1 && curl -fsS "http://localhost:8123/?query=SHOW%20TABLES%20FROM%20bronze%20LIKE%20'src_%25'%20FORMAT%20TabSeparated" | `Expected`: exit 0 and output lists all seven source tables with `_v1` suffixes

- [x] 10. Replace sample-input promotion with bronze-driven ClickHouse ELT
  - What to do: Remove runtime dependence on `PROMOTE_PIPELINE_INPUT` / `SampleInputs()` for normal promote flow. Make `promote` operate on bronze tables selected by `source_id` and `promote_profile`, using deterministic `INSERT ... SELECT` SQL into canonical silver tables and the existing metric materialization path. Keep `internal/promote/pipeline.go` only as reusable canonical-shape logic and anti-join pattern reference where helpful, but the runtime path must promote directly from bronze tables in ClickHouse. Promotion must remain idempotent by source/time slice and must use deterministic delete-and-reinsert or anti-join strategy per target table. Refresh existing metric/gold outputs after silver changes.
  - Start here: `cmd/control-plane/jobs_promote.go:30`; `internal/promote/pipeline.go:263`; `internal/metrics/materialization_sql.go:19`
  - Verify: `go test ./internal/promote ./cmd/control-plane -count=1` exits `0`.
  - QA: `Tool`: Bash | `Steps`: go test ./cmd/control-plane -run TestPromoteFromBronzeSourceTables -count=1 && go test ./internal/promote -run TestBronzePromoteSQLIsIdempotent -count=1 | `Expected`: both tests exit 0 and prove bronze->silver promotion plus metric refresh are deterministic

- [x] 11. Migrate geopolitical sources from fixture loaders to HTTP -> bronze -> silver
  - What to do: Replace the runtime geopolitical path so `seed:gdelt`, `fixture:reliefweb`, and `fixture:acled` no longer use in-process fixture loaders. Keep `internal/packs/geopolitical/geopolitical.go` only as transformation logic and/or test helpers, but runtime data must come from `bronze.src_seed_gdelt_v1`, `bronze.src_fixture_reliefweb_v1`, and `bronze.src_fixture_acled_v1`. Add parser/profile config and promote SQL so these bronze tables produce the same canonical `silver.fact_event`, `silver.dim_entity`, bridge tables, and pack metrics currently emitted by the fixture plan. ACLED remains credential-gated; when env is absent, bronze/promotion for ACLED is skipped and the domain job still succeeds. Task-level validation here uses `httptest.NewServer` and stubbed ClickHouse patterns, not the later compose E2E fixture service.
  - Start here: `cmd/control-plane/jobs_geopolitical.go:23`; `internal/packs/geopolitical/geopolitical.go:170`; `internal/packs/geopolitical/geopolitical_test.go`
  - Verify: `go test ./internal/packs/geopolitical ./cmd/control-plane -run 'TestGeopoliticalBronzePromote|TestGeopoliticalJob' -count=1` exits `0`.
  - QA: `Tool`: Bash | `Steps`: go test ./cmd/control-plane -run TestGeopoliticalBronzePromote -count=1 && go test ./internal/packs/geopolitical -count=1 | `Expected`: both tests exit 0 and prove GDELT/ReliefWeb bronze rows promote to the same canonical event/entity/metric outputs as the legacy deterministic fixtures

- [x] 12. Migrate safety/security concrete sources from fixture loaders to HTTP -> bronze -> silver
  - What to do: Replace runtime safety/security fixture ingestion with bronze-driven processing for `fixture:opensanctions`, `fixture:nasa-firms`, `fixture:noaa-hazards`, and `fixture:kev`. Preserve `fixture:safety` only as orchestration alias. Each concrete source must fetch via HTTP, parse into its source bronze table, and promote into canonical observation/entity rows plus the existing safety/security metric set. Keep the exact shipped metric IDs unchanged. Move any source-specific mapping logic out of runtime fixture loaders and into parser config or bronze-to-silver SQL. Task-level validation here uses `httptest.NewServer` and stubbed ClickHouse patterns, not the later compose E2E fixture service.
  - Start here: `cmd/control-plane/jobs_safety.go:22`; `internal/packs/safety/safety.go:146`; `internal/packs/safety/safety_test.go`
  - Verify: `go test ./internal/packs/safety ./cmd/control-plane -run 'TestSafetyBronzePromote|TestSafetyJob' -count=1` exits `0`.
  - QA: `Tool`: Bash | `Steps`: go test ./cmd/control-plane -run TestSafetyBronzePromote -count=1 && go test ./internal/packs/safety -count=1 | `Expected`: both tests exit 0 and prove OpenSanctions/FIRMS/NOAA/KEV bronze rows promote into canonical observation/entity rows and unchanged safety metrics

- [x] 13. Add compose-level HTTP fixture service, docs, and full E2E pipeline coverage
  - What to do: Add a compose-level deterministic HTTP fixture service for full-stack E2E so no compose test depends on public internet. This service is only for end-to-end/docker-compose verification; earlier task-level tests continue to use `httptest` and stubbed ClickHouse. Use the compose fixture service to serve stable payloads for GDELT, ReliefWeb, OpenSanctions, NASA FIRMS, NOAA hazards, KEV, and ACLED credential-gated stubs. Update `docker-compose.yml`, `test/e2e/pipeline_test.go`, `README.md`, `docs/capability-matrix.json`, and `docs/capability-matrix.md` so the documented runtime is the new HTTP -> bronze -> silver architecture. Explicitly document that maritime, aviation, and space remain non-HTTP fixture packs until concrete source registry entries are introduced.
  - Start here: `test/e2e/pipeline_test.go:19`; `docker-compose.yml`; `docs/capability-matrix.json`
  - Verify: `docker compose up -d --build` exits successfully with the local HTTP fixture service available for tests.
  - QA: `Tool`: Bash | `Steps`: docker compose up -d --build && go test ./test/e2e -tags=e2e -run TestHTTPSourcePipeline -count=1 | `Expected`: compose exits successfully, the E2E test exits 0, and no public endpoint dependency is required

- [x] 14. Cut over migrated HTTP sources, remove runtime fixture bypasses, and prove replay/backfill safety
  - What to do: Remove runtime fixture-only bypasses for migrated HTTP sources from `cmd/control-plane/jobs_geopolitical.go`, `cmd/control-plane/jobs_safety.go`, and any helper path those jobs still use. Keep non-migrated maritime/aviation/space runtime paths unchanged. Add replay/backfill safety tests that seed duplicate or retried bronze inputs, rerun fetch/parse/promote, and prove no canonical duplication. Pin explicit ClickHouse dedup settings in runtime SQL where async inserts or dependent materializations rely on dedup behavior. Add one isolated backfill path using duplicate bronze/silver staging tables plus `MOVE PARTITION` or equivalent tested batch cutover for replayable source backfills.
  - Start here: `cmd/control-plane/jobs_geopolitical.go:36`; `cmd/control-plane/jobs_safety.go:35`; `internal/promote/pipeline.go:263`
  - Verify: `grep -R "loadGDELTFixtures\|loadReliefWebFixtures\|loadACLEDFixtures\|loadOpenSanctionsFixtures\|loadFIRMSFixtures\|loadNOAAHazardFixtures\|loadKEVFixtures" cmd/control-plane internal/packs | grep -v _test.go` returns no migrated runtime path matches.
  - QA: `Tool`: Bash | `Steps`: go test ./cmd/control-plane ./internal/promote -run 'TestReplayDoesNotDuplicateCanonicalRows|TestBackfillCutover' -count=1 | `Expected`: exit 0 and tests prove repeated fetch/parse/promote cycles keep canonical row identities and counts stable

### Legacy Final Verification
- [x] F1. Plan Compliance Audit — oracle
- [x] F2. Code Quality Review — unspecified-high
- [x] F3. Real Manual QA — unspecified-high (+ playwright if UI)
- [x] F4. Scope Fidelity Check — deep

## Source Plan: Global OSINT Backend Delta Closure Plan (`global-osint-backend-delta-closure.md`)
- Original scope summary: Close the detected manifest delta by freezing one canonical runtime contract, finishing the missing analytics surface, eliminating correctness drift in rerun/materialization behavior, and rebasing documentation only where the current architecture intentionally differs from the manifest.
- Carried numbered tasks: `9`
- Carried legacy final-verification tasks: `4`

### Numbered Tasks
- [x] 1. Freeze the canonical delta matrix and source-of-truth contract
  - What to do: Create one machine-readable and one human-readable capability matrix that classifies every manifest claim as `implemented`, `partial`, or `roadmap`. Lock these defaults: `control-plane run-once --job ...` is the canonical orchestration surface, `meta.schema_migrations` remains bootstrap-owned, single-node runtime is canonical, renderer stays stubbed unless a consumer appears, and advanced ClickHouse optimizations are not considered shipped unless runtime evidence exists.
  - Start here: `cmd/control-plane/main.go:51`; `cmd/api/main.go:47`; `cmd/renderer/main.go:8`
  - Verify: A capability matrix exists in repo docs mapping every detected delta claim to `implemented`, `partial`, or `roadmap`.
  - QA: `Tool`: Bash | `Steps`: Read the new capability matrix and grep for job surface, renderer scope, and migration-ledger ownership across docs. | `Expected`: Every searched artifact agrees with the matrix and uses the same chosen defaults.

- [x] 2. Align migration-ledger ownership and immutable migration policy
  - What to do: Keep `meta.schema_migrations` bootstrap-owned, but make that ownership explicit everywhere. Remove stale assumptions about `id` columns or SQL-created ledger state, add immutable-migration guardrails so edited historical files cannot be re-applied unnoticed, and verify the exact ledger schema and checksum behavior in docs/tests.
  - Start here: `internal/migrate/http_runner.go:21`; `docs/runbooks/upgrade-migration.md:28`; `cmd/bootstrap/main.go`
  - Verify: `curl -fsS "http://localhost:8123/?query=DESCRIBE%20TABLE%20meta.schema_migrations%20FORMAT%20TabSeparated"` matches the documented bootstrap-owned ledger schema.
  - QA: `Tool`: Bash | `Steps`: Bootstrap the stack, describe `meta.schema_migrations`, and run the documented upgrade verification commands. | `Expected`: The ledger schema matches docs exactly and verification uses real columns (`version`, `applied_at`, `checksum`, `success`, `notes`).

- [x] 3. Align jobs, docs, fixtures, and E2E tests to the actual orchestration contract
  - What to do: Remove or rewrite stale assumptions that jobs are triggered by POST `/v1/jobs/...`. The runtime contract stays `control-plane run-once --job ...`; docs, E2E tests, fixtures, and runbooks must stop assuming HTTP job execution. Reconcile the actual supported job names with the docs and add the missing maritime and space `run-once` jobs so all shipped packs share one orchestration surface.
  - Start here: `cmd/control-plane/main.go:68`; `test/e2e/pipeline_test.go:50`; `cmd/control-plane/jobs_geopolitical.go`
  - Verify: `go run ./cmd/control-plane run-once --help` lists exactly the jobs the updated docs/tests claim are supported.
  - QA: `Tool`: Bash | `Steps`: Run `go run ./cmd/control-plane run-once --help`; execute one documented in-scope ingest job through `run-once`. | `Expected`: Help output and docs agree exactly, and the documented ingest job succeeds through the CLI surface.

- [x] 4. Standardize metric materialization and rerun idempotency
  - What to do: Pick one metric materialization path and enforce it repo-wide. Convert pack/runtime writers to a single pipeline from metric registry -> `silver.metric_contribution` -> `gold.metric_state` -> `gold.metric_snapshot`, then make reruns idempotent for duplicate-sensitive `MergeTree` tables by adding deterministic upsert strategy, latest-version semantics, or explicit dedup-on-write/read contract. Extend the same decision to `gold.hotspot_snapshot` and any cross-domain outputs.
  - Start here: `internal/metrics/rollup.go:183`; `internal/metrics/rollup.go:227`; `internal/packs/geopolitical/geopolitical.go:222`
  - Verify: One documented materialization path exists and all metric-producing jobs use it consistently.
  - QA: `Tool`: Bash | `Steps`: Run one in-scope ingest job; inspect `silver.metric_contribution`, `gold.metric_state`, and `gold.metric_snapshot` for the emitted metric IDs. | `Expected`: The chosen canonical path is observable end to end and no direct side-path contradicts it.

- [x] 5. Complete core metric parity and exact metric registry coverage
  - What to do: Add the 10 missing core metrics, preserve exact manifest IDs, and make each metric formally registered and computed. This task covers `entity_count_approx`, `source_count_approx`, `confidence_weighted_activity`, `dedup_rate`, `schema_drift_rate`, `evidence_density`, `cross_source_confirmation_rate`, `trend_24h`, `acceleration_7d_vs_30d`, and `anomaly_zscore_30d`, while preserving the already-shipped core metrics.
  - Start here: `internal/metrics/registry.go:46`; `internal/metrics/contribution.go:55`; `internal/metrics/rollup.go:258`
  - Verify: `meta.metric_registry` contains all 18 requested core metric IDs exactly as named in the manifest.
  - QA: `Tool`: Bash | `Steps`: Run the core metric fixture pipeline; query `meta.metric_registry` and `gold.metric_snapshot` for all requested core metric IDs. | `Expected`: All 18 core metric IDs exist exactly and have runtime-backed outputs.

- [x] 6. Make hotspots and cross-domain runtime outputs real and testable
  - What to do: Either populate `gold.hotspot_snapshot` and `gold.api_v1_cross_domain` from real runtime data or explicitly reclassify them as non-shipped. Default for this plan: make them real. Define how hotspot ranking is computed from the completed metric surface, define cross-domain composition inputs, and ensure the API endpoints return populated runtime outputs rather than thin projections over empty tables.
  - Start here: `migrations/clickhouse/0005_baseline_tables.sql:415`; `migrations/clickhouse/0007_api_expansion_views.sql:177`; `migrations/clickhouse/0007_api_expansion_views.sql:243`
  - Verify: `SELECT count() FROM gold.hotspot_snapshot` returns `> 0` after fixture ingests complete.
  - QA: `Tool`: Bash | `Steps`: Run in-scope ingest jobs after core/domain metric closure; query `gold.hotspot_snapshot` and call `/v1/analytics/hotspots`. | `Expected`: Hotspot rows exist, API payload is non-empty, and ranking semantics match the documented formula.

- [x] 7. Complete geopolitical, maritime, and aviation metric parity
  - What to do: Finish the missing manifest-aligned metric sets for geopolitical, maritime, and aviation. Geopolitical must add `sanction_activity_score`, `humanitarian_pressure_score`, `media_attention_acceleration`, and `infrastructure_disruption_score`. Maritime must expand from `ais_dark_hours` / `shadow_fleet_score` to the full requested family and normalize `ais_dark_hours_sum` exactly. Aviation must move `military_likelihood_score` and `route_irregularity_score` into the formal registry and add the six missing aviation metrics.
  - Start here: `internal/packs/geopolitical/geopolitical.go:204`; `internal/packs/geopolitical/geopolitical.go:267`; `internal/packs/maritime/metrics.go:39`
  - Verify: `meta.metric_registry` contains all requested geopolitical, maritime, and aviation metric IDs exactly as named in the manifest.
  - QA: `Tool`: Bash | `Steps`: Run the three ingest jobs on fixture mode; query registry and snapshots for every requested metric ID in those packs. | `Expected`: All requested IDs exist exactly and produce runtime-backed data.

- [x] 8. Complete space and safety/security metric parity with exact manifest IDs
  - What to do: Finish the exact requested metric families for space and safety/security. Rename or alias current space outputs from `overpass_density` / `conjunction_risk` to exact manifest IDs and add the missing five space metrics. Rename or alias current safety outputs from `fire_hotspot` / `sanctions_exposure` to exact manifest IDs and add the missing four safety metrics, including cyber and weather/coastal outputs.
  - Start here: `internal/packs/space/analysis.go:249`; `internal/packs/space/adapter.go:14`; `internal/packs/safety/safety.go:584`
  - Verify: `meta.metric_registry` contains all requested space and safety/security metric IDs exactly as named in the manifest.
  - QA: `Tool`: Bash | `Steps`: Run the space and safety ingest jobs on fixtures; query `meta.metric_registry` and `gold.metric_snapshot` for every requested metric ID. | `Expected`: All requested IDs exist exactly and produce runtime-backed outputs.

- [x] 9. Re-baseline renderer and advanced ClickHouse feature claims with targeted implementation only where justified
  - What to do: Close the remaining non-analytics delta by distinguishing shipped features from roadmap features. Keep renderer health-only and update docs/manifests/tests accordingly. For advanced ClickHouse features, implement `s3()`-backed staged bulk-dump ingestion and worker-side async inserts because they directly support existing staged dataset and telemetry paths; explicitly reclassify `url()`, `file()`, `S3Queue`, projections, and data skipping indexes as deferred optimization claims for this release.
  - Start here: `cmd/renderer/main.go:8`; `cmd/control-plane/jobs_place_build.go:92`; `internal/metrics/rollup.go:206`
  - Verify: All shipped docs/manifests clearly distinguish `implemented` vs `roadmap` for renderer and advanced ClickHouse features.
  - QA: `Tool`: Bash | `Steps`: Call renderer `/health`; grep docs/manifests for renderer claims. | `Expected`: Runtime behavior and docs agree exactly on the renderer contract.

### Legacy Final Verification
- [x] F1. Plan Compliance Audit - oracle
- [x] F2. Delta Coverage Review - unspecified-high
- [x] F3. Runtime Contract QA - unspecified-high
- [x] F4. Scope Fidelity Check - deep

## Source Plan: Global OSINT Backend Completion Plan (`global-osint-backend-completion.md`)
- Original scope summary: Take the repo from Phase A scaffold to full backlog completion by first repairing reproducibility and bootstrap safety, then building the place/governance spine, then the ingest-promote-serve pipeline, then domain packs, and finally the optional scale-out path.
- Carried numbered tasks: `26`
- Carried legacy final-verification tasks: `4`

### Numbered Tasks
- [x] 1. Repair repo reality and reproducible build baseline
  - What to do: Make the current scaffold truthfully buildable before any epic closure. Fix `bootstrap` image packaging, replace nonexistent bind-mount assumptions with checked-in config assets, pin all container images to explicit versions, and align ClickHouse health probing with the repo's HTTP-only architecture.
  - Start here: `build/bootstrap.Dockerfile:3`; `build/bootstrap.Dockerfile:9`; `cmd/bootstrap/main.go:16`
  - Verify: `docker compose config` exits `0`.
  - QA: `Tool`: Bash | `Steps`: Run `docker compose config`; run `docker compose build bootstrap api control-plane worker-fetch worker-parse renderer`. | `Expected`: Both commands exit 0 and bootstrap no longer fails on missing `internal/` or `seed` assets.

- [x] 2. Freeze migration, readiness, and internal job execution contracts
  - What to do: Decide and implement one migration-authoring rule, one readiness contract, and one deterministic internal job invocation pattern. Replace the naive SQL splitter or constrain migration files to a parser-safe format with tests, make `/v1/ready` reflect actual bootstrap/job state, define `control-plane run-once --job <job-name>` as the internal execution interface, and make `meta.schema_migrations` have one authoritative owner.
  - Start here: `internal/migrate/split.go:5`; `internal/migrate/split_test.go:5`; `internal/migrate/http_runner.go:21`
  - Verify: `go test ./internal/migrate -run TestSplitStatements` exits `0` with cases covering comments, blank statements, and semicolons inside valid ClickHouse SQL bodies or the new migration rule rejects unsupported forms.
  - QA: `Tool`: Bash | `Steps`: Start `clickhouse` and `api` without a bootstrap-ready marker; call `curl -fsS http://localhost:8080/v1/ready`; run bootstrap; call the endpoint again. | `Expected`: First response is not ready; second response is ready only after bootstrap succeeds.

- [x] 3. Complete Compose/bootstrap topology, buckets, RBAC, smoke runner, and backup hooks
  - What to do: Finish E0 as install-time substrate only. Bootstrap must create MinIO buckets (`raw`, `stage`, `backup`), initialize ClickHouse users/roles/databases, apply migrations, seed registries, expose a `verify` mode for smoke assertions, and register backup/restore hooks and manifests without pulling heavy data loads into install time.
  - Start here: `cmd/bootstrap/main.go:41`; `docker-compose.yml:30`; `internal/migrate/http_runner.go:55`
  - Verify: `docker compose up -d clickhouse minio` followed by `docker compose run --rm bootstrap` exits `0`.
  - QA: `Tool`: Bash | `Steps`: Run `docker compose up -d clickhouse minio`; run `docker compose run --rm bootstrap`; run `docker compose run --rm bootstrap verify`. | `Expected`: All commands exit 0; verification confirms buckets `raw`, `stage`, and `backup`, plus schema and seed readiness.

- [x] 4. Expand meta registries and schema standards
  - What to do: Add the remaining `meta` registries (`parser_registry`, `metric_registry`, `api_schema_registry`) and freeze naming, timestamp, versioning, JSON, partitioning, ordering-key, and compatibility conventions. Capture these standards in migration files plus a repo-local ADR or schema standard document used by later tasks.
  - Start here: `migrations/clickhouse/0002_core_tables.sql:1`; `migrations/clickhouse/0001_init.sql:1`; `cmd/api/main.go:54`
  - Verify: After bootstrap, ClickHouse queries confirm `meta.parser_registry`, `meta.metric_registry`, and `meta.api_schema_registry` exist.
  - QA: `Tool`: Bash | `Steps`: Run bootstrap; query `system.tables` for `meta.parser_registry`, `meta.metric_registry`, and `meta.api_schema_registry`. | `Expected`: All three tables exist exactly once with the expected engines.

- [x] 5. Expand ops/bronze/silver/gold baseline tables and performance conventions
  - What to do: Add the remaining baseline tables required for the full platform: `ops.parse_log`, `ops.unresolved_location_queue`, `ops.quality_incident`, `bronze.raw_structured_row`, `silver.dim_place`, `silver.place_polygon`, `silver.place_hierarchy`, `silver.dim_entity`, `silver.entity_alias`, `silver.fact_observation`, `silver.fact_event`, `silver.fact_track_point`, `silver.fact_track_segment`, bridge tables, `silver.metric_contribution`, `gold.metric_state`, `gold.metric_snapshot`, `gold.hotspot_snapshot`, and compatibility views scaffolding. Bake in monthly partitions, order keys, low-cardinality columns, TTL, and codec/projection defaults where justified.
  - Start here: `migrations/clickhouse/0003_ops_bronze.sql:15`; `migrations/clickhouse/0002_core_tables.sql:26`; `https://clickhouse.com/docs/engines/table-engines/mergetree-family/replacingmergetree`
  - Verify: After bootstrap, `system.tables` contains every planned baseline table for `ops`, `bronze`, `silver`, and `gold`.
  - QA: `Tool`: Bash | `Steps`: Run bootstrap; query `system.tables` for the required `ops`, `bronze`, `silver`, and `gold` tables. | `Expected`: All planned baseline tables exist exactly once.

- [x] 6. Freeze `source_registry` governance schema and seed evolution behavior
  - What to do: Make `meta.source_registry` decision-complete for crawl governance. Add fields for rate limiting, retention class, kill-switch reason/state, review state, auth configuration mode, backfill policy, license/terms provenance, and parser routing; define seed versioning and update semantics so registry evolution is idempotent and auditable instead of insert-only.
  - Start here: `seed/source_registry.json:11`; `cmd/bootstrap/main.go:108`; `migrations/clickhouse/0002_core_tables.sql:1`
  - Verify: `DESCRIBE TABLE meta.source_registry` includes explicit governance columns for rate limit, review state, retention class, kill switch, and auth/refresh policy.
  - QA: `Tool`: Bash | `Steps`: Apply bootstrap with seed version A; change one governance field in the seed fixture; rerun bootstrap; query the source row history/current version. | `Expected`: Current-state row updates as designed, version history is preserved, and uncontrolled duplicates are not created.

- [x] 7. Implement place dataset acquisition and staging jobs
  - What to do: Implement deterministic control-plane jobs for `geoBoundaries gbOpen` and GeoNames acquisition. Each job must download, checksum, stage to MinIO, write provenance into ClickHouse job logs, support reruns, and separate raw acquisition from later place-graph materialization.
  - Start here: `cmd/control-plane/main.go:8`; `ops.job_run`; `https://www.geoboundaries.org/api.html`
  - Verify: `docker compose run --rm control-plane run-once --job geoboundaries-sync` exits `0` and records a successful `ops.job_run` row.
  - QA: `Tool`: Bash | `Steps`: Run `control-plane run-once --job geoboundaries-sync`; run `control-plane run-once --job geonames-sync`; query `ops.job_run`. | `Expected`: Both jobs succeed, record provenance/checksum metadata, and stage artifacts for later materialization.

- [x] 8. Build place graph tables, internal IDs, hierarchy, and polygon dictionary
  - What to do: Materialize the staged place datasets into `silver.dim_place`, `silver.place_hierarchy`, and `silver.place_polygon`. Generate internal place IDs, world/continent pseudo-places, parent chains, alternate names, centroid/bbox fields, H3 coverage, deepest admin level, and the reverse-geocoding polygon dictionary.
  - Start here: `migrations/clickhouse/0002_core_tables.sql:29`; `https://www.geoboundaries.org/index.html`; `https://www.geonames.org/export/codes.html`
  - Verify: `docker compose run --rm control-plane run-once --job place-build` exits `0`.
  - QA: `Tool`: Bash | `Steps`: Run `control-plane run-once --job place-build`; query counts from `silver.dim_place`, `silver.place_hierarchy`, and `silver.place_polygon`. | `Expected`: All three tables are populated and world/continent pseudo-places exist.

- [x] 9. Add place validation fixtures, coverage reports, and reverse-geocode benchmarks
  - What to do: Add deterministic fixture suites and reporting for the place system. Create fixture coordinates, ambiguous-name cases, missing-depth cases, overlap checks, and per-country coverage reporting; benchmark reverse-geocode latency and publish coverage/quality artifacts for release gates.
  - Start here: `cmd/api/main_test.go:9`; `internal/migrate/split_test.go:5`; `https://clickhouse.com/docs/sql-reference/statements/create/dictionary/layouts/polygon`
  - Verify: `go test ./... -run TestReverseGeocodeFixtures` exits `0`.
  - QA: `Tool`: Bash | `Steps`: Run `go test ./... -run TestReverseGeocodeFixtures`; run `go test ./... -run TestPlaceCoverageReport`. | `Expected`: Both suites exit 0 and the coverage artifact shows the expected per-country depth availability.

- [x] 10. Implement discovery engine and frontier ranking
  - What to do: Build the discovery pipeline that turns registry seeds into normalized frontier rows. Implement exact RFC 9309 robots handling, sitemap parsing (including gzip and indexes), RSS/Atom discovery and parsing, URL normalization, duplicate suppression, host-level policy enforcement, and baseline frontier ranking for freshness, source quality, and diversity.
  - Start here: `cmd/control-plane/main.go:8`; `migrations/clickhouse/0003_ops_bronze.sql:15`; `https://www.rfc-editor.org/rfc/rfc9309.html`
  - Verify: `go test ./... -run TestDiscoveryRobotsPolicy` exits `0` and proves correct handling of `4xx`, `5xx`, redirects, `Allow`, and `Disallow` precedence.
  - QA: `Tool`: Bash | `Steps`: Run the discovery integration suite against the local fixture site; query `ops.crawl_frontier` for discovered canonical URLs. | `Expected`: Expected URLs are present once each, normalized, and ranked deterministically.

- [x] 11. Implement fetch worker and raw retention/replay policy
  - What to do: Build `worker-fetch` into the public-source fetch runtime. Support GET/HEAD, conditional fetch, retry/backoff, gzip/br handling, MIME sniffing, max-size guardrails, provenance capture, content hashing, large-body object-store persistence, and retention/replay classes by source policy.
  - Start here: `cmd/worker-fetch/main.go:8`; `migrations/clickhouse/0003_ops_bronze.sql:32`; `migrations/clickhouse/0003_ops_bronze.sql:48`
  - Verify: `go test ./... -run TestFetchWorker` exits `0` for conditional GET, compression, retries, and size-limit fixtures.
  - QA: `Tool`: Bash | `Steps`: Run the fetch integration job against fixture URLs; query `ops.fetch_log` and `bronze.raw_document`. | `Expected`: Successful fetch rows, content hashes, and object-store references are recorded.

- [x] 12. Implement parser framework and structured parsers/extractors
  - What to do: Turn `worker-parse` into the parser runtime. Define a versioned parser interface plus contracts for raw input, canonical candidate output, and parser errors; implement JSON, CSV/TSV, XML, RSS, Atom, and WARC-capable structured parsing first, with HTML profile extraction second and browser-rendered extraction left disabled until explicitly required by a pack.
  - Start here: `cmd/worker-parse/main.go:8`; `migrations/clickhouse/0002_core_tables.sql:1`; `https://www.rssboard.org/rss-specification`
  - Verify: `go test ./... -run TestParserRegistry` exits `0` and proves parser lookup/version routing works.
  - QA: `Tool`: Bash | `Steps`: Run the structured parser suite; execute `control-plane run-once --job parse-raw --source-id fixture:site`; query parse logs. | `Expected`: Supported fixture formats parse successfully and route through the declared parser registry entries.

- [x] 13. Implement canonical envelopes, IDs, evidence, and schema-version contracts
  - What to do: Finalize the stable canonical schemas for observations, events, entities, tracks, evidence, and metric contributions. Define deterministic ID generation, source-native ID retention, content-hash fallback rules, evidence payload structure, parser/version provenance, and `schema_version` / `record_version` semantics.
  - Start here: `cmd/api/main.go:54`; `migrations/clickhouse/0003_ops_bronze.sql:48`; `https://clickhouse.com/docs/sql-reference/data-types/newjson`
  - Verify: `go test ./... -run TestCanonicalIDs` exits `0` and proves deterministic ID generation and source-native retention.
  - QA: `Tool`: Bash | `Steps`: Run `go test ./... -run TestCanonicalEnvelope`; inspect fixture outputs or query candidate tables. | `Expected`: Observation, event, entity, track, and evidence contracts all include the required stable fields.

- [x] 14. Implement location attribution and unresolved-location workflow
  - What to do: Implement the mandatory geo-anchor precedence chain for points, polygons, place names, track-derived context, entity-home fallback, and source-jurisdiction fallback. Populate `place_id`, `continent_id`, `admin0_id`-`admin4_id`, `geo_anchor_type`, `geo_method`, `geo_confidence`, and `deepest_admin_level`; route low-confidence or failed records into `ops.unresolved_location_queue` with reprocessing support.
  - Start here: `migrations/clickhouse/0003_ops_bronze.sql:15`; `https://clickhouse.com/docs/sql-reference/statements/create/dictionary/layouts/polygon`; `https://www.geonames.org/export/codes.html`
  - Verify: `go test ./... -run TestLocationAttribution` exits `0` for explicit coordinates, place names with context, and source-jurisdiction fallback fixtures.
  - QA: `Tool`: Bash | `Steps`: Run the attribution fixture suite for explicit coordinates and place-name fixtures. | `Expected`: Records resolve to the expected `place_id` and deepest available admin chain with recorded method/confidence.

- [x] 15. Implement end-to-end promotion pipeline into silver facts
  - What to do: Connect discovery, fetch, parse, canonicalization, and location attribution into the promotion path that writes only resolved records into `silver` fact tables. Promotion must be idempotent, record provenance, and distinguish observations, events, entities, and tracks according to the canonical schema.
  - Start here: `cmd/bootstrap/main.go:65`; `migrations/clickhouse/0003_ops_bronze.sql:48`; `cmd/api/main.go:28`
  - Verify: `docker compose run --rm control-plane run-once --job promote --source-id fixture:events` exits `0`.
  - QA: `Tool`: Bash | `Steps`: Run the fixture discovery/fetch/parse/promotion sequence via `control-plane run-once --job promote --source-id fixture:events`; query `silver.fact_observation` and `silver.fact_event`. | `Expected`: Canonical silver facts are created with populated geo fields and provenance links.

- [x] 16. Implement core serving views and core API read paths
  - What to do: Replace the route stubs for the core read surface with real query paths backed by compatibility views and explicit filters/pagination. This task covers `GET /v1/health`, `GET /v1/ready`, `GET /v1/version`, `GET /v1/schema`, `GET /v1/jobs`, `GET /v1/jobs/{jobId}`, `GET /v1/sources`, `GET /v1/sources/{sourceId}`, `GET /v1/places`, `GET /v1/places/{placeId}`, `GET /v1/events`, `GET /v1/events/{eventId}`, `GET /v1/observations`, and `GET /v1/observations/{recordId}`.
  - Start here: `cmd/api/main.go:17`; `cmd/api/main.go:48`; `cmd/api/main.go:54`
  - Verify: `go test ./... -run TestAPICoreContracts` exits `0`.
  - QA: `Tool`: Bash | `Steps`: Start the stack; call `/v1/jobs`, `/v1/sources`, `/v1/places`, `/v1/events`, and `/v1/observations` against fixture data. | `Expected`: Each endpoint returns a real JSON envelope with data from serving views, not stubbed `kind/items/path` payloads.

- [x] 17. Implement deduplication and entity resolution baseline
  - What to do: Add document, observation, and entity resolution logic. Normalize URLs, hash content, handle replay/live/archive collisions, generate entity candidates from strong identifiers and aliases, score matches into `exact`, `probable`, `possible`, and `unknown`, and materialize current-state entity/alias tables with lineage preserved.
  - Start here: `migrations/clickhouse/0003_ops_bronze.sql:48`; `https://clickhouse.com/docs/engines/table-engines/mergetree-family/replacingmergetree`; `docs/comprehensive_delivery_plan.md:544`
  - Verify: `go test ./... -run TestDocumentDedup` exits `0` for canonical URL, content-hash, and live-vs-archive collision fixtures.
  - QA: `Tool`: Bash | `Steps`: Run the dedup/entity-resolution test suites on fixtures with repeated URLs, source-native IDs, and strong aliases. | `Expected`: Exact duplicates collapse as designed, and strong identifier matches resolve to canonical entities with lineage preserved.

- [x] 18. Implement metric registry, contributions, state/snapshot MVs, and explainability payloads
  - What to do: Implement the analytics framework. Populate `meta.metric_registry`, emit per-record metric contributions, build `gold.metric_state` incremental MVs plus refreshable snapshot views, and expose explainability payloads containing evidence, feature contributions, and confidence. This task must implement the full core metric family: `obs_count`, `event_count`, `entity_count_approx`, `source_count_approx`, `confidence_weighted_activity`, `source_diversity_score`, `freshness_lag_minutes`, `geolocation_success_rate`, `dedup_rate`, `schema_drift_rate`, `evidence_density`, `cross_source_confirmation_rate`, `trend_24h`, `trend_7d`, `acceleration_7d_vs_30d`, `anomaly_zscore_30d`, `burst_score`, and `risk_composite_global`; later domain-pack tasks add their domain metrics on the same substrate.
  - Start here: `migrations/clickhouse/0002_core_tables.sql:1`; `https://clickhouse.com/docs/best-practices/use-materialized-views`; `https://clickhouse.com/docs/materialized-view/refreshable-materialized-view`
  - Verify: `go test ./... -run TestMetricContributions` exits `0`.
  - QA: `Tool`: Bash | `Steps`: Run the metric correctness suites; query `gold.metric_state` and `gold.metric_snapshot` after fixture promotion. | `Expected`: Declared core metrics exist with correct rollups and non-empty explainability payloads.

- [x] 19. Implement CI, fixtures, dashboards, review workflows, and runbooks
  - What to do: Build the quality and release harness. Add CI workflows for lint, unit, integration, migration, contract, and performance-smoke stages; add deterministic fixture bundles; add source freshness/geolocation/schema-drift dashboards; implement unresolved-location, low-confidence, and source-failure review workflows; write fresh-bootstrap, upgrade, backup/restore, kill-switch, and contract-break runbooks.
  - Start here: `README.md:11`; `cmd/api/main_test.go:9`; `docs/comprehensive_delivery_plan.md:282`
  - Verify: `.github/workflows/` contains runnable CI workflow files for lint, unit, integration, migration, contract, and performance-smoke stages.
  - QA: `Tool`: Bash | `Steps`: Run the full local CI command set exactly as defined in workflow files. | `Expected`: Lint, unit, migration, integration, contract, and performance-smoke commands all exit 0.

- [x] 20. Deliver geopolitical/general-web pack
  - What to do: Implement the first full domain pack on the completed substrate. Use `GDELT`, `ReliefWeb`, approved public feeds, and archive-friendly replay adapters; implement ACLED behind `user_supplied_key` mode and keep it disabled by default until credentials are provided. Normalize actors, events, locations, and cross-source links, then compute the full geopolitical metric family: `conflict_intensity_score`, `protest_activity_score`, `sanction_activity_score`, `humanitarian_pressure_score`, `cross_border_spillover_score`, `media_attention_score`, `media_attention_acceleration`, and `infrastructure_disruption_score`.
  - Start here: `seed/source_registry.json:3`; `https://www.gdeltproject.org/data.html`; `https://reliefweb.int/help/api`
  - Verify: `docker compose run --rm control-plane run-once --job ingest-geopolitical --source-id seed:gdelt` exits `0` on fixture or staged sample inputs.
  - QA: `Tool`: Bash | `Steps`: Run the geopolitical ingestion jobs for GDELT and ReliefWeb fixtures; query canonical events and geopolitical metrics. | `Expected`: Events, place links, and geopolitical metrics are materialized with explainability payloads.

- [x] 21. Expand API to metrics, analytics, entities, tracks, and search
  - What to do: Finish the public API surface using real views and query handlers. Implement `GET /v1/metrics`, `GET /v1/metrics/{metricId}`, `GET /v1/analytics/rollups`, `GET /v1/analytics/time-series`, `GET /v1/analytics/hotspots`, `GET /v1/analytics/cross-domain`, `GET /v1/entities`, `GET /v1/entities/{entityId}`, `GET /v1/entities/{entityId}/tracks`, `GET /v1/entities/{entityId}/events`, `GET /v1/entities/{entityId}/places`, `GET /v1/search`, `GET /v1/search/places`, and `GET /v1/search/entities` with stable cursor pagination, filters, sort semantics, and compatibility views.
  - Start here: `cmd/api/main.go:17`; `cmd/api/main.go:54`; `cmd/api/main_test.go:20`
  - Verify: `go test ./... -run TestAPIExpandedContracts` exits `0`.
  - QA: `Tool`: Bash | `Steps`: Start the full fixture stack and call all metrics, analytics, entities, tracks, and search endpoints. | `Expected`: Each endpoint returns a stable JSON envelope and non-stub payload backed by real views or tables.

- [x] 22. Deliver maritime pack
  - What to do: Implement the maritime domain pack using open port/registry metadata, OpenSanctions linkages, and a community/public AIS adapter path. If the live telemetry source needs credentials, implement it in `user_supplied_key` mode and keep the live adapter disabled by default while shipping replay fixtures for deterministic testing. Materialize vessel entities, track points/segments, port calls, AIS gap events, ownership/flag relations, and the full maritime metric family: `maritime_activity_score`, `ais_dark_hours_sum`, `ais_gap_frequency`, `identity_inconsistency_score`, `flag_ownership_mismatch_score`, `sanctions_exposure_score`, `port_loiter_score`, `rendezvous_probability`, `sts_transfer_suspicion_score`, `route_deviation_score`, `shadow_fleet_score`, and `maritime_risk_composite`.
  - Start here: `https://www.opensanctions.org/`; `https://unece.org/trade/cefact/unlocode-code-list-country-and-territory`; `docs/comprehensive_delivery_plan.md:560`
  - Verify: `docker compose run --rm control-plane run-once --job ingest-maritime --source-id fixture:maritime` exits `0`.
  - QA: `Tool`: Bash | `Steps`: Run the maritime ingestion job on replay fixtures; query vessel entities, track tables, and maritime metrics. | `Expected`: Vessel facts, port calls, AIS gaps, and maritime risk metrics are present with explainability payloads.

- [x] 23. Deliver aviation pack
  - What to do: Implement the aviation pack using OpenSky or equivalent public state-vector data, public aircraft registry data, airport metadata, and public NOTAM/weather context where available. If a live telemetry source requires credentials or tight rate limits, keep it in `user_supplied_key` mode and validate with replay fixtures. Materialize aircraft entities, track points, flight segments, transponder gap events, airport interaction events, and the full aviation metric family: `air_activity_score`, `transponder_gap_hours_sum`, `route_irregularity_score`, `military_likelihood_score`, `restricted_airspace_proximity_score`, `high_risk_airport_exposure_score`, `holding_pattern_anomaly_score`, and `air_risk_composite`.
  - Start here: `https://opensky-network.org/`; `https://www.faa.gov/licenses_certificates/aircraft_certification/aircraft_registry/releasable_aircraft_download`; `docs/comprehensive_delivery_plan.md:567`
  - Verify: `docker compose run --rm control-plane run-once --job ingest-aviation --source-id fixture:aviation` exits `0`.
  - QA: `Tool`: Bash | `Steps`: Run the aviation replay fixtures and query aircraft entities, track segments, and aviation metrics. | `Expected`: Flight activity, gaps, airport interactions, and aviation risk metrics are materialized with explainability payloads.

- [x] 24. Deliver space pack
  - What to do: Implement the space pack using public TLE/OMM feeds, public catalog history where available, transmitter metadata, and public conjunction/advisory sources that do not require private access. Compute orbit propagation, ground tracks, overpass windows, revisit metrics, place intersections, and the full space metric family: `satellite_activity_score`, `overpass_density_score`, `revisit_capability_score`, `conjunction_risk_score`, `maritime_observation_opportunity_score`, `critical_infrastructure_overpass_score`, and `space_risk_composite`.
  - Start here: `https://celestrak.org/`; `docs/comprehensive_delivery_plan.md:574`
  - Verify: `docker compose run --rm control-plane run-once --job ingest-space --source-id fixture:space` exits `0`.
  - QA: `Tool`: Bash | `Steps`: Run the space pack replay fixtures and query satellite entities, pass events, and space metrics. | `Expected`: Ground-track passes and place-linked space metrics are materialized with explainability payloads.

- [x] 25. Deliver safety/security pack
  - What to do: Implement the safety/security pack using OpenSanctions, public hazard feeds (for example NASA FIRMS and NOAA/coastal/weather feeds), emergency bulletins, and public vulnerability catalogs such as CISA KEV. Materialize sanctions/entity-graph relations, hazard observations, vulnerability observations, place/sector mappings, and the full safety/security metric family: `cyber_exposure_score`, `known_exploited_vuln_pressure`, `fire_hotspot_score`, `coastal_hazard_score`, `weather_disruption_score`, and `safety_security_composite`.
  - Start here: `https://www.opensanctions.org/`; `https://www.earthdata.nasa.gov/learn/find-data/near-real-time/firms`; `https://www.cisa.gov/known-exploited-vulnerabilities-catalog`
  - Verify: `docker compose run --rm control-plane run-once --job ingest-safety-security --source-id fixture:safety` exits `0`.
  - QA: `Tool`: Bash | `Steps`: Run the safety/security ingestion fixtures and query sanctions links, hazard observations, vulnerability observations, and metrics. | `Expected`: All declared record types and safety/security metrics are present with evidence payloads.

- [x] 26. Deliver scale-out/HA topology, distributed tables, cluster DR, and cost controls
  - What to do: Complete E18 after the single-node platform is stable. Add a documented shard/replica topology with ClickHouse Keeper, replicated local tables, distributed tables, cluster-aware backup/restore drills, and cost-control policies for TTL, projections, MV spend, and raw retention. Keep single-node mode as the default deployment and expose cluster mode as an explicit optional profile.
  - Start here: `docker-compose.yml:5`; `https://clickhouse.com/docs/guides/sre/keeper/clickhouse-keeper`; `https://clickhouse.com/docs/engines/table-engines/special/distributed`
  - Verify: A cluster-mode compose/profile or equivalent deployment definition brings up Keeper and replicated ClickHouse nodes successfully.
  - QA: `Tool`: Bash | `Steps`: Start the cluster profile; run replicated-table and distributed-table integration tests; query cluster health. | `Expected`: Keeper quorum is healthy, replicated tables accept writes, and distributed queries return the expected fixture data.

### Legacy Final Verification
- [x] F1. Plan Compliance Audit - oracle
- [x] F2. Code Quality Review - unspecified-high
- [x] F3. Real Runtime QA - unspecified-high (+ playwright if renderer/browser-only connector paths exist)
- [x] F4. Scope Fidelity Check - deep

## Success Criteria
- The consolidated plan remains the only file under `.sisyphus/plans/`.
- All `110` numbered tasks and all `40` legacy final-verification tasks are preserved with source provenance.
- Every numbered task includes enough context to start work without reopening a deleted source plan file.
- The superseded-plan manifest records every removed plan filename and original title.
