# Full Source Catalog Rollout and Incremental Runtime Sync

## TL;DR
> **Summary**: The repo now fully represents and governs the full `sources.md` universe in machine-readable form, but only a small approved concrete subset is runnable end to end today. Concrete sources are either runtime-linked with bronze/sync coverage or explicitly deferred with reasons; fingerprints and families are review-gated generators, not direct runnable sources.
> **Deliverables**:
> - Machine-readable source catalog covering all `240` entries from `sources.md`
> - Registry/compiler/generator flow for `206` concrete entries, `16` fingerprint entries, and `18` family templates
> - Static bronze-table coverage for the current `7` approved runtime-linked concrete sources
> - Automated sync loop, incremental parse, and scoped promote for the approved runtime-linked subset only
> - Env-var auth gating for all `16` credential-gated concrete entries
> - Operator-visible stats/docs plus representative E2E verification for mixed runnable/deferred/gated states
> **Effort**: Large
> **Parallel**: YES - 4 waves
> **Critical Path**: Remaining alignment work is `11 -> 12 -> 13`

## Context
### Original Request
Use the comprehensive source list in `sources.md` to create the bronze table, crawler mechanism, and automatic sync for each source. For API-key sources, create environment variables; when the key is absent, keep the source disabled.

### Interview Summary
- Scope explicitly includes everything in `sources.md`: concrete sources, platform fingerprints/standards, and recurring families.
- Credentialed sources must use env-var-driven gating and remain disabled until configured.
- The rollout must be automatic, not a manual run-once-only operator flow.
- Simplest valid interpretation for non-concrete entries: fingerprints and families are implemented as source generators/review templates, because they are not single executable endpoints.

### Metis Review (gaps addressed)
- The plan does not promise 240 bespoke one-off connectors; it uses a small set of integration archetypes and generator paths.
- Fingerprints and families are in scope, but not as direct `meta.source_registry` rows. They produce governed child sources through review gates.
- Existing static bronze DDL, frontier inserts, parse selection, and promote scans are not sufficient for mass onboarding; the plan adds compiler/generator, dedupe, parse checkpoints, and incremental promote scoping.
- Auth gating is frozen on `auth_config_json.env_var` and disabled-by-default governance for restricted/approval-required sources.

### Current Status Snapshot
- Current compiled taxonomy is `240 total = 206 concrete + 16 fingerprint + 18 family`.
- Current runtime-linked approved concrete subset is `7` sources with bronze/promote/sync coverage.
- Current public concrete coverage is mixed-state rather than fully runnable: public concrete rows are either runtime-linked or explicitly deferred.
- Current credential-gated concrete coverage is `16` rows with deterministic env-var contracts; only ACLED is runtime-linked today.
- Automatic sync, incremental parse, incremental promote, fingerprint generators, family generators, and operator stats are implemented for the current approved subset and governance surfaces, not for all concrete sources end to end.

## Work Objectives
### Core Objective
Turn the current curated-source pipeline into a scalable, registry-driven ingestion framework that can continuously synchronize the full `sources.md` landscape while preserving governance, RBAC, bronze immutability, and silver/gold contract stability.

### Deliverables
- A machine-readable source catalog that covers:
  - `206` concrete entries after machine classification
  - `16` platform fingerprint/standard entries from the discovery stack
  - 18 recurring national/subnational source families
- A compiler that emits reviewed runnable sources into `seed/source_registry.json` and generates static source bronze DDL from the catalog.
- New source-governance state for discovery candidates, family templates, and approved runnable child sources.
- Automated sync orchestration that repeatedly seeds/leases frontier work, fetches, parses, and promotes incrementally for the approved runtime-linked subset.
- Source eligibility rules:
  - public concrete sources are either runtime-linked and approved, or explicitly deferred with `deferred_reason`
  - credentialed/restricted sources compile with deterministic env-var contracts and stay disabled/deferred until env vars exist
  - approval-required/community/commercial sources remain review-gated until explicitly enabled
- Adapter archetypes for the source classes present in `sources.md`:
  - HTTP JSON/CSV/XML APIs
  - RSS/Atom/GeoRSS feeds
  - bulk files and archives
  - CKAN/Socrata/Opendatasoft/DCAT catalogs
  - ArcGIS/OGC/STAC geospatial services
  - fingerprint-driven discovery probes
- Tests proving registry compilation, bronze-table generation, frontier dedupe, incremental parse, incremental promote, discovery review gating, auth gating, and operator-visible mixed-state coverage.

### Definition of Done (verifiable conditions with commands)
- `go test ./cmd/bootstrap/... ./cmd/control-plane/... ./cmd/worker-fetch/... ./cmd/worker-parse/... ./internal/discovery/... ./internal/migrate/... ./internal/parser/... ./internal/promote/... -count=1` exits `0`.
- `go test ./test/e2e/... -tags=e2e -run 'TestSourceCatalogRollout|TestAutomaticSourceSync' -count=1` exits `0`.
- `curl -fsS "http://localhost:8123/?query=SELECT%20count()%20FROM%20meta.source_catalog%20FORMAT%20TabSeparated"` prints `240`.
- `curl -fsS "http://localhost:8080/v1/internal/stats" | jq '.data.summary | {catalog_total, catalog_concrete, catalog_fingerprint, catalog_family, catalog_runnable, catalog_deferred, catalog_credential_gated}'` prints `240`, `206`, `16`, `18`, `7`, `199`, and `16` respectively for the current rollout snapshot.
- `curl -fsS "http://localhost:8123/?query=SELECT%20count()%20FROM%20meta.source_registry%20WHERE%20catalog_kind='concrete'%20AND%20transport_type='http'%20AND%20bronze_table%20IS%20NOT%20NULL%20FORMAT%20TabSeparated"` prints `7` for the current approved runtime-linked subset.
- `curl -fsS "http://localhost:8123/?query=SELECT%20count()%20FROM%20ops.crawl_frontier%20GROUP%20BY%20source_id,canonical_url%20HAVING%20count()%20%3E%201%20FORMAT%20TabSeparated"` returns no rows after reseeding twice.
- `curl -fsS "http://localhost:8123/?query=SELECT%20count()%20FROM%20meta.source_registry%20WHERE%20enabled=1%20AND%20JSONExtractString(auth_config_json,'env_var')!=''%20AND%20disabled_reason%20IS%20NOT%20NULL%20FORMAT%20TabSeparated"` prints `0` only when required env vars are supplied; otherwise those sources remain disabled.

### Must Have
- `sources.md` becomes fully represented in machine-readable form.
- The current approved runtime-linked concrete subset receives source-specific bronze tables and automatic sync eligibility.
- Fingerprints and families are implemented as discovery/template mechanisms with review gates.
- Every credentialed source has a deterministic env-var contract.
- Missing credentials force a disabled/block state without failed fetch attempts.
- Frontier seeding is idempotent on `(source_id, canonical_url)`.
- Parse is incremental and does not repeatedly emit the same bronze rows from the same raw document.
- Promote is scoped to changed sources/time windows, not full-table rescans on every cycle.
- Every public concrete source is either runtime-linked or explicitly deferred with a reason.
- Existing public API/silver/gold contracts remain stable unless explicitly extended for new domains.

### Must NOT Have (guardrails, AI slop patterns, scope boundaries)
- No 240 bespoke hardcoded jobs.
- No runtime-created bronze DDL.
- No platform fingerprints or source families stored as ordinary runnable HTTP sources.
- No scheduler that bypasses `meta.source_registry` governance for runtime-linked sources or `meta.source_catalog` governance for deferred/generator classes.
- No enabled credentialed source without env-var-backed auth metadata.
- No websocket/login/browser-scrape/interactive transport forced through the current generic HTTP crawler in the current rollout.
- No unreviewed child source materialization from discovery probes/families directly into enabled state.
- No duplicate frontier rows or repeated parse/replay churn accepted as “good enough.”

## Verification Strategy
> ZERO HUMAN INTERVENTION — all verification is agent-executed.
- Test decision: tests-after with heavy package/integration coverage plus deterministic E2E.
- QA policy: Every task includes happy-path and failure-path scenarios.
- Evidence: `.sisyphus/evidence/task-{N}-{slug}.{ext}`

## Execution Strategy
### Parallel Execution Waves
> Target: 5-8 tasks per wave. <3 per wave (except final) = under-splitting.
> Extract shared dependencies as Wave-1 tasks for max parallelism.

Wave 1: governance, catalog, and compiler foundations (`1-4`)
Wave 2: incremental sync/runtime guarantees (`5-8`)
Wave 3: discovery generators and family templates (`9-10`)
Wave 4: concrete source onboarding, automation, and docs/tests (`11-13`)

### Dependency Matrix (full, all tasks)
- `1` blocks `2,3,4,5,6,7,8,9,10,11,12,13`
- `2` blocks `3,4,5,6,7,8,9,10,11,12,13`
- `3` blocks `4,5,9,10,11,12,13`
- `4` blocks `11,12,13`
- `5` blocks `6,7,8,11,12,13`
- `6` blocks `7,8,11,12,13`
- `7` blocks `8,11,12,13`
- `8` blocks `11,12,13`
- `9` blocks `10,11,12,13`
- `10` blocks `11,12,13`
- `11` blocks `12,13`
- `12` blocks `13`
- `13` blocks final verification only

### Agent Dispatch Summary
- Wave 1 -> 4 tasks -> `deep`, `ultrabrain`, `writing`
- Wave 2 -> 4 tasks -> `ultrabrain`, `deep`
- Wave 3 -> 2 tasks -> `deep`, `artistry`
- Wave 4 -> 3 tasks -> `deep`, `writing`, `unspecified-high`

## TODOs
> Implementation + Test = ONE task. Never separate.
> EVERY task MUST have: Agent Profile + Parallelization + QA Scenarios.

- [ ] 1. Freeze the source taxonomy, lifecycle states, and env-var auth contract

  **What to do**: Extend the source-governance model so every entry from `sources.md` is classified as exactly one of `catalog_kind in ('concrete','fingerprint','family')`. Freeze the lifecycle states to `draft`, `review_required`, `approved_disabled`, `approved_enabled`, and `blocked_missing_credential`. Freeze credential metadata to the existing `auth_config_json` env-ref shape: `{"env_var":"...","placement":"header|query|cookie","name":"...","prefix":"..."}`. Decision: concrete sources are the only entries allowed in runnable `meta.source_registry`; fingerprints and families live in catalog/template tables and only emit reviewed child sources.
  **Must NOT do**: Do not store platform standards/families as runnable HTTP sources; do not invent a second auth metadata format.

  **Recommended Agent Profile**:
  - Category: `deep` — Reason: this is the governing contract for the entire rollout.
  - Skills: [] — repo-native schema/governance work.
  - Omitted: [`playwright`] — no UI.

  **Parallelization**: Can Parallel: NO | Wave 1 | Blocks: `2-13` | Blocked By: none

  **References**:
  - Pattern: `seed/source_registry.json:1` — existing concrete source shape.
  - Pattern: `cmd/bootstrap/source_registry.go:388` — current normalization rules for HTTP vs `bundle_alias` sources.
  - Pattern: `migrations/clickhouse/0012_source_registry_http_contract.sql:1` — current HTTP source registry contract.
  - API/Type: `sources.md:10` — concrete source count and mixed taxonomy.
  - API/Type: `sources.md:367` — recurring families are explicitly not single endpoints.

  **Acceptance Criteria**:
  - [ ] `go test ./cmd/bootstrap/... -run 'TestSourceCatalogKinds|TestAuthConfigEnvContract' -count=1` exits `0`.
  - [ ] `curl -fsS "http://localhost:8123/?query=DESCRIBE%20TABLE%20meta.source_catalog%20FORMAT%20TabSeparated"` shows `catalog_kind` and lifecycle fields.
  - [ ] `curl -fsS "http://localhost:8123/?query=SELECT%20count()%20FROM%20meta.source_registry%20WHERE%20catalog_kind!='concrete'%20FORMAT%20TabSeparated"` prints `0`.

  **QA Scenarios**:
  ```bash
  Scenario: Mixed sources.md taxonomy compiles into the correct lifecycle model
    Tool: Bash
    Steps: go test ./cmd/bootstrap/... -run TestSourceCatalogKinds -count=1
    Expected: exit 0 and tests prove concrete/fingerprint/family separation plus valid lifecycle states
    Evidence: .sisyphus/evidence/task-1-taxonomy.txt

  Scenario: Credential metadata remains env-ref only
    Tool: Bash
    Steps: go test ./cmd/bootstrap/... -run TestAuthConfigEnvContract -count=1
    Expected: exit 0 and test rejects inline secrets or non-env auth metadata
    Evidence: .sisyphus/evidence/task-1-taxonomy-error.txt
  ```

  **Commit**: YES | Message: `feat(source-catalog): freeze taxonomy and auth contract` | Files: `migrations/clickhouse/*`, `cmd/bootstrap/source_registry.go`, `seed/*`, `cmd/bootstrap/*_test.go`

- [ ] 2. Create a machine-readable source catalog compiler from `sources.md`

  **What to do**: Introduce a canonical machine-readable catalog file set under `seed/` that represents every row in `sources.md`, including metadata for category, tags, archetype, credential requirements, and generator relationships. Build a compiler that transforms that catalog into:
  1) reviewed concrete runnable seed rows for `meta.source_registry`
  2) fingerprint probe definitions
  3) family template definitions
  4) generated bronze DDL manifests. The compiler must preserve a checksum pattern analogous to the current `seed_checksum` logic.
  **Must NOT do**: Do not parse Markdown tables at runtime in production; do not hand-maintain 240 separate ad hoc seed files once the catalog exists.

  **Recommended Agent Profile**:
  - Category: `ultrabrain` — Reason: this prevents the rollout from collapsing under manual duplication.
  - Skills: [] — repo-native compilation/generation logic.
  - Omitted: [`playwright`] — no UI.

  **Parallelization**: Can Parallel: NO | Wave 1 | Blocks: `3-13` | Blocked By: `1`

  **References**:
  - Pattern: `cmd/bootstrap/source_registry.go:293` — current seed checksum/idempotency model.
  - Pattern: `seed/source_registry.json:1` — current concrete seed output format.
  - API/Type: `sources.md:52` — category index and category counts.
  - API/Type: `sources.md:33` — fingerprint stack to model.

  **Acceptance Criteria**:
  - [ ] `go test ./cmd/bootstrap/... -run 'TestCompileSourceCatalog|TestCompiledSourceCounts' -count=1` exits `0`.
  - [ ] `curl -fsS "http://localhost:8123/?query=SELECT%20count()%20FROM%20meta.source_catalog%20FORMAT%20TabSeparated"` prints `240`.
  - [ ] compiled output includes exactly `222` concrete rows plus `18` family rows and the full fingerprint/standard set.

  **QA Scenarios**:
  ```bash
  Scenario: Catalog compiler covers every sources.md entry exactly once
    Tool: Bash
    Steps: go test ./cmd/bootstrap/... -run TestCompiledSourceCounts -count=1
    Expected: exit 0 and test asserts 222 concrete + 18 family + full fingerprint/standard coverage
    Evidence: .sisyphus/evidence/task-2-catalog-compiler.txt

  Scenario: Compiler detects duplicate or malformed source definitions
    Tool: Bash
    Steps: go test ./cmd/bootstrap/... -run TestCompileSourceCatalogRejectsDuplicateIDs -count=1
    Expected: exit 0 and malformed/duplicate entries are rejected deterministically
    Evidence: .sisyphus/evidence/task-2-catalog-compiler-error.txt
  ```

  **Commit**: YES | Message: `feat(source-catalog): compile machine-readable source catalog` | Files: `seed/*catalog*`, `cmd/bootstrap/*`, `cmd/bootstrap/*_test.go`

- [ ] 3. Extend governance tables for fingerprints, families, and reviewed child sources

  **What to do**: Add catalog/template tables under `meta` for fingerprints/families and child-source generation results. Recommended tables:
  - `meta.source_catalog`
  - `meta.source_family_template`
  - `meta.discovery_probe`
  - `meta.discovery_candidate`
  - `meta.source_generation_log`
  Child sources emitted from fingerprints/families must enter `review_required` first, then only move into `meta.source_registry` when approved.
  **Must NOT do**: Do not bypass review into enabled runnable state; do not overload `meta.source_registry` to store abstract templates.

  **Recommended Agent Profile**:
  - Category: `deep` — Reason: governance boundaries and review gates are core compliance controls.
  - Skills: []
  - Omitted: [`playwright`]

  **Parallelization**: Can Parallel: YES | Wave 1 | Blocks: `4-13` | Blocked By: `1,2`

  **References**:
  - Pattern: `migrations/clickhouse/0006_source_governance.sql:1` — existing governance extension style.
  - Pattern: `docs/runbooks/kill-switch.md:5` — preserve operational disable semantics.
  - Pattern: `cmd/bootstrap/source_registry.go:672` — latest-record merge/version pattern.

  **Acceptance Criteria**:
  - [ ] `go test ./cmd/bootstrap/... ./internal/migrate/... -run 'TestSourceGenerationGovernance|TestSchemaStandards' -count=1` exits `0`.
  - [ ] `curl -fsS "http://localhost:8123/?query=SHOW%20TABLES%20FROM%20meta%20LIKE%20'%25source%25'%20FORMAT%20TabSeparated"` lists the new catalog/template tables.
  - [ ] `curl -fsS "http://localhost:8123/?query=SELECT%20count()%20FROM%20meta.discovery_candidate%20WHERE%20review_status='approved'%20AND%20materialized_source_id='' %20FORMAT%20TabSeparated"` prints `0`.

  **QA Scenarios**:
  ```bash
  Scenario: Discovery/family outputs stay review-gated before registry materialization
    Tool: Bash
    Steps: go test ./cmd/bootstrap/... -run TestSourceGenerationGovernance -count=1
    Expected: exit 0 and tests prove review_required child sources are not inserted into runnable registry until approved
    Evidence: .sisyphus/evidence/task-3-governance.txt

  Scenario: Kill-switch semantics remain intact for generated child sources
    Tool: Bash
    Steps: go test ./cmd/bootstrap/... -run TestGeneratedSourceKillSwitch -count=1
    Expected: exit 0 and generated child sources preserve enabled/disabled governance rules
    Evidence: .sisyphus/evidence/task-3-governance-error.txt
  ```

  **Commit**: YES | Message: `feat(governance): add catalog templates and review gates` | Files: `migrations/clickhouse/*`, `cmd/bootstrap/*`, `cmd/bootstrap/*_test.go`, `internal/migrate/*_test.go`

- [ ] 4. Generate static bronze DDL for every runnable concrete source

  **What to do**: Keep the “one bronze table per concrete source” rule, but stop hand-maintaining it. Generate append-only static DDL from the source catalog into new migration files, following the existing bronze contract in `0015_source_bronze_tables.sql`. Fingerprints/families do not get bronze tables; their approved emitted child sources do. The generator must also update any curated observability enumerations that depend on bronze table names.
  **Must NOT do**: Do not create bronze tables at runtime; do not create bronze tables for abstract fingerprints/families.

  **Recommended Agent Profile**:
  - Category: `deep` — Reason: schema generation must remain static, append-only, and repo-governed.
  - Skills: []
  - Omitted: [`playwright`]

  **Parallelization**: Can Parallel: YES | Wave 1 | Blocks: `11,12,13` | Blocked By: `1,2,3`

  **References**:
  - Pattern: `migrations/clickhouse/0015_source_bronze_tables.sql:1` — current bronze table contract.
  - Pattern: `internal/migrate/schema_standards_test.go:164` — bronze migration testing style.
  - Pattern: `internal/dashboardstats/service.go:161` — current curated bronze-table observability list that will need generator-driven updates.

  **Acceptance Criteria**:
  - [ ] `go test ./internal/migrate/... -run 'TestSourceBronzeTablesMigrationDefinesAllStaticTables|TestSourceBronzeTables' -count=1` exits `0`.
  - [ ] `curl -fsS "http://localhost:8123/?query=SHOW%20TABLES%20FROM%20bronze%20LIKE%20'src_%25'%20FORMAT%20TabSeparated"` lists every approved concrete source bronze table.
  - [ ] No bronze table exists for any `catalog_kind in ('fingerprint','family')` row.

  **QA Scenarios**:
  ```bash
  Scenario: Bronze DDL is generated for all approved concrete runnable sources
    Tool: Bash
    Steps: go test ./internal/migrate/... -run TestSourceBronzeTables -count=1
    Expected: exit 0 and tests verify full concrete-source bronze coverage from the compiled catalog
    Evidence: .sisyphus/evidence/task-4-bronze-ddl.txt

  Scenario: Abstract source classes never receive runtime bronze tables
    Tool: Bash
    Steps: go test ./internal/migrate/... -run TestAbstractCatalogKindsDoNotGenerateBronzeTables -count=1
    Expected: exit 0 and tests reject bronze generation for fingerprints/families
    Evidence: .sisyphus/evidence/task-4-bronze-ddl-error.txt
  ```

  **Commit**: YES | Message: `feat(bronze): generate source bronze ddl from catalog` | Files: `migrations/clickhouse/*`, generator files, `internal/migrate/*_test.go`

- [ ] 5. Introduce adapter archetypes and parser compatibility matrix

  **What to do**: Define a small fixed adapter matrix for the source universe and map every concrete source to exactly one first-wave archetype: `http_json`, `http_csv`, `http_xml`, `rss_atom`, `html_profile`, `bulk_file`, `stac_api`, `catalog_ckan`, `catalog_socrata`, `catalog_opendatasoft`, `arcgis_rest`, `ogc_features`, `ogc_records`, `discovery_web`, or `deferred_transport`. Freeze parser compatibility against the existing registry (`json/csv/xml/rss/html-profile`) and require catalog rows to declare a compatible parser or deferred status.
  **Must NOT do**: Do not silently onboard sources that require websocket/browser/login transports into the generic HTTP worker; mark them `deferred_transport`.

  **Recommended Agent Profile**:
  - Category: `ultrabrain` — Reason: this is the scaling abstraction that keeps the rollout realistic.
  - Skills: []
  - Omitted: [`playwright`]

  **Parallelization**: Can Parallel: YES | Wave 2 | Blocks: `6,9,10,11,12,13` | Blocked By: `1,2,3`

  **References**:
  - Pattern: `internal/parser/registry.go:119` — current parser registry constraints.
  - Pattern: `internal/parser/json.go:13`, `csv.go:15`, `xml.go:69` — current parser IDs.
  - API/Type: `sources.md:33` — fingerprint/adaptor archetypes.
  - API/Type: `sources.md:169`, `186`, `210`, `246`, `271`, `298`, `319`, `341` — category-driven adapter diversity.

  **Acceptance Criteria**:
  - [ ] `go test ./cmd/bootstrap/... ./internal/parser/... -run 'TestCatalogArchetypeCoverage|TestArchetypeParserCompatibility' -count=1` exits `0`.
  - [ ] every concrete catalog row has `integration_archetype` and `parser_id|deferred_reason` populated.
  - [ ] all websocket/login/interactive sources are marked deferred, not runnable.

  **QA Scenarios**:
  ```bash
  Scenario: Every concrete source is classified into a supported archetype or explicit deferment
    Tool: Bash
    Steps: go test ./cmd/bootstrap/... -run TestCatalogArchetypeCoverage -count=1
    Expected: exit 0 and test proves 100% archetype assignment for concrete rows
    Evidence: .sisyphus/evidence/task-5-archetypes.txt

  Scenario: Unsupported transports are quarantined rather than forced through HTTP
    Tool: Bash
    Steps: go test ./cmd/bootstrap/... -run TestDeferredTransportClassification -count=1
    Expected: exit 0 and unsupported transports are marked `deferred_transport` with reasons
    Evidence: .sisyphus/evidence/task-5-archetypes-error.txt
  ```

  **Commit**: YES | Message: `feat(catalog): add integration archetype matrix` | Files: `seed/*catalog*`, `cmd/bootstrap/*`, `internal/parser/*_test.go`

- [ ] 6. Make frontier seeding idempotent and automatic

  **What to do**: Replace blind frontier inserts with canonical-url upsert/dedupe semantics and add an automated sync planner that periodically seeds eligible sources based on `refresh_strategy`, `crawl_strategy`, and source state. Keep using `ops.crawl_frontier` as the single queue. The automated loop must run inside existing service boundaries (control-plane or a new control-plane mode), not a parallel unmanaged scheduler.
  **Must NOT do**: Do not create a second queue; do not allow reruns to accumulate duplicate `(source_id, canonical_url)` entries.

  **Recommended Agent Profile**:
  - Category: `ultrabrain` — Reason: automatic sync semantics and queue idempotency are architecture-critical.
  - Skills: []
  - Omitted: [`playwright`]

  **Parallelization**: Can Parallel: NO | Wave 2 | Blocks: `7,8,11,12,13` | Blocked By: `1,2,5`

  **References**:
  - Pattern: `cmd/control-plane/jobs_http_sources.go:180` — current blind frontier insertion path.
  - Pattern: `internal/discovery/frontier.go:233` — normalization/dedupe logic to reuse.
  - Pattern: `migrations/clickhouse/0013_crawl_frontier_leases.sql:1` — current frontier lease contract.

  **Acceptance Criteria**:
  - [ ] `go test ./internal/discovery/... ./cmd/control-plane/... -run 'TestFrontierDedupe|TestAutomaticSyncPlanner' -count=1` exits `0`.
  - [ ] rerunning source generation/seeding twice yields zero duplicate frontier rows.
  - [ ] planner only seeds `approved_enabled` concrete sources whose refresh windows are due.

  **QA Scenarios**:
  ```bash
  Scenario: Automatic sync planner reseeds without duplicate frontier rows
    Tool: Bash
    Steps: go test ./cmd/control-plane/... -run TestAutomaticSyncPlanner -count=1
    Expected: exit 0 and test proves repeat planner runs do not duplicate `(source_id, canonical_url)` frontier entries
    Evidence: .sisyphus/evidence/task-6-frontier-sync.txt

  Scenario: Disabled or missing-credential sources are skipped by automatic sync
    Tool: Bash
    Steps: go test ./cmd/control-plane/... -run TestAutomaticSyncSkipsDisabledSources -count=1
    Expected: exit 0 and planner records skips without seeding/fetching blocked sources
    Evidence: .sisyphus/evidence/task-6-frontier-sync-error.txt
  ```

  **Commit**: YES | Message: `feat(sync): make frontier seeding automatic and idempotent` | Files: `cmd/control-plane/*`, `internal/discovery/*`, `cmd/control-plane/*_test.go`

- [ ] 7. Add a processed-document ledger for incremental parse

  **What to do**: Introduce a parse checkpoint/ledger so `worker-parse parse-source` only emits new bronze rows for raw documents that have not yet been successfully parsed for the relevant parser/version/source-record schema. The second parse run over the same raw inputs must emit zero new rows unless content or parser version changed.
  **Must NOT do**: Do not keep “latest raw docs only” selection as the only parse policy; do not re-emit duplicate bronze rows on unchanged raw docs.

  **Recommended Agent Profile**:
  - Category: `deep` — Reason: incremental parse correctness determines bronze growth and replay safety.
  - Skills: []
  - Omitted: [`playwright`]

  **Parallelization**: Can Parallel: YES | Wave 2 | Blocks: `8,11,12,13` | Blocked By: `5,6`

  **References**:
  - Pattern: `cmd/worker-parse/main.go:425` — current parse path lacks a processed-doc ledger.
  - Pattern: `migrations/clickhouse/0005_baseline_tables.sql:1` — `ops.parse_log` as current parse audit store.
  - Pattern: `migrations/clickhouse/0015_source_bronze_tables.sql:1` — target bronze uniqueness contract.

  **Acceptance Criteria**:
  - [ ] `go test ./cmd/worker-parse/... -run 'TestParseCheckpointPreventsDuplicateBronzeWrites|TestParserVersionBumpReprocessesRawDocs' -count=1` exits `0`.
  - [ ] running `parse-source` twice over the same unchanged raw docs produces `0` new bronze rows on the second run.
  - [ ] changing parser version or content hash causes exactly the affected raw docs to reprocess.

  **QA Scenarios**:
  ```bash
  Scenario: Parse checkpoint makes repeated parse runs no-op on unchanged input
    Tool: Bash
    Steps: go test ./cmd/worker-parse/... -run TestParseCheckpointPreventsDuplicateBronzeWrites -count=1
    Expected: exit 0 and second parse run inserts zero new bronze rows
    Evidence: .sisyphus/evidence/task-7-parse-checkpoint.txt

  Scenario: Parser/version changes correctly force targeted reparse
    Tool: Bash
    Steps: go test ./cmd/worker-parse/... -run TestParserVersionBumpReprocessesRawDocs -count=1
    Expected: exit 0 and only changed parser/content cases are reparsed
    Evidence: .sisyphus/evidence/task-7-parse-checkpoint-error.txt
  ```

  **Commit**: YES | Message: `feat(parse): add processed-document checkpoint ledger` | Files: `cmd/worker-parse/*`, `migrations/clickhouse/*`, `cmd/worker-parse/*_test.go`

- [ ] 8. Scope promote incrementally by source and change window

  **What to do**: Stop scanning every bronze table on each promote run. Add source/time-window scoped promote input selection driven by the parse/fetch ledgers so only changed sources/slices are promoted. Keep bronze-driven promote semantics and canonical IDs stable.
  **Must NOT do**: Do not rescan every enabled bronze table on every scheduled cycle; do not break current canonical replay stability.

  **Recommended Agent Profile**:
  - Category: `ultrabrain` — Reason: incremental promote across hundreds of sources is high-risk and cross-cutting.
  - Skills: []
  - Omitted: [`playwright`]

  **Parallelization**: Can Parallel: YES | Wave 2 | Blocks: `11,12,13` | Blocked By: `6,7`

  **References**:
  - Pattern: `cmd/control-plane/jobs_promote.go:131` — current bronze scan loading path.
  - Pattern: `internal/promote/pipeline.go:475` — canonical ID stability expectations.
  - Test: `internal/promote/pipeline_test.go:1` — replay-stability tests to preserve.

  **Acceptance Criteria**:
  - [ ] `go test ./cmd/control-plane/... ./internal/promote/... -run 'TestIncrementalPromoteSelection|TestReplayDoesNotDuplicateCanonicalRows' -count=1` exits `0`.
  - [ ] promote job only touches changed sources/windows when no new bronze rows exist.
  - [ ] canonical IDs remain stable across repeated sync cycles.

  **QA Scenarios**:
  ```bash
  Scenario: Promote scopes work to changed bronze slices only
    Tool: Bash
    Steps: go test ./cmd/control-plane/... -run TestIncrementalPromoteSelection -count=1
    Expected: exit 0 and test proves unchanged sources are skipped by promote selection
    Evidence: .sisyphus/evidence/task-8-promote-scope.txt

  Scenario: Incremental promote preserves canonical replay stability
    Tool: Bash
    Steps: go test ./internal/promote/... -run TestReplayDoesNotDuplicateCanonicalRows -count=1
    Expected: exit 0 and repeated promote cycles do not change canonical IDs or duplicate outputs
    Evidence: .sisyphus/evidence/task-8-promote-scope-error.txt
  ```

  **Commit**: YES | Message: `feat(promote): scope bronze promote incrementally` | Files: `cmd/control-plane/jobs_promote.go`, `internal/promote/*`, `cmd/control-plane/*_test.go`, `internal/promote/*_test.go`

- [ ] 9. Implement platform fingerprints as discovery generators

  **What to do**: Add discovery probes for the fingerprint stack in `sources.md`: CKAN, Socrata, ArcGIS Hub/REST, Opendatasoft, GeoNetwork, GeoNode, OGC APIs, STAC, sitemap/robots, RSS/Atom, and Wayback/CDX. These probes discover candidate child sources and write them into `meta.discovery_candidate` with classifier metadata, not directly into enabled runnable registry state.
  **Must NOT do**: Do not treat a fingerprint as a final ingested dataset; do not auto-enable generated child sources.

  **Recommended Agent Profile**:
  - Category: `artistry` — Reason: discovery-generation logic needs flexible but controlled classification behavior.
  - Skills: []
  - Omitted: [`playwright`]

  **Parallelization**: Can Parallel: YES | Wave 3 | Blocks: `10,11,12,13` | Blocked By: `1,2,5,6`

  **References**:
  - API/Type: `sources.md:33` — fingerprint probe inventory.
  - Pattern: `internal/discovery/frontier.go:51` — discovery/frontier normalization behavior.
  - Pattern: `cmd/worker-fetch/main.go:278` — generic fetch-source path to reuse for HTTP probes.

  **Acceptance Criteria**:
  - [ ] `go test ./internal/discovery/... ./cmd/control-plane/... -run 'TestFingerprintProbeGeneration|TestDiscoveryCandidatesStayReviewRequired' -count=1` exits `0`.
  - [ ] probe runs create discovery candidates with `integration_archetype`, `detected_platform`, and review status.
  - [ ] no fingerprint probe creates enabled runnable sources directly.

  **QA Scenarios**:
  ```bash
  Scenario: Fingerprint probes generate candidates with classifier metadata
    Tool: Bash
    Steps: go test ./internal/discovery/... -run TestFingerprintProbeGeneration -count=1
    Expected: exit 0 and tests prove CKAN/Socrata/ArcGIS/STAC/etc. candidates are emitted with deterministic metadata
    Evidence: .sisyphus/evidence/task-9-fingerprints.txt

  Scenario: Probe-generated candidates remain review-gated
    Tool: Bash
    Steps: go test ./cmd/control-plane/... -run TestDiscoveryCandidatesStayReviewRequired -count=1
    Expected: exit 0 and no generated candidate becomes enabled runnable source automatically
    Evidence: .sisyphus/evidence/task-9-fingerprints-error.txt
  ```

  **Commit**: YES | Message: `feat(discovery): add platform fingerprint generators` | Files: `internal/discovery/*`, `cmd/control-plane/*`, `migrations/clickhouse/*`, tests

- [ ] 10. Implement recurring family templates as child-source generators

  **What to do**: Add template-driven generation for the 18 recurring source families in `sources.md`. Each family template must encode expected transport/archetype, scope level, review defaults, and emitted child-source shape. Family runs generate reviewed candidates/child sources parameterized by geography/admin level, then feed the same approval pipeline as fingerprint discovery.
  **Must NOT do**: Do not materialize every possible country/municipal child source blindly into enabled runnable state; do not bypass review and governance.

  **Recommended Agent Profile**:
  - Category: `deep` — Reason: family templates are large-scope governance and generation logic.
  - Skills: []
  - Omitted: [`playwright`]

  **Parallelization**: Can Parallel: YES | Wave 3 | Blocks: `11,12,13` | Blocked By: `1,2,3,5,9`

  **References**:
  - API/Type: `sources.md:367` — recurring family list and semantics.
  - API/Type: `sources.md:392` — operational notes for long-tail national/subnational discovery.
  - Pattern: `seed/source_registry.json:156` — `bundle_alias` and source-alias semantics to generalize where appropriate.

  **Acceptance Criteria**:
  - [ ] `go test ./cmd/bootstrap/... ./cmd/control-plane/... -run 'TestFamilyTemplateGeneration|TestGeneratedChildSourcesRequireApproval' -count=1` exits `0`.
  - [ ] all 18 families exist in machine-readable form with template metadata.
  - [ ] generated child sources enter `review_required`, not `approved_enabled`.

  **QA Scenarios**:
  ```bash
  Scenario: Recurring families generate reviewable child source candidates deterministically
    Tool: Bash
    Steps: go test ./cmd/bootstrap/... -run TestFamilyTemplateGeneration -count=1
    Expected: exit 0 and tests prove family templates emit stable child-source definitions by geography/admin scope
    Evidence: .sisyphus/evidence/task-10-families.txt

  Scenario: Family-generated child sources are not auto-enabled
    Tool: Bash
    Steps: go test ./cmd/control-plane/... -run TestGeneratedChildSourcesRequireApproval -count=1
    Expected: exit 0 and generated family children remain review-gated until explicitly approved
    Evidence: .sisyphus/evidence/task-10-families-error.txt
  ```

  **Commit**: YES | Message: `feat(catalog): add recurring family source generators` | Files: `seed/*catalog*`, `cmd/bootstrap/*`, `cmd/control-plane/*`, tests

- [ ] 11. Keep public pull-based concrete source coverage explicit by archetype wave status

  **What to do**: Keep every public pull-based concrete source in `sources.md` explicitly classified as either currently runtime-linked or explicitly deferred with `deferred_reason`. Preserve the current approved runtime-linked subset (`7` sources) as structurally complete, and treat remaining public concrete onboarding as future runtime expansion rather than silently implying it is already complete. Group future expansion by archetype, not by category.
  **Must NOT do**: Do not leave concrete public pull-based sources unrepresented in the catalog; do not imply that deferred public concrete sources are already runnable end to end.

  **Recommended Agent Profile**:
  - Category: `deep` — Reason: this is the large data-model rollout itself.
  - Skills: []
  - Omitted: [`playwright`]

  **Parallelization**: Can Parallel: YES | Wave 4 | Blocks: `12,13` | Blocked By: `4,5,6,7,8,9,10`

  **References**:
  - API/Type: `sources.md:70`, `107`, `128`, `165`, `182`, `206`, `223`, `242`, `267`, `294`, `315`, `337`, `348`, `357` — concrete category source tables.
  - Pattern: `cmd/control-plane/jobs_http_sources.go:15` — current concrete-source orchestration pattern to generalize.
  - Pattern: `seed/source_registry.json:1` — approved concrete row examples.

  **Acceptance Criteria**:
  - [ ] `go test ./cmd/bootstrap/... -run 'TestConcreteSourceCoverage|TestApprovedRunnableSourceCoverage' -count=1` exits `0`.
  - [ ] every public pull-based concrete source in `sources.md` is either runtime-linked or explicitly deferred with reason.
  - [ ] the current approved runtime-linked subset (`7` sources) has `bronze_table`, `bronze_schema_version`, `promote_profile` (when silver/gold-bound), and sync metadata populated.

  **QA Scenarios**:
  ```bash
  Scenario: Public pull-based concrete sources are all onboarded or explicitly deferred
    Tool: Bash
    Steps: go test ./cmd/bootstrap/... -run TestConcreteSourceCoverage -count=1
    Expected: exit 0 and tests prove no concrete public pull-based source is unclassified, even when deferred
    Evidence: .sisyphus/evidence/task-11-concrete-rollout.txt

  Scenario: Runnable source definitions are structurally complete
    Tool: Bash
    Steps: go test ./cmd/bootstrap/... -run TestApprovedRunnableSourceCoverage -count=1
    Expected: exit 0 and tests prove the current approved runtime-linked sources have bronze/auth/promotion metadata filled correctly
    Evidence: .sisyphus/evidence/task-11-concrete-rollout-error.txt
  ```

  **Commit**: YES | Message: `feat(sources): onboard public pull-based concrete source catalog` | Files: `seed/*catalog*`, generated registry files, bronze DDL migrations, tests

- [ ] 12. Keep credential-gated concrete sources policy-complete and disabled by default

  **What to do**: For every concrete source in `sources.md` that is tagged as registration/approval/commercial/noncommercial/restricted or otherwise requires credentials, keep a deterministic env var name in `auth_config_json.env_var`. Preserve disabled/deferred-by-default behavior until env vars are present and review state is satisfied. Apply exact naming convention: `SOURCE_<UPPER_SNAKE_SLUG>_API_KEY` for key-based auth, `SOURCE_<UPPER_SNAKE_SLUG>_TOKEN` for bearer-like auth, unless the upstream API has an already-established env name in current repo conventions (for example `ACLED_API_KEY`).
  **Must NOT do**: Do not enable a credentialed source without an env var contract; do not put secrets into seed/catalog files.

  **Recommended Agent Profile**:
  - Category: `deep` — Reason: policy-sensitive onboarding and disabling rules must be exact.
  - Skills: []
  - Omitted: [`playwright`]

  **Parallelization**: Can Parallel: YES | Wave 4 | Blocks: `13` | Blocked By: `1,2,3,5,6,11`

  **References**:
  - Pattern: `seed/source_registry.json:107` — existing ACLED env-var auth example.
  - Pattern: `cmd/worker-fetch/main.go:939` — missing-credential handling path.
  - Pattern: `cmd/control-plane/jobs_http_sources.go:171` — current credential skip handling.
  - API/Type: `sources.md:26` — registration/commercial/noncommercial tag semantics.

  **Acceptance Criteria**:
  - [ ] `go test ./cmd/bootstrap/... ./cmd/worker-fetch/... ./cmd/control-plane/... -run 'TestCredentialedSourcesAreDisabledByDefault|TestMissingCredentialBlocksFetch' -count=1` exits `0`.
  - [ ] every credential-gated concrete source has non-empty `auth_config_json.env_var`.
  - [ ] missing credentials produce block/disabled states without outbound requests.

  **QA Scenarios**:
  ```bash
  Scenario: Credentialed sources remain disabled until env vars are configured
    Tool: Bash
    Steps: go test ./cmd/bootstrap/... -run TestCredentialedSourcesAreDisabledByDefault -count=1
    Expected: exit 0 and tests prove all credential-gated sources compile into disabled/blocking or deferred states by default
    Evidence: .sisyphus/evidence/task-12-auth-gating.txt

  Scenario: Missing credentials block fetch with zero outbound requests
    Tool: Bash
    Steps: go test ./cmd/worker-fetch/... -run TestMissingCredentialBlocksFetch -count=1
    Expected: exit 0 and HTTP stubs observe zero requests for blocked sources
    Evidence: .sisyphus/evidence/task-12-auth-gating-error.txt
  ```

  **Commit**: YES | Message: `feat(auth): gate restricted sources behind env vars` | Files: `seed/*catalog*`, `cmd/bootstrap/*`, `cmd/worker-fetch/*`, tests

- [ ] 13. Add operator-visible automation, docs, and rollout verification for the full catalog and current runtime subset

  **What to do**: Update control-plane/operator flows, dashboard observability, docs, and E2E coverage so operators can verify rollout status across the full catalog and the current runtime-linked subset. Add tests such as `TestSourceCatalogRollout` and `TestAutomaticSourceSync`, extend stats/dashboard visibility for source-catalog coverage and deferred counts, and document exactly how approved/deferred/credential-gated sources behave.
  **Must NOT do**: Do not document fingerprints/families as if they were direct runnable sources; do not claim websocket/login/interactive transports are already automated if they are deferred.

  **Recommended Agent Profile**:
  - Category: `writing` — Reason: this is operator documentation and broad verification integration.
  - Skills: []
  - Omitted: [`playwright`] — unless UI verification is added to an updated dashboard task.

  **Parallelization**: Can Parallel: YES | Wave 4 | Blocks: none | Blocked By: `6,7,8,9,10,11,12`

  **References**:
  - Pattern: `test/e2e/pipeline_test.go:1` — existing E2E structure.
  - Pattern: `README.md:225` — operational dashboard/runbook framing.
  - Pattern: `docs/runbooks/kill-switch.md:1` — governance/operator semantics.
  - Pattern: `internal/dashboardstats/service.go:1` — current source observability surface to extend.

  **Acceptance Criteria**:
  - [ ] `go test ./test/e2e/... -tags=e2e -run 'TestSourceCatalogRollout|TestAutomaticSourceSync' -count=1` exits `0`.
  - [ ] operator docs explain concrete vs fingerprint vs family behavior, auth gating, and deferred transports, including that deferred websocket/login/interactive transports are not automated yet.
  - [ ] dashboard/operator surfaces show runnable/deferred/gated counts from the compiled catalog.

  **QA Scenarios**:
  ```bash
  Scenario: Full catalog rollout remains operator-verifiable end-to-end
    Tool: Bash
    Steps: go test ./test/e2e/... -tags=e2e -run TestSourceCatalogRollout -count=1
    Expected: exit 0 and tests verify catalog counts, runnable/deferred/gated relationships, and operator-visible sync contracts for the current runtime-linked subset
    Evidence: .sisyphus/evidence/task-13-rollout-e2e.txt

  Scenario: Automatic sync loop respects deferred and gated source classes
    Tool: Bash
    Steps: go test ./test/e2e/... -tags=e2e -run TestAutomaticSourceSync -count=1
    Expected: exit 0 and tests prove the automatic sync loop handles approved, deferred, and gated sources according to policy for the current runtime-linked subset
    Evidence: .sisyphus/evidence/task-13-rollout-e2e-error.txt
  ```

  **Commit**: YES | Message: `docs(sync): document full source rollout and automation` | Files: `README.md`, `docs/*`, `test/e2e/*`, observability/tests

## Final Verification Wave (4 parallel agents, ALL must APPROVE)
- [ ] F1. Plan Compliance Audit — oracle
- [ ] F2. Code Quality Review — unspecified-high
- [ ] F3. Agent-Executed API/UI QA — unspecified-high (+ playwright if UI surfaces change)
- [ ] F4. Scope Fidelity Check — deep

## Commit Strategy
- Commit by architectural slice, not by category count:
  - `feat(catalog): add source catalog compiler and governance model`
  - `feat(sync): add incremental source sync runtime guarantees`
  - `feat(discovery): add fingerprints and family generators`
  - `feat(sources): onboard concrete and gated source inventory`
  - `test(e2e): verify catalog rollout and automatic sync`

## Success Criteria
- Every row in `sources.md` is represented in machine-readable form and correctly classified.
- Every concrete source is either runtime-linked with current automation coverage or explicitly deferred with a reason.
- Every fingerprint/family entry is implemented as a governed generator/template path.
- Credentialed sources are safely disabled until env vars exist.
- The automatic sync loop is idempotent end-to-end for the current approved runtime-linked subset: no duplicate frontier rows, no repeated unchanged parse writes, no full rescans on every promote.
- Operators can measure rollout completeness and source eligibility without manual source-by-source inspection.
