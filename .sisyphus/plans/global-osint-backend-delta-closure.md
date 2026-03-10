# Global OSINT Backend Delta Closure Plan

## TL;DR
> **Summary**: Close the detected manifest delta by freezing one canonical runtime contract, finishing the missing analytics surface, eliminating correctness drift in rerun/materialization behavior, and rebasing documentation only where the current architecture intentionally differs from the manifest.
> **Deliverables**:
> - one source-of-truth capability matrix covering shipped, partial, and roadmap claims
> - exact metric parity for in-scope core and domain metrics, including registry alignment and name normalization
> - one canonical metric materialization path with rerun-idempotent writes
> - corrected docs, tests, fixtures, and runbooks that describe the real runtime contract
> - explicit handling of renderer and advanced ClickHouse feature deltas without speculative scope creep
> **Effort**: Large
> **Parallel**: YES - 4 waves
> **Critical Path**: Task 1 -> Task 2 -> Task 3 -> Task 4 -> Task 5 -> Task 6 -> Task 7 -> Task 8 -> Task 9

## Context
### Original Request
Create a comprehensive plan of the detected delta between the provided manifest and the actual project implementation.

### Interview Summary
- Repo-grounded audit shows the project is strongest in services, schema footprint, and API route coverage, but materially under-complete in metrics, advanced ClickHouse ingestion features, and renderer behavior.
- The delta is not purely “missing code”; stale docs/tests also overstate runtime behavior and use contracts that the code does not expose.
- The plan therefore needs two closure modes: implement missing product/runtime capabilities where the manifest is the intended shipped contract, and re-baseline docs/tests where the repo intentionally uses a different architecture.

### Oracle Review (gaps addressed)
- Treat the manifest as a mixed contract/roadmap, not a command to implement every listed optimization feature immediately.
- Align code upward for analytics that are part of the current product surface; align docs/manifest downward for intentional stubs like the renderer and bootstrap-owned migration ledger.
- Keep single-node runtime canonical; do not let optional cluster features distort the delta plan.

### Metis Review (gaps addressed)
- Lock the canonical job-execution surface to `control-plane run-once --job ...`; do not add POST job orchestration in this delta.
- Make rerun idempotency an explicit deliverable because current direct writes into `MergeTree` tables can duplicate rows.
- Decide one owner for `meta.schema_migrations` and one metric materialization path.
- Require hotspots and cross-domain endpoints to be either truly populated or explicitly de-scoped from runtime closure.

## Work Objectives
### Core Objective
Close the implementation-vs-manifest delta without rewriting the platform: make shipped runtime capabilities match declared behavior where that behavior is product-visible, and make documentation/tests accurately describe everything else.

### Deliverables
- Canonical delta matrix for services, schema, endpoints, metrics, jobs, and ClickHouse features
- Exact runtime/docs contract for job execution, migration ledger ownership, renderer scope, and cluster feature scope
- Complete core metric family and the missing in-scope domain metric families with exact manifest IDs
- Rerun-safe contribution/state/snapshot/hotspot/cross-domain materialization
- Corrected docs, runbooks, fixtures, and E2E/contract tests that no longer assume nonexistent POST job endpoints or wrong schema fields

### Definition of Done (verifiable conditions with commands)
- `go run ./cmd/control-plane run-once --help` lists exactly the supported jobs the plan declares in scope.
- `curl -fsS "http://localhost:8123/?query=DESCRIBE%20TABLE%20meta.schema_migrations%20FORMAT%20TabSeparated"` matches the chosen bootstrap-owned ledger contract.
- `go test ./...` exits `0`.
- `go test ./test/e2e/... -tags=e2e` exits `0` against the chosen runtime contract.
- `curl -fsS http://localhost:8080/v1/metrics/<metric-id>` succeeds for every metric that the plan declares shipped.
- Rerunning each in-scope ingest job twice does not increase duplicate-sensitive row counts where idempotency is declared mandatory.

### Must Have
- One canonical truth table for `implemented`, `partial`, and `roadmap` claims.
- Exact manifest metric IDs for every metric declared shipped after this delta closes.
- One canonical runtime path from metric registry to contribution to state to snapshot.
- Explicit rerun-idempotency checks on `silver.fact_event`, `silver.metric_contribution`, `gold.metric_snapshot`, and any hotspot/cross-domain writers.
- Docs/tests aligned to GET-only API and `control-plane run-once` job orchestration.

### Must NOT Have
- No speculative browser-renderer build-out without a concrete render consumer.
- No new POST `/v1/jobs/...` API surface in this delta plan.
- No cluster-first redesign; optional cluster mode remains secondary.
- No silent metric renames or semantic changes without compatibility handling.
- No implementing advanced ClickHouse optimizations merely because they are named in the manifest.

## Verification Strategy
> ZERO HUMAN INTERVENTION - all verification is agent-executed.
- Test decision: tests-after with Go unit/integration tests, ClickHouse SQL assertions, and corrected E2E tests.
- QA policy: every task includes a contract validation step plus a negative-path or duplicate-path scenario.
- Evidence: `.sisyphus/evidence/task-{N}-{slug}.{ext}`

## Execution Strategy
### Parallel Execution Waves
Wave 1: contract truth, docs drift, and migration/job guardrails
- Task 1. Freeze the canonical delta matrix and source-of-truth contract
- Task 2. Align migration-ledger ownership and immutable migration policy
- Task 3. Align jobs, docs, fixtures, and E2E tests to the actual orchestration contract

Wave 2: analytics substrate and core metric closure
- Task 4. Standardize metric materialization and rerun idempotency
- Task 5. Complete core metric parity and exact metric registry coverage
- Task 6. Make hotspots and cross-domain runtime outputs real and testable

Wave 3: domain metric parity closure
- Task 7. Complete geopolitical, maritime, and aviation metric parity
- Task 8. Complete space and safety/security metric parity with exact manifest IDs

Wave 4: non-analytics delta closure
- Task 9. Re-baseline renderer and advanced ClickHouse feature claims with targeted implementation only where justified

### Dependency Matrix (full, all tasks)
- Task 1: blocked by none; blocks Tasks 2-9.
- Task 2: blocked by Task 1; blocks Tasks 3-6.
- Task 3: blocked by Tasks 1-2; blocks Tasks 5-9.
- Task 4: blocked by Tasks 1-2; blocks Tasks 5-8.
- Task 5: blocked by Tasks 3-4; blocks Tasks 6-8.
- Task 6: blocked by Tasks 2, 4-5; blocks Task 9 and final verification.
- Task 7: blocked by Tasks 4-5; blocks final verification.
- Task 8: blocked by Tasks 4-5; blocks final verification.
- Task 9: blocked by Tasks 1-3, 6; blocks final verification.

### Agent Dispatch Summary
- Wave 1 -> 3 tasks -> `deep`, `writing`
- Wave 2 -> 3 tasks -> `deep`, `unspecified-high`
- Wave 3 -> 2 tasks -> `deep`, `ultrabrain`
- Wave 4 -> 1 task -> `writing`

## TODOs
> Implementation + Test = ONE task. Never separate.
> EVERY task MUST have: Agent Profile + Parallelization + QA Scenarios.

- [ ] 1. Freeze the canonical delta matrix and source-of-truth contract

  **What to do**: Create one machine-readable and one human-readable capability matrix that classifies every manifest claim as `implemented`, `partial`, or `roadmap`. Lock these defaults: `control-plane run-once --job ...` is the canonical orchestration surface, `meta.schema_migrations` remains bootstrap-owned, single-node runtime is canonical, renderer stays stubbed unless a consumer appears, and advanced ClickHouse optimizations are not considered shipped unless runtime evidence exists.
  **Must NOT do**: Do not silently narrow the manifest without recording why each claim becomes `partial` or `roadmap`.

  **Recommended Agent Profile**:
  - Category: `writing` - Reason: this task formalizes the contract every later fix will target.
  - Skills: `[]` - Standard repo analysis is sufficient.
  - Omitted: [`playwright`] - No browser verification needed.

  **Parallelization**: Can Parallel: NO | Wave 1 | Blocks: [2, 3, 4, 5, 6, 7, 8, 9] | Blocked By: []

  **References**:
  - Pattern: `cmd/control-plane/main.go:51` - Canonical `run-once` job surface.
  - Pattern: `cmd/api/main.go:47` - GET-only job/resource surface already exposed by the API.
  - Pattern: `cmd/renderer/main.go:8` - Renderer is currently a health-only service.
  - Pattern: `internal/migrate/http_runner.go:21` - Bootstrap-owned migration ledger contract.
  - Pattern: `docs/comprehensive_delivery_plan.md:861` - Existing research-backed implementation notes to preserve where still valid.

  **Acceptance Criteria** (agent-executable only):
  - [ ] A capability matrix exists in repo docs mapping every detected delta claim to `implemented`, `partial`, or `roadmap`.
  - [ ] The matrix explicitly records the chosen defaults for job surface, migration-ledger ownership, renderer scope, cluster scope, and advanced ClickHouse feature scope.
  - [ ] `grep -R "run-once --job" docs README.md test testdata .github` shows the contract matrix and related docs agree on the canonical job surface.

  **QA Scenarios** (MANDATORY - task incomplete without these):
  ```
  Scenario: Contract matrix happy path
    Tool: Bash
    Steps: Read the new capability matrix and grep for job surface, renderer scope, and migration-ledger ownership across docs.
    Expected: Every searched artifact agrees with the matrix and uses the same chosen defaults.
    Evidence: .sisyphus/evidence/task-1-delta-matrix.txt

  Scenario: Drift detection edge case
    Tool: Bash
    Steps: Grep for known stale assumptions such as `POST /v1/jobs`, `schema_migrations.id`, and unsupported advanced ClickHouse features in shipped-status docs.
    Expected: Any remaining stale claims are found and queued by later tasks; none remain marked as shipped in the matrix.
    Evidence: .sisyphus/evidence/task-1-delta-drift.txt
  ```

  **Commit**: YES | Message: `docs(delta): freeze shipped versus roadmap contract` | Files: [`README.md`, `docs/*`, `.sisyphus/*`]

- [ ] 2. Align migration-ledger ownership and immutable migration policy

  **What to do**: Keep `meta.schema_migrations` bootstrap-owned, but make that ownership explicit everywhere. Remove stale assumptions about `id` columns or SQL-created ledger state, add immutable-migration guardrails so edited historical files cannot be re-applied unnoticed, and verify the exact ledger schema and checksum behavior in docs/tests.
  **Must NOT do**: Do not create a second owner for `meta.schema_migrations` in SQL migrations.

  **Recommended Agent Profile**:
  - Category: `deep` - Reason: touches bootstrap correctness, migration safety, and release process.
  - Skills: `[]` - Repo-local migration analysis only.
  - Omitted: [`playwright`] - Not relevant.

  **Parallelization**: Can Parallel: PARTIAL | Wave 1 | Blocks: [3, 4, 5, 6] | Blocked By: [1]

  **References**:
  - Pattern: `internal/migrate/http_runner.go:21` - Current authoritative ledger creation path.
  - Pattern: `docs/runbooks/upgrade-migration.md:28` - Stale `id`-based verification that must change.
  - Pattern: `cmd/bootstrap/main.go` - Bootstrap applies migrations and records checksums.
  - Pattern: `migrations/clickhouse/0001_init.sql:1` - SQL migrations do not own `meta.schema_migrations` today.

  **Acceptance Criteria** (agent-executable only):
  - [ ] `curl -fsS "http://localhost:8123/?query=DESCRIBE%20TABLE%20meta.schema_migrations%20FORMAT%20TabSeparated"` matches the documented bootstrap-owned ledger schema.
  - [ ] Migration docs and tests no longer reference nonexistent `id` columns or SQL ownership for the ledger table.
  - [ ] A migration immutability check exists and fails if an already-applied migration file is edited in place.

  **QA Scenarios** (MANDATORY - task incomplete without these):
  ```
  Scenario: Ledger contract happy path
    Tool: Bash
    Steps: Bootstrap the stack, describe `meta.schema_migrations`, and run the documented upgrade verification commands.
    Expected: The ledger schema matches docs exactly and verification uses real columns (`version`, `applied_at`, `checksum`, `success`, `notes`).
    Evidence: .sisyphus/evidence/task-2-ledger.txt

  Scenario: Edited historical migration edge case
    Tool: Bash
    Steps: Run the new immutability guard against a deliberately modified already-applied migration fixture.
    Expected: The guard fails loudly and prevents silent checksum drift from being treated as a normal upgrade.
    Evidence: .sisyphus/evidence/task-2-ledger-immutability.txt
  ```

  **Commit**: YES | Message: `fix(delta): lock migration ledger ownership and immutability` | Files: [`internal/migrate/*`, `cmd/bootstrap/*`, `docs/runbooks/upgrade-migration.md`, `testdata/*`]

- [ ] 3. Align jobs, docs, fixtures, and E2E tests to the actual orchestration contract

  **What to do**: Remove or rewrite stale assumptions that jobs are triggered by POST `/v1/jobs/...`. The runtime contract stays `control-plane run-once --job ...`; docs, E2E tests, fixtures, and runbooks must stop assuming HTTP job execution. Reconcile the actual supported job names with the docs and add the missing maritime and space `run-once` jobs so all shipped packs share one orchestration surface.
  **Must NOT do**: Do not add a new POST job API surface in this delta plan.

  **Recommended Agent Profile**:
  - Category: `deep` - Reason: closes contract drift across runtime, tests, and docs.
  - Skills: `[]` - Standard repo analysis only.
  - Omitted: [`playwright`] - Not needed.

  **Parallelization**: Can Parallel: PARTIAL | Wave 1 | Blocks: [5, 7, 8, 9] | Blocked By: [1, 2]

  **References**:
  - Pattern: `cmd/control-plane/main.go:68` - Real `run-once` CLI contract.
  - Pattern: `test/e2e/pipeline_test.go:50` - Current stale POST job assumptions.
  - Pattern: `cmd/control-plane/jobs_geopolitical.go`, `cmd/control-plane/jobs_aviation.go`, `cmd/control-plane/jobs_safety.go` - Existing implemented ingest-job pattern.
  - Pattern: `docs/comprehensive_delivery_plan.md:626` - Endpoint checklist that should not be used as evidence of POST job support.

  **Acceptance Criteria** (agent-executable only):
  - [ ] `go run ./cmd/control-plane run-once --help` lists exactly the jobs the updated docs/tests claim are supported.
  - [ ] `grep -R "/v1/jobs/" test docs README.md .github` returns no stale POST job execution assumptions.
  - [ ] The E2E suite uses the chosen orchestration surface and passes against it.

  **QA Scenarios** (MANDATORY - task incomplete without these):
  ```
  Scenario: Job contract happy path
    Tool: Bash
    Steps: Run `go run ./cmd/control-plane run-once --help`; execute one documented in-scope ingest job through `run-once`.
    Expected: Help output and docs agree exactly, and the documented ingest job succeeds through the CLI surface.
    Evidence: .sisyphus/evidence/task-3-job-contract.txt

  Scenario: Stale HTTP orchestration edge case
    Tool: Bash
    Steps: Grep docs/tests/fixtures for POST `/v1/jobs` usage and old query params such as `metric=` / `grain=` for rollups.
    Expected: No stale POST job calls remain; analytics tests use the real supported query contract.
    Evidence: .sisyphus/evidence/task-3-job-contract-drift.txt
  ```

  **Commit**: YES | Message: `docs(delta): align jobs fixtures and e2e contract` | Files: [`test/e2e/*`, `docs/*`, `README.md`, `.github/*`, `cmd/control-plane/*`]

- [ ] 4. Standardize metric materialization and rerun idempotency

  **What to do**: Pick one metric materialization path and enforce it repo-wide. Convert pack/runtime writers to a single pipeline from metric registry -> `silver.metric_contribution` -> `gold.metric_state` -> `gold.metric_snapshot`, then make reruns idempotent for duplicate-sensitive `MergeTree` tables by adding deterministic upsert strategy, latest-version semantics, or explicit dedup-on-write/read contract. Extend the same decision to `gold.hotspot_snapshot` and any cross-domain outputs.
  **Must NOT do**: Do not leave a mixed world where some packs write snapshots directly while helper MVs exist but are not authoritative.

  **Recommended Agent Profile**:
  - Category: `deep` - Reason: this is the main correctness guardrail for all metric and analytics closure work.
  - Skills: `[]` - Standard repo analysis only.
  - Omitted: [`playwright`] - Not relevant.

  **Parallelization**: Can Parallel: NO | Wave 2 | Blocks: [5, 6, 7, 8] | Blocked By: [1, 2]

  **References**:
  - Pattern: `internal/metrics/rollup.go:183` - Shared state/snapshot table and MV SQL helpers.
  - Pattern: `internal/metrics/rollup.go:227` - Refreshable snapshot view helpers currently exist as generated SQL.
  - Pattern: `internal/packs/geopolitical/geopolitical.go:222` - Pack currently emits direct SQL statements including snapshot writes.
  - Pattern: `internal/packs/safety/safety.go:867` - Direct contribution inserts into `silver.metric_contribution` already exist.
  - Pattern: `migrations/clickhouse/0005_baseline_tables.sql:347` - `silver.metric_contribution` is a plain `MergeTree` target and must be made rerun-safe.
  - Pattern: `migrations/clickhouse/0005_baseline_tables.sql:392` - `gold.metric_snapshot` is currently duplicate-sensitive on reruns.

  **Acceptance Criteria** (agent-executable only):
  - [ ] One documented materialization path exists and all metric-producing jobs use it consistently.
  - [ ] Rerunning the same ingest job twice keeps duplicate-sensitive counts stable in `silver.metric_contribution` and `gold.metric_snapshot`.
  - [ ] Hotspot and cross-domain writers either use the same canonical path or are explicitly documented as derived outputs built from the canonical path.

  **QA Scenarios** (MANDATORY - task incomplete without these):
  ```
  Scenario: Materialization happy path
    Tool: Bash
    Steps: Run one in-scope ingest job; inspect `silver.metric_contribution`, `gold.metric_state`, and `gold.metric_snapshot` for the emitted metric IDs.
    Expected: The chosen canonical path is observable end to end and no direct side-path contradicts it.
    Evidence: .sisyphus/evidence/task-4-materialization.txt

  Scenario: Duplicate-on-rerun edge case
    Tool: Bash
    Steps: Run the same ingest job twice; compare `count()` vs `uniqExact()` for contribution and snapshot identifiers.
    Expected: Duplicate-sensitive tables remain idempotent under rerun according to the chosen strategy.
    Evidence: .sisyphus/evidence/task-4-materialization-idempotency.txt
  ```

  **Commit**: YES | Message: `fix(delta): standardize metric materialization and reruns` | Files: [`internal/metrics/*`, `internal/packs/*`, `migrations/clickhouse/*`, `cmd/control-plane/*`]

- [ ] 5. Complete core metric parity and exact metric registry coverage

  **What to do**: Add the 10 missing core metrics, preserve exact manifest IDs, and make each metric formally registered and computed. This task covers `entity_count_approx`, `source_count_approx`, `confidence_weighted_activity`, `dedup_rate`, `schema_drift_rate`, `evidence_density`, `cross_source_confirmation_rate`, `trend_24h`, `acceleration_7d_vs_30d`, and `anomaly_zscore_30d`, while preserving the already-shipped core metrics.
  **Must NOT do**: Do not change existing metric semantics under the same ID without compatibility notes and validation fixtures.

  **Recommended Agent Profile**:
  - Category: `deep` - Reason: closes the biggest product-visible analytics gap.
  - Skills: `[]` - No external skill required.
  - Omitted: [`playwright`] - Not needed.

  **Parallelization**: Can Parallel: PARTIAL | Wave 2 | Blocks: [6, 7, 8] | Blocked By: [3, 4]

  **References**:
  - Pattern: `internal/metrics/registry.go:46` - Only 8 core metrics are currently registered.
  - Pattern: `internal/metrics/contribution.go:55` - Current contribution emitter proves where new core metrics must be added.
  - Pattern: `internal/metrics/rollup.go:258` - Finalizer logic must expand for new ratio/rolling metrics.
  - Pattern: `testdata/fixtures/quality/schema_drift.json:2` - Existing quality fixture is stale and should become a real validation source.

  **Acceptance Criteria** (agent-executable only):
  - [ ] `meta.metric_registry` contains all 18 requested core metric IDs exactly as named in the manifest.
  - [ ] Fixture runs produce non-empty contribution/state/snapshot records for all 18 core metric IDs.
  - [ ] `curl -fsS http://localhost:8080/v1/metrics/<metric-id>` succeeds for every core metric and returns the expected ID.

  **QA Scenarios** (MANDATORY - task incomplete without these):
  ```
  Scenario: Core metric registry happy path
    Tool: Bash
    Steps: Run the core metric fixture pipeline; query `meta.metric_registry` and `gold.metric_snapshot` for all requested core metric IDs.
    Expected: All 18 core metric IDs exist exactly and have runtime-backed outputs.
    Evidence: .sisyphus/evidence/task-5-core-metrics.txt

  Scenario: Metric naming and semantic drift edge case
    Tool: Bash
    Steps: Grep for legacy or partial core metric IDs and run validation fixtures for rolling and ratio metrics.
    Expected: No silent renames remain, and ratio/rolling metrics validate against the declared formulas and windows.
    Evidence: .sisyphus/evidence/task-5-core-metrics-drift.txt
  ```

  **Commit**: YES | Message: `feat(delta): complete core metric parity` | Files: [`internal/metrics/*`, `testdata/*`, `cmd/api/*`]

- [ ] 6. Make hotspots and cross-domain runtime outputs real and testable

  **What to do**: Either populate `gold.hotspot_snapshot` and `gold.api_v1_cross_domain` from real runtime data or explicitly reclassify them as non-shipped. Default for this plan: make them real. Define how hotspot ranking is computed from the completed metric surface, define cross-domain composition inputs, and ensure the API endpoints return populated runtime outputs rather than thin projections over empty tables.
  **Must NOT do**: Do not accept `200 OK` from these endpoints as evidence of completion when the underlying tables are empty.

  **Recommended Agent Profile**:
  - Category: `unspecified-high` - Reason: combines analytics semantics with API-serving closure.
  - Skills: `[]` - Standard repo tooling is sufficient.
  - Omitted: [`playwright`] - HTTP assertions are enough.

  **Parallelization**: Can Parallel: PARTIAL | Wave 2 | Blocks: [9] | Blocked By: [2, 4, 5]

  **References**:
  - Pattern: `migrations/clickhouse/0005_baseline_tables.sql:415` - `gold.hotspot_snapshot` exists but needs a real writer.
  - Pattern: `migrations/clickhouse/0007_api_expansion_views.sql:177` - `gold.api_v1_hotspots` is only a projection over `gold.hotspot_snapshot`.
  - Pattern: `migrations/clickhouse/0007_api_expansion_views.sql:243` - `gold.api_v1_cross_domain` exists but still needs true runtime semantics.
  - Pattern: `cmd/api/handlers_expanded.go:148` - API already points at hotspot and cross-domain views.

  **Acceptance Criteria** (agent-executable only):
  - [ ] `SELECT count() FROM gold.hotspot_snapshot` returns `> 0` after fixture ingests complete.
  - [ ] `curl -fsS http://localhost:8080/v1/analytics/hotspots` returns non-empty data backed by runtime snapshots.
  - [ ] `curl -fsS http://localhost:8080/v1/analytics/cross-domain` returns non-empty data backed by declared cross-domain composition rules.

  **QA Scenarios** (MANDATORY - task incomplete without these):
  ```
  Scenario: Hotspots happy path
    Tool: Bash
    Steps: Run in-scope ingest jobs after core/domain metric closure; query `gold.hotspot_snapshot` and call `/v1/analytics/hotspots`.
    Expected: Hotspot rows exist, API payload is non-empty, and ranking semantics match the documented formula.
    Evidence: .sisyphus/evidence/task-6-hotspots.txt

  Scenario: Thin-endpoint edge case
    Tool: Bash
    Steps: Assert that hotspot and cross-domain endpoints fail or clearly signal emptiness before population, then succeed after writers run.
    Expected: Endpoint success is tied to real runtime population, not just static view presence.
    Evidence: .sisyphus/evidence/task-6-hotspots-cross-domain.txt
  ```

  **Commit**: YES | Message: `feat(delta): close hotspot and cross-domain analytics` | Files: [`internal/metrics/*`, `cmd/api/*`, `migrations/clickhouse/*`, `cmd/control-plane/*`]

- [ ] 7. Complete geopolitical, maritime, and aviation metric parity

  **What to do**: Finish the missing manifest-aligned metric sets for geopolitical, maritime, and aviation. Geopolitical must add `sanction_activity_score`, `humanitarian_pressure_score`, `media_attention_acceleration`, and `infrastructure_disruption_score`. Maritime must expand from `ais_dark_hours` / `shadow_fleet_score` to the full requested family and normalize `ais_dark_hours_sum` exactly. Aviation must move `military_likelihood_score` and `route_irregularity_score` into the formal registry and add the six missing aviation metrics.
  **Must NOT do**: Do not leave near-match names in shipped code; use exact manifest IDs or explicit compatibility aliases.

  **Recommended Agent Profile**:
  - Category: `ultrabrain` - Reason: several domain metric families need semantic completion without breaking existing pack logic.
  - Skills: `[]` - Standard repo/domain analysis is sufficient.
  - Omitted: [`playwright`] - Not needed.

  **Parallelization**: Can Parallel: YES | Wave 3 | Blocks: [] | Blocked By: [4, 5]

  **References**:
  - Pattern: `internal/packs/geopolitical/geopolitical.go:204` - Geopolitical plan already emits registry, contributions, and snapshots.
  - Pattern: `internal/packs/geopolitical/geopolitical.go:267` - Existing adapters define the current source envelope.
  - Pattern: `internal/packs/maritime/metrics.go:39` - Maritime registry currently defines only two metrics.
  - Pattern: `internal/packs/maritime/metrics.go:119` - `ais_dark_hours` implementation shows the current naming drift.
  - Pattern: `internal/packs/aviation/types.go:13` - Only two aviation metric IDs are currently declared.
  - Pattern: `internal/packs/aviation/analyze.go:118` - Aviation currently computes only two metric snapshots.

  **Acceptance Criteria** (agent-executable only):
  - [ ] `meta.metric_registry` contains all requested geopolitical, maritime, and aviation metric IDs exactly as named in the manifest.
  - [ ] Fixture ingests for those three packs populate contributions and snapshots for the full requested metric sets.
  - [ ] No near-match metric IDs remain as the only shipped names for these packs.

  **QA Scenarios** (MANDATORY - task incomplete without these):
  ```
  Scenario: Geopolitical/maritime/aviation happy path
    Tool: Bash
    Steps: Run the three ingest jobs on fixture mode; query registry and snapshots for every requested metric ID in those packs.
    Expected: All requested IDs exist exactly and produce runtime-backed data.
    Evidence: .sisyphus/evidence/task-7-domain-metrics-a.txt

  Scenario: Naming drift edge case
    Tool: Bash
    Steps: Grep for legacy metric IDs like `ais_dark_hours`, then call `/v1/metrics/ais_dark_hours_sum`, `/v1/metrics/military_likelihood_score`, and other renamed/registered metrics.
    Expected: Manifest IDs are canonical, and any legacy names are handled only through explicit compatibility logic if retained at all.
    Evidence: .sisyphus/evidence/task-7-domain-metrics-a-drift.txt
  ```

  **Commit**: YES | Message: `feat(delta): complete geopolitical maritime and aviation metrics` | Files: [`internal/packs/geopolitical/*`, `internal/packs/maritime/*`, `internal/packs/aviation/*`, `internal/metrics/*`, `cmd/control-plane/*`]

- [ ] 8. Complete space and safety/security metric parity with exact manifest IDs

  **What to do**: Finish the exact requested metric families for space and safety/security. Rename or alias current space outputs from `overpass_density` / `conjunction_risk` to exact manifest IDs and add the missing five space metrics. Rename or alias current safety outputs from `fire_hotspot` / `sanctions_exposure` to exact manifest IDs and add the missing four safety metrics, including cyber and weather/coastal outputs.
  **Must NOT do**: Do not claim pack completion while exact manifest metric IDs are still absent.

  **Recommended Agent Profile**:
  - Category: `ultrabrain` - Reason: these two packs currently have the highest semantic and naming drift.
  - Skills: `[]` - No special skill required.
  - Omitted: [`playwright`] - Not needed.

  **Parallelization**: Can Parallel: YES | Wave 3 | Blocks: [] | Blocked By: [4, 5]

  **References**:
  - Pattern: `internal/packs/space/analysis.go:249` - Current space metrics are emitted under shortened IDs.
  - Pattern: `internal/packs/space/adapter.go:14` - Existing TLE/OMM parsing should remain the source envelope.
  - Pattern: `internal/packs/safety/safety.go:584` - Safety registry currently defines only two metrics under shortened IDs.
  - Pattern: `internal/packs/safety/safety.go:622` - Safety contributions currently emit only `sanctions_exposure` and `fire_hotspot`.

  **Acceptance Criteria** (agent-executable only):
  - [ ] `meta.metric_registry` contains all requested space and safety/security metric IDs exactly as named in the manifest.
  - [ ] Space and safety fixture ingests populate snapshot rows for every requested metric in those families.
  - [ ] API metric lookups succeed using manifest IDs for all space and safety/security metrics.

  **QA Scenarios** (MANDATORY - task incomplete without these):
  ```
  Scenario: Space/safety happy path
    Tool: Bash
    Steps: Run the space and safety ingest jobs on fixtures; query `meta.metric_registry` and `gold.metric_snapshot` for every requested metric ID.
    Expected: All requested IDs exist exactly and produce runtime-backed outputs.
    Evidence: .sisyphus/evidence/task-8-domain-metrics-b.txt

  Scenario: Exact-ID edge case
    Tool: Bash
    Steps: Call `/v1/metrics/overpass_density_score`, `/v1/metrics/conjunction_risk_score`, `/v1/metrics/fire_hotspot_score`, and `/v1/metrics/sanctions_exposure_score` after ingest.
    Expected: Exact manifest IDs resolve successfully; shortened legacy names are not the sole runtime contract.
    Evidence: .sisyphus/evidence/task-8-domain-metrics-b-drift.txt
  ```

  **Commit**: YES | Message: `feat(delta): complete space and safety metric parity` | Files: [`internal/packs/space/*`, `internal/packs/safety/*`, `internal/metrics/*`, `cmd/control-plane/*`]

- [ ] 9. Re-baseline renderer and advanced ClickHouse feature claims with targeted implementation only where justified

  **What to do**: Close the remaining non-analytics delta by distinguishing shipped features from roadmap features. Keep renderer health-only and update docs/manifests/tests accordingly. For advanced ClickHouse features, implement `s3()`-backed staged bulk-dump ingestion and worker-side async inserts because they directly support existing staged dataset and telemetry paths; explicitly reclassify `url()`, `file()`, `S3Queue`, projections, and data skipping indexes as deferred optimization claims for this release.
  **Must NOT do**: Do not mark optimization-tier ClickHouse features as shipped merely because they are listed in the manifest.

  **Recommended Agent Profile**:
  - Category: `writing` - Reason: this is primarily contract closure with small targeted implementation where justified by evidence.
  - Skills: `[]` - Standard repo analysis is sufficient.
  - Omitted: [`playwright`] - Browser work is explicitly not in scope unless the contract changes.

  **Parallelization**: Can Parallel: NO | Wave 4 | Blocks: [] | Blocked By: [1, 3, 6]

  **References**:
  - Pattern: `cmd/renderer/main.go:8` - Current renderer contract is only `/health`.
  - Pattern: `cmd/control-plane/jobs_place_build.go:92` - Dictionary plumbing shows which advanced ClickHouse features are actually exercised today.
  - Pattern: `internal/metrics/rollup.go:206` - MV SQL generation exists, but runtime adoption needs an explicit decision.
  - Pattern: `infra/clickhouse/cluster/README.md:16` - Cluster profile already documents some deferred optimization choices.

  **Acceptance Criteria** (agent-executable only):
  - [ ] All shipped docs/manifests clearly distinguish `implemented` vs `roadmap` for renderer and advanced ClickHouse features.
  - [ ] Shipped advanced ClickHouse features are limited to the chosen implemented set (`s3()` ingestion and async inserts) and have runtime callers plus verification.
  - [ ] `curl -fsS http://localhost:8090/health` remains the documented renderer contract unless a broader one is explicitly implemented and tested.

  **QA Scenarios** (MANDATORY - task incomplete without these):
  ```
  Scenario: Renderer/docs happy path
    Tool: Bash
    Steps: Call renderer `/health`; grep docs/manifests for renderer claims.
    Expected: Runtime behavior and docs agree exactly on the renderer contract.
    Evidence: .sisyphus/evidence/task-9-renderer-contract.txt

  Scenario: Advanced ClickHouse claim edge case
    Tool: Bash
    Steps: Grep code and docs for `url()`, `file()`, `s3()`, `S3Queue`, `async_inserts`, `projections`, and skip indexes; verify each claimed shipped feature has an implementation path or benchmark evidence.
    Expected: No optimization-tier ClickHouse feature remains mislabeled as shipped without evidence.
    Evidence: .sisyphus/evidence/task-9-clickhouse-features.txt
  ```

  **Commit**: YES | Message: `docs(delta): align renderer and clickhouse feature claims` | Files: [`README.md`, `docs/*`, `cmd/renderer/*`, `cmd/worker-fetch/*`, `migrations/clickhouse/*`]

## Final Verification Wave (4 parallel agents, ALL must APPROVE)
- [ ] F1. Plan Compliance Audit - oracle
- [ ] F2. Delta Coverage Review - unspecified-high
- [ ] F3. Runtime Contract QA - unspecified-high
- [ ] F4. Scope Fidelity Check - deep

## Commit Strategy
- One commit per task unless a task requires a follow-up compatibility fix generated by its own acceptance tests.
- Compatibility renames, docs alignment, and test alignment should commit after the runtime behavior they describe.
- No manifest-claim edits should merge before the runtime/docs truth table for that area is complete.

## Success Criteria
- The project can answer “is the manifest covered?” with an evidence-backed `yes` for all shipped claims.
- Every remaining mismatch is either implemented or explicitly reclassified as non-shipped roadmap behavior with docs/tests aligned.
- No shipped metric ID, analytics endpoint, or migration contract remains semantically ambiguous.
