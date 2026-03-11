# Silver Coverage Rollout

## TL;DR
> **Summary**: Make runtime orchestration registry-driven end to end, then guarantee every in-scope HTTP concrete source lands in a real `silver.*` destination and is provable through a per-source coverage contract.
> **Deliverables**:
> - registry-driven automatic frontier seeding for all in-scope sources
> - automatic promote execution after parse, not just stage markers
> - per-source silver coverage ledger and terminal silver routing proof
> - canonical-or-source-specific silver routing for every promote profile
> - repeatable verification queries and tests for bronze->silver coverage
> **Effort**: XL
> **Parallel**: YES - 3 waves
> **Critical Path**: 1 -> 2 -> 3 -> 5 -> 7 -> 8

## Context
### Original Request
Ensure all configured data sources move from bronze to at least one silver table or silver view, with multiple silver destinations allowed when the data model requires it.

### Interview Summary
- The source inventory from `sources.md` and `sources2.md` is already runtime-linked for concrete HTTP sources.
- Current runtime coverage is misleading: most sources are configured, but automatic sync only seeds a small hard-coded subset and does not execute real promote runs.
- Bronze-only or ops-only terminal states are not acceptable; every in-scope source must have a real `silver.*` destination.
- Shared canonical silver tables are preferred; source-specific silver tables/views are allowed only when canonical mapping would be materially lossy.

### Metis Review (gaps addressed)
- Freeze the denominator as all runtime-linked concrete HTTP sources with non-null `bronze_table`, including blocked credential-gated sources in coverage accounting.
- Do not treat `catalog_runnable`, `promote_profile`, stage markers, or `ops.unresolved_location_queue` as proof of silver coverage.
- Add an explicit per-source coverage artifact and terminal-routing proof rather than inferring success from metadata.
- Keep the rollout archetype/profile-driven; do not create bespoke jobs for every source unless a profile truly requires it.

## Work Objectives
### Core Objective
Guarantee that every in-scope runtime source either lands data in at least one `silver.*` table/view or is explicitly tracked in a non-success state that explains why silver landing has not happened yet.

### Deliverables
- `meta.source_silver_coverage` contract with executable coverage states per source
- automatic control-plane sync that enumerates all in-scope sources instead of hard-coded source lists
- automatic execution of the bronze->silver promote path
- profile-to-terminal-silver routing matrix covering canonical and non-canonical source shapes
- source-facing silver destinations (`silver` tables or views) for every profile
- regression tests and verification queries proving per-source silver coverage

### Definition of Done (verifiable conditions with commands)
- `curl -fsS "http://localhost:8123/?query=SELECT%20count()%20FROM%20meta.source_registry%20FINAL%20WHERE%20catalog_kind%3D'concrete'%20AND%20transport_type%3D'http'%20AND%20bronze_table%20IS%20NOT%20NULL%20FORMAT%20TabSeparated"` returns the fixed in-scope denominator.
- `curl -fsS "http://localhost:8123/?query=SELECT%20coverage_state%2C%20count()%20FROM%20meta.source_silver_coverage%20GROUP%20BY%20coverage_state%20ORDER%20BY%20coverage_state%20FORMAT%20JSONEachRow"` returns only documented states.
- `curl -fsS "http://localhost:8123/?query=SELECT%20count()%20FROM%20meta.source_silver_coverage%20WHERE%20coverage_state%20IN%20('silver_landed'%2C'silver_view_only')%20FORMAT%20TabSeparated"` equals the number of sources expected to be silver-covered in the seeded test environment.
- `curl -fsS "http://localhost:8123/?query=SELECT%20count()%20FROM%20meta.source_silver_coverage%20WHERE%20coverage_state%3D'unresolved_only'%20FORMAT%20TabSeparated"` reports only explicitly accepted unresolved-only fixtures, otherwise `0`.
- `curl -fsS "http://localhost:8123/?query=SELECT%20count()%20FROM%20ops.job_run%20WHERE%20job_name%3D'promote'%20AND%20status%3D'success'%20AND%20started_at%20%3E%20now()%20-%20INTERVAL%2015%20MINUTE%20FORMAT%20TabSeparated"` is non-zero after automatic sync runs.

### Must Have
- one frozen denominator query for in-scope sources
- one explicit silver coverage state machine
- registry-driven orchestration for fetch, parse, and promote
- silver terminal destinations traceable back to `source_id`
- idempotent repeat-run behavior for unchanged bronze rows

### Must NOT Have (guardrails, AI slop patterns, scope boundaries)
- no reliance on hard-coded source ID lists for automatic sync
- no counting `ops.*` rows as silver success
- no treating metadata presence as downstream coverage proof
- no per-source bespoke tables unless a profile-level lossiness review requires it
- no stale rollout counts copied from `.sisyphus/plans/sources-rollout.md`

## Verification Strategy
> ZERO HUMAN INTERVENTION - all verification is agent-executed.
- Test decision: tests-after using existing Go unit/integration coverage plus ClickHouse query verification
- QA policy: every task includes executable database or service-level scenarios
- Evidence: `.sisyphus/evidence/task-{N}-{slug}.{ext}`

## Execution Strategy
### Parallel Execution Waves
> Target: 5-8 tasks per wave. Shared contracts and schema changes happen first so later source-routing work can run in parallel.

Wave 1: coverage contract, denominator freeze, registry-driven automatic sync, automatic promote execution
Wave 2: canonical routing proof, non-canonical/source-specific silver destinations, coverage state materialization
Wave 3: verification hardening, idempotency, e2e/runtime audit

### Dependency Matrix (full, all tasks)
- 1 blocks 2, 3, 4, 5, 6, 7, 8
- 2 blocks 3 and 7
- 3 blocks 7 and 8
- 4 blocks 5, 6, 7, 8
- 5 and 6 can run in parallel after 4
- 7 blocks 8

### Agent Dispatch Summary (wave -> task count -> categories)
- Wave 1 -> 3 tasks -> `deep`, `unspecified-high`
- Wave 2 -> 3 tasks -> `deep`, `unspecified-high`, `writing`
- Wave 3 -> 2 tasks -> `unspecified-high`, `deep`

## TODOs

- [ ] 1. Freeze the denominator and silver coverage contract

  **What to do**: Add one authoritative in-scope source definition based on `meta.source_registry`, then introduce a coverage artifact contract named `meta.source_silver_coverage` keyed by `source_id` with required fields: `coverage_state`, `routing_mode`, `promote_profile`, `terminal_kind`, `terminal_destination`, `last_bronze_at`, `last_parse_at`, `last_promote_at`, `last_silver_at`, `reason`, `attrs`, and `updated_at`. States must include exactly `silver_landed`, `silver_view_only`, `blocked_missing_credential`, `parsed_no_promotable_rows`, `unresolved_only`, and `unsupported_profile`.
  **Must NOT do**: Do not infer coverage from `catalog_runnable`, `runtime_source_id`, `bronze_table`, or stage markers. Do not count `ops.*` outputs as terminal success.

  **Recommended Agent Profile**:
  - Category: `deep` - Reason: freezes the denominator and success contract for the entire rollout
  - Skills: `[]` - no special skill required
  - Omitted: [`git-master`] - no git work needed during implementation

  **Parallelization**: Can Parallel: NO | Wave 1 | Blocks: 2, 3, 4, 5, 6, 7, 8 | Blocked By: none

  **References** (executor has NO interview context - be exhaustive):
  - Pattern: `cmd/bootstrap/source_registry.go:413` - HTTP sources are required to declare `bronze_table` and `promote_profile`
  - Pattern: `internal/dashboardstats/service.go:136` - current stats only expose runnable/deferred metadata, not true silver coverage
  - Schema: `migrations/clickhouse/0005_baseline_tables.sql:165` - canonical silver tables already exist and must remain the preferred terminal model
  - Contract: `cmd/bootstrap/source_catalog.go:524` - runtime-linked catalog sources are now synthesized from catalog entries and must be reflected in the denominator

  **Acceptance Criteria** (agent-executable only):
  - [ ] `go test ./cmd/bootstrap/... ./internal/dashboardstats/...` passes with the new coverage artifact contract
  - [ ] `curl -fsS "http://localhost:8123/?query=SELECT%20count()%20FROM%20meta.source_silver_coverage%20FORMAT%20TabSeparated"` returns the same denominator as the in-scope source query in the plan header
  - [ ] `curl -fsS "http://localhost:8123/?query=SELECT%20count()%20FROM%20meta.source_silver_coverage%20WHERE%20coverage_state%20NOT%20IN%20('silver_landed'%2C'silver_view_only'%2C'blocked_missing_credential'%2C'parsed_no_promotable_rows'%2C'unresolved_only'%2C'unsupported_profile')%20FORMAT%20TabSeparated"` returns `0`

  **QA Scenarios** (MANDATORY - task incomplete without these):
  ```
  Scenario: Coverage contract exists
    Tool: Bash
    Steps: Run the denominator query from the plan, then query `meta.source_silver_coverage` count.
    Expected: Counts match exactly for the in-scope source set.
    Evidence: .sisyphus/evidence/task-1-coverage-contract.txt

  Scenario: Invalid state rejected
    Tool: Bash
    Steps: Run tests covering coverage-state validation and schema contract checks.
    Expected: Contract tests fail before implementation and pass after valid states are enforced.
    Evidence: .sisyphus/evidence/task-1-coverage-contract-error.txt
  ```

  **Commit**: YES | Message: `feat(control-plane): add per-source silver coverage contract` | Files: `migrations/clickhouse/*`, `cmd/bootstrap/*`, `internal/dashboardstats/*`

- [ ] 2. Replace hard-coded auto-sync source selection with registry-driven enumeration

  **What to do**: Remove the automatic reliance on the fixed `geopoliticalConcreteSources` and `safetyConcreteSources` lists for long-running sync. Build one registry-driven source enumerator that selects every in-scope HTTP concrete source, preserves bundle-alias handling only when explicitly requested, respects due-time logic, and seeds frontier for every eligible source.
  **Must NOT do**: Do not remove `run-once --source-id` behavior. Do not seed fingerprint or family catalog entries into frontier.

  **Recommended Agent Profile**:
  - Category: `deep` - Reason: changes the runtime orchestration denominator and automatic control-plane behavior
  - Skills: `[]` - no special skill required
  - Omitted: [`git-master`] - no git work needed during implementation

  **Parallelization**: Can Parallel: NO | Wave 1 | Blocks: 3, 7, 8 | Blocked By: 1

  **References** (executor has NO interview context - be exhaustive):
  - Pattern: `cmd/control-plane/jobs_http_sources.go:18` - current hard-coded source lists that must no longer drive automatic sync
  - Pattern: `cmd/control-plane/main.go:183` - automatic sync tick currently runs only family generation plus two ingest jobs
  - Pattern: `cmd/control-plane/jobs_http_sources.go:205` - `seedFrontier` is the real source of frontier rows
  - Pattern: `cmd/worker-fetch/main.go:695` - workers already enumerate eligible registry sources once frontier exists
  - Pattern: `cmd/worker-parse/main.go:574` - parse workers already enumerate eligible registry sources once bronze rows exist

  **Acceptance Criteria** (agent-executable only):
  - [ ] `go test ./cmd/control-plane/...` passes with automatic sync tests updated to the registry-driven behavior
  - [ ] `curl -fsS "http://localhost:8123/?query=SELECT%20countDistinct(source_id)%20FROM%20ops.crawl_frontier%20FORMAT%20TabSeparated"` converges to the expected in-scope source count in the seeded runtime test environment
  - [ ] `curl -fsS "http://localhost:8123/?query=SELECT%20count()%20FROM%20ops.crawl_frontier%20WHERE%20source_id%20NOT%20IN%20(SELECT%20source_id%20FROM%20meta.source_registry%20FINAL%20WHERE%20catalog_kind%3D'concrete'%20AND%20transport_type%3D'http'%20AND%20bronze_table%20IS%20NOT%20NULL)%20FORMAT%20TabSeparated"` returns `0`

  **QA Scenarios** (MANDATORY - task incomplete without these):
  ```
  Scenario: Registry-driven sync seeds all eligible sources
    Tool: Bash
    Steps: Start the stack, wait one control-plane tick, then query `ops.crawl_frontier` distinct `source_id` count.
    Expected: Frontier coverage matches the denominator for eligible HTTP concrete sources in the test dataset.
    Evidence: .sisyphus/evidence/task-2-registry-sync.txt

  Scenario: Fingerprints and families stay out of frontier
    Tool: Bash
    Steps: Query frontier rows joined to `meta.source_catalog` and inspect `catalog_kind`.
    Expected: No `fingerprint` or `family` rows are frontier-seeded.
    Evidence: .sisyphus/evidence/task-2-registry-sync-error.txt
  ```

  **Commit**: YES | Message: `feat(control-plane): seed frontier from registry` | Files: `cmd/control-plane/*`

- [ ] 3. Execute real automatic promote runs after bronze parsing

  **What to do**: Extend long-running orchestration so automatic sync executes the real promote job or shared promote function after due fetch/parse windows, rather than only recording a `promote` stage marker. Preserve idempotent bronze checkpoint semantics and keep the promote path registry-driven.
  **Must NOT do**: Do not leave `recordPipelineStage(..., "promote", ...)` as the only evidence of promotion. Do not create a second competing promote codepath.

  **Recommended Agent Profile**:
  - Category: `unspecified-high` - Reason: substantial orchestration change with existing logic reuse
  - Skills: `[]` - no special skill required
  - Omitted: [`git-master`] - no git work needed during implementation

  **Parallelization**: Can Parallel: NO | Wave 1 | Blocks: 7, 8 | Blocked By: 1, 2

  **References** (executor has NO interview context - be exhaustive):
  - Pattern: `cmd/control-plane/jobs_http_sources.go:93` - current promote stage marker that is insufficient
  - Entry: `cmd/control-plane/jobs_promote.go:35` - existing promote job entrypoint to reuse
  - Incremental logic: `cmd/control-plane/jobs_promote.go:213` - changed-source window selection and checkpoint semantics
  - Pipeline: `internal/promote/pipeline.go:186` - canonical plan preparation

  **Acceptance Criteria** (agent-executable only):
  - [ ] `go test ./cmd/control-plane/... ./internal/promote/...` passes
  - [ ] `curl -fsS "http://localhost:8123/?query=SELECT%20count()%20FROM%20ops.job_run%20WHERE%20job_name%3D'promote'%20AND%20status%3D'success'%20AND%20started_at%20%3E%20now()%20-%20INTERVAL%2015%20MINUTE%20FORMAT%20TabSeparated"` is non-zero after automatic sync
  - [ ] repeating the same automatic sync window without new bronze rows does not increase `silver.fact_event` + `silver.fact_observation` counts unexpectedly

  **QA Scenarios** (MANDATORY - task incomplete without these):
  ```
  Scenario: Automatic sync runs promote for real
    Tool: Bash
    Steps: Wait for a control-plane tick, then query `ops.job_run` for recent successful `promote` runs.
    Expected: At least one real promote job run exists; stage markers are no longer the only promote evidence.
    Evidence: .sisyphus/evidence/task-3-auto-promote.txt

  Scenario: Unchanged bronze rows are idempotent
    Tool: Bash
    Steps: Capture silver row counts, rerun automatic sync without injecting new bronze rows, capture counts again.
    Expected: Counts do not increase except for explicitly allowed bookkeeping rows.
    Evidence: .sisyphus/evidence/task-3-auto-promote-error.txt
  ```

  **Commit**: YES | Message: `fix(control-plane): execute real promote runs in automatic sync` | Files: `cmd/control-plane/*`, `internal/promote/*`

- [ ] 4. Define profile routing so every source has one terminal silver strategy

  **What to do**: Build an explicit routing matrix keyed by `promote_profile` and source shape. For `promote:geopolitical`, `promote:safety_security`, and `promote:catalog`, declare whether the terminal route is shared canonical silver tables, pack-specific silver writers, or a source-specific silver destination. For every in-scope source, persist exactly one routing mode into the coverage artifact.
  **Must NOT do**: Do not leave routing implicit in scattered conditionals. Do not permit a source to have multiple competing terminal routing modes.

  **Recommended Agent Profile**:
  - Category: `deep` - Reason: this is the core modeling decision that prevents bronze-only dead ends
  - Skills: `[]` - no special skill required
  - Omitted: [`git-master`] - no git work needed during implementation

  **Parallelization**: Can Parallel: NO | Wave 2 | Blocks: 5, 6, 7, 8 | Blocked By: 1

  **References** (executor has NO interview context - be exhaustive):
  - Pattern: `cmd/bootstrap/source_catalog.go:589` - synthesized runtime seeds currently default to `promote:catalog`
  - Pattern: `cmd/bootstrap/source_registry.go:417` - every HTTP source must declare a `promote_profile`
  - Pattern: `cmd/control-plane/jobs_promote.go:138` - generic promote currently scans bronze sources without profile-based routing enforcement
  - Canonical route: `internal/promote/pipeline.go:824` - shared silver inserts for entities, observations, and events
  - Existing pack routes: `internal/packs/geopolitical/geopolitical.go:891`, `internal/packs/safety/safety.go:902`, `cmd/control-plane/jobs_aviation.go:233`, `cmd/control-plane/jobs_maritime.go:371`, `cmd/control-plane/jobs_space.go:432`

  **Acceptance Criteria** (agent-executable only):
  - [ ] all in-scope sources in `meta.source_silver_coverage` have non-null `routing_mode`, `promote_profile`, and `terminal_destination`
  - [ ] `curl -fsS "http://localhost:8123/?query=SELECT%20count()%20FROM%20meta.source_silver_coverage%20WHERE%20routing_mode%20%3D%20''%20OR%20terminal_destination%20%3D%20''%20FORMAT%20TabSeparated"` returns `0`
  - [ ] tests fail if a new `promote_profile` is introduced without an explicit terminal routing rule

  **QA Scenarios** (MANDATORY - task incomplete without these):
  ```
  Scenario: Every source has one routing mode
    Tool: Bash
    Steps: Query `meta.source_silver_coverage` for null/empty routing fields.
    Expected: Zero rows with missing routing metadata.
    Evidence: .sisyphus/evidence/task-4-routing-matrix.txt

  Scenario: Unknown promote profile is rejected
    Tool: Bash
    Steps: Run tests covering profile routing validation.
    Expected: Unknown or unhandled profiles fail validation and cannot silently default to bronze-only behavior.
    Evidence: .sisyphus/evidence/task-4-routing-matrix-error.txt
  ```

  **Commit**: YES | Message: `feat(promote): add explicit source-to-silver routing matrix` | Files: `cmd/bootstrap/*`, `cmd/control-plane/*`, `internal/promote/*`

- [ ] 5. Complete the shared canonical silver route for promotable generic sources

  **What to do**: Ensure the generic promote pipeline can emit terminal proof for every source that can be represented as canonical entities, observations, events, or tracks. Add any missing source-preserving lineage needed so canonical silver outputs can be tied back to `source_id`, especially for entity-heavy flows where `silver.dim_entity` alone is insufficient for source-scoped coverage.
  **Must NOT do**: Do not redefine the canonical silver schema wholesale. Do not allow entity-only landings with no source-preserving proof surface.

  **Recommended Agent Profile**:
  - Category: `unspecified-high` - Reason: requires careful augmentation of the generic promote path without breaking existing packs
  - Skills: `[]` - no special skill required
  - Omitted: [`git-master`] - no git work needed during implementation

  **Parallelization**: Can Parallel: YES | Wave 2 | Blocks: 7, 8 | Blocked By: 1, 4

  **References** (executor has NO interview context - be exhaustive):
  - Pipeline: `internal/promote/pipeline.go:186` - plan preparation entrypoint
  - SQL: `internal/promote/pipeline.go:824` - entity/observation/event silver inserts
  - Schema: `migrations/clickhouse/0005_baseline_tables.sql:207` - `silver.fact_observation`
  - Schema: `migrations/clickhouse/0005_baseline_tables.sql:230` - `silver.fact_event`
  - Gap: `migrations/clickhouse/0005_baseline_tables.sql:165` - `silver.dim_entity` lacks `source_id`, so source-preserving lineage must be added elsewhere

  **Acceptance Criteria** (agent-executable only):
  - [ ] `go test ./internal/promote/... ./cmd/control-plane/...` passes with source-preserving lineage assertions
  - [ ] `curl -fsS "http://localhost:8123/?query=SELECT%20count()%20FROM%20meta.source_silver_coverage%20WHERE%20routing_mode%3D'canonical'%20AND%20coverage_state%20NOT%20IN%20('silver_landed'%2C'blocked_missing_credential'%2C'parsed_no_promotable_rows'%2C'unresolved_only')%20FORMAT%20TabSeparated"` returns `0`
  - [ ] at least one executable silver query exists that can list landed records by `source_id` for every canonical-routed source

  **QA Scenarios** (MANDATORY - task incomplete without these):
  ```
  Scenario: Canonical sources land with source-preserving proof
    Tool: Bash
    Steps: Run promote, then query the chosen silver proof surface for a sample canonical-routed source.
    Expected: Rows are returned with the correct `source_id` and terminal silver destination.
    Evidence: .sisyphus/evidence/task-5-canonical-route.txt

  Scenario: Entity-only source does not disappear into dim_entity
    Tool: Bash
    Steps: Promote an entity-heavy fixture and query coverage plus the silver proof surface.
    Expected: Coverage shows `silver_landed` or `silver_view_only`, not an unprovable implicit entity insert.
    Evidence: .sisyphus/evidence/task-5-canonical-route-error.txt
  ```

  **Commit**: YES | Message: `feat(promote): preserve per-source lineage for canonical silver landings` | Files: `internal/promote/*`, `migrations/clickhouse/*`

- [ ] 6. Add non-canonical source-specific silver destinations only where canonical mapping is lossy

  **What to do**: For sources or profiles that cannot be represented safely in the shared canonical silver model, add a source-specific or profile-specific `silver` table/view and register that destination in the routing matrix and coverage artifact. Prefer one profile-level destination over many per-source variants when the shape is shared.
  **Must NOT do**: Do not create one bespoke silver table per source unless the source truly has a unique shape. Do not create pass-through `silver` views that merely rename bronze columns without normalization.

  **Recommended Agent Profile**:
  - Category: `deep` - Reason: requires lossiness review and disciplined schema choices
  - Skills: `[]` - no special skill required
  - Omitted: [`git-master`] - no git work needed during implementation

  **Parallelization**: Can Parallel: YES | Wave 2 | Blocks: 7, 8 | Blocked By: 1, 4

  **References** (executor has NO interview context - be exhaustive):
  - Existing domain writers: `cmd/control-plane/jobs_aviation.go:233`, `cmd/control-plane/jobs_maritime.go:371`, `cmd/control-plane/jobs_space.go:432`
  - Existing safety/geopolitical patterns: `internal/packs/safety/safety.go:902`, `internal/packs/geopolitical/geopolitical.go:891`
  - Guardrail: `migrations/clickhouse/gold_api_views.sql:92` - downstream API reads shared silver facts, so source-specific silver must still be coherent with later consumers

  **Acceptance Criteria** (agent-executable only):
  - [ ] every source marked `routing_mode='source_specific'` or `routing_mode='profile_specific'` has a non-empty `terminal_destination` in `silver.*`
  - [ ] `go test ./cmd/control-plane/... ./internal/packs/...` passes for every affected non-canonical profile
  - [ ] no source remains in `unsupported_profile` unless explicitly listed in the plan summary as an unresolved blocker

  **QA Scenarios** (MANDATORY - task incomplete without these):
  ```
  Scenario: Non-canonical profile lands in dedicated silver destination
    Tool: Bash
    Steps: Promote a source routed to a source/profile-specific silver destination and query that destination by `source_id`.
    Expected: At least one landed row exists in `silver.*`, and coverage records the same destination.
    Evidence: .sisyphus/evidence/task-6-source-specific-silver.txt

  Scenario: Pass-through bronze mirroring is rejected
    Tool: Bash
    Steps: Run schema/tests that assert source-specific silver destinations include normalized fields and provenance, not only raw bronze payload copies.
    Expected: Tests fail for raw pass-through definitions and pass for normalized silver definitions.
    Evidence: .sisyphus/evidence/task-6-source-specific-silver-error.txt
  ```

  **Commit**: YES | Message: `feat(silver): add profile-specific destinations for non-canonical sources` | Files: `migrations/clickhouse/*`, `cmd/control-plane/*`, `internal/packs/*`

- [ ] 7. Materialize and maintain per-source silver coverage state during runtime

  **What to do**: Update runtime jobs so coverage state is refreshed from real pipeline outcomes: frontier, fetch, parse, promote, and terminal silver landing. Populate blocked/partial states for credentialed, empty, unresolved-only, and unsupported cases. Make the coverage artifact queryable from tests and operator tooling.
  **Must NOT do**: Do not backfill coverage states from static seed metadata alone. Do not leave stale coverage rows after routing changes.

  **Recommended Agent Profile**:
  - Category: `unspecified-high` - Reason: touches runtime accounting and state maintenance across multiple stages
  - Skills: `[]` - no special skill required
  - Omitted: [`git-master`] - no git work needed during implementation

  **Parallelization**: Can Parallel: NO | Wave 3 | Blocks: 8 | Blocked By: 1, 2, 3, 4, 5, 6

  **References** (executor has NO interview context - be exhaustive):
  - Runtime logs: `cmd/control-plane/jobs_http_sources.go:85` - current stage accounting hooks
  - Promote runs: `cmd/control-plane/jobs_promote.go:35` - real promote execution path
  - Stats limitation: `internal/dashboardstats/service.go:136` - existing stats are not enough; coverage artifact must become the operator truth

  **Acceptance Criteria** (agent-executable only):
  - [ ] `curl -fsS "http://localhost:8123/?query=SELECT%20count()%20FROM%20meta.source_silver_coverage%20WHERE%20last_promote_at%20IS%20NULL%20AND%20coverage_state%20IN%20('silver_landed'%2C'silver_view_only')%20FORMAT%20TabSeparated"` returns `0`
  - [ ] `curl -fsS "http://localhost:8123/?query=SELECT%20count()%20FROM%20meta.source_silver_coverage%20WHERE%20coverage_state%3D'blocked_missing_credential'%20AND%20reason%20%3D%20''%20FORMAT%20TabSeparated"` returns `0`
  - [ ] operator-facing queries can distinguish silver-covered sources from merely parsed or unresolved ones

  **QA Scenarios** (MANDATORY - task incomplete without these):
  ```
  Scenario: Coverage state reflects successful silver landing
    Tool: Bash
    Steps: Run the pipeline for a known good source, then query `meta.source_silver_coverage` for timestamps and state.
    Expected: `coverage_state` is `silver_landed` or `silver_view_only`, with non-null bronze/parse/promote/silver timestamps.
    Evidence: .sisyphus/evidence/task-7-coverage-state.txt

  Scenario: Missing credential source is tracked but not falsely covered
    Tool: Bash
    Steps: Leave a credentialed source unset, run automatic sync, then query its coverage row.
    Expected: State is `blocked_missing_credential`, reason is populated, and no silver-success state is recorded.
    Evidence: .sisyphus/evidence/task-7-coverage-state-error.txt
  ```

  **Commit**: YES | Message: `feat(runtime): maintain per-source silver coverage states` | Files: `cmd/control-plane/*`, `internal/dashboardstats/*`, `migrations/clickhouse/*`

- [ ] 8. Harden verification, repeat-run parity, and source-to-silver audits

  **What to do**: Add tests and runtime audits that prove end-to-end silver coverage, including denominator freeze, per-state counts, idempotent reruns, and direct silver row presence by `source_id`. Update stale tests and plan/docs that still assume the old 7-source runtime subset.
  **Must NOT do**: Do not use `/v1/internal/stats` alone as proof. Do not leave old `catalog_runnable=7` assumptions in tests or docs.

  **Recommended Agent Profile**:
  - Category: `writing` - Reason: combines test updates with operator-facing verification text and stale-count cleanup
  - Skills: `[]` - no special skill required
  - Omitted: [`git-master`] - no git work needed during implementation

  **Parallelization**: Can Parallel: NO | Wave 3 | Blocks: none | Blocked By: 1, 2, 3, 4, 5, 6, 7

  **References** (executor has NO interview context - be exhaustive):
  - Stale expectations: `internal/dashboardstats/service_test.go:21` - older runnable/deferred assumptions still exist in tests
  - E2E assertions: `test/e2e/pipeline_test.go:264` - current stats assertions need silver-coverage-aware verification
  - Old plan counts: `.sisyphus/plans/sources-rollout.md:33` - stale rollout numbers must not remain the operator reference

  **Acceptance Criteria** (agent-executable only):
  - [ ] `go test ./cmd/control-plane/... ./internal/promote/... ./internal/dashboardstats/... ./test/e2e/... -tags=e2e` passes in the intended environment
  - [ ] a single audit query can list every in-scope source with its routing mode, coverage state, and terminal silver destination
  - [ ] rerunning the full stack without new input keeps per-source silver counts stable within expected idempotent bounds

  **QA Scenarios** (MANDATORY - task incomplete without these):
  ```
  Scenario: Full audit lists every source and destination
    Tool: Bash
    Steps: Run the final audit query over `meta.source_silver_coverage` and save the result.
    Expected: Every in-scope source appears exactly once with a documented terminal silver destination or blocked state.
    Evidence: .sisyphus/evidence/task-8-final-audit.txt

  Scenario: Old 7-source assumptions are gone
    Tool: Bash
    Steps: Run the relevant test suites and search the repo for stale runnable/deferred assertions.
    Expected: Tests pass and no live assertions still expect the old fixed small subset.
    Evidence: .sisyphus/evidence/task-8-final-audit-error.txt
  ```

  **Commit**: YES | Message: `test(coverage): verify per-source bronze-to-silver landing` | Files: `test/e2e/*`, `internal/dashboardstats/*`, `.sisyphus/plans/*` if references need sync

## Final Verification Wave (4 parallel agents, ALL must APPROVE)
- [ ] F1. Plan Compliance Audit - oracle
- [ ] F2. Code Quality Review - unspecified-high
- [ ] F3. Real Manual QA - unspecified-high (+ playwright if UI)
- [ ] F4. Scope Fidelity Check - deep

## Commit Strategy
- Use one schema/contract commit, one orchestration/promote commit, and one coverage/verification commit unless implementation naturally collapses into fewer reviewable units.
- Commit messages should reflect why coverage proof and orchestration behavior changed, not just table additions.

## Success Criteria
- Every in-scope source has exactly one documented routing mode and one documented terminal `silver` destination.
- Automatic sync causes real fetch, parse, and promote execution for all due in-scope sources without hand-curated source lists.
- Coverage queries distinguish `silver_landed`, `silver_view_only`, and blocked/partial states without ambiguity.
- Re-running the pipeline on unchanged bronze data does not create duplicate silver rows.
