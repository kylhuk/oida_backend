# HTTP Source Adapter and Source-Specific Bronze ELT

## TL;DR
> **Summary**: Replace fixture-backed HTTP ingestion with a production path that uses `worker-fetch` as the only generic HTTP transport, persists immutable fetch artifacts in `bronze.raw_document` + MinIO, lands parsed rows into one typed bronze table per concrete source, and promotes bronze to canonical silver with deterministic ClickHouse ELT.
> **Deliverables**:
> - Generic HTTP crawl/fetch/parse/promote runtime for all currently configured HTTP-capable sources
> - Source registry/frontier/fetch/parse schema upgrades plus per-source bronze tables
> - Bronze-to-silver promotion jobs that preserve existing silver/gold/API contracts
> - Full unit, ClickHouse integration, and docker-compose E2E coverage without public-internet dependencies
> **Effort**: XL
> **Parallel**: YES - 3 waves
> **Critical Path**: 1 -> 2 -> 6 -> 7 -> 8 -> 9 -> 10 -> 11/12 -> 13 -> 14

## Context
### Original Request
Replace fixture-backed source ingestion with a generic HTTP adapter that can crawl requested resources, create source-specific bronze tables for each source, and perform bronze-to-silver transformation inside ClickHouse ELT.

### Interview Summary
- Rollout scope is full migration for all currently configured HTTP-capable sources now.
- Credential-gated sources remain gated until env vars are present; they must not break domain jobs while gated.
- `bronze.raw_document` remains the universal immutable fetch ledger keyed to MinIO raw bodies.
- Every concrete source gets its own typed bronze landing table keyed by `raw_id` plus source row identity.
- ClickHouse, not Go, owns bronze-to-silver transformation into the canonical OIDA model.
- Existing public API, silver/gold contract, and metric semantics stay unchanged.

### Metis Review (gaps addressed)
- Frontier must become the single fetch work queue; no second queue is introduced.
- Typed bronze writes happen after parse, not in `worker-fetch`.
- Canonical business IDs must stop depending on `raw_id` or fetch time.
- `bronze.raw_structured_row` cannot remain a second co-equal primary bronze store; it becomes audit/debug-only for non-migrated flows.
- Acceptance criteria must use deterministic local HTTP fixtures, live ClickHouse assertions, and no manual inspection.

## Work Objectives
### Core Objective
Deliver a contract-preserving ingest pipeline of `source_registry -> crawl_frontier -> worker-fetch -> bronze.raw_document + bronze.src_<slug>_v1 -> worker-parse -> ClickHouse promote SQL -> silver -> existing gold/API`, covering every currently configured concrete HTTP source without public fixture-only bypasses.

### Deliverables
- Source registry contract extended for HTTP transport, crawl, bronze, and promote metadata.
- `ops.crawl_frontier` upgraded with explicit leasing, retries, validators, and result-state tracking.
- `bronze.raw_document` upgraded into an immutable fetch ledger with typed replay/filter columns and MinIO-backed raw retention.
- Static migration-defined bronze source tables for:
  - `bronze.src_seed_gdelt_v1`
  - `bronze.src_fixture_reliefweb_v1`
  - `bronze.src_fixture_acled_v1`
  - `bronze.src_fixture_opensanctions_v1`
  - `bronze.src_fixture_nasa_firms_v1`
  - `bronze.src_fixture_noaa_hazards_v1`
  - `bronze.src_fixture_kev_v1`
- Runtime parse/promotion path that reads from bronze instead of embedded fixtures.
- Updated domain jobs where `ingest-geopolitical` and `ingest-safety-security` orchestrate live-source pipeline stages; `fixture:safety` becomes orchestration-only alias and is not fetchable.
- Tests proving replay stability, fetch/parse/promote idempotency, and unchanged canonical API behavior.

### Definition of Done (verifiable conditions with commands)
- `docker compose up -d --build` exits successfully and all services are healthy.
- `go test ./internal/fetch ./cmd/worker-fetch ./cmd/worker-parse ./cmd/control-plane ./internal/promote ./internal/metrics ./internal/migrate -count=1` exits `0`.
- `go test ./test/e2e -tags=e2e -run TestHTTPSourcePipeline -count=1` exits `0`.
- `curl -fsS "http://localhost:8123/?query=SELECT%20count()%20FROM%20meta.source_registry%20WHERE%20source_id%20IN%20('seed:gdelt','fixture:reliefweb','fixture:acled','fixture:opensanctions','fixture:nasa-firms','fixture:noaa-hazards','fixture:kev')%20AND%20enabled=1%20FORMAT%20TabSeparated"` prints `7` when credentials are present or `6` when ACLED is gated.
- `curl -fsS "http://localhost:8123/?query=SELECT%20count()%20FROM%20bronze.raw_document%20WHERE%20JSONExtractString(fetch_metadata,'storage_class')='inline'%20FORMAT%20TabSeparated"` prints `0` for migrated HTTP sources.
- `curl -fsS "http://localhost:8123/?query=SELECT%20count()%20FROM%20gold.metric_snapshot%20FORMAT%20TabSeparated"` returns a value greater than `0` after migrated source jobs run.

### Must Have
- `worker-fetch` remains the only generic GET/HEAD transport.
- `worker-parse` writes typed bronze rows and `ops.parse_log`; it does not write silver/gold.
- Promote path uses deterministic `INSERT ... SELECT` from bronze to silver.
- All current concrete HTTP sources migrate off embedded fixture loaders.
- ACLED stays disabled until env-backed credentials are supplied, but the runtime supports future activation.
- Raw bytes are stored in MinIO/object-store only for migrated HTTP sources.
- Existing `gold.api_v1_*` views and route surfaces remain stable.

### Must NOT Have (guardrails, AI slop patterns, scope boundaries)
- No second fetch queue or scheduler separate from `ops.crawl_frontier`.
- No source-schema logic in `worker-fetch`.
- No silver/gold writes from `worker-fetch` or `worker-parse`.
- No runtime-created DDL for source bronze tables; all source tables are migration-defined.
- No non-HTTP transports, JS/browser scraping, or renderer expansion.
- No public-internet dependency in tests.
- No business IDs that depend on `raw_id`, fetch timestamp, or process runtime.
- No `FINAL`-dependent read-path design for normal operation.

## Verification Strategy
> ZERO HUMAN INTERVENTION — all verification is agent-executed.
- Test decision: tests-after using Go unit tests, ClickHouse integration assertions, and docker-compose E2E.
- QA policy: Every task includes executable happy-path and failure-path scenarios.
- Evidence: `.sisyphus/evidence/task-{N}-{slug}.{ext}`

## Execution Strategy
### Parallel Execution Waves
> Target: 5-8 tasks per wave. <3 per wave (except final) = under-splitting.
> Extract shared dependencies as Wave-1 tasks for max parallelism.

Wave 1: contract/schema/guardrail foundations (`1-5`)
Wave 2: runtime fetch/parse/bronze infrastructure (`6-10`)
Wave 3: source migrations, docs, parity, and end-to-end cutover (`11-14`)

### Dependency Matrix (full, all tasks)
- `1` blocks `2,3,6,9,11,12,13`
- `2` blocks `6,7,8,13,14`
- `3` blocks `7,8,13,14`
- `4` blocks `7,8,10,14`
- `5` blocks `10,11,12,14`
- `6` blocks `7,8,11,12`
- `7` blocks `8,11,12,14`
- `8` blocks `9,10,11,12,14`
- `9` blocks `10,11,12,14`
- `10` blocks `11,12,13,14`
- `11` blocks `13,14`
- `12` blocks `13,14`
- `13` blocks `14`
- `14` blocks final verification only

### Agent Dispatch Summary
- Wave 1 -> 5 tasks -> `deep`, `writing`, `ultrabrain`
- Wave 2 -> 5 tasks -> `deep`, `unspecified-high`, `ultrabrain`
- Wave 3 -> 4 tasks -> `deep`, `writing`, `unspecified-high`

## TODOs
> Implementation + Test = ONE task. Never separate.
> EVERY task MUST have: Agent Profile + Parallelization + QA Scenarios.

- [ ] 1. Freeze source registry contract for HTTP crawl, bronze routing, and source aliases

  **What to do**: Extend `meta.source_registry` and bootstrap seed handling so every concrete HTTP source declares: `transport_type`, `crawl_enabled`, `allowed_hosts`, `crawl_strategy`, `crawl_config_json`, `parse_config_json`, `bronze_table`, `bronze_schema_version`, and `promote_profile`. Freeze `auth_config_json` to the exact env-ref contract `{"env_var":"...","placement":"header|query|cookie","name":"...","prefix":"..."}`. Preserve existing `source_id` values for compatibility. Set `fixture:safety` to `transport_type='bundle_alias'`, `crawl_enabled=0`, no bronze table, and require domain orchestration to fan out to `fixture:opensanctions`, `fixture:nasa-firms`, `fixture:noaa-hazards`, and `fixture:kev`. Set concrete bronze targets exactly to `bronze.src_seed_gdelt_v1`, `bronze.src_fixture_reliefweb_v1`, `bronze.src_fixture_acled_v1`, `bronze.src_fixture_opensanctions_v1`, `bronze.src_fixture_nasa_firms_v1`, `bronze.src_fixture_noaa_hazards_v1`, and `bronze.src_fixture_kev_v1`. Keep `fixture:acled` disabled by default in this plan and do not require live activation in acceptance until its official auth flow is remapped into the frozen contract.
  **Must NOT do**: Do not rename existing `source_id` values; do not make `fixture:safety` fetchable; do not encode secrets inline in registry JSON.

  **Recommended Agent Profile**:
  - Category: `deep` — Reason: schema, seed, and orchestration metadata contract must be locked without ambiguity.
  - Skills: [] — repo-native Go/SQL contract work; no specialized skill pack needed.
  - Omitted: [`playwright`] — no browser interaction is involved.

  **Parallelization**: Can Parallel: NO | Wave 1 | Blocks: `2,3,6,9,11,12,13` | Blocked By: none

  **References** (executor has NO interview context — be exhaustive):
  - Pattern: `cmd/bootstrap/main.go:105` — current `sourceSeed` shape to extend without breaking bootstrap.
  - Pattern: `cmd/bootstrap/source_registry.go:324` — normalized source seed and insert SQL path.
  - API/Type: `seed/source_registry.json:1` — current source catalog and concrete `source_id` set.
  - API/Type: `migrations/clickhouse/0002_core_tables.sql` — original `meta.source_registry` table contract.
  - API/Type: `migrations/clickhouse/0006_source_governance.sql` — current source governance extension pattern.
  - Test: `cmd/bootstrap/source_registry_test.go` — source registry verification and seed SQL assertions.

  **Acceptance Criteria** (agent-executable only):
  - [ ] `go test ./cmd/bootstrap -run TestSourceRegistry -count=1` exits `0`.
  - [ ] `curl -fsS "http://localhost:8123/?query=DESCRIBE%20TABLE%20meta.source_registry%20FORMAT%20TabSeparated"` includes all new columns.
  - [ ] `curl -fsS "http://localhost:8123/?query=SELECT%20source_id,transport_type,crawl_enabled,bronze_table,promote_profile%20FROM%20meta.source_registry%20WHERE%20source_id%20IN%20('seed:gdelt','fixture:reliefweb','fixture:acled','fixture:safety','fixture:opensanctions','fixture:nasa-firms','fixture:noaa-hazards','fixture:kev')%20ORDER%20BY%20source_id%20FORMAT%20TabSeparated"` returns the exact configured routing table.

  **QA Scenarios** (MANDATORY — task incomplete without these):
  ```bash
  Scenario: Registry seed produces exact live-source routing contract
    Tool: Bash
    Steps: docker compose up -d --build && go test ./cmd/bootstrap -run TestSourceRegistry -count=1 && curl -fsS "http://localhost:8123/?query=SELECT%20source_id,transport_type,crawl_enabled,bronze_table%20FROM%20meta.source_registry%20ORDER%20BY%20source_id%20FORMAT%20TabSeparated"
    Expected: test exits 0; every concrete source has the exact bronze table name above; `fixture:safety` shows `bundle_alias` and `crawl_enabled=0`
    Evidence: .sisyphus/evidence/task-1-source-registry.txt

  Scenario: Secrets stay out of registry payloads
    Tool: Bash
    Steps: curl -fsS "http://localhost:8123/?query=SELECT%20auth_config_json%20FROM%20meta.source_registry%20WHERE%20source_id='fixture:acled'%20FORMAT%20TabSeparated"
    Expected: output contains env-ref metadata only and no literal secret value
    Evidence: .sisyphus/evidence/task-1-source-registry-error.txt
  ```

  **Commit**: YES | Message: `feat(source-registry): add http bronze routing contract` | Files: `migrations/clickhouse/*`, `cmd/bootstrap/main.go`, `cmd/bootstrap/source_registry.go`, `seed/source_registry.json`, `cmd/bootstrap/source_registry_test.go`

- [ ] 2. Extend `ops.crawl_frontier` into the single leased fetch queue

  **What to do**: Upgrade `ops.crawl_frontier` to support production fetch orchestration. Add exactly these new columns: `lease_owner Nullable(String)`, `lease_expires_at Nullable(DateTime64(3, 'UTC'))`, `attempt_count UInt16`, `last_attempt_at Nullable(DateTime64(3, 'UTC'))`, `last_fetch_id Nullable(String)`, `last_status_code Nullable(UInt16)`, `last_error_code Nullable(String)`, `last_error_message Nullable(String)`, `etag Nullable(String)`, `last_modified Nullable(String)`, and `discovery_kind LowCardinality(String)`. Freeze the fetch state machine to: `pending`, `leased`, `fetched`, `not_modified`, `retry`, `dead`, `blocked`. Map outcomes exactly as follows: `200/204 -> fetched`; `304 -> not_modified`; `404/410 -> dead`; `429/5xx/network timeout -> retry`; `disabled/missing-auth/unsupported-auth -> blocked`; `body-too-large -> dead`. Parse failures are not frontier states; they are tracked only in `ops.parse_log`.
  **Must NOT do**: Do not create a second queue; do not use frontier to track parser replay state; do not leave lease semantics implicit.

  **Recommended Agent Profile**:
  - Category: `ultrabrain` — Reason: queue state transitions, retries, and leases are architecture-critical and easy to get subtly wrong.
  - Skills: [] — repo-native schema/runtime reasoning is sufficient.
  - Omitted: [`playwright`] — no UI or browser work.

  **Parallelization**: Can Parallel: NO | Wave 1 | Blocks: `6,7,8,13,14` | Blocked By: `1`

  **References** (executor has NO interview context — be exhaustive):
  - Pattern: `migrations/clickhouse/0003_ops_bronze.sql:15` — current frontier schema to extend in place.
  - Pattern: `internal/discovery/frontier.go:51` — current `FrontierEntry` shape and normalization behavior.
  - API/Type: `internal/discovery/frontier.go:187` — frontier build path that already emits `DiscoveryKind`, priority, and schedule fields.
  - Test: `internal/discovery/discovery_test.go` — existing frontier/discovery assertions to extend.
  - Test: `internal/migrate/schema_standards_test.go` — schema invariant pattern for new queue columns.

  **Acceptance Criteria** (agent-executable only):
  - [ ] `go test ./internal/discovery ./internal/migrate -count=1` exits `0`.
  - [ ] `curl -fsS "http://localhost:8123/?query=DESCRIBE%20TABLE%20ops.crawl_frontier%20FORMAT%20TabSeparated"` lists the required lease/result columns.
  - [ ] A new frontier state-machine test proves the exact mapping for `200`, `304`, `404`, `429`, missing auth, and body-too-large.

  **QA Scenarios** (MANDATORY — task incomplete without these):
  ```bash
  Scenario: Frontier lease and state transitions are deterministic
    Tool: Bash
    Steps: go test ./internal/discovery -run TestFrontierStateMachine -count=1 && go test ./cmd/worker-fetch -run TestClaimFrontierLease -count=1
    Expected: both tests exit 0 and prove the exact state mapping plus single-worker lease ownership
    Evidence: .sisyphus/evidence/task-2-frontier.txt

  Scenario: Parse failures do not mutate fetch state machine
    Tool: Bash
    Steps: go test ./cmd/worker-parse -run TestParseFailureLeavesFrontierFetchStateUnchanged -count=1
    Expected: exit 0 and test asserts `ops.parse_log` records failure while `ops.crawl_frontier.state` stays `fetched` or `not_modified`
    Evidence: .sisyphus/evidence/task-2-frontier-error.txt
  ```

  **Commit**: YES | Message: `feat(frontier): add leased fetch queue semantics` | Files: `migrations/clickhouse/*`, `internal/discovery/frontier.go`, `internal/discovery/*_test.go`, `cmd/worker-fetch/*_test.go`

- [ ] 3. Preserve immutable raw fetch ledger and force object-store retention for migrated HTTP sources

  **What to do**: Keep `bronze.raw_document` as the immutable fetch ledger, but add typed fetch columns used by runtime filtering and replay: `fetch_id`, `final_url`, `etag`, `last_modified`, `not_modified`, and `storage_class`. Keep raw body bytes in MinIO/object-store only for migrated HTTP sources by setting their retention policy to object-store-backed behavior and updating retention code/tests accordingly. Also add fetch attempt visibility to `ops.fetch_log` with `attempt_count` and `retry_count`. `worker-fetch` must remain fetch-only and never write typed bronze source tables.
  **Must NOT do**: Do not remove `bronze.raw_document`; do not keep inline raw payloads for migrated HTTP sources; do not push typed source schema into `worker-fetch`.

  **Recommended Agent Profile**:
  - Category: `deep` — Reason: retention, replay, and fetch ledger lineage must stay immutable while changing runtime defaults.
  - Skills: [] — repo-native Go + ClickHouse work.
  - Omitted: [`playwright`] — not relevant.

  **Parallelization**: Can Parallel: YES | Wave 1 | Blocks: `7,8,13,14` | Blocked By: `1`

  **References** (executor has NO interview context — be exhaustive):
  - Pattern: `migrations/clickhouse/0003_ops_bronze.sql:32` — current `ops.fetch_log` and `bronze.raw_document` schema.
  - Pattern: `cmd/worker-fetch/main.go:361` — fetch log insert path.
  - Pattern: `cmd/worker-fetch/main.go:379` — raw document insert path.
  - API/Type: `internal/fetch/retention.go:74` — current metadata contract including inline/object-store behavior.
  - API/Type: `internal/fetch/retention.go:118` — retention policy resolution that currently allows inline bytes.
  - Test: `internal/fetch/client_test.go:17` — existing fetch behavior test harness.

  **Acceptance Criteria** (agent-executable only):
  - [ ] `go test ./internal/fetch ./cmd/worker-fetch -count=1` exits `0`.
  - [ ] `curl -fsS "http://localhost:8123/?query=DESCRIBE%20TABLE%20bronze.raw_document%20FORMAT%20TabSeparated"` includes the new typed fetch columns.
  - [ ] `curl -fsS "http://localhost:8123/?query=SELECT%20count()%20FROM%20bronze.raw_document%20WHERE%20JSONExtractString(fetch_metadata,'storage_class')='inline'%20AND%20source_id%20IN%20('seed:gdelt','fixture:reliefweb','fixture:acled','fixture:opensanctions','fixture:nasa-firms','fixture:noaa-hazards','fixture:kev')%20FORMAT%20TabSeparated"` prints `0`.

  **QA Scenarios** (MANDATORY — task incomplete without these):
  ```bash
  Scenario: Migrated HTTP fetches always land in object storage
    Tool: Bash
    Steps: go test ./cmd/worker-fetch -run TestFetchOncePersistsObjectStoreOnlyForMigratedSources -count=1 && curl -fsS "http://localhost:8123/?query=SELECT%20count()%20FROM%20bronze.raw_document%20WHERE%20storage_class='inline'%20FORMAT%20TabSeparated"
    Expected: test exits 0 and ClickHouse query prints `0`
    Evidence: .sisyphus/evidence/task-3-raw-ledger.txt

  Scenario: Non-success fetches stay in fetch log without typed bronze writes
    Tool: Bash
    Steps: go test ./cmd/worker-fetch -run TestFailedFetchPersistsLogOnly -count=1
    Expected: exit 0 and test proves `ops.fetch_log` is written while `bronze.raw_document` or source bronze is not written for failed non-304 fetches
    Evidence: .sisyphus/evidence/task-3-raw-ledger-error.txt
  ```

  **Commit**: YES | Message: `feat(fetch): harden raw ledger and object-store retention` | Files: `migrations/clickhouse/*`, `cmd/worker-fetch/main.go`, `internal/fetch/retention.go`, `internal/fetch/*_test.go`, `cmd/worker-fetch/*_test.go`, `seed/source_registry.json`

- [ ] 4. Tighten RBAC and stage ownership boundaries

  **What to do**: Update bootstrap RBAC so `osint_ingest` can read `meta/ops/bronze` and insert only into `ops/bronze`. Add a new `osint_promote` role that can read `meta/ops/bronze/silver` and insert into `silver/gold`. Ensure `worker-fetch` and `worker-parse` use ingest-only privileges, while bronze-driven promote/control-plane jobs use promote privileges. Keep `osint_reader` unchanged and `osint_admin` as bootstrap/admin only.
  **Must NOT do**: Do not leave `osint_ingest` with `INSERT` on `silver.*` or `gold.*`; do not broaden privileges beyond the new stage boundary.

  **Recommended Agent Profile**:
  - Category: `deep` — Reason: access-control mistakes would silently defeat the architecture boundary.
  - Skills: [] — repo-native bootstrap + ClickHouse role work.
  - Omitted: [`playwright`] — no UI work.

  **Parallelization**: Can Parallel: YES | Wave 1 | Blocks: `7,8,10,14` | Blocked By: none

  **References** (executor has NO interview context — be exhaustive):
  - Pattern: `cmd/bootstrap/main.go:42` — existing role definitions and current overly broad `osint_ingest` grants.
  - Pattern: `cmd/bootstrap/main.go` — user/role bootstrap flow to extend.
  - Test: `cmd/bootstrap/source_registry_test.go` — existing bootstrap/source-registry verification pattern to extend for role assertions.
  - Test: `PRODUCTION_READINESS.md` — current intended verify workflow to preserve operational checks.

  **Acceptance Criteria** (agent-executable only):
  - [ ] `go test ./cmd/bootstrap -count=1` exits `0`.
  - [ ] `curl -fsS "http://localhost:8123/?query=SHOW%20GRANTS%20FOR%20osint_ingest"` contains no `INSERT ON silver.*` or `INSERT ON gold.*`.
  - [ ] `curl -fsS "http://localhost:8123/?query=SHOW%20GRANTS%20FOR%20osint_promote"` includes `INSERT ON silver.*` and `INSERT ON gold.*`.

  **QA Scenarios** (MANDATORY — task incomplete without these):
  ```bash
  Scenario: Ingest role cannot cross stage boundaries
    Tool: Bash
    Steps: go test ./cmd/bootstrap -run TestRoleContracts -count=1 && curl -fsS "http://localhost:8123/?query=SHOW%20GRANTS%20FOR%20osint_ingest"
    Expected: exit 0 and grant output proves ingest writes only to `ops.*` and `bronze.*`
    Evidence: .sisyphus/evidence/task-4-rbac.txt

  Scenario: Promote role can write silver/gold while reader cannot
    Tool: Bash
    Steps: go test ./cmd/bootstrap -run TestPromoteRoleContracts -count=1
    Expected: exit 0 and test proves `osint_promote` has the required write grants while `osint_reader` remains read-only
    Evidence: .sisyphus/evidence/task-4-rbac-error.txt
  ```

  **Commit**: YES | Message: `feat(rbac): separate ingest and promote permissions` | Files: `cmd/bootstrap/main.go`, `cmd/bootstrap/*_test.go`, `PRODUCTION_READINESS.md`

- [ ] 5. Freeze replay-stable canonical ID rules and parity harness before cutover

  **What to do**: Remove `raw_id` from canonical event/observation identity generation in `internal/promote/pipeline.go`. Use exact deterministic ID formulas: `event = stableID('event', source_id, native_id_or_contenthash, event_type, starts_at)` and `observation = stableID('observation', source_id, native_id_or_contenthash, observation_type, subject_natural_key, observed_at)`. Keep entity IDs on natural-key logic. Add parity tests that run old fixture semantics and new bronze-driven semantics over the same deterministic local payloads and assert identical silver IDs plus stable gold rollups across reruns.
  **Must NOT do**: Do not change API response shape; do not leave any event/observation ID path dependent on `raw_id` or wall-clock time.

  **Recommended Agent Profile**:
  - Category: `ultrabrain` — Reason: identity changes are cross-cutting, high-risk, and must be exact.
  - Skills: [] — repo-native Go and SQL reasoning is enough.
  - Omitted: [`playwright`] — not applicable.

  **Parallelization**: Can Parallel: YES | Wave 1 | Blocks: `10,11,12,14` | Blocked By: none

  **References** (executor has NO interview context — be exhaustive):
  - Pattern: `internal/promote/pipeline.go:475` — current event/observation ID generation incorrectly depends on `raw_id`.
  - Pattern: `internal/promote/pipeline.go:32` — promote input model and lineage contract.
  - Pattern: `cmd/control-plane/jobs_promote.go:42` — current sample-input bronze bypass to replace with parity harness.
  - Test: `internal/promote/pipeline_test.go` — current promote SQL/idempotency test patterns.
  - Test: `cmd/control-plane/jobs_promote_test.go` — control-plane job assertions to extend for parity checks.

  **Acceptance Criteria** (agent-executable only):
  - [ ] `go test ./internal/promote ./cmd/control-plane -run 'TestPromoteFromBronzePreservesCanonicalIDs|TestPromoteParity' -count=1` exits `0`.
  - [ ] Rerunning the same bronze payload twice produces identical event and observation IDs.
  - [ ] A bronze-vs-fixture parity test proves identical canonical outputs for migrated domains during cutover.

  **QA Scenarios** (MANDATORY — task incomplete without these):
  ```bash
  Scenario: Replay keeps canonical IDs stable
    Tool: Bash
    Steps: go test ./internal/promote -run TestCanonicalIDsIgnoreRawID -count=1 && go test ./cmd/control-plane -run TestPromoteFromBronzePreservesCanonicalIDs -count=1
    Expected: both tests exit 0 and prove identical IDs across refetch/reparse/replay
    Evidence: .sisyphus/evidence/task-5-identity.txt

  Scenario: New bronze-driven promote remains parity-equivalent during migration
    Tool: Bash
    Steps: go test ./cmd/control-plane -run TestPromoteParity -count=1
    Expected: exit 0 and test proves old fixture semantics and bronze-driven semantics yield identical silver IDs and metric rows for the deterministic stub payloads
    Evidence: .sisyphus/evidence/task-5-identity-error.txt
  ```

  **Commit**: YES | Message: `fix(promote): make canonical ids replay stable` | Files: `internal/promote/pipeline.go`, `internal/promote/pipeline_test.go`, `cmd/control-plane/jobs_promote.go`, `cmd/control-plane/jobs_promote_test.go`

- [ ] 6. Rewire control-plane jobs to seed frontier and orchestrate source pipeline stages

  **What to do**: Keep the existing public job names (`ingest-geopolitical`, `ingest-safety-security`) and repurpose them into orchestration wrappers. Each job must: load concrete source metadata from `meta.source_registry`, seed `ops.crawl_frontier` from `entrypoints` when needed, invoke fetch stage, invoke parse stage, invoke promote stage, and record per-source stats. The exact source fan-out is fixed to: geopolitical -> `seed:gdelt`, `fixture:reliefweb`, `fixture:acled`; safety/security -> `fixture:opensanctions`, `fixture:nasa-firms`, `fixture:noaa-hazards`, `fixture:kev`. `fixture:safety` is only a selector alias that expands to those four safety sources and never fetches itself. ACLED is skipped cleanly when credentials are absent and reported in job stats under `disabled_sources`.
  **Must NOT do**: Do not change the `run-once --job` surface; do not shell out to binaries from control-plane; do not keep direct fixture-to-silver writes for migrated sources.

  **Recommended Agent Profile**:
  - Category: `deep` — Reason: orchestration cutover must preserve runtime contract while replacing internals.
  - Skills: [] — existing Go runtime patterns are enough.
  - Omitted: [`playwright`] — no browser work.

  **Parallelization**: Can Parallel: NO | Wave 2 | Blocks: `7,8,11,12` | Blocked By: `1,2`

  **References** (executor has NO interview context — be exhaustive):
  - Pattern: `cmd/control-plane/jobs_geopolitical.go:23` — current geopolitical orchestrator shape to preserve externally.
  - Pattern: `cmd/control-plane/jobs_safety.go:22` — current safety orchestrator shape to preserve externally.
  - Pattern: `cmd/control-plane/main.go` — current `run-once` job registration/help output.
  - Pattern: `internal/discovery/frontier.go:187` — frontier seeding/build helper.
  - Test: `cmd/control-plane/main_test.go` — run-once help/job exposure assertions.
  - Test: `test/e2e/pipeline_test.go:28` — E2E expectations for `run-once --help` and pipeline jobs.

  **Acceptance Criteria** (agent-executable only):
  - [ ] `go test ./cmd/control-plane -run 'TestRunOnceHelp|TestIngestDomainJobOrchestratesSources' -count=1` exits `0`.
  - [ ] `docker compose exec control-plane /control-plane run-once --help` still lists `ingest-geopolitical` and `ingest-safety-security`.
  - [ ] Running `ingest-geopolitical` with no ACLED env completes successfully and reports ACLED under disabled sources rather than failing the job.

  **QA Scenarios** (MANDATORY — task incomplete without these):
  ```bash
  Scenario: Domain jobs preserve public CLI contract while orchestrating source stages
    Tool: Bash
    Steps: go test ./cmd/control-plane -run TestIngestDomainJobOrchestratesSources -count=1 && docker compose exec control-plane /control-plane run-once --help
    Expected: test exits 0 and help output still includes the same public job names
    Evidence: .sisyphus/evidence/task-6-control-plane.txt

  Scenario: Credential-gated source is skipped, not fatal
    Tool: Bash
    Steps: go test ./cmd/control-plane -run TestGeopoliticalJobSkipsACLEDWithoutCredential -count=1
    Expected: exit 0 and test proves job stats contain `disabled_sources=[fixture:acled]` while other sources proceed
    Evidence: .sisyphus/evidence/task-6-control-plane-error.txt
  ```

  **Commit**: YES | Message: `refactor(control-plane): orchestrate http source pipeline` | Files: `cmd/control-plane/main.go`, `cmd/control-plane/jobs_geopolitical.go`, `cmd/control-plane/jobs_safety.go`, `cmd/control-plane/*_test.go`, `test/e2e/pipeline_test.go`

- [ ] 7. Implement frontier-driven fetch execution in `worker-fetch`

  **What to do**: Extend `worker-fetch` so it can claim frontier work for one source at a time, honor source rate limits, fetch entrypoint/request URLs via the existing HTTP client, persist only `ops.fetch_log` + `bronze.raw_document`, and update frontier state/lease/result columns deterministically. Keep existing `fetch-once` and `replay-once` commands, and add one new runtime command `fetch-source --source-id <id> --limit <n>` for deterministic stage execution. Use `allowed_hosts` + `NormalizeURL` validation before fetch. Auth support must resolve the frozen env-ref contract from Task 1; if the referenced env value is absent, the fetch is blocked and frontier state becomes `blocked` without an HTTP request. In this plan, task-level verification uses `httptest.NewServer`-style local HTTP stubs; the compose fixture service is introduced later for full-stack E2E only.
  **Must NOT do**: Do not write source-specific bronze tables here; do not bypass frontier leasing; do not perform live fetches for `bundle_alias` sources.

  **Recommended Agent Profile**:
  - Category: `ultrabrain` — Reason: fetch semantics combine queueing, auth gating, retries, and storage invariants.
  - Skills: [] — existing HTTP client and retention layers are already in repo.
  - Omitted: [`playwright`] — no browser work.

  **Parallelization**: Can Parallel: YES | Wave 2 | Blocks: `8,11,12,14` | Blocked By: `2,3,4,6`

  **References** (executor has NO interview context — be exhaustive):
  - Pattern: `cmd/worker-fetch/main.go:315` — config and source-policy loading path.
  - Pattern: `cmd/worker-fetch/main.go:361` — fetch-log persistence.
  - Pattern: `cmd/worker-fetch/main.go:379` — raw-document persistence.
  - Pattern: `internal/fetch/client.go:309` — current source validation/auth gate that only supports `auth_mode='none'`.
  - Pattern: `internal/discovery/frontier.go:150` — host allow-listing and URL normalization rules.
  - Test: `internal/fetch/client_test.go:17` — `httptest` pattern for HTTP fetch behavior.

  **Acceptance Criteria** (agent-executable only):
  - [ ] `go test ./internal/fetch ./cmd/worker-fetch -count=1` exits `0`.
  - [ ] `go test ./cmd/worker-fetch -run TestFetchSourceClaimsFrontierAndPersistsLedger -count=1` exits `0`.
  - [ ] `go test ./cmd/worker-fetch -run TestFetchSourceBlocksMissingCredential -count=1` exits `0`.
  - [ ] `docker compose exec worker-fetch /worker-fetch fetch-source --source-id seed:gdelt --limit 1` exits `0` in E2E.

  **QA Scenarios** (MANDATORY — task incomplete without these):
  ```bash
  Scenario: Worker fetches leased frontier rows and persists only ledger artifacts
    Tool: Bash
    Steps: go test ./cmd/worker-fetch -run TestFetchSourceClaimsFrontierAndPersistsLedger -count=1
    Expected: exit 0 and test proves frontier state updates plus writes only to `ops.fetch_log` and `bronze.raw_document`
    Evidence: .sisyphus/evidence/task-7-worker-fetch.txt

  Scenario: Missing auth env blocks source without outbound request
    Tool: Bash
    Steps: go test ./cmd/worker-fetch -run TestFetchSourceBlocksMissingCredential -count=1
    Expected: exit 0 and test proves state becomes `blocked`, attempt count increments, and HTTP stub sees zero requests
    Evidence: .sisyphus/evidence/task-7-worker-fetch-error.txt
  ```

  **Commit**: YES | Message: `feat(worker-fetch): execute leased frontier fetches` | Files: `cmd/worker-fetch/main.go`, `internal/fetch/client.go`, `internal/fetch/*_test.go`, `cmd/worker-fetch/*_test.go`

- [ ] 8. Implement parse-to-bronze runtime in `worker-parse`

  **What to do**: Extend `worker-parse` to read stored raw payloads via `raw_id`, resolve parser and parse config from `meta.source_registry`, run the registered parser, write one `ops.parse_log` row per parse attempt, and persist one or more typed bronze rows into the configured `bronze.src_<slug>_v1` table. Add a new command `parse-source --source-id <id> --limit <n>`. Freeze the parsed bronze row identity to `source_record_key + source_record_index`, where `source_record_key = firstNonEmpty(candidate.NativeID, candidate.ContentHash)` and `source_record_index` is the zero-based candidate index within one parse result. Parse failures remain in `ops.parse_log` and never mutate frontier fetch state.
  **Must NOT do**: Do not write silver/gold here; do not key bronze rows by `raw_id` alone; do not dual-write migrated sources into `bronze.raw_structured_row`.

  **Recommended Agent Profile**:
  - Category: `deep` — Reason: parser registry, raw replay, and typed bronze persistence must line up exactly.
  - Skills: [] — repo-native parser/runtime work.
  - Omitted: [`playwright`] — not relevant.

  **Parallelization**: Can Parallel: YES | Wave 2 | Blocks: `9,10,11,12,14` | Blocked By: `2,3,4,6,7`

  **References** (executor has NO interview context — be exhaustive):
  - Pattern: `cmd/worker-parse/main.go:22` — current CLI-only parser entrypoint to extend.
  - Pattern: `internal/parser/registry.go:60` — one-to-many parse result contract.
  - Pattern: `internal/parser/registry.go:218` — parser resolution and execution.
  - Pattern: `migrations/clickhouse/0005_baseline_tables.sql:1` — `ops.parse_log` contract.
  - Pattern: `migrations/clickhouse/0005_baseline_tables.sql:71` — current `bronze.raw_structured_row` table to demote to audit/debug role.
  - Test: `internal/parser/registry_test.go` — parser registry behavior patterns.

  **Acceptance Criteria** (agent-executable only):
  - [ ] `go test ./internal/parser ./cmd/worker-parse -count=1` exits `0`.
  - [ ] `go test ./cmd/worker-parse -run TestParseSourceWritesTypedBronzeRows -count=1` exits `0`.
  - [ ] `go test ./cmd/worker-parse -run TestParseFailureWritesParseLogOnly -count=1` exits `0`.

  **QA Scenarios** (MANDATORY — task incomplete without these):
  ```bash
  Scenario: One raw payload can emit multiple typed bronze rows
    Tool: Bash
    Steps: go test ./cmd/worker-parse -run TestParseSourceWritesTypedBronzeRows -count=1
    Expected: exit 0 and test proves multiple candidates map to unique `(source_record_key, source_record_index)` rows in the source bronze table
    Evidence: .sisyphus/evidence/task-8-worker-parse.txt

  Scenario: Parse failure records log without silver/gold side effects
    Tool: Bash
    Steps: go test ./cmd/worker-parse -run TestParseFailureWritesParseLogOnly -count=1
    Expected: exit 0 and test proves `ops.parse_log` records failure while source bronze and silver remain unchanged
    Evidence: .sisyphus/evidence/task-8-worker-parse-error.txt
  ```

  **Commit**: YES | Message: `feat(worker-parse): write typed source bronze rows` | Files: `cmd/worker-parse/main.go`, `internal/parser/*`, `cmd/worker-parse/*_test.go`

- [ ] 9. Add static per-source bronze tables and retire `bronze.raw_structured_row` from migrated flows

  **What to do**: Create migration-defined typed bronze tables for the seven concrete sources listed in Task 1. Every table must share this mandatory column contract exactly: `raw_id String`, `fetch_id String`, `source_id LowCardinality(String)`, `parser_id LowCardinality(String)`, `parser_version String`, `source_record_key String`, `source_record_index UInt32`, `record_kind LowCardinality(String)`, `native_id Nullable(String)`, `source_url String`, `canonical_url Nullable(String)`, `fetched_at DateTime64(3, 'UTC')`, `parsed_at DateTime64(3, 'UTC')`, `occurred_at Nullable(DateTime64(3, 'UTC'))`, `published_at Nullable(DateTime64(3, 'UTC'))`, `title Nullable(String)`, `summary Nullable(String)`, `status Nullable(String)`, `place_hint Nullable(String)`, `lat Nullable(Float64)`, `lon Nullable(Float64)`, `severity Nullable(String)`, `content_hash String`, `schema_version UInt32`, `record_version UInt64`, `attrs String`, `evidence String`, `payload_json String`. Use `ReplacingMergeTree(record_version)`, `PARTITION BY toYYYYMM(parsed_at)`, and `ORDER BY (source_record_key, parsed_at, raw_id, source_record_index)`. Keep `bronze.raw_structured_row` in schema, but migrated sources must not write to it.
  **Must NOT do**: Do not create tables dynamically at runtime; do not make `bronze.raw_structured_row` a co-equal typed bronze store for migrated sources.

  **Recommended Agent Profile**:
  - Category: `deep` — Reason: schema contract and migration DDL must be fixed and replay-safe.
  - Skills: [] — repo-native ClickHouse work.
  - Omitted: [`playwright`] — not relevant.

  **Parallelization**: Can Parallel: YES | Wave 2 | Blocks: `10,11,12,14` | Blocked By: `1,8`

  **References** (executor has NO interview context — be exhaustive):
  - Pattern: `migrations/clickhouse/0003_ops_bronze.sql:48` — universal raw ledger table that remains fetch-only.
  - Pattern: `migrations/clickhouse/0005_baseline_tables.sql:71` — existing bronze structured-row pattern to supersede for migrated sources.
  - Pattern: `migrations/clickhouse/0005_baseline_tables.sql:90` — current silver table conventions for partition/order style.
  - External: `https://github.com/ClickHouse/clickhouse-docs/blob/6f6639f7a6ff63b863639858d50582a925028b4a/docs/best-practices/choosing_a_primary_key.md` — ordering-key guidance to keep source tables queryable.
  - External: `https://github.com/ClickHouse/clickhouse-docs/blob/6f6639f7a6ff63b863639858d50582a925028b4a/docs/best-practices/json_type.md` — typed columns plus JSON-tail guidance.

  **Acceptance Criteria** (agent-executable only):
  - [ ] `go test ./internal/migrate -count=1` exits `0`.
  - [ ] `curl -fsS "http://localhost:8123/?query=SHOW%20TABLES%20FROM%20bronze%20LIKE%20'src_%25'%20FORMAT%20TabSeparated"` lists all seven required source bronze tables.
  - [ ] `curl -fsS "http://localhost:8123/?query=DESCRIBE%20TABLE%20bronze.src_seed_gdelt_v1%20FORMAT%20TabSeparated"` matches the mandatory bronze contract.

  **QA Scenarios** (MANDATORY — task incomplete without these):
  ```bash
  Scenario: Static bronze DDL exists for every concrete source
    Tool: Bash
    Steps: go test ./internal/migrate -run TestSourceBronzeTables -count=1 && curl -fsS "http://localhost:8123/?query=SHOW%20TABLES%20FROM%20bronze%20LIKE%20'src_%25'%20FORMAT%20TabSeparated"
    Expected: exit 0 and output lists all seven source tables with `_v1` suffixes
    Evidence: .sisyphus/evidence/task-9-source-bronze.txt

  Scenario: Migrated sources no longer write to `bronze.raw_structured_row`
    Tool: Bash
    Steps: go test ./cmd/worker-parse -run TestMigratedSourcesBypassRawStructuredRow -count=1
    Expected: exit 0 and test proves migrated sources write only to `bronze.src_*_v1`
    Evidence: .sisyphus/evidence/task-9-source-bronze-error.txt
  ```

  **Commit**: YES | Message: `feat(bronze): add typed per-source landing tables` | Files: `migrations/clickhouse/*`, `internal/migrate/*_test.go`, `cmd/worker-parse/*_test.go`

- [ ] 10. Replace sample-input promotion with bronze-driven ClickHouse ELT

  **What to do**: Remove runtime dependence on `PROMOTE_PIPELINE_INPUT` / `SampleInputs()` for normal promote flow. Make `promote` operate on bronze tables selected by `source_id` and `promote_profile`, using deterministic `INSERT ... SELECT` SQL into canonical silver tables and the existing metric materialization path. Keep `internal/promote/pipeline.go` only as reusable canonical-shape logic and anti-join pattern reference where helpful, but the runtime path must promote directly from bronze tables in ClickHouse. Promotion must remain idempotent by source/time slice and must use deterministic delete-and-reinsert or anti-join strategy per target table. Refresh existing metric/gold outputs after silver changes.
  **Must NOT do**: Do not leave `SampleInputs()` on the normal runtime path; do not promote directly from fixture structs; do not change silver/gold schema shape.

  **Recommended Agent Profile**:
  - Category: `ultrabrain` — Reason: this is the actual bronze->silver architecture cutover and must preserve canonical semantics.
  - Skills: [] — repo-native Go/SQL work.
  - Omitted: [`playwright`] — no UI work.

  **Parallelization**: Can Parallel: YES | Wave 2 | Blocks: `11,12,13,14` | Blocked By: `4,5,8,9`

  **References** (executor has NO interview context — be exhaustive):
  - Pattern: `cmd/control-plane/jobs_promote.go:30` — current sample-input runtime path to replace.
  - Pattern: `internal/promote/pipeline.go:263` — anti-join and canonical insert SQL style to preserve.
  - Pattern: `internal/promote/pipeline.go:964` — unresolved queue insert pattern to preserve.
  - Pattern: `internal/metrics/materialization_sql.go:19` — current silver->gold metric materialization path.
  - Pattern: `migrations/clickhouse/gold_api_views.sql` — API-facing views that must stay stable.
  - Test: `internal/promote/pipeline_test.go` — promotion SQL verification patterns.

  **Acceptance Criteria** (agent-executable only):
  - [ ] `go test ./internal/promote ./cmd/control-plane -count=1` exits `0`.
  - [ ] `go test ./cmd/control-plane -run TestPromoteFromBronzeSourceTables -count=1` exits `0`.
  - [ ] `docker compose exec control-plane /control-plane run-once --job promote` succeeds without `PROMOTE_PIPELINE_INPUT*` env vars when bronze rows exist.

  **QA Scenarios** (MANDATORY — task incomplete without these):
  ```bash
  Scenario: Promote job reads source bronze and updates canonical silver/gold
    Tool: Bash
    Steps: go test ./cmd/control-plane -run TestPromoteFromBronzeSourceTables -count=1 && go test ./internal/promote -run TestBronzePromoteSQLIsIdempotent -count=1
    Expected: both tests exit 0 and prove bronze->silver promotion plus metric refresh are deterministic
    Evidence: .sisyphus/evidence/task-10-promote.txt

  Scenario: Promote no longer depends on sample input env vars
    Tool: Bash
    Steps: go test ./cmd/control-plane -run TestPromoteIgnoresSampleInputInRuntimeMode -count=1
    Expected: exit 0 and test proves bronze-backed runtime path succeeds without env/json sample input
    Evidence: .sisyphus/evidence/task-10-promote-error.txt
  ```

  **Commit**: YES | Message: `refactor(promote): drive silver elt from bronze tables` | Files: `cmd/control-plane/jobs_promote.go`, `internal/promote/*`, `internal/metrics/materialization_sql.go`, `cmd/control-plane/*_test.go`, `internal/promote/*_test.go`

- [ ] 11. Migrate geopolitical sources from fixture loaders to HTTP -> bronze -> silver

  **What to do**: Replace the runtime geopolitical path so `seed:gdelt`, `fixture:reliefweb`, and `fixture:acled` no longer use in-process fixture loaders. Keep `internal/packs/geopolitical/geopolitical.go` only as transformation logic and/or test helpers, but runtime data must come from `bronze.src_seed_gdelt_v1`, `bronze.src_fixture_reliefweb_v1`, and `bronze.src_fixture_acled_v1`. Add parser/profile config and promote SQL so these bronze tables produce the same canonical `silver.fact_event`, `silver.dim_entity`, bridge tables, and pack metrics currently emitted by the fixture plan. ACLED remains credential-gated; when env is absent, bronze/promotion for ACLED is skipped and the domain job still succeeds. Task-level validation here uses `httptest.NewServer` and stubbed ClickHouse patterns, not the later compose E2E fixture service.
  **Must NOT do**: Do not keep `loadGDELTFixtures`, `loadReliefWebFixtures`, or `loadACLEDFixtures` on the runtime path; do not change geopolitical metric IDs or API semantics.

  **Recommended Agent Profile**:
  - Category: `deep` — Reason: this is the first real cutover from fixture runtime to bronze-driven canonical outputs.
  - Skills: [] — repo-native Go/SQL transformation work.
  - Omitted: [`playwright`] — not relevant.

  **Parallelization**: Can Parallel: YES | Wave 3 | Blocks: `13,14` | Blocked By: `6,7,8,9,10`

  **References** (executor has NO interview context — be exhaustive):
  - Pattern: `cmd/control-plane/jobs_geopolitical.go:23` — current domain job wrapper to preserve externally.
  - Pattern: `internal/packs/geopolitical/geopolitical.go:170` — current `BuildIngestPlan` transform logic to preserve semantically while changing inputs.
  - Pattern: `internal/packs/geopolitical/geopolitical.go:264` — current adapter list showing fixture loaders to retire from runtime.
  - Test: `internal/packs/geopolitical/geopolitical_test.go` — deterministic pack expectations to preserve.
  - Test: `cmd/control-plane/jobs_geopolitical_test.go` — control-plane runtime test harness.

  **Acceptance Criteria** (agent-executable only):
  - [ ] `go test ./internal/packs/geopolitical ./cmd/control-plane -run 'TestGeopoliticalBronzePromote|TestGeopoliticalJob' -count=1` exits `0`.
  - [ ] `docker compose exec control-plane /control-plane run-once --job ingest-geopolitical` exits `0`.
  - [ ] `go test ./cmd/control-plane -run TestGeopoliticalBronzePromote -count=1` proves `bronze.src_seed_gdelt_v1` and `bronze.src_fixture_reliefweb_v1` receive rows from `httptest`-backed source fetches.

  **QA Scenarios** (MANDATORY — task incomplete without these):
  ```bash
  Scenario: Geopolitical HTTP sources populate bronze and canonical silver outputs
    Tool: Bash
    Steps: go test ./cmd/control-plane -run TestGeopoliticalBronzePromote -count=1 && go test ./internal/packs/geopolitical -count=1
    Expected: both tests exit 0 and prove GDELT/ReliefWeb bronze rows promote to the same canonical event/entity/metric outputs as the legacy deterministic fixtures
    Evidence: .sisyphus/evidence/task-11-geopolitical.txt

  Scenario: ACLED stays gated until credential exists
    Tool: Bash
    Steps: go test ./cmd/control-plane -run TestGeopoliticalJobSkipsACLEDWithoutCredential -count=1
    Expected: exit 0 and test proves ACLED contributes zero bronze rows without env activation while the job still succeeds
    Evidence: .sisyphus/evidence/task-11-geopolitical-error.txt
  ```

  **Commit**: YES | Message: `feat(geopolitical): promote http sources from bronze` | Files: `cmd/control-plane/jobs_geopolitical.go`, `internal/packs/geopolitical/*`, `cmd/control-plane/jobs_geopolitical_test.go`, `seed/source_registry.json`

- [ ] 12. Migrate safety/security concrete sources from fixture loaders to HTTP -> bronze -> silver

  **What to do**: Replace runtime safety/security fixture ingestion with bronze-driven processing for `fixture:opensanctions`, `fixture:nasa-firms`, `fixture:noaa-hazards`, and `fixture:kev`. Preserve `fixture:safety` only as orchestration alias. Each concrete source must fetch via HTTP, parse into its source bronze table, and promote into canonical observation/entity rows plus the existing safety/security metric set. Keep the exact shipped metric IDs unchanged. Move any source-specific mapping logic out of runtime fixture loaders and into parser config or bronze-to-silver SQL. Task-level validation here uses `httptest.NewServer` and stubbed ClickHouse patterns, not the later compose E2E fixture service.
  **Must NOT do**: Do not fetch `fixture:safety` directly; do not keep runtime fixture loaders for OpenSanctions/FIRMS/NOAA/KEV; do not change metric names or API behavior.

  **Recommended Agent Profile**:
  - Category: `deep` — Reason: four heterogeneous source shapes converge into one canonical safety domain.
  - Skills: [] — repo-native Go/SQL transformation work.
  - Omitted: [`playwright`] — not needed.

  **Parallelization**: Can Parallel: YES | Wave 3 | Blocks: `13,14` | Blocked By: `6,7,8,9,10`

  **References** (executor has NO interview context — be exhaustive):
  - Pattern: `cmd/control-plane/jobs_safety.go:22` — current safety domain wrapper to preserve externally.
  - Pattern: `internal/packs/safety/safety.go:146` — current `BuildIngestPlan` transform logic to preserve semantically while changing inputs.
  - Pattern: `internal/packs/safety/safety.go:221` — current adapter list showing fixture loaders to retire from runtime.
  - Test: `internal/packs/safety/safety_test.go` — deterministic safety expectations to preserve.
  - Test: `cmd/control-plane/jobs_safety_test.go` — control-plane runtime test harness.

  **Acceptance Criteria** (agent-executable only):
  - [ ] `go test ./internal/packs/safety ./cmd/control-plane -run 'TestSafetyBronzePromote|TestSafetyJob' -count=1` exits `0`.
  - [ ] `docker compose exec control-plane /control-plane run-once --job ingest-safety-security` exits `0`.
  - [ ] `go test ./cmd/control-plane -run TestSafetyBronzePromote -count=1` proves the four safety bronze tables receive rows from `httptest`-backed source fetches.

  **QA Scenarios** (MANDATORY — task incomplete without these):
  ```bash
  Scenario: Safety/security concrete sources populate bronze and canonical outputs
    Tool: Bash
    Steps: go test ./cmd/control-plane -run TestSafetyBronzePromote -count=1 && go test ./internal/packs/safety -count=1
    Expected: both tests exit 0 and prove OpenSanctions/FIRMS/NOAA/KEV bronze rows promote into canonical observation/entity rows and unchanged safety metrics
    Evidence: .sisyphus/evidence/task-12-safety.txt

  Scenario: Bundle alias expands but never fetches directly
    Tool: Bash
    Steps: go test ./cmd/control-plane -run TestSafetyAliasExpandsConcreteSourcesOnly -count=1
    Expected: exit 0 and test proves `fixture:safety` orchestrates four concrete sources without any `bronze.src_fixture_safety_*` table or fetch requests
    Evidence: .sisyphus/evidence/task-12-safety-error.txt
  ```

  **Commit**: YES | Message: `feat(safety): promote concrete http sources from bronze` | Files: `cmd/control-plane/jobs_safety.go`, `internal/packs/safety/*`, `cmd/control-plane/jobs_safety_test.go`, `seed/source_registry.json`

- [ ] 13. Add compose-level HTTP fixture service, docs, and full E2E pipeline coverage

  **What to do**: Add a compose-level deterministic HTTP fixture service for full-stack E2E so no compose test depends on public internet. This service is only for end-to-end/docker-compose verification; earlier task-level tests continue to use `httptest` and stubbed ClickHouse. Use the compose fixture service to serve stable payloads for GDELT, ReliefWeb, OpenSanctions, NASA FIRMS, NOAA hazards, KEV, and ACLED credential-gated stubs. Update `docker-compose.yml`, `test/e2e/pipeline_test.go`, `README.md`, `docs/capability-matrix.json`, and `docs/capability-matrix.md` so the documented runtime is the new HTTP -> bronze -> silver architecture. Explicitly document that maritime, aviation, and space remain non-HTTP fixture packs until concrete source registry entries are introduced.
  **Must NOT do**: Do not document public live crawling as verified in CI; do not claim maritime/aviation/space are part of this HTTP migration.

  **Recommended Agent Profile**:
  - Category: `writing` — Reason: this is contract/documentation/E2E alignment over an already-decided runtime shape.
  - Skills: [] — repo-native docs/test work.
  - Omitted: [`playwright`] — browser testing is not needed.

  **Parallelization**: Can Parallel: YES | Wave 3 | Blocks: `14` | Blocked By: `1,2,3,10,11,12`

  **References** (executor has NO interview context — be exhaustive):
  - Pattern: `test/e2e/pipeline_test.go:19` — current E2E suite to extend.
  - Pattern: `README.md` — documented docker-compose and test workflow.
  - Pattern: `docs/capability-matrix.json` — machine-readable implementation truth table.
  - Pattern: `docs/capability-matrix.md` — human-readable implementation contract.
  - Test: `internal/fetch/client_test.go:17` — deterministic local HTTP server pattern.

  **Acceptance Criteria** (agent-executable only):
  - [ ] `docker compose up -d --build` exits successfully with the local HTTP fixture service available for tests.
  - [ ] `go test ./test/e2e -tags=e2e -run TestHTTPSourcePipeline -count=1` exits `0`.
  - [ ] `grep -R "fixture-backed HTTP runtime" README.md docs test/e2e` returns no stale runtime claims.

  **QA Scenarios** (MANDATORY — task incomplete without these):
  ```bash
  Scenario: Full compose E2E uses local HTTP fixture service only
    Tool: Bash
    Steps: docker compose up -d --build && go test ./test/e2e -tags=e2e -run TestHTTPSourcePipeline -count=1
    Expected: compose exits successfully, the E2E test exits 0, and no public endpoint dependency is required
    Evidence: .sisyphus/evidence/task-13-e2e.txt

  Scenario: Documentation reflects actual migrated scope only
    Tool: Bash
    Steps: grep -R "fixture-backed HTTP runtime\|all sources live" README.md docs test/e2e
    Expected: no stale claims remain; migrated scope explicitly calls out geopolitical+safety concrete HTTP sources only
    Evidence: .sisyphus/evidence/task-13-e2e-error.txt
  ```

  **Commit**: YES | Message: `docs(e2e): document local http bronze pipeline` | Files: `docker-compose.yml`, `test/e2e/pipeline_test.go`, `README.md`, `docs/capability-matrix.json`, `docs/capability-matrix.md`, `docs/runbooks/*`

- [ ] 14. Cut over migrated HTTP sources, remove runtime fixture bypasses, and prove replay/backfill safety

  **What to do**: Remove runtime fixture-only bypasses for migrated HTTP sources from `cmd/control-plane/jobs_geopolitical.go`, `cmd/control-plane/jobs_safety.go`, and any helper path those jobs still use. Keep non-migrated maritime/aviation/space runtime paths unchanged. Add replay/backfill safety tests that seed duplicate or retried bronze inputs, rerun fetch/parse/promote, and prove no canonical duplication. Pin explicit ClickHouse dedup settings in runtime SQL where async inserts or dependent materializations rely on dedup behavior. Add one isolated backfill path using duplicate bronze/silver staging tables plus `MOVE PARTITION` or equivalent tested batch cutover for replayable source backfills.
  **Must NOT do**: Do not leave dormant fixture runtime branches for migrated HTTP sources; do not rely on implicit ClickHouse version defaults for dedup semantics.

  **Recommended Agent Profile**:
  - Category: `unspecified-high` — Reason: final cutover mixes cleanup, replay safety, and operational verification.
  - Skills: [] — repo-native runtime/test work.
  - Omitted: [`playwright`] — not relevant.

  **Parallelization**: Can Parallel: NO | Wave 3 | Blocks: final verification only | Blocked By: `2,3,5,7,8,9,10,11,12,13`

  **References** (executor has NO interview context — be exhaustive):
  - Pattern: `cmd/control-plane/jobs_geopolitical.go:36` — current runtime fixture entrypoint to remove.
  - Pattern: `cmd/control-plane/jobs_safety.go:35` — current runtime fixture entrypoint to remove.
  - Pattern: `internal/promote/pipeline.go:263` — idempotent insert style to preserve.
  - Pattern: `internal/metrics/materialization_sql.go:19` — current metric refresh path to keep deterministic under replay.
  - External: `https://github.com/ClickHouse/clickhouse-docs/blob/6f6639f7a6ff63b863639858d50582a925028b4a/docs/guides/developer/deduplicating-inserts-on-retries.md` — retry dedup guidance.
  - External: `https://github.com/ClickHouse/clickhouse-docs/blob/6f6639f7a6ff63b863639858d50582a925028b4a/docs/data-modeling/backfilling.md` — duplicate-table backfill pattern.

  **Acceptance Criteria** (agent-executable only):
  - [ ] `grep -R "loadGDELTFixtures\|loadReliefWebFixtures\|loadACLEDFixtures\|loadOpenSanctionsFixtures\|loadFIRMSFixtures\|loadNOAAHazardFixtures\|loadKEVFixtures" cmd/control-plane internal/packs | grep -v _test.go` returns no migrated runtime path matches.
  - [ ] `go test ./cmd/control-plane ./internal/promote ./internal/metrics -run 'TestReplayDoesNotDuplicateCanonicalRows|TestBackfillCutover' -count=1` exits `0`.
  - [ ] Rerunning the same compose E2E pipeline twice leaves silver/gold row counts stable for deterministic local fixture payloads.

  **QA Scenarios** (MANDATORY — task incomplete without these):
  ```bash
  Scenario: Replay and rerun remain duplication-safe
    Tool: Bash
    Steps: go test ./cmd/control-plane ./internal/promote -run 'TestReplayDoesNotDuplicateCanonicalRows|TestBackfillCutover' -count=1
    Expected: exit 0 and tests prove repeated fetch/parse/promote cycles keep canonical row identities and counts stable
    Evidence: .sisyphus/evidence/task-14-cutover.txt

  Scenario: Migrated runtime fixture loaders are fully removed
    Tool: Bash
    Steps: grep -R "loadGDELTFixtures\|loadReliefWebFixtures\|loadACLEDFixtures\|loadOpenSanctionsFixtures\|loadFIRMSFixtures\|loadNOAAHazardFixtures\|loadKEVFixtures" cmd/control-plane internal/packs | grep -v _test.go
    Expected: no output for migrated runtime paths
    Evidence: .sisyphus/evidence/task-14-cutover-error.txt
  ```

  **Commit**: YES | Message: `refactor(ingest): cut over http sources and retire runtime fixtures` | Files: `cmd/control-plane/jobs_geopolitical.go`, `cmd/control-plane/jobs_safety.go`, `internal/packs/geopolitical/*`, `internal/packs/safety/*`, `internal/promote/*`, `internal/metrics/*`, `test/e2e/*`

## Final Verification Wave (4 parallel agents, ALL must APPROVE)
- [ ] F1. Plan Compliance Audit — oracle
- [ ] F2. Code Quality Review — unspecified-high
- [ ] F3. Real Manual QA — unspecified-high (+ playwright if UI)
- [ ] F4. Scope Fidelity Check — deep

## Commit Strategy
- Commit after every completed numbered task.
- Use one commit per task; do not bundle unrelated tasks.
- Preserve compatibility commits around source registry, frontier, and business ID changes so reversions are isolated.

## Success Criteria
- All currently configured concrete HTTP sources ingest through the new HTTP -> raw ledger -> source bronze -> silver path.
- Fixture-only HTTP bypasses are removed from runtime paths for geopolitical and safety/security sources.
- Canonical silver/gold/API outputs remain stable under replay and rerun.
- Source-specific bronze tables exist, are populated, and are the sole typed bronze truth for migrated sources.
- The repo has deterministic local verification for fetch, parse, promote, and full E2E pipeline behavior.
