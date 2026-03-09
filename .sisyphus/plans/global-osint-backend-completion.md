# Global OSINT Backend Completion Plan

## TL;DR
> **Summary**: Take the repo from Phase A scaffold to full backlog completion by first repairing reproducibility and bootstrap safety, then building the place/governance spine, then the ingest-promote-serve pipeline, then domain packs, and finally the optional scale-out path.
> **Deliverables**:
> - reproducible Docker/Go baseline with idempotent bootstrap and executable smoke checks
> - full ClickHouse schema zones, serving views, place graph, source governance, discovery, fetch, parse, promote, dedup, metrics, and explainability layers
> - complete REST `/v1/*` surface backed by real queries and compatibility tests
> - completed geopolitical, maritime, aviation, space, and safety/security packs
> - runbooks, CI, DR drills, and scale-out/HA implementation path through E18
> **Effort**: XL
> **Parallel**: YES - 6 waves
> **Critical Path**: Task 1 -> Task 2 -> Task 3 -> Task 4 -> Task 7 -> Task 8 -> Task 10 -> Task 11 -> Task 12 -> Task 13 -> Task 14 -> Task 15 -> Task 18 -> Task 20 -> Task 21 -> Tasks 22-25 -> Task 26

## Context
### Original Request
Create a comprehensive plan, using `docs/comprehensive_delivery_plan.md` plus the provided backlog, that covers all work needed to mark the Global OSINT backend complete.

### Interview Summary
- The request is architecture-tier planning, not implementation.
- Repo exploration shows only Phase A scaffold is real today: Compose, bootstrap migration runner, API stubs, minimal migrations, one seed file, and minimal unit tests.
- The plan must stay grounded in existing files such as `cmd/bootstrap/main.go:41`, `cmd/api/main.go:13`, `internal/migrate/http_runner.go:21`, `migrations/clickhouse/0002_core_tables.sql:1`, and `migrations/clickhouse/0003_ops_bronze.sql:1`.
- The backlog still stands as the scope source of truth, but the current delivery doc overstates completion and front-loads later-wave work into foundation.

### Metis Review (gaps addressed)
- Added an explicit repo-reality gate before any epic closure.
- Kept bootstrap install-only; moved heavy dataset loads, dictionaries, and backfills into explicit jobs.
- Froze single-node baseline as mandatory until the core path and quality gates pass.
- Converted acceptance expectations into agent-executable commands and negative-path checks.
- Treated Section 21 of `docs/comprehensive_delivery_plan.md` as normative architecture guidance rather than optional commentary.

## Work Objectives
### Core Objective
Finish all backlog epics E0-E18 in one execution program while preserving Go-first, ClickHouse-first, Docker Compose, REST-only, and backward-compatible defaults.

### Deliverables
- Buildable and reproducible service images for `bootstrap`, `api`, `control-plane`, `worker-fetch`, `worker-parse`, and `renderer`
- Idempotent bootstrap and upgrade path with pinned versions, smoke tests, and backup hooks
- Complete ClickHouse `meta`, `ops`, `bronze`, `silver`, and `gold` schemas plus serving views and dictionaries
- Global place graph through continent/admin4 where data exists, with reverse-geocoding and validation fixtures
- Source governance, discovery, fetch, parse, canonicalization, location attribution, promotion, dedup, and entity resolution pipelines
- Metric registry, contributions, rollups, snapshots, explainability payloads, and quality scorecards
- Full REST API surface backed by real queries, compatibility views, and contract tests
- Domain packs for geopolitical/general-web, maritime, aviation, space, and safety/security
- CI, runbooks, DR drills, SLO verification, and optional cluster/HA rollout

### Definition of Done (verifiable conditions with commands)
- `docker compose config` exits `0`.
- `docker compose build` exits `0` for every defined service.
- `docker compose up -d --build` yields healthy `clickhouse`, `minio`, `bootstrap`, `api`, `control-plane`, `worker-fetch`, `worker-parse`, and `renderer` services.
- `go test ./...` exits `0`.
- Contract, integration, migration, and replay suites exit `0`.
- `curl -fsS http://localhost:8080/v1/health` returns HTTP `200`.
- `curl -fsS http://localhost:8080/v1/ready` returns `{"data":{"ready":true}}` only after bootstrap and required jobs complete.
- ClickHouse queries prove all required databases, tables, views, dictionaries, and metric snapshots exist and are populated by fixture runs.

### Must Have
- Pinned image versions; no `latest` tags.
- One authoritative migration ledger and one authoritative seed-evolution strategy.
- One internal job execution contract for deterministic, agent-executable runs.
- Exact RFC 9309 robots behavior and first-class source governance fields.
- `geoBoundaries gbOpen` plus GeoNames as the location spine.
- Client-side batch inserts first; async inserts only where batching is not feasible and always with `wait_for_async_insert=1`.
- Incremental MVs for real-time aggregate state; refreshable MVs for scheduled snapshots and denormalized caches.
- All `/v1/*` changes additive by default and guarded by contract fixtures.
- Every task verified by agent-executable happy-path and failure-path scenarios.

### Must NOT Have
- No bootstrap backfills, dictionary builds, or long-running dataset loads hidden inside install-time startup.
- No second primary database, Kafka-like bus, Airflow/Temporal, Kubernetes dependency, or graph database in baseline completion scope.
- No browser-rendered connectors unless the source class has no non-browser path and the pack explicitly requires it.
- No Common Crawl or Wayback data mixed into freshness-first live pipelines.
- No breaking `/v1/*` changes, no manual post-deploy SQL, and no “works by inspection” acceptance.
- No use of `ReplacingMergeTree` where immediate correctness is required without `FINAL` or alternative correctness handling.

## Verification Strategy
> ZERO HUMAN INTERVENTION - all verification is agent-executed.
- Test decision: tests-after + Go stdlib tests, docker-compose integration runs, ClickHouse SQL assertions, and HTTP contract fixtures.
- QA policy: every task must ship runnable happy-path and failure-path checks using `bash`, `docker compose`, `curl`, and `go test`; `playwright` is only allowed for renderer/browser-specific source-pack work if no HTTP-only alternative exists.
- Evidence: `.sisyphus/evidence/task-{N}-{slug}.{ext}`

## Execution Strategy
### Parallel Execution Waves
> Target: 5-8 tasks per wave. Extract shared contracts and substrate first to maximize later parallelism.

Wave 1: repo reality + contract freeze + bootstrap/schema substrate
- Task 1. Repair repo reality and reproducible build baseline
- Task 2. Freeze migration, readiness, and internal job execution contracts
- Task 3. Complete Compose/bootstrap topology, buckets, RBAC, smoke runner, and backup hooks
- Task 4. Expand meta registries and schema standards
- Task 5. Expand ops/bronze/silver/gold baseline tables and performance conventions

Wave 2: governance + place spine
- Task 6. Freeze `source_registry` governance schema and seed evolution behavior
- Task 7. Implement place dataset acquisition and staging jobs
- Task 8. Build place graph tables, internal IDs, hierarchy, and polygon dictionary
- Task 9. Add place validation fixtures, coverage reports, and reverse-geocode benchmarks

Wave 3: discovery + fetch + parse + canonical envelopes
- Task 10. Implement discovery engine and frontier ranking
- Task 11. Implement fetch worker and raw retention/replay policy
- Task 12. Implement parser framework and structured parsers/extractors
- Task 13. Implement canonical envelopes, IDs, evidence, and schema-version contracts

Wave 4: promotion + serving core + resolution
- Task 14. Implement location attribution and unresolved-location workflow
- Task 15. Implement end-to-end promotion pipeline into silver facts
- Task 16. Implement core serving views and core API read paths
- Task 17. Implement deduplication and entity resolution baseline

Wave 5: metrics + quality + first domain pack + API expansion
- Task 18. Implement metric registry, contributions, state/snapshot MVs, and explainability payloads
- Task 19. Implement CI, fixtures, dashboards, review workflows, and runbooks
- Task 20. Deliver geopolitical/general-web pack
- Task 21. Expand API to metrics, analytics, entities, tracks, and search

Wave 6: remaining domain packs + scale path
- Task 22. Deliver maritime pack
- Task 23. Deliver aviation pack
- Task 24. Deliver space pack
- Task 25. Deliver safety/security pack
- Task 26. Deliver scale-out/HA topology, distributed tables, cluster DR, and cost controls

### Dependency Matrix (full, all tasks)
- Task 1: blocked by none; blocks Tasks 2, 3, 19.
- Task 2: blocked by Task 1; blocks Tasks 3, 4, 6, 10, 19.
- Task 3: blocked by Tasks 1-2; blocks Tasks 7, 10, 11, 19, 26.
- Task 4: blocked by Task 2; blocks Tasks 5, 6, 7, 13.
- Task 5: blocked by Task 4; blocks Tasks 8, 16, 18, 26.
- Task 6: blocked by Tasks 2, 4; blocks Tasks 10, 11, 16.
- Task 7: blocked by Tasks 3-4; blocks Task 8.
- Task 8: blocked by Tasks 5, 7; blocks Tasks 9, 14.
- Task 9: blocked by Task 8; blocks Tasks 14, 19.
- Task 10: blocked by Tasks 3, 6; blocks Tasks 11, 15, 20.
- Task 11: blocked by Tasks 3, 6, 10; blocks Tasks 12, 15, 20, 22, 23, 24, 25.
- Task 12: blocked by Task 11; blocks Tasks 13, 15, 20, 22, 23, 24, 25.
- Task 13: blocked by Tasks 4, 12; blocks Tasks 14, 15, 17, 18, 20, 22, 23, 24, 25.
- Task 14: blocked by Tasks 8-9, 13; blocks Tasks 15, 18, 20, 22, 23, 24, 25.
- Task 15: blocked by Tasks 10-14; blocks Tasks 16, 17, 18, 20.
- Task 16: blocked by Tasks 5-6, 15; blocks Tasks 19, 21.
- Task 17: blocked by Tasks 13, 15; blocks Tasks 18, 21, 22, 23, 24, 25.
- Task 18: blocked by Tasks 5, 13-17; blocks Tasks 19, 20, 21, 22, 23, 24, 25, 26.
- Task 19: blocked by Tasks 1-3, 9, 16, 18; blocks Task 26 and release readiness.
- Task 20: blocked by Tasks 10-15, 18; blocks Task 21.
- Task 21: blocked by Tasks 16-18, 20; blocks Tasks 22-26.
- Task 22: blocked by Tasks 11-14, 17-18, 21; blocks Task 26.
- Task 23: blocked by Tasks 11-14, 17-18, 21; blocks Task 26.
- Task 24: blocked by Tasks 11-14, 17-18, 21; blocks Task 26.
- Task 25: blocked by Tasks 11-14, 17-18, 21; blocks Task 26.
- Task 26: blocked by Tasks 3, 5, 18-19, 21-25.

### Agent Dispatch Summary
- Wave 1 -> 5 tasks -> `deep`, `unspecified-high`, `writing`
- Wave 2 -> 4 tasks -> `deep`, `unspecified-high`
- Wave 3 -> 4 tasks -> `deep`, `unspecified-high`
- Wave 4 -> 4 tasks -> `deep`, `unspecified-high`
- Wave 5 -> 4 tasks -> `deep`, `unspecified-high`, `writing`
- Wave 6 -> 5 tasks -> `deep`, `ultrabrain`, `unspecified-high`

## TODOs
> Implementation + Test = ONE task. Never separate.
> EVERY task MUST have: Agent Profile + Parallelization + QA Scenarios.

- [ ] 1. Repair repo reality and reproducible build baseline

  **What to do**: Make the current scaffold truthfully buildable before any epic closure. Fix `bootstrap` image packaging, replace nonexistent bind-mount assumptions with checked-in config assets, pin all container images to explicit versions, and align ClickHouse health probing with the repo's HTTP-only architecture.
  **Must NOT do**: Do not add new infrastructure or hide current build failures behind documentation-only notes.

  **Recommended Agent Profile**:
  - Category: `deep` - Reason: touches Docker, bootstrap packaging, repo reproducibility, and baseline runtime contracts.
  - Skills: `[]` - No special skill is required beyond repo and container inspection.
  - Omitted: [`playwright`] - No browser work is needed for baseline backend repair.

  **Parallelization**: Can Parallel: NO | Wave 1 | Blocks: [2, 3, 19] | Blocked By: []

  **References**:
  - Pattern: `build/bootstrap.Dockerfile:3` - Current build stage only copies `go.mod` and `cmd`, which is insufficient for `bootstrap`.
  - Pattern: `build/bootstrap.Dockerfile:9` - Only `migrations` are copied into the runtime image today; `seed` is missing.
  - API/Type: `cmd/bootstrap/main.go:16` - `bootstrap` imports `internal/migrate` and therefore requires `internal/` in the build context.
  - API/Type: `cmd/bootstrap/main.go:45` - `bootstrap` expects a source seed file at runtime.
  - Pattern: `docker-compose.yml:7` - ClickHouse image is unpinned today.
  - Pattern: `docker-compose.yml:13` - Compose references missing `infra/clickhouse` host paths.
  - Pattern: `docker-compose.yml:16` - Current healthcheck uses native-client assumptions instead of the HTTP-first baseline.

  **Acceptance Criteria** (agent-executable only):
  - [ ] `docker compose config` exits `0`.
  - [ ] `docker compose build bootstrap api control-plane worker-fetch worker-parse renderer` exits `0`.
  - [ ] `docker compose up -d clickhouse` starts ClickHouse and `curl -fsS http://localhost:8123/ping` returns `Ok.`.
  - [ ] `grep -n ':latest' docker-compose.yml` returns no matches.

  **QA Scenarios** (MANDATORY - task incomplete without these):
  ```
  Scenario: Clean baseline build
    Tool: Bash
    Steps: Run `docker compose config`; run `docker compose build bootstrap api control-plane worker-fetch worker-parse renderer`.
    Expected: Both commands exit 0 and bootstrap no longer fails on missing `internal/` or `seed` assets.
    Evidence: .sisyphus/evidence/task-1-repo-reality.txt

  Scenario: Version-pin and healthcheck policy
    Tool: Bash
    Steps: Run `grep -n ':latest' docker-compose.yml || true`; run `docker compose up -d clickhouse`; run `curl -fsS http://localhost:8123/ping`.
    Expected: No `:latest` matches; ClickHouse responds over HTTP with `Ok.`.
    Evidence: .sisyphus/evidence/task-1-repo-reality-policy.txt
  ```

  **Commit**: YES | Message: `fix(infra): make scaffold reproducible and buildable` | Files: [`docker-compose.yml`, `build/bootstrap.Dockerfile`, `build/*.Dockerfile`, `infra/clickhouse/*`]

- [ ] 2. Freeze migration, readiness, and internal job execution contracts

  **What to do**: Decide and implement one migration-authoring rule, one readiness contract, and one deterministic internal job invocation pattern. Replace the naive SQL splitter or constrain migration files to a parser-safe format with tests, make `/v1/ready` reflect actual bootstrap/job state, define `control-plane run-once --job <job-name>` as the internal execution interface, and make `meta.schema_migrations` have one authoritative owner.
  **Must NOT do**: Do not leave unconditional readiness, dual migration ownership, or ad hoc per-job CLIs.

  **Recommended Agent Profile**:
  - Category: `deep` - Reason: this task freezes execution semantics used by every later task.
  - Skills: `[]` - The work is repo-local and contract-driven.
  - Omitted: [`playwright`] - Backend contract work only.

  **Parallelization**: Can Parallel: NO | Wave 1 | Blocks: [3, 4, 6, 10, 19] | Blocked By: [1]

  **References**:
  - Pattern: `internal/migrate/split.go:5` - Current migration parsing is naive semicolon splitting.
  - Test: `internal/migrate/split_test.go:5` - Existing migration parser test is minimal and should be expanded.
  - API/Type: `internal/migrate/http_runner.go:21` - `HTTPRunner` currently creates `meta.schema_migrations` directly.
  - Pattern: `migrations/clickhouse/0001_init.sql:7` - The same table is also created in SQL today.
  - Pattern: `cmd/bootstrap/main.go:58` - Bootstrap writes a readiness marker.
  - Pattern: `cmd/api/main.go:25` - `/v1/ready` currently returns `true` unconditionally.

  **Acceptance Criteria** (agent-executable only):
  - [ ] `go test ./internal/migrate -run TestSplitStatements` exits `0` with cases covering comments, blank statements, and semicolons inside valid ClickHouse SQL bodies or the new migration rule rejects unsupported forms.
  - [ ] `go test ./cmd/api -run TestReady` exits `0` and proves readiness is false before bootstrap completion and true after completion.
  - [ ] `docker compose run --rm control-plane run-once --help` exits `0` and documents the stable internal job contract.
  - [ ] A fresh bootstrap run creates exactly one authoritative `meta.schema_migrations` table definition path.

  **QA Scenarios** (MANDATORY - task incomplete without these):
  ```
  Scenario: Readiness follows bootstrap state
    Tool: Bash
    Steps: Start `clickhouse` and `api` without a bootstrap-ready marker; call `curl -fsS http://localhost:8080/v1/ready`; run bootstrap; call the endpoint again.
    Expected: First response is not ready; second response is ready only after bootstrap succeeds.
    Evidence: .sisyphus/evidence/task-2-readiness.txt

  Scenario: Complex migration edge case
    Tool: Bash
    Steps: Run the migration parser unit suite including a fixture with semicolons/comments; if the repo adopts single-statement-per-file, run the linter/test that rejects a multi-statement invalid fixture.
    Expected: The suite exits 0 and prevents ambiguous SQL parsing from reaching runtime.
    Evidence: .sisyphus/evidence/task-2-migration-parser.txt
  ```

  **Commit**: YES | Message: `fix(contracts): freeze migration and readiness semantics` | Files: [`internal/migrate/*`, `cmd/api/*`, `cmd/control-plane/*`, `migrations/clickhouse/*`]

- [ ] 3. Complete Compose/bootstrap topology, buckets, RBAC, smoke runner, and backup hooks

  **What to do**: Finish E0 as install-time substrate only. Bootstrap must create MinIO buckets (`raw`, `stage`, `backup`), initialize ClickHouse users/roles/databases, apply migrations, seed registries, expose a `verify` mode for smoke assertions, and register backup/restore hooks and manifests without pulling heavy data loads into install time.
  **Must NOT do**: Do not hide place downloads, dictionary builds, or long-running backfills inside the default bootstrap path.

  **Recommended Agent Profile**:
  - Category: `unspecified-high` - Reason: mixed Go, Compose, ClickHouse, and MinIO work with careful idempotency requirements.
  - Skills: `[]` - Standard repo tooling is sufficient.
  - Omitted: [`playwright`] - No browser automation required.

  **Parallelization**: Can Parallel: PARTIAL | Wave 1 | Blocks: [7, 10, 11, 19, 26] | Blocked By: [1, 2]

  **References**:
  - Pattern: `cmd/bootstrap/main.go:41` - Existing bootstrap orchestration entrypoint.
  - Pattern: `docker-compose.yml:30` - Bootstrap service wiring in Compose.
  - API/Type: `internal/migrate/http_runner.go:55` - Existing SQL application path.
  - Pattern: `README.md:13` - One-command developer entrypoint that this task must keep valid.
  - External: `https://clickhouse.com/docs/operations/backup/overview` - Backup and restore primitives to standardize.

  **Acceptance Criteria** (agent-executable only):
  - [ ] `docker compose up -d clickhouse minio` followed by `docker compose run --rm bootstrap` exits `0`.
  - [ ] `docker compose run --rm bootstrap verify` exits `0` and checks buckets, users/roles, databases, migrations, and seed load.
  - [ ] `curl -fsS 'http://localhost:8123/?query=SELECT%20count()%20FROM%20meta.schema_migrations%20FORMAT%20TabSeparated'` returns a value greater than `0`.
  - [ ] A second `docker compose run --rm bootstrap` also exits `0` without duplicate seed rows or duplicate schema objects.

  **QA Scenarios** (MANDATORY - task incomplete without these):
  ```
  Scenario: Fresh install bootstrap
    Tool: Bash
    Steps: Run `docker compose up -d clickhouse minio`; run `docker compose run --rm bootstrap`; run `docker compose run --rm bootstrap verify`.
    Expected: All commands exit 0; verification confirms buckets `raw`, `stage`, and `backup`, plus schema and seed readiness.
    Evidence: .sisyphus/evidence/task-3-bootstrap-fresh.txt

  Scenario: Idempotent rerun
    Tool: Bash
    Steps: Run `docker compose run --rm bootstrap` a second time; query `meta.source_registry` and `meta.schema_migrations` counts before and after.
    Expected: The rerun exits 0; seed rows are not duplicated; migration rows only grow when new migrations exist.
    Evidence: .sisyphus/evidence/task-3-bootstrap-rerun.txt
  ```

  **Commit**: YES | Message: `feat(bootstrap): finish install-time initialization and smoke checks` | Files: [`cmd/bootstrap/*`, `docker-compose.yml`, `build/bootstrap.Dockerfile`, `infra/*`, `migrations/clickhouse/*`]

- [ ] 4. Expand meta registries and schema standards

  **What to do**: Add the remaining `meta` registries (`parser_registry`, `metric_registry`, `api_schema_registry`) and freeze naming, timestamp, versioning, JSON, partitioning, ordering-key, and compatibility conventions. Capture these standards in migration files plus a repo-local ADR or schema standard document used by later tasks.
  **Must NOT do**: Do not let later tasks invent table conventions ad hoc.

  **Recommended Agent Profile**:
  - Category: `unspecified-high` - Reason: mostly schema design with direct impact on all later migrations and API compatibility.
  - Skills: `[]` - No special skill required.
  - Omitted: [`playwright`] - Backend schema task.

  **Parallelization**: Can Parallel: YES | Wave 1 | Blocks: [5, 6, 7, 13] | Blocked By: [2]

  **References**:
  - Pattern: `migrations/clickhouse/0002_core_tables.sql:1` - Current `meta.source_registry` style and `gold.api_v1_sources` view pattern.
  - Pattern: `migrations/clickhouse/0001_init.sql:1` - Logical database creation baseline.
  - API/Type: `cmd/api/main.go:54` - Existing response envelope fields to preserve in compatibility planning.
  - External: `https://clickhouse.com/docs/sql-reference/data-types/newjson` - JSON type guidance for evolving fields.
  - External: `https://clickhouse.com/docs/engines/table-engines/mergetree-family/mergetree` - Baseline MergeTree design guidance.

  **Acceptance Criteria** (agent-executable only):
  - [ ] After bootstrap, ClickHouse queries confirm `meta.parser_registry`, `meta.metric_registry`, and `meta.api_schema_registry` exist.
  - [ ] Registry tables use explicit engine choices and version/timestamp fields consistent with the chosen standard.
  - [ ] Schema-standard tests or lint checks exit `0` and prove new migrations follow the declared naming/order rules.
  - [ ] `go test ./...` exits `0` after the schema additions.

  **QA Scenarios** (MANDATORY - task incomplete without these):
  ```
  Scenario: Meta registries present
    Tool: Bash
    Steps: Run bootstrap; query `system.tables` for `meta.parser_registry`, `meta.metric_registry`, and `meta.api_schema_registry`.
    Expected: All three tables exist exactly once with the expected engines.
    Evidence: .sisyphus/evidence/task-4-meta-registries.txt

  Scenario: Convention guardrail
    Tool: Bash
    Steps: Run the schema convention test/lint suite against migrations.
    Expected: The suite exits 0 and rejects migrations that violate naming, versioning, or partition/order conventions.
    Evidence: .sisyphus/evidence/task-4-schema-standards.txt
  ```

  **Commit**: YES | Message: `feat(schema): add meta registries and storage standards` | Files: [`migrations/clickhouse/*`, `docs/*`, `seed/*`]

- [ ] 5. Expand ops/bronze/silver/gold baseline tables and performance conventions

  **What to do**: Add the remaining baseline tables required for the full platform: `ops.parse_log`, `ops.unresolved_location_queue`, `ops.quality_incident`, `bronze.raw_structured_row`, `silver.dim_place`, `silver.place_polygon`, `silver.place_hierarchy`, `silver.dim_entity`, `silver.entity_alias`, `silver.fact_observation`, `silver.fact_event`, `silver.fact_track_point`, `silver.fact_track_segment`, bridge tables, `silver.metric_contribution`, `gold.metric_state`, `gold.metric_snapshot`, `gold.hotspot_snapshot`, and compatibility views scaffolding. Bake in monthly partitions, order keys, low-cardinality columns, TTL, and codec/projection defaults where justified.
  **Must NOT do**: Do not wait for business logic tasks to define physical storage shape or misuse projections as ETL logic.

  **Recommended Agent Profile**:
  - Category: `unspecified-high` - Reason: large schema expansion with engine and performance decisions.
  - Skills: `[]` - ClickHouse schema work is repo-local.
  - Omitted: [`playwright`] - No browser surface.

  **Parallelization**: Can Parallel: PARTIAL | Wave 1 | Blocks: [8, 16, 18, 26] | Blocked By: [4]

  **References**:
  - Pattern: `migrations/clickhouse/0003_ops_bronze.sql:15` - Existing frontier/fetch table style.
  - Pattern: `migrations/clickhouse/0002_core_tables.sql:26` - Current `ReplacingMergeTree(version)` convention.
  - External: `https://clickhouse.com/docs/engines/table-engines/mergetree-family/replacingmergetree` - Correct use of `ReplacingMergeTree`.
  - External: `https://clickhouse.com/docs/engines/table-engines/mergetree-family/aggregatingmergetree` - Aggregate-state table guidance.
  - External: `https://clickhouse.com/docs/guides/developer/ttl` - TTL timing and partition-alignment guardrails.

  **Acceptance Criteria** (agent-executable only):
  - [ ] After bootstrap, `system.tables` contains every planned baseline table for `ops`, `bronze`, `silver`, and `gold`.
  - [ ] `gold.metric_state` uses `AggregatingMergeTree` and `silver` dimensions/facts use the planned engines.
  - [ ] `SHOW CREATE TABLE` output proves monthly partitioning and explicit `ORDER BY` on large append tables.
  - [ ] Schema tests exit `0` and no table is left on default engine settings.

  **QA Scenarios** (MANDATORY - task incomplete without these):
  ```
  Scenario: Baseline table inventory
    Tool: Bash
    Steps: Run bootstrap; query `system.tables` for the required `ops`, `bronze`, `silver`, and `gold` tables.
    Expected: All planned baseline tables exist exactly once.
    Evidence: .sisyphus/evidence/task-5-table-inventory.txt

  Scenario: Engine and lifecycle policy
    Tool: Bash
    Steps: Run `SHOW CREATE TABLE` for `silver.dim_place`, `silver.fact_observation`, and `gold.metric_state`.
    Expected: Engines, partitions, order keys, and TTL clauses match the plan and there is no misuse of projections or `ReplacingMergeTree` semantics.
    Evidence: .sisyphus/evidence/task-5-storage-policy.txt
  ```

  **Commit**: YES | Message: `feat(storage): add full baseline ClickHouse tables` | Files: [`migrations/clickhouse/*`]

- [ ] 6. Freeze `source_registry` governance schema and seed evolution behavior

  **What to do**: Make `meta.source_registry` decision-complete for crawl governance. Add fields for rate limiting, retention class, kill-switch reason/state, review state, auth configuration mode, backfill policy, license/terms provenance, and parser routing; define seed versioning and update semantics so registry evolution is idempotent and auditable instead of insert-only.
  **Must NOT do**: Do not keep governance-critical fields only in JSON seeds or docs.

  **Recommended Agent Profile**:
  - Category: `deep` - Reason: this is the policy backbone for discovery, fetch, retention, and legal controls.
  - Skills: `[]` - Standard repo work only.
  - Omitted: [`playwright`] - No UI or browser workflow.

  **Parallelization**: Can Parallel: YES | Wave 2 | Blocks: [10, 11, 16] | Blocked By: [2, 4]

  **References**:
  - Pattern: `seed/source_registry.json:11` - Seed already contains `rate_limit`, which is not modeled in SQL today.
  - Pattern: `cmd/bootstrap/main.go:108` - Seed loading is currently insert-only.
  - Pattern: `migrations/clickhouse/0002_core_tables.sql:1` - Existing `source_registry` schema baseline.
  - External: `https://www.rfc-editor.org/rfc/rfc9309.html` - Robots behavior that the registry must govern.
  - External: `https://www.geoboundaries.org/api.html` - Example of dataset/version/license provenance fields that must be first-class.

  **Acceptance Criteria** (agent-executable only):
  - [ ] `DESCRIBE TABLE meta.source_registry` includes explicit governance columns for rate limit, review state, retention class, kill switch, and auth/refresh policy.
  - [ ] Re-running bootstrap with an updated seed row updates versioned registry state instead of creating uncontrolled duplicates.
  - [ ] `gold.api_v1_sources` or its successor compatibility view still serves additive, backward-compatible source metadata.
  - [ ] Registry tests exit `0` for create, update, disable, and re-enable workflows.

  **QA Scenarios** (MANDATORY - task incomplete without these):
  ```
  Scenario: Seed update idempotency
    Tool: Bash
    Steps: Apply bootstrap with seed version A; change one governance field in the seed fixture; rerun bootstrap; query the source row history/current version.
    Expected: Current-state row updates as designed, version history is preserved, and uncontrolled duplicates are not created.
    Evidence: .sisyphus/evidence/task-6-source-registry-update.txt

  Scenario: Kill-switch edge case
    Tool: Bash
    Steps: Mark a fixture source disabled in the registry; run the discovery/fetch job for that source.
    Expected: The job refuses to run the disabled source and records an auditable skip/deny reason.
    Evidence: .sisyphus/evidence/task-6-source-registry-killswitch.txt
  ```

  **Commit**: YES | Message: `feat(governance): freeze source registry semantics` | Files: [`migrations/clickhouse/*`, `seed/source_registry.json`, `cmd/bootstrap/*`, `cmd/api/*`]

- [ ] 7. Implement place dataset acquisition and staging jobs

  **What to do**: Implement deterministic control-plane jobs for `geoBoundaries gbOpen` and GeoNames acquisition. Each job must download, checksum, stage to MinIO, write provenance into ClickHouse job logs, support reruns, and separate raw acquisition from later place-graph materialization.
  **Must NOT do**: Do not load global place data during default bootstrap or depend on undocumented manual downloads.

  **Recommended Agent Profile**:
  - Category: `deep` - Reason: combines external dataset handling, job semantics, provenance, and staging rules.
  - Skills: `[]` - No special skill needed.
  - Omitted: [`playwright`] - Dataset ingestion is non-browser.

  **Parallelization**: Can Parallel: YES | Wave 2 | Blocks: [8] | Blocked By: [3, 4]

  **References**:
  - Pattern: `cmd/control-plane/main.go:8` - Current control-plane is a stub and should become the job orchestrator.
  - Pattern: `ops.job_run` in `migrations/clickhouse/0003_ops_bronze.sql:1` - Existing job-log table to extend.
  - External: `https://www.geoboundaries.org/api.html` - Boundary dataset source and metadata.
  - External: `https://download.geonames.org/export/dump/readme.txt` - GeoNames dump structure and daily-update model.

  **Acceptance Criteria** (agent-executable only):
  - [ ] `docker compose run --rm control-plane run-once --job geoboundaries-sync` exits `0` and records a successful `ops.job_run` row.
  - [ ] `docker compose run --rm control-plane run-once --job geonames-sync` exits `0` and records a successful `ops.job_run` row.
  - [ ] Staged dataset manifests/checksums are queryable from ClickHouse or the job output contract.
  - [ ] Rerunning either job does not re-download unchanged artifacts unnecessarily.

  **QA Scenarios** (MANDATORY - task incomplete without these):
  ```
  Scenario: Dataset staging happy path
    Tool: Bash
    Steps: Run `control-plane run-once --job geoboundaries-sync`; run `control-plane run-once --job geonames-sync`; query `ops.job_run`.
    Expected: Both jobs succeed, record provenance/checksum metadata, and stage artifacts for later materialization.
    Evidence: .sisyphus/evidence/task-7-place-sync.txt

  Scenario: Rerun/no-op edge case
    Tool: Bash
    Steps: Run each dataset sync job twice against the same remote version.
    Expected: The second run exits 0 and reports no-op or cache-hit behavior instead of duplicating staged artifacts.
    Evidence: .sisyphus/evidence/task-7-place-sync-rerun.txt
  ```

  **Commit**: YES | Message: `feat(place): add dataset staging jobs for boundaries and gazetteers` | Files: [`cmd/control-plane/*`, `cmd/bootstrap/*`, `migrations/clickhouse/*`]

- [ ] 8. Build place graph tables, internal IDs, hierarchy, and polygon dictionary

  **What to do**: Materialize the staged place datasets into `silver.dim_place`, `silver.place_hierarchy`, and `silver.place_polygon`. Generate internal place IDs, world/continent pseudo-places, parent chains, alternate names, centroid/bbox fields, H3 coverage, deepest admin level, and the reverse-geocoding polygon dictionary.
  **Must NOT do**: Do not depend on a single upstream keyspace or fabricate missing lower admin levels.

  **Recommended Agent Profile**:
  - Category: `deep` - Reason: complex geospatial modeling with ClickHouse dictionary design and place-identity decisions.
  - Skills: `[]` - Standard repo tools are enough.
  - Omitted: [`playwright`] - Non-UI geospatial work.

  **Parallelization**: Can Parallel: NO | Wave 2 | Blocks: [9, 14] | Blocked By: [5, 7]

  **References**:
  - Pattern: `migrations/clickhouse/0002_core_tables.sql:29` - Existing serving-view style to follow later for place views.
  - External: `https://www.geoboundaries.org/index.html` - `gbOpen` source and license track.
  - External: `https://www.geonames.org/export/codes.html` - GeoNames admin and feature-code semantics.
  - External: `https://clickhouse.com/docs/sql-reference/statements/create/dictionary/layouts/polygon` - Polygon dictionary layout guidance.

  **Acceptance Criteria** (agent-executable only):
  - [ ] `docker compose run --rm control-plane run-once --job place-build` exits `0`.
  - [ ] `silver.dim_place`, `silver.place_hierarchy`, and `silver.place_polygon` contain rows after the build job.
  - [ ] Reverse-geocoding fixtures resolve test coordinates to the deepest available admin level where source boundaries exist.
  - [ ] Countries without ADM3/ADM4 coverage retain nullable lower-level columns and correct `deepest_admin_level` values.

  **QA Scenarios** (MANDATORY - task incomplete without these):
  ```
  Scenario: Place build happy path
    Tool: Bash
    Steps: Run `control-plane run-once --job place-build`; query counts from `silver.dim_place`, `silver.place_hierarchy`, and `silver.place_polygon`.
    Expected: All three tables are populated and world/continent pseudo-places exist.
    Evidence: .sisyphus/evidence/task-8-place-build.txt

  Scenario: Incomplete admin-depth edge case
    Tool: Bash
    Steps: Run the place fixture suite against a country/fixture lacking ADM3 or ADM4 coverage.
    Expected: Lower admin IDs remain null, `deepest_admin_level` reflects the actual maximum depth, and no fabricated IDs appear.
    Evidence: .sisyphus/evidence/task-8-place-depth.txt
  ```

  **Commit**: YES | Message: `feat(place): materialize global place graph and reverse geocoder` | Files: [`migrations/clickhouse/*`, `cmd/control-plane/*`, `internal/*`, `seed/*`]

- [ ] 9. Add place validation fixtures, coverage reports, and reverse-geocode benchmarks

  **What to do**: Add deterministic fixture suites and reporting for the place system. Create fixture coordinates, ambiguous-name cases, missing-depth cases, overlap checks, and per-country coverage reporting; benchmark reverse-geocode latency and publish coverage/quality artifacts for release gates.
  **Must NOT do**: Do not rely on ad hoc manual spot checks for place correctness.

  **Recommended Agent Profile**:
  - Category: `unspecified-high` - Reason: test-heavy validation work with some geospatial reporting.
  - Skills: `[]` - Standard repo work only.
  - Omitted: [`playwright`] - No browser involvement.

  **Parallelization**: Can Parallel: YES | Wave 2 | Blocks: [14, 19] | Blocked By: [8]

  **References**:
  - Pattern: `cmd/api/main_test.go:9` - Existing lightweight Go test style to expand from.
  - Test: `internal/migrate/split_test.go:5` - Example of colocated stdlib test placement.
  - External: `https://clickhouse.com/docs/sql-reference/statements/create/dictionary/layouts/polygon` - Reverse-geocode dictionary behavior to validate.
  - External: `https://download.geonames.org/export/dump/readme.txt` - GeoNames hierarchy/data caveats that fixtures must encode.

  **Acceptance Criteria** (agent-executable only):
  - [ ] `go test ./... -run TestReverseGeocodeFixtures` exits `0`.
  - [ ] `go test ./... -run TestPlaceCoverageReport` exits `0` and writes a machine-readable coverage artifact.
  - [ ] `go test ./... -run ^$ -bench BenchmarkReverseGeocode` exits `0` and records benchmark output.
  - [ ] Fixture cases cover ambiguous names, missing ADM depth, and overlapping/invalid polygons.

  **QA Scenarios** (MANDATORY - task incomplete without these):
  ```
  Scenario: Coverage and correctness suite
    Tool: Bash
    Steps: Run `go test ./... -run TestReverseGeocodeFixtures`; run `go test ./... -run TestPlaceCoverageReport`.
    Expected: Both suites exit 0 and the coverage artifact shows the expected per-country depth availability.
    Evidence: .sisyphus/evidence/task-9-place-validation.txt

  Scenario: Benchmark and overlap edge case
    Tool: Bash
    Steps: Run `go test ./... -run ^$ -bench BenchmarkReverseGeocode`; run the overlap/invalid-geometry fixture suite.
    Expected: Benchmarks complete and invalid/overlap cases are detected or handled deterministically.
    Evidence: .sisyphus/evidence/task-9-place-benchmark.txt
  ```

  **Commit**: YES | Message: `test(place): add coverage fixtures and reverse-geocode benchmarks` | Files: [`internal/*`, `testdata/*`, `docs/*`]

- [ ] 10. Implement discovery engine and frontier ranking

  **What to do**: Build the discovery pipeline that turns registry seeds into normalized frontier rows. Implement exact RFC 9309 robots handling, sitemap parsing (including gzip and indexes), RSS/Atom discovery and parsing, URL normalization, duplicate suppression, host-level policy enforcement, and baseline frontier ranking for freshness, source quality, and diversity.
  **Must NOT do**: Do not start with generic deep spidering, Common Crawl, or Wayback replay in the live path.

  **Recommended Agent Profile**:
  - Category: `deep` - Reason: combines network policy, discovery semantics, and frontier modeling.
  - Skills: `[]` - Repo exploration and standard libraries are sufficient.
  - Omitted: [`playwright`] - Browser rendering is explicitly deferred.

  **Parallelization**: Can Parallel: PARTIAL | Wave 3 | Blocks: [11, 15, 20] | Blocked By: [3, 6]

  **References**:
  - Pattern: `cmd/control-plane/main.go:8` - Control-plane will own scheduled discovery orchestration.
  - Pattern: `migrations/clickhouse/0003_ops_bronze.sql:15` - `ops.crawl_frontier` baseline to extend.
  - External: `https://www.rfc-editor.org/rfc/rfc9309.html` - Robots redirect/cache/error semantics.
  - External: `https://www.sitemaps.org/protocol.html` - Sitemap limits, gzip/index behavior, and same-host rules.
  - External: `https://www.rfc-editor.org/rfc/rfc4287.html` - Atom feed semantics.

  **Acceptance Criteria** (agent-executable only):
  - [ ] `go test ./... -run TestDiscoveryRobotsPolicy` exits `0` and proves correct handling of `4xx`, `5xx`, redirects, `Allow`, and `Disallow` precedence.
  - [ ] `go test ./... -run TestDiscoverySitemapsAndFeeds` exits `0` for gzip sitemaps, sitemap indexes, RSS, and Atom fixtures.
  - [ ] `docker compose run --rm control-plane run-once --job discovery-seed --source-id fixture:site` exits `0` and writes deduplicated rows into `ops.crawl_frontier`.
  - [ ] Frontier ranking produces deterministic ordering for fixture URLs with different freshness/priority scores.

  **QA Scenarios** (MANDATORY - task incomplete without these):
  ```
  Scenario: Discovery happy path
    Tool: Bash
    Steps: Run the discovery integration suite against the local fixture site; query `ops.crawl_frontier` for discovered canonical URLs.
    Expected: Expected URLs are present once each, normalized, and ranked deterministically.
    Evidence: .sisyphus/evidence/task-10-discovery.txt

  Scenario: Robots failure-policy edge case
    Tool: Bash
    Steps: Run the robots policy suite with fixtures for 4xx unavailable, 5xx unreachable, redirects, and conflicting allow/disallow paths.
    Expected: Behavior matches RFC 9309 exactly and denied URLs do not enter the frontier.
    Evidence: .sisyphus/evidence/task-10-robots.txt
  ```

  **Commit**: YES | Message: `feat(discovery): implement robots sitemap and feed frontiering` | Files: [`cmd/control-plane/*`, `internal/*`, `migrations/clickhouse/*`, `testdata/*`]

- [ ] 11. Implement fetch worker and raw retention/replay policy

  **What to do**: Build `worker-fetch` into the public-source fetch runtime. Support GET/HEAD, conditional fetch, retry/backoff, gzip/br handling, MIME sniffing, max-size guardrails, provenance capture, content hashing, large-body object-store persistence, and retention/replay classes by source policy.
  **Must NOT do**: Do not fetch disabled sources, bypass auth walls, or write giant bodies directly into ClickHouse rows.

  **Recommended Agent Profile**:
  - Category: `deep` - Reason: network runtime, provenance, object storage, and retention semantics are all coupled here.
  - Skills: `[]` - No special skill required.
  - Omitted: [`playwright`] - Fetch runtime is HTTP/file-oriented.

  **Parallelization**: Can Parallel: PARTIAL | Wave 3 | Blocks: [12, 15, 20, 22, 23, 24, 25] | Blocked By: [3, 6, 10]

  **References**:
  - Pattern: `cmd/worker-fetch/main.go:8` - Current fetch worker stub to replace.
  - Pattern: `migrations/clickhouse/0003_ops_bronze.sql:32` - `ops.fetch_log` baseline.
  - Pattern: `migrations/clickhouse/0003_ops_bronze.sql:48` - `bronze.raw_document` baseline.
  - External: `https://clickhouse.com/docs/optimize/asynchronous-inserts` - Async insert guardrails for fragmented writers.
  - External: `https://clickhouse.com/docs/integrations/s3` - Staging and object-storage integration patterns.

  **Acceptance Criteria** (agent-executable only):
  - [ ] `go test ./... -run TestFetchWorker` exits `0` for conditional GET, compression, retries, and size-limit fixtures.
  - [ ] `docker compose run --rm control-plane run-once --job fetch-frontier --source-id fixture:site` exits `0` and records `ops.fetch_log` plus `bronze.raw_document` rows.
  - [ ] Large fixture bodies are stored in object storage with `object_key` references while tiny bodies follow the chosen inline policy.
  - [ ] Retention/replay metadata is persisted and replay mode can re-emit stored payloads without refetching live content.

  **QA Scenarios** (MANDATORY - task incomplete without these):
  ```
  Scenario: Fetch and raw retention happy path
    Tool: Bash
    Steps: Run the fetch integration job against fixture URLs; query `ops.fetch_log` and `bronze.raw_document`.
    Expected: Successful fetch rows, content hashes, and object-store references are recorded.
    Evidence: .sisyphus/evidence/task-11-fetch.txt

  Scenario: Disabled-source and max-size edge case
    Tool: Bash
    Steps: Run fetch against a disabled fixture source and against an oversized body fixture.
    Expected: Disabled source is skipped with an auditable reason; oversized body is rejected or truncated according to policy without crashing the worker.
    Evidence: .sisyphus/evidence/task-11-fetch-edge.txt
  ```

  **Commit**: YES | Message: `feat(fetch): add public-source fetch runtime and raw retention` | Files: [`cmd/worker-fetch/*`, `cmd/control-plane/*`, `migrations/clickhouse/*`, `internal/*`, `testdata/*`]

- [ ] 12. Implement parser framework and structured parsers/extractors

  **What to do**: Turn `worker-parse` into the parser runtime. Define a versioned parser interface plus contracts for raw input, canonical candidate output, and parser errors; implement JSON, CSV/TSV, XML, RSS, Atom, and WARC-capable structured parsing first, with HTML profile extraction second and browser-rendered extraction left disabled until explicitly required by a pack.
  **Must NOT do**: Do not build one-off parsers without registry entries or silently swallow parse failures.

  **Recommended Agent Profile**:
  - Category: `deep` - Reason: this task freezes parse contracts used by every adapter and domain pack.
  - Skills: `[]` - Standard repo and test tooling is enough.
  - Omitted: [`playwright`] - Browser-rendered extraction remains deferred.

  **Parallelization**: Can Parallel: PARTIAL | Wave 3 | Blocks: [13, 15, 20, 22, 23, 24, 25] | Blocked By: [11]

  **References**:
  - Pattern: `cmd/worker-parse/main.go:8` - Current parser worker stub to replace.
  - Pattern: `migrations/clickhouse/0002_core_tables.sql:1` - Registry-driven style to mirror for `parser_registry` usage.
  - External: `https://www.rssboard.org/rss-specification` - RSS parsing and GUID semantics.
  - External: `https://www.rfc-editor.org/rfc/rfc4287.html` - Atom feed schema requirements.
  - External: `https://commoncrawl.org/get-started` - WARC/WAT/WET context for later corpus parsing.

  **Acceptance Criteria** (agent-executable only):
  - [ ] `go test ./... -run TestParserRegistry` exits `0` and proves parser lookup/version routing works.
  - [ ] `go test ./... -run TestStructuredParsers` exits `0` for JSON, CSV/TSV, XML, RSS, and Atom fixtures.
  - [ ] `docker compose run --rm control-plane run-once --job parse-raw --source-id fixture:site` exits `0` and writes parse logs plus canonical candidates.
  - [ ] Parser failures record machine-readable error codes/messages in `ops.parse_log`.

  **QA Scenarios** (MANDATORY - task incomplete without these):
  ```
  Scenario: Structured parser happy path
    Tool: Bash
    Steps: Run the structured parser suite; execute `control-plane run-once --job parse-raw --source-id fixture:site`; query parse logs.
    Expected: Supported fixture formats parse successfully and route through the declared parser registry entries.
    Evidence: .sisyphus/evidence/task-12-parse.txt

  Scenario: Schema-drift/error edge case
    Tool: Bash
    Steps: Feed an invalid or drifted fixture payload through the parser job.
    Expected: The parser logs a deterministic error contract, emits no invalid promoted candidates, and leaves the raw document available for replay.
    Evidence: .sisyphus/evidence/task-12-parse-edge.txt
  ```

  **Commit**: YES | Message: `feat(parse): add parser framework and structured format support` | Files: [`cmd/worker-parse/*`, `cmd/control-plane/*`, `migrations/clickhouse/*`, `internal/*`, `testdata/*`]

- [ ] 13. Implement canonical envelopes, IDs, evidence, and schema-version contracts

  **What to do**: Finalize the stable canonical schemas for observations, events, entities, tracks, evidence, and metric contributions. Define deterministic ID generation, source-native ID retention, content-hash fallback rules, evidence payload structure, parser/version provenance, and `schema_version` / `record_version` semantics.
  **Must NOT do**: Do not let domain packs invent incompatible envelopes or opaque IDs.

  **Recommended Agent Profile**:
  - Category: `deep` - Reason: this task locks the cross-domain record contract for the whole system.
  - Skills: `[]` - Repo-local schema and test work.
  - Omitted: [`playwright`] - No browser involvement.

  **Parallelization**: Can Parallel: NO | Wave 3 | Blocks: [14, 15, 17, 18, 20, 22, 23, 24, 25] | Blocked By: [4, 12]

  **References**:
  - Pattern: `cmd/api/main.go:54` - Existing API envelope fields that canonical facts must align with downstream.
  - Pattern: `migrations/clickhouse/0003_ops_bronze.sql:48` - Raw document provenance fields to preserve.
  - External: `https://clickhouse.com/docs/sql-reference/data-types/newjson` - JSON strategy for `attrs` and `evidence`.
  - External: `docs/comprehensive_delivery_plan.md:829` - Existing plan intent for stable envelopes and dynamic attrs.

  **Acceptance Criteria** (agent-executable only):
  - [ ] `go test ./... -run TestCanonicalIDs` exits `0` and proves deterministic ID generation and source-native retention.
  - [ ] `go test ./... -run TestCanonicalEnvelope` exits `0` and proves every canonical record type carries required fields.
  - [ ] Parser integration tests emit canonical candidates with `schema_version`, `record_version`, evidence payloads, and raw references.
  - [ ] Compatibility fixtures exist for additive schema evolution without breaking prior consumers.

  **QA Scenarios** (MANDATORY - task incomplete without these):
  ```
  Scenario: Canonical contract happy path
    Tool: Bash
    Steps: Run `go test ./... -run TestCanonicalEnvelope`; inspect fixture outputs or query candidate tables.
    Expected: Observation, event, entity, track, and evidence contracts all include the required stable fields.
    Evidence: .sisyphus/evidence/task-13-canonical.txt

  Scenario: Duplicate-source and schema-version edge case
    Tool: Bash
    Steps: Run `go test ./... -run TestCanonicalIDs`; feed two payloads with the same source-native ID and one additive-field schema change.
    Expected: IDs remain deterministic, source-native IDs are preserved, and additive schema evolution does not break compatibility fixtures.
    Evidence: .sisyphus/evidence/task-13-canonical-edge.txt
  ```

  **Commit**: YES | Message: `feat(canonical): freeze record envelopes ids and evidence contracts` | Files: [`migrations/clickhouse/*`, `internal/*`, `cmd/*`, `testdata/*`]

- [ ] 14. Implement location attribution and unresolved-location workflow

  **What to do**: Implement the mandatory geo-anchor precedence chain for points, polygons, place names, track-derived context, entity-home fallback, and source-jurisdiction fallback. Populate `place_id`, `continent_id`, `admin0_id`-`admin4_id`, `geo_anchor_type`, `geo_method`, `geo_confidence`, and `deepest_admin_level`; route low-confidence or failed records into `ops.unresolved_location_queue` with reprocessing support.
  **Must NOT do**: Do not promote records without a location anchor or fabricate missing place matches.

  **Recommended Agent Profile**:
  - Category: `deep` - Reason: complex geospatial resolution logic with strict product rules.
  - Skills: `[]` - Standard backend and ClickHouse work only.
  - Omitted: [`playwright`] - No UI work.

  **Parallelization**: Can Parallel: PARTIAL | Wave 4 | Blocks: [15, 18, 20, 22, 23, 24, 25] | Blocked By: [8, 9, 13]

  **References**:
  - Pattern: `migrations/clickhouse/0003_ops_bronze.sql:15` - Existing ops-style tables to mirror for unresolved workflow.
  - External: `https://clickhouse.com/docs/sql-reference/statements/create/dictionary/layouts/polygon` - Point-in-polygon lookup mechanism.
  - External: `https://www.geonames.org/export/codes.html` - Place-name/admin-code matching hints.
  - External: `docs/comprehensive_delivery_plan.md:831` - Existing geocoding precedence intent.

  **Acceptance Criteria** (agent-executable only):
  - [ ] `go test ./... -run TestLocationAttribution` exits `0` for explicit coordinates, place names with context, and source-jurisdiction fallback fixtures.
  - [ ] `go test ./... -run TestUnresolvedLocationQueue` exits `0` and proves low-confidence records are queued, not promoted.
  - [ ] Successful attribution populates the full parent chain as deep as available and preserves null lower levels where unavailable.
  - [ ] Reprocessing an unresolved fixture after improved place data or parser logic can promote it successfully.

  **QA Scenarios** (MANDATORY - task incomplete without these):
  ```
  Scenario: Location attribution happy path
    Tool: Bash
    Steps: Run the attribution fixture suite for explicit coordinates and place-name fixtures.
    Expected: Records resolve to the expected `place_id` and deepest available admin chain with recorded method/confidence.
    Evidence: .sisyphus/evidence/task-14-location.txt

  Scenario: Unresolved queue edge case
    Tool: Bash
    Steps: Run the unresolved-location fixture suite with ambiguous or low-confidence records; then rerun after a resolver improvement fixture.
    Expected: Initial records land in `ops.unresolved_location_queue`; reprocessing promotes only records that now clear the threshold.
    Evidence: .sisyphus/evidence/task-14-unresolved.txt
  ```

  **Commit**: YES | Message: `feat(location): add attribution pipeline and unresolved workflow` | Files: [`internal/*`, `cmd/control-plane/*`, `migrations/clickhouse/*`, `testdata/*`]

- [ ] 15. Implement end-to-end promotion pipeline into silver facts

  **What to do**: Connect discovery, fetch, parse, canonicalization, and location attribution into the promotion path that writes only resolved records into `silver` fact tables. Promotion must be idempotent, record provenance, and distinguish observations, events, entities, and tracks according to the canonical schema.
  **Must NOT do**: Do not let unresolved or malformed records leak into public-serving tables.

  **Recommended Agent Profile**:
  - Category: `deep` - Reason: this is the critical end-to-end data path that validates the whole substrate.
  - Skills: `[]` - Standard repo tools are sufficient.
  - Omitted: [`playwright`] - HTTP and DB integration only.

  **Parallelization**: Can Parallel: NO | Wave 4 | Blocks: [16, 17, 18, 20] | Blocked By: [10, 11, 12, 13, 14]

  **References**:
  - Pattern: `cmd/bootstrap/main.go:65` - Existing ordered workflow style for sequential orchestration.
  - Pattern: `migrations/clickhouse/0003_ops_bronze.sql:48` - Raw-document baseline feeding promotion.
  - Pattern: `cmd/api/main.go:28` - Jobs endpoint stub that later becomes a real read path for this pipeline's job telemetry.

  **Acceptance Criteria** (agent-executable only):
  - [ ] `docker compose run --rm control-plane run-once --job promote --source-id fixture:events` exits `0`.
  - [ ] Fixture runs create rows in `silver.fact_observation` and `silver.fact_event` with complete place chains and raw/evidence references.
  - [ ] Re-running the same promotion job is idempotent and does not duplicate current-state records.
  - [ ] Malformed or unresolved fixture records remain in `ops.parse_log` or `ops.unresolved_location_queue`, not `silver` facts.

  **QA Scenarios** (MANDATORY - task incomplete without these):
  ```
  Scenario: End-to-end promotion happy path
    Tool: Bash
    Steps: Run the fixture discovery/fetch/parse/promotion sequence via `control-plane run-once --job promote --source-id fixture:events`; query `silver.fact_observation` and `silver.fact_event`.
    Expected: Canonical silver facts are created with populated geo fields and provenance links.
    Evidence: .sisyphus/evidence/task-15-promotion.txt

  Scenario: Idempotent rerun and unresolved edge case
    Tool: Bash
    Steps: Run the same promotion job twice; run a promotion job for an unresolved fixture source.
    Expected: The first source remains deduplicated on rerun, and unresolved records are diverted out of serving tables.
    Evidence: .sisyphus/evidence/task-15-promotion-edge.txt
  ```

  **Commit**: YES | Message: `feat(promotion): wire canonical records into silver facts` | Files: [`cmd/control-plane/*`, `cmd/worker-fetch/*`, `cmd/worker-parse/*`, `internal/*`, `migrations/clickhouse/*`]

- [ ] 16. Implement core serving views and core API read paths

  **What to do**: Replace the route stubs for the core read surface with real query paths backed by compatibility views and explicit filters/pagination. This task covers `GET /v1/health`, `GET /v1/ready`, `GET /v1/version`, `GET /v1/schema`, `GET /v1/jobs`, `GET /v1/jobs/{jobId}`, `GET /v1/sources`, `GET /v1/sources/{sourceId}`, `GET /v1/places`, `GET /v1/places/{placeId}`, `GET /v1/events`, `GET /v1/events/{eventId}`, `GET /v1/observations`, and `GET /v1/observations/{recordId}`.
  **Must NOT do**: Do not count route skeletons as done; every endpoint must be backed by a real view/query plus contract tests.

  **Recommended Agent Profile**:
  - Category: `unspecified-high` - Reason: combines SQL serving views, Go handlers, pagination, and compatibility contracts.
  - Skills: `[]` - Standard backend work only.
  - Omitted: [`playwright`] - Endpoint verification is HTTP/JSON only.

  **Parallelization**: Can Parallel: PARTIAL | Wave 4 | Blocks: [19, 21] | Blocked By: [5, 6, 15]

  **References**:
  - Pattern: `cmd/api/main.go:17` - Existing route inventory that must be upgraded from stubs.
  - Pattern: `cmd/api/main.go:48` - `listStub()` is the temporary behavior to replace.
  - Pattern: `cmd/api/main.go:54` - Preserve the existing response envelope.
  - Pattern: `migrations/clickhouse/0002_core_tables.sql:29` - Existing `gold.api_v1_sources` compatibility-view pattern.
  - Test: `cmd/api/main_test.go:20` - Existing HTTP handler test pattern.

  **Acceptance Criteria** (agent-executable only):
  - [ ] `go test ./... -run TestAPICoreContracts` exits `0`.
  - [ ] `curl -fsS http://localhost:8080/v1/sources` returns real source rows from ClickHouse rather than `listStub` placeholders.
  - [ ] `curl -fsS http://localhost:8080/v1/places` and `curl -fsS http://localhost:8080/v1/observations` return fixture-backed records with stable envelopes.
  - [ ] Cursor pagination, field filtering, and 404 behavior are covered by integration tests.

  **QA Scenarios** (MANDATORY - task incomplete without these):
  ```
  Scenario: Core API happy path
    Tool: Bash
    Steps: Start the stack; call `/v1/jobs`, `/v1/sources`, `/v1/places`, `/v1/events`, and `/v1/observations` against fixture data.
    Expected: Each endpoint returns a real JSON envelope with data from serving views, not stubbed `kind/items/path` payloads.
    Evidence: .sisyphus/evidence/task-16-api-core.txt

  Scenario: Pagination and not-found edge case
    Tool: Bash
    Steps: Call a paginated list endpoint with cursor/fields params; call a detail endpoint with an unknown ID.
    Expected: Pagination is stable and deterministic; unknown IDs return the defined error contract rather than an empty success stub.
    Evidence: .sisyphus/evidence/task-16-api-core-edge.txt
  ```

  **Commit**: YES | Message: `feat(api): replace core stubs with real serving views` | Files: [`cmd/api/*`, `migrations/clickhouse/*`, `internal/*`, `testdata/*`]

- [ ] 17. Implement deduplication and entity resolution baseline

  **What to do**: Add document, observation, and entity resolution logic. Normalize URLs, hash content, handle replay/live/archive collisions, generate entity candidates from strong identifiers and aliases, score matches into `exact`, `probable`, `possible`, and `unknown`, and materialize current-state entity/alias tables with lineage preserved.
  **Must NOT do**: Do not force ambiguous merges or collapse archive/live duplicates without provenance.

  **Recommended Agent Profile**:
  - Category: `deep` - Reason: cross-table matching and explainable scoring require careful logic.
  - Skills: `[]` - Standard repo tools are sufficient.
  - Omitted: [`playwright`] - No browser surface.

  **Parallelization**: Can Parallel: PARTIAL | Wave 4 | Blocks: [18, 21, 22, 23, 24, 25] | Blocked By: [13, 15]

  **References**:
  - Pattern: `migrations/clickhouse/0003_ops_bronze.sql:48` - Raw-document fields used for content-hash and URL dedup.
  - External: `https://clickhouse.com/docs/engines/table-engines/mergetree-family/replacingmergetree` - Current-state materialization caveats.
  - External: `docs/comprehensive_delivery_plan.md:544` - Existing E9 scope for confidence-banded resolution.

  **Acceptance Criteria** (agent-executable only):
  - [ ] `go test ./... -run TestDocumentDedup` exits `0` for canonical URL, content-hash, and live-vs-archive collision fixtures.
  - [ ] `go test ./... -run TestEntityResolution` exits `0` for strong-ID, alias, and ambiguous-match fixtures.
  - [ ] Entity current-state materialization writes canonical rows and alias/lineage relations without destructive merges.
  - [ ] Unknown or ambiguous matches remain unresolved rather than forced into a canonical entity.

  **QA Scenarios** (MANDATORY - task incomplete without these):
  ```
  Scenario: Dedup and resolution happy path
    Tool: Bash
    Steps: Run the dedup/entity-resolution test suites on fixtures with repeated URLs, source-native IDs, and strong aliases.
    Expected: Exact duplicates collapse as designed, and strong identifier matches resolve to canonical entities with lineage preserved.
    Evidence: .sisyphus/evidence/task-17-resolution.txt

  Scenario: Ambiguity retention edge case
    Tool: Bash
    Steps: Run fixtures with partial identifier overlap and conflicting source evidence.
    Expected: Records stay in `possible` or `unknown` bands instead of being force-merged.
    Evidence: .sisyphus/evidence/task-17-resolution-edge.txt
  ```

  **Commit**: YES | Message: `feat(resolution): add dedup and entity matching baseline` | Files: [`internal/*`, `migrations/clickhouse/*`, `cmd/control-plane/*`, `testdata/*`]


- [ ] 18. Implement metric registry, contributions, state/snapshot MVs, and explainability payloads

  **What to do**: Implement the analytics framework. Populate `meta.metric_registry`, emit per-record metric contributions, build `gold.metric_state` incremental MVs plus refreshable snapshot views, and expose explainability payloads containing evidence, feature contributions, and confidence. This task must implement the full core metric family: `obs_count`, `event_count`, `entity_count_approx`, `source_count_approx`, `confidence_weighted_activity`, `source_diversity_score`, `freshness_lag_minutes`, `geolocation_success_rate`, `dedup_rate`, `schema_drift_rate`, `evidence_density`, `cross_source_confirmation_rate`, `trend_24h`, `trend_7d`, `acceleration_7d_vs_30d`, `anomaly_zscore_30d`, `burst_score`, and `risk_composite_global`; later domain-pack tasks add their domain metrics on the same substrate.
  **Must NOT do**: Do not compute metrics directly in API handlers or skip world/continent pseudo-place rollups.

  **Recommended Agent Profile**:
  - Category: `deep` - Reason: combines schema, MV strategy, formulas, and API-serving implications.
  - Skills: `[]` - Standard repo tooling is enough.
  - Omitted: [`playwright`] - No browser work.

  **Parallelization**: Can Parallel: PARTIAL | Wave 5 | Blocks: [19, 20, 21, 22, 23, 24, 25, 26] | Blocked By: [5, 13, 14, 15, 16, 17]

  **References**:
  - Pattern: `migrations/clickhouse/0002_core_tables.sql:1` - Registry/table style to mirror for `metric_registry`.
  - External: `https://clickhouse.com/docs/best-practices/use-materialized-views` - Incremental MV guidance.
  - External: `https://clickhouse.com/docs/materialized-view/refreshable-materialized-view` - Refresh cadence and replace/append behavior.
  - External: `https://clickhouse.com/docs/engines/table-engines/mergetree-family/aggregatingmergetree` - Aggregate-state design.

  **Acceptance Criteria** (agent-executable only):
  - [ ] `go test ./... -run TestMetricContributions` exits `0`.
  - [ ] `go test ./... -run TestMetricSnapshots` exits `0` and verifies rollups at place, admin0, continent, and world levels.
  - [ ] Fixture promotion runs populate `gold.metric_state` and `gold.metric_snapshot` for the declared core metrics.
  - [ ] Explainability payloads are present in metric outputs and include evidence references plus confidence context.

  **QA Scenarios** (MANDATORY - task incomplete without these):
  ```
  Scenario: Metric pipeline happy path
    Tool: Bash
    Steps: Run the metric correctness suites; query `gold.metric_state` and `gold.metric_snapshot` after fixture promotion.
    Expected: Declared core metrics exist with correct rollups and non-empty explainability payloads.
    Evidence: .sisyphus/evidence/task-18-metrics.txt

  Scenario: Refresh and zero-denominator edge case
    Tool: Bash
    Steps: Run fixtures that trigger ratio metrics with zero denominators and refreshable snapshot recomputation.
    Expected: Metrics handle zero/empty inputs safely and refresh cadence remains valid without query-overlap failures.
    Evidence: .sisyphus/evidence/task-18-metrics-edge.txt
  ```

  **Commit**: YES | Message: `feat(metrics): add contribution state snapshot and explainability layers` | Files: [`migrations/clickhouse/*`, `internal/*`, `cmd/control-plane/*`, `cmd/api/*`, `testdata/*`]

- [ ] 19. Implement CI, fixtures, dashboards, review workflows, and runbooks

  **What to do**: Build the quality and release harness. Add CI workflows for lint, unit, integration, migration, contract, and performance-smoke stages; add deterministic fixture bundles; add source freshness/geolocation/schema-drift dashboards; implement unresolved-location, low-confidence, and source-failure review workflows; write fresh-bootstrap, upgrade, backup/restore, kill-switch, and contract-break runbooks.
  **Must NOT do**: Do not leave release quality dependent on tribal knowledge or manual ad hoc commands.

  **Recommended Agent Profile**:
  - Category: `writing` - Reason: mixes CI config with documentation, evidence layout, and operational runbooks.
  - Skills: `[]` - No special skill required.
  - Omitted: [`playwright`] - Use only if a future browser-only source path needs explicit manual QA.

  **Parallelization**: Can Parallel: PARTIAL | Wave 5 | Blocks: [26] | Blocked By: [1, 2, 3, 9, 16, 18]

  **References**:
  - Pattern: `README.md:11` - Current quick-start and validation baseline to keep aligned with runbooks.
  - Test: `cmd/api/main_test.go:9` - Existing Go test style to extend into contract and integration suites.
  - External: `docs/comprehensive_delivery_plan.md:282` - Planned CI stages to make real.

  **Acceptance Criteria** (agent-executable only):
  - [ ] `.github/workflows/` contains runnable CI workflow files for lint, unit, integration, migration, contract, and performance-smoke stages.
  - [ ] `go test ./...` exits `0` locally and the same command set is mirrored in CI.
  - [ ] Runbooks exist for fresh bootstrap, upgrade migration, backup/restore, source kill switch, unresolved-location triage, and contract break handling.
  - [ ] Quality dashboards or machine-readable scorecard jobs emit source freshness, parser success, geolocation success, and schema-drift metrics.

  **QA Scenarios** (MANDATORY - task incomplete without these):
  ```
  Scenario: CI and fixture harness happy path
    Tool: Bash
    Steps: Run the full local CI command set exactly as defined in workflow files.
    Expected: Lint, unit, migration, integration, contract, and performance-smoke commands all exit 0.
    Evidence: .sisyphus/evidence/task-19-ci.txt

  Scenario: Review-workflow edge case
    Tool: Bash
    Steps: Trigger fixture failures for unresolved location, low-confidence resolution, and disabled source runs.
    Expected: Review queues or scorecards record the incidents and the runbooks match the observed handling path.
    Evidence: .sisyphus/evidence/task-19-review-workflows.txt
  ```

  **Commit**: YES | Message: `chore(quality): add ci fixtures dashboards and runbooks` | Files: [`.github/workflows/*`, `docs/*`, `testdata/*`, `cmd/*`, `internal/*`]

- [ ] 20. Deliver geopolitical/general-web pack

  **What to do**: Implement the first full domain pack on the completed substrate. Use `GDELT`, `ReliefWeb`, approved public feeds, and archive-friendly replay adapters; implement ACLED behind `user_supplied_key` mode and keep it disabled by default until credentials are provided. Normalize actors, events, locations, and cross-source links, then compute the full geopolitical metric family: `conflict_intensity_score`, `protest_activity_score`, `sanction_activity_score`, `humanitarian_pressure_score`, `cross_border_spillover_score`, `media_attention_score`, `media_attention_acceleration`, and `infrastructure_disruption_score`.
  **Must NOT do**: Do not let GDELT define the generic crawl architecture or pull Common Crawl/Wayback into the freshness lane.

  **Recommended Agent Profile**:
  - Category: `deep` - Reason: this pack validates the full OSINT substrate and multiple source classes together.
  - Skills: `[]` - Standard repo work only.
  - Omitted: [`playwright`] - Browser-rendered sources remain disabled unless a specific source truly requires it.

  **Parallelization**: Can Parallel: PARTIAL | Wave 5 | Blocks: [21] | Blocked By: [10, 11, 12, 13, 14, 15, 18]

  **References**:
  - Pattern: `seed/source_registry.json:3` - Existing `seed:gdelt` seed shows the first geopolitical source anchor.
  - External: `https://www.gdeltproject.org/data.html` - GDELT update model and dataset references.
  - External: `https://reliefweb.int/help/api` - ReliefWeb public API surface.
  - External: `https://archive.org/help/wayback_api.php` - Historical replay and verification interface.

  **Acceptance Criteria** (agent-executable only):
  - [ ] `docker compose run --rm control-plane run-once --job ingest-geopolitical --source-id seed:gdelt` exits `0` on fixture or staged sample inputs.
  - [ ] `docker compose run --rm control-plane run-once --job ingest-geopolitical --source-id fixture:reliefweb` exits `0`.
  - [ ] Domain fixtures populate canonical events plus geopolitical metric contributions and snapshots.
  - [ ] ACLED adapter tests pass in disabled-by-default mode without credentials and in enabled mode when a fixture key is supplied.

  **QA Scenarios** (MANDATORY - task incomplete without these):
  ```
  Scenario: Geopolitical pack happy path
    Tool: Bash
    Steps: Run the geopolitical ingestion jobs for GDELT and ReliefWeb fixtures; query canonical events and geopolitical metrics.
    Expected: Events, place links, and geopolitical metrics are materialized with explainability payloads.
    Evidence: .sisyphus/evidence/task-20-geopolitical.txt

  Scenario: Credential-gated source edge case
    Tool: Bash
    Steps: Run the ACLED adapter without credentials and then with a fixture credential.
    Expected: Without credentials the source stays disabled and logs a controlled reason; with credentials the adapter passes fixture tests without changing default public-only behavior.
    Evidence: .sisyphus/evidence/task-20-geopolitical-edge.txt
  ```

  **Commit**: YES | Message: `feat(geo-pack): deliver geopolitical and general-web ingestion` | Files: [`cmd/*`, `internal/*`, `migrations/clickhouse/*`, `seed/*`, `testdata/*`]

- [ ] 21. Expand API to metrics, analytics, entities, tracks, and search

  **What to do**: Finish the public API surface using real views and query handlers. Implement `GET /v1/metrics`, `GET /v1/metrics/{metricId}`, `GET /v1/analytics/rollups`, `GET /v1/analytics/time-series`, `GET /v1/analytics/hotspots`, `GET /v1/analytics/cross-domain`, `GET /v1/entities`, `GET /v1/entities/{entityId}`, `GET /v1/entities/{entityId}/tracks`, `GET /v1/entities/{entityId}/events`, `GET /v1/entities/{entityId}/places`, `GET /v1/search`, `GET /v1/search/places`, and `GET /v1/search/entities` with stable cursor pagination, filters, sort semantics, and compatibility views.
  **Must NOT do**: Do not expose raw SQL, pack-specific ad hoc JSON, or break the existing envelope/compatibility rules.

  **Recommended Agent Profile**:
  - Category: `unspecified-high` - Reason: this is the broadest serving-layer task and depends on the completed data substrate.
  - Skills: `[]` - Standard backend work only.
  - Omitted: [`playwright`] - HTTP and contract verification are sufficient.

  **Parallelization**: Can Parallel: NO | Wave 5 | Blocks: [22, 23, 24, 25, 26] | Blocked By: [16, 17, 18, 20]

  **References**:
  - Pattern: `cmd/api/main.go:17` - Existing route inventory to complete.
  - Pattern: `cmd/api/main.go:54` - Stable response envelope to preserve.
  - Test: `cmd/api/main_test.go:20` - Existing handler-test style to extend into full contract fixtures.
  - External: `docs/comprehensive_delivery_plan.md:618` - Complete `/v1/*` checklist to finish.

  **Acceptance Criteria** (agent-executable only):
  - [ ] `go test ./... -run TestAPIExpandedContracts` exits `0`.
  - [ ] Every endpoint listed in Section 14 of `docs/comprehensive_delivery_plan.md` returns a real response or defined empty result from view-backed handlers.
  - [ ] Search endpoints support deterministic filters and pagination on fixture data.
  - [ ] Analytics endpoints read from `gold` serving views/tables rather than computing live joins in handlers.

  **QA Scenarios** (MANDATORY - task incomplete without these):
  ```
  Scenario: Expanded API happy path
    Tool: Bash
    Steps: Start the full fixture stack and call all metrics, analytics, entities, tracks, and search endpoints.
    Expected: Each endpoint returns a stable JSON envelope and non-stub payload backed by real views or tables.
    Evidence: .sisyphus/evidence/task-21-api-expanded.txt

  Scenario: Empty-result and bad-filter edge case
    Tool: Bash
    Steps: Call search and analytics endpoints with unsupported filters, bad cursors, and empty-result queries.
    Expected: Handlers return the defined error or empty-result contract rather than panics, SQL leakage, or stub payloads.
    Evidence: .sisyphus/evidence/task-21-api-expanded-edge.txt
  ```

  **Commit**: YES | Message: `feat(api): complete metrics entities analytics and search surface` | Files: [`cmd/api/*`, `migrations/clickhouse/*`, `internal/*`, `testdata/*`]

- [ ] 22. Deliver maritime pack

  **What to do**: Implement the maritime domain pack using open port/registry metadata, OpenSanctions linkages, and a community/public AIS adapter path. If the live telemetry source needs credentials, implement it in `user_supplied_key` mode and keep the live adapter disabled by default while shipping replay fixtures for deterministic testing. Materialize vessel entities, track points/segments, port calls, AIS gap events, ownership/flag relations, and the full maritime metric family: `maritime_activity_score`, `ais_dark_hours_sum`, `ais_gap_frequency`, `identity_inconsistency_score`, `flag_ownership_mismatch_score`, `sanctions_exposure_score`, `port_loiter_score`, `rendezvous_probability`, `sts_transfer_suspicion_score`, `route_deviation_score`, `shadow_fleet_score`, and `maritime_risk_composite`.
  **Must NOT do**: Do not hardcode private telemetry credentials or present low-confidence shadow-fleet classification as certainty.

  **Recommended Agent Profile**:
  - Category: `ultrabrain` - Reason: multi-source telemetry, entity linkage, and risk analytics interact in non-trivial ways.
  - Skills: `[]` - Repo-local implementation is sufficient.
  - Omitted: [`playwright`] - Domain pack is backend-only.

  **Parallelization**: Can Parallel: YES | Wave 6 | Blocks: [26] | Blocked By: [11, 12, 13, 14, 17, 18, 21]

  **References**:
  - External: `https://www.opensanctions.org/` - Open entity/sanctions graph for exposure linkage.
  - External: `https://unece.org/trade/cefact/unlocode-code-list-country-and-territory` - Port/UN-LOCODE metadata anchor.
  - External: `docs/comprehensive_delivery_plan.md:560` - Maritime pack scope and metrics.

  **Acceptance Criteria** (agent-executable only):
  - [ ] `docker compose run --rm control-plane run-once --job ingest-maritime --source-id fixture:maritime` exits `0`.
  - [ ] Maritime fixtures create vessel entities, track points, port calls, and AIS gap events in `silver` tables.
  - [ ] Maritime metric fixtures populate the declared maritime metrics in `gold.metric_snapshot`.
  - [ ] Live credential-gated adapters stay disabled by default and pass fixture-mode tests without secrets.

  **QA Scenarios** (MANDATORY - task incomplete without these):
  ```
  Scenario: Maritime pack happy path
    Tool: Bash
    Steps: Run the maritime ingestion job on replay fixtures; query vessel entities, track tables, and maritime metrics.
    Expected: Vessel facts, port calls, AIS gaps, and maritime risk metrics are present with explainability payloads.
    Evidence: .sisyphus/evidence/task-22-maritime.txt

  Scenario: Credential-gated telemetry edge case
    Tool: Bash
    Steps: Run the live AIS adapter without credentials and then run replay-mode fixtures.
    Expected: The live adapter is safely disabled without credentials; replay fixtures still validate the full maritime pipeline.
    Evidence: .sisyphus/evidence/task-22-maritime-edge.txt
  ```

  **Commit**: YES | Message: `feat(maritime): deliver vessel telemetry and risk analytics` | Files: [`cmd/*`, `internal/*`, `migrations/clickhouse/*`, `seed/*`, `testdata/*`]

- [ ] 23. Deliver aviation pack

  **What to do**: Implement the aviation pack using OpenSky or equivalent public state-vector data, public aircraft registry data, airport metadata, and public NOTAM/weather context where available. If a live telemetry source requires credentials or tight rate limits, keep it in `user_supplied_key` mode and validate with replay fixtures. Materialize aircraft entities, track points, flight segments, transponder gap events, airport interaction events, and the full aviation metric family: `air_activity_score`, `transponder_gap_hours_sum`, `route_irregularity_score`, `military_likelihood_score`, `restricted_airspace_proximity_score`, `high_risk_airport_exposure_score`, `holding_pattern_anomaly_score`, and `air_risk_composite`.
  **Must NOT do**: Do not label aircraft as military unless the evidence model supports the claim.

  **Recommended Agent Profile**:
  - Category: `ultrabrain` - Reason: movement analytics, registry matching, and military-likelihood logic are complex.
  - Skills: `[]` - Standard repo tooling is sufficient.
  - Omitted: [`playwright`] - Backend-only pack.

  **Parallelization**: Can Parallel: YES | Wave 6 | Blocks: [26] | Blocked By: [11, 12, 13, 14, 17, 18, 21]

  **References**:
  - External: `https://opensky-network.org/` - Public state-vector context.
  - External: `https://www.faa.gov/licenses_certificates/aircraft_certification/aircraft_registry/releasable_aircraft_download` - Public aircraft registry source.
  - External: `docs/comprehensive_delivery_plan.md:567` - Aviation pack scope and metrics.

  **Acceptance Criteria** (agent-executable only):
  - [ ] `docker compose run --rm control-plane run-once --job ingest-aviation --source-id fixture:aviation` exits `0`.
  - [ ] Aviation fixtures create aircraft entities, flight segments, transponder gap events, and place-linked airport interactions.
  - [ ] Aviation metric fixtures populate the declared aviation metrics in `gold.metric_snapshot`.
  - [ ] Credential-gated live adapters stay disabled by default and replay fixtures still validate the full pack.

  **QA Scenarios** (MANDATORY - task incomplete without these):
  ```
  Scenario: Aviation pack happy path
    Tool: Bash
    Steps: Run the aviation replay fixtures and query aircraft entities, track segments, and aviation metrics.
    Expected: Flight activity, gaps, airport interactions, and aviation risk metrics are materialized with explainability payloads.
    Evidence: .sisyphus/evidence/task-23-aviation.txt

  Scenario: Low-evidence military edge case
    Tool: Bash
    Steps: Run fixtures with weak military indicators and conflicting registry data.
    Expected: The pack preserves low-confidence or unknown classification instead of asserting military status.
    Evidence: .sisyphus/evidence/task-23-aviation-edge.txt
  ```

  **Commit**: YES | Message: `feat(aviation): deliver aircraft telemetry and anomaly analytics` | Files: [`cmd/*`, `internal/*`, `migrations/clickhouse/*`, `seed/*`, `testdata/*`]

- [ ] 24. Deliver space pack

  **What to do**: Implement the space pack using public TLE/OMM feeds, public catalog history where available, transmitter metadata, and public conjunction/advisory sources that do not require private access. Compute orbit propagation, ground tracks, overpass windows, revisit metrics, place intersections, and the full space metric family: `satellite_activity_score`, `overpass_density_score`, `revisit_capability_score`, `conjunction_risk_score`, `maritime_observation_opportunity_score`, `critical_infrastructure_overpass_score`, and `space_risk_composite`.
  **Must NOT do**: Do not rely on private catalog sources or skip propagation validation against fixtures.

  **Recommended Agent Profile**:
  - Category: `ultrabrain` - Reason: orbital propagation and place intersection logic are mathematically demanding.
  - Skills: `[]` - Standard repo work only.
  - Omitted: [`playwright`] - No browser dependency.

  **Parallelization**: Can Parallel: YES | Wave 6 | Blocks: [26] | Blocked By: [11, 12, 13, 14, 17, 18, 21]

  **References**:
  - External: `https://celestrak.org/` - Public TLE/OMM feeds and satellite metadata context.
  - External: `docs/comprehensive_delivery_plan.md:574` - Space pack scope and metrics.

  **Acceptance Criteria** (agent-executable only):
  - [ ] `docker compose run --rm control-plane run-once --job ingest-space --source-id fixture:space` exits `0`.
  - [ ] Space fixtures create satellite entities, orbital observations, pass events, and place-overpass relations.
  - [ ] Space metric fixtures populate the declared space metrics in `gold.metric_snapshot`.
  - [ ] Propagation and intersection fixtures prove stable overpass-window calculations for known samples.

  **QA Scenarios** (MANDATORY - task incomplete without these):
  ```
  Scenario: Space pack happy path
    Tool: Bash
    Steps: Run the space pack replay fixtures and query satellite entities, pass events, and space metrics.
    Expected: Ground-track passes and place-linked space metrics are materialized with explainability payloads.
    Evidence: .sisyphus/evidence/task-24-space.txt

  Scenario: Propagation/intersection edge case
    Tool: Bash
    Steps: Run fixtures with near-threshold overpasses and conjunction samples.
    Expected: Overpass windows and conjunction outputs remain deterministic and do not create spurious place intersections.
    Evidence: .sisyphus/evidence/task-24-space-edge.txt
  ```

  **Commit**: YES | Message: `feat(space): deliver orbital overpass and risk analytics` | Files: [`cmd/*`, `internal/*`, `migrations/clickhouse/*`, `seed/*`, `testdata/*`]

- [ ] 25. Deliver safety/security pack

  **What to do**: Implement the safety/security pack using OpenSanctions, public hazard feeds (for example NASA FIRMS and NOAA/coastal/weather feeds), emergency bulletins, and public vulnerability catalogs such as CISA KEV. Materialize sanctions/entity-graph relations, hazard observations, vulnerability observations, place/sector mappings, and the full safety/security metric family: `cyber_exposure_score`, `known_exploited_vuln_pressure`, `fire_hotspot_score`, `coastal_hazard_score`, `weather_disruption_score`, and `safety_security_composite`.
  **Must NOT do**: Do not ingest private threat-intel feeds or hide uncertainty in sector/location mapping.

  **Recommended Agent Profile**:
  - Category: `deep` - Reason: this pack blends public sanctions, hazard, cyber, and emergency data into one metric system.
  - Skills: `[]` - Standard repo work only.
  - Omitted: [`playwright`] - Backend-only ingestion and analytics.

  **Parallelization**: Can Parallel: YES | Wave 6 | Blocks: [26] | Blocked By: [11, 12, 13, 14, 17, 18, 21]

  **References**:
  - External: `https://www.opensanctions.org/` - Open sanctions/entity graph.
  - External: `https://www.earthdata.nasa.gov/learn/find-data/near-real-time/firms` - Public fire hotspot context.
  - External: `https://www.cisa.gov/known-exploited-vulnerabilities-catalog` - KEV catalog context.
  - External: `docs/comprehensive_delivery_plan.md:588` - Safety/security pack scope and metrics.

  **Acceptance Criteria** (agent-executable only):
  - [ ] `docker compose run --rm control-plane run-once --job ingest-safety-security --source-id fixture:safety` exits `0`.
  - [ ] Fixtures create sanctions/entity links, hazard observations, vulnerability observations, and place-risk contributions.
  - [ ] Safety/security metrics are materialized in `gold.metric_snapshot` with explainability payloads.
  - [ ] Sector/location mapping retains explicit confidence or unresolved status where evidence is weak.

  **QA Scenarios** (MANDATORY - task incomplete without these):
  ```
  Scenario: Safety/security pack happy path
    Tool: Bash
    Steps: Run the safety/security ingestion fixtures and query sanctions links, hazard observations, vulnerability observations, and metrics.
    Expected: All declared record types and safety/security metrics are present with evidence payloads.
    Evidence: .sisyphus/evidence/task-25-safety.txt

  Scenario: Weak location/sector mapping edge case
    Tool: Bash
    Steps: Run fixtures with incomplete sector or location evidence.
    Expected: Records preserve uncertainty or enter review/unresolved flows instead of claiming precise mappings.
    Evidence: .sisyphus/evidence/task-25-safety-edge.txt
  ```

  **Commit**: YES | Message: `feat(safety): deliver sanctions hazard and cyber analytics` | Files: [`cmd/*`, `internal/*`, `migrations/clickhouse/*`, `seed/*`, `testdata/*`]

- [ ] 26. Deliver scale-out/HA topology, distributed tables, cluster DR, and cost controls

  **What to do**: Complete E18 after the single-node platform is stable. Add a documented shard/replica topology with ClickHouse Keeper, replicated local tables, distributed tables, cluster-aware backup/restore drills, and cost-control policies for TTL, projections, MV spend, and raw retention. Keep single-node mode as the default deployment and expose cluster mode as an explicit optional profile.
  **Must NOT do**: Do not replace the single-node baseline or treat `Distributed` as the source of truth.

  **Recommended Agent Profile**:
  - Category: `ultrabrain` - Reason: HA topology, Keeper semantics, and cluster cost controls are high-risk infrastructure work.
  - Skills: `[]` - Standard repo and ClickHouse tooling are sufficient.
  - Omitted: [`playwright`] - Infrastructure-only task.

  **Parallelization**: Can Parallel: NO | Wave 6 | Blocks: [] | Blocked By: [3, 5, 18, 19, 21, 22, 23, 24, 25]

  **References**:
  - Pattern: `docker-compose.yml:5` - Current single-node baseline that must remain supported.
  - External: `https://clickhouse.com/docs/guides/sre/keeper/clickhouse-keeper` - Keeper topology and quorum requirements.
  - External: `https://clickhouse.com/docs/engines/table-engines/special/distributed` - `Distributed` engine caveats.
  - External: `https://clickhouse.com/docs/operations/backup/overview` - Cluster backup/restore guidance.

  **Acceptance Criteria** (agent-executable only):
  - [ ] A cluster-mode compose/profile or equivalent deployment definition brings up Keeper and replicated ClickHouse nodes successfully.
  - [ ] Cluster integration tests prove replicated local tables plus distributed tables serve reads and writes as designed.
  - [ ] Cluster backup and restore drills exit `0` and produce evidence artifacts.
  - [ ] Cost-control checks verify TTL, projection, MV, and raw-retention policies for both single-node and cluster modes.

  **QA Scenarios** (MANDATORY - task incomplete without these):
  ```
  Scenario: Cluster happy path
    Tool: Bash
    Steps: Start the cluster profile; run replicated-table and distributed-table integration tests; query cluster health.
    Expected: Keeper quorum is healthy, replicated tables accept writes, and distributed queries return the expected fixture data.
    Evidence: .sisyphus/evidence/task-26-cluster.txt

  Scenario: Restore and unavailable-shard edge case
    Tool: Bash
    Steps: Run a backup/restore drill; simulate one unavailable shard or replica during read/write tests.
    Expected: Restore completes successfully, and shard-unavailability behavior follows the configured policy without silent data loss.
    Evidence: .sisyphus/evidence/task-26-cluster-edge.txt
  ```

  **Commit**: YES | Message: `feat(scale): add keeper cluster mode and dr controls` | Files: [`docker-compose.yml`, `build/*`, `infra/*`, `migrations/clickhouse/*`, `docs/*`]

## Final Verification Wave (4 parallel agents, ALL must APPROVE)
- [ ] F1. Plan Compliance Audit - oracle
- [ ] F2. Code Quality Review - unspecified-high
- [ ] F3. Real Runtime QA - unspecified-high (+ playwright if renderer/browser-only connector paths exist)
- [ ] F4. Scope Fidelity Check - deep

## Commit Strategy
- Use one commit per completed task unless the task is split across multiple agents and a final integration commit is needed.
- Keep commit messages imperative and scope-specific, e.g. `fix(bootstrap): make image buildable with seed and internal packages`.
- Never amend shared history; if hooks change files after commit, make a new follow-up commit unless the just-created local commit is still safe to amend under repo policy.
- Do not push until the full task acceptance criteria pass.

## Success Criteria
- Every epic E0-E18 has an implemented and verified closure path in this repo.
- All required services build and boot from a clean machine with one compose command plus explicit data-load jobs.
- The place graph, source governance, ingest pipeline, promotion path, metrics engine, and full REST API all have fixture-backed verification.
- Domain packs produce place-linked facts and metrics with explainability payloads.
- Upgrade safety, contract safety, and DR safety are demonstrated with automated evidence.
