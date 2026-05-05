# Production Platform Roadmap

## TL;DR
> **Summary**: Build the missing production foundations for the Go backend in dependency order: migrations, execution, resilience, observability, security, recovery, governance, and finally UI/read-model surfaces.
> **Deliverables**:
> - Real schema migration framework and schema safety controls
> - Executable pipeline/orchestration engine with retries/backfills/DLQ
> - Production observability, alerting, backups, secrets, RBAC/SSO, and rate-limits
> - Governance/data-management features: quality, export/import, audit, catalog, retention, compatibility
> - UI read models and operator views for lineage, alerts, runs, and artifacts
> **Effort**: XL
> **Parallel**: YES - 4 implementation waves + final review wave
> **Critical Path**: Migration framework → pipeline engine → resilience/telemetry/security → governance read models → UI/operator surfaces

## Context
### Original Request
Plan the implementation of the missing, critical, important, QoL, and UI-oriented production features for the system.

### Interview Summary
- Backend is a Go 1.23 repo; root Python-era docs are stale.
- Real owners: `cmd/api`, `cmd/bootstrap`, `cmd/control-plane`, `cmd/worker-fetch`, `cmd/worker-parse`, `internal/*`, `migrations/clickhouse`.
- Current shipped surface already includes schema ledger/migrations, deterministic control-plane jobs, retries in fetch, catalog/coverage/lineage read models, and contract/e2e test scaffolding.
- Production gaps cluster around migrations, execution, resilience, observability, alerts, backup/restore, secrets, RBAC/SSO, validation, compatibility, quality, export/import, audit, retention, search, and UI read models.

### Current State Inventory
- **Already present as runtime primitives:** migration ledger/bootstrap install-verify, pipeline metadata and execution-plan surfaces, retry/backfill checkpoints, observability summary/activity endpoints, catalog/search, export/import, backup/restore, artifact manifests, data-product versioning, audit trail, lazy init, and dependency pinning.
- **Still needs production hardening:** a first-class schema migration path, explicit schema diff/approval flow, full pipeline execution semantics, DLQ/poison isolation, alert delivery, stronger RBAC/SSO/secrets policy, multi-tenancy, rate limiting, lifecycle cleanup, and the UI/operator surfaces that consume the read models.

### Metis/Oracle Review (gaps addressed)
- Freeze one top-level sequence: migrations → execution → resilience → observability/security → governance → UI.
- Treat the backend contracts as prerequisites for any renderer/UI work.
- Encode one authoritative authN/authZ and secrets policy before API hardening.
- Keep orchestration state, backfills, retries, and DLQ semantics on one lifecycle model.
- Preserve migration immutability, seed compatibility, and bootstrap discoverability.

## Work Objectives
### Core Objective
Deliver a production-grade platform roadmap that is decision-complete for execution: every task has a concrete owner, dependency order, acceptance criteria, and agent-executable verification.

### Deliverables
- One master roadmap covering all requested features
- Phase-gated execution plan
- Dependency matrix and parallelization strategy
- Task-level acceptance criteria and QA scenarios

### Definition of Done (verifiable conditions with commands)
- Plan file exists at `.sisyphus/plans/production-roadmap.md`
- Every task has: implementation scope, references, acceptance criteria, and QA scenarios
- Critical-path dependencies are explicit and ordered
- Final verification wave is included

### Must Have
- Decision-complete sequencing
- No stale-doc assumptions
- Agent-run verification paths only

### Must NOT Have (guardrails, AI slop patterns, scope boundaries)
- No splitting into separate plans
- No fake support for features only documented in stale root files
- No unapplied migration rewrites in place
- No UI work before the backend contracts it consumes are defined

## Verification Strategy
> ZERO HUMAN INTERVENTION during implementation verification; the post-review approval gate still requires user confirmation before execution starts.
- Test decision: Go stdlib unit tests + contract tests + tagged e2e + `bootstrap verify`
- QA policy: Every task has agent-executed scenarios
- Evidence: `.sisyphus/evidence/task-{N}-{slug}.{ext}`

## Execution Strategy
### Parallel Execution Waves
> Target: 4-8 tasks per wave. <3 per wave (except final) = under-splitting.
> Extract shared dependencies as Wave-1 tasks for max parallelism.

Wave 1: foundation contracts, migration framework, execution engine skeleton, security baseline, telemetry baseline
Wave 2: retries/backfills, schema validation/diff/compatibility, backup/restore, alerting/notifications
Wave 3: quality, export/import, object-URI ingestion, retention, audit, catalog/search
Wave 4: multi-tenant/rate-limit/versioning/artifact browsing/worker console/lazy init
Wave 5: final review and verification gate

### Wave Exit Criteria
- **Wave 1 exit:** migration framework exists, execution can claim/transition a stored run, security/secrets baseline is wired, telemetry emits correlation IDs, and the verify/lazy-init path is usable.
- **Wave 2 exit:** backfills/retries/DLQ are stable, observability/alerts are emitting, and backup/restore has a successful dry-run.
- **Wave 3 exit:** retention, quality, export/import, catalog, audit, and data-product versioning all have persisted read models.
- **Wave 4 exit:** UI surfaces render from real API contracts and the e2e suite exercises a real stack without relying on stale compose workflows.

### Dependency Matrix (full, all tasks)
- 1 → 2, 3, 9, 12, 13, 14, 15
- 2 → 3, 4, 14, 15, 16
- 3 → 4, 9, 10, 16
- 4 → 5, 6
- 5 → 6, 13, 14, 15
- 6 → 15
- 7 → 13, 14, 15
- 8 → 9, 13, 14, 15
- 9 → 13, 15
- 10 → 13, 14, 15
- 11 → 16
- 12 → 14, 15
- 13 → 14, 15
- 14 → 15
- 15 → 16

### Agent Dispatch Summary (wave → task count → categories)
- Wave 1 → 4 tasks → deep/unspecified-high
- Wave 2 → 4 tasks → deep/unspecified-high
- Wave 3 → 4 tasks → unspecified-high/deep
- Wave 4 → 4 tasks → unspecified-high + visual-engineering
- Wave 5 → 4 review agents → oracle/unspecified-high/deep

## TODOs
- [x] 1. Schema migration framework + schema safety

  **What to do**: Introduce a real metadata schema migration path with immutable migration application, checksum validation, compatibility metadata, and a diff/approval record so schema changes can be planned before rollout.
  **Must NOT do**: Do not rewrite applied SQL in place, bypass the migration ledger, or couple schema changes to bootstrap-only table creation.

  **Recommended Agent Profile**:
  - Category: `deep` - Reason: This touches the migration ledger, bootstrap lifecycle, and ClickHouse schema guarantees.
  - Skills: `[]` - No special skill required.
  - Omitted: `visual-engineering` - Not a UI task.

  **Parallelization**: Can Parallel: NO | Wave 1 | Blocks: tasks 2, 3, 9, 12, 14, 15 | Blocked By: none

  **References**:
  - `internal/migrate/http_runner.go` - current HTTP-only migration runner and ledger owner.
  - `internal/migrate/schema_standards_test.go` - schema/migration invariants.
  - `migrations/clickhouse/0004_meta_registries.sql` - schema registry and compatibility metadata pattern.
  - `cmd/bootstrap/main.go` - install/verify lifecycle that must continue to pass.

  **Acceptance Criteria**:
  - [ ] A new metadata schema change can be introduced through the migration path and recorded in the migration ledger.
  - [ ] Checksums on already-applied migrations fail fast if modified.
  - [ ] Compatibility/diff status is persisted in a queryable form for later approval/rollout decisions.
  - [ ] `go run ./cmd/bootstrap verify` still succeeds after the migration framework change.

  **QA Scenarios**:
  ```
  Scenario: Apply a new migration
    Tool: Bash
    Steps: Run the migration verification path against a fixture migration and then run `go run ./cmd/bootstrap verify`.
    Expected: The migration is applied once, recorded, and verification exits 0.
    Evidence: .sisyphus/evidence/task-1-migration-framework-happy.txt

  Scenario: Detect checksum drift
    Tool: Bash
    Steps: Modify an already-applied migration fixture checksum and run the migration verification path.
    Expected: The run fails with a checksum mismatch and does not reapply the migration.
    Evidence: .sisyphus/evidence/task-1-migration-framework-drift.txt
  ```

  **Commit**: NO | Message: `n/a` | Files: [.sisyphus/plans/production-roadmap.md]

- [x] 2. Pipeline execution engine

  **What to do**: Make stored pipeline definitions executable through the control-plane with an authoritative run state machine, deterministic job dispatch, and durable run outputs instead of metadata-only registration.
  **Must NOT do**: Do not embed orchestration logic in the API layer or make pipeline execution depend on manual shell steps.

  **Recommended Agent Profile**:
  - Category: `deep` - Reason: This is the core orchestration model for the platform.
  - Skills: `[]` - No special skill required.
  - Omitted: `quick` - This is a multi-package change.

  **Parallelization**: Can Parallel: NO | Wave 1 | Blocks: tasks 3, 4, 14, 15, 16 | Blocked By: task 1

  **References**:
  - `cmd/control-plane/main.go` - long-running and run-once job entrypoint.
  - `cmd/control-plane/jobs_*.go` - existing job registration patterns.
  - `cmd/control-plane/jobs_promote.go` - canonical promotion pipeline owner.
  - `internal/promote/pipeline.go` - materialization/pipeline execution logic.

  **Acceptance Criteria**:
  - [ ] A stored pipeline definition can be executed from the control-plane and produces durable run state.
  - [ ] Run transitions are deterministic and idempotent for repeated invocations.
  - [ ] Failures are persisted with enough context to resume or inspect the run.
  - [ ] Existing control-plane discovery/CLI behaviors still work.

  **QA Scenarios**:
  ```
  Scenario: Execute a stored pipeline
    Tool: Bash
    Steps: Run the control-plane against a known pipeline fixture and verify resulting materialized state.
    Expected: Run completes successfully and outputs are visible in the expected ClickHouse tables/views.
    Evidence: .sisyphus/evidence/task-2-pipeline-exec-happy.txt

  Scenario: Re-run the same pipeline
    Tool: Bash
    Steps: Execute the same pipeline definition twice.
    Expected: The second run is idempotent and does not create duplicate work artifacts.
    Evidence: .sisyphus/evidence/task-2-pipeline-exec-idempotent.txt
  ```

  **Commit**: NO | Message: `n/a` | Files: [.sisyphus/plans/production-roadmap.md]

- [x] 3. Incremental loads, backfills, and replay

  **What to do**: Add native support for replaying history, processing deltas only, and backfilling explicit ranges with durable checkpoints and range-aware state.
  **Must NOT do**: Do not require full reprocessing for historical repair, and do not treat replay/backfill as an ad hoc script-only workflow.

  **Recommended Agent Profile**:
  - Category: `deep` - Reason: This touches stateful ingestion semantics and checkpointing.
  - Skills: `[]` - No special skill required.
  - Omitted: `visual-engineering` - Not a UI task.

  **Parallelization**: Can Parallel: YES | Wave 1 | Blocks: tasks 4, 9, 10, 16 | Blocked By: tasks 1, 2

  **References**:
  - `internal/discovery/frontier.go` - frontier/retry/checkpoint primitives.
  - `migrations/clickhouse/0013_crawl_frontier_leases.sql` - lease/state storage pattern.
  - `migrations/clickhouse/0014_fetch_ledger_contract.sql` - attempt and ledger contract pattern.
  - `migrations/clickhouse/0018_parse_checkpoint_ledger.sql` - checkpointing pattern.
  - `internal/fetch/retention.go` - retention-class handling that influences replay scope.

  **Acceptance Criteria**:
  - [ ] A run can target an explicit historical range or delta-only window.
  - [ ] Replays/backfills are checkpointed so partial failure resumes without duplicating outputs.
  - [ ] Range boundaries are persisted in a queryable form.
  - [ ] A backfill of an already-processed range is safely no-op or idempotent.

  **QA Scenarios**:
  ```
  Scenario: Backfill a historical window
    Tool: Bash
    Steps: Run a range-limited ingestion/backfill against a known fixture window.
    Expected: Only rows in the requested range are processed and checkpoint state is written.
    Evidence: .sisyphus/evidence/task-3-backfill-happy.txt

  Scenario: Resume after interruption
    Tool: Bash
    Steps: Interrupt a range run midway, then rerun the same window.
    Expected: The second execution resumes from checkpoint without duplicating outputs.
    Evidence: .sisyphus/evidence/task-3-backfill-resume.txt
  ```

  **Commit**: NO | Message: `n/a` | Files: [.sisyphus/plans/production-roadmap.md]

- [x] 4. Retry, backoff, and dead-letter handling

  **What to do**: Standardize retry policy, exponential backoff, poison-message isolation, and failure buckets for bad jobs across the fetch/parse/control-plane path.
  **Must NOT do**: Do not silently drop bad jobs, and do not let poison payloads block healthy work.

  **Recommended Agent Profile**:
  - Category: `deep` - Reason: This is a distributed failure-handling path.
  - Skills: `[]` - No special skill required.
  - Omitted: `quick` - This spans multiple workers and shared libs.

  **Parallelization**: Can Parallel: YES | Wave 1 | Blocks: tasks 5, 6, 7, 16 | Blocked By: tasks 1, 2, 3

  **References**:
  - `internal/fetch/client.go` - existing HTTP retry/backoff behavior.
  - `cmd/worker-fetch/main.go` - fetch worker entrypoint and retry surface.
  - `cmd/worker-parse/main.go` - parse worker failure surface.
  - `internal/discovery/frontier.go` - attempt counters/retry states.

  **Acceptance Criteria**:
  - [ ] Transient failures retry with bounded backoff.
  - [ ] Poison jobs land in a dead-letter/failure bucket with reason and attempt history.
  - [ ] Healthy jobs continue even when one item becomes poison.
  - [ ] Retry policy is consistent across the worker/control-plane path.

  **QA Scenarios**:
  ```
  Scenario: Retry a transient upstream failure
    Tool: Bash
    Steps: Simulate a 429/5xx response and run the fetch path.
    Expected: The job retries with backoff and eventually succeeds or exhausts the limit cleanly.
    Evidence: .sisyphus/evidence/task-4-retry-happy.txt

  Scenario: Isolate a poison payload
    Tool: Bash
    Steps: Feed an invalid payload that always fails parsing/execution.
    Expected: The item is moved to the dead-letter bucket and other jobs continue.
    Evidence: .sisyphus/evidence/task-4-dlq-poison.txt
  ```

  **Commit**: NO | Message: `n/a` | Files: [.sisyphus/plans/production-roadmap.md]

- [ ] 5. Observability stack + worker live tail plumbing

  **What to do**: Add structured logs, metrics, traces, and correlation IDs across API, workers, storage interactions, and serving surfaces; expose dashboard-ready stats and a live tail path for worker activity.
  **Must NOT do**: Do not introduce ad hoc logging formats per binary, and do not make observability depend on human log scraping.

  **Recommended Agent Profile**:
  - Category: `unspecified-high` - Reason: Cross-cutting operational instrumentation across many binaries.
  - Skills: `[]` - No special skill required.
  - Omitted: `quick` - This is a platform-wide concern.

  **Parallelization**: Can Parallel: YES | Wave 2 | Blocks: tasks 6, 14, 15, 16 | Blocked By: tasks 1, 2, 4

  **References**:
  - `internal/dashboardstats/service.go` - current stats aggregation path.
  - `cmd/api/internal_stats.go` - API surface for internal stats.
  - `cmd/renderer/main.go` - current rendering/serving entrypoint.
  - `cmd/control-plane/main.go` - run/job execution path that needs correlation.
  - `cmd/worker-fetch/main.go` and `cmd/worker-parse/main.go` - worker logging/metric surfaces.

  **Acceptance Criteria**:
  - [ ] Every API request and worker job emits a correlation ID that can be traced end-to-end.
  - [ ] Structured logs and metrics are emitted consistently from API, control-plane, and workers.
  - [ ] A dashboard-safe stats endpoint remains available for renderer consumption.
  - [ ] Worker live activity can be surfaced without scraping raw pod logs.

  **QA Scenarios**:
  ```
  Scenario: Trace a request across binaries
    Tool: Bash
    Steps: Send one API request that triggers a worker path and inspect logs/metrics for the same correlation ID.
    Expected: The ID appears in API, control-plane, and worker output.
    Evidence: .sisyphus/evidence/task-5-observability-correlation.txt

  Scenario: Check dashboard stats endpoint
    Tool: Bash
    Steps: Query the internal stats endpoint and render endpoint after generating sample activity.
    Expected: The endpoint returns structured data suitable for UI consumption and does not error.
    Evidence: .sisyphus/evidence/task-5-observability-stats.txt
  ```

  **Commit**: NO | Message: `n/a` | Files: [.sisyphus/plans/production-roadmap.md]

- [ ] 6. Alerting and notifications

  **What to do**: Build alert emission and notification handling for failed ingestions, stuck runs, schema drift, quality failures, and degraded services, with a consumable alert-center data model.
  **Must NOT do**: Do not hide critical failures inside logs or require manual log inspection for operator awareness.

  **Recommended Agent Profile**:
  - Category: `deep` - Reason: Alert policy and failure semantics cross multiple subsystems.
  - Skills: `[]` - No special skill required.
  - Omitted: `visual-engineering` - Backend-first feature.

  **Parallelization**: Can Parallel: YES | Wave 2 | Blocks: tasks 15, 16 | Blocked By: tasks 4, 5

  **References**:
  - `cmd/control-plane/jobs_space.go` - existing alert-like domain records and failure signaling.
  - `internal/dashboardstats/service.go` - stats source for alert thresholds.
  - `cmd/api/*` - new alert-center read routes will live here.
  - `migrations/clickhouse/*` - alert event storage/read-model migrations.

  **Acceptance Criteria**:
  - [ ] A failed ingestion or stuck run creates a queryable alert event.
  - [ ] Schema drift and quality failures are represented as distinct alert types.
  - [ ] Notification delivery is retryable and failure-aware.
  - [ ] The alert stream is consumable by future UI surfaces.

  **QA Scenarios**:
  ```
  Scenario: Raise an ingestion alert
    Tool: Bash
    Steps: Force a known failure path and inspect the persisted alert record.
    Expected: A new alert appears with the correct type and severity.
    Evidence: .sisyphus/evidence/task-6-alerting-happy.txt

  Scenario: Retry a failed notification
    Tool: Bash
    Steps: Simulate a delivery sink failure.
    Expected: Notification retry/backoff occurs and the failure is visible in the alert stream.
    Evidence: .sisyphus/evidence/task-6-alerting-retry.txt
  ```

  **Commit**: NO | Message: `n/a` | Files: [.sisyphus/plans/production-roadmap.md]

- [ ] 7. Backup, restore, and disaster recovery

  **What to do**: Define and implement backup/restore procedures for metadata, object storage, and serving data, including operator-facing restore validation and disaster-recovery runbooks.
  **Must NOT do**: Do not treat backup as documentation-only, and do not ship a restore path that has never been verified against a clean environment.

  **Recommended Agent Profile**:
  - Category: `deep` - Reason: Recovery paths span multiple stores and operational workflows.
  - Skills: `[]` - No special skill required.
  - Omitted: `quick` - This is a production recovery feature.

  **Parallelization**: Can Parallel: YES | Wave 2 | Blocks: tasks 12, 13, 14, 15, 16 | Blocked By: tasks 1, 2, 5, 8

  **References**:
  - `cmd/bootstrap/main.go` - install/verify lifecycle that already knows how to prepare data stores.
  - `infra/backup/hooks/*` - existing backup hook assets.
  - `infra/backup/manifests/*` - manifest-driven backup artifacts.
  - `docs/runbooks/backup-restore.md` - current operator-facing recovery guidance.

  **Acceptance Criteria**:
  - [ ] Metadata backup can be created and restored into a clean environment.
  - [ ] Object-storage and serving-data recovery steps are defined and runnable.
  - [ ] A restore dry-run/verification command proves the procedure works before release.
  - [ ] Recovery steps preserve readiness and do not corrupt the schema ledger.

  **QA Scenarios**:
  ```
  Scenario: Backup and restore metadata
    Tool: Bash
    Steps: Create a backup snapshot, restore it into a clean environment, then run verification.
    Expected: The restored environment reports ready and the core records are present.
    Evidence: .sisyphus/evidence/task-7-backup-restore-happy.txt

  Scenario: Failed restore validation
    Tool: Bash
    Steps: Corrupt a backup artifact and attempt restore validation.
    Expected: The restore fails cleanly and reports the artifact problem.
    Evidence: .sisyphus/evidence/task-7-backup-restore-fail.txt
  ```

  **Commit**: NO | Message: `n/a` | Files: [.sisyphus/plans/production-roadmap.md]

- [ ] 8. Secrets management + RBAC/SSO

  **What to do**: Replace `.env`-only credential handling with a real secrets loading strategy and enforce role-based access plus identity mapping for API users and operators.
  **Must NOT do**: Do not bake secrets into repo files, and do not leave the system with a single dev-only API key path for production users.

  **Recommended Agent Profile**:
  - Category: `deep` - Reason: This is a security boundary and identity model change.
  - Skills: `[]` - No special skill required.
  - Omitted: `quick` - This needs coordinated API/bootstrap changes.

  **Parallelization**: Can Parallel: YES | Wave 1 | Blocks: tasks 9, 10, 13, 15, 16 | Blocked By: none

  **References**:
  - `cmd/bootstrap/main.go` - bootstrap install/verify path that seeds platform credentials.
  - `cmd/bootstrap/source_registry.go` - current env-based source credential indirection.
  - `cmd/worker-fetch/main.go` - worker credential loading path.
  - `cmd/api/*` - authorization middleware and API access checks.

  **Acceptance Criteria**:
  - [ ] Secrets are loaded from a controlled runtime source with a dev fallback only.
  - [ ] Roles/claims map to explicit permissions for API and operator actions.
  - [ ] SSO/identity integration is abstracted so production auth can be configured without code changes.
  - [ ] Existing bootstrap/setup flows still work for local development.

  **QA Scenarios**:
  ```
  Scenario: Load secrets from runtime source
    Tool: Bash
    Steps: Start the service with mounted secrets and verify the process reads them without `.env` values.
    Expected: The service starts and uses the mounted secret values.
    Evidence: .sisyphus/evidence/task-8-secrets-runtime.txt

  Scenario: Deny unauthorized access
    Tool: Bash
    Steps: Call a protected API path with an identity that lacks the needed role.
    Expected: The request is rejected with the correct authorization error.
    Evidence: .sisyphus/evidence/task-8-rbac-deny.txt
  ```

  **Commit**: NO | Message: `n/a` | Files: [.sisyphus/plans/production-roadmap.md]

- [ ] 9. Serving-config validation + rate limiting + multi-tenant isolation

  **What to do**: Validate user-supplied serving options before DDL, add API throttling/abuse protection, and introduce tenant-aware namespace/quota/access separation.
  **Must NOT do**: Do not allow invalid `order_by`/`partition_by`-style input to reach DDL, and do not share unbounded namespaces across tenants.

  **Recommended Agent Profile**:
  - Category: `deep` - Reason: This is a safety boundary on user input and tenancy.
  - Skills: `[]` - No special skill required.
  - Omitted: `quick` - This is a cross-cutting contract change.

  **Parallelization**: Can Parallel: YES | Wave 3 | Blocks: tasks 13, 15, 16 | Blocked By: tasks 1, 2, 8

  **References**:
  - `cmd/api/handlers_expanded.go` - expanded API surface and serving-related request handling.
  - `cmd/api/route_contracts.go` - route/contract shape for request validation.
  - `internal/promote/pipeline.go` - DDL/materialization path that must receive validated config.
  - `migrations/clickhouse/0004_meta_registries.sql` - registry pattern for storing approved config metadata.

  **Acceptance Criteria**:
  - [ ] Invalid serving configuration is rejected before DDL is emitted.
  - [ ] Rate limiting/abuse protection is enforced on expensive or unsafe API patterns.
  - [ ] Tenant-scoped namespaces and quotas prevent cross-tenant access by default.
  - [ ] Validation errors are explicit enough for callers to correct input without guesswork.

  **QA Scenarios**:
  ```
  Scenario: Reject invalid serving config
    Tool: Bash
    Steps: Submit a serving definition with an invalid sort/partition field and inspect the response.
    Expected: The request is rejected before any DDL work starts.
    Evidence: .sisyphus/evidence/task-9-serving-validation.txt

  Scenario: Throttle abusive requests
    Tool: Bash
    Steps: Send a burst of expensive API requests from one identity/tenant.
    Expected: Requests are rate limited and the limit response is explicit.
    Evidence: .sisyphus/evidence/task-9-rate-limit.txt
  ```

  **Commit**: NO | Message: `n/a` | Files: [.sisyphus/plans/production-roadmap.md]

- [ ] 10. Retention, TTL, and cleanup policies

  **What to do**: Add automated cleanup for raw, silver, gold, manifest, and historical artifacts with policy-driven TTLs and safe retention windows.
  **Must NOT do**: Do not delete artifacts without policy provenance or a deterministic retention rule.

  **Recommended Agent Profile**:
  - Category: `unspecified-high` - Reason: This is a lifecycle policy across storage and metadata.
  - Skills: `[]` - No special skill required.
  - Omitted: `visual-engineering` - Backend lifecycle work.

  **Parallelization**: Can Parallel: YES | Wave 3 | Blocks: tasks 13, 15, 16 | Blocked By: tasks 3, 7, 9

  **References**:
  - `internal/fetch/retention.go` - current retention-class semantics.
  - `migrations/clickhouse/0010_runtime_analytics_outputs.sql` - TTL pattern.
  - `migrations/clickhouse/0018_parse_checkpoint_ledger.sql` - checkpoint retention pattern.
  - `cmd/control-plane/jobs_*.go` - scheduled job surface for cleanup execution.

  **Acceptance Criteria**:
  - [ ] Retention rules are expressed as code/config and enforced by a scheduled cleanup path.
  - [ ] Cleanup respects artifact class boundaries and does not delete live or required checkpoint data.
  - [ ] Cleanup outcomes are recorded for auditability and operator review.
  - [ ] A dry-run mode shows what would be deleted before action is taken.

  **QA Scenarios**:
  ```
  Scenario: Run cleanup in dry-run mode
    Tool: Bash
    Steps: Execute the cleanup job against a fixture dataset with dry-run enabled.
    Expected: The job lists deletions but does not remove files or rows.
    Evidence: .sisyphus/evidence/task-10-retention-dry-run.txt

  Scenario: Enforce retention window
    Tool: Bash
    Steps: Advance the retention clock or fixture timestamp beyond the allowed window and run cleanup.
    Expected: Expired artifacts are removed and current artifacts remain.
    Evidence: .sisyphus/evidence/task-10-retention-enforce.txt
  ```

  **Commit**: NO | Message: `n/a` | Files: [.sisyphus/plans/production-roadmap.md]

- [ ] 11. Dependency pinning, lazy init, and one-command verification

  **What to do**: Lock down reproducible dependency resolution, defer expensive/fragile service initialization until needed, and formalize the repo’s one-command verification path around the existing Go runtime checks.
  **Must NOT do**: Do not leave verification dependent on stale compose-era workflows, and do not eagerly initialize services in tests/CLIs when a lazy path is sufficient.

  **Recommended Agent Profile**:
  - Category: `unspecified-high` - Reason: This is a repo-wide reliability and developer-experience improvement.
  - Skills: `[]` - No special skill required.
  - Omitted: `visual-engineering` - Not UI work.

  **Parallelization**: Can Parallel: YES | Wave 3 | Blocks: tasks 15, 16 | Blocked By: tasks 1, 2, 5, 7

  **References**:
  - `go.mod` - module/dependency source of truth.
  - `cmd/api/main_test.go` - test startup patterns that benefit from lazy init.
  - `cmd/bootstrap/main.go` - expensive setup path that should initialize only when needed.
  - `.github/workflows/unit.yml`, `.github/workflows/contract.yml`, `.github/workflows/e2e.yml` - real CI gates.
  - `test/e2e/pipeline_test.go` - stack-level verification path.

  **Acceptance Criteria**:
  - [ ] Dependency resolution is reproducible in dev/CI/prod from the pinned module state.
  - [ ] Expensive services initialize lazily in tests and CLIs that do not need them.
  - [ ] The one-command verification path covers the authoritative Go checks from the repo guidance.
  - [ ] Stale compose-era verification paths are not treated as the primary source of truth.

  **QA Scenarios**:
  ```
  Scenario: Run the verify command
    Tool: Bash
    Steps: Run the repository’s one-command verification path from a clean checkout.
    Expected: The command completes using the Go runtime checks and reports success.
    Evidence: .sisyphus/evidence/task-11-verify-command.txt

  Scenario: Avoid eager initialization
    Tool: Bash
    Steps: Run a unit test or CLI path that should not require external services.
    Expected: The process starts without initializing unused expensive services.
    Evidence: .sisyphus/evidence/task-11-lazy-init.txt
  ```

  **Commit**: NO | Message: `n/a` | Files: [.sisyphus/plans/production-roadmap.md]

- [ ] 12. Quality-rule management + quality trend history

  **What to do**: Build full CRUD, versioning, activation, and ownership for quality rules, and persist quality results over time so trends can be queried instead of only latest outcomes.
  **Must NOT do**: Do not stop at a static latest-pass/fail flag, and do not make rule changes unversioned or ownerless.

  **Recommended Agent Profile**:
  - Category: `unspecified-high` - Reason: This spans rules, metrics, and reporting read models.
  - Skills: `[]` - No special skill required.
  - Omitted: `quick` - This is more than a single-file change.

  **Parallelization**: Can Parallel: YES | Wave 3 | Blocks: tasks 14, 15, 16 | Blocked By: tasks 5, 6, 10

  **References**:
  - `internal/metrics/registry.go` - metrics families, including quality/trend-related groups.
  - `internal/dashboardstats/service.go` - existing quality incident/statistics read path.
  - `cmd/api/internal_stats.go` - API surface for dashboard-style quality data.
  - `cmd/renderer/main.go` - renderer consumer of quality stats.

  **Acceptance Criteria**:
  - [ ] Quality rules can be created, updated, versioned, activated, and assigned an owner.
  - [ ] Trend history is queryable by dataset/run/time window.
  - [ ] Rule changes do not erase prior quality results.
  - [ ] The UI-facing stats endpoint can render quality trend data.

  **QA Scenarios**:
  ```
  Scenario: Create and activate a quality rule
    Tool: Bash
    Steps: Create a rule fixture, activate it, and run a sample evaluation.
    Expected: The rule appears as active and the evaluation result is recorded.
    Evidence: .sisyphus/evidence/task-12-quality-rule-happy.txt

  Scenario: Query quality trend history
    Tool: Bash
    Steps: Insert or replay multiple quality results over time and query the trend view.
    Expected: The response shows historical pass/fail movement rather than only the latest state.
    Evidence: .sisyphus/evidence/task-12-quality-trend.txt
  ```

  **Commit**: NO | Message: `n/a` | Files: [.sisyphus/plans/production-roadmap.md]

- [ ] 13. Dataset export/import + external object URI ingestion + artifact manifest retrieval

  **What to do**: Enable promotion of dataset definitions, rules, products, and pipelines between environments; ingest directly from already-landed object storage URIs; and expose manifests/artifacts for browsing and retrieval. Keep this task strictly on transport, ingestion, and manifest access.
  **Must NOT do**: Do not require re-uploading already-landed files through the API, do not make promotion a manual copy/paste process, and do not add catalog search/audit/product-version concerns here.

  **Recommended Agent Profile**:
  - Category: `unspecified-high` - Reason: This is a data-movement and artifact-access feature bundle.
  - Skills: `[]` - No special skill required.
  - Omitted: `quick` - This spans API, storage, and catalog behavior.

  **Parallelization**: Can Parallel: YES | Wave 4 | Blocks: task 15, 16 | Blocked By: tasks 1, 5, 7, 8, 9, 10, 12

  **References**:
  - `internal/sourcecatalog/catalog.go` - canonical catalog/metadata store pattern.
  - `cmd/bootstrap/source_catalog.go` - seed/catalog bootstrapping.
  - `internal/location/resolver.go` - location/URI resolution pattern.
  - `internal/place/materialize.go` - data placement/materialization lineage support.
  - `cmd/api/*` - new import/export and artifact retrieval endpoints.

  **Acceptance Criteria**:
  - [ ] Dataset/pipeline/rule/product definitions can be exported and imported across environments.
  - [ ] External object URIs can be ingested without re-uploading bytes through the API.
  - [ ] Artifact manifests are retrievable and browseable from the API layer.
  - [ ] Imported definitions preserve version/ownership metadata where applicable.

  **QA Scenarios**:
  ```
  Scenario: Export and import a dataset bundle
    Tool: Bash
    Steps: Export a known dataset configuration, import it into a clean environment, and compare the resulting definitions.
    Expected: The imported bundle matches the source bundle and remains runnable.
    Evidence: .sisyphus/evidence/task-13-export-import-happy.txt

  Scenario: Ingest from external object URI
    Tool: Bash
    Steps: Point ingestion at an already-landed object storage URI.
    Expected: The bytes are consumed directly and no duplicate upload path is required.
    Evidence: .sisyphus/evidence/task-13-external-uri.txt
  ```

  **Commit**: NO | Message: `n/a` | Files: [.sisyphus/plans/production-roadmap.md]

- [ ] 14. Searchable catalog + full audit trail + data product versioning

  **What to do**: Add search/tag/owner/status discovery for datasets and products using the canonical catalog metadata fields, persist human-readable and machine-queryable audit events for create/update/run/delete operations, and version frontend-facing data products with lineage to their query/config state. Keep this task strictly on discovery, audit, and version history.
  **Must NOT do**: Do not leave search limited to a single entity type, do not record audits as opaque logs only, and do not fold export/import transport or artifact-manifest retrieval into this task.

  **Recommended Agent Profile**:
  - Category: `unspecified-high` - Reason: This is a discovery/governance read-model bundle.
  - Skills: `[]` - No special skill required.
  - Omitted: `visual-engineering` - Backend read models first.

  **Parallelization**: Can Parallel: YES | Wave 4 | Blocks: task 15, 16 | Blocked By: tasks 1, 2, 5, 10, 12, 13

  **References**:
  - `internal/sourcecatalog/catalog.go` - canonical catalog field definitions for owner/environment/tag/status discovery.
  - `cmd/bootstrap/source_catalog.go` - seed-time catalog generation and metadata normalization.
  - `cmd/control-plane/jobs_discovery_candidates.go` - discovery candidate generation.
  - `migrations/clickhouse/0021_source_coverage_lineage.sql` - lineage read-model pattern.
  - `migrations/clickhouse/0023_source_coverage_runtime.sql` - runtime coverage read model.
  - `cmd/api/route_contracts.go` - API contract extension point for catalog/audit routes.

  **Acceptance Criteria**:
  - [ ] Users can search datasets/products/pipelines by tag, owner, environment, and status.
  - [ ] Create/update/run/delete operations emit durable audit events.
  - [ ] Data products have revision history that links to their config/query lineage.
  - [ ] Search and audit results are queryable via API read models, not only internal jobs.

  **QA Scenarios**:
  ```
  Scenario: Search the catalog
    Tool: Bash
    Steps: Create a few tagged assets and query the catalog with owner/tag filters.
    Expected: The correct assets are returned and ranked deterministically.
    Evidence: .sisyphus/evidence/task-14-catalog-search.txt

  Scenario: Emit an audit event
    Tool: Bash
    Steps: Create, update, and delete a tracked asset.
    Expected: Each operation creates a readable audit record with actor and timestamp.
    Evidence: .sisyphus/evidence/task-14-audit-trail.txt
  ```

  **Commit**: NO | Message: `n/a` | Files: [.sisyphus/plans/production-roadmap.md]

- [ ] 15. Lineage-first UI and operator dashboards

  **What to do**: Build the UI surfaces that let a user understand the system end-to-end: topology dashboard, dataset lineage graph, run timeline/Gantt, stage-by-stage execution view, live worker activity board, quality dashboard, schema evolution view, artifact browser, run detail page, data product map, searchable catalog UI, alert center, audit timeline, preview explorer, dependency impact view, and SLA/freshness dashboard.
  **Must NOT do**: Do not build these screens before the backend read models/routes they consume are ready, and do not make the UI depend on log spelunking.

  **Recommended Agent Profile**:
  - Category: `visual-engineering` - Reason: This is a UI/UX and information-architecture task.
  - Skills: `[]` - No special skill required.
  - Omitted: `quick` - This is a multi-screen product surface.

  **Parallelization**: Can Parallel: YES | Wave 4 | Blocks: task 16 | Blocked By: tasks 5, 6, 9, 12, 13, 14

  **References**:
  - `cmd/renderer/main.go` - current renderer entrypoint.
  - `cmd/api/internal_stats.go` - stats payload consumed by the UI.
  - `cmd/api/route_contracts.go` - API contract source for UI-backed views.
  - `internal/dashboardstats/service.go` - backend data source for dashboard cards and trends.
  - `migrations/clickhouse/0021_source_coverage_lineage.sql` and `0023_source_coverage_runtime.sql` - lineage/coverage read models.

  **Acceptance Criteria**:
  - [ ] The UI can explain topology, lineage, run status, quality, alerts, artifacts, and audit history without requiring logs or storage access.
  - [ ] Each dashboard route is backed by a stable API/read-model contract.
  - [ ] Lineage and schema evolution are presented visually with drill-down links.
  - [ ] The search and alert experiences are discoverable from the main UI entrypoint.

  **QA Scenarios**:
  ```
  Scenario: Open the lineage graph
    Tool: Playwright
    Steps: Visit the lineage route in the renderer and open a dataset node.
    Expected: The graph renders, nodes expand, and linked assets are reachable.
    Evidence: .sisyphus/evidence/task-15-lineage-ui.png

  Scenario: Inspect a run detail page
    Tool: Playwright
    Steps: Open a known run from the run timeline and inspect stage, errors, and artifacts.
    Expected: The page shows the execution path, timings, and linked artifacts without console errors.
    Evidence: .sisyphus/evidence/task-15-run-detail.png
  ```

  **Commit**: NO | Message: `n/a` | Files: [.sisyphus/plans/production-roadmap.md]

- [ ] 16. End-to-end integration tests

  **What to do**: Expand system-level tests so they cover real API → worker → storage → serving flows, not just package-level unit cases, and make sure the CI gates point at the Go runtime contract.
  **Must NOT do**: Do not treat stale compose workflows as authoritative integration coverage, and do not leave the e2e suite disconnected from the real binaries.

  **Recommended Agent Profile**:
  - Category: `unspecified-high` - Reason: This is cross-binary QA coverage.
  - Skills: `[]` - No special skill required.
  - Omitted: `visual-engineering` - Backend/system tests first.

  **Parallelization**: Can Parallel: YES | Wave 4 | Blocks: none | Blocked By: tasks 1, 2, 4, 5, 7, 8, 9, 12, 13, 14

  **References**:
  - `test/e2e/pipeline_test.go` - current tagged e2e flow.
  - `test/e2e/testdata/phase1/*` - e2e fixture data.
  - `cmd/api/contract_test.go` - contract test pattern to preserve.
  - `.github/workflows/e2e.yml` - tagged e2e CI gate.
  - `.github/workflows/unit.yml` and `.github/workflows/contract.yml` - other authoritative gates.

  **Acceptance Criteria**:
  - [ ] The e2e suite exercises a real stack with the Go binaries and storage backend.
  - [ ] A failure in API/worker/storage interaction is caught by the e2e suite.
  - [ ] Contract tests and e2e tests remain separate and both run in CI.
  - [ ] The suite has deterministic fixtures and bounded runtime.

  **QA Scenarios**:
  ```
  Scenario: Run the real e2e pipeline flow
    Tool: Bash
    Steps: Start the backing services, then run `go test ./test/e2e/... -tags=e2e -v`.
    Expected: The suite passes and verifies a complete flow across binaries.
    Evidence: .sisyphus/evidence/task-16-e2e-happy.txt

  Scenario: Catch an integration regression
    Tool: Bash
    Steps: Break one API/worker/storage contract in a fixture-controlled way and rerun the suite.
    Expected: The e2e suite fails at the affected step with a clear error.
    Evidence: .sisyphus/evidence/task-16-e2e-regression.txt
  ```

  **Commit**: NO | Message: `n/a` | Files: [.sisyphus/plans/production-roadmap.md]

## Final Verification Wave (MANDATORY — after ALL implementation tasks)
> 4 review agents run in PARALLEL. ALL must APPROVE. Present consolidated results to user and get explicit "okay" before completing.
> **Do NOT auto-proceed after verification. Wait for user's explicit approval before marking work complete.**
> **Never mark F1-F4 as checked before getting user's okay.** Rejection or user feedback -> fix -> re-run -> present again -> wait for okay.
- **Repo verification checklist**:
  - `go test ./...`
  - `go test ./cmd/api -run Contract`
  - `go test ./internal/migrate/...`
  - `go test ./cmd/bootstrap/...`
  - `go run ./cmd/bootstrap verify`
  - `CGO_ENABLED=0 go build ./...`
- [ ] F1. Plan Compliance Audit — oracle
- [ ] F2. Code Quality Review — unspecified-high
- [ ] F3. Real Manual QA — unspecified-high (+ playwright if UI)
- [ ] F4. Scope Fidelity Check — deep

## Commit Strategy
NO commit unless explicitly requested by the user.

## Success Criteria
- The roadmap is executable without further architecture decisions.
- The critical features are ordered before QoL/UI features.
- Each task is sized for agent execution and verification.
