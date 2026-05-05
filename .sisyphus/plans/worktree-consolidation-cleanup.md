# Consolidate Dirty Worktrees and Remove Stale Worktree

## TL;DR
> **Summary**: Consolidate the dirty changes from the current repo and the external atlas worktree into a single clean `main` tree, then remove the stale worktree and leftover build/planning debris.
> **Deliverables**: single canonical repo state; cleaned source tree; removed external worktree; verified tests/build; clean git status.
> **Effort**: Large
> **Parallel**: YES - 3 waves
> **Critical Path**: snapshot → reconcile source changes → clean artifacts/worktree → verify

## Context
### Original Request
- Consolidate the dirty work from the current repo and the external atlas worktree into this repository, then delete the stale external worktree and the old mess around it.
### Interview Summary
- Current repo on `main` is dirty across source, docs, migrations, binaries, and `.sisyphus` artifacts; the external `atlas/open-plan-backlog-consolidation` worktree is also dirty with its own control-plane, pack, and planning changes.
- User confirmed TDD for verification.
- The checked-in README is stale relative to the live tree; the active repo identity comes from `AGENTS.md` and the current filesystem layout, which is why this plan targets the Go/ClickHouse workspace that actually exists here.
### Metis Review (gaps addressed)
- Snapshot both trees before deletion.
- Treat the current repo as canonical.
- Do not prune worktree metadata until the consolidated tree is verified clean.
- Clean up generated junk (`.tmp/`, stale notes, root binaries) separately from source reconciliation.
- Capture immutable backup artifacts before any destructive cleanup.

## Work Objectives
### Core Objective
- Produce one clean, canonical `main` tree in `/home/hal9000/docker/oida_backend` that contains the merged intent of both dirty worktrees.
### Deliverables
- Snapshot evidence for both worktrees.
- Reconciled source changes across API/bootstrap/control-plane/core packages.
- Cleaned generated artifacts and stale planning notes.
- Removed external worktree and stale git metadata.
- Passing Go tests/build verification.
### Definition of Done (verifiable conditions with commands)
- `git worktree list --porcelain` shows only the current repo.
- `git status --short --branch` is clean on `main`.
- `go test ./...` passes.
- `CGO_ENABLED=0 go build ./...` passes.
### Must Have
- Snapshot before deletion.
- TDD-oriented verification on touched packages.
- No unresolved conflict markers.
- No stale worktree directory.
### Must NOT Have (guardrails, AI slop patterns, scope boundaries)
- No deletion before backup.
- No hand-edited generated artifacts.
- No accidental removal of the current plan/draft artifacts until the handoff is complete.
- No worktree pruning until the tree is verified clean.

## Verification Strategy
> VERIFICATION RUNS ARE AGENT-EXECUTED; final plan completion still requires the review gate and explicit user approval.
- Test decision: TDD + existing Go test/build tooling
- QA policy: Every task has agent-executed scenarios
- Evidence: .sisyphus/evidence/task-{N}-{slug}.{ext}
- Fallback policy: if host `go` is unavailable, rerun the same test/build commands in the repo's containerized Go environment; if compose-backed e2e prerequisites are unavailable, record the skip reason and continue with unit/build verification.

### Preflight Guardrails
> Before any destructive cleanup, confirm the workspace identity and capture immutable rollback artifacts.
- Record `git rev-parse HEAD` (and branch name) for both worktrees.
- Save `git worktree list --porcelain` plus `git diff --binary` / bundle-style snapshots for both trees.
- Treat `AGENTS.md` + live tree layout as source of truth if the README disagrees.
- Keep all snapshot evidence in `.sisyphus/evidence/` inside the canonical repo; do not place backup artifacts in the stale external worktree.
- If the stale worktree contains unique commits or local refs, archive them as a bundle or local ref before deletion.
- Validate every referenced path against the live canonical tree before dispatching tasks 2-4; record a SHA→path manifest and a per-file disposition log (`keep` / `merge` / `regenerate` / `delete`) in the inventory evidence.

### Ownership Boundaries
- Task 2 owns `cmd/api`, `cmd/bootstrap`, `internal/discovery`, `internal/fetch`, `internal/migrate`, `internal/promote`, `internal/sourcecatalog`, `internal/dashboardstats`, `internal/parser`, `internal/observability`, and `internal/retry`.
- Task 3 owns `cmd/control-plane/*`, `internal/packs/geopolitical/*`, `internal/packs/safety/*`, and `test/e2e/pipeline_test.go`.
- Task 4 owns only generated/derived artifacts, root build outputs, and stale notes.
- Task 5 owns only worktree metadata and directory removal.
- Task 6 owns only verification.

## Execution Strategy
### Parallel Execution Waves
> Target: 5-8 tasks per wave. <3 per wave (except final) = under-splitting.
> Extract shared dependencies as Wave-1 tasks for max parallelism.

Wave 1: snapshot/inventory, artifact classification, and merge-prep tasks
Wave 2: source reconciliation tasks across shared Go packages and branch-specific changes
Wave 3: cleanup, worktree removal, and full verification

### Dependency Matrix (full, all tasks)
- 1 → none
- 2 → 1
- 3 → 1
- 4 → 1, 2, 3
- 5 → 1, 2, 3, 4
- 6 → 1, 2, 3, 4, 5
### Agent Dispatch Summary (wave → task count → categories)
- Wave 1 → 1 task → quick
- Wave 2 → 3 tasks → deep, deep, unspecified-low
- Wave 3 → 2 tasks → quick, deep

## TODOs
> Implementation + Test = ONE task. Never separate.
> EVERY task MUST have: Agent Profile + Parallelization + QA Scenarios.

- [ ] 1. Snapshot both dirty worktrees and inventory the delta

  **What to do**: Capture a backup snapshot of the current repo and the external atlas worktree before touching anything; record `git rev-parse HEAD`, `git worktree list --porcelain`, `git status --short --branch`, `git ls-files --others --exclude-standard`, and a file inventory that separates current-tree edits, external-tree edits, shared overlaps, generated artifacts, and stale planning notes. Save immutable diff/archive snapshots for each worktree as `.sisyphus/evidence/task-1-current-worktree.patch` and `.sisyphus/evidence/task-1-external-worktree.patch` before any deletion, plus untracked-file manifests for each tree and a SHA→path / disposition manifest for every referenced file path.
  **Must NOT do**: Do not delete, move, or rewrite any files yet; do not prune the external worktree before the snapshot exists.

  **Recommended Agent Profile**:
  - Category: `quick` - Reason: this is an inventory/snapshot pass with no code changes.
  - Skills: `[]` - no specialized toolchain needed.
  - Omitted: `deep` - not needed until reconciliation starts.

  **Parallelization**: Can Parallel: NO | Wave 1 | Blocks: [2, 3, 4, 5, 6] | Blocked By: none

  **References** (executor has NO interview context - be exhaustive):
  - Pattern: `AGENTS.md` - repo constraints and cleanup conventions.
  - Pattern: `cmd/api/main.go:28` - canonical API router entrypoint.
  - Pattern: `cmd/bootstrap/main.go:246` - bootstrap symmetry boundary.
  - Pattern: `cmd/control-plane/main.go:68` - job registration boundary.
  - Pattern: `internal/promote/pipeline.go:186` - promotion pipeline invariants.
  - Pattern: `internal/migrate/http_runner.go:12` - checksum-sensitive migration execution.
  - Pattern: `.sisyphus/notepads/open-plan-backlog-consolidation/` - stale planning debris to classify.
  - Pattern: `api`, `bootstrap`, `control-plane` - root build artifacts to classify before cleanup.

  **Acceptance Criteria** (agent-executable only):
  - [ ] Backup evidence files exist for both worktrees and include the current `git status`/`git worktree list` output.
  - [ ] Untracked-file manifests exist for both worktrees.
  - [ ] Immutable backup artifacts (HEAD SHAs plus binary diff/archive snapshots) exist before any deletion.
  - [ ] Snapshot artifacts are stored as named files for each worktree (`task-1-worktree-inventory.txt`, `task-1-current-worktree.patch`, `task-1-external-worktree.patch`) so the backup can be replayed.
  - [ ] Every referenced path in tasks 2-4 is validated against the live canonical tree and recorded in the disposition log.
- [ ] A file inventory exists that labels every dirty path as current-tree, external-tree, shared, generated, or stale-note.
- [ ] No source file content has changed during the snapshot pass.

  **QA Scenarios** (MANDATORY - task incomplete without these):
  ```
  Scenario: Happy path inventory
    Tool: Bash
    Steps: run `git rev-parse HEAD`, `git worktree list --porcelain`, `git status --short --branch` in both worktrees, and save the results plus a diff/archive snapshot to .sisyphus/evidence/task-1-worktree-inventory.txt.
    Expected: both worktrees are captured and the snapshot file is present without any repository edits.
    Evidence: .sisyphus/evidence/task-1-worktree-inventory.txt

  Scenario: Stale path / missing worktree
    Tool: Bash
    Steps: run the inventory command against a nonexistent worktree path or after temporarily moving the external worktree aside.
    Expected: the step fails safely and leaves both repositories untouched.
    Evidence: .sisyphus/evidence/task-1-worktree-inventory-error.txt
  ```

  **Commit**: NO | Message: `chore(consolidation): snapshot dirty worktrees` | Files: [.sisyphus/evidence/*]

- [ ] 2. Reconcile shared Go surface changes in the canonical tree

  **What to do**: Merge the overlapping edits that affect the current repo's core Go surface: API routing/contracts, bootstrap lifecycle and source registry generation, migration metadata handling, promotion pipeline behavior, discovery/fetch retention, source catalog helpers, dashboard stats plumbing, parser compatibility, and the new API/retry/observability support files. Keep the current repo as the source of truth and resolve any conflicts without changing the existing API envelope or migration checksum invariants.
  **Must NOT do**: Do not touch `cmd/control-plane/jobs_*` pack-specific logic yet; do not rewrite generated seed or docs artifacts in this task.

  **Recommended Agent Profile**:
  - Category: `deep` - Reason: this is the high-risk merge of the core surface area.
  - Skills: `[]` - no additional skills required.
  - Omitted: `quick` - too much cross-file reconciliation for a trivial profile.

  **Parallelization**: Can Parallel: YES | Wave 2 | Blocks: [5, 6] | Blocked By: 1

  **References** (executor has NO interview context - be exhaustive):
  - Pattern: `cmd/api/main.go:28` - route wiring and resource registration.
  - Pattern: `cmd/api/handlers.go:260` - list/detail contract shaping.
  - Pattern: `cmd/api/route_contracts.go` - API contract surface.
  - Pattern: `cmd/api/contract_test.go` / `cmd/api/handlers_test.go` / `cmd/api/main_test.go` / `cmd/api/internal_stats_test.go` - API verification patterns.
  - Pattern: `cmd/api/worker_tail.go` / `cmd/api/worker_tail_test.go` - new API tail/observability support.
  - Pattern: `cmd/bootstrap/main.go:246` - install/verify symmetry.
  - Pattern: `cmd/bootstrap/source_catalog.go` / `source_generation.go` / `source_registry.go` - source governance and seed generation.
  - Pattern: `cmd/bootstrap/migration_metadata.go` / `migration_metadata_test.go` - migration metadata handling.
  - Pattern: `cmd/bootstrap/source_bronze_migration.go` / `source_silver_coverage_test.go` - bronze/silver coverage bridging.
  - Pattern: `internal/discovery/*` - frontier/fingerprint/family logic.
  - Pattern: `internal/fetch/*` - retention and fetch client behavior.
  - Pattern: `internal/migrate/http_runner.go:12` - checksum-sensitive HTTP execution.
  - Pattern: `internal/promote/pipeline.go:186` - canonical stage-to-silver promotion.
  - Pattern: `internal/sourcecatalog/catalog.go` - source catalog semantics.
  - Pattern: `internal/dashboardstats/*` - dashboard stats rollups and bronze table mapping.
  - Pattern: `internal/parser/catalog_compatibility.go` / `catalog_compatibility_test.go` - parser/catalog compatibility bridge.
  - Pattern: `internal/observability/` / `internal/retry/` - new support packages.
  - Pattern: `README.md`, `docs/api-reference.md`, `docs/runbooks/*` - user-facing behavior descriptions that must remain consistent with the code.

  **Acceptance Criteria** (agent-executable only):
  - [ ] `go test` passes for the touched core packages.
  - [ ] `git grep -nE '^(<<<<<<<|=======|>>>>>>>)' -- cmd/api cmd/bootstrap internal/discovery internal/fetch internal/migrate internal/promote internal/sourcecatalog internal/dashboardstats internal/parser internal/observability internal/retry` returns no matches.
  - [ ] `git diff --check` is clean for the reconciled files.

  **QA Scenarios** (MANDATORY - task incomplete without these):
  ```
  Scenario: Happy path core merge verification
    Tool: Bash
    Steps: run `go test ./cmd/api ./cmd/bootstrap ./internal/discovery ./internal/fetch ./internal/migrate ./internal/promote ./internal/sourcecatalog ./internal/dashboardstats ./internal/parser ./internal/observability ./internal/retry`.
    Expected: all targeted core packages pass.
    Evidence: .sisyphus/evidence/task-2-core-merge-test.txt

  Scenario: Conflict-marker guard
    Tool: Bash
    Steps: run `git grep -nE '^(<<<<<<<|=======|>>>>>>>)' -- cmd/api cmd/bootstrap internal/discovery internal/fetch internal/migrate internal/promote internal/sourcecatalog internal/dashboardstats internal/parser internal/observability internal/retry`.
    Expected: no unresolved merge markers remain.
    Evidence: .sisyphus/evidence/task-2-core-merge-conflicts.txt
  ```

  **Commit**: NO | Message: `chore(consolidation): reconcile core go surface` | Files: [cmd/api/*, cmd/bootstrap/*, internal/discovery/*, internal/fetch/*, internal/migrate/*, internal/promote/*, internal/sourcecatalog/*, internal/dashboardstats/*, internal/parser/*, internal/observability/*, internal/retry/*, README.md, docs/*]

- [ ] 3. Reconcile control-plane jobs and pack-specific changes

  **What to do**: Merge the worktree-specific control-plane job updates and domain-pack logic into the canonical tree: discovery candidate jobs, geopolitical/safety jobs, HTTP source ingestion, promotion orchestration, place build/sync, and any pipeline-execution wiring. Bring along the matching runtime/contract tests so the control-plane registration surface, pack outputs, and end-to-end pipeline behavior stay aligned.
  **Must NOT do**: Do not rework the core API/bootstrap code from task 2; do not delete or regenerate fixtures yet; keep the job registration model deterministic.

  **Recommended Agent Profile**:
  - Category: `deep` - Reason: this is the branch-specific merge with the most cross-file coupling.
  - Skills: `[]` - no specialized skills needed.
  - Omitted: `quick` - too many dependent job and pack files.

  **Parallelization**: Can Parallel: YES | Wave 2 | Blocks: [5, 6] | Blocked By: 1

  **References** (executor has NO interview context - be exhaustive):
  - Pattern: `cmd/control-plane/main.go:68` - run-once job registry contract.
  - Pattern: `cmd/control-plane/jobs_discovery_candidates.go` / `jobs_discovery_candidates_test.go` - discovery job pattern.
  - Pattern: `cmd/control-plane/jobs_geopolitical.go` / `jobs_geopolitical_test.go` / `jobs_geopolitical_runtime_test.go` - geopolitical job and runtime coverage.
  - Pattern: `cmd/control-plane/jobs_http_sources.go` - HTTP source ingestion path.
  - Pattern: `cmd/control-plane/jobs_place_build.go` / `jobs_place_sync.go` - place graph build/sync flow.
  - Pattern: `cmd/control-plane/jobs_pipeline_execute.go` / `jobs_pipeline_execute_test.go` - pipeline execution wiring.
  - Pattern: `cmd/control-plane/jobs_promote.go` / `jobs_promote_test.go` - promotion orchestration.
  - Pattern: `cmd/control-plane/jobs_safety.go` / `jobs_safety_test.go` - safety domain pack job.
  - Pattern: `internal/packs/geopolitical/geopolitical.go` / `geopolitical_test.go` - geopolitical pack behavior.
  - Pattern: `internal/packs/safety/safety.go` / `safety_test.go` - safety pack behavior.
  - Pattern: `test/e2e/pipeline_test.go` - end-to-end pipeline contract.
  - Pattern: `internal/promote/pipeline_test.go` - promotion pipeline expectations to preserve while wiring jobs.

  **Acceptance Criteria** (agent-executable only):
  - [ ] Targeted control-plane and pack tests pass after the merge.
  - [ ] The control-plane job registry still registers the expected run-once jobs.
  - [ ] No unresolved merge markers remain in the job and pack files.

  **QA Scenarios** (MANDATORY - task incomplete without these):
  ```
  Scenario: Happy path control-plane verification
    Tool: Bash
    Steps: run `go test ./cmd/control-plane ./internal/packs/geopolitical ./internal/packs/safety`.
    Expected: the control-plane jobs and pack-specific unit tests pass.
    Evidence: .sisyphus/evidence/task-3-control-plane-pack-test.txt

  Scenario: Runtime job edge case
    Tool: Bash
    Steps: run `go test ./cmd/control-plane -run TestJobsGeopoliticalRuntime -count=1`, then run `git grep -nE '^(<<<<<<<|=======|>>>>>>>)' -- cmd/control-plane internal/packs`.
    Expected: runtime coverage passes and no conflict markers remain at line starts.
    Evidence: .sisyphus/evidence/task-3-control-plane-pack-runtime.txt
  ```

  **Commit**: NO | Message: `chore(consolidation): reconcile control-plane jobs` | Files: [cmd/control-plane/*, internal/packs/geopolitical/*, internal/packs/safety/*, test/e2e/pipeline_test.go]

- [ ] 4. Normalize generated artifacts, stale notes, and root build outputs

  **What to do**: Clean up the generated and derived artifacts so the repo contains only canonical sources. Bucket A: remove or rebuild the root ELF binaries (`api`, `control-plane`, `bootstrap`), clear `.tmp/`, and prune stale notes from `.sisyphus/notepads/open-plan-backlog-consolidation/`. Bucket B: regenerate the derived catalog, capability-matrix, fixture-manifest, seed, and migration outputs instead of hand-editing them, and record the exact generator command used for each artifact family in the evidence log. Policy: binaries come from build commands and JSON fixtures/manifests come from their generator; do not hand-edit those derived artifacts.
  **Must NOT do**: Do not delete the new consolidation plan/draft artifacts; do not touch source merge logic that belongs to tasks 2 or 3; do not leave generated files half-updated.

  **Recommended Agent Profile**:
  - Category: `unspecified-low` - Reason: this is hygiene/regen work with a clear checklist.
  - Skills: `[]` - no special skills needed.
  - Omitted: `deep` - the decision making is already done.

  **Parallelization**: Can Parallel: YES | Wave 2 | Blocks: [5, 6] | Blocked By: 1, 2, 3

  **References** (executor has NO interview context - be exhaustive):
  - Pattern: `.env.example` - public config placeholders must remain safe.
  - Pattern: `.sisyphus/notepads/open-plan-backlog-consolidation/` - stale notes to remove.
  - Pattern: `api`, `bootstrap`, `control-plane` - root build artifacts to remove/rebuild.
  - Pattern: `seed/source_catalog.json` / `seed/source_catalog_compiled.json` - derived catalog outputs.
  - Pattern: `docs/capability-matrix.md` / `docs/capability-matrix.json` - derived capability docs.
  - Pattern: `docker-compose.yml` / `docker/go.Dockerfile` - build/runtime packaging assumptions.
  - Pattern: `migrations/clickhouse/0028_schema_change_registry.sql` through `0033_observability_tail.sql` and `zz*_source_*` files - new migration artifacts to keep consistent.
  - Pattern: `testdata/http-fixture/manifest.json` / `testdata/http-fixture/geopolitical/` / `testdata/http-fixture/safety/` - generated HTTP fixtures and manifests.

  **Acceptance Criteria** (agent-executable only):
  - [ ] Root build outputs are either absent from git status or intentionally rebuilt from source with a documented reason.
  - [ ] `.tmp/` and stale notepad trees are gone.
  - [ ] Derived JSON/fixture/migration outputs match the source inputs and no longer look hand-edited.

  **QA Scenarios** (MANDATORY - task incomplete without these):
  ```
  Scenario: Bucket A hygiene cleanup
    Tool: Bash
    Steps: verify `test ! -d .tmp`, verify `test ! -d .sisyphus/notepads/open-plan-backlog-consolidation`, and run `git status --short` to confirm the root binaries are either removed or intentionally rebuilt.
    Expected: build outputs and stale notes are gone, and only canonical source files remain dirty, if any.
    Evidence: .sisyphus/evidence/task-4-artifact-cleanup.txt

  Scenario: Bucket B regeneration
    Tool: Bash
    Steps: confirm the derived docs/seed/fixture/migration outputs were regenerated from their source-of-truth inputs, then run `git status --short` and ensure no stale generated artifact paths such as `.tmp/` or the old notepad tree remain.
    Expected: regenerated outputs match their inputs and no stale generated junk survives.
    Evidence: .sisyphus/evidence/task-4-artifact-cleanup-guard.txt
  ```

  **Commit**: NO | Message: `chore(consolidation): clean generated artifacts` | Files: [.env.example, docs/*, seed/*, testdata/http-fixture/*, migrations/clickhouse/*, api, bootstrap, control-plane, .tmp/, .sisyphus/notepads/*]

- [ ] 5. Remove the external worktree and stale git metadata

  **What to do**: After the backup snapshot and source/hygiene reconciliation are stable, unregister and delete `/home/hal9000/docker/oida_backend_open_plan_backlog_consolidation` (update the path if the preflight inventory shows a different stale worktree location), then prune any stale worktree metadata so the current repo is the only remaining working tree.
  **Must NOT do**: Do not remove the external worktree before the snapshot exists; do not delete the branch history until the current repo is verified clean.

  **Recommended Agent Profile**:
  - Category: `quick` - Reason: this is a deterministic cleanup once the source merge is done.
  - Skills: `[]` - no special skills needed.
  - Omitted: `deep` - the merge decisions are already handled elsewhere.

  **Parallelization**: Can Parallel: NO | Wave 3 | Blocks: [6] | Blocked By: 1, 2, 3, 4

  **References** (executor has NO interview context - be exhaustive):
  - Pattern: `git worktree list --porcelain` - authoritative worktree inventory.
  - Pattern: `/home/hal9000/docker/oida_backend_open_plan_backlog_consolidation` - stale external worktree path to delete.
  - Pattern: `AGENTS.md` - cleanup constraints and repo conventions.

  **Acceptance Criteria** (agent-executable only):
  - [ ] `git worktree list --porcelain` shows only `/home/hal9000/docker/oida_backend` on `main`.
  - [ ] The stale path to delete was re-derived from the latest `git worktree list --porcelain` output immediately before removal.
  - [ ] `test ! -d /home/hal9000/docker/oida_backend_open_plan_backlog_consolidation` passes.
  - [ ] No stale `.git/worktrees` metadata for the deleted path remains.

  **QA Scenarios** (MANDATORY - task incomplete without these):
  ```
  Scenario: Happy path worktree removal
    Tool: Bash
    Steps: unregister the stale worktree, delete the directory, then run `git worktree list --porcelain`.
    Expected: only the current repo remains registered.
    Evidence: .sisyphus/evidence/task-5-worktree-removal.txt

  Scenario: Removal guard
    Tool: Bash
    Steps: run `test -d /home/hal9000/docker/oida_backend_open_plan_backlog_consolidation` and `git worktree list --porcelain` after cleanup.
    Expected: the directory is gone and the worktree list no longer mentions it.
    Evidence: .sisyphus/evidence/task-5-worktree-removal-guard.txt
  ```

  **Commit**: NO | Message: `chore(consolidation): remove stale worktree metadata` | Files: []

- [ ] 6. Run final TDD verification and finish the consolidation

  **What to do**: Execute the full verification stack against the consolidated tree: package-level tests for every touched area, the repository-wide Go suite, the end-to-end pipeline tests, and a static build. Finish by proving the tree is clean and that no second worktree remains.
  **Must NOT do**: Do not reintroduce generated outputs or stale worktree metadata; do not declare success until the final `git status` is clean.

  **Recommended Agent Profile**:
  - Category: `deep` - Reason: this is the final cross-cutting verification wave.
  - Skills: `[]` - no special skills needed.
  - Omitted: `quick` - this is a repo-wide validation pass.

  **Parallelization**: Can Parallel: NO | Wave 3 | Blocks: none | Blocked By: 1, 2, 3, 4, 5

  **References** (executor has NO interview context - be exhaustive):
  - Pattern: `test/e2e/pipeline_test.go` - end-to-end pipeline verification.
  - Pattern: `cmd/api/contract_test.go` / `cmd/bootstrap/source_registry_test.go` / `cmd/control-plane/jobs_promote_test.go` / `internal/migrate/http_runner_test.go` / `internal/promote/pipeline_test.go` - key package-level checks.
  - Pattern: `go test ./...` - repository-wide Go test sweep.
  - Pattern: `CGO_ENABLED=0 go build ./...` - static build verification.
  - Pattern: `git status --short --branch` - final cleanliness check.

  **Acceptance Criteria** (agent-executable only):
  - [ ] `git worktree list --porcelain` and `git status --short --branch` are captured before the test/build sweep and again after it.
  - [ ] `go test ./...` passes.
  - [ ] `go test ./test/e2e/... -tags=e2e` passes if the e2e environment is available.
  - [ ] `CGO_ENABLED=0 go build ./...` passes.
  - [ ] `git status --short --branch` shows a clean `main` tree.
  - [ ] `git worktree list --porcelain` still shows only the current repo.

  **QA Scenarios** (MANDATORY - task incomplete without these):
  ```
  Scenario: Happy path final verification
    Tool: Bash
    Steps: run `git worktree list --porcelain` and `git status --short --branch` before the sweep, run `go test ./...`, then `CGO_ENABLED=0 go build ./...`, then run `git worktree list --porcelain` and `git status --short --branch` again.
    Expected: tests/build pass and the tree is clean.
    Evidence: .sisyphus/evidence/task-6-final-verification.txt

  Scenario: Regression guard
    Tool: Bash
    Steps: run the final verification after one of the cleanup outputs was left behind.
    Expected: the command sequence fails loudly and surfaces the remaining dirty path or test failure.
    Evidence: .sisyphus/evidence/task-6-final-verification-failure.txt
  ```

  **Commit**: NO | Message: `chore(consolidation): finish verification sweep` | Files: []

## Final Verification Wave (MANDATORY — after ALL implementation tasks)
> 4 review agents run in PARALLEL. ALL must APPROVE. Present consolidated results to user and get explicit "okay" before completing.
> **Do NOT auto-proceed after verification. Wait for user's explicit approval before marking work complete.**
> **Never mark F1-F4 as checked before getting user's okay.** Rejection or user feedback -> fix -> re-run -> present again -> wait for okay.
- [ ] F1. Plan Compliance Audit — oracle
- [ ] F2. Code Quality Review — unspecified-high
- [ ] F3. Real Manual QA — unspecified-high (+ playwright if UI)
- [ ] F4. Scope Fidelity Check — deep

## Commit Strategy
- Do not create a commit unless explicitly requested; keep the consolidation work focused on producing one clean working tree.

## Success Criteria
- Current repo is the only remaining worktree.
- Dirty current and external changes are consolidated without unresolved conflicts.
- Generated debris and stale notes are removed.
- Full verification passes with a clean `git status`.
