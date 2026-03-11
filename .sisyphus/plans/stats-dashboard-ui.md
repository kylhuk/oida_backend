# Internal Stats Dashboard UI

## TL;DR
> **Summary**: Add a minimal internal operations dashboard served by `cmd/renderer` and styled with Tailwind CSS v4. Back it with a dedicated `GET /v1/internal/stats` contract in `cmd/api` so the UI can show real operational and dataset statistics without inventing new backend semantics.
> **Deliverables**:
> - Renderer-hosted dashboard UI on `:8090`
> - Dedicated internal stats JSON endpoint on `cmd/api`
> - Curated operational/domain statistics derived from existing `meta`, `ops`, `bronze`, `silver`, and `gold` data
> - Tailwind CSS v4 asset pipeline with minimal repo/tooling expansion
> - API, renderer, and browser/E2E verification for seeded local stack behavior
> **Effort**: Medium
> **Parallel**: YES - 2 waves
> **Critical Path**: 1 -> 2 -> 3 -> 5 -> 6 -> 7

## Context
### Original Request
Add a minimalistic UI, using Tailwind CSS v4, to show system statistics such as number of sources, number of rows per table, number of crawlers, and other meaningful stats available from the system, and visualize them beautifully.

### Interview Summary
- Dashboard is internal-ops oriented, not a public product surface.
- Implementation may add a dedicated stats endpoint/query layer.
- The UI should stay minimal and polished rather than becoming a full admin console.
- Tailwind CSS v4 is required.

### Metis Review (gaps addressed)
- Route ownership is fixed: UI is served by `cmd/renderer`, stats JSON by `cmd/api` at `GET /v1/internal/stats`.
- MVP is bounded to summary cards, curated operational panels, quality panels, and compact charts/tables; schema drift and a geographic map are explicitly excluded.
- Row counts use curated approximate counts from ClickHouse metadata for large tables instead of unbounded exact scans.
- “Number of crawlers” is resolved as queue/crawl breadth metrics because the repo does not persist worker-instance counts.
- Tailwind CSS v4 uses the smallest viable asset pipeline; no large SPA framework or charting framework is added.

## Work Objectives
### Core Objective
Ship an internal dashboard that gives operators a fast view of source coverage, dataset volume, queue health, freshness, parser quality, and output readiness using only existing system data plus a dedicated read-only stats contract.

### Deliverables
- Renderer route set expanded from `/health` to serve a dashboard shell and static assets.
- Dedicated internal stats API route returning a stable composed payload for the dashboard.
- Curated stats sections covering:
  - summary cards: total sources, enabled sources, disabled sources, active/running jobs, queued frontier items, unresolved queue depth, open quality incidents
  - storage cards/tables: approximate row counts for curated `bronze`, `silver`, and `gold` tables plus per-source bronze table counts
  - freshness panel: lag by source with thresholding and stale-source callouts
  - parser/fetch quality panel: 15-minute parser success and 24-hour fetch outcome summaries
  - output readiness panel: metric registry count, latest metric snapshot time, hotspot rows, cross-domain rows
- Tailwind CSS v4 minimal asset pipeline and dashboard visual system.
- Package tests, API contract tests, and browser/E2E verification for seeded local stack behavior.

### Definition of Done (verifiable conditions with commands)
- `go test ./cmd/api/... ./cmd/renderer/...` exits `0`.
- `go test ./test/e2e/... -tags=e2e -run TestStatsDashboard` exits `0`.
- `curl -fsS http://localhost:8080/v1/internal/stats | jq -r '.data.summary.sources_total | type'` prints `number`.
- `curl -fsS http://localhost:8080/v1/internal/stats | jq -r '.data.quality.parser_success.window_minutes'` prints `15`.
- `curl -fsS http://localhost:8080/v1/internal/stats | jq -r '.data.storage.table_rows[0].count_mode'` prints `approximate`.
- `curl -fsS http://localhost:8090/ | grep -q 'Pipeline overview'` exits `0`.

### Must Have
- UI is served by `cmd/renderer` on `:8090`.
- Stats JSON is served by `cmd/api` at `GET /v1/internal/stats`.
- Tailwind CSS v4 is used for styling.
- Dashboard panels are backed by real repo data sources, not fabricated numbers.
- Large-table row counts are curated and explicitly labeled approximate when sourced from `system.parts`/metadata.
- Empty-state and partial-failure states are rendered cleanly.
- All timestamps and freshness values are UTC-based.

### Must NOT Have (guardrails, AI slop patterns, scope boundaries)
- No full admin console, mutation workflows, auth redesign, or user management.
- No React/Vite/Next.js-style SPA framework or heavy charting/map dependency for this MVP.
- No unbounded `count(*)` scans across every table in the system.
- No direct renderer writes or background orchestration from the UI.
- No geolocation map or schema-drift panel in this first cut.
- No public-facing exposure of this dashboard via the existing user-facing API docs/schema list.

## Verification Strategy
> ZERO HUMAN INTERVENTION — all verification is agent-executed.
- Test decision: tests-after using Go package tests, API contract tests, browser verification, and E2E stack checks.
- QA policy: Every task includes executable happy-path and failure-path scenarios.
- Evidence: `.sisyphus/evidence/task-{N}-{slug}.{ext}`

## Execution Strategy
### Parallel Execution Waves
> Target: 5-8 tasks per wave. <3 per wave (except final) = under-splitting.
> Extract shared dependencies as Wave-1 tasks for max parallelism.

Wave 1: stats contract, query layer, asset pipeline foundations (`1-4`)
Wave 2: renderer UI, verification, docs, and integration (`5-7`)

### Dependency Matrix (full, all tasks)
- `1` blocks `2,3,5,6,7`
- `2` blocks `3,5,6,7`
- `3` blocks `5,6,7`
- `4` blocks `5,6,7`
- `5` blocks `6,7`
- `6` blocks `7`
- `7` blocks final verification only

### Agent Dispatch Summary
- Wave 1 -> 4 tasks -> `deep`, `ultrabrain`, `writing`
- Wave 2 -> 3 tasks -> `visual-engineering`, `deep`, `writing`

## TODOs
> Implementation + Test = ONE task. Never separate.
> EVERY task MUST have: Agent Profile + Parallelization + QA Scenarios.

- [ ] 1. Freeze the dashboard stats contract and route ownership

  **What to do**: Add a dedicated composed stats route in `cmd/api` at `GET /v1/internal/stats`, and keep the dashboard page itself in `cmd/renderer` at `/`. Freeze the JSON contract to four top-level sections: `summary`, `storage`, `quality`, and `outputs`, plus `generated_at` and `warnings`. Fix the concrete fields now: `summary` includes `sources_total`, `sources_enabled`, `sources_disabled`, `jobs_running`, `frontier_pending`, `frontier_retry`, `unresolved_open`, and `quality_open`; `storage` includes curated `table_rows` and `source_bronze_rows`; `quality` includes `freshness` and `parser_success`; `outputs` includes `metrics_total`, `latest_snapshot_at`, `hotspots_total`, and `cross_domain_total`.
  **Must NOT do**: Do not add the dashboard page to `/v1/schema`; do not expose write actions; do not leave field names or null behavior implicit.

  **Recommended Agent Profile**:
  - Category: `deep` — Reason: route ownership and payload contract drive every later task.
  - Skills: [] — repo-native Go/API work.
  - Omitted: [`playwright`] — no browser verification in this task.

  **Parallelization**: Can Parallel: NO | Wave 1 | Blocks: `2,3,5,6,7` | Blocked By: none

  **References**:
  - Pattern: `cmd/api/main.go:28` — centralized route registry pattern.
  - Pattern: `cmd/api/handlers_expanded.go:210` — accepted dedicated composed handler pattern outside `resourceSpec`.
  - Pattern: `cmd/api/AGENTS.md:18` — keep handlers contract-oriented and `FORMAT JSONEachRow` based.
  - API/Type: `docs/dashboards/quality-dashboards.md:5` — repo-defined freshness/parser-success concepts to encode.
  - Test: `cmd/api/handlers_test.go:117` — package-level API behavior testing style.

  **Acceptance Criteria**:
  - [ ] `go test ./cmd/api/... -run TestInternalStatsContract -count=1` exits `0`.
  - [ ] `curl -fsS http://localhost:8080/v1/internal/stats | jq -e '.data.summary.sources_total != null and .data.quality.parser_success.window_minutes == 15'` exits `0`.
  - [ ] `curl -fsS http://localhost:8080/v1/schema | jq -e '.data.endpoints | index("/v1/internal/stats") | not'` exits `0`.

  **QA Scenarios**:
  ```bash
  Scenario: Internal stats endpoint returns the frozen payload shape
    Tool: Bash
    Steps: go test ./cmd/api/... -run TestInternalStatsContract -count=1 && curl -fsS http://localhost:8080/v1/internal/stats
    Expected: test exits 0; JSON contains `summary`, `storage`, `quality`, `outputs`, `generated_at`, and `warnings`
    Evidence: .sisyphus/evidence/task-1-stats-contract.txt

  Scenario: Stats route stays internal to the dashboard implementation
    Tool: Bash
    Steps: curl -fsS http://localhost:8080/v1/schema
    Expected: response remains valid and does not advertise `/v1/internal/stats`
    Evidence: .sisyphus/evidence/task-1-stats-contract-error.txt
  ```

  **Commit**: YES | Message: `feat(api): define internal stats contract` | Files: `cmd/api/main.go`, `cmd/api/*_test.go`, `cmd/api/handlers_expanded.go` or new stats handler file under `cmd/api`

- [ ] 2. Implement the internal stats query layer over curated repo tables

  **What to do**: Add reusable read-only query code under `internal/` that composes dashboard metrics from existing tables/views. Use exact counts only for bounded operational tables (`ops.crawl_frontier`, `ops.unresolved_location_queue`, `ops.quality_incident`, active `ops.job_run` slices) and use curated approximate counts from ClickHouse metadata for large tables (`bronze.raw_document`, each `bronze.src_*_v1`, `silver.fact_event`, `silver.fact_observation`, `gold.metric_snapshot`, `gold.cross_domain_snapshot`). Derive freshness from latest successful fetch per source and parser success from rolling 15-minute `ops.parse_log` windows. Return explicit warnings when a subquery fails or a section is partial.
  **Must NOT do**: Do not query every table in the warehouse; do not use unbounded `count(*)` on large fact tables; do not invent a persisted dashboard table in this MVP.

  **Recommended Agent Profile**:
  - Category: `ultrabrain` — Reason: mixed exact/approximate aggregation and partial-failure semantics are the highest-risk logic in the feature.
  - Skills: [] — repo-native ClickHouse/Go work.
  - Omitted: [`playwright`] — no browser work.

  **Parallelization**: Can Parallel: NO | Wave 1 | Blocks: `3,5,6,7` | Blocked By: `1`

  **References**:
  - Pattern: `migrations/clickhouse/0003_ops_bronze.sql:1` — `ops.job_run`, `ops.crawl_frontier`, `ops.fetch_log`, `bronze.raw_document` sources.
  - Pattern: `migrations/clickhouse/0005_baseline_tables.sql:1` — `ops.parse_log`, `ops.unresolved_location_queue`, `ops.quality_incident`, silver/gold storage sources.
  - Pattern: `migrations/clickhouse/0006_source_governance.sql:36` — source totals and enabled/disabled state come from `meta.source_registry`.
  - Pattern: `migrations/clickhouse/0007_api_expansion_views.sql:139` — existing analytics shape for outputs.
  - Pattern: `migrations/clickhouse/0015_source_bronze_tables.sql:1` — curated per-source bronze row-count table set.
  - API/Type: `docs/dashboards/quality-dashboards.md:7` — freshness threshold/default semantics.
  - API/Type: `docs/dashboards/quality-dashboards.md:11` — parser success 15-minute window requirement.

  **Acceptance Criteria**:
  - [ ] `go test ./cmd/api/... -run TestStatsAggregator -count=1` exits `0`.
  - [ ] `curl -fsS http://localhost:8080/v1/internal/stats | jq -e '.data.storage.table_rows[] | select(.count_mode == "approximate")'` exits `0`.
  - [ ] `curl -fsS http://localhost:8080/v1/internal/stats | jq -e '.data.quality.freshness.threshold_seconds == 600'` exits `0`.

  **QA Scenarios**:
  ```bash
  Scenario: Dashboard stats compose exact and approximate metrics correctly
    Tool: Bash
    Steps: go test ./cmd/api/... -run TestStatsAggregator -count=1 && curl -fsS http://localhost:8080/v1/internal/stats
    Expected: test exits 0; queue/job counts are exact, curated storage counts are present and labeled `approximate`
    Evidence: .sisyphus/evidence/task-2-stats-queries.txt

  Scenario: Partial stats failure degrades gracefully
    Tool: Bash
    Steps: go test ./cmd/api/... -run TestStatsAggregatorPartialFailure -count=1
    Expected: exit 0 and response keeps healthy sections while adding a warning entry for the failed subsection
    Evidence: .sisyphus/evidence/task-2-stats-queries-error.txt
  ```

  **Commit**: YES | Message: `feat(api): aggregate dashboard stats from ops and gold data` | Files: `internal/*dashboard*` or equivalent shared package files, `cmd/api/*_test.go`

- [ ] 3. Expose the stats contract through `cmd/api`

  **What to do**: Add the concrete route handler for `GET /v1/internal/stats`, wire it in `newAPIMuxWithServer()`, use the shared query layer from Task 2, apply a dashboard-specific timeout, and return the standard API envelope via `respond()`/`respondError()`. Add package tests for success, unsupported query params, and upstream query failure.
  **Must NOT do**: Do not force this route through `resourceSpec`; do not return raw ClickHouse rows; do not bypass standard API envelopes.

  **Recommended Agent Profile**:
  - Category: `deep` — Reason: API integration must match existing route/contract conventions precisely.
  - Skills: [] — repo-native Go/API work.
  - Omitted: [`playwright`] — browser is not needed yet.

  **Parallelization**: Can Parallel: YES | Wave 1 | Blocks: `5,6,7` | Blocked By: `1,2`

  **References**:
  - Pattern: `cmd/api/main.go:28` — route registration shape.
  - Pattern: `cmd/api/handlers_expanded.go:210` — dedicated composed handler with timeout and envelope behavior.
  - Pattern: `cmd/api/handlers_test.go:117` — query stub testing style.
  - API/Type: `cmd/api/AGENTS.md:21` — all responses use `respond()` / `respondError()`.

  **Acceptance Criteria**:
  - [ ] `go test ./cmd/api/... -run 'TestInternalStatsContract|TestInternalStatsQueryFailure|TestInternalStatsRejectsUnsupportedParams' -count=1` exits `0`.
  - [ ] `curl -fsS http://localhost:8080/v1/internal/stats | jq -r '.api_version'` prints a non-empty string.
  - [ ] `curl -i "http://localhost:8080/v1/internal/stats?bad=1"` returns HTTP `400`.

  **QA Scenarios**:
  ```bash
  Scenario: Internal stats route returns the standard API envelope
    Tool: Bash
    Steps: go test ./cmd/api/... -run TestInternalStatsContract -count=1 && curl -fsS http://localhost:8080/v1/internal/stats
    Expected: test exits 0; response contains `api_version`, `generated_at`, and `data`
    Evidence: .sisyphus/evidence/task-3-stats-handler.txt

  Scenario: Invalid query params are rejected cleanly
    Tool: Bash
    Steps: go test ./cmd/api/... -run TestInternalStatsRejectsUnsupportedParams -count=1
    Expected: exit 0 and handler returns the standard `invalid_request` envelope
    Evidence: .sisyphus/evidence/task-3-stats-handler-error.txt
  ```

  **Commit**: YES | Message: `feat(api): add internal dashboard stats endpoint` | Files: `cmd/api/main.go`, `cmd/api/*stats*.go`, `cmd/api/*_test.go`

- [ ] 4. Add a minimal Tailwind CSS v4 asset pipeline for renderer-hosted UI

  **What to do**: Extend `cmd/renderer` from a health stub into a tiny static-site host. Add a minimal asset pipeline for Tailwind CSS v4 using the smallest possible build path: dedicated renderer UI source files and compiled CSS committed or generated in the renderer build flow without introducing a full SPA stack. Update `build/renderer.Dockerfile` so renderer images include the compiled dashboard assets. Keep the HTML shell intentionally small and server-hosted.
  **Must NOT do**: Do not introduce React/Vite/Next.js; do not depend on a browser CDN for Tailwind; do not leave renderer unable to serve assets in Docker.

  **Recommended Agent Profile**:
  - Category: `visual-engineering` — Reason: this is the design system and asset-pipeline foundation for the UI.
  - Skills: [`frontend-ui-ux`] — needed for a polished minimal visual direction.
  - Omitted: [`playwright`] — visual verification comes later.

  **Parallelization**: Can Parallel: YES | Wave 1 | Blocks: `5,6,7` | Blocked By: `1`

  **References**:
  - Pattern: `cmd/renderer/main.go:8` — current renderer ownership and route surface.
  - Pattern: `build/renderer.Dockerfile:1` — current build only compiles Go binary; must be updated to package UI assets.
  - Pattern: `docker-compose.yml:107` — renderer service stays on `:8090`.
  - API/Type: `README.md:225` — quality-dashboard framing to echo in copy and IA.

  **Acceptance Criteria**:
  - [ ] `go test ./cmd/renderer/... -count=1` exits `0`.
  - [ ] `docker compose build renderer` exits `0`.
  - [ ] `curl -fsS http://localhost:8090/ | grep -q '<title>'` exits `0`.

  **QA Scenarios**:
  ```bash
  Scenario: Renderer serves dashboard shell and static assets
    Tool: Bash
    Steps: go test ./cmd/renderer/... -count=1 && docker compose build renderer && curl -fsS http://localhost:8090/
    Expected: tests/build exit 0; renderer root serves HTML shell instead of only a health stub
    Evidence: .sisyphus/evidence/task-4-renderer-assets.txt

  Scenario: Renderer still preserves health endpoint
    Tool: Bash
    Steps: curl -fsS http://localhost:8090/health
    Expected: output remains `ok`
    Evidence: .sisyphus/evidence/task-4-renderer-assets-error.txt
  ```

  **Commit**: YES | Message: `feat(renderer): add dashboard asset pipeline` | Files: `cmd/renderer/*`, `build/renderer.Dockerfile`, renderer asset files

- [ ] 5. Build the dashboard UI shell and curated visual panels

  **What to do**: Implement a minimal, beautiful dashboard page in `cmd/renderer` that fetches `GET /v1/internal/stats` and renders five concrete sections only: hero summary cards, storage table, freshness table with threshold highlighting, parser/fetch quality cards plus a compact bar/sparkline treatment, and output readiness cards. Use restrained typography, whitespace, subtle borders, and a calm palette; prefer simple SVG/CSS bars over extra chart libraries. Ensure the layout works on mobile and desktop and gracefully handles loading, empty, partial-warning, and hard-error states.
  **Must NOT do**: Do not add filters, drill-down pages, map widgets, or realtime polling in this first cut; do not expose raw JSON on the page except for debug tests if explicitly hidden.

  **Recommended Agent Profile**:
  - Category: `visual-engineering` — Reason: this is the actual UI composition and experience work.
  - Skills: [`frontend-ui-ux`] — needed for a deliberate, non-boilerplate dashboard.
  - Omitted: [`playwright`] — implementation first, browser proof in the next task.

  **Parallelization**: Can Parallel: NO | Wave 2 | Blocks: `6,7` | Blocked By: `1,2,3,4`

  **References**:
  - Pattern: `docs/dashboards/quality-dashboards.md:7` — freshness panel content and threshold behavior.
  - Pattern: `docs/dashboards/quality-dashboards.md:11` — parser success panel expectations.
  - Pattern: `cmd/renderer/main.go:8` — renderer ownership.
  - API/Type: `cmd/api/main.go:28` — stats JSON comes from the API service, not renderer-owned DB queries.
  - Test: `test/e2e/pipeline_test.go:156` — existing end-to-end HTTP verification style.

  **Acceptance Criteria**:
  - [ ] `curl -fsS http://localhost:8090/ | grep -q 'Pipeline overview'` exits `0`.
  - [ ] `curl -fsS http://localhost:8090/ | grep -q 'Freshness'` exits `0`.
  - [ ] Page remains readable at mobile and desktop viewport widths in browser verification.

  **QA Scenarios**:
  ```bash
  Scenario: Dashboard renders all MVP panels from the stats contract
    Tool: Playwright
    Steps: Open `http://localhost:8090/`; wait for stats load; verify visible headings `Pipeline overview`, `Storage`, `Freshness`, `Parser success`, and `Outputs`
    Expected: all five sections render with non-empty numeric/text content
    Evidence: .sisyphus/evidence/task-5-dashboard-ui.png

  Scenario: Dashboard shows partial-warning state without collapsing layout
    Tool: Playwright
    Steps: Run fixture/stub state where one stats subsection returns a warning; open dashboard
    Expected: warning banner appears for the affected panel while the rest of the dashboard remains visible and styled correctly
    Evidence: .sisyphus/evidence/task-5-dashboard-ui-error.png
  ```

  **Commit**: YES | Message: `feat(renderer): render internal stats dashboard` | Files: `cmd/renderer/*`, renderer asset files

- [ ] 6. Add contract, package, browser, and E2E verification for the dashboard flow

  **What to do**: Add deterministic tests for the stats endpoint payload, renderer page rendering, and a browser/E2E path that exercises the dashboard against the local stack. Reuse the documented quality fixtures under `testdata/fixtures/quality` for exact freshness/parser-success copy and sample states. Add a dedicated E2E test entry such as `TestStatsDashboard` that verifies API stats JSON and renderer HTML together.
  **Must NOT do**: Do not rely on manual browser inspection; do not make tests depend on public internet or unstubbed external data.

  **Recommended Agent Profile**:
  - Category: `deep` — Reason: this task spans API tests, renderer tests, and stack-level browser verification.
  - Skills: [`playwright`] — needed for dashboard browser assertions.
  - Omitted: []

  **Parallelization**: Can Parallel: YES | Wave 2 | Blocks: `7` | Blocked By: `1,2,3,4,5`

  **References**:
  - Pattern: `cmd/api/handlers_test.go:117` — contract-style API tests.
  - Pattern: `test/e2e/pipeline_test.go:156` — end-to-end HTTP checks structure.
  - Pattern: `docs/dashboards/quality-dashboards.md:3` — fixture-driven dashboard verification guidance.
  - Test: `testdata/fixtures/quality/freshness_snapshot.json` — exact freshness fixture.
  - Test: `testdata/fixtures/quality/parser_success.json` — exact parser-success fixture.

  **Acceptance Criteria**:
  - [ ] `go test ./cmd/api/... ./cmd/renderer/... -count=1` exits `0`.
  - [ ] `go test ./test/e2e/... -tags=e2e -run TestStatsDashboard -count=1` exits `0`.
  - [ ] Playwright screenshot/assertion evidence is generated for desktop and mobile.

  **QA Scenarios**:
  ```bash
  Scenario: Dashboard works end-to-end against the local stack
    Tool: Bash
    Steps: go test ./test/e2e/... -tags=e2e -run TestStatsDashboard -count=1
    Expected: exit 0 and test verifies both `http://localhost:8080/v1/internal/stats` and `http://localhost:8090/`
    Evidence: .sisyphus/evidence/task-6-dashboard-e2e.txt

  Scenario: Browser layout stays intact on mobile and desktop
    Tool: Playwright
    Steps: Open dashboard at 1280x900 and 390x844; capture screenshots after data load
    Expected: no clipped headings, no overlapping cards, and all core panels remain visible at both sizes
    Evidence: .sisyphus/evidence/task-6-dashboard-e2e-mobile.png
  ```

  **Commit**: YES | Message: `test(renderer): verify dashboard contract and ui` | Files: `cmd/api/*_test.go`, `cmd/renderer/*_test.go`, `test/e2e/*`, `testdata/fixtures/quality/*`

- [ ] 7. Wire docs, compose expectations, and operator guidance for the dashboard

  **What to do**: Update repo documentation so operators know where the dashboard lives, what it shows, and how to verify it. Keep documentation tight: renderer root URL, internal stats API path, which metrics are exact vs approximate, and what is intentionally excluded from MVP. Update any compose/runtime notes needed for renderer/api coordination, but do not add a new service.
  **Must NOT do**: Do not overstate the dashboard as a full observability suite; do not document deferred schema-drift/map features as implemented.

  **Recommended Agent Profile**:
  - Category: `writing` — Reason: this is concise operator-facing documentation work.
  - Skills: [] — repo-native docs work.
  - Omitted: [`playwright`] — not needed.

  **Parallelization**: Can Parallel: YES | Wave 2 | Blocks: none | Blocked By: `1,5,6`

  **References**:
  - Pattern: `README.md:225` — existing dashboard/quality framing.
  - Pattern: `docs/dashboards/quality-dashboards.md:1` — dashboard concepts already documented.
  - Pattern: `docker-compose.yml:107` — renderer service remains the dashboard host.
  - Test: `test/AGENTS.md:15` — E2E tests should use operator-visible HTTP endpoints.

  **Acceptance Criteria**:
  - [ ] `grep -n "/v1/internal/stats\|8090" README.md docs/dashboards/quality-dashboards.md` returns the new operator guidance.
  - [ ] Docs state that curated large-table counts are approximate.
  - [ ] Docs do not claim schema drift or a geolocation map are live in this MVP.

  **QA Scenarios**:
  ```bash
  Scenario: Operator docs point to the correct dashboard and stats URLs
    Tool: Bash
    Steps: grep -n "/v1/internal/stats\|8090" README.md docs/dashboards/quality-dashboards.md
    Expected: output documents the renderer UI URL and internal stats API path
    Evidence: .sisyphus/evidence/task-7-dashboard-docs.txt

  Scenario: Docs accurately describe MVP boundaries
    Tool: Bash
    Steps: grep -n "approximate\|schema drift\|map" README.md docs/dashboards/quality-dashboards.md
    Expected: docs mention approximate counts and do not falsely claim deferred features are implemented
    Evidence: .sisyphus/evidence/task-7-dashboard-docs-error.txt
  ```

  **Commit**: YES | Message: `docs(dashboard): document internal stats ui` | Files: `README.md`, `docs/dashboards/quality-dashboards.md`, optional related runbook/docs files

## Final Verification Wave (4 parallel agents, ALL must APPROVE)
- [ ] F1. Plan Compliance Audit — oracle
- [ ] F2. Code Quality Review — unspecified-high
- [ ] F3. Real Manual QA — unspecified-high (+ playwright if UI)
- [ ] F4. Scope Fidelity Check — deep

## Commit Strategy
- Commit after task groups, not every file, using focused commits:
  - `feat(api): add internal dashboard stats contract`
  - `feat(renderer): add stats dashboard ui`
  - `test(e2e): cover stats dashboard flow`

## Success Criteria
- Operators can open `http://localhost:8090/` and immediately understand pipeline status, storage volume, and output readiness.
- Every visible number is traceable to a concrete table/view/query in the repo.
- The dashboard remains fast, minimal, and beautiful on desktop and mobile.
- The implementation fits existing service boundaries: API owns stats JSON, renderer owns UI.
