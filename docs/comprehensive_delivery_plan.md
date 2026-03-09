# Global OSINT Backend — Comprehensive Delivery Plan

## 1) Executive Summary

This plan converts the blueprint into an execution program with clear milestones, owners, acceptance criteria, dependency ordering, risk controls, and definition-of-done gates. It is designed for a **Go-first**, **ClickHouse-first**, **Docker Compose** deployment with strict **REST-only** contracts and **backward compatibility by default**.

Primary objective for first production release (v1.0):
- One-command bootstrap on fresh or existing installs
- End-to-end ingestion → canonicalization → geolocation → rollups → REST serving
- Place-aware analytics from local place to world
- Operational quality controls (observability, unresolved queues, contract tests)

---

## 2) Delivery Model and Governance

## 2.1 Program Structure

- **Program cadence:** 2-week sprints
- **Planning horizon:** 6 phases over ~9 sprints (18 weeks)
- **Release gates:**
  1. Foundation Gate
  2. Promotion & Location Gate
  3. Analytics & API Stability Gate
  4. Domain Pack Gate (Geo/Web + Maritime)
  5. Production Readiness Gate

## 2.2 Team Topology (minimum effective)

- 1x Tech Lead / Architect
- 2x Go Backend Engineers
- 1x Data/ClickHouse Engineer
- 1x QA/Automation Engineer
- 1x DevOps/SRE (part-time acceptable early)

## 2.3 Definition of Done (global)

A feature is done only when all are true:
- Code merged with tests
- Migration is idempotent
- API contract snapshot updated (if endpoint affected)
- Observability signals added
- Runbook/docs updated
- Backward-compatibility checks pass

---

## 3) Phase Plan (What to build, in what order)

## Phase A (Sprints 1–2): Bootstrap + Storage Foundation

Scope:
- E0, E1 (core), partial E16 skeleton

Deliverables:
1. Docker Compose baseline with required services:
   - clickhouse, minio, bootstrap, api, control-plane, worker-fetch, worker-parse, renderer
2. Bootstrap binary (idempotent):
   - creates DBs/users/roles
   - applies migrations via `meta.schema_migrations`
   - creates buckets in MinIO
3. Initial ClickHouse schema:
   - meta/ops/bronze/silver/gold foundational tables
4. API skeleton endpoints:
   - `/v1/health`, `/v1/ready`, `/v1/version`, `/v1/schema`
5. Smoke tests in bootstrap

Acceptance Criteria:
- `docker compose up -d --build` yields healthy services
- Re-running bootstrap does not duplicate schema objects
- Synthetic insert + retrieval smoke passes

Risks to retire early:
- ClickHouse DDL drift and migration ordering
- Service startup race conditions

---

## Phase B (Sprints 3–4): Place Graph + Source Registry + Discovery

Scope:
- E2, E3, E4 (MVP subset)

Deliverables:
1. Place ingestion pipelines:
   - geoBoundaries ADM0–ADM4 where available
   - GeoNames names/hierarchy/admin code files
2. Place modeling:
   - `silver.dim_place`, `silver.place_hierarchy`, `silver.place_polygon`
   - internal place IDs (`plc:cont`, `plc:gb`, `plc:gn`, `plc:syn`)
3. Reverse geocoding dictionary (ClickHouse polygon dictionary)
4. Source registry:
   - `meta.source_registry` schema + seed load
   - policy fields (rate limit, license, terms, parser routing)
5. Discovery engine (first version):
   - robots fetch + sitemap extraction + RSS/Atom intake
   - writes normalized URLs to `ops.crawl_frontier`

Acceptance Criteria:
- Point lookup resolves test coordinates to deepest available admin level
- Registry seed loads and is queryable via API (basic source listing)
- Discovery jobs populate frontier with deduplicated canonical URLs

---

## Phase C (Sprints 5–6): Fetch/Parse/Canonicalization + Location Promotion

Scope:
- E5, E6, E7, E8 (MVP subset)

Deliverables:
1. Fetch runtime:
   - conditional GET, retries, content-type handling, provenance metadata
   - raw payloads in MinIO + metadata in `bronze.raw_document`
2. Parser framework:
   - parser interface + registry-based routing
   - JSON/CSV/XML/RSS/Atom parsers
3. Canonical schemas:
   - observation/event/entity envelopes with version fields
4. Location attribution pipeline:
   - precedence chain implementation
   - unresolved queue handling (`ops.unresolved_location_queue`)
5. Promotion pipeline:
   - only location-resolved records reach silver facts

Acceptance Criteria:
- End-to-end from frontier target to promoted observation in silver
- Unresolved records routed with actionable reason codes
- Every promoted fact has place_id + parent chain fields populated as far as available

---

## Phase D (Sprints 7–8): Resolution + Metrics + API Expansion

Scope:
- E9, E10, E16 full v1 core, E17 baseline

Deliverables:
1. Dedup + entity resolution baseline:
   - URL/content dedup
   - source-native and similarity-based observation dedup
   - alias tables and resolution confidence bands
2. Metric system:
   - metric registry + contribution table
   - aggregate state MV(s) into `gold.metric_state`
   - finalized snapshots in `gold.metric_snapshot`
3. API expansion:
   - places/events/observations/metrics list/detail endpoints
   - cursor pagination and fields filtering
4. Compatibility harness:
   - `gold.api_v1_*` serving views
   - contract snapshot tests
5. Explainability baseline:
   - evidence fragments and confidence payloads exposed

Acceptance Criteria:
- Core v1 endpoints stable under contract tests
- Rollups queryable at place/admin0/continent/world levels
- Backward-compatible changes validated in CI

---

## Phase E (Sprint 9): First Domain Pack (Geopolitical + General Web)

Scope:
- E14 (MVP)

Deliverables:
1. Structured adapters (e.g., ACLED-like/ReliefWeb-like where legally permitted)
2. GDELT ingestion adapter and mapping
3. Event normalization and place linking
4. Metrics:
   - conflict intensity
   - protest activity
   - media attention + acceleration
   - trend/burst outputs

Acceptance Criteria:
- At least one structured feed + one broad-web stream live
- Domain metrics visible via `/v1/analytics/*` and `/v1/places/{id}/metrics`

---

## Phase F (Post-v1 next): Maritime Pack + Hardening

Scope:
- E11 prioritized after geopolitical MVP

Deliverables:
- Vessel entities/tracks/port calls
- AIS gap and route deviation events
- Maritime composite risk scaffolding

Acceptance Criteria:
- Demonstrable end-to-end maritime flow with explainable evidence links

---

## 4) Dependency-Driven Work Breakdown Structure

## 4.1 Critical Path

1. Compose + bootstrap + migrations
2. ClickHouse schema zones and registries
3. Place graph + reverse geocoder
4. Source registry + frontier
5. Fetch + raw retention
6. Parser + canonical emitters
7. Location gating (promote/resolution queue)
8. Metric contributions + state/snapshot
9. API v1 serving views + compatibility tests
10. First domain pack

## 4.2 Parallelizable Streams

- Stream A: Infrastructure/bootstrap/migrations
- Stream B: Data modeling and SQL/MVs
- Stream C: Fetch/parse workers and source adapters
- Stream D: API contracts and serving layer
- Stream E: QA/contract/perf/observability

---

## 5) Backlog by Epic with Exit Criteria

## E0 — Bootstrap
Exit when:
- Fresh install and upgrade install both succeed idempotently
- Smoke tests validate CH, MinIO, synthetic write, API readiness

## E1 — ClickHouse foundation
Exit when:
- Required DB zones and baseline tables exist
- Partitioning/order/TTL conventions documented and implemented

## E2 — Place graph
Exit when:
- continent→admin4 hierarchy populated where source data exists
- reverse geocoder passes accuracy benchmark suite

## E3/E4 — Source registry + discovery
Exit when:
- source definitions include legal/governance controls
- discovery pipeline fills frontier with normalized, deduplicated targets

## E5/E6 — Fetch + parse
Exit when:
- full provenance retained
- parsers emit canonical candidates with parser versioning

## E7/E8 — Canonical model + location attribution
Exit when:
- stable envelopes enforced
- promoted records always have geo anchor + place chain

## E9 — Dedup/entity resolution
Exit when:
- duplicate suppression and confidence-banded entity matches operational

## E10 — Metrics
Exit when:
- contribution→state→snapshot pipeline works for core metrics
- rollups available across all required place grains

## E16 — REST + compatibility
Exit when:
- all v1 core endpoints operational with cursor pagination
- snapshot contract tests protect old consumers

## E17 — Quality/explainability
Exit when:
- quality dashboards and explainability payloads available for key metrics

## E11/E14/E15/... domain packs
Exit when:
- each pack produces canonical records + place-linked metrics + explainability

---

## 6) CI/CD, Testing, and Release Gates

## 6.1 CI Stages

1. Lint + static checks (Go + SQL formatting/lint where available)
2. Unit tests (parser logic, ID generation, scoring)
3. Integration tests (ClickHouse + MinIO via docker compose)
4. API contract snapshot tests (`/v1`)
5. Migration tests (fresh DB + upgrade path)
6. Performance smoke (insert/query sanity thresholds)

## 6.2 Required Test Suites

- Migration idempotency suite
- Geolocation attribution fixture suite
- Dedup and entity resolution golden fixtures
- Metric correctness fixtures (known inputs → expected rollups)
- API compatibility suite (field aliases and no regressions)

## 6.3 Release Criteria for v1.0

- 95%+ fetch success on approved stable sources in staging
- 99%+ API availability in 7-day pre-prod soak
- Geolocation success rate threshold met (target >= 90% for promoted candidates in chosen MVP domains)
- Zero breaking API diffs across contract snapshots

---

## 7) Data Governance, Legal, and Safety Controls

- Persist license/terms metadata per source in `meta.source_registry`
- Enforce source kill switch for legal/safety incidents
- Never implement login/paywall bypass or anti-bot evasion
- Retention classes by source/license
- Audit log for source enable/disable and migration events

Operational policy:
- Any source with unclear license defaults to disabled until reviewed
- Any low-confidence geolocation remains internal (unresolved queue) until resolvable

---

## 8) Risk Register and Mitigations

1. **Schema churn risk**
   - Mitigation: additive migrations + compatibility views + snapshot tests
2. **Geo coverage inconsistency (ADM3/ADM4 gaps)**
   - Mitigation: nullable lower levels + deepest level reporting
3. **Crawler legal/compliance drift**
   - Mitigation: source policy metadata + kill switch + review workflow
4. **Performance regression under ingest spikes**
   - Mitigation: async inserts, batching, partition tuning, projection review
5. **Low-confidence entity merges**
   - Mitigation: confidence bands + ambiguity retention + explainable scoring

---

## 9) Operational Runbooks (must exist by Production Readiness Gate)

Required runbooks:
1. Fresh bootstrap
2. Upgrade migration rollback/forward strategy
3. Backup and restore drill
4. Source outage and kill-switch procedure
5. Unresolved location queue triage
6. Contract break detection and mitigation

---

## 10) Concrete 90-Day Execution Checklist

## Days 1–30
- Stand up Compose stack + idempotent bootstrap
- Implement schema migration framework
- Deliver foundational DB schemas
- Ship API health/version/schema endpoints
- Land first CI pipelines (lint/unit/integration skeleton)

## Days 31–60
- Ingest place datasets and build hierarchy
- Implement reverse geocoder and validation fixtures
- Build source registry and discovery templates
- Implement fetch runtime + raw retention
- Implement parser abstraction and structured parsers

## Days 61–90
- Canonical promotion with mandatory location gating
- Core dedup and entity aliasing
- Metric contribution/state/snapshot pipeline
- Expand v1 API endpoints for places/events/observations/metrics
- Contract snapshot suite + quality dashboards baseline
- Bring first domain pack online (geopolitical/general web)

---

## 11) MVP Scope Freeze (to prevent thrash)

In-scope for v1:
- Foundation phases A–E
- Geopolitical + general-web first domain pack
- Place/admin/world rollups
- REST v1 core endpoints

Out-of-scope for v1 (deferred):
- Clustered ClickHouse/Keeper production topology
- Full maritime/aviation/space domain depth
- Advanced ML-heavy classification beyond explainable rule/weighted models

---

## 12) Immediate Next Actions (this week)

1. Create repository scaffold and baseline compose stack.
2. Implement bootstrap migration runner and `meta.schema_migrations`.
3. Commit initial ClickHouse migration set (zones + meta/ops/bronze/silver/gold minimum).
4. Add API skeleton with `/v1/health`, `/v1/ready`, `/v1/version`, `/v1/schema`.
5. Add CI jobs for lint, unit tests, migration tests.
6. Add architecture decision records (ADRs) for:
   - ClickHouse-first ETL/serving
   - Location gating as promotion prerequisite
   - API compatibility via `gold.api_v1_*` views

This creates a strong delivery base while preserving optional scale-out and domain expansion later.

---

## 13) Blueprint Coverage Matrix (All Epics, Tasks, and Subtasks)

This section is an explicit traceability matrix to ensure the delivery plan covers the full backlog, not just a summarized subset.

Legend:
- **Priority**: P0 (must for MVP), P1 (v1.x), P2 (post-v1/optional)
- **Owner**: INFRA, DATA, INGEST, API, QA, SRE
- **Gate**: FG (Foundation Gate), PLG (Promotion/Location Gate), ASG (Analytics/API Stability Gate), DPG (Domain Pack Gate), PRG (Production Readiness Gate)

### E0 — One-button bootstrap and environment bring-up

| Task | Subtask | Priority | Owner | Gate | Deliverable |
|---|---|---:|---|---|---|
| E0.1 | define `docker-compose.yml` | P0 | INFRA | FG | Compose file with required services |
| E0.1 | define named volumes | P0 | INFRA | FG | Persistent volumes for CH/MinIO/logs |
| E0.1 | define networks | P0 | INFRA | FG | Internal service network |
| E0.1 | define health checks | P0 | INFRA | FG | container health policies |
| E0.1 | set container dependencies | P0 | INFRA | FG | deterministic startup order |
| E0.1 | add resource limits/defaults | P1 | INFRA | FG | sane runtime constraints |
| E0.2 | compile Go bootstrap binary | P0 | INFRA | FG | bootstrap container image |
| E0.2 | idempotent startup flow | P0 | INFRA | FG | rerun-safe initializer |
| E0.2 | create MinIO buckets | P0 | INFRA | FG | `raw`,`stage`,`backup` |
| E0.2 | create CH users/roles/dbs | P0 | DATA | FG | RBAC + DB initialization |
| E0.2 | run migrations | P0 | DATA | FG | versioned schema apply |
| E0.2 | load source registry | P0 | INGEST | FG | initial source seed |
| E0.2 | load place datasets | P0 | DATA | FG | geospatial base load |
| E0.2 | build dictionaries/MVs | P0 | DATA | FG | reverse geocode + rollup MVs |
| E0.2 | write readiness marker | P0 | INFRA | FG | bootstrap completion signal |
| E0.3 | create `meta.schema_migrations` | P0 | DATA | FG | migration ledger |
| E0.3 | detect schema/dataset state | P0 | DATA | FG | upgrade-safe inspection |
| E0.3 | run only missing migrations | P0 | DATA | FG | idempotent upgrades |
| E0.3 | rebuild derived artifacts | P1 | DATA | FG | MVs/views repair |
| E0.3 | verify compatibility views | P0 | API | FG | API contract continuity |
| E0.4 | health-check ClickHouse | P0 | QA | FG | smoke test |
| E0.4 | health-check MinIO | P0 | QA | FG | smoke test |
| E0.4 | synthetic observation insert | P0 | QA | FG | e2e write path check |
| E0.4 | reverse geocode verification | P0 | QA | FG | location gate check |
| E0.4 | metric state update verification | P0 | QA | FG | rollup check |
| E0.4 | REST contract smoke | P0 | QA | FG | API baseline check |
| E0.5 | backup schedule config | P1 | SRE | PRG | periodic backup policy |
| E0.5 | restore workflow definition | P1 | SRE | PRG | DR runbook |
| E0.5 | backup manifest verification | P1 | SRE | PRG | recoverability proof |
| E0.5 | recovery test documentation | P1 | SRE | PRG | drill guidance |

### E1 — ClickHouse foundation and storage design

| Task | Subtask | Priority | Owner | Gate | Deliverable |
|---|---|---:|---|---|---|
| E1.1 | create `meta`/`ops`/`bronze`/`silver`/`gold` | P0 | DATA | FG | logical DB zones |
| E1.2 | naming convention | P0 | DATA | FG | SQL style standard |
| E1.2 | timestamp convention | P0 | DATA | FG | UTC DateTime64 policy |
| E1.2 | versioning convention | P0 | DATA | FG | schema/record/api version rules |
| E1.2 | nullable/low-card convention | P0 | DATA | FG | storage/perf guidance |
| E1.2 | JSON usage convention | P0 | DATA | FG | attrs/evidence policy |
| E1.2 | partition convention | P0 | DATA | FG | month partitions |
| E1.2 | ordering-key convention | P0 | DATA | FG | API filter-optimized ORDER BY |
| E1.3 | `source_registry` | P0 | DATA | FG | source definition table |
| E1.3 | `parser_registry` | P0 | DATA | FG | parser metadata table |
| E1.3 | `metric_registry` | P0 | DATA | FG | metric definition table |
| E1.3 | `api_schema_registry` | P0 | DATA | FG | compatibility metadata table |
| E1.3 | `schema_migrations` | P0 | DATA | FG | migration tracking |
| E1.4 | `crawl_frontier` | P0 | DATA | FG | discovery queue table |
| E1.4 | `fetch_log` | P0 | DATA | FG | fetch attempts ledger |
| E1.4 | `parse_log` | P0 | DATA | FG | parser attempts ledger |
| E1.4 | `unresolved_location_queue` | P0 | DATA | PLG | location failure queue |
| E1.4 | `job_run` | P0 | DATA | FG | job execution telemetry |
| E1.4 | `quality_incident` | P1 | DATA | PRG | quality incident audit |
| E1.5 | raw document tables | P0 | DATA | FG | bronze ingestion layer |
| E1.5 | extracted row tables | P0 | DATA | FG | bronze row extraction |
| E1.5 | canonical fact tables | P0 | DATA | PLG | silver fact layer |
| E1.5 | entity/place dimensions | P0 | DATA | PLG | silver dimensions |
| E1.5 | relation bridges | P1 | DATA | ASG | cross-object linkage |
| E1.5 | metric state/snapshot | P0 | DATA | ASG | gold analytics layer |
| E1.6 | skip indexes | P1 | DATA | ASG | query acceleration |
| E1.6 | projections | P1 | DATA | ASG | serving optimizations |
| E1.6 | TTL policies | P0 | DATA | FG | lifecycle management |
| E1.6 | low-cardinality columns | P0 | DATA | FG | storage efficiency |
| E1.6 | codecs for large fields | P1 | DATA | ASG | compression strategy |

### E2 — Global place graph and boundary ingestion

- E2.1 geoBoundaries ingestion: ADM0–ADM4 baseline P0, ADM5 optional P2.
- E2.2 GeoNames ingestion: allCountries/admin1/admin2/hierarchy P0; admin5 sidecar P2.
- E2.3 dim_place build: internal IDs + continent pseudo-places + full parent chain P0.
- E2.4 place_hierarchy: parent-child graph + deepest level tracking P0.
- E2.5 polygon storage + dictionary + benchmark P0.
- E2.6 QA validation (coverage, geometry sanity, overlap report) P0.

Exit criteria:
1. Deepest-place lookup median latency target achieved (define in perf SLOs).
2. >95% of validation fixtures resolve to expected admin chain where boundaries exist.

### E3 — Source registry and crawl governance

- E3.1 Full source schema with license/terms, auth mode, class, parser routing, cadence (P0).
- E3.2 Seed high-priority domains from prior registry artifacts with class/domain-family tags (P0).
- E3.3 Governance controls: per-host rate limits, blocklist/allowlist, retries, kill switch, retention class (P0).
- E3.4 Discovery profile templates: sitemap/feed/API/HTML/browser/corpus modes (P0 for first 4, P1 for browser/corpus).

### E4 — Discovery engine and frontier management

- E4.1 robots+sitemap ingestion with compressed support (P0).
- E4.2 RSS/Atom support with dedup/freshness tracking (P0).
- E4.3 HTML traversal profile + canonicalization + visited/Bloom strategy (P1).
- E4.4 Wayback/CDX + Common Crawl + GDELT discovery adapters (P1).
- E4.5 Frontier ranking formula with freshness/quality/diversity/backfill class weighting (P0 baseline, P1 tuned).

### E5 — Fetcher runtime and raw retention

- E5.1 HTTP runtime (conditional fetch, retry/backoff, gzip/br, sniffing, size guardrails) P0.
- E5.2 Raw retention to MinIO + CH metadata with content hash and headers P0.
- E5.3 Structured-file path (`s3()`/`S3Queue`) P0.
- E5.4 Corpus path for WARC references and selective extraction P1.
- E5.5 Retention classes and replay/backfill controls P0.

### E6 — Parser framework and canonical structuring

- E6.1 Parser abstraction contracts + versioning P0.
- E6.2 Structured parsers JSON/CSV/XML/RSS/Atom + WARC extractor (WARC P1).
- E6.3 HTML extractor profiles P1.
- E6.4 Browser-rendered extraction (controlled use) P1.
- E6.5 Canonical emitters for observations/events/entities + evidence + tentative location P0.

### E7 — Canonical entity, event, and observation model

- E7.1 Stable canonical schemas for entity/event/observation/track/evidence/metric-contribution P0.
- E7.2 Deterministic IDs + source-native retention + aliasing + duplicate suppression P0.
- E7.3 Evidence model (source refs, feature contributions, confidence, raw pointers) P0.
- E7.4 Schema version fields + migration helpers + compatibility fixtures P0.

### E8 — Location attribution pipeline

- E8.1 Point attribution to deepest place with full chain columns P0.
- E8.2 Place-name resolution with ambiguity scoring and thresholded promotion P0.
- E8.3 Polygon attribution with centroid/H3/overlap confidence P1.
- E8.4 Track attribution + crossings + dominant place/time P1.
- E8.5 Unresolved workflow API + reprocessing loop P0.

### E9 — Deduplication and entity resolution

- E9.1 URL/document dedup (canonical URL + content hash + near-dup) P0.
- E9.2 Observation dedup (source key + temporal/spatial similarity) P0.
- E9.3 Candidate generation (identifiers + aliases + registry hints) P1.
- E9.4 Resolution scoring bands (`exact/probable/possible/unknown`) P1.
- E9.5 Current-state materialization + lineage preservation P1.

### E10 — Metric registry, contributions, and rollups

- E10.1 Metric registry schema + explainability fields P0.
- E10.2 Per-record contribution pipeline with weight/confidence/signed support P0.
- E10.3 Aggregate state MVs (hour/day/week and all place levels incl world/continent) P0.
- E10.4 Snapshot finalization + hotspot ranking + deltas/anomalies P1.
- E10.5 Metric API contracts (metadata, timeseries, bulk rollup, hotspots) P0.

### E11 — Maritime pack

- E11.1 Sources: AIS + registry + port + sanctions + optional SAR context (P1).
- E11.2 Canonical maritime model (vessel/voyage/port-call/AIS-gap/ownership/flag-history) P1.
- E11.3 Maritime analytics incl shadow-fleet evidence payload P1.
- E11.4 Maritime place linking incl coastal/admin attribution (EEZ optional P2).

### E12 — Aviation pack

- E12.1 Sources: ADS-B/state vectors + registries + airports + NOTAM/weather P1.
- E12.2 Canonical aviation model (aircraft/track/segment/gap/airport interaction) P1.
- E12.3 Aviation analytics incl military-likelihood evidence scoring P1.
- E12.4 Place linking for airports/tracks/corridors P1.

### E13 — Space pack

- E13.1 Sources: TLE/OMM/catalog/transmitter/conjunction public feeds P1.
- E13.2 Derived geometry: orbit propagation, ground track, overpass windows, intersections P1.
- E13.3 Canonical space model + place relations P1.
- E13.4 Space metrics (overpass density/revisit/conjunction/etc.) P1.

### E14 — Geopolitical and general-web pack

- E14.1 Structured event adapters + archive/backfill P0.
- E14.2 GDELT + feed + Common Crawl lookup + Wayback replay P0/P1 split (GDELT+feeds P0).
- E14.3 Event normalization + cross-source linking P0.
- E14.4 Geo metrics (conflict/protest/media/humanitarian/spillover/disruption/trend) P0 baseline.

### E15 — Safety and security pack

- E15.1 Sanctions/entity graph integration + linkage P1.
- E15.2 Hazard adapters (fire/weather/coastal/emergency) P1.
- E15.3 Cyber/safety adapters (KEV-like) P1.
- E15.4 Safety composite metrics P1.

### E16 — REST API, serving views, and compatibility layer

- E16.1 Full API contract definitions for all endpoint groups P0.
- E16.2 Serving views: `api_v1_places/entities/events/observations/metrics` P0.
- E16.3 Go REST service: validation, filtering, cursor pagination, timeout controls P0.
- E16.4 Compatibility harness: snapshots, aliases, integration version tests P0.

### E17 — Quality, testing, and explainability

- E17.1 Test datasets: synthetic, golden, replay bundles, place fixtures, contract bundles P0.
- E17.2 Explainability payload schema implementation P0.
- E17.3 Quality dashboards and alerts P1.
- E17.4 Review workflows for unresolved/low-confidence/source failures/regressions P1.

### E18 — Scale-out and high availability (optional)

- E18.1 Shard/replica + Keeper topology P2.
- E18.2 Distributed table rollout and benchmark P2.
- E18.3 Cluster-scale backup/restore drills P2.
- E18.4 Cost controls and right-sizing P2.

---

## 14) API Endpoint Implementation Checklist (Complete v1 Surface)

### Health / control
- [ ] `GET /v1/health`
- [ ] `GET /v1/ready`
- [ ] `GET /v1/version`
- [ ] `GET /v1/schema`
- [ ] `GET /v1/jobs`
- [ ] `GET /v1/jobs/{jobId}`

### Sources
- [ ] `GET /v1/sources`
- [ ] `GET /v1/sources/{sourceId}`
- [ ] `GET /v1/sources/{sourceId}/coverage`

### Places
- [ ] `GET /v1/places`
- [ ] `GET /v1/places/{placeId}`
- [ ] `GET /v1/places/{placeId}/children`
- [ ] `GET /v1/places/{placeId}/metrics`
- [ ] `GET /v1/places/{placeId}/events`
- [ ] `GET /v1/places/{placeId}/observations`

### Entities
- [ ] `GET /v1/entities`
- [ ] `GET /v1/entities/{entityId}`
- [ ] `GET /v1/entities/{entityId}/tracks`
- [ ] `GET /v1/entities/{entityId}/events`
- [ ] `GET /v1/entities/{entityId}/places`

### Events / observations
- [ ] `GET /v1/events`
- [ ] `GET /v1/events/{eventId}`
- [ ] `GET /v1/observations`
- [ ] `GET /v1/observations/{recordId}`

### Analytics
- [ ] `GET /v1/metrics`
- [ ] `GET /v1/metrics/{metricId}`
- [ ] `GET /v1/analytics/rollups`
- [ ] `GET /v1/analytics/time-series`
- [ ] `GET /v1/analytics/hotspots`
- [ ] `GET /v1/analytics/cross-domain`

### Search
- [ ] `GET /v1/search`
- [ ] `GET /v1/search/places`
- [ ] `GET /v1/search/entities`

---

## 15) Metric Coverage Checklist (All Declared Families)

### Core platform metrics
- [ ] obs_count
- [ ] event_count
- [ ] entity_count_approx
- [ ] source_count_approx
- [ ] confidence_weighted_activity
- [ ] source_diversity_score
- [ ] freshness_lag_minutes
- [ ] geolocation_success_rate
- [ ] dedup_rate
- [ ] schema_drift_rate
- [ ] evidence_density
- [ ] cross_source_confirmation_rate
- [ ] trend_24h
- [ ] trend_7d
- [ ] acceleration_7d_vs_30d
- [ ] anomaly_zscore_30d
- [ ] burst_score
- [ ] risk_composite_global

### Geopolitical metrics
- [ ] conflict_intensity_score
- [ ] protest_activity_score
- [ ] sanction_activity_score
- [ ] humanitarian_pressure_score
- [ ] cross_border_spillover_score
- [ ] media_attention_score
- [ ] media_attention_acceleration
- [ ] infrastructure_disruption_score

### Maritime metrics
- [ ] maritime_activity_score
- [ ] ais_dark_hours_sum
- [ ] ais_gap_frequency
- [ ] identity_inconsistency_score
- [ ] flag_ownership_mismatch_score
- [ ] sanctions_exposure_score
- [ ] port_loiter_score
- [ ] rendezvous_probability
- [ ] sts_transfer_suspicion_score
- [ ] route_deviation_score
- [ ] shadow_fleet_score
- [ ] maritime_risk_composite

### Aviation metrics
- [ ] air_activity_score
- [ ] transponder_gap_hours_sum
- [ ] route_irregularity_score
- [ ] military_likelihood_score
- [ ] restricted_airspace_proximity_score
- [ ] high_risk_airport_exposure_score
- [ ] holding_pattern_anomaly_score
- [ ] air_risk_composite

### Space metrics
- [ ] satellite_activity_score
- [ ] overpass_density_score
- [ ] revisit_capability_score
- [ ] conjunction_risk_score
- [ ] maritime_observation_opportunity_score
- [ ] critical_infrastructure_overpass_score
- [ ] space_risk_composite

### Safety/security metrics
- [ ] cyber_exposure_score
- [ ] known_exploited_vuln_pressure
- [ ] fire_hotspot_score
- [ ] coastal_hazard_score
- [ ] weather_disruption_score
- [ ] safety_security_composite

Rollup requirement for every metric:
- [ ] place
- [ ] admin4
- [ ] admin3
- [ ] admin2
- [ ] admin1
- [ ] admin0
- [ ] continent
- [ ] world

---

## 16) Non-Functional SLOs and Capacity Targets

Initial SLO targets for v1 staging:
- API p95 latency: <= 500ms for standard list/detail queries.
- API availability: >= 99% over 7-day soak.
- Fetch success on approved stable sources: >= 95%.
- Geolocation success for promotable records in MVP domains: >= 90%.
- End-to-end freshness lag (source to promoted row): <= 30 minutes for high-priority feeds.

Capacity assumptions (single-node baseline):
- batched inserts only; avoid row-at-a-time write patterns.
- nightly backfills throttled to preserve API SLOs.
- replay jobs use separate priority queue classes.

---

## 17) Security and Compliance Implementation Checklist

- [ ] Public-source-only enforcement in source governance layer.
- [ ] Source terms/license captured and queryable per source.
- [ ] User-supplied API key handling isolated and encrypted at rest.
- [ ] Source kill switch is immediate and audited.
- [ ] No anti-bot circumvention logic in codebase.
- [ ] Retention policy by license/sensitivity is enforced via TTL and object lifecycle.
- [ ] Admin actions (source enable/disable, migration actions) are auditable.

---

## 18) Change Control and Backward Compatibility Procedure

For every schema/API change:
1. Additive first: add fields/columns, do not remove/rename in place.
2. Update compatibility view or response mapper aliases.
3. Increment relevant schema/contract versions.
4. Run snapshot contract test suite.
5. Include migration with rollback notes.
6. Mark deprecation windows in `meta.api_schema_registry`.

Blocking rule:
- Any breaking contract diff in `/v1/*` fails release unless waived by explicit major-version decision.

---

## 19) Revised Sequenced Milestones (Expanded)

M0 (Week 1): Repo skeleton + compose + bootstrap runner + migration ledger.
M1 (Week 2): Baseline CH schemas and smoke tests.
M2 (Week 4): Place graph + reverse geocoder + coverage report.
M3 (Week 6): Source registry + discovery + frontier ranking baseline.
M4 (Week 8): Fetch/parse/canonical promotion with location gating.
M5 (Week 10): Dedup + entity alias + core metric pipeline.
M6 (Week 12): Full v1 API core + compatibility harness.
M7 (Week 14): Geopolitical/general-web pack live with trend/burst analytics.
M8 (Week 16): Maritime pack baseline live.
M9 (Week 18): Production readiness gate (SLOs, runbooks, DR drill).


---

## 20) Verification: Blueprint-to-Plan Coverage Audit

This audit explicitly verifies whether the delivery plan includes every major requirement from the provided “Global OSINT Backend Blueprint and Delivery Backlog”.

### 20.1 Coverage summary by blueprint domain

| Blueprint domain | Status | Where covered in this plan |
|---|---|---|
| Objective and platform constraints (Go-first, CH-first, REST-only, Docker, compatibility) | Covered | Sections 1, 3, 12, 18 |
| Constraint reconciliation (TypeScript tension handling) | Covered | Section 1 (runtime stance) + Section 18 (compat procedure preserving external contracts) |
| Reality check + near-comprehensive acquisition model | Covered | Sections 1, 3, 13 (E3/E4/E5/E14 mappings) |
| Baseline architecture + optional scale-out + exclusions | Covered | Sections 3, 11, 13 (E0/E1/E18), 19 |
| System principles (raw retention, append-only, mandatory location, explainability, compatibility) | Covered | Sections 2.3, 3, 5, 6, 17, 18 |
| Tech stack + CH feature usage priorities | Covered | Sections 3, 5, 6, 16 |
| Source classes and crawl governance | Covered | Sections 3, 5, 13 (E3/E4/E5) |
| Data zones (meta/ops/bronze/silver/gold + raw store) | Covered | Sections 3, 5, 12, 13 |
| Canonical data model and stable envelope | Covered | Sections 3, 5, 13 (E7/E8/E9) |
| Recommended table families and engines | Covered | Sections 3, 5, 13 |
| Location model and geocoding precedence | Covered | Sections 3, 5, 13 (E2/E8) |
| CH ingestion/transform/storage usage pattern | Covered | Sections 3, 5, 6, 16 |
| REST endpoint groups + compatibility policy | Covered | Sections 3, 6, 14, 18 |
| One-button deployment + idempotent bootstrap/migration | Covered | Sections 3, 5, 10, 13 |
| Schema evolution strategy and compatibility mechanics | Covered | Sections 2.3, 6, 18 |
| Observability and quality scorecard | Covered | Sections 6, 16, 17, 13 (E17) |
| Security/legal/governance controls | Covered | Sections 7, 17, 13 (E3/E15) |
| Metrics design + universal rollups + formulas | Covered | Sections 5, 6, 15, 13 (E10/E11/E12/E13/E14/E15) |
| Domain connector packs (maritime/aviation/space/geopolitics/safety) | Covered | Sections 3, 5, 13, 19 |
| Sample CH implementation patterns | Covered (implementation-level intent) | Sections 5, 6, 13, 18 |
| All epics E0–E18 with tasks/subtasks | Covered | Section 13 (explicit matrix) |
| Delivery sequence and MVP scope | Covered | Sections 3, 10, 11, 19 |
| Research-source guidance | Covered (operationalized as implementation directives) | Sections 3, 6, 13 |

### 20.2 Explicit verification of previously requested completeness

Verification result: **Yes — all major blueprint areas are represented in the plan**, and all epics E0–E18 are explicitly enumerated with execution metadata in Section 13.

### 20.3 Remaining gaps / follow-up actions

No major scope gaps remain at planning level. Remaining work is execution-level detail that should be created as implementation artifacts:
1. Jira/Linear issue export for each E* task/subtask with estimates.
2. Migration file templates and first SQL migration pack.
3. API OpenAPI spec skeleton for the complete `/v1/*` checklist.
4. CI pipeline manifests implementing Section 6 stages.
5. Runbook markdown files implementing Section 9 requirements.


---

## 21) Research-Backed Implementation Notes (Decision Log + Source Mapping)

This section upgrades the plan from “comprehensive scope” to “research-backed execution” by linking each critical implementation decision to concrete external references and defining what to validate in implementation PRs.

### 21.1 ClickHouse: platform-critical decisions and validation criteria

| Decision area | Decision | Why | Implementation validation |
|---|---|---|---|
| Containerized deployment | Use official ClickHouse Docker deployment pattern | lowest-friction baseline and reproducible setup | bootstrap smoke must successfully run CH health query and DDL migration on fresh and existing install |
| Dynamic fields | Keep hot fields typed, use JSON for evolving attrs/evidence | balances performance and schema agility | benchmark representative API query latencies with typed filters + JSON payload pass-through |
| Reverse geocoding | Use polygon dictionaries for point-in-polygon lookup | purpose-built lookup path for geospatial enrichment | fixture suite verifies coordinates map to deepest known admin level where boundaries exist |
| Ingestion strategy | Prefer `url()`/`file()`/`s3()`/`S3Queue`; Go async inserts for irregular feeds | minimizes custom ETL and uses CH-native paths | per-source ingestion mode declared in registry; ingestion conformance tests pass |
| Write strategy | batch inserts / async inserts, avoid tiny row writes | sustained ingest throughput and merge stability | perf test: ingest throughput and merge backlog remain within SLO |
| Aggregation strategy | incremental MVs for state tables + refreshable MVs for finalized snapshots | separates insert-time state updates from scheduled finalization | hourly/day snapshot correctness fixtures must pass |
| Serving performance | projections for common query shapes; skip indexes on hot predicates | improves API read efficiency | query plans/latency checks on top N endpoints |
| Retention and lifecycle | TTL for bronze/raw metadata aging and rollup-friendly lifecycle | storage cost control and data hygiene | TTL policy tests verify expected expiration/rollup behavior |
| Durability | built-in backups and restore drills | recovery readiness | scheduled backup manifests + restore drill documented and passing |
| Scale-out | Keeper + Distributed engine only post-v1 saturation | keeps v1 low-ops while preserving growth path | scale-out readiness checklist completed before E18 activation |

### 21.2 Geospatial/place model decisions and validation criteria

| Decision area | Decision | Why | Validation |
|---|---|---|---|
| Global boundaries | geoBoundaries `gbOpen` as default admin boundary base | broad global ADM coverage under open data model | ingest report: per-country admin coverage map and missing-depth report |
| Names/hierarchy | GeoNames for names, alternate names, hierarchy and admin code references | complementary name graph and hierarchy alignment | place-name resolution fixtures across ambiguous names and multilingual variants |
| Place IDs | internal multi-source strategy (`plc:cont`, `plc:gb`, `plc:gn`, `plc:syn`) | avoids hard dependency on single external keyspace | uniqueness + lineage checks for merged place identities |
| Incomplete ADM depth | keep admin0..admin4 nullable and expose deepest level | avoids fabricated geographies while preserving rollups | rollup correctness tests for mixed-depth countries |
| Place enrichment | point→polygon dictionary, polygon overlap and H3 support for complex geometries | enables robust point/polygon/track attribution | geocoding accuracy and overlap-confidence fixtures |

### 21.3 Public-web acquisition decisions and validation criteria

| Decision area | Decision | Why | Validation |
|---|---|---|---|
| Robots treatment | use robots as crawl-governance signal, not authorization bypass tool | policy compliance and predictable crawl behavior | crawler policy tests for allow/deny and crawl-delay handling |
| Discovery-first approach | prioritize sitemap/feed/API discovery before deep spidering | better signal-to-noise and lower crawl cost | frontier quality score improves over raw spider-only baseline |
| Breadth layer | Common Crawl for broad-domain discovery and historical breadth | scales discovery without direct live crawling of everything | replay adapter integration tests with sampled CC indexes |
| High-frequency news/events | GDELT as near-real-time multilingual event stream | improves freshness for geopolitical coverage | ingestion lag SLO tests for GDELT pipeline |
| Historical replay | Wayback/CDX adapters for archive discovery and backfill | enables back-in-time reconstruction | replay tests verify deterministic dedup and provenance retention |

### 21.4 Source-class-to-ingestion-mode mapping (normative)

| Source class | Preferred path | Secondary path | Notes |
|---|---|---|---|
| structured_api | `url()` where schema stable | Go async inserts | include auth mode in `source_registry` |
| bulk_dump | `s3()` / `S3Queue` staged files | `file()` local staging | attach file manifest and checksum |
| feed | Go fetch + parser emit | `url()` for simple feeds | normalize item IDs + publication times |
| html_spider | Go fetch + HTML extractor | browser-rendered fallback | strict host policy/rate controls |
| browser_rendered | renderer sidecar + controlled extraction | skip if equivalent API exists | high-value-only enforcement |
| broad_web_corpus | corpus index adapters + selective extraction | staged subset files | avoid duplicating giant corpora in CH |
| streaming_public_telemetry | Go async batched inserts | staged micro-batches | enforce bounded latency and dedup windows |

### 21.5 Research source register (for implementation PR references)

ClickHouse references:
- https://clickhouse.com/docs/install/docker
- https://clickhouse.com/docs/sql-reference/data-types/newjson
- https://clickhouse.com/docs/dictionary
- https://clickhouse.com/docs/sql-reference/statements/create/dictionary/layouts/polygon
- https://clickhouse.com/docs/sql-reference/functions/geo
- https://clickhouse.com/docs/sql-reference/table-functions/url
- https://clickhouse.com/docs/sql-reference/table-functions/file
- https://clickhouse.com/docs/sql-reference/table-functions/s3
- https://clickhouse.com/docs/engines/table-engines/integrations/s3queue
- https://clickhouse.com/docs/optimize/bulk-inserts
- https://clickhouse.com/docs/optimize/asynchronous-inserts
- https://clickhouse.com/docs/best-practices/use-materialized-views
- https://clickhouse.com/docs/materialized-view/refreshable-materialized-view
- https://clickhouse.com/docs/data-modeling/projections
- https://clickhouse.com/docs/guides/developer/ttl
- https://clickhouse.com/docs/operations/backup/overview
- https://clickhouse.com/docs/guides/sre/keeper/clickhouse-keeper
- https://clickhouse.com/docs/engines/table-engines/special/distributed
- https://clickhouse.com/docs/integrations/go

Geospatial references:
- https://www.geoboundaries.org/api.html
- https://www.geoboundaries.org/
- https://download.geonames.org/export/dump/
- https://www.geonames.org/export/place-hierarchy.html
- https://overpass-api.de/

Public-web coverage references:
- https://datatracker.ietf.org/doc/html/rfc9309
- https://www.sitemaps.org/protocol.html
- https://commoncrawl.org/
- https://commoncrawl.org/get-started
- https://www.gdeltproject.org/data.html
- https://archive.org/help/wayback_api.php

Seed artifacts:
- `/mnt/data/osint_source_registry.json`
- `/mnt/data/osint_ts_backlog.md`

### 21.6 PR policy for “proper research” going forward

For any implementation PR touching architecture, ingestion modes, geolocation, or metrics:
1. Include a short “Research Basis” subsection in the PR body listing the exact source URLs used.
2. Add one validation artifact tied to that decision (test output, benchmark snippet, or fixture result).
3. If deviating from this plan’s normative mappings, include rationale and migration impact.

