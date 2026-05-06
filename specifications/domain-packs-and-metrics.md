# Domain Packs And Metrics

## Source of Truth

- Core metric registry and materialization: `internal/metrics/registry.go`, `contribution.go`, `rollup.go`, `materialization_sql.go`, `runtime_outputs.go`
- Domain packs: `internal/packs/aviation`, `internal/packs/geopolitical`, `internal/packs/maritime`, `internal/packs/safety`, `internal/packs/space`
- Pack jobs: `cmd/control-plane/jobs_aviation.go`, `jobs_geopolitical.go`, `jobs_maritime.go`, `jobs_safety.go`, `jobs_space.go`
- Metric tables: `migrations/clickhouse/0004_meta_registries.sql`, `0005_baseline_tables.sql`, `0010_runtime_analytics_outputs.sql`

## Runtime Behavior

Core metrics define shared activity, quality, trend, and risk measures in `internal/metrics`. `EmitCoreMetricContributions()` converts canonical records into per-place hierarchy contributions for metrics such as observation count, event count, confidence-weighted activity, geolocation success rate, evidence density, risk composite, and trend measures. `UpsertMaterializationSQL()` writes contributions, refreshes `gold.metric_state`, writes `gold.metric_snapshot`, and refreshes runtime analytics outputs.

Domain packs add specialized analytics:

- Aviation: fixture-driven aircraft entities, tracks, events, and metrics such as military likelihood, route irregularity, transponder gap, altitude variance, squawk changes, hold pattern frequency, diversion rate, and proximity.
- Maritime: vessel entities, track/event outputs, and metrics such as AIS dark hours, anchorage dwell, flag mismatch, port gap, route deviation, and shadow fleet score.
- Geopolitical: HTTP-source orchestration and pack-scored event metrics from geopolitical fixtures.
- Safety/security: HTTP-source orchestration for safety/security sources and fixture-backed safety logic.
- Space: satellite entities, overpass events, and metrics such as conjunction risk, coverage gaps, maneuver frequency, orbital decay, overpass density, revisit capability, and satellite health.

Pack jobs insert or update metric registry records, domain silver facts/entities/tracks/events, metric contributions, and job-run evidence.

## Metric Rules

- Metric definitions live in code and are materialized to `meta.metric_registry`.
- Contributions carry `contribution_id`, metric id, subject grain/id, source record metadata, window, materialization key, value, weight, attrs, and evidence.
- Rollup behavior is controlled by registry `rollup_rule` values such as `sum`, `weighted_avg`, `distinct_sources_per_contribution`, `latest`, and `latest_daily_pack_score`.
- API analytics routes read gold snapshots and runtime outputs, not pack-private structures.

## Deferred Or Catalog-Only Behavior

Pack fixture jobs demonstrate domain materialization and are runtime-backed jobs, but they do not imply every cataloged live source in the same domain is actively ingested. Cataloged aviation, maritime, safety, geopolitical, or space sources still need runtime source policy and worker support.

## Extension Knobs

- Add shared metrics in `internal/metrics/registry.go` and contribution rules in `internal/metrics/contribution.go`.
- Add domain metrics in the relevant `internal/packs/<domain>` package and wire registry/materialization SQL in its control-plane job.
- Use `metrics.UpsertMaterializationSQL()` for derived metric rows unless a new materialization engine is required.
- Keep metric ids stable once exposed through `meta.metric_registry` and API views.
- Add pack tests with deterministic fixtures and expected metric ids/values.
