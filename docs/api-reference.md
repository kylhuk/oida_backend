# API Reference

The REST API is read-only. Frontend traffic should flow through a server-side BFF; the BFF attaches a scoped API key in `X-API-Key` for protected routes and keeps raw keys out of browser clients.

Public routes: `/v1/health`, `/v1/ready`, `/v1/version`, `/v1/schema`. All other `/v1/*` routes require `X-API-Key` with the route's documented scopes.

## Endpoint Contracts

## GET /v1/health

- Summary: Liveness probe for API process
- Auth: Not required
- Kind: `operational`
- Response container: `status`

Path parameters
- none

Query parameters
- none

Selectable fields
- none

Notes
- Public operational probe.

## GET /v1/ready

- Summary: Readiness probe for bootstrap completion
- Auth: Not required
- Kind: `operational`
- Response container: `status`

Path parameters
- none

Query parameters
- none

Selectable fields
- none

Notes
- Public operational probe.
- Reports bootstrap marker readiness.

## GET /v1/version

- Summary: Service and API version metadata
- Auth: Not required
- Kind: `operational`
- Response container: `item`

Path parameters
- none

Query parameters
- none

Selectable fields
- none

Notes
- Public operational probe.

## GET /v1/schema

- Summary: Machine-readable API contract for frontend integration
- Auth: Not required
- Kind: `contract`
- Response container: `items`

Path parameters
- none

Query parameters
- none

Selectable fields
- none

Notes
- Public route used for route/auth/query/field discovery.

## GET /v1/jobs

- Summary: List control-plane jobs
- Auth: Required (`X-API-Key`)
- Kind: `jobs`
- Item kind: `job`
- Response container: `items`
- Response sort: `job_id:asc`

Path parameters
- none

Query parameters
- `limit` (int, optional): Page size, default 200, max 1000.
- `cursor` (string, optional): Opaque base64url cursor from prior response next_cursor.
- `offset` (int, optional): Skip this many rows before returning results. Non-negative integer; mutually exclusive with cursor.
- `fields` (csv, optional): Optional projected field list; all fields returned when omitted.
- `q` (string, optional): Case-insensitive search text matched across route-specific searchable columns.
- `job_type` (string, optional): Allowlisted exact-match filter parameter.
- `status` (string, optional): Allowlisted exact-match filter parameter.

Selectable fields
- job_id, job_type, status, started_at, finished_at, message, stats

## GET /v1/jobs/{jobId}

- Summary: Get a single control-plane job
- Auth: Required (`X-API-Key`)
- Kind: `jobs`
- Item kind: `job`
- Response container: `item`
- Response sort: `job_id:asc`

Path parameters
- `jobId` (string, required): Path identifier segment.

Query parameters
- `fields` (csv, optional): Optional projected field list; all fields returned when omitted.

Selectable fields
- job_id, job_type, status, started_at, finished_at, message, stats

## GET /v1/sources

- Summary: List source registry entries
- Auth: Required (`X-API-Key`)
- Kind: `sources`
- Item kind: `source`
- Response container: `items`
- Response sort: `source_id:asc`

Path parameters
- none

Query parameters
- `limit` (int, optional): Page size, default 200, max 1000.
- `cursor` (string, optional): Opaque base64url cursor from prior response next_cursor.
- `offset` (int, optional): Skip this many rows before returning results. Non-negative integer; mutually exclusive with cursor.
- `fields` (csv, optional): Optional projected field list; all fields returned when omitted.
- `q` (string, optional): Case-insensitive search text matched across route-specific searchable columns.
- `domain_family` (string, optional): Allowlisted exact-match filter parameter.
- `enabled` (string, optional): Allowlisted exact-match filter parameter.
- `geo_scope` (string, optional): Allowlisted exact-match filter parameter.
- `source_class` (string, optional): Allowlisted exact-match filter parameter.

Selectable fields
- source_id, domain, domain_family, source_class, entrypoints, auth_mode, auth_config_json, format_hint, robots_policy, refresh_strategy, requests_per_minute, burst_size, retention_class, license, terms_url, attribution_required, geo_scope, priority, parser_id, entity_types, expected_place_types, supports_historical, supports_delta, backfill_priority, confidence_baseline, enabled, disabled_reason, disabled_at, disabled_by, review_status, review_notes, schema_version, record_version, api_contract_version, updated_at, attrs, evidence

Notes
- Boolean fields are normalized from ClickHouse scalar values.

## GET /v1/sources/{sourceId}

- Summary: Get a single source registry entry
- Auth: Required (`X-API-Key`)
- Kind: `sources`
- Item kind: `source`
- Response container: `item`
- Response sort: `source_id:asc`

Path parameters
- `sourceId` (string, required): Path identifier segment.

Query parameters
- `fields` (csv, optional): Optional projected field list; all fields returned when omitted.

Selectable fields
- source_id, domain, domain_family, source_class, entrypoints, auth_mode, auth_config_json, format_hint, robots_policy, refresh_strategy, requests_per_minute, burst_size, retention_class, license, terms_url, attribution_required, geo_scope, priority, parser_id, entity_types, expected_place_types, supports_historical, supports_delta, backfill_priority, confidence_baseline, enabled, disabled_reason, disabled_at, disabled_by, review_status, review_notes, schema_version, record_version, api_contract_version, updated_at, attrs, evidence

Notes
- Boolean and JSON-like fields are normalized.

## GET /v1/sources/{sourceId}/coverage

- Summary: List coverage records for a source
- Auth: Required (`X-API-Key`)
- Kind: `source_coverage`
- Item kind: `source_coverage`
- Response container: `items`
- Response sort: `coverage_id:asc`

Path parameters
- `sourceId` (string, required): Path identifier segment.

Query parameters
- `limit` (int, optional): Page size, default 200, max 1000.
- `cursor` (string, optional): Opaque base64url cursor from prior response next_cursor.
- `offset` (int, optional): Skip this many rows before returning results. Non-negative integer; mutually exclusive with cursor.
- `fields` (csv, optional): Optional projected field list; all fields returned when omitted.
- `q` (string, optional): Case-insensitive search text matched across route-specific searchable columns.
- `geo_scope` (string, optional): Allowlisted exact-match filter parameter.
- `scope_id` (string, optional): Allowlisted exact-match filter parameter.
- `scope_type` (string, optional): Allowlisted exact-match filter parameter.
- `source_id` (string, optional): Allowlisted exact-match filter parameter.

Selectable fields
- coverage_id, source_id, scope_type, scope_id, geo_scope, place_count, event_count, coverage_state, reason, updated_at

Notes
- Nested list uses fixed source_id filter from path parameter.

## GET /v1/places

- Summary: List places
- Auth: Required (`X-API-Key`)
- Kind: `places`
- Item kind: `place`
- Response container: `items`
- Response sort: `place_id:asc`

Path parameters
- none

Query parameters
- `limit` (int, optional): Page size, default 200, max 1000.
- `cursor` (string, optional): Opaque base64url cursor from prior response next_cursor.
- `offset` (int, optional): Skip this many rows before returning results. Non-negative integer; mutually exclusive with cursor.
- `fields` (csv, optional): Optional projected field list; all fields returned when omitted.
- `q` (string, optional): Case-insensitive search text matched across route-specific searchable columns.
- `continent_code` (string, optional): Allowlisted exact-match filter parameter.
- `country_code` (string, optional): Allowlisted exact-match filter parameter.
- `parent_place_id` (string, optional): Allowlisted exact-match filter parameter.
- `place_type` (string, optional): Allowlisted exact-match filter parameter.
- `status` (string, optional): Allowlisted exact-match filter parameter.

Selectable fields
- place_id, parent_place_id, canonical_name, place_type, admin_level, country_code, continent_code, source_place_key, source_system, status, centroid_lat, centroid_lon, bbox_min_lat, bbox_min_lon, bbox_max_lat, bbox_max_lon, valid_from, valid_to, schema_version, record_version, api_contract_version, updated_at, attrs, evidence

## GET /v1/places/{placeId}

- Summary: Get a single place
- Auth: Required (`X-API-Key`)
- Kind: `places`
- Item kind: `place`
- Response container: `item`
- Response sort: `place_id:asc`

Path parameters
- `placeId` (string, required): Path identifier segment.

Query parameters
- `fields` (csv, optional): Optional projected field list; all fields returned when omitted.

Selectable fields
- place_id, parent_place_id, canonical_name, place_type, admin_level, country_code, continent_code, source_place_key, source_system, status, centroid_lat, centroid_lon, bbox_min_lat, bbox_min_lon, bbox_max_lat, bbox_max_lon, valid_from, valid_to, schema_version, record_version, api_contract_version, updated_at, attrs, evidence

## GET /v1/places/{placeId}/children

- Summary: List child places for a parent place
- Auth: Required (`X-API-Key`)
- Kind: `place_children`
- Item kind: `place`
- Response container: `items`
- Response sort: `place_id:asc`

Path parameters
- `placeId` (string, required): Path identifier segment.

Query parameters
- `limit` (int, optional): Page size, default 200, max 1000.
- `cursor` (string, optional): Opaque base64url cursor from prior response next_cursor.
- `offset` (int, optional): Skip this many rows before returning results. Non-negative integer; mutually exclusive with cursor.
- `fields` (csv, optional): Optional projected field list; all fields returned when omitted.
- `q` (string, optional): Case-insensitive search text matched across route-specific searchable columns.
- `continent_code` (string, optional): Allowlisted exact-match filter parameter.
- `country_code` (string, optional): Allowlisted exact-match filter parameter.
- `parent_place_id` (string, optional): Allowlisted exact-match filter parameter.
- `place_type` (string, optional): Allowlisted exact-match filter parameter.
- `status` (string, optional): Allowlisted exact-match filter parameter.

Selectable fields
- place_id, parent_place_id, canonical_name, place_type, admin_level, country_code, continent_code, source_place_key, source_system, status, centroid_lat, centroid_lon, bbox_min_lat, bbox_min_lon, bbox_max_lat, bbox_max_lon, valid_from, valid_to, schema_version, record_version, api_contract_version, updated_at, attrs, evidence

Notes
- Nested list uses fixed parent_place_id filter from path parameter.

## GET /v1/places/{placeId}/metrics

- Summary: List metric rollups for a place
- Auth: Required (`X-API-Key`)
- Kind: `place_metrics`
- Item kind: `metric_rollup`
- Response container: `items`
- Response sort: `snapshot_id:asc`

Path parameters
- `placeId` (string, required): Path identifier segment.

Query parameters
- `limit` (int, optional): Page size, default 200, max 1000.
- `cursor` (string, optional): Opaque base64url cursor from prior response next_cursor.
- `offset` (int, optional): Skip this many rows before returning results. Non-negative integer; mutually exclusive with cursor.
- `fields` (csv, optional): Optional projected field list; all fields returned when omitted.
- `q` (string, optional): Case-insensitive search text matched across route-specific searchable columns.
- `metric_id` (string, optional): Allowlisted exact-match filter parameter.
- `place_id` (string, optional): Allowlisted exact-match filter parameter.
- `subject_grain` (string, optional): Allowlisted exact-match filter parameter.
- `subject_id` (string, optional): Allowlisted exact-match filter parameter.
- `window_grain` (string, optional): Allowlisted exact-match filter parameter.

Selectable fields
- snapshot_id, metric_id, subject_grain, subject_id, place_id, window_grain, window_start, window_end, snapshot_at, metric_value, metric_delta, rank, attrs, evidence

Notes
- Nested list uses fixed place_id filter from path parameter.

## GET /v1/places/{placeId}/events

- Summary: List events for a place
- Auth: Required (`X-API-Key`)
- Kind: `place_events`
- Item kind: `event`
- Response container: `items`
- Response sort: `event_id:asc`

Path parameters
- `placeId` (string, required): Path identifier segment.

Query parameters
- `limit` (int, optional): Page size, default 200, max 1000.
- `cursor` (string, optional): Opaque base64url cursor from prior response next_cursor.
- `offset` (int, optional): Skip this many rows before returning results. Non-negative integer; mutually exclusive with cursor.
- `fields` (csv, optional): Optional projected field list; all fields returned when omitted.
- `q` (string, optional): Case-insensitive search text matched across route-specific searchable columns.
- `event_subtype` (string, optional): Allowlisted exact-match filter parameter.
- `event_type` (string, optional): Allowlisted exact-match filter parameter.
- `place_id` (string, optional): Allowlisted exact-match filter parameter.
- `source_id` (string, optional): Allowlisted exact-match filter parameter.
- `status` (string, optional): Allowlisted exact-match filter parameter.

Selectable fields
- event_id, source_id, event_type, event_subtype, place_id, parent_place_chain, starts_at, ends_at, status, confidence_band, impact_score, schema_version, attrs, evidence

Notes
- Nested list uses fixed place_id filter from path parameter.

## GET /v1/places/{placeId}/observations

- Summary: List observations for a place
- Auth: Required (`X-API-Key`)
- Kind: `place_observations`
- Item kind: `observation`
- Response container: `items`
- Response sort: `observation_id:asc`

Path parameters
- `placeId` (string, required): Path identifier segment.

Query parameters
- `limit` (int, optional): Page size, default 200, max 1000.
- `cursor` (string, optional): Opaque base64url cursor from prior response next_cursor.
- `offset` (int, optional): Skip this many rows before returning results. Non-negative integer; mutually exclusive with cursor.
- `fields` (csv, optional): Optional projected field list; all fields returned when omitted.
- `q` (string, optional): Case-insensitive search text matched across route-specific searchable columns.
- `observation_type` (string, optional): Allowlisted exact-match filter parameter.
- `place_id` (string, optional): Allowlisted exact-match filter parameter.
- `source_id` (string, optional): Allowlisted exact-match filter parameter.
- `subject_id` (string, optional): Allowlisted exact-match filter parameter.
- `subject_type` (string, optional): Allowlisted exact-match filter parameter.

Selectable fields
- observation_id, source_id, subject_type, subject_id, observation_type, place_id, parent_place_chain, observed_at, published_at, confidence_band, measurement_unit, measurement_value, schema_version, attrs, evidence

Notes
- Nested list uses fixed place_id filter from path parameter.

## GET /v1/entities

- Summary: List entities
- Auth: Required (`X-API-Key`)
- Kind: `entities`
- Item kind: `entity`
- Response container: `items`
- Response sort: `entity_id:asc`

Path parameters
- none

Query parameters
- `limit` (int, optional): Page size, default 200, max 1000.
- `cursor` (string, optional): Opaque base64url cursor from prior response next_cursor.
- `offset` (int, optional): Skip this many rows before returning results. Non-negative integer; mutually exclusive with cursor.
- `fields` (csv, optional): Optional projected field list; all fields returned when omitted.
- `q` (string, optional): Case-insensitive search text matched across route-specific searchable columns.
- `entity_type` (string, optional): Allowlisted exact-match filter parameter.
- `primary_place_id` (string, optional): Allowlisted exact-match filter parameter.
- `risk_band` (string, optional): Allowlisted exact-match filter parameter.
- `status` (string, optional): Allowlisted exact-match filter parameter.

Selectable fields
- entity_id, entity_type, canonical_name, status, risk_band, primary_place_id, source_system, valid_from, valid_to, schema_version, record_version, api_contract_version, updated_at, attrs, evidence

## GET /v1/entities/{entityId}

- Summary: Get a single entity
- Auth: Required (`X-API-Key`)
- Kind: `entities`
- Item kind: `entity`
- Response container: `item`
- Response sort: `entity_id:asc`

Path parameters
- `entityId` (string, required): Path identifier segment.

Query parameters
- `fields` (csv, optional): Optional projected field list; all fields returned when omitted.

Selectable fields
- entity_id, entity_type, canonical_name, status, risk_band, primary_place_id, source_system, valid_from, valid_to, schema_version, record_version, api_contract_version, updated_at, attrs, evidence

## GET /v1/entities/{entityId}/tracks

- Summary: List tracks for an entity
- Auth: Required (`X-API-Key`)
- Kind: `entity_tracks`
- Item kind: `track`
- Response container: `items`
- Response sort: `track_record_id:asc`

Path parameters
- `entityId` (string, required): Path identifier segment.

Query parameters
- `limit` (int, optional): Page size, default 200, max 1000.
- `cursor` (string, optional): Opaque base64url cursor from prior response next_cursor.
- `fields` (csv, optional): Optional projected field list; all fields returned when omitted.
- `q` (string, optional): Case-insensitive search text matched across route-specific searchable columns.
- `entity_id` (string, optional): Allowlisted exact-match filter parameter.
- `from_place_id` (string, optional): Allowlisted exact-match filter parameter.
- `place_id` (string, optional): Allowlisted exact-match filter parameter.
- `to_place_id` (string, optional): Allowlisted exact-match filter parameter.
- `track_type` (string, optional): Allowlisted exact-match filter parameter.

Selectable fields
- track_record_id, track_id, track_type, entity_id, place_id, from_place_id, to_place_id, started_at, ended_at, distance_km, point_count, avg_speed_kph

Notes
- Nested list uses fixed entity_id filter from path parameter.

## GET /v1/entities/{entityId}/events

- Summary: List events linked to an entity
- Auth: Required (`X-API-Key`)
- Kind: `entity_events`
- Item kind: `event`
- Response container: `items`
- Response sort: `event_id:asc`

Path parameters
- `entityId` (string, required): Path identifier segment.

Query parameters
- `limit` (int, optional): Page size, default 200, max 1000.
- `cursor` (string, optional): Opaque base64url cursor from prior response next_cursor.
- `offset` (int, optional): Skip this many rows before returning results. Non-negative integer; mutually exclusive with cursor.
- `fields` (csv, optional): Optional projected field list; all fields returned when omitted.
- `q` (string, optional): Case-insensitive search text matched across route-specific searchable columns.
- `entity_id` (string, optional): Allowlisted exact-match filter parameter.
- `event_subtype` (string, optional): Allowlisted exact-match filter parameter.
- `event_type` (string, optional): Allowlisted exact-match filter parameter.
- `place_id` (string, optional): Allowlisted exact-match filter parameter.
- `status` (string, optional): Allowlisted exact-match filter parameter.

Selectable fields
- entity_id, event_id, event_type, event_subtype, place_id, starts_at, status, confidence_band, impact_score

Notes
- Nested list uses fixed entity_id filter from path parameter.

## GET /v1/entities/{entityId}/places

- Summary: List place links for an entity
- Auth: Required (`X-API-Key`)
- Kind: `entity_places`
- Item kind: `place_link`
- Response container: `items`
- Response sort: `place_id:asc`

Path parameters
- `entityId` (string, required): Path identifier segment.

Query parameters
- `limit` (int, optional): Page size, default 200, max 1000.
- `cursor` (string, optional): Opaque base64url cursor from prior response next_cursor.
- `offset` (int, optional): Skip this many rows before returning results. Non-negative integer; mutually exclusive with cursor.
- `fields` (csv, optional): Optional projected field list; all fields returned when omitted.
- `q` (string, optional): Case-insensitive search text matched across route-specific searchable columns.
- `entity_id` (string, optional): Allowlisted exact-match filter parameter.
- `place_type` (string, optional): Allowlisted exact-match filter parameter.
- `relation_type` (string, optional): Allowlisted exact-match filter parameter.

Selectable fields
- entity_id, place_id, canonical_name, place_type, relation_type, linked_at

Notes
- Nested list uses fixed entity_id filter from path parameter.

## GET /v1/events

- Summary: List events
- Auth: Required (`X-API-Key`)
- Kind: `events`
- Item kind: `event`
- Response container: `items`
- Response sort: `event_id:asc`

Path parameters
- none

Query parameters
- `limit` (int, optional): Page size, default 200, max 1000.
- `cursor` (string, optional): Opaque base64url cursor from prior response next_cursor.
- `offset` (int, optional): Skip this many rows before returning results. Non-negative integer; mutually exclusive with cursor.
- `fields` (csv, optional): Optional projected field list; all fields returned when omitted.
- `q` (string, optional): Case-insensitive search text matched across route-specific searchable columns.
- `event_subtype` (string, optional): Allowlisted exact-match filter parameter.
- `event_type` (string, optional): Allowlisted exact-match filter parameter.
- `place_id` (string, optional): Allowlisted exact-match filter parameter.
- `source_id` (string, optional): Allowlisted exact-match filter parameter.
- `status` (string, optional): Allowlisted exact-match filter parameter.

Selectable fields
- event_id, source_id, event_type, event_subtype, place_id, parent_place_chain, starts_at, ends_at, status, confidence_band, impact_score, schema_version, attrs, evidence

Notes
- parent_place_chain is normalized from JSON text when present.

## GET /v1/events/{eventId}

- Summary: Get a single event
- Auth: Required (`X-API-Key`)
- Kind: `events`
- Item kind: `event`
- Response container: `item`
- Response sort: `event_id:asc`

Path parameters
- `eventId` (string, required): Path identifier segment.

Query parameters
- `fields` (csv, optional): Optional projected field list; all fields returned when omitted.

Selectable fields
- event_id, source_id, event_type, event_subtype, place_id, parent_place_chain, starts_at, ends_at, status, confidence_band, impact_score, schema_version, attrs, evidence

Notes
- parent_place_chain is normalized from JSON text when present.

## GET /v1/observations

- Summary: List observations
- Auth: Required (`X-API-Key`)
- Kind: `observations`
- Item kind: `observation`
- Response container: `items`
- Response sort: `observation_id:asc`

Path parameters
- none

Query parameters
- `limit` (int, optional): Page size, default 200, max 1000.
- `cursor` (string, optional): Opaque base64url cursor from prior response next_cursor.
- `offset` (int, optional): Skip this many rows before returning results. Non-negative integer; mutually exclusive with cursor.
- `fields` (csv, optional): Optional projected field list; all fields returned when omitted.
- `q` (string, optional): Case-insensitive search text matched across route-specific searchable columns.
- `observation_type` (string, optional): Allowlisted exact-match filter parameter.
- `place_id` (string, optional): Allowlisted exact-match filter parameter.
- `source_id` (string, optional): Allowlisted exact-match filter parameter.
- `subject_id` (string, optional): Allowlisted exact-match filter parameter.
- `subject_type` (string, optional): Allowlisted exact-match filter parameter.

Selectable fields
- observation_id, source_id, subject_type, subject_id, observation_type, place_id, parent_place_chain, observed_at, published_at, confidence_band, measurement_unit, measurement_value, schema_version, attrs, evidence

Notes
- parent_place_chain is normalized from JSON text when present.

## GET /v1/observations/{recordId}

- Summary: Get a single observation
- Auth: Required (`X-API-Key`)
- Kind: `observations`
- Item kind: `observation`
- Response container: `item`
- Response sort: `observation_id:asc`

Path parameters
- `recordId` (string, required): Path identifier segment.

Query parameters
- `fields` (csv, optional): Optional projected field list; all fields returned when omitted.

Selectable fields
- observation_id, source_id, subject_type, subject_id, observation_type, place_id, parent_place_chain, observed_at, published_at, confidence_band, measurement_unit, measurement_value, schema_version, attrs, evidence

Notes
- parent_place_chain is normalized from JSON text when present.

## GET /v1/metrics

- Summary: List metric definitions
- Auth: Required (`X-API-Key`)
- Kind: `metrics`
- Item kind: `metric`
- Response container: `items`
- Response sort: `metric_id:asc`

Path parameters
- none

Query parameters
- `limit` (int, optional): Page size, default 200, max 1000.
- `cursor` (string, optional): Opaque base64url cursor from prior response next_cursor.
- `offset` (int, optional): Skip this many rows before returning results. Non-negative integer; mutually exclusive with cursor.
- `fields` (csv, optional): Optional projected field list; all fields returned when omitted.
- `q` (string, optional): Case-insensitive search text matched across route-specific searchable columns.
- `enabled` (string, optional): Allowlisted exact-match filter parameter.
- `metric_family` (string, optional): Allowlisted exact-match filter parameter.
- `subject_grain` (string, optional): Allowlisted exact-match filter parameter.

Selectable fields
- metric_id, metric_family, subject_grain, unit, value_type, rollup_engine, rollup_rule, enabled, updated_at, attrs, evidence

Notes
- enabled is normalized from ClickHouse scalar values.

## GET /v1/metrics/{metricId}

- Summary: Get a single metric definition
- Auth: Required (`X-API-Key`)
- Kind: `metrics`
- Item kind: `metric`
- Response container: `item`
- Response sort: `metric_id:asc`

Path parameters
- `metricId` (string, required): Path identifier segment.

Query parameters
- `fields` (csv, optional): Optional projected field list; all fields returned when omitted.

Selectable fields
- metric_id, metric_family, subject_grain, unit, value_type, rollup_engine, rollup_rule, enabled, updated_at, attrs, evidence

Notes
- enabled is normalized from ClickHouse scalar values.

## GET /v1/analytics/rollups

- Summary: List metric rollups
- Auth: Required (`X-API-Key`)
- Kind: `metric_rollups`
- Item kind: `metric_rollup`
- Response container: `items`
- Response sort: `snapshot_id:asc`

Path parameters
- none

Query parameters
- `limit` (int, optional): Page size, default 200, max 1000.
- `cursor` (string, optional): Opaque base64url cursor from prior response next_cursor.
- `offset` (int, optional): Skip this many rows before returning results. Non-negative integer; mutually exclusive with cursor.
- `fields` (csv, optional): Optional projected field list; all fields returned when omitted.
- `q` (string, optional): Case-insensitive search text matched across route-specific searchable columns.
- `metric_id` (string, optional): Allowlisted exact-match filter parameter.
- `place_id` (string, optional): Allowlisted exact-match filter parameter.
- `subject_grain` (string, optional): Allowlisted exact-match filter parameter.
- `subject_id` (string, optional): Allowlisted exact-match filter parameter.
- `window_grain` (string, optional): Allowlisted exact-match filter parameter.

Selectable fields
- snapshot_id, metric_id, subject_grain, subject_id, place_id, window_grain, window_start, window_end, snapshot_at, metric_value, metric_delta, rank, attrs, evidence

## GET /v1/analytics/time-series

- Summary: List metric time-series points
- Auth: Required (`X-API-Key`)
- Kind: `metric_time_series`
- Item kind: `metric_point`
- Response container: `items`
- Response sort: `point_id:asc`

Path parameters
- none

Query parameters
- `limit` (int, optional): Page size, default 200, max 1000.
- `cursor` (string, optional): Opaque base64url cursor from prior response next_cursor.
- `offset` (int, optional): Skip this many rows before returning results. Non-negative integer; mutually exclusive with cursor.
- `fields` (csv, optional): Optional projected field list; all fields returned when omitted.
- `q` (string, optional): Case-insensitive search text matched across route-specific searchable columns.
- `metric_id` (string, optional): Allowlisted exact-match filter parameter.
- `place_id` (string, optional): Allowlisted exact-match filter parameter.
- `subject_grain` (string, optional): Allowlisted exact-match filter parameter.
- `subject_id` (string, optional): Allowlisted exact-match filter parameter.
- `window_grain` (string, optional): Allowlisted exact-match filter parameter.

Selectable fields
- point_id, metric_id, subject_grain, subject_id, place_id, window_grain, window_start, window_end, snapshot_at, metric_value, metric_delta, rank

## GET /v1/analytics/hotspots

- Summary: List metric hotspots
- Auth: Required (`X-API-Key`)
- Kind: `metric_hotspots`
- Item kind: `hotspot`
- Response container: `items`
- Response sort: `hotspot_id:asc`

Path parameters
- none

Query parameters
- `limit` (int, optional): Page size, default 200, max 1000.
- `cursor` (string, optional): Opaque base64url cursor from prior response next_cursor.
- `offset` (int, optional): Skip this many rows before returning results. Non-negative integer; mutually exclusive with cursor.
- `fields` (csv, optional): Optional projected field list; all fields returned when omitted.
- `q` (string, optional): Case-insensitive search text matched across route-specific searchable columns.
- `metric_id` (string, optional): Allowlisted exact-match filter parameter.
- `place_id` (string, optional): Allowlisted exact-match filter parameter.
- `scope_id` (string, optional): Allowlisted exact-match filter parameter.
- `scope_type` (string, optional): Allowlisted exact-match filter parameter.
- `window_grain` (string, optional): Allowlisted exact-match filter parameter.

Selectable fields
- hotspot_id, metric_id, scope_type, scope_id, place_id, snapshot_at, window_grain, window_start, window_end, rank, hotspot_score, attrs, evidence

## GET /v1/analytics/cross-domain

- Summary: List cross-domain metric composites
- Auth: Required (`X-API-Key`)
- Kind: `metric_cross_domain`
- Item kind: `cross_domain`
- Response container: `items`
- Response sort: `cross_domain_id:asc`

Path parameters
- none

Query parameters
- `limit` (int, optional): Page size, default 200, max 1000.
- `cursor` (string, optional): Opaque base64url cursor from prior response next_cursor.
- `offset` (int, optional): Skip this many rows before returning results. Non-negative integer; mutually exclusive with cursor.
- `fields` (csv, optional): Optional projected field list; all fields returned when omitted.
- `q` (string, optional): Case-insensitive search text matched across route-specific searchable columns.
- `place_id` (string, optional): Allowlisted exact-match filter parameter.
- `subject_grain` (string, optional): Allowlisted exact-match filter parameter.
- `subject_id` (string, optional): Allowlisted exact-match filter parameter.

Selectable fields
- cross_domain_id, subject_grain, subject_id, place_id, domains, composite_score, snapshot_at, metric_ids, attrs, evidence

Notes
- domains and metric_ids are normalized from JSON text when present.

## GET /v1/search

- Summary: Combined place/entity search with cursor pagination
- Auth: Required (`X-API-Key`)
- Kind: `search`
- Item kind: `search_result`
- Response container: `items`
- Response sort: `cursor_key:asc`

Path parameters
- none

Query parameters
- `q` (string, optional): Case-insensitive search text applied to both place and entity dimensions.
- `limit` (int, optional): Page size, default 200, max 1000.
- `cursor` (string, optional): Opaque base64url cursor from prior response next_cursor.
- `offset` (int, optional): Skip this many rows before returning results. Non-negative integer; mutually exclusive with cursor.
- `fields` (csv, optional): Optional projected field list for combined search rows.

Selectable fields
- kind, place_id, entity_id, canonical_name, place_type, entity_type, country_code, continent_code, risk_band, primary_place_id

Notes
- Search merges place and entity rows then sorts by synthetic cursor_key.
- next_cursor is present when additional merged rows are available.

## GET /v1/search/classes

- Summary: List distinct entity and place data classes with counts
- Auth: Required (`X-API-Key`)
- Kind: `classes`
- Item kind: `schema_class`
- Response container: `items`

Path parameters
- none

Query parameters
- none

Selectable fields
- none

Notes
- Returns all distinct entity_type and place_type values with row counts.
- category and description are merged from operator-curated seed metadata when available.

## GET /v1/search/places

- Summary: List place search results
- Auth: Required (`X-API-Key`)
- Kind: `search_places`
- Item kind: `place`
- Response container: `items`
- Response sort: `place_id:asc`

Path parameters
- none

Query parameters
- `limit` (int, optional): Page size, default 200, max 1000.
- `cursor` (string, optional): Opaque base64url cursor from prior response next_cursor.
- `offset` (int, optional): Skip this many rows before returning results. Non-negative integer; mutually exclusive with cursor.
- `fields` (csv, optional): Optional projected field list; all fields returned when omitted.
- `q` (string, required): Case-insensitive search text matched across route-specific searchable columns.
- `continent_code` (string, optional): Allowlisted exact-match filter parameter.
- `country_code` (string, optional): Allowlisted exact-match filter parameter.
- `place_type` (string, optional): Allowlisted exact-match filter parameter.

Selectable fields
- place_id, canonical_name, place_type, country_code, continent_code

## GET /v1/search/entities

- Summary: List entity search results
- Auth: Required (`X-API-Key`)
- Kind: `search_entities`
- Item kind: `entity`
- Response container: `items`
- Response sort: `entity_id:asc`

Path parameters
- none

Query parameters
- `limit` (int, optional): Page size, default 200, max 1000.
- `cursor` (string, optional): Opaque base64url cursor from prior response next_cursor.
- `offset` (int, optional): Skip this many rows before returning results. Non-negative integer; mutually exclusive with cursor.
- `fields` (csv, optional): Optional projected field list; all fields returned when omitted.
- `q` (string, required): Case-insensitive search text matched across route-specific searchable columns.
- `entity_type` (string, optional): Allowlisted exact-match filter parameter.
- `primary_place_id` (string, optional): Allowlisted exact-match filter parameter.
- `risk_band` (string, optional): Allowlisted exact-match filter parameter.

Selectable fields
- entity_id, canonical_name, entity_type, risk_band, primary_place_id

## GET /v1/query-dialects

- Summary: List registered OIDA-QL query dialects
- Auth: Required (`X-API-Key`)
- Kind: `query_dialects`
- Item kind: `query_dialect`
- Response container: `items`
- Response sort: `dialect:asc`

Path parameters
- none

Query parameters
- `limit` (int, optional): Page size, default 200, max 1000.
- `cursor` (string, optional): Opaque base64url cursor from prior response next_cursor.
- `offset` (int, optional): Skip this many rows before returning results. Non-negative integer; mutually exclusive with cursor.
- `fields` (csv, optional): Optional projected field list; all fields returned when omitted.
- `q` (string, optional): Case-insensitive search text matched across route-specific searchable columns.
- `case_sensitivity` (string, optional): Allowlisted exact-match filter parameter.
- `shape_policy` (string, optional): Allowlisted exact-match filter parameter.

Selectable fields
- dialect, entity_projection_rule, shape_policy, case_sensitivity, max_timeout_ms, comment_prefix, enabled, schema_version, record_version, api_contract_version, updated_at, attrs, evidence

## GET /v1/registry/{name}

- Summary: Fetch a saved query by name
- Auth: Required (`X-API-Key`)
- Kind: `saved_query`
- Response container: `item`

Path parameters
- `name` (string, required): Path identifier segment.

Query parameters
- `version` (string, optional): Specific version to retrieve; defaults to latest.

Selectable fields
- none

Notes
- Returns the latest version when ?version= is omitted.

## GET /v1/internal/stats

- Summary: Service-side dashboard statistics
- Auth: Required (`X-API-Key`)
- Kind: `internal_stats`
- Item kind: `internal_stat`
- Response container: `item`

Path parameters
- none

Query parameters
- none

Selectable fields
- none

Notes
- Protected operational endpoint for internal dashboards.

## GET /v1/internal/worker-tail

- Summary: Recent worker and control-plane activity tail
- Auth: Required (`X-API-Key`)
- Kind: `worker_tail`
- Item kind: `worker_tail_entry`
- Response container: `items`

Path parameters
- none

Query parameters
- `limit` (integer, optional): Maximum number of tail entries to return.
- `cursor` (string, optional): Opaque cursor for older tail entries.
- `source_id` (string, optional): Optional source filter across fetch and parse activity.
- `correlation_id` (string, optional): Optional correlation filter across API, workers, and control-plane jobs.

Selectable fields
- none

Notes
- Protected operational endpoint backed by persisted worker/control-plane ledgers.

