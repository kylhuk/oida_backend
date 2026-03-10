CREATE TABLE IF NOT EXISTS ops.parse_log
(
    parse_id String,
    job_id String,
    source_id String,
    parser_id String,
    parser_family LowCardinality(String),
    raw_id String,
    input_format LowCardinality(String),
    status LowCardinality(String),
    started_at DateTime64(3, 'UTC'),
    finished_at Nullable(DateTime64(3, 'UTC')),
    duration_ms UInt32,
    extracted_rows UInt32,
    extracted_entities UInt32,
    error_class LowCardinality(String),
    error_message Nullable(String) CODEC(ZSTD(3)),
    attrs String CODEC(ZSTD(3)),
    evidence String CODEC(ZSTD(3))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(started_at)
ORDER BY (parser_id, status, started_at, parse_id)
TTL started_at + INTERVAL 180 DAY DELETE;

CREATE TABLE IF NOT EXISTS ops.unresolved_location_queue
(
    queue_id String,
    subject_kind LowCardinality(String),
    subject_id String,
    source_id String,
    raw_id String,
    resolver_stage LowCardinality(String),
    failure_reason LowCardinality(String),
    state LowCardinality(String),
    priority Int16,
    retry_count UInt16,
    first_failed_at DateTime64(3, 'UTC'),
    last_failed_at DateTime64(3, 'UTC'),
    next_retry_at DateTime64(3, 'UTC'),
    location_hint String CODEC(ZSTD(3)),
    attrs String CODEC(ZSTD(3)),
    evidence String CODEC(ZSTD(3))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(first_failed_at)
ORDER BY (state, priority, next_retry_at, subject_kind, subject_id, queue_id)
TTL first_failed_at + INTERVAL 365 DAY DELETE WHERE state IN ('resolved', 'discarded');

CREATE TABLE IF NOT EXISTS ops.quality_incident
(
    incident_id String,
    incident_type LowCardinality(String),
    severity LowCardinality(String),
    status LowCardinality(String),
    detector LowCardinality(String),
    subject_kind LowCardinality(String),
    subject_id String,
    detected_at DateTime64(3, 'UTC'),
    acknowledged_at Nullable(DateTime64(3, 'UTC')),
    resolved_at Nullable(DateTime64(3, 'UTC')),
    incident_summary String CODEC(ZSTD(3)),
    attrs String CODEC(ZSTD(3)),
    evidence String CODEC(ZSTD(3))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(detected_at)
ORDER BY (severity, status, detected_at, incident_id)
TTL detected_at + INTERVAL 365 DAY DELETE;

CREATE TABLE IF NOT EXISTS bronze.raw_structured_row
(
    row_id String,
    raw_id String,
    source_id String,
    dataset_key String,
    sheet_name String,
    row_number UInt64,
    extracted_at DateTime64(3, 'UTC'),
    content_hash String,
    row_payload String CODEC(ZSTD(5)),
    attrs String CODEC(ZSTD(3)),
    evidence String CODEC(ZSTD(3))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(extracted_at)
ORDER BY (source_id, extracted_at, raw_id, row_number, row_id)
TTL extracted_at + INTERVAL 180 DAY DELETE;

CREATE TABLE IF NOT EXISTS silver.dim_place
(
    place_id String,
    parent_place_id Nullable(String),
    canonical_name String,
    place_type LowCardinality(String),
    admin_level UInt8,
    country_code LowCardinality(String),
    continent_code LowCardinality(String),
    source_place_key String,
    source_system LowCardinality(String),
    status LowCardinality(String),
    centroid_lat Float64,
    centroid_lon Float64,
    bbox_min_lat Float64,
    bbox_min_lon Float64,
    bbox_max_lat Float64,
    bbox_max_lon Float64,
    valid_from DateTime64(3, 'UTC'),
    valid_to Nullable(DateTime64(3, 'UTC')),
    schema_version UInt32,
    record_version UInt64,
    api_contract_version UInt32,
    updated_at DateTime64(3, 'UTC'),
    attrs String CODEC(ZSTD(3)),
    evidence String CODEC(ZSTD(3))
)
ENGINE = ReplacingMergeTree(record_version)
ORDER BY (country_code, place_type, place_id);

CREATE TABLE IF NOT EXISTS silver.place_polygon
(
    polygon_id String,
    place_id String,
    polygon_role LowCardinality(String),
    geometry_format LowCardinality(String),
    source_system LowCardinality(String),
    ring_count UInt32,
    point_count UInt32,
    bbox_min_lat Float64,
    bbox_min_lon Float64,
    bbox_max_lat Float64,
    bbox_max_lon Float64,
    valid_from DateTime64(3, 'UTC'),
    valid_to Nullable(DateTime64(3, 'UTC')),
    schema_version UInt32,
    record_version UInt64,
    updated_at DateTime64(3, 'UTC'),
    geometry String CODEC(ZSTD(9)),
    attrs String CODEC(ZSTD(3)),
    evidence String CODEC(ZSTD(3))
)
ENGINE = ReplacingMergeTree(record_version)
ORDER BY (place_id, polygon_role, polygon_id);

CREATE TABLE IF NOT EXISTS silver.place_hierarchy
(
    edge_id String,
    ancestor_place_id String,
    descendant_place_id String,
    relationship_type LowCardinality(String),
    depth UInt8,
    is_direct UInt8,
    path_source LowCardinality(String),
    valid_from DateTime64(3, 'UTC'),
    valid_to Nullable(DateTime64(3, 'UTC')),
    schema_version UInt32,
    record_version UInt64,
    updated_at DateTime64(3, 'UTC'),
    attrs String CODEC(ZSTD(3)),
    evidence String CODEC(ZSTD(3))
)
ENGINE = ReplacingMergeTree(record_version)
ORDER BY (ancestor_place_id, relationship_type, depth, descendant_place_id, edge_id);

CREATE TABLE IF NOT EXISTS silver.dim_entity
(
    entity_id String,
    entity_type LowCardinality(String),
    canonical_name String,
    status LowCardinality(String),
    risk_band LowCardinality(String),
    primary_place_id Nullable(String),
    source_entity_key String,
    source_system LowCardinality(String),
    valid_from DateTime64(3, 'UTC'),
    valid_to Nullable(DateTime64(3, 'UTC')),
    schema_version UInt32,
    record_version UInt64,
    api_contract_version UInt32,
    updated_at DateTime64(3, 'UTC'),
    attrs String CODEC(ZSTD(3)),
    evidence String CODEC(ZSTD(3))
)
ENGINE = ReplacingMergeTree(record_version)
ORDER BY (entity_type, entity_id);

CREATE TABLE IF NOT EXISTS silver.entity_alias
(
    alias_id String,
    entity_id String,
    alias_type LowCardinality(String),
    namespace LowCardinality(String),
    alias_value String,
    alias_value_norm String,
    is_primary UInt8,
    valid_from DateTime64(3, 'UTC'),
    valid_to Nullable(DateTime64(3, 'UTC')),
    schema_version UInt32,
    record_version UInt64,
    updated_at DateTime64(3, 'UTC'),
    attrs String CODEC(ZSTD(3)),
    evidence String CODEC(ZSTD(3))
)
ENGINE = ReplacingMergeTree(record_version)
ORDER BY (entity_id, alias_type, namespace, alias_value_norm, alias_id);

CREATE TABLE IF NOT EXISTS silver.fact_observation
(
    observation_id String,
    source_id String,
    subject_type LowCardinality(String),
    subject_id String,
    observation_type LowCardinality(String),
    place_id String,
    parent_place_chain Array(String),
    observed_at DateTime64(3, 'UTC'),
    published_at Nullable(DateTime64(3, 'UTC')),
    confidence_band LowCardinality(String),
    measurement_unit LowCardinality(String),
    measurement_value Float64,
    schema_version UInt32,
    attrs String CODEC(ZSTD(3)),
    evidence String CODEC(ZSTD(3))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(observed_at)
ORDER BY (place_id, observation_type, observed_at, observation_id)
TTL observed_at + INTERVAL 1095 DAY DELETE;

CREATE TABLE IF NOT EXISTS silver.fact_event
(
    event_id String,
    source_id String,
    event_type LowCardinality(String),
    event_subtype LowCardinality(String),
    place_id String,
    parent_place_chain Array(String),
    starts_at DateTime64(3, 'UTC'),
    ends_at Nullable(DateTime64(3, 'UTC')),
    status LowCardinality(String),
    confidence_band LowCardinality(String),
    impact_score Float32,
    schema_version UInt32,
    attrs String CODEC(ZSTD(3)),
    evidence String CODEC(ZSTD(3))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(starts_at)
ORDER BY (place_id, event_type, starts_at, event_id)
TTL starts_at + INTERVAL 1095 DAY DELETE;

CREATE TABLE IF NOT EXISTS silver.fact_track_point
(
    track_point_id String,
    track_id String,
    source_id String,
    track_type LowCardinality(String),
    entity_id String,
    place_id String,
    observed_at DateTime64(3, 'UTC'),
    latitude Float64,
    longitude Float64,
    altitude_m Nullable(Float64),
    speed_kph Nullable(Float32),
    course_deg Nullable(Float32),
    schema_version UInt32,
    attrs String CODEC(ZSTD(3)),
    evidence String CODEC(ZSTD(3))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(observed_at)
ORDER BY (track_id, observed_at, track_point_id)
TTL observed_at + INTERVAL 365 DAY DELETE;

CREATE TABLE IF NOT EXISTS silver.fact_track_segment
(
    track_segment_id String,
    track_id String,
    source_id String,
    track_type LowCardinality(String),
    entity_id String,
    from_place_id String,
    to_place_id String,
    started_at DateTime64(3, 'UTC'),
    ended_at DateTime64(3, 'UTC'),
    point_count UInt32,
    distance_km Float64,
    avg_speed_kph Nullable(Float32),
    schema_version UInt32,
    attrs String CODEC(ZSTD(3)),
    evidence String CODEC(ZSTD(3))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(started_at)
ORDER BY (track_id, started_at, track_segment_id)
TTL started_at + INTERVAL 730 DAY DELETE;

CREATE TABLE IF NOT EXISTS silver.bridge_event_entity
(
    bridge_id String,
    event_id String,
    entity_id String,
    role_type LowCardinality(String),
    confidence_band LowCardinality(String),
    linked_at DateTime64(3, 'UTC'),
    schema_version UInt32,
    attrs String CODEC(ZSTD(3)),
    evidence String CODEC(ZSTD(3))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(linked_at)
ORDER BY (event_id, role_type, entity_id, linked_at, bridge_id)
TTL linked_at + INTERVAL 1095 DAY DELETE;

CREATE TABLE IF NOT EXISTS silver.bridge_event_place
(
    bridge_id String,
    event_id String,
    place_id String,
    relation_type LowCardinality(String),
    linked_at DateTime64(3, 'UTC'),
    schema_version UInt32,
    attrs String CODEC(ZSTD(3)),
    evidence String CODEC(ZSTD(3))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(linked_at)
ORDER BY (event_id, relation_type, place_id, linked_at, bridge_id)
TTL linked_at + INTERVAL 1095 DAY DELETE;

CREATE TABLE IF NOT EXISTS silver.bridge_entity_place
(
    bridge_id String,
    entity_id String,
    place_id String,
    relation_type LowCardinality(String),
    linked_at DateTime64(3, 'UTC'),
    schema_version UInt32,
    attrs String CODEC(ZSTD(3)),
    evidence String CODEC(ZSTD(3))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(linked_at)
ORDER BY (entity_id, relation_type, place_id, linked_at, bridge_id)
TTL linked_at + INTERVAL 1095 DAY DELETE;

CREATE TABLE IF NOT EXISTS silver.metric_contribution
(
    contribution_id String,
    metric_id String,
    subject_grain LowCardinality(String),
    subject_id String,
    source_record_type LowCardinality(String),
    source_record_id String,
    place_id String,
    window_grain LowCardinality(String),
    window_start DateTime64(3, 'UTC'),
    window_end DateTime64(3, 'UTC'),
    contribution_type LowCardinality(String),
    contribution_value Float64,
    contribution_weight Float32,
    schema_version UInt32,
    attrs String CODEC(ZSTD(3)),
    evidence String CODEC(ZSTD(3))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(window_start)
ORDER BY (metric_id, subject_grain, subject_id, window_grain, window_start, source_record_id)
TTL window_start + INTERVAL 730 DAY DELETE;

CREATE TABLE IF NOT EXISTS gold.metric_state
(
    metric_id String,
    subject_grain LowCardinality(String),
    subject_id String,
    place_id String,
    window_grain LowCardinality(String),
    window_start DateTime64(3, 'UTC'),
    window_end DateTime64(3, 'UTC'),
    contribution_count_state AggregateFunction(count),
    contribution_value_state AggregateFunction(sum, Float64),
    contribution_weight_state AggregateFunction(sum, Float64),
    peak_value_state AggregateFunction(max, Float64),
    last_contribution_at_state AggregateFunction(max, DateTime64(3, 'UTC')),
    updated_at DateTime64(3, 'UTC')
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(window_start)
ORDER BY (metric_id, subject_grain, subject_id, window_grain, window_start)
TTL window_start + INTERVAL 730 DAY DELETE;

CREATE TABLE IF NOT EXISTS gold.metric_snapshot
(
    snapshot_id String,
    metric_id String,
    subject_grain LowCardinality(String),
    subject_id String,
    place_id String,
    window_grain LowCardinality(String),
    window_start DateTime64(3, 'UTC'),
    window_end DateTime64(3, 'UTC'),
    snapshot_at DateTime64(3, 'UTC'),
    metric_value Float64,
    metric_delta Float64,
    rank UInt32,
    schema_version UInt32,
    attrs String CODEC(ZSTD(3)),
    evidence String CODEC(ZSTD(3))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(snapshot_at)
ORDER BY (metric_id, subject_grain, subject_id, window_grain, snapshot_at, snapshot_id)
TTL snapshot_at + INTERVAL 365 DAY DELETE;

CREATE TABLE IF NOT EXISTS gold.hotspot_snapshot
(
    hotspot_id String,
    metric_id String,
    scope_type LowCardinality(String),
    scope_id String,
    place_id String,
    snapshot_at DateTime64(3, 'UTC'),
    window_grain LowCardinality(String),
    window_start DateTime64(3, 'UTC'),
    window_end DateTime64(3, 'UTC'),
    rank UInt32,
    hotspot_score Float64,
    schema_version UInt32,
    attrs String CODEC(ZSTD(3)),
    evidence String CODEC(ZSTD(3))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(snapshot_at)
ORDER BY (metric_id, scope_type, scope_id, snapshot_at, rank, hotspot_id)
TTL snapshot_at + INTERVAL 180 DAY DELETE;
