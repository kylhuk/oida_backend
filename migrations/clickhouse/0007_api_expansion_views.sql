DROP VIEW IF EXISTS gold.api_v1_jobs;

CREATE VIEW IF NOT EXISTS gold.api_v1_jobs AS
SELECT
    job_id,
    job_type,
    status,
    started_at,
    finished_at,
    message,
    stats
FROM ops.job_run;

DROP VIEW IF EXISTS gold.api_v1_places;

CREATE VIEW IF NOT EXISTS gold.api_v1_places AS
SELECT
    place_id,
    parent_place_id,
    canonical_name,
    place_type,
    admin_level,
    country_code,
    continent_code,
    source_place_key,
    source_system,
    status,
    centroid_lat,
    centroid_lon,
    bbox_min_lat,
    bbox_min_lon,
    bbox_max_lat,
    bbox_max_lon,
    valid_from,
    valid_to,
    schema_version,
    record_version,
    api_contract_version,
    updated_at,
    attrs,
    evidence
FROM silver.dim_place FINAL;

DROP VIEW IF EXISTS gold.api_v1_entities;

CREATE VIEW IF NOT EXISTS gold.api_v1_entities AS
SELECT
    entity_id,
    entity_type,
    canonical_name,
    status,
    risk_band,
    primary_place_id,
    source_system,
    valid_from,
    valid_to,
    schema_version,
    record_version,
    api_contract_version,
    updated_at,
    attrs,
    evidence
FROM silver.dim_entity FINAL;

DROP VIEW IF EXISTS gold.api_v1_events;

CREATE VIEW IF NOT EXISTS gold.api_v1_events AS
SELECT
    event_id,
    source_id,
    event_type,
    event_subtype,
    place_id,
    parent_place_chain,
    starts_at,
    ends_at,
    status,
    confidence_band,
    impact_score,
    schema_version,
    attrs,
    evidence
FROM silver.fact_event;

DROP VIEW IF EXISTS gold.api_v1_observations;

CREATE VIEW IF NOT EXISTS gold.api_v1_observations AS
SELECT
    observation_id,
    source_id,
    subject_type,
    subject_id,
    observation_type,
    place_id,
    parent_place_chain,
    observed_at,
    published_at,
    confidence_band,
    measurement_unit,
    measurement_value,
    schema_version,
    attrs,
    evidence
FROM silver.fact_observation;

DROP VIEW IF EXISTS gold.api_v1_metrics;

CREATE VIEW IF NOT EXISTS gold.api_v1_metrics AS
SELECT
    metric_id,
    metric_family,
    subject_grain,
    unit,
    value_type,
    rollup_engine,
    rollup_rule,
    enabled,
    updated_at,
    attrs,
    evidence
FROM meta.metric_registry FINAL;

DROP VIEW IF EXISTS gold.api_v1_source_coverage;

CREATE VIEW IF NOT EXISTS gold.api_v1_source_coverage AS
SELECT
    concat(source_id, ':coverage') AS coverage_id,
    source_id,
    'source' AS scope_type,
    source_id AS scope_id,
    geo_scope,
    toUInt32(length(expected_place_types)) AS place_count,
    toUInt32(0) AS event_count,
    updated_at
FROM meta.source_registry FINAL;

DROP VIEW IF EXISTS gold.api_v1_metric_rollups;

CREATE VIEW IF NOT EXISTS gold.api_v1_metric_rollups AS
SELECT
    snapshot_id,
    metric_id,
    subject_grain,
    subject_id,
    place_id,
    window_grain,
    window_start,
    window_end,
    snapshot_at,
    metric_value,
    metric_delta,
    rank,
    attrs,
    evidence
FROM gold.metric_snapshot;

DROP VIEW IF EXISTS gold.api_v1_time_series;

CREATE VIEW IF NOT EXISTS gold.api_v1_time_series AS
SELECT
    concat(metric_id, ':', subject_id, ':', toString(window_start)) AS point_id,
    metric_id,
    subject_grain,
    subject_id,
    place_id,
    window_grain,
    window_start,
    window_end,
    snapshot_at,
    metric_value,
    metric_delta,
    rank
FROM gold.metric_snapshot;

DROP VIEW IF EXISTS gold.api_v1_hotspots;

CREATE VIEW IF NOT EXISTS gold.api_v1_hotspots AS
SELECT
    hotspot_id,
    metric_id,
    scope_type,
    scope_id,
    place_id,
    snapshot_at,
    window_grain,
    window_start,
    window_end,
    rank,
    hotspot_score,
    attrs,
    evidence
FROM gold.hotspot_snapshot;

DROP VIEW IF EXISTS gold.api_v1_tracks;

CREATE VIEW IF NOT EXISTS gold.api_v1_tracks AS
SELECT
    track_segment_id AS track_record_id,
    track_id,
    track_type,
    entity_id,
    from_place_id AS place_id,
    from_place_id,
    to_place_id,
    started_at,
    ended_at,
    distance_km,
    point_count,
    avg_speed_kph
FROM silver.fact_track_segment;

DROP VIEW IF EXISTS gold.api_v1_entity_events;

CREATE VIEW IF NOT EXISTS gold.api_v1_entity_events AS
SELECT
    bee.entity_id,
    e.event_id,
    e.event_type,
    e.event_subtype,
    e.place_id,
    e.starts_at,
    e.status,
    e.confidence_band,
    e.impact_score
FROM silver.bridge_event_entity AS bee
INNER JOIN gold.api_v1_events AS e ON e.event_id = bee.event_id;

DROP VIEW IF EXISTS gold.api_v1_entity_places;

CREATE VIEW IF NOT EXISTS gold.api_v1_entity_places AS
SELECT
    bep.entity_id,
    p.place_id,
    p.canonical_name,
    p.place_type,
    bep.relation_type,
    bep.linked_at
FROM silver.bridge_entity_place AS bep
INNER JOIN gold.api_v1_places AS p ON p.place_id = bep.place_id;

DROP VIEW IF EXISTS gold.api_v1_cross_domain;

CREATE VIEW IF NOT EXISTS gold.api_v1_cross_domain AS
SELECT
    snapshot_id AS cross_domain_id,
    subject_grain,
    subject_id,
    place_id,
    [metric_id] AS domains,
    metric_value AS composite_score,
    snapshot_at,
    [metric_id] AS metric_ids,
    attrs,
    evidence
FROM gold.metric_snapshot;
