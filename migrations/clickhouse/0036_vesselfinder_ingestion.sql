-- migrate:diff_summary Add opt-in VesselFinder browser-rendered ingestion state, bronze routing, silver materialization, and point-aware tracks API view.
-- migrate:compatibility_notes New tables and views are append-only except gold.api_v1_tracks, which is replaced to preserve existing segment columns while adding nullable point fields.
-- migrate:approval_ref vessel-finder-ingestion

CREATE TABLE IF NOT EXISTS ops.vesselfinder_dimension
(
    source_id String,
    dimension_kind LowCardinality(String),
    dimension_code String,
    dimension_label String,
    discovered_at DateTime64(3, 'UTC'),
    schema_version UInt16 DEFAULT 1,
    record_version UInt64 DEFAULT 1,
    updated_at DateTime64(3, 'UTC') DEFAULT now64(3),
    attrs String DEFAULT '{}',
    evidence String DEFAULT '[]'
)
ENGINE = ReplacingMergeTree(record_version)
ORDER BY (source_id, dimension_kind, dimension_code);

CREATE TABLE IF NOT EXISTS ops.vesselfinder_page_job
(
    source_id String,
    country_code String,
    type_code String,
    page UInt16,
    status LowCardinality(String),
    status_code UInt16 DEFAULT 0,
    last_error_code String DEFAULT '',
    updated_at DateTime64(3, 'UTC') DEFAULT now64(3),
    schema_version UInt16 DEFAULT 1,
    record_version UInt64 DEFAULT 1,
    attrs String DEFAULT '{}',
    evidence String DEFAULT '[]'
)
ENGINE = ReplacingMergeTree(record_version)
ORDER BY (source_id, country_code, type_code, page);

CREATE TABLE IF NOT EXISTS ops.vesselfinder_scan_queue
(
    source_id String,
    detail_id String,
    detail_url String,
    status LowCardinality(String),
    discovered_at DateTime64(3, 'UTC'),
    next_scan_at DateTime64(3, 'UTC'),
    last_scanned_at Nullable(DateTime64(3, 'UTC')),
    attempt_count UInt16 DEFAULT 0,
    lease_owner String DEFAULT '',
    lease_expires_at Nullable(DateTime64(3, 'UTC')),
    last_error_code String DEFAULT '',
    status_code UInt16 DEFAULT 0,
    updated_at DateTime64(3, 'UTC') DEFAULT now64(3),
    schema_version UInt16 DEFAULT 1,
    record_version UInt64 DEFAULT 1,
    attrs String DEFAULT '{}',
    evidence String DEFAULT '[]'
)
ENGINE = ReplacingMergeTree(record_version)
ORDER BY (source_id, detail_id);

CREATE TABLE IF NOT EXISTS ops.vesselfinder_vessel_state
(
    source_id String,
    detail_id String,
    entity_id String,
    metadata_fingerprint FixedString(32),
    last_seen_at DateTime64(3, 'UTC'),
    payload_json String CODEC(ZSTD(3)),
    schema_version UInt16 DEFAULT 1,
    record_version UInt64 DEFAULT 1,
    updated_at DateTime64(3, 'UTC') DEFAULT now64(3),
    attrs String DEFAULT '{}',
    evidence String DEFAULT '[]'
)
ENGINE = ReplacingMergeTree(record_version)
ORDER BY (source_id, detail_id);

CREATE TABLE IF NOT EXISTS ops.vesselfinder_vessel_change
(
    source_id String,
    detail_id String,
    entity_id String,
    field_name LowCardinality(String),
    old_value String,
    new_value String,
    changed_at DateTime64(3, 'UTC'),
    schema_version UInt16 DEFAULT 1,
    record_version UInt64 DEFAULT 1,
    attrs String DEFAULT '{}',
    evidence String DEFAULT '[]'
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(changed_at)
ORDER BY (source_id, detail_id, changed_at, field_name);

CREATE TABLE IF NOT EXISTS ops.vesselfinder_position_history
(
    source_id String,
    detail_id String,
    entity_id String,
    observed_at DateTime64(3, 'UTC'),
    latitude Float64,
    longitude Float64,
    speed_kph Nullable(Float32),
    course_deg Nullable(Float32),
    raw_id String,
    schema_version UInt16 DEFAULT 1,
    record_version UInt64 DEFAULT 1,
    attrs String DEFAULT '{}',
    evidence String DEFAULT '[]'
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(observed_at)
ORDER BY (source_id, detail_id, observed_at);

CREATE TABLE IF NOT EXISTS `bronze`.`src_catalog-auto-maritime-ocean-and-coastal-_f8f33fd7_v1` AS `bronze`.`src_seed_gdelt_v1`;

CREATE MATERIALIZED VIEW IF NOT EXISTS silver.mv_source_catalog_auto_maritime_ocean_and_coastal_sources_vesselfinder_to_dim_entity
TO silver.dim_entity AS
SELECT
    coalesce(nullIf(JSONExtractString(payload_json, 'entity_id'), ''), concat('ent:vessel:', coalesce(nullIf(JSONExtractString(payload_json, 'imo'), ''), nullIf(JSONExtractString(payload_json, 'mmsi'), ''), ifNull(native_id, source_record_key)))) AS entity_id,
    'vessel' AS entity_type,
    coalesce(nullIf(JSONExtractString(payload_json, 'canonical_name'), ''), nullIf(title, ''), coalesce(nullIf(native_id, ''), source_record_key)) AS canonical_name,
    coalesce(nullIf(JSONExtractString(payload_json, 'status'), ''), ifNull(status, 'active')) AS status,
    'watch' AS risk_band,
    coalesce(nullIf(JSONExtractString(payload_json, 'place_id'), ''), ifNull(place_hint, '')) AS primary_place_id,
    coalesce(nullIf(native_id, ''), source_record_key) AS source_entity_key,
    source_id AS source_system,
    coalesce(occurred_at, parsed_at, fetched_at) AS valid_from,
    CAST(NULL, 'Nullable(DateTime64(3, \'UTC\'))') AS valid_to,
    schema_version,
    record_version,
    toUInt32(1) AS api_contract_version,
    parsed_at AS updated_at,
    attrs,
    evidence
FROM bronze.`src_catalog-auto-maritime-ocean-and-coastal-_f8f33fd7_v1`
WHERE record_kind = 'entity';

CREATE MATERIALIZED VIEW IF NOT EXISTS silver.mv_source_catalog_auto_maritime_ocean_and_coastal_sources_vesselfinder_to_fact_track_point
TO silver.fact_track_point AS
SELECT
    concat('trkpt:', source_id, ':', source_record_key, ':', toString(source_record_index)) AS track_point_id,
    coalesce(nullIf(JSONExtractString(payload_json, 'track_id'), ''), concat('trk:vessel:', coalesce(nullIf(JSONExtractString(payload_json, 'imo'), ''), nullIf(JSONExtractString(payload_json, 'mmsi'), ''), ifNull(native_id, source_record_key)))) AS track_id,
    source_id,
    'vessel' AS track_type,
    coalesce(nullIf(JSONExtractString(payload_json, 'entity_id'), ''), concat('ent:vessel:', coalesce(nullIf(JSONExtractString(payload_json, 'imo'), ''), nullIf(JSONExtractString(payload_json, 'mmsi'), ''), ifNull(native_id, source_record_key)))) AS entity_id,
    coalesce(nullIf(JSONExtractString(payload_json, 'place_id'), ''), ifNull(place_hint, '')) AS place_id,
    coalesce(occurred_at, parsed_at, fetched_at) AS observed_at,
    coalesce(lat, JSONExtractFloat(payload_json, 'lat'), JSONExtractFloat(payload_json, 'latitude')) AS latitude,
    coalesce(lon, JSONExtractFloat(payload_json, 'lon'), JSONExtractFloat(payload_json, 'longitude')) AS longitude,
    toNullable(JSONExtractFloat(payload_json, 'altitude_m')) AS altitude_m,
    toNullable(toFloat32(JSONExtractFloat(payload_json, 'speed_kph'))) AS speed_kph,
    toNullable(toFloat32(JSONExtractFloat(payload_json, 'course_deg'))) AS course_deg,
    schema_version,
    attrs,
    evidence
FROM bronze.`src_catalog-auto-maritime-ocean-and-coastal-_f8f33fd7_v1`
WHERE record_kind = 'track_point';

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
    toNullable(distance_km) AS distance_km,
    toNullable(point_count) AS point_count,
    avg_speed_kph,
    source_id,
    CAST(NULL, 'Nullable(DateTime64(3, \'UTC\'))') AS observed_at,
    CAST(NULL, 'Nullable(Float64)') AS latitude,
    CAST(NULL, 'Nullable(Float64)') AS longitude,
    CAST(NULL, 'Nullable(Float32)') AS speed_kph,
    CAST(NULL, 'Nullable(Float32)') AS course_deg
FROM silver.fact_track_segment
UNION ALL
SELECT
    track_point_id AS track_record_id,
    track_id,
    track_type,
    entity_id,
    place_id,
    '' AS from_place_id,
    '' AS to_place_id,
    observed_at AS started_at,
    observed_at AS ended_at,
    CAST(NULL, 'Nullable(Float64)') AS distance_km,
    CAST(NULL, 'Nullable(UInt32)') AS point_count,
    CAST(NULL, 'Nullable(Float32)') AS avg_speed_kph,
    source_id,
    observed_at,
    toNullable(latitude) AS latitude,
    toNullable(longitude) AS longitude,
    speed_kph,
    course_deg
FROM silver.fact_track_point;
