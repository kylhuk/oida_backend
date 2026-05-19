-- migrate:diff_summary Add AISstream.io bronze landing table, ops subscription state, and silver MVs (entity + track_point)
-- migrate:compatibility_notes New tables/MVs only; clones canonical bronze shape; record_version uses occurred_at for AIS-native dedup so latest AIS-emitted timestamp wins across re-deliveries.
-- migrate:approval_ref aisstream-ingestion

CREATE TABLE IF NOT EXISTS bronze.`src_catalog-auto-maritime-ocean-and-coastal-_fbd36bff_v1`
AS bronze.src_seed_gdelt_v1;

CREATE TABLE IF NOT EXISTS ops.aisstream_subscription_state (
    bbox_id         LowCardinality(String),
    subscribed_at   DateTime64(3, 'UTC'),
    last_frame_at   Nullable(DateTime64(3, 'UTC')),
    frames_received UInt64,
    connection_id   String,
    status          LowCardinality(String),
    updated_at      DateTime64(3, 'UTC') DEFAULT now64(),
    record_version  UInt64 MATERIALIZED toUInt64(toUnixTimestamp64Nano(updated_at))
) ENGINE = ReplacingMergeTree(record_version)
  ORDER BY (bbox_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS silver.mv_source_catalog_auto_maritime_ocean_and_coastal_sources_aisstream_to_dim_entity
TO silver.dim_entity AS
SELECT
    coalesce(
        nullIf(JSONExtractString(payload_json, 'entity_id'), ''),
        if(JSONExtractString(payload_json, 'imo') != '', concat('ent:vessel:imo:', JSONExtractString(payload_json, 'imo')), ''),
        if(JSONExtractString(payload_json, 'mmsi') != '', concat('ent:vessel:mmsi:', JSONExtractString(payload_json, 'mmsi')), ''),
        source_record_key
    ) AS entity_id,
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
    toUInt64(toUnixTimestamp64Nano(occurred_at)) AS record_version,
    toUInt32(1) AS api_contract_version,
    occurred_at AS updated_at,
    attrs,
    evidence
FROM bronze.`src_catalog-auto-maritime-ocean-and-coastal-_fbd36bff_v1`
WHERE record_kind = 'entity'
  AND parser_id = 'parser:aisstream-json';

CREATE MATERIALIZED VIEW IF NOT EXISTS silver.mv_source_catalog_auto_maritime_ocean_and_coastal_sources_aisstream_to_fact_track_point
TO silver.fact_track_point AS
SELECT
    source_record_key AS track_point_id,
    coalesce(nullIf(JSONExtractString(payload_json, 'track_id'), ''), concat('trk:vessel:mmsi:', JSONExtractString(payload_json, 'mmsi'))) AS track_id,
    source_id,
    'vessel' AS track_type,
    coalesce(
        nullIf(JSONExtractString(payload_json, 'entity_id'), ''),
        if(JSONExtractString(payload_json, 'imo') != '', concat('ent:vessel:imo:', JSONExtractString(payload_json, 'imo')), ''),
        if(JSONExtractString(payload_json, 'mmsi') != '', concat('ent:vessel:mmsi:', JSONExtractString(payload_json, 'mmsi')), ''),
        source_record_key
    ) AS entity_id,
    coalesce(nullIf(JSONExtractString(payload_json, 'place_id'), ''), ifNull(place_hint, '')) AS place_id,
    occurred_at AS observed_at,
    coalesce(lat, JSONExtractFloat(payload_json, 'lat'), JSONExtractFloat(payload_json, 'latitude')) AS latitude,
    coalesce(lon, JSONExtractFloat(payload_json, 'lon'), JSONExtractFloat(payload_json, 'longitude')) AS longitude,
    toNullable(JSONExtractFloat(payload_json, 'altitude_m')) AS altitude_m,
    toNullable(toFloat32(JSONExtractFloat(payload_json, 'speed_kph'))) AS speed_kph,
    toNullable(toFloat32(JSONExtractFloat(payload_json, 'course_deg'))) AS course_deg,
    schema_version,
    attrs,
    evidence
FROM bronze.`src_catalog-auto-maritime-ocean-and-coastal-_fbd36bff_v1`
WHERE record_kind = 'track_point'
  AND parser_id = 'parser:aisstream-json';

INSERT INTO silver.dim_entity
SELECT
    coalesce(
        nullIf(JSONExtractString(payload_json, 'entity_id'), ''),
        if(JSONExtractString(payload_json, 'imo') != '', concat('ent:vessel:imo:', JSONExtractString(payload_json, 'imo')), ''),
        if(JSONExtractString(payload_json, 'mmsi') != '', concat('ent:vessel:mmsi:', JSONExtractString(payload_json, 'mmsi')), ''),
        source_record_key
    ) AS entity_id,
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
    toUInt64(toUnixTimestamp64Nano(occurred_at)) AS record_version,
    toUInt32(1) AS api_contract_version,
    occurred_at AS updated_at,
    attrs,
    evidence
FROM bronze.`src_catalog-auto-maritime-ocean-and-coastal-_fbd36bff_v1` FINAL
WHERE record_kind = 'entity'
  AND parser_id = 'parser:aisstream-json';

INSERT INTO silver.fact_track_point
SELECT
    source_record_key AS track_point_id,
    coalesce(nullIf(JSONExtractString(payload_json, 'track_id'), ''), concat('trk:vessel:mmsi:', JSONExtractString(payload_json, 'mmsi'))) AS track_id,
    source_id,
    'vessel' AS track_type,
    coalesce(
        nullIf(JSONExtractString(payload_json, 'entity_id'), ''),
        if(JSONExtractString(payload_json, 'imo') != '', concat('ent:vessel:imo:', JSONExtractString(payload_json, 'imo')), ''),
        if(JSONExtractString(payload_json, 'mmsi') != '', concat('ent:vessel:mmsi:', JSONExtractString(payload_json, 'mmsi')), ''),
        source_record_key
    ) AS entity_id,
    coalesce(nullIf(JSONExtractString(payload_json, 'place_id'), ''), ifNull(place_hint, '')) AS place_id,
    occurred_at AS observed_at,
    coalesce(lat, JSONExtractFloat(payload_json, 'lat'), JSONExtractFloat(payload_json, 'latitude')) AS latitude,
    coalesce(lon, JSONExtractFloat(payload_json, 'lon'), JSONExtractFloat(payload_json, 'longitude')) AS longitude,
    toNullable(JSONExtractFloat(payload_json, 'altitude_m')) AS altitude_m,
    toNullable(toFloat32(JSONExtractFloat(payload_json, 'speed_kph'))) AS speed_kph,
    toNullable(toFloat32(JSONExtractFloat(payload_json, 'course_deg'))) AS course_deg,
    schema_version,
    attrs,
    evidence
FROM bronze.`src_catalog-auto-maritime-ocean-and-coastal-_fbd36bff_v1` FINAL
WHERE record_kind = 'track_point'
  AND parser_id = 'parser:aisstream-json';
