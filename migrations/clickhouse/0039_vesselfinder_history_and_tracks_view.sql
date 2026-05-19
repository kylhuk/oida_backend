-- migrate:diff_summary Wire VesselFinder bronze rows into ops state/history tables and make the tracks API use the latest VesselFinder parser output.
-- migrate:compatibility_notes Append-only materialized views and a replacement gold view; existing polluted silver rows are left intact but no longer exposed through gold for VesselFinder.
-- migrate:approval_ref vessel-finder-history-and-location-repair

CREATE MATERIALIZED VIEW IF NOT EXISTS ops.mv_vesselfinder_bronze_entity_to_vessel_state
TO ops.vesselfinder_vessel_state AS
SELECT
    source_id,
    JSONExtractString(payload_json, 'detail_id') AS detail_id,
    JSONExtractString(payload_json, 'entity_id') AS entity_id,
    toFixedString(coalesce(nullIf(JSONExtractString(attrs, 'metadata_fingerprint'), ''), '00000000000000000000000000000000'), 32) AS metadata_fingerprint,
    coalesce(occurred_at, parsed_at, fetched_at) AS last_seen_at,
    payload_json,
    schema_version,
    toUInt64(toUnixTimestamp64Nano(parsed_at)) AS record_version,
    parsed_at AS updated_at,
    attrs,
    evidence
FROM bronze.`src_catalog-auto-maritime-ocean-and-coastal-_f8f33fd7_v1`
WHERE record_kind = 'entity'
  AND parser_version >= '1.0.4'
  AND JSONExtractString(payload_json, 'detail_id') != ''
  AND JSONExtractString(payload_json, 'entity_id') != '';

CREATE MATERIALIZED VIEW IF NOT EXISTS ops.mv_vesselfinder_bronze_track_to_position_history
TO ops.vesselfinder_position_history AS
SELECT
    source_id,
    JSONExtractString(payload_json, 'detail_id') AS detail_id,
    JSONExtractString(payload_json, 'entity_id') AS entity_id,
    coalesce(occurred_at, parsed_at, fetched_at) AS observed_at,
    coalesce(lat, JSONExtractFloat(payload_json, 'lat'), JSONExtractFloat(payload_json, 'latitude')) AS latitude,
    coalesce(lon, JSONExtractFloat(payload_json, 'lon'), JSONExtractFloat(payload_json, 'longitude')) AS longitude,
    toNullable(toFloat32(JSONExtractFloat(payload_json, 'speed_kph'))) AS speed_kph,
    toNullable(toFloat32(JSONExtractFloat(payload_json, 'course_deg'))) AS course_deg,
    raw_id,
    schema_version,
    toUInt64(toUnixTimestamp64Nano(parsed_at)) AS record_version,
    attrs,
    evidence
FROM bronze.`src_catalog-auto-maritime-ocean-and-coastal-_f8f33fd7_v1`
WHERE record_kind = 'track_point'
  AND parser_version >= '1.0.4'
  AND JSONExtractString(payload_json, 'detail_id') != ''
  AND JSONExtractString(payload_json, 'entity_id') != ''
  AND coalesce(lat, JSONExtractFloat(payload_json, 'lat'), JSONExtractFloat(payload_json, 'latitude')) BETWEEN -90 AND 90
  AND coalesce(lon, JSONExtractFloat(payload_json, 'lon'), JSONExtractFloat(payload_json, 'longitude')) BETWEEN -180 AND 180;

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
FROM silver.fact_track_point
WHERE source_id != 'catalog:auto:maritime-ocean-and-coastal-sources-vesselfinder'
UNION ALL
SELECT
    concat('trkpt:', source_id, ':', source_record_key, ':', toString(source_record_index)) AS track_record_id,
    coalesce(nullIf(JSONExtractString(payload_json, 'track_id'), ''), concat('trk:vessel:', coalesce(nullIf(JSONExtractString(payload_json, 'imo'), ''), nullIf(JSONExtractString(payload_json, 'mmsi'), ''), ifNull(native_id, source_record_key)))) AS track_id,
    'vessel' AS track_type,
    coalesce(nullIf(JSONExtractString(payload_json, 'entity_id'), ''), concat('ent:vessel:', coalesce(nullIf(JSONExtractString(payload_json, 'imo'), ''), nullIf(JSONExtractString(payload_json, 'mmsi'), ''), ifNull(native_id, source_record_key)))) AS entity_id,
    coalesce(nullIf(JSONExtractString(payload_json, 'place_id'), ''), ifNull(place_hint, '')) AS place_id,
    '' AS from_place_id,
    '' AS to_place_id,
    coalesce(occurred_at, parsed_at, fetched_at) AS started_at,
    coalesce(occurred_at, parsed_at, fetched_at) AS ended_at,
    CAST(NULL, 'Nullable(Float64)') AS distance_km,
    CAST(NULL, 'Nullable(UInt32)') AS point_count,
    CAST(NULL, 'Nullable(Float32)') AS avg_speed_kph,
    source_id,
    coalesce(occurred_at, parsed_at, fetched_at) AS observed_at,
    toNullable(coalesce(lat, JSONExtractFloat(payload_json, 'lat'), JSONExtractFloat(payload_json, 'latitude'))) AS latitude,
    toNullable(coalesce(lon, JSONExtractFloat(payload_json, 'lon'), JSONExtractFloat(payload_json, 'longitude'))) AS longitude,
    toNullable(toFloat32(JSONExtractFloat(payload_json, 'speed_kph'))) AS speed_kph,
    toNullable(toFloat32(JSONExtractFloat(payload_json, 'course_deg'))) AS course_deg
FROM bronze.`src_catalog-auto-maritime-ocean-and-coastal-_f8f33fd7_v1`
WHERE record_kind = 'track_point'
  AND parser_version = (
      SELECT max(parser_version)
      FROM bronze.`src_catalog-auto-maritime-ocean-and-coastal-_f8f33fd7_v1`
      WHERE source_id = 'catalog:auto:maritime-ocean-and-coastal-sources-vesselfinder'
        AND record_kind = 'track_point'
  );
