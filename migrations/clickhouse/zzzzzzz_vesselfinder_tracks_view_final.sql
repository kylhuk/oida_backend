-- migrate:diff_summary Re-apply the VesselFinder-aware tracks API view after legacy non-numbered gold view migrations.
-- migrate:compatibility_notes Replacement gold view only; needed because older non-numbered migrations sort after numbered migrations on fresh installs.
-- migrate:approval_ref vessel-finder-tracks-view-final

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
