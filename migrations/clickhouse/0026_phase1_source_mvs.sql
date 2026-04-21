CREATE MATERIALIZED VIEW IF NOT EXISTS silver.mv_source_catalog_auto_aviation_airports_drones_and_mobility_opensky_network_to_fact_track_point
TO silver.fact_track_point AS
SELECT
    concat('trkpt:', source_id, ':', source_record_key, ':', toString(source_record_index)) AS track_point_id,
    coalesce(nullIf(JSONExtractString(payload_json, 'track_id'), ''), concat('trk:', source_id, ':', nullIf(lower(ifNull(native_id, '')), ''), ':', source_record_key)) AS track_id,
    source_id,
    'aircraft' AS track_type,
    coalesce(nullIf(JSONExtractString(payload_json, 'entity_id'), ''), concat('ent:aircraft:', lower(coalesce(nullIf(JSONExtractString(payload_json, 'icao24'), ''), ifNull(native_id, source_record_key))))) AS entity_id,
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
FROM bronze.`src_catalog-auto-aviation-airports-drones-an_9465a4d4_v1`;

CREATE MATERIALIZED VIEW IF NOT EXISTS silver.mv_source_catalog_auto_aviation_airports_drones_and_mobility_airplanes_live_to_fact_track_point
TO silver.fact_track_point AS
SELECT
    concat('trkpt:', source_id, ':', source_record_key, ':', toString(source_record_index)) AS track_point_id,
    coalesce(nullIf(JSONExtractString(payload_json, 'track_id'), ''), concat('trk:', source_id, ':', nullIf(lower(ifNull(native_id, '')), ''), ':', source_record_key)) AS track_id,
    source_id,
    'aircraft' AS track_type,
    coalesce(nullIf(JSONExtractString(payload_json, 'entity_id'), ''), concat('ent:aircraft:', lower(coalesce(nullIf(JSONExtractString(payload_json, 'icao24'), ''), ifNull(native_id, source_record_key))))) AS entity_id,
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
FROM bronze.`src_catalog-auto-aviation-airports-drones-an_a0520040_v1`;

CREATE MATERIALIZED VIEW IF NOT EXISTS silver.mv_source_catalog_auto_security_addendum_air_adsblol_api_to_fact_track_point
TO silver.fact_track_point AS
SELECT
    concat('trkpt:', source_id, ':', source_record_key, ':', toString(source_record_index)) AS track_point_id,
    coalesce(nullIf(JSONExtractString(payload_json, 'track_id'), ''), concat('trk:', source_id, ':', nullIf(lower(ifNull(native_id, '')), ''), ':', source_record_key)) AS track_id,
    source_id,
    'aircraft' AS track_type,
    coalesce(nullIf(JSONExtractString(payload_json, 'entity_id'), ''), concat('ent:aircraft:', lower(coalesce(nullIf(JSONExtractString(payload_json, 'icao24'), ''), ifNull(native_id, source_record_key))))) AS entity_id,
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
FROM bronze.`src_catalog-auto-security-addendum-air-adsbl_7cb342d6_v1`;

CREATE MATERIALIZED VIEW IF NOT EXISTS silver.mv_source_catalog_auto_maritime_ocean_and_coastal_sources_aishub_to_fact_track_point
TO silver.fact_track_point AS
SELECT
    concat('trkpt:', source_id, ':', source_record_key, ':', toString(source_record_index)) AS track_point_id,
    coalesce(nullIf(JSONExtractString(payload_json, 'track_id'), ''), concat('trk:', source_id, ':', nullIf(ifNull(native_id, ''), ''), ':', source_record_key)) AS track_id,
    source_id,
    'vessel' AS track_type,
    coalesce(nullIf(JSONExtractString(payload_json, 'entity_id'), ''), concat('ent:vessel:', coalesce(nullIf(JSONExtractString(payload_json, 'mmsi'), ''), ifNull(native_id, source_record_key)))) AS entity_id,
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
FROM bronze.`src_catalog-auto-maritime-ocean-and-coastal-_7d499104_v1`;

CREATE MATERIALIZED VIEW IF NOT EXISTS silver.mv_source_catalog_auto_aviation_airports_drones_and_mobility_openaip_core_api_to_dim_entity
TO silver.dim_entity AS
SELECT
    coalesce(nullIf(JSONExtractString(payload_json, 'entity_id'), ''), concat('ent:openaip:', coalesce(nullIf(native_id, ''), source_record_key))) AS entity_id,
    coalesce(nullIf(JSONExtractString(payload_json, 'entity_type'), ''), nullIf(record_kind, ''), 'aviation_reference') AS entity_type,
    coalesce(nullIf(JSONExtractString(payload_json, 'name'), ''), nullIf(title, ''), coalesce(nullIf(native_id, ''), source_record_key)) AS canonical_name,
    coalesce(nullIf(status, ''), 'active') AS status,
    'low' AS risk_band,
    coalesce(nullIf(JSONExtractString(payload_json, 'place_id'), ''), ifNull(place_hint, '')) AS primary_place_id,
    coalesce(nullIf(native_id, ''), source_record_key) AS source_entity_key,
    source_id AS source_system,
    coalesce(published_at, occurred_at, fetched_at) AS valid_from,
    CAST(NULL, 'Nullable(DateTime64(3, \'UTC\'))') AS valid_to,
    schema_version,
    record_version,
    toUInt32(1) AS api_contract_version,
    parsed_at AS updated_at,
    concat('{"source_id":"', replaceAll(source_id, '"', ''), '"}') AS attrs,
    evidence
FROM bronze.`src_catalog-auto-aviation-airports-drones-an_06dda31d_v1`;
