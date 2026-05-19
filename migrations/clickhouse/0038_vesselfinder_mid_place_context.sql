-- migrate:diff_summary Materialize VesselFinder parser-derived flag/MID places from source bronze rows.
-- migrate:compatibility_notes Append-only materialized view and backfill for parser-derived place IDs; complements discovery-dimension context.
-- migrate:approval_ref vessel-finder-mid-place-context

CREATE MATERIALIZED VIEW IF NOT EXISTS silver.mv_vesselfinder_entity_payload_to_dim_place
TO silver.dim_place AS
SELECT
    JSONExtractString(payload_json, 'place_id') AS place_id,
    'plc:world' AS parent_place_id,
    coalesce(nullIf(JSONExtractString(payload_json, 'flag_state'), ''), nullIf(JSONExtractString(payload_json, 'flag_state_code'), ''), JSONExtractString(payload_json, 'place_id')) AS canonical_name,
    if(startsWith(JSONExtractString(payload_json, 'place_id'), 'plc:flag:'), 'country', 'maritime_mid_area') AS place_type,
    toUInt8(0) AS admin_level,
    JSONExtractString(payload_json, 'flag_state_code') AS country_code,
    '' AS continent_code,
    concat('vesselfinder:', JSONExtractString(payload_json, 'place_id')) AS source_place_key,
    source_id AS source_system,
    'active' AS status,
    toFloat64(0) AS centroid_lat,
    toFloat64(0) AS centroid_lon,
    toFloat64(0) AS bbox_min_lat,
    toFloat64(0) AS bbox_min_lon,
    toFloat64(0) AS bbox_max_lat,
    toFloat64(0) AS bbox_max_lon,
    coalesce(occurred_at, parsed_at, fetched_at) AS valid_from,
    CAST(NULL, 'Nullable(DateTime64(3, \'UTC\'))') AS valid_to,
    schema_version,
    record_version,
    toUInt32(1) AS api_contract_version,
    parsed_at AS updated_at,
    attrs,
    evidence
FROM bronze.`src_catalog-auto-maritime-ocean-and-coastal-_f8f33fd7_v1`
WHERE record_kind = 'entity'
  AND JSONExtractString(payload_json, 'place_id') != '';

INSERT INTO silver.dim_place
SELECT
    JSONExtractString(payload_json, 'place_id') AS place_id,
    'plc:world' AS parent_place_id,
    coalesce(nullIf(JSONExtractString(payload_json, 'flag_state'), ''), nullIf(JSONExtractString(payload_json, 'flag_state_code'), ''), JSONExtractString(payload_json, 'place_id')) AS canonical_name,
    if(startsWith(JSONExtractString(payload_json, 'place_id'), 'plc:flag:'), 'country', 'maritime_mid_area') AS place_type,
    toUInt8(0) AS admin_level,
    JSONExtractString(payload_json, 'flag_state_code') AS country_code,
    '' AS continent_code,
    concat('vesselfinder:', JSONExtractString(payload_json, 'place_id')) AS source_place_key,
    source_id AS source_system,
    'active' AS status,
    toFloat64(0) AS centroid_lat,
    toFloat64(0) AS centroid_lon,
    toFloat64(0) AS bbox_min_lat,
    toFloat64(0) AS bbox_min_lon,
    toFloat64(0) AS bbox_max_lat,
    toFloat64(0) AS bbox_max_lon,
    coalesce(occurred_at, parsed_at, fetched_at) AS valid_from,
    CAST(NULL, 'Nullable(DateTime64(3, \'UTC\'))') AS valid_to,
    schema_version,
    record_version,
    toUInt32(1) AS api_contract_version,
    parsed_at AS updated_at,
    attrs,
    evidence
FROM bronze.`src_catalog-auto-maritime-ocean-and-coastal-_f8f33fd7_v1`
WHERE record_kind = 'entity'
  AND JSONExtractString(payload_json, 'place_id') != '';
