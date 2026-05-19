-- migrate:diff_summary Materialize VesselFinder discovery country context into places and entity-place relations.
-- migrate:compatibility_notes Append-only materialized views; existing VesselFinder source bronze rows continue to work without discovery context.
-- migrate:approval_ref vessel-finder-geo-context

CREATE MATERIALIZED VIEW IF NOT EXISTS silver.mv_vesselfinder_country_dimension_to_dim_place
TO silver.dim_place AS
SELECT
    concat('plc:flag:', lowerUTF8(dimension_code)) AS place_id,
    'plc:world' AS parent_place_id,
    dimension_label AS canonical_name,
    'country' AS place_type,
    toUInt8(0) AS admin_level,
    upperUTF8(dimension_code) AS country_code,
    '' AS continent_code,
    concat('vesselfinder:flag:', lowerUTF8(dimension_code)) AS source_place_key,
    source_id AS source_system,
    'active' AS status,
    toFloat64(0) AS centroid_lat,
    toFloat64(0) AS centroid_lon,
    toFloat64(0) AS bbox_min_lat,
    toFloat64(0) AS bbox_min_lon,
    toFloat64(0) AS bbox_max_lat,
    toFloat64(0) AS bbox_max_lon,
    discovered_at AS valid_from,
    CAST(NULL, 'Nullable(DateTime64(3, \'UTC\'))') AS valid_to,
    toUInt32(schema_version) AS schema_version,
    record_version,
    toUInt32(1) AS api_contract_version,
    updated_at,
    concat('{"source_id":"', source_id, '","dimension_kind":"country","country_code":"', upperUTF8(dimension_code), '"}') AS attrs,
    '[]' AS evidence
FROM ops.vesselfinder_dimension
WHERE dimension_kind = 'country'
  AND dimension_code != ''
  AND dimension_label != '';

CREATE MATERIALIZED VIEW IF NOT EXISTS silver.mv_vesselfinder_entity_to_bridge_entity_place
TO silver.bridge_entity_place AS
SELECT
    concat('bridge:vesselfinder:', JSONExtractString(payload_json, 'entity_id'), ':', JSONExtractString(payload_json, 'place_id')) AS bridge_id,
    JSONExtractString(payload_json, 'entity_id') AS entity_id,
    JSONExtractString(payload_json, 'place_id') AS place_id,
    'flag_state' AS relation_type,
    coalesce(occurred_at, parsed_at, fetched_at) AS linked_at,
    schema_version,
    attrs,
    evidence
FROM bronze.`src_catalog-auto-maritime-ocean-and-coastal-_f8f33fd7_v1`
WHERE record_kind = 'entity'
  AND JSONExtractString(payload_json, 'entity_id') != ''
  AND JSONExtractString(payload_json, 'place_id') != '';

INSERT INTO silver.dim_place
SELECT
    concat('plc:flag:', lowerUTF8(dimension_code)) AS place_id,
    'plc:world' AS parent_place_id,
    dimension_label AS canonical_name,
    'country' AS place_type,
    toUInt8(0) AS admin_level,
    upperUTF8(dimension_code) AS country_code,
    '' AS continent_code,
    concat('vesselfinder:flag:', lowerUTF8(dimension_code)) AS source_place_key,
    source_id AS source_system,
    'active' AS status,
    toFloat64(0) AS centroid_lat,
    toFloat64(0) AS centroid_lon,
    toFloat64(0) AS bbox_min_lat,
    toFloat64(0) AS bbox_min_lon,
    toFloat64(0) AS bbox_max_lat,
    toFloat64(0) AS bbox_max_lon,
    discovered_at AS valid_from,
    CAST(NULL, 'Nullable(DateTime64(3, \'UTC\'))') AS valid_to,
    toUInt32(schema_version) AS schema_version,
    record_version,
    toUInt32(1) AS api_contract_version,
    updated_at,
    concat('{"source_id":"', source_id, '","dimension_kind":"country","country_code":"', upperUTF8(dimension_code), '"}') AS attrs,
    '[]' AS evidence
FROM ops.vesselfinder_dimension FINAL
WHERE dimension_kind = 'country'
  AND dimension_code != ''
  AND dimension_label != '';

INSERT INTO silver.bridge_entity_place
SELECT
    concat('bridge:vesselfinder:', JSONExtractString(payload_json, 'entity_id'), ':', JSONExtractString(payload_json, 'place_id')) AS bridge_id,
    JSONExtractString(payload_json, 'entity_id') AS entity_id,
    JSONExtractString(payload_json, 'place_id') AS place_id,
    'flag_state' AS relation_type,
    coalesce(occurred_at, parsed_at, fetched_at) AS linked_at,
    schema_version,
    attrs,
    evidence
FROM bronze.`src_catalog-auto-maritime-ocean-and-coastal-_f8f33fd7_v1`
WHERE record_kind = 'entity'
  AND JSONExtractString(payload_json, 'entity_id') != ''
  AND JSONExtractString(payload_json, 'place_id') != '';
