-- migrate:diff_summary Make VesselFinder entity promotion use parsed_at as the silver replacement version and backfill latest parser rows.
-- migrate:compatibility_notes Replaces only the VesselFinder materialized view; inserts newer replacement rows so place-enriched entities win in silver/gold.
-- migrate:approval_ref vessel-finder-entity-place-repair

DROP VIEW IF EXISTS silver.mv_source_catalog_auto_maritime_ocean_and_coastal_sources_vesselfinder_to_dim_entity;

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
    toUInt64(toUnixTimestamp64Nano(parsed_at)) AS record_version,
    toUInt32(1) AS api_contract_version,
    parsed_at AS updated_at,
    attrs,
    evidence
FROM bronze.`src_catalog-auto-maritime-ocean-and-coastal-_f8f33fd7_v1`
WHERE record_kind = 'entity';

INSERT INTO silver.dim_entity
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
    toUInt64(toUnixTimestamp64Nano(parsed_at)) AS record_version,
    toUInt32(1) AS api_contract_version,
    parsed_at AS updated_at,
    attrs,
    evidence
FROM bronze.`src_catalog-auto-maritime-ocean-and-coastal-_f8f33fd7_v1`
WHERE record_kind = 'entity'
  AND parser_version = (
      SELECT max(parser_version)
      FROM bronze.`src_catalog-auto-maritime-ocean-and-coastal-_f8f33fd7_v1`
      WHERE source_id = 'catalog:auto:maritime-ocean-and-coastal-sources-vesselfinder'
        AND record_kind = 'entity'
  );
