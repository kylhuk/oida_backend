-- migrate:diff_summary Add VesselFinder route/waypoint ingestion state, bronze routing, and ops materialization.
-- migrate:compatibility_notes New tables and views only. No existing tables modified.
-- migrate:approval_ref vessel-finder-route-ingestion

CREATE TABLE IF NOT EXISTS ops.vesselfinder_route_queue
(
    source_id String,
    mmsi String,
    detail_id String DEFAULT '',
    status LowCardinality(String),
    discovered_at DateTime64(3, 'UTC'),
    next_fetch_at DateTime64(3, 'UTC'),
    last_fetched_at Nullable(DateTime64(3, 'UTC')),
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
ORDER BY (source_id, mmsi);

CREATE TABLE IF NOT EXISTS ops.vesselfinder_route_plan
(
    source_id String,
    mmsi String,
    entity_id String DEFAULT '',
    fetched_at DateTime64(3, 'UTC'),
    destination_locode String DEFAULT '',
    destination_name String DEFAULT '',
    reta Nullable(DateTime64(3, 'UTC')),
    waypoint_count UInt16 DEFAULT 0,
    raw_id String DEFAULT '',
    schema_version UInt16 DEFAULT 1,
    record_version UInt64 DEFAULT 1,
    updated_at DateTime64(3, 'UTC') DEFAULT now64(3),
    attrs String DEFAULT '{}',
    evidence String DEFAULT '[]'
)
ENGINE = ReplacingMergeTree(record_version)
ORDER BY (source_id, mmsi);

CREATE TABLE IF NOT EXISTS ops.vesselfinder_route_waypoint
(
    source_id String,
    mmsi String,
    entity_id String DEFAULT '',
    fetched_at DateTime64(3, 'UTC'),
    sequence UInt16,
    latitude Float64,
    longitude Float64,
    eta Nullable(DateTime64(3, 'UTC')),
    raw_id String DEFAULT '',
    schema_version UInt16 DEFAULT 1,
    attrs String DEFAULT '{}',
    evidence String DEFAULT '[]'
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(fetched_at)
ORDER BY (source_id, mmsi, fetched_at, sequence)
TTL fetched_at + INTERVAL 365 DAY;

CREATE TABLE IF NOT EXISTS `bronze`.`src_catalog-auto-maritime-ocean-and-coastal-_91b97aca_v1` AS `bronze`.`src_seed_gdelt_v1`;

CREATE MATERIALIZED VIEW IF NOT EXISTS ops.mv_vesselfinder_bronze_route_plan_to_route_plan
TO ops.vesselfinder_route_plan AS
SELECT
    source_id,
    JSONExtractString(payload_json, 'mmsi') AS mmsi,
    coalesce(nullIf(JSONExtractString(payload_json, 'entity_id'), ''), concat('ent:vessel:', JSONExtractString(payload_json, 'mmsi'))) AS entity_id,
    fetched_at,
    JSONExtractString(payload_json, 'destination_locode') AS destination_locode,
    JSONExtractString(payload_json, 'destination_name') AS destination_name,
    if(JSONExtractUInt(payload_json, 'reta_unix') > 0, toDateTime64(toDateTime(toUInt32(JSONExtractUInt(payload_json, 'reta_unix'))), 3, 'UTC'), CAST(NULL, 'Nullable(DateTime64(3, \'UTC\'))')) AS reta,
    toUInt16(JSONExtractInt(payload_json, 'waypoint_count')) AS waypoint_count,
    raw_id,
    schema_version,
    toUInt64(toUnixTimestamp64Nano(parsed_at)) AS record_version,
    parsed_at AS updated_at,
    attrs,
    evidence
FROM bronze.`src_catalog-auto-maritime-ocean-and-coastal-_91b97aca_v1`
WHERE record_kind = 'route_plan'
  AND parser_id = 'parser:vesselfinder-route-json'
  AND parser_version >= '1.0.0';

INSERT INTO ops.vesselfinder_route_plan
SELECT
    source_id,
    JSONExtractString(payload_json, 'mmsi') AS mmsi,
    coalesce(nullIf(JSONExtractString(payload_json, 'entity_id'), ''), concat('ent:vessel:', JSONExtractString(payload_json, 'mmsi'))) AS entity_id,
    fetched_at,
    JSONExtractString(payload_json, 'destination_locode') AS destination_locode,
    JSONExtractString(payload_json, 'destination_name') AS destination_name,
    if(JSONExtractUInt(payload_json, 'reta_unix') > 0, toDateTime64(toDateTime(toUInt32(JSONExtractUInt(payload_json, 'reta_unix'))), 3, 'UTC'), CAST(NULL, 'Nullable(DateTime64(3, \'UTC\'))')) AS reta,
    toUInt16(JSONExtractInt(payload_json, 'waypoint_count')) AS waypoint_count,
    raw_id,
    schema_version,
    toUInt64(toUnixTimestamp64Nano(parsed_at)) AS record_version,
    parsed_at AS updated_at,
    attrs,
    evidence
FROM bronze.`src_catalog-auto-maritime-ocean-and-coastal-_91b97aca_v1` FINAL
WHERE record_kind = 'route_plan'
  AND parser_id = 'parser:vesselfinder-route-json'
  AND parser_version = (
      SELECT max(parser_version)
      FROM bronze.`src_catalog-auto-maritime-ocean-and-coastal-_91b97aca_v1`
      WHERE record_kind = 'route_plan'
        AND parser_id = 'parser:vesselfinder-route-json'
  );

CREATE MATERIALIZED VIEW IF NOT EXISTS ops.mv_vesselfinder_bronze_route_waypoint_to_route_waypoint
TO ops.vesselfinder_route_waypoint AS
SELECT
    source_id,
    JSONExtractString(payload_json, 'mmsi') AS mmsi,
    coalesce(nullIf(JSONExtractString(payload_json, 'entity_id'), ''), concat('ent:vessel:', JSONExtractString(payload_json, 'mmsi'))) AS entity_id,
    fetched_at,
    toUInt16(JSONExtractInt(payload_json, 'sequence')) AS sequence,
    coalesce(lat, JSONExtractFloat(payload_json, 'latitude')) AS latitude,
    coalesce(lon, JSONExtractFloat(payload_json, 'longitude')) AS longitude,
    if(JSONExtractUInt(payload_json, 'eta_unix') > 0, toDateTime64(toDateTime(toUInt32(JSONExtractUInt(payload_json, 'eta_unix'))), 3, 'UTC'), CAST(NULL, 'Nullable(DateTime64(3, \'UTC\'))')) AS eta,
    raw_id,
    schema_version,
    attrs,
    evidence
FROM bronze.`src_catalog-auto-maritime-ocean-and-coastal-_91b97aca_v1`
WHERE record_kind = 'route_waypoint'
  AND parser_id = 'parser:vesselfinder-route-json'
  AND parser_version >= '1.0.0'
  AND abs(coalesce(lat, JSONExtractFloat(payload_json, 'latitude'))) <= 90
  AND abs(coalesce(lon, JSONExtractFloat(payload_json, 'longitude'))) <= 180;

INSERT INTO ops.vesselfinder_route_waypoint
SELECT
    source_id,
    JSONExtractString(payload_json, 'mmsi') AS mmsi,
    coalesce(nullIf(JSONExtractString(payload_json, 'entity_id'), ''), concat('ent:vessel:', JSONExtractString(payload_json, 'mmsi'))) AS entity_id,
    fetched_at,
    toUInt16(JSONExtractInt(payload_json, 'sequence')) AS sequence,
    coalesce(lat, JSONExtractFloat(payload_json, 'latitude')) AS latitude,
    coalesce(lon, JSONExtractFloat(payload_json, 'longitude')) AS longitude,
    if(JSONExtractUInt(payload_json, 'eta_unix') > 0, toDateTime64(toDateTime(toUInt32(JSONExtractUInt(payload_json, 'eta_unix'))), 3, 'UTC'), CAST(NULL, 'Nullable(DateTime64(3, \'UTC\'))')) AS eta,
    raw_id,
    schema_version,
    attrs,
    evidence
FROM bronze.`src_catalog-auto-maritime-ocean-and-coastal-_91b97aca_v1` FINAL
WHERE record_kind = 'route_waypoint'
  AND parser_id = 'parser:vesselfinder-route-json'
  AND parser_version = (
      SELECT max(parser_version)
      FROM bronze.`src_catalog-auto-maritime-ocean-and-coastal-_91b97aca_v1`
      WHERE record_kind = 'route_waypoint'
        AND parser_id = 'parser:vesselfinder-route-json'
  )
  AND abs(coalesce(lat, JSONExtractFloat(payload_json, 'latitude'))) <= 90
  AND abs(coalesce(lon, JSONExtractFloat(payload_json, 'longitude'))) <= 180;
