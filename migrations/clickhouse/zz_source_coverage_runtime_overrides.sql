CREATE OR REPLACE VIEW silver.v_entity_source_lineage AS
SELECT
    JSONExtractString(attrs, 'source_id') AS source_id,
    entity_id,
    updated_at AS landed_at
FROM silver.dim_entity
WHERE JSONExtractString(attrs, 'source_id') != '';

CREATE OR REPLACE VIEW silver.v_source_terminal_catalog AS
SELECT source_id, 'silver.fact_observation' AS terminal_destination, max(observed_at) AS landed_at, count() AS row_count
FROM silver.fact_observation
GROUP BY source_id
UNION ALL
SELECT source_id, 'silver.fact_event' AS terminal_destination, max(starts_at) AS landed_at, count() AS row_count
FROM silver.fact_event
GROUP BY source_id
UNION ALL
SELECT source_id, 'silver.fact_track_point' AS terminal_destination, max(observed_at) AS landed_at, count() AS row_count
FROM silver.fact_track_point
GROUP BY source_id
UNION ALL
SELECT source_id, 'silver.v_entity_source_lineage' AS terminal_destination, max(landed_at) AS landed_at, count() AS row_count
FROM silver.v_entity_source_lineage
GROUP BY source_id;

CREATE OR REPLACE VIEW meta.source_silver_coverage AS
WITH
in_scope AS (
    SELECT source_id, promote_profile, lifecycle_state, disabled_reason
    FROM meta.source_registry FINAL
    WHERE catalog_kind = 'concrete'
      AND transport_type = 'http'
      AND bronze_table IS NOT NULL
),
bronze AS (
    SELECT source_id, max(fetched_at) AS last_bronze_at
    FROM bronze.raw_document
    GROUP BY source_id
),
parse_success AS (
    SELECT source_id, max(finished_at) AS last_parse_at
    FROM ops.parse_log
    WHERE status = 'success'
    GROUP BY source_id
),
promote_stage AS (
    SELECT JSONExtractString(stats, 'source_id') AS source_id, max(started_at) AS last_promote_at
    FROM ops.job_run
    WHERE job_type = 'promote'
      AND status = 'success'
      AND JSONExtractString(stats, 'source_id') != ''
    GROUP BY source_id
),
promote_global AS (
    SELECT max(started_at) AS last_promote_at
    FROM ops.job_run
    WHERE job_type = 'promote'
      AND status = 'success'
),
observation_landing AS (
    SELECT source_id, max(observed_at) AS last_observed_at, count() AS rows_observed
    FROM silver.fact_observation
    GROUP BY source_id
),
event_landing AS (
    SELECT source_id, max(starts_at) AS last_event_at, count() AS rows_event
    FROM silver.fact_event
    GROUP BY source_id
),
track_landing AS (
    SELECT source_id, max(observed_at) AS last_track_at, count() AS rows_track
    FROM silver.fact_track_point
    GROUP BY source_id
),
entity_landing AS (
    SELECT source_id, max(landed_at) AS last_entity_at, count() AS rows_entity
    FROM silver.v_entity_source_lineage
    GROUP BY source_id
),
terminal_catalog AS (
    SELECT source_id, any(terminal_destination) AS terminal_destination, max(landed_at) AS landed_at, sum(row_count) AS row_count
    FROM silver.v_source_terminal_catalog
    GROUP BY source_id
)
SELECT
    s.source_id,
    multiIf(
        positionCaseInsensitiveUTF8(coalesce(s.disabled_reason, ''), 'missing credential') > 0 OR s.lifecycle_state = 'blocked_missing_credential', 'blocked_missing_credential',
        silver_rows > 0, 'silver_landed',
        coalesce(p.last_parse_at, b.last_bronze_at) IS NOT NULL, 'parsed_no_promotable_rows',
        'parsed_no_promotable_rows'
    ) AS coverage_state,
    routing_mode,
    coalesce(s.promote_profile, '') AS promote_profile,
    terminal_kind,
    s.terminal_destination AS terminal_destination,
    b.last_bronze_at,
    p.last_parse_at,
    coalesce(ps.last_promote_at, pg.last_promote_at) AS last_promote_at,
    last_silver_at,
    multiIf(
        positionCaseInsensitiveUTF8(coalesce(s.disabled_reason, ''), 'missing credential') > 0 OR s.lifecycle_state = 'blocked_missing_credential', coalesce(s.disabled_reason, 'missing credential'),
        silver_rows > 0, '',
        coalesce(p.last_parse_at, b.last_bronze_at) IS NOT NULL, 'parse ran but produced no terminal silver rows',
        'awaiting data'
    ) AS reason,
    '{}' AS attrs,
    now64(3) AS updated_at
FROM (
    SELECT
        source_id,
        promote_profile,
        lifecycle_state,
        disabled_reason,
        multiIf(
            source_id = 'catalog:auto:aviation-airports-drones-and-mobility-openaip-core-api', 'mv_source',
            source_id IN (
                'catalog:auto:aviation-airports-drones-and-mobility-opensky-network',
                'catalog:auto:aviation-airports-drones-and-mobility-airplanes-live',
                'catalog:auto:security-addendum-air-adsblol-api',
                'catalog:auto:maritime-ocean-and-coastal-sources-aishub'
            ), 'mv_source',
            promote_profile = 'promote:catalog', 'canonical',
            promote_profile IN ('promote:geopolitical', 'promote:safety_security', 'promote:aviation', 'promote:maritime', 'promote:space'), 'profile_specific',
            'unsupported_profile'
        ) AS routing_mode,
        multiIf(
            source_id = 'catalog:auto:aviation-airports-drones-and-mobility-openaip-core-api', 'table',
            source_id IN (
                'catalog:auto:aviation-airports-drones-and-mobility-opensky-network',
                'catalog:auto:aviation-airports-drones-and-mobility-airplanes-live',
                'catalog:auto:security-addendum-air-adsblol-api',
                'catalog:auto:maritime-ocean-and-coastal-sources-aishub'
            ), 'table',
            promote_profile = 'promote:catalog', 'view',
            promote_profile IN ('promote:geopolitical', 'promote:safety_security', 'promote:aviation', 'promote:maritime', 'promote:space'), 'table',
            'none'
        ) AS terminal_kind,
        multiIf(
            source_id = 'catalog:auto:aviation-airports-drones-and-mobility-openaip-core-api', 'silver.dim_entity',
            source_id IN (
                'catalog:auto:aviation-airports-drones-and-mobility-opensky-network',
                'catalog:auto:aviation-airports-drones-and-mobility-airplanes-live',
                'catalog:auto:security-addendum-air-adsblol-api',
                'catalog:auto:maritime-ocean-and-coastal-sources-aishub'
            ), 'silver.fact_track_point',
            promote_profile = 'promote:geopolitical', 'silver.fact_event',
            promote_profile = 'promote:safety_security', 'silver.fact_observation',
            promote_profile = 'promote:catalog', 'silver.v_source_terminal_catalog',
            promote_profile IN ('promote:aviation', 'promote:maritime', 'promote:space'), 'silver.fact_track_point',
            ''
        ) AS terminal_destination
    FROM in_scope
) AS s
LEFT JOIN bronze AS b ON b.source_id = s.source_id
LEFT JOIN parse_success AS p ON p.source_id = s.source_id
LEFT JOIN promote_stage AS ps ON ps.source_id = s.source_id
LEFT JOIN promote_global AS pg ON 1 = 1
LEFT JOIN observation_landing AS o ON o.source_id = s.source_id
LEFT JOIN event_landing AS e ON e.source_id = s.source_id
LEFT JOIN track_landing AS t ON t.source_id = s.source_id
LEFT JOIN entity_landing AS entity ON entity.source_id = s.source_id
LEFT JOIN terminal_catalog AS c ON c.source_id = s.source_id
ARRAY JOIN [
    multiIf(
        s.terminal_destination = 'silver.fact_event', coalesce(e.rows_event, 0),
        s.terminal_destination = 'silver.fact_observation', coalesce(o.rows_observed, 0),
        s.terminal_destination = 'silver.fact_track_point', coalesce(t.rows_track, 0),
        s.terminal_destination = 'silver.dim_entity', coalesce(entity.rows_entity, 0),
        s.terminal_destination = 'silver.v_source_terminal_catalog', coalesce(c.row_count, 0),
        0
    )
] AS silver_rows,
[
    multiIf(
        s.terminal_destination = 'silver.fact_event', e.last_event_at,
        s.terminal_destination = 'silver.fact_observation', o.last_observed_at,
        s.terminal_destination = 'silver.fact_track_point', t.last_track_at,
        s.terminal_destination = 'silver.dim_entity', entity.last_entity_at,
        s.terminal_destination = 'silver.v_source_terminal_catalog', c.landed_at,
        null
    )
] AS last_silver_at;

DROP VIEW IF EXISTS gold.api_v1_source_coverage;

CREATE VIEW IF NOT EXISTS gold.api_v1_source_coverage AS
WITH
event_counts AS (
    SELECT source_id, countDistinct(event_id) AS event_count
    FROM gold.api_v1_events
    GROUP BY source_id
),
place_anchors AS (
    SELECT source_id, place_id
    FROM gold.api_v1_events
    WHERE place_id != ''
    UNION ALL
    SELECT source_id, place_id
    FROM gold.api_v1_observations
    WHERE place_id != ''
    UNION ALL
    SELECT source_id, place_id
    FROM silver.fact_track_point
    WHERE place_id != ''
    UNION ALL
    SELECT JSONExtractString(attrs, 'source_id') AS source_id, primary_place_id AS place_id
    FROM gold.api_v1_entities
    WHERE primary_place_id != ''
      AND JSONExtractString(attrs, 'source_id') != ''
),
place_counts AS (
    SELECT source_id, countDistinct(place_id) AS place_count
    FROM place_anchors
    GROUP BY source_id
),
landing_counts AS (
    SELECT source_id, sum(row_count) AS landed_rows
    FROM silver.v_source_terminal_catalog
    GROUP BY source_id
)
SELECT
    concat(sr.source_id, ':coverage') AS coverage_id,
    sr.source_id AS source_id,
    'source' AS scope_type,
    sr.source_id AS scope_id,
    sr.geo_scope AS geo_scope,
    toUInt32(coalesce(pc.place_count, 0)) AS place_count,
    toUInt32(coalesce(ec.event_count, 0)) AS event_count,
    multiIf(coalesce(lc.landed_rows, 0) > 0, 'silver_landed', 'parsed_no_promotable_rows') AS coverage_state,
    multiIf(coalesce(lc.landed_rows, 0) > 0, '', 'parse ran but produced no terminal silver rows') AS reason,
    now64(3) AS updated_at
FROM (
    SELECT *
    FROM meta.source_registry FINAL
) AS sr
LEFT JOIN event_counts AS ec ON ec.source_id = sr.source_id
LEFT JOIN place_counts AS pc ON pc.source_id = sr.source_id
LEFT JOIN landing_counts AS lc ON lc.source_id = sr.source_id
WHERE sr.catalog_kind = 'concrete'
  AND sr.transport_type = 'http';
