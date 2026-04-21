DROP VIEW IF EXISTS gold.api_v1_source_coverage;

CREATE VIEW IF NOT EXISTS gold.api_v1_source_coverage AS
WITH
event_counts AS (
    SELECT
        source_id,
        countDistinct(event_id) AS event_count
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
    SELECT
        JSONExtractString(attrs, 'source_id') AS source_id,
        primary_place_id AS place_id
    FROM gold.api_v1_entities
    WHERE primary_place_id != ''
      AND JSONExtractString(attrs, 'source_id') != ''
),
place_counts AS (
    SELECT
        source_id,
        countDistinct(place_id) AS place_count
    FROM place_anchors
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
    coalesce(sc.coverage_state, 'parsed_no_promotable_rows') AS coverage_state,
    coalesce(sc.reason, '') AS reason,
    now64(3) AS updated_at
FROM (
    SELECT *
    FROM meta.source_registry FINAL
) AS sr
LEFT JOIN event_counts AS ec ON ec.source_id = sr.source_id
LEFT JOIN place_counts AS pc ON pc.source_id = sr.source_id
LEFT JOIN meta.source_silver_coverage AS sc ON sc.source_id = sr.source_id
WHERE sr.catalog_kind = 'concrete'
  AND sr.transport_type = 'http';
