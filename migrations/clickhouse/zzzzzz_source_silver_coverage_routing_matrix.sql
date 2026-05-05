CREATE OR REPLACE VIEW meta.source_silver_coverage AS
WITH
in_scope AS (
    SELECT source_id, source_class, entity_types, promote_profile, lifecycle_state, disabled_reason
    FROM meta.source_registry FINAL
    WHERE catalog_kind = 'concrete'
      AND transport_type = 'http'
      AND bronze_table IS NOT NULL
),
frontier AS (
    SELECT source_id, max(discovered_at) AS last_frontier_at
    FROM ops.crawl_frontier
    GROUP BY source_id
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
),
unresolved_open AS (
    SELECT source_id, countIf(state NOT IN ('resolved', 'discarded')) AS unresolved_rows
    FROM ops.unresolved_location_queue
    GROUP BY source_id
),
routing_matrix AS (
    SELECT 'promote:geopolitical' AS promote_profile, 'event_records' AS source_shape, 'profile_specific' AS routing_mode, 'table' AS terminal_kind, 'silver.fact_event' AS terminal_destination
    UNION ALL
    SELECT 'promote:safety_security', 'observation_records', 'profile_specific', 'table', 'silver.fact_observation'
    UNION ALL
    SELECT 'promote:catalog', 'catalog_metadata', 'canonical', 'view', 'silver.v_source_terminal_catalog'
    UNION ALL
    SELECT 'promote:aviation', 'track_points', 'mv_source', 'table', 'silver.fact_track_point'
    UNION ALL
    SELECT 'promote:aviation', 'reference_entities', 'mv_source', 'table', 'silver.dim_entity'
    UNION ALL
    SELECT 'promote:maritime', 'track_points', 'mv_source', 'table', 'silver.fact_track_point'
),
source_shapes AS (
    SELECT
        source_id,
        coalesce(promote_profile, '') AS promote_profile,
        lifecycle_state,
        disabled_reason,
        multiIf(
            promote_profile = 'promote:geopolitical' AND source_class IN ('broad_web_corpus', 'humanitarian_feed', 'conflict_dataset'), 'event_records',
            promote_profile = 'promote:safety_security' AND source_class IN ('sanctions_graph', 'hazard_feed', 'vulnerability_catalog'), 'observation_records',
            promote_profile = 'promote:catalog' AND source_class = 'catalog_source', 'catalog_metadata',
            promote_profile = 'promote:aviation' AND has(entity_types, 'aircraft'), 'track_points',
            promote_profile = 'promote:aviation' AND hasAny(entity_types, ['airport', 'airspace', 'navaid', 'reporting_point']), 'reference_entities',
            promote_profile = 'promote:maritime' AND has(entity_types, 'vessel'), 'track_points',
            'unsupported'
        ) AS source_shape
    FROM in_scope
),
routed_sources AS (
    SELECT
        s.source_id,
        s.promote_profile,
        s.lifecycle_state,
        s.disabled_reason,
        s.source_shape,
        if(s.source_shape = 'unsupported', 'unsupported_profile', coalesce(m.routing_mode, 'unsupported_profile')) AS routing_mode,
        if(s.source_shape = 'unsupported', 'none', coalesce(m.terminal_kind, 'none')) AS terminal_kind,
        if(s.source_shape = 'unsupported', '', coalesce(m.terminal_destination, '')) AS terminal_destination
    FROM source_shapes AS s
    LEFT JOIN routing_matrix AS m
        ON m.promote_profile = s.promote_profile
       AND m.source_shape = s.source_shape
)
SELECT
    s.source_id AS source_id,
    multiIf(
        s.routing_mode = 'unsupported_profile', 'unsupported_profile',
        positionCaseInsensitiveUTF8(coalesce(s.disabled_reason, ''), 'missing credential') > 0 OR s.lifecycle_state = 'blocked_missing_credential', 'blocked_missing_credential',
        silver_rows > 0 AND s.terminal_kind = 'view', 'silver_view_only',
        silver_rows > 0, 'silver_landed',
        coalesce(u.unresolved_rows, 0) > 0 AND silver_rows = 0, 'unresolved_only',
        coalesce(p.last_parse_at, b.last_bronze_at) IS NOT NULL, 'parsed_no_promotable_rows',
        'parsed_no_promotable_rows'
    ) AS coverage_state,
    s.routing_mode AS routing_mode,
    s.promote_profile AS promote_profile,
    s.terminal_kind AS terminal_kind,
    s.terminal_destination AS terminal_destination,
    b.last_bronze_at,
    p.last_parse_at,
    coalesce(ps.last_promote_at, pg.last_promote_at) AS last_promote_at,
    last_silver_at,
    multiIf(
        s.routing_mode = 'unsupported_profile', concat('unsupported routing for promote_profile ', s.promote_profile, ' and source_shape ', s.source_shape),
        positionCaseInsensitiveUTF8(coalesce(s.disabled_reason, ''), 'missing credential') > 0 OR s.lifecycle_state = 'blocked_missing_credential', coalesce(s.disabled_reason, 'missing credential'),
        silver_rows > 0, '',
        coalesce(u.unresolved_rows, 0) > 0, 'only unresolved rows present',
        f.last_frontier_at IS NULL, 'awaiting frontier seed',
        coalesce(p.last_parse_at, b.last_bronze_at) IS NOT NULL, 'parse ran but produced no terminal silver rows',
        'awaiting data'
    ) AS reason,
    concat('{"source_shape":"', replaceAll(s.source_shape, '"', ''), '"}') AS attrs,
    now64(3) AS updated_at
FROM routed_sources AS s
LEFT JOIN frontier AS f ON f.source_id = s.source_id
LEFT JOIN bronze AS b ON b.source_id = s.source_id
LEFT JOIN parse_success AS p ON p.source_id = s.source_id
LEFT JOIN promote_stage AS ps ON ps.source_id = s.source_id
LEFT JOIN promote_global AS pg ON 1 = 1
LEFT JOIN observation_landing AS o ON o.source_id = s.source_id
LEFT JOIN event_landing AS e ON e.source_id = s.source_id
LEFT JOIN track_landing AS t ON t.source_id = s.source_id
LEFT JOIN entity_landing AS entity ON entity.source_id = s.source_id
LEFT JOIN terminal_catalog AS c ON c.source_id = s.source_id
LEFT JOIN unresolved_open AS u ON u.source_id = s.source_id
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
