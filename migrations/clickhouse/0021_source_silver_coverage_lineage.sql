CREATE VIEW IF NOT EXISTS silver.v_entity_source_lineage AS
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
unresolved_open AS (
	SELECT source_id, countIf(state NOT IN ('resolved', 'discarded')) AS unresolved_rows
	FROM ops.unresolved_location_queue
	GROUP BY source_id
)
SELECT
	s.source_id AS source_id,
	multiIf(
		routing_mode = 'unsupported_profile', 'unsupported_profile',
		positionCaseInsensitiveUTF8(coalesce(s.disabled_reason, ''), 'missing credential') > 0 OR s.lifecycle_state = 'blocked_missing_credential', 'blocked_missing_credential',
		silver_rows > 0 AND terminal_kind = 'view', 'silver_view_only',
		silver_rows > 0, 'silver_landed',
		coalesce(u.unresolved_rows, 0) > 0 AND silver_rows = 0, 'unresolved_only',
		coalesce(p.last_parse_at, b.last_bronze_at) IS NOT NULL, 'parsed_no_promotable_rows',
		'parsed_no_promotable_rows'
	) AS coverage_state,
	routing_mode,
	coalesce(s.promote_profile, '') AS promote_profile,
	terminal_kind,
	terminal_destination,
	b.last_bronze_at,
	p.last_parse_at,
	coalesce(ps.last_promote_at, pg.last_promote_at) AS last_promote_at,
	last_silver_at,
	multiIf(
		routing_mode = 'unsupported_profile', concat('unsupported promote_profile ', coalesce(s.promote_profile, '')),
		positionCaseInsensitiveUTF8(coalesce(s.disabled_reason, ''), 'missing credential') > 0 OR s.lifecycle_state = 'blocked_missing_credential', coalesce(s.disabled_reason, 'missing credential'),
		silver_rows > 0, '',
		coalesce(u.unresolved_rows, 0) > 0, 'only unresolved rows present',
		f.last_frontier_at IS NULL, 'awaiting frontier seed',
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
			promote_profile IN ('promote:geopolitical', 'promote:safety_security', 'promote:catalog'), 'canonical',
			promote_profile IN ('promote:aviation', 'promote:maritime', 'promote:space'), 'profile_specific',
			'unsupported_profile'
		) AS routing_mode,
		multiIf(
			promote_profile = 'promote:catalog', 'view',
			promote_profile IN ('promote:geopolitical', 'promote:safety_security', 'promote:aviation', 'promote:maritime', 'promote:space'), 'table',
			'none'
		) AS terminal_kind,
		multiIf(
			promote_profile = 'promote:geopolitical', 'silver.fact_event',
			promote_profile = 'promote:safety_security', 'silver.fact_observation',
			promote_profile = 'promote:catalog', 'silver.v_source_terminal_catalog',
			promote_profile IN ('promote:aviation', 'promote:maritime', 'promote:space'), 'silver.fact_track_point',
			''
		) AS terminal_destination
	FROM in_scope
) AS s
LEFT JOIN frontier AS f ON f.source_id = s.source_id
LEFT JOIN bronze AS b ON b.source_id = s.source_id
LEFT JOIN parse_success AS p ON p.source_id = s.source_id
LEFT JOIN promote_stage AS ps ON ps.source_id = s.source_id
LEFT JOIN promote_global AS pg ON 1 = 1
LEFT JOIN observation_landing AS o ON o.source_id = s.source_id
LEFT JOIN event_landing AS e ON e.source_id = s.source_id
LEFT JOIN track_landing AS t ON t.source_id = s.source_id
LEFT JOIN entity_landing AS entity ON entity.source_id = s.source_id
LEFT JOIN unresolved_open AS u ON u.source_id = s.source_id
LEFT JOIN (
	SELECT
		source_id,
		coalesce(max(landed_at), toDateTime64(0, 3, 'UTC')) AS last_catalog_at,
		sum(row_count) AS catalog_rows
	FROM silver.v_source_terminal_catalog
	GROUP BY source_id
) AS c ON c.source_id = s.source_id
ARRAY JOIN [
	multiIf(
		s.promote_profile = 'promote:geopolitical', coalesce(e.rows_event, 0),
		s.promote_profile = 'promote:safety_security', coalesce(o.rows_observed, 0),
		s.promote_profile = 'promote:catalog', coalesce(c.catalog_rows, 0),
		s.promote_profile IN ('promote:aviation', 'promote:maritime', 'promote:space'), coalesce(t.rows_track, 0),
		0
	)
] AS silver_rows,
[
	multiIf(
		s.promote_profile = 'promote:geopolitical', e.last_event_at,
		s.promote_profile = 'promote:safety_security', o.last_observed_at,
		s.promote_profile = 'promote:catalog', greatest(c.last_catalog_at, coalesce(entity.last_entity_at, toDateTime64(0, 3, 'UTC'))),
		s.promote_profile IN ('promote:aviation', 'promote:maritime', 'promote:space'), t.last_track_at,
		null
	)
] AS last_silver_at;
