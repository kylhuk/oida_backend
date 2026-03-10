DROP VIEW IF EXISTS gold.api_v1_sources;

CREATE VIEW IF NOT EXISTS gold.api_v1_sources AS
SELECT
    source_id,
    domain,
    domain_family,
    source_class,
    entrypoints,
    auth_mode,
    auth_config_json,
    format_hint,
    robots_policy,
    refresh_strategy,
    requests_per_minute,
    burst_size,
    retention_class,
    license,
    terms_url,
    attribution_required,
    geo_scope,
    priority,
    parser_id,
    entity_types,
    expected_place_types,
    supports_historical,
    supports_delta,
    backfill_priority,
    confidence_baseline,
    enabled,
    disabled_reason,
    disabled_at,
    disabled_by,
    review_status,
    review_notes,
    schema_version,
    record_version,
    api_contract_version,
    updated_at,
    attrs,
    evidence
FROM meta.source_registry FINAL;

DROP VIEW IF EXISTS gold.api_v1_places;

CREATE VIEW IF NOT EXISTS gold.api_v1_places AS
SELECT
    place_id,
    parent_place_id,
    canonical_name,
    place_type,
    admin_level,
    country_code,
    continent_code,
    source_place_key,
    source_system,
    status,
    centroid_lat,
    centroid_lon,
    bbox_min_lat,
    bbox_min_lon,
    bbox_max_lat,
    bbox_max_lon,
    valid_from,
    valid_to,
    schema_version,
    record_version,
    api_contract_version,
    updated_at,
    attrs,
    evidence
FROM silver.dim_place FINAL;

DROP VIEW IF EXISTS gold.api_v1_events;

CREATE VIEW IF NOT EXISTS gold.api_v1_events AS
SELECT
    event_id,
    source_id,
    event_type,
    event_subtype,
    place_id,
    parent_place_chain,
    starts_at,
    ends_at,
    status,
    confidence_band,
    impact_score,
    schema_version,
    attrs,
    evidence
FROM silver.fact_event;

DROP VIEW IF EXISTS gold.api_v1_observations;

CREATE VIEW IF NOT EXISTS gold.api_v1_observations AS
SELECT
    observation_id,
    source_id,
    subject_type,
    subject_id,
    observation_type,
    place_id,
    parent_place_chain,
    observed_at,
    published_at,
    confidence_band,
    measurement_unit,
    measurement_value,
    schema_version,
    attrs,
    evidence
FROM silver.fact_observation;
