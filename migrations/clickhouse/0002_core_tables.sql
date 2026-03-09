CREATE TABLE IF NOT EXISTS meta.source_registry
(
    source_id String,
    domain String,
    domain_family LowCardinality(String),
    source_class LowCardinality(String),
    entrypoints Array(String),
    auth_mode LowCardinality(String),
    format_hint LowCardinality(String),
    robots_policy LowCardinality(String),
    refresh_strategy LowCardinality(String),
    license String,
    terms_url String,
    geo_scope LowCardinality(String),
    priority UInt16,
    parser_id String,
    entity_types Array(String),
    expected_place_types Array(String),
    supports_historical UInt8,
    supports_delta UInt8,
    confidence_baseline Float32,
    enabled UInt8,
    version UInt64,
    updated_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (source_id);

CREATE VIEW IF NOT EXISTS gold.api_v1_sources AS
SELECT source_id, domain, source_class, domain_family, enabled, updated_at
FROM meta.source_registry;
