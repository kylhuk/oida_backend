ALTER TABLE meta.source_registry
    RENAME COLUMN IF EXISTS version TO record_version;

ALTER TABLE meta.source_registry
    ADD COLUMN IF NOT EXISTS schema_version UInt32 DEFAULT 1 AFTER enabled;

ALTER TABLE meta.source_registry
    ADD COLUMN IF NOT EXISTS api_contract_version UInt32 DEFAULT 1 AFTER record_version;

ALTER TABLE meta.source_registry
    ADD COLUMN IF NOT EXISTS attrs String DEFAULT '{}' AFTER api_contract_version;

ALTER TABLE meta.source_registry
    ADD COLUMN IF NOT EXISTS evidence String DEFAULT '[]' AFTER attrs;

CREATE TABLE IF NOT EXISTS meta.parser_registry
(
    parser_id String,
    parser_family LowCardinality(String),
    route_scope LowCardinality(String),
    input_format LowCardinality(String),
    source_class LowCardinality(String),
    handler_ref String,
    attrs String,
    evidence String,
    schema_version UInt32,
    record_version UInt64,
    api_contract_version UInt32,
    enabled UInt8,
    updated_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(record_version)
ORDER BY (source_class, route_scope, parser_id);

CREATE TABLE IF NOT EXISTS meta.metric_registry
(
    metric_id String,
    metric_family LowCardinality(String),
    subject_grain LowCardinality(String),
    unit LowCardinality(String),
    value_type LowCardinality(String),
    rollup_engine LowCardinality(String),
    rollup_rule String,
    attrs String,
    evidence String,
    schema_version UInt32,
    record_version UInt64,
    api_contract_version UInt32,
    enabled UInt8,
    updated_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(record_version)
ORDER BY (metric_family, subject_grain, metric_id);

CREATE TABLE IF NOT EXISTS meta.api_schema_registry
(
    api_name String,
    route_pattern String,
    http_method LowCardinality(String),
    compatibility_status LowCardinality(String),
    request_schema_ref Nullable(String),
    response_schema_ref String,
    deprecation_starts_at Nullable(DateTime64(3, 'UTC')),
    deprecation_ends_at Nullable(DateTime64(3, 'UTC')),
    attrs String,
    evidence String,
    schema_version UInt32,
    record_version UInt64,
    api_contract_version UInt32,
    enabled UInt8,
    updated_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(record_version)
ORDER BY (api_contract_version, http_method, route_pattern);
