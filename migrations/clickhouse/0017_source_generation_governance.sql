CREATE TABLE IF NOT EXISTS meta.source_catalog
(
    catalog_id String,
    catalog_kind LowCardinality(String),
    name String,
    category String,
    scope String,
    produces String,
    tags Array(String),
    access_notes String,
    official_docs_url String,
    integration_archetype LowCardinality(String),
    generator_kind LowCardinality(String),
    runtime_source_id Nullable(String),
    generator_relationships Array(String),
    source_markdown_line UInt32,
    source_markdown_path String,
    source_markdown_checksum String,
    review_status LowCardinality(String),
    materialized_source_id Nullable(String),
    schema_version UInt32,
    record_version UInt64,
    api_contract_version UInt32,
    updated_at DateTime64(3, 'UTC'),
    attrs String,
    evidence String
)
ENGINE = ReplacingMergeTree(record_version)
ORDER BY (catalog_id);

CREATE TABLE IF NOT EXISTS meta.source_family_template
(
    template_id String,
    catalog_id String,
    family_name String,
    outputs String,
    tags Array(String),
    review_status LowCardinality(String),
    schema_version UInt32,
    record_version UInt64,
    api_contract_version UInt32,
    updated_at DateTime64(3, 'UTC'),
    attrs String,
    evidence String
)
ENGINE = ReplacingMergeTree(record_version)
ORDER BY (template_id);

CREATE TABLE IF NOT EXISTS meta.discovery_probe
(
    probe_id String,
    catalog_id String,
    probe_name String,
    integration_archetype LowCardinality(String),
    probe_patterns Array(String),
    review_status LowCardinality(String),
    schema_version UInt32,
    record_version UInt64,
    api_contract_version UInt32,
    updated_at DateTime64(3, 'UTC'),
    attrs String,
    evidence String
)
ENGINE = ReplacingMergeTree(record_version)
ORDER BY (probe_id);

CREATE TABLE IF NOT EXISTS meta.discovery_candidate
(
    candidate_id String,
    catalog_id String,
    candidate_name String,
    candidate_url String,
    integration_archetype LowCardinality(String),
    detected_platform LowCardinality(String),
    review_status LowCardinality(String),
    materialized_source_id Nullable(String),
    schema_version UInt32,
    record_version UInt64,
    api_contract_version UInt32,
    updated_at DateTime64(3, 'UTC'),
    attrs String,
    evidence String
)
ENGINE = ReplacingMergeTree(record_version)
ORDER BY (candidate_id);

CREATE TABLE IF NOT EXISTS meta.source_generation_log
(
    generation_id String,
    catalog_id String,
    generator_kind LowCardinality(String),
    emitted_candidate_id Nullable(String),
    emitted_source_id Nullable(String),
    review_status LowCardinality(String),
    schema_version UInt32,
    record_version UInt64,
    api_contract_version UInt32,
    updated_at DateTime64(3, 'UTC'),
    attrs String,
    evidence String
)
ENGINE = ReplacingMergeTree(record_version)
ORDER BY (generation_id);
