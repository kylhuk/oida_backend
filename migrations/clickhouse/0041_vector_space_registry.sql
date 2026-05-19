-- migrate:diff_summary Add vector space registry for named embedding spaces.
-- migrate:compatibility_notes New table and view only; no existing data is rewritten.
-- migrate:approval_ref frontend-spec-alignment/p3c

CREATE TABLE IF NOT EXISTS meta.vector_space
(
    name                 String,
    version              String,
    dimensions           UInt32               DEFAULT 384,
    entity_types         Array(String)        DEFAULT [],
    metric               LowCardinality(String) DEFAULT 'cosine',
    storage_table        String               DEFAULT 'silver.entity_embedding',
    model_ref            String               DEFAULT '',
    enabled              UInt8                DEFAULT 1,
    schema_version       UInt16               DEFAULT 0,
    record_version       UInt64               DEFAULT 0,
    api_contract_version UInt16               DEFAULT 0,
    updated_at           DateTime64(3, 'UTC') DEFAULT now64(3),
    attrs                String               DEFAULT '{}',
    evidence             String               DEFAULT '[]'
) ENGINE = ReplacingMergeTree(record_version)
ORDER BY (name, version);

DROP VIEW IF EXISTS gold.api_v1_vector_spaces;

CREATE VIEW IF NOT EXISTS gold.api_v1_vector_spaces AS
SELECT
    name,
    version,
    dimensions,
    entity_types,
    metric,
    storage_table,
    model_ref,
    enabled,
    schema_version,
    record_version,
    api_contract_version,
    updated_at,
    attrs,
    evidence
FROM meta.vector_space
FINAL
WHERE enabled = 1;
