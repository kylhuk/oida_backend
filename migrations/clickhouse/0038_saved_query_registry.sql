-- migrate:diff_summary Add saved query registry for named, versioned CriteriaTree queries.
-- migrate:compatibility_notes New table and view only; no existing data is rewritten.
-- migrate:approval_ref frontend-spec-alignment/p3d

CREATE TABLE IF NOT EXISTS meta.saved_query
(
    name                 String,
    version              String,
    criteria             String,
    result_limit         Nullable(UInt32),
    ordering             String               DEFAULT '[]',
    created_at           DateTime64(3, 'UTC') DEFAULT now64(3),
    created_by           String               DEFAULT '',
    description          String               DEFAULT '',
    enabled              UInt8                DEFAULT 1,
    schema_version       UInt16               DEFAULT 0,
    record_version       UInt64               DEFAULT 0,
    api_contract_version UInt16               DEFAULT 0,
    updated_at           DateTime64(3, 'UTC') DEFAULT now64(3),
    attrs                String               DEFAULT '{}',
    evidence             String               DEFAULT '[]'
) ENGINE = ReplacingMergeTree(record_version)
ORDER BY (name, version);

DROP VIEW IF EXISTS gold.api_v1_saved_queries;

CREATE VIEW IF NOT EXISTS gold.api_v1_saved_queries AS
SELECT
    name,
    version,
    criteria,
    result_limit,
    ordering,
    created_at,
    created_by,
    description,
    enabled,
    schema_version,
    record_version,
    api_contract_version,
    updated_at,
    attrs,
    evidence
FROM meta.saved_query
FINAL
WHERE enabled = 1;
