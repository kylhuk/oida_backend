-- migrate:diff_summary Add query dialect registry for OIDA-QL validation and raw-query dispatch.
-- migrate:compatibility_notes New table and view only; no existing data is rewritten.
-- migrate:approval_ref frontend-spec-alignment/p3a

CREATE TABLE IF NOT EXISTS meta.query_dialect
(
    dialect                  String,
    entity_projection_rule   String               DEFAULT 'strict',
    shape_policy             String               DEFAULT 'both',
    case_sensitivity         String               DEFAULT 'sensitive',
    max_timeout_ms           UInt32               DEFAULT 30000,
    comment_prefix           String               DEFAULT '--',
    enabled                  UInt8                DEFAULT 1,
    schema_version           UInt16               DEFAULT 0,
    record_version           UInt64               DEFAULT 0,
    api_contract_version     UInt16               DEFAULT 0,
    updated_at               DateTime64(3, 'UTC') DEFAULT now64(3),
    attrs                    String               DEFAULT '{}',
    evidence                 String               DEFAULT '[]'
) ENGINE = ReplacingMergeTree(record_version)
ORDER BY (dialect);

DROP VIEW IF EXISTS gold.api_v1_query_dialects;

CREATE VIEW IF NOT EXISTS gold.api_v1_query_dialects AS
SELECT
    dialect,
    entity_projection_rule,
    shape_policy,
    case_sensitivity,
    max_timeout_ms,
    comment_prefix,
    enabled,
    schema_version,
    record_version,
    api_contract_version,
    updated_at,
    attrs,
    evidence
FROM meta.query_dialect
FINAL
WHERE enabled = 1;
