-- migrate:scope metadata
-- migrate:target_kind table
-- migrate:target_name meta.schema_change_registry
-- migrate:diff additive
-- migrate:compatibility backward_compatible
-- migrate:approval auto_approved
-- migrate:approval_notes Review captured in-repo for bootstrap-owned rollout
-- migrate:approved_by bootstrap
-- migrate:summary Establish a queryable metadata schema change registry for diff and approval status.

CREATE TABLE IF NOT EXISTS meta.schema_change_registry
(
    migration_version String,
    migration_checksum String,
    schema_scope LowCardinality(String),
    target_kind LowCardinality(String),
    target_name String,
    diff_status LowCardinality(String),
    compatibility_status LowCardinality(String),
    approval_status LowCardinality(String),
    approval_notes String,
    approved_by Nullable(String),
    approved_at Nullable(DateTime64(3, 'UTC')),
    summary String,
    schema_version UInt32,
    record_version UInt64,
    api_contract_version UInt32,
    updated_at DateTime64(3, 'UTC'),
    attrs String,
    evidence String
)
ENGINE = ReplacingMergeTree(record_version)
ORDER BY (schema_scope, target_kind, target_name, migration_version);
