-- migrate:diff_summary Add hashed API client registry for scoped API-key authentication.
-- migrate:compatibility_notes New table only; no existing API data is rewritten.
-- migrate:approval_ref production-readiness/auth

CREATE TABLE IF NOT EXISTS meta.api_clients
(
    key_id String,
    name String,
    key_sha256 FixedString(64),
    scopes Array(String),
    enabled UInt8 DEFAULT 1,
    expires_at Nullable(DateTime64(3, 'UTC')),
    disabled_reason String DEFAULT '',
    schema_version UInt16 DEFAULT 1,
    record_version UInt64 DEFAULT 1,
    api_contract_version UInt16 DEFAULT 1,
    updated_at DateTime64(3, 'UTC') DEFAULT now64(3),
    attrs String DEFAULT '{}',
    evidence String DEFAULT '[]'
)
ENGINE = ReplacingMergeTree(record_version)
ORDER BY (key_id);
