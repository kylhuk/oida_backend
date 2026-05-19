-- migrate:diff_summary Add data snapshot registry for temporal query anchoring.
-- migrate:compatibility_notes New table only; no existing data is rewritten.
-- migrate:approval_ref frontend-spec-alignment/p2d

CREATE TABLE IF NOT EXISTS meta.data_snapshot
(
    snapshot_id          String,
    captured_at          DateTime64(3, 'UTC'),
    tables               Array(String),
    description          String,
    schema_version       UInt16               DEFAULT 0,
    record_version       UInt64               DEFAULT 0,
    api_contract_version UInt16               DEFAULT 0,
    updated_at           DateTime64(3, 'UTC') DEFAULT now64(3),
    attrs                String               DEFAULT '{}',
    evidence             String               DEFAULT '[]'
) ENGINE = ReplacingMergeTree(record_version)
ORDER BY (snapshot_id);
