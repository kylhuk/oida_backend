CREATE TABLE IF NOT EXISTS ops.promote_checkpoint
(
    checkpoint_id String,
    source_id String,
    bronze_table String,
    selection_mode LowCardinality(String),
    window_start DateTime64(3, 'UTC'),
    window_end DateTime64(3, 'UTC'),
    status LowCardinality(String),
    attempt_count UInt16,
    started_at DateTime64(3, 'UTC'),
    finished_at Nullable(DateTime64(3, 'UTC')),
    input_rows UInt32,
    error_message Nullable(String) CODEC(ZSTD(3)),
    schema_version UInt32,
    record_version UInt64,
    api_contract_version UInt32,
    updated_at DateTime64(3, 'UTC'),
    attrs String CODEC(ZSTD(3)),
    evidence String CODEC(ZSTD(3))
)
ENGINE = ReplacingMergeTree(record_version)
PARTITION BY toYYYYMM(updated_at)
ORDER BY (source_id, bronze_table, selection_mode, window_start, window_end, checkpoint_id);
