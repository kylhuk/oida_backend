CREATE TABLE IF NOT EXISTS meta.pipeline_registry
(
    pipeline_id String,
    pipeline_name String,
    pipeline_kind LowCardinality(String),
    definition_json String CODEC(ZSTD(3)),
    definition_checksum String,
    enabled UInt8,
    schema_version UInt32,
    record_version UInt64,
    api_contract_version UInt32,
    updated_at DateTime64(3, 'UTC'),
    attrs String CODEC(ZSTD(3)),
    evidence String CODEC(ZSTD(3))
)
ENGINE = ReplacingMergeTree(record_version)
ORDER BY (pipeline_kind, pipeline_id);

CREATE TABLE IF NOT EXISTS ops.pipeline_run
(
    pipeline_id String,
    run_id String,
    run_key String,
    definition_checksum String,
    status LowCardinality(String),
    attempt_count UInt16,
    started_at Nullable(DateTime64(3, 'UTC')),
    finished_at Nullable(DateTime64(3, 'UTC')),
    message String CODEC(ZSTD(3)),
    error_message Nullable(String) CODEC(ZSTD(3)),
    outputs_json String CODEC(ZSTD(3)),
    schema_version UInt32,
    record_version UInt64,
    api_contract_version UInt32,
    updated_at DateTime64(3, 'UTC'),
    attrs String CODEC(ZSTD(3)),
    evidence String CODEC(ZSTD(3))
)
ENGINE = ReplacingMergeTree(record_version)
PARTITION BY toYYYYMM(updated_at)
ORDER BY (pipeline_id, run_key, run_id);
