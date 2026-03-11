CREATE TABLE IF NOT EXISTS ops.parse_checkpoint
(
    checkpoint_id String,
    source_id String,
    raw_id String,
    parser_id String,
    parser_version String,
    content_hash String,
    bronze_table String,
    status LowCardinality(String),
    parsed_at DateTime64(3, 'UTC'),
    schema_version UInt32,
    record_version UInt64,
    api_contract_version UInt32,
    updated_at DateTime64(3, 'UTC'),
    attrs String CODEC(ZSTD(3)),
    evidence String CODEC(ZSTD(3))
)
ENGINE = ReplacingMergeTree(record_version)
PARTITION BY toYYYYMM(parsed_at)
ORDER BY (source_id, raw_id, parser_id, parser_version, content_hash, bronze_table, checkpoint_id)
TTL toDateTime(parsed_at) + INTERVAL 180 DAY DELETE;
