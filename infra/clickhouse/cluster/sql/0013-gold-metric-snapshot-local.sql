CREATE TABLE IF NOT EXISTS gold.metric_snapshot_local ON CLUSTER 'osint_cluster_2s2r'
(
    snapshot_id String,
    metric_id String,
    subject_grain LowCardinality(String),
    subject_id String,
    place_id String,
    window_grain LowCardinality(String),
    window_start DateTime64(3, 'UTC'),
    window_end DateTime64(3, 'UTC'),
    materialization_key String,
    snapshot_at DateTime64(3, 'UTC'),
    metric_value Float64,
    metric_delta Float64,
    rank UInt32,
    schema_version UInt32,
    attrs String CODEC(ZSTD(3)),
    evidence String CODEC(ZSTD(3))
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/osint_cluster_2s2r/{database}/{table}/{shard}', '{replica}')
PARTITION BY toYYYYMM(snapshot_at)
ORDER BY (metric_id, subject_grain, subject_id, window_grain, window_start, materialization_key, snapshot_at, snapshot_id)
TTL toDateTime(snapshot_at) + INTERVAL 365 DAY DELETE;
