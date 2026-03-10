CREATE TABLE IF NOT EXISTS silver.fact_event_local ON CLUSTER 'osint_cluster_2s2r'
(
    event_id String,
    source_id String,
    event_type LowCardinality(String),
    event_subtype LowCardinality(String),
    place_id String,
    parent_place_chain Array(String),
    starts_at DateTime64(3, 'UTC'),
    ends_at Nullable(DateTime64(3, 'UTC')),
    status LowCardinality(String),
    confidence_band LowCardinality(String),
    impact_score Float32,
    schema_version UInt32,
    attrs String CODEC(ZSTD(3)),
    evidence String CODEC(ZSTD(3))
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/osint_cluster_2s2r/{database}/{table}/{shard}', '{replica}')
PARTITION BY toYYYYMM(starts_at)
ORDER BY (place_id, event_type, starts_at, event_id)
TTL toDateTime(starts_at) + INTERVAL 1095 DAY DELETE;
