CREATE TABLE IF NOT EXISTS bronze.raw_document_local ON CLUSTER 'osint_cluster_2s2r'
(
    raw_id String,
    source_id String,
    url String,
    fetched_at DateTime64(3, 'UTC'),
    status_code UInt16,
    content_type String,
    content_hash String,
    body_bytes UInt64,
    object_key Nullable(String),
    fetch_metadata String
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/osint_cluster_2s2r/{database}/{table}/{shard}', '{replica}')
PARTITION BY toYYYYMM(fetched_at)
ORDER BY (source_id, fetched_at, raw_id)
TTL toDateTime(fetched_at) + INTERVAL 180 DAY DELETE;
