CREATE TABLE IF NOT EXISTS ops.job_run
(
    job_id String,
    job_type LowCardinality(String),
    status LowCardinality(String),
    started_at DateTime64(3, 'UTC'),
    finished_at Nullable(DateTime64(3, 'UTC')),
    message String,
    stats String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(started_at)
ORDER BY (job_type, started_at, job_id);

CREATE TABLE IF NOT EXISTS ops.crawl_frontier
(
    frontier_id String,
    source_id String,
    domain String,
    url String,
    canonical_url String,
    url_hash String,
    priority Int32,
    state LowCardinality(String),
    discovered_at DateTime64(3, 'UTC'),
    next_fetch_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(discovered_at)
ORDER BY (domain, priority, next_fetch_at, url_hash);

CREATE TABLE IF NOT EXISTS ops.fetch_log
(
    fetch_id String,
    source_id String,
    url_hash String,
    status_code UInt16,
    success UInt8,
    fetched_at DateTime64(3, 'UTC'),
    latency_ms UInt32,
    body_bytes UInt64,
    error_message Nullable(String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(fetched_at)
ORDER BY (source_id, fetched_at, url_hash);

CREATE TABLE IF NOT EXISTS bronze.raw_document
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
ENGINE = MergeTree
PARTITION BY toYYYYMM(fetched_at)
ORDER BY (source_id, fetched_at, raw_id);
