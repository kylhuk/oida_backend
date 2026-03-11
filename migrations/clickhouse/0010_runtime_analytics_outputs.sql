CREATE TABLE IF NOT EXISTS gold.cross_domain_snapshot
(
    cross_domain_id String,
    subject_grain LowCardinality(String),
    subject_id String,
    place_id String,
    domains Array(String),
    composite_score Float64,
    snapshot_at DateTime64(3, 'UTC'),
    metric_ids Array(String),
    attrs String CODEC(ZSTD(3)),
    evidence String CODEC(ZSTD(3))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(snapshot_at)
ORDER BY (subject_grain, subject_id, place_id, snapshot_at, cross_domain_id)
TTL toDateTime(snapshot_at) + INTERVAL 365 DAY DELETE;

DROP VIEW IF EXISTS gold.api_v1_cross_domain;

CREATE VIEW IF NOT EXISTS gold.api_v1_cross_domain AS
SELECT
    cross_domain_id,
    subject_grain,
    subject_id,
    place_id,
    domains,
    composite_score,
    snapshot_at,
    metric_ids,
    attrs,
    evidence
FROM gold.cross_domain_snapshot;
