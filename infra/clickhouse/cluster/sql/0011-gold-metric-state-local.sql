CREATE TABLE IF NOT EXISTS gold.metric_state_local ON CLUSTER 'osint_cluster_2s2r'
(
    metric_id String,
    subject_grain LowCardinality(String),
    subject_id String,
    place_id String,
    window_grain LowCardinality(String),
    window_start DateTime64(3, 'UTC'),
    window_end DateTime64(3, 'UTC'),
    contribution_count_state AggregateFunction(count),
    contribution_value_state AggregateFunction(sum, Float64),
    contribution_weight_state AggregateFunction(sum, Float64),
    peak_value_state AggregateFunction(max, Float64),
    last_contribution_at_state AggregateFunction(max, DateTime64(3, 'UTC')),
    updated_at DateTime64(3, 'UTC')
)
ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/osint_cluster_2s2r/{database}/{table}/{shard}', '{replica}')
PARTITION BY toYYYYMM(window_start)
ORDER BY (metric_id, subject_grain, subject_id, window_grain, window_start)
TTL toDateTime(window_start) + INTERVAL 730 DAY DELETE;
