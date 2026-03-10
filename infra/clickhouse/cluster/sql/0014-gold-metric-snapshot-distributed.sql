CREATE TABLE IF NOT EXISTS gold.metric_snapshot_all ON CLUSTER 'osint_cluster_2s2r' AS gold.metric_snapshot_local
ENGINE = Distributed('osint_cluster_2s2r', 'gold', 'metric_snapshot_local', cityHash64(snapshot_id));
