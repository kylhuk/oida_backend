CREATE TABLE IF NOT EXISTS gold.metric_state_all ON CLUSTER 'osint_cluster_2s2r' AS gold.metric_state_local
ENGINE = Distributed('osint_cluster_2s2r', 'gold', 'metric_state_local', cityHash64(subject_id));
