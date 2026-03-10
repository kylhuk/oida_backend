CREATE TABLE IF NOT EXISTS silver.fact_event_all ON CLUSTER 'osint_cluster_2s2r' AS silver.fact_event_local
ENGINE = Distributed('osint_cluster_2s2r', 'silver', 'fact_event_local', cityHash64(event_id));
