CREATE TABLE IF NOT EXISTS meta.source_registry_all ON CLUSTER 'osint_cluster_2s2r' AS meta.source_registry_local
ENGINE = Distributed('osint_cluster_2s2r', 'meta', 'source_registry_local', cityHash64(source_id));
