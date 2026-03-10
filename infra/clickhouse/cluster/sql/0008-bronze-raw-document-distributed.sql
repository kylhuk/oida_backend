CREATE TABLE IF NOT EXISTS bronze.raw_document_all ON CLUSTER 'osint_cluster_2s2r' AS bronze.raw_document_local
ENGINE = Distributed('osint_cluster_2s2r', 'bronze', 'raw_document_local', cityHash64(raw_id));
