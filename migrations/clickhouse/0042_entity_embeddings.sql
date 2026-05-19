-- migrate:diff_summary Add entity embedding store for vector similarity search.
-- migrate:compatibility_notes New table only; no existing data is rewritten.
-- migrate:approval_ref frontend-spec-alignment/p3c

CREATE TABLE IF NOT EXISTS silver.entity_embedding
(
    vector_space         String,
    version              String,
    entity_id            String,
    entity_type          LowCardinality(String) DEFAULT '',
    embedding            Array(Float32)        CODEC(ZSTD(3)),
    source_text_sha256   String               DEFAULT '',
    generated_at         DateTime64(3, 'UTC') DEFAULT now64(3),
    schema_version       UInt16               DEFAULT 0,
    record_version       UInt64               DEFAULT 0,
    api_contract_version UInt16               DEFAULT 0,
    updated_at           DateTime64(3, 'UTC') DEFAULT now64(3),
    attrs                String               DEFAULT '{}',
    evidence             String               DEFAULT '[]'
) ENGINE = ReplacingMergeTree(record_version)
ORDER BY (vector_space, version, entity_type, entity_id);

-- Optional HNSW index for accelerated search; disabled by default.
-- Enable by running: ALTER TABLE silver.entity_embedding ADD INDEX vec_idx
-- embedding TYPE vector_similarity('hnsw', 'cosineDistance') GRANULARITY 64;
