ALTER TABLE ops.fetch_log
    ADD COLUMN IF NOT EXISTS attempt_count UInt16 DEFAULT 1 AFTER latency_ms;

ALTER TABLE ops.fetch_log
    ADD COLUMN IF NOT EXISTS retry_count UInt16 DEFAULT 0 AFTER attempt_count;

ALTER TABLE bronze.raw_document
    ADD COLUMN IF NOT EXISTS fetch_id String DEFAULT '' AFTER raw_id;

ALTER TABLE bronze.raw_document
    ADD COLUMN IF NOT EXISTS final_url String DEFAULT url AFTER url;

ALTER TABLE bronze.raw_document
    ADD COLUMN IF NOT EXISTS etag Nullable(String) AFTER object_key;

ALTER TABLE bronze.raw_document
    ADD COLUMN IF NOT EXISTS last_modified Nullable(String) AFTER etag;

ALTER TABLE bronze.raw_document
    ADD COLUMN IF NOT EXISTS not_modified UInt8 DEFAULT 0 AFTER last_modified;

ALTER TABLE bronze.raw_document
    ADD COLUMN IF NOT EXISTS storage_class LowCardinality(String) DEFAULT 'metadata-only' AFTER not_modified;
