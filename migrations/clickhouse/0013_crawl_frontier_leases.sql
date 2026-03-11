ALTER TABLE ops.crawl_frontier
    ADD COLUMN IF NOT EXISTS lease_owner Nullable(String) AFTER state;

ALTER TABLE ops.crawl_frontier
    ADD COLUMN IF NOT EXISTS lease_expires_at Nullable(DateTime64(3, 'UTC')) AFTER lease_owner;

ALTER TABLE ops.crawl_frontier
    ADD COLUMN IF NOT EXISTS attempt_count UInt16 DEFAULT 0 AFTER lease_expires_at;

ALTER TABLE ops.crawl_frontier
    ADD COLUMN IF NOT EXISTS last_attempt_at Nullable(DateTime64(3, 'UTC')) AFTER attempt_count;

ALTER TABLE ops.crawl_frontier
    ADD COLUMN IF NOT EXISTS last_fetch_id Nullable(String) AFTER last_attempt_at;

ALTER TABLE ops.crawl_frontier
    ADD COLUMN IF NOT EXISTS last_status_code Nullable(UInt16) AFTER last_fetch_id;

ALTER TABLE ops.crawl_frontier
    ADD COLUMN IF NOT EXISTS last_error_code Nullable(String) AFTER last_status_code;

ALTER TABLE ops.crawl_frontier
    ADD COLUMN IF NOT EXISTS last_error_message Nullable(String) AFTER last_error_code;

ALTER TABLE ops.crawl_frontier
    ADD COLUMN IF NOT EXISTS etag Nullable(String) AFTER last_error_message;

ALTER TABLE ops.crawl_frontier
    ADD COLUMN IF NOT EXISTS last_modified Nullable(String) AFTER etag;

ALTER TABLE ops.crawl_frontier
    ADD COLUMN IF NOT EXISTS discovery_kind LowCardinality(String) DEFAULT 'unknown' AFTER canonical_url;
