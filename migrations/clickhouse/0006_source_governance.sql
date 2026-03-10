ALTER TABLE meta.source_registry
    ADD COLUMN IF NOT EXISTS auth_config_json String DEFAULT '{}' AFTER auth_mode;

ALTER TABLE meta.source_registry
    ADD COLUMN IF NOT EXISTS requests_per_minute UInt32 DEFAULT 60 AFTER refresh_strategy;

ALTER TABLE meta.source_registry
    ADD COLUMN IF NOT EXISTS burst_size UInt16 DEFAULT 10 AFTER requests_per_minute;

ALTER TABLE meta.source_registry
    ADD COLUMN IF NOT EXISTS retention_class LowCardinality(String) DEFAULT 'warm' AFTER geo_scope;

ALTER TABLE meta.source_registry
    ADD COLUMN IF NOT EXISTS attribution_required UInt8 DEFAULT 0 AFTER terms_url;

ALTER TABLE meta.source_registry
    ADD COLUMN IF NOT EXISTS disabled_reason Nullable(String) AFTER enabled;

ALTER TABLE meta.source_registry
    ADD COLUMN IF NOT EXISTS disabled_at Nullable(DateTime64(3, 'UTC')) AFTER disabled_reason;

ALTER TABLE meta.source_registry
    ADD COLUMN IF NOT EXISTS disabled_by Nullable(String) AFTER disabled_at;

ALTER TABLE meta.source_registry
    ADD COLUMN IF NOT EXISTS review_status LowCardinality(String) DEFAULT 'approved' AFTER disabled_by;

ALTER TABLE meta.source_registry
    ADD COLUMN IF NOT EXISTS review_notes String DEFAULT '' AFTER review_status;

ALTER TABLE meta.source_registry
    ADD COLUMN IF NOT EXISTS backfill_priority UInt16 DEFAULT 100 AFTER supports_delta;

DROP VIEW IF EXISTS gold.api_v1_sources;

CREATE VIEW IF NOT EXISTS gold.api_v1_sources AS
SELECT source_id, domain, source_class, domain_family, enabled, updated_at
FROM meta.source_registry FINAL;
