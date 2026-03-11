ALTER TABLE meta.source_registry
    ADD COLUMN IF NOT EXISTS transport_type LowCardinality(String) DEFAULT 'http' AFTER auth_config_json;

ALTER TABLE meta.source_registry
    ADD COLUMN IF NOT EXISTS crawl_enabled UInt8 DEFAULT 1 AFTER transport_type;

ALTER TABLE meta.source_registry
    ADD COLUMN IF NOT EXISTS allowed_hosts Array(String) DEFAULT [] AFTER entrypoints;

ALTER TABLE meta.source_registry
    ADD COLUMN IF NOT EXISTS crawl_strategy LowCardinality(String) DEFAULT 'delta' AFTER refresh_strategy;

ALTER TABLE meta.source_registry
    ADD COLUMN IF NOT EXISTS crawl_config_json String DEFAULT '{}' AFTER crawl_strategy;

ALTER TABLE meta.source_registry
    ADD COLUMN IF NOT EXISTS parse_config_json String DEFAULT '{}' AFTER parser_id;

ALTER TABLE meta.source_registry
    ADD COLUMN IF NOT EXISTS bronze_table Nullable(String) AFTER parse_config_json;

ALTER TABLE meta.source_registry
    ADD COLUMN IF NOT EXISTS bronze_schema_version UInt32 DEFAULT 1 AFTER bronze_table;

ALTER TABLE meta.source_registry
    ADD COLUMN IF NOT EXISTS promote_profile Nullable(String) AFTER bronze_schema_version;
