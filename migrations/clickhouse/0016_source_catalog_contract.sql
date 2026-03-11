ALTER TABLE meta.source_registry
    ADD COLUMN IF NOT EXISTS catalog_kind LowCardinality(String) DEFAULT 'concrete' AFTER source_id;

ALTER TABLE meta.source_registry
    ADD COLUMN IF NOT EXISTS lifecycle_state LowCardinality(String) DEFAULT 'approved_enabled' AFTER catalog_kind;
