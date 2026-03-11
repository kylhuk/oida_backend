ALTER TABLE meta.source_family_template
    ADD COLUMN IF NOT EXISTS scope String AFTER family_name;

ALTER TABLE meta.source_family_template
    ADD COLUMN IF NOT EXISTS integration_archetype LowCardinality(String) AFTER outputs;

ALTER TABLE meta.source_family_template
    ADD COLUMN IF NOT EXISTS review_status_default LowCardinality(String) AFTER integration_archetype;

ALTER TABLE meta.source_family_template
    ADD COLUMN IF NOT EXISTS generator_relationships Array(String) AFTER review_status_default;
