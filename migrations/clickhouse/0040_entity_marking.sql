-- Entity marking: adds a data-classification label to entities.
-- marking_default on source_registry lets operators declare the classification
-- for everything a source produces; marking on dim_entity carries the resolved
-- value at ingestion time (defaulting to 'UNCLASSIFIED' when not set).

ALTER TABLE meta.source_registry
    ADD COLUMN IF NOT EXISTS marking_default LowCardinality(String) DEFAULT 'UNCLASSIFIED';

ALTER TABLE silver.dim_entity
    ADD COLUMN IF NOT EXISTS marking LowCardinality(String) DEFAULT 'UNCLASSIFIED';

-- Rebuild the gold view to include marking.
DROP VIEW IF EXISTS gold.api_v1_entities;

CREATE VIEW IF NOT EXISTS gold.api_v1_entities AS
SELECT
    entity_id,
    entity_type,
    canonical_name,
    status,
    risk_band,
    primary_place_id,
    source_system,
    marking,
    valid_from,
    valid_to,
    schema_version,
    record_version,
    api_contract_version,
    updated_at,
    attrs,
    evidence
FROM silver.dim_entity FINAL;
