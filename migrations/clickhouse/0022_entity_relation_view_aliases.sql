DROP VIEW IF EXISTS gold.api_v1_entity_events;

CREATE VIEW IF NOT EXISTS gold.api_v1_entity_events AS
SELECT
    bee.entity_id AS entity_id,
    e.event_id AS event_id,
    e.event_type AS event_type,
    e.event_subtype AS event_subtype,
    e.place_id AS place_id,
    e.starts_at AS starts_at,
    e.status AS status,
    e.confidence_band AS confidence_band,
    e.impact_score AS impact_score
FROM silver.bridge_event_entity AS bee
INNER JOIN gold.api_v1_events AS e ON e.event_id = bee.event_id;

DROP VIEW IF EXISTS gold.api_v1_entity_places;

CREATE VIEW IF NOT EXISTS gold.api_v1_entity_places AS
SELECT
    bep.entity_id AS entity_id,
    p.place_id AS place_id,
    p.canonical_name AS canonical_name,
    p.place_type AS place_type,
    bep.relation_type AS relation_type,
    bep.linked_at AS linked_at
FROM silver.bridge_entity_place AS bep
INNER JOIN gold.api_v1_places AS p ON p.place_id = bep.place_id;
