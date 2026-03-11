CREATE TABLE IF NOT EXISTS ops.bulk_dump
(
    dataset_id String,
    item_key String,
    item_value String,
    loaded_from String,
    inserted_at DateTime
)
ENGINE = ReplacingMergeTree(inserted_at)
PARTITION BY toYYYYMM(inserted_at)
ORDER BY (dataset_id, inserted_at)
TTL inserted_at + INTERVAL 90 DAY DELETE;
