ALTER TABLE ops.fetch_log
    MODIFY TTL toDateTime(fetched_at) + INTERVAL 180 DAY DELETE;

ALTER TABLE bronze.raw_document
    MODIFY TTL toDateTime(fetched_at) + INTERVAL 180 DAY DELETE;
