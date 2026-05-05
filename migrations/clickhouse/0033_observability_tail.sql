ALTER TABLE ops.job_run
    ADD COLUMN IF NOT EXISTS correlation_id Nullable(String) AFTER job_id;

ALTER TABLE ops.fetch_log
    ADD COLUMN IF NOT EXISTS correlation_id Nullable(String) AFTER fetch_id;

ALTER TABLE ops.parse_log
    ADD COLUMN IF NOT EXISTS correlation_id Nullable(String) AFTER parse_id;
