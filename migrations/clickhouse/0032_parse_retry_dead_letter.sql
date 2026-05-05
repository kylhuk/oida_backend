ALTER TABLE ops.parse_checkpoint
    ADD COLUMN IF NOT EXISTS attempt_count UInt16 DEFAULT 0 AFTER status;

ALTER TABLE ops.parse_checkpoint
    ADD COLUMN IF NOT EXISTS next_attempt_at Nullable(DateTime64(3, 'UTC')) AFTER parsed_at;

ALTER TABLE ops.parse_checkpoint
    ADD COLUMN IF NOT EXISTS last_error_code Nullable(String) AFTER next_attempt_at;

ALTER TABLE ops.parse_checkpoint
    ADD COLUMN IF NOT EXISTS last_error_message Nullable(String) AFTER last_error_code;

ALTER TABLE ops.parse_checkpoint
    ADD COLUMN IF NOT EXISTS dead_lettered_at Nullable(DateTime64(3, 'UTC')) AFTER last_error_message;
