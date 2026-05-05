-- migrate:scope metadata
-- migrate:target_kind table
-- migrate:target_name meta.schema_change_registry
-- migrate:diff additive
-- migrate:diff_summary Add queryable planning detail columns for schema diffs, compatibility notes, and approval references.
-- migrate:compatibility backward_compatible
-- migrate:compatibility_notes Existing rows backfill safely while preserving previously recorded rollout statuses.
-- migrate:approval auto_approved
-- migrate:approval_notes Bootstrap-owned registry extension for immutable migration planning metadata.
-- migrate:approval_ref roadmap/task-1
-- migrate:approved_by bootstrap
-- migrate:summary Persist richer schema planning details for migration rollout decisions.

ALTER TABLE meta.schema_change_registry
    ADD COLUMN IF NOT EXISTS diff_summary String DEFAULT '' AFTER diff_status;

ALTER TABLE meta.schema_change_registry
    ADD COLUMN IF NOT EXISTS compatibility_notes String DEFAULT '' AFTER compatibility_status;

ALTER TABLE meta.schema_change_registry
    ADD COLUMN IF NOT EXISTS approval_ref Nullable(String) AFTER approval_notes;
