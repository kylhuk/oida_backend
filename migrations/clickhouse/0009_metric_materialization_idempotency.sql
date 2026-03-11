ALTER TABLE silver.metric_contribution
    ADD COLUMN IF NOT EXISTS source_id String AFTER source_record_id;

ALTER TABLE silver.metric_contribution
    ADD COLUMN IF NOT EXISTS materialization_key String AFTER window_end;

ALTER TABLE gold.metric_state
    ADD COLUMN IF NOT EXISTS materialization_key String AFTER window_end;

ALTER TABLE gold.metric_state
    ADD COLUMN IF NOT EXISTS distinct_source_count_state AggregateFunction(uniqExact, String) AFTER last_contribution_at_state;

ALTER TABLE gold.metric_state
    ADD COLUMN IF NOT EXISTS latest_value_state AggregateFunction(argMax, Float64, DateTime64(3, 'UTC')) AFTER distinct_source_count_state;

ALTER TABLE gold.metric_snapshot
    ADD COLUMN IF NOT EXISTS materialization_key String AFTER window_end;
