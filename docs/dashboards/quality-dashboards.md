# Quality Dashboards

Each view surfaces the metrics we will guard before we mark the backend complete. The fixtures under `testdata/fixtures/quality` encode the sample payloads that drive the dashboard UI and aid automated tests.

## Freshness

Track how many minutes of lag each source has accumulated and whether the global pipeline mixes sources that are slow or blocked. The fixture `freshness_snapshot.json` captures the snapshot we expect the UI to show when the system meets the freshness policy. Use `sources_over_threshold`, `median_lag_seconds`, and `max_lag_seconds` to trigger alerts, and list the individual sources with their `source_id`, `freshness_seconds`, and `lag_reason` for quick triage. The dashboard should highlight the two sources that exceed the drift threshold of 600 seconds.

## Parser Success

This view summarizes parser throughput in rolling 15-minute windows. The sample fixture `parser_success.json` lists the total runs, success rate, and a failure breakdown. Build a bar chart that shows `success_rate` over time and a table that expands each failure reason with the count and an example source. Use the fixture data to lock the tooltip text and to rehearse alert conditions such as repeated `schema-drift` hits.

## Geolocation Coverage

Coverage concerns are captured by `geolocation_coverage.json`. The dashboard should map each region to its `coverage_pct`, and it should call out the missing admin levels for regions like the Pacific Islands. The `sample_points` array proves we can resolve Paris, Texas, and Yaren even when the spatial data is spotty; unresolved points (for example `plc:ovl-west`) should be flagged for review. Use that fixture to confirm that the coverage map defaults to North America at 98.3% while keeping the coverage color scale stable.

## Schema Drift

Track schema drift per table using the `schema_drift.json` fixture. The dashboard highlights the `drift_score` and surfaces `alerts` that require migration updates. Use the fixture to verify the narrative text for `gold.metric_state` when it is missing the `weight` column and to confirm the table-level severity mapping (high, medium, low). Add a query against `meta.schema_migrations` and `system.columns` that compares live column sets with the expected columns stored in this fixture to keep the drift count deterministic.
