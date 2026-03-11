# Quality Dashboards

Each view surfaces the metrics we will guard before we mark the backend complete. The fixtures under `testdata/fixtures/quality` encode the sample payloads that drive the dashboard UI and aid automated tests.

## Internal Dashboard Surface

- UI: `http://localhost:8090/`
- Stats JSON: `http://localhost:8080/v1/internal/stats`
- Row-count policy: curated large-table counts are reported as `approximate`.
- MVP boundaries: geolocation map and schema-drift panels are intentionally excluded from the first dashboard cut.

## Source Catalog Rollout

Use the `summary` payload to verify rollout coverage without inspecting sources one by one. Operators should track:

- `catalog_total`, `catalog_concrete`, `catalog_fingerprint`, `catalog_family` to confirm the full machine-readable catalog is loaded.
- `catalog_runnable` to show concrete sources currently wired into the approved runtime seed set.
- `catalog_deferred` to show public concrete sources that are intentionally not runnable yet and must carry explicit `deferred_reason` metadata.
- `catalog_credential_gated` to show concrete sources that require env-var credentials or approval before they can move into runnable state.
- Deferred websocket/login/interactive transports remain counted in `catalog_deferred`; they are intentionally excluded from the current automatic sync loop and should not be treated as runnable automation gaps.

Interpretation rules:

- Concrete sources are either runnable or explicitly deferred.
- Fingerprints and family templates are generator inputs and remain review-gated; they are not counted as runnable sources.
- Credential-gated sources should stay visible in the catalog counts even when disabled or blocked by missing env vars.

## Freshness

Track how many minutes of lag each source has accumulated and whether the global pipeline mixes sources that are slow or blocked. The fixture `freshness_snapshot.json` captures the snapshot we expect the UI to show when the system meets the freshness policy. Use `sources_over_threshold`, `median_lag_seconds`, and `max_lag_seconds` to trigger alerts, and list the individual sources with their `source_id`, `freshness_seconds`, and `lag_reason` for quick triage. The dashboard should highlight the two sources that exceed the drift threshold of 600 seconds.

## Parser Success

This view summarizes parser throughput in rolling 15-minute windows. The sample fixture `parser_success.json` lists the total runs, success rate, and a failure breakdown. Build a bar chart that shows `success_rate` over time and a table that expands each failure reason with the count and an example source. Use the fixture data to lock the tooltip text and to rehearse alert conditions such as repeated `schema-drift` hits.

## Geolocation Coverage (Deferred)

Coverage concerns are captured by `geolocation_coverage.json`. This panel is deferred for a later dashboard iteration and is not rendered in the first Tailwind v4 MVP.

## Schema Drift (Deferred)

Track schema drift per table using the `schema_drift.json` fixture. This panel is deferred for a later dashboard iteration and is not rendered in the first Tailwind v4 MVP.
