# Unresolved Location Triage

## Purpose

Deal with rows that could not anchor to a place so that downstream metrics, analytics, and API views do not lose confidence in geography. The `ops.unresolved_location_queue` table keeps the offenders in one place.

## Steps

1. Look at the queue to understand which records stalled and why:
   ```sh
   curl -fsS "http://localhost:8123/?query=SELECT%20source_id%2C%20record_id%2C%20geo_method%2C%20geo_confidence%2C%20error_reason%20FROM%20ops.unresolved_location_queue%20ORDER%20BY%20created_at%20DESC%20LIMIT%2050%20FORMAT%20JSONCompact"
   ```
2. Identify whether the failure stems from missing place data, ambiguous coordinates, or parse drift. If the location reference is valid, update `silver.dim_place` or the underlying fixtures so the reverse-geocoder has a match.
3. Once the supporting data is ready, rerun the supported control-plane jobs for place materialization and promotion. There is no queue-specific run-once job today, so the canonical retry path is the same CLI contract used elsewhere:
   ```sh
   docker compose run --rm control-plane run-once --job place-build
   docker compose run --rm control-plane run-once --job promote
   ```
4. When the job completes, verify the problematic row no longer appears in the queue and that `silver.dim_place` gained any missing rows needed for the lookup.

## Verification

- `ops.unresolved_location_queue` returns zero rows for the affected source and the `error_reason` clears.
- The new place rows appear in `silver.dim_place` or `silver.place_hierarchy` with the expected IDs and `geo_scope`.
- The API outputs with location data (for example `/v1/places`) now include the place IDs that were previously unresolved.

## Notes

- Keep a copy of the fixture coordinates that surface each day so you can replay the queue after schema or parser updates.
- Re-running `place-build` and `promote` is the supported retry path until a queue-specific job exists.
