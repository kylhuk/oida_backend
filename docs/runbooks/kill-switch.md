# Kill Switch

## Purpose

Temporarily stop ingestion of a problematic source by toggling its governance flag in `meta.source_registry`. The kill switch gives operators a safe way to pause crawling, fetching, or parsing without touching the downstream jobs.

## Steps to Flip the Kill Switch

1. Identify the source row and current state:
   ```sh
   curl -fsS "http://localhost:8123/?query=SELECT%20source_id%2C%20enabled%2C%20disabled_reason%2C%20disabled_at%20FROM%20meta.source_registry%20WHERE%20source_id%3D'seed%3Agdelt'%20FORMAT%20TabSeparated"
   ```
2. Disable the source by writing `enabled=0` and capturing a reason. Include the operator name in `disabled_by` so the audit trail stays clear:
   ```sh
   curl -fsS "http://localhost:8123/?query=ALTER%20TABLE%20meta.source_registry%20UPDATE%20enabled%3D0%2C%20disabled_reason%3D'Daily%20crawl%20failure'%2C%20disabled_at%3Dnow64(3)%2C%20disabled_by%3D'svc_admin'%20WHERE%20source_id%3D'seed%3Agdelt'"
   ```
3. Confirm the kill switch took effect:
   ```sh
   curl -fsS "http://localhost:8123/?query=SELECT%20enabled%20FROM%20meta.source_registry%20WHERE%20source_id%3D'seed%3Agdelt'%20FORMAT%20TabSeparated"
   ```
4. Check your discovery/fetch logs to ensure the disabled source now bypasses job runs. For example, run the fetch job for that source and watch for an auditable skip:
   ```sh
   docker compose run --rm control-plane run-once --job fetch-frontier --source-id seed:gdelt
   ```

## Re-enabling the Source

1. Reverse the update so `enabled` becomes `1` and the disable fields go to defaults:
   ```sh
   curl -fsS "http://localhost:8123/?query=ALTER%20TABLE%20meta.source_registry%20UPDATE%20enabled%3D1%2C%20disabled_reason%3D''%2C%20disabled_at%3DNULL%2C%20disabled_by%3DNULL%20WHERE%20source_id%3D'seed%3Agdelt'"
   ```
2. Re-run the discovery/fetch job to make sure the pipeline picks the source back up.

## Verification

- `meta.source_registry` returns `enabled=0` (and the reason text) when the switch is off and `enabled=1` after re-enabling.
- The discovery/fetch job logs a skip reason when the source is disabled and resumes normal records once re-enabled.
- The ready-state API remains unaffected because the marker is independent of the kill switch.
