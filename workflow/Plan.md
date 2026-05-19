# Plan

## Milestones

1. Workflow refresh
   - Rewrite `workflow/Prompt.md`, `workflow/Plan.md`, `workflow/Implement.md`, `workflow/Completion.md`, and `workflow/Documentation.md` for AISstream ingestion.
   - Record the task shift in `workflow/Documentation.md`.
   - Acceptance: workflow files describe this task, validation commands, and evidence slots.
   - Status: completed.

2. WebSocket transport scaffolding
   - Register the `websocket` crawl strategy value in the source governance schema via `cmd/bootstrap` seed/bootstrap operations (this is a catalog/seed registration step, not network transport code; the actual WebSocket network code lives in `cmd/worker-aisstream`).
   - Add `parser:aisstream-json` scaffolding stub so the parser registry recognizes the source before the full decoder is implemented.
   - Acceptance: `go test ./cmd/bootstrap ./internal/parser` passes and `worker-parse list-parsers` includes the AISstream parser.
   - Status: pending.

3. AISstream domain pack
   - Add fixture-backed tests for all six AISstream message types (PositionReport, ShipStaticData, StandardClassBPositionReport, ExtendedClassBPositionReport, AidToNavigationReport, BaseStationReport), coordinate/speed/course parsing, MMSI/IMO identity derivation, and field diffs. Unknown or other message types received from the stream must be handled gracefully — logged and skipped, not panicked.
   - Implement `internal/packs/maritime/aisstream` with deterministic JSON decoding and entity/track-point candidate emission.
   - Acceptance: targeted package tests pass.
   - Status: pending.

4. ClickHouse migration
   - Add `migrations/clickhouse/0042_aisstream_ingestion.sql` with the AISstream source bronze table, a position-report ops table (ReplacingMergeTree keyed by MMSI + occurred_at), materialized views into `silver.dim_entity` and `silver.fact_track_point`, and any required `gold.api_v1_tracks` view update to surface AISstream point rows.
   - Deduplication strategy: ReplacingMergeTree with `record_version = occurred_at`; cross-source identity uses `ent:vessel:<imo>` or `ent:vessel:mmsi:<mmsi>` to collide with VesselFinder-sourced entities automatically.
   - Acceptance: `go test ./internal/migrate` passes and migration checksums are stable.
   - Status: pending.

5. AISstream JSON parser
   - Add tests for `parser:aisstream-json` entity and track-point candidates, malformed-message typed errors, and default registry visibility.
   - Implement the parser wrapper in `internal/parser/aisstream_json.go` and register it in `internal/parser/registry.go`.
   - Acceptance: parser tests pass and `worker-parse list-parsers` includes the AISstream parser.
   - Status: pending.

6. Seed catalog and registry
   - Update and activate the existing AISstream entry at `catalog:concrete:maritime-ocean-and-coastal-sources:aisstream` in `seed/source_catalog.json`, regenerate `seed/source_catalog_compiled.json` via `cmd/bootstrap compile-catalog`, and add a source-registry entry for `catalog:auto:maritime-ocean-and-coastal-sources-aisstream`.
   - Update `sources.md` with the AISstream source entry.
   - Acceptance: bootstrap/catalog tests pass and the source appears in compiled output.
   - Status: pending.

7. Worker binary
   - Add `cmd/worker-aisstream` with WebSocket subscribe/receive loop, global bounding-box subscription, reconnect-on-close behavior, configurable API key via environment variable, and ClickHouse/MinIO persistence reusing internal ClickHouse and MinIO client helpers from the existing worker shared packages (not the worker-parse binary itself).
   - Acceptance: `go test ./cmd/worker-aisstream` passes.
   - Status: pending.

8. Docker Compose service and .env config
   - Add `worker-aisstream` service to `docker-compose.yml` under the explicit `live-stream` Compose profile.
   - Add `AISSTREAM_API_KEY` to `.env.example` and document the opt-in profile in the README.
   - Acceptance: `docker compose config` passes; `docker compose --profile live-stream config` includes the service.
   - Status: pending.

9. End-to-end test
   - Add a fixture-driven e2e test in `test/e2e/pipeline_test.go` that replays AISstream JSON fixtures through the parser, inserts into ClickHouse bronze, and verifies silver/gold promotion produces the expected entity and track-point rows.
   - Acceptance: e2e test passes under `go test ./test/e2e/... -tags=e2e`.
   - Status: pending.

10. Documentation
    - Extend API track resource fields and contract docs for AISstream point columns if new fields are introduced.
    - Update `specifications/*`, `docs/capability-matrix.md`, `docs/api-reference.md`, README API inventory, and workflow evidence.
    - Acceptance: `go test ./cmd/api` passes and docs reflect AISstream coverage.
    - Status: pending.

## Acceptance Criteria

- AISstream runtime source is governed, seeded, and kill-switch compatible.
- Live AISstream streaming is opt-in only via the `live-stream` Compose profile.
- Received AIS messages can be retained through existing raw/bronze paths and parsed into vessel entities and track points.
- `gold.api_v1_tracks` and `/v1/entities/{entityId}/tracks` expose AISstream point rows alongside VesselFinder rows.
- Cross-source deduplication is automatic: entities with matching IMO or MMSI from AISstream and VesselFinder collapse in `silver.dim_entity` via ReplacingMergeTree.
- Existing migration checksum expectations remain immutable; new schema work is append-only.
- Parser and decoder behavior is covered by focused tests.
- AISstream ops position-report table is populated from parsed retained messages.
- Verification evidence is recorded in `workflow/Completion.md`.

## Validation Commands

Run targeted tests as each milestone is implemented, then run:

```bash
go test ./...
docker compose config
./scripts/verify.sh
FULL=1 ./scripts/verify.sh
```
