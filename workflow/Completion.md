# Completion Evidence

## Requirements

| Requirement | Evidence |
| --- | --- |
| Workflow refreshed for AISstream ingestion | `workflow/Prompt.md`, `workflow/Plan.md`, `workflow/Implement.md`, `workflow/Documentation.md`, and this evidence ledger were refreshed for the AISstream runtime path. |
| AISstream decoder and domain pack implemented with tests | (pending) Add `internal/packs/maritime/aisstream` with deterministic JSON decoding, message-type dispatch, coordinate/speed/course parsing, MMSI/IMO identity derivation, and candidate emission. Verify by `go test ./internal/packs/maritime/aisstream`. |
| `parser:aisstream-json` registered and tested | (pending) Add `internal/parser/aisstream_json.go`, register it in `internal/parser/registry.go`, and add parser compatibility for `websocket` crawl strategy. Verify by `go test ./internal/parser -run AISstream` and full package tests. |
| Source governance and bronze routing added append-only | (pending) Add the AISstream source to `sources.md`, `seed/source_catalog.json`, and regenerate `seed/source_catalog_compiled.json`; existing bronze migrations remain immutable and the new bronze table is added in `0042`. Verify by bootstrap/catalog tests and full test suite. |
| ClickHouse bronze table, ops table, MVs, and tracks view added append-only | (pending) Add `migrations/clickhouse/0042_aisstream_ingestion.sql` with source bronze table, position-report ops table (ReplacingMergeTree), silver materialized views, and `gold.api_v1_tracks` update. Verify by `go test ./internal/migrate` and `FULL=1 ./scripts/verify.sh`. |
| Cross-source deduplication works automatically | (pending) Verify that AISstream and VesselFinder entities with matching IMO or MMSI collapse in `silver.dim_entity` via ReplacingMergeTree `record_version = occurred_at`. Verify by ClickHouse count query after live or fixture-driven ingestion. |
| Opt-in WebSocket worker binary and Compose profile added | (pending) Add `cmd/worker-aisstream` and wire `worker-aisstream` under the explicit `live-stream` Compose profile. Verify by `go test ./cmd/worker-aisstream` and `docker compose --profile live-stream config`. |
| API tracks fields and docs updated | (pending) Extend entity track resource metadata if new AISstream-specific fields are introduced, and regenerate `docs/api-reference.md`, README API inventory, and `testdata/fixtures/contracts/api_v1_schema.json`. Verify by `go test ./cmd/api` and `go test ./...`. |
| Default verification does not perform live AISstream streaming | (pending) `worker-aisstream` is profile-gated and not part of default `docker compose config`; `./scripts/verify.sh` passes without live streaming. |
| `go test ./...` passed | (pending) |
| `docker compose config` passed | (pending) |
| `./scripts/verify.sh` passed | (pending) |
| `FULL=1 ./scripts/verify.sh` passed | (pending) |
| Live AISstream service check | (pending) Start `worker-aisstream` under the `live-stream` profile. Verify: retained AIS message count in `bronze.raw_document`, parser/bronze row counts in the AISstream source bronze table, silver entity and track-point row counts, deduplication collision counts with VesselFinder-sourced entities, ops position-report row counts, and worker logs showing message-rate and any error conditions. |

## Command Log

(to be filled during execution)

## Changed Files

(to be filled during execution)
