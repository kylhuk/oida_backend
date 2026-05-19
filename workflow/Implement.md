# Implement

## Runbook

1. Read workflow files and relevant package `AGENTS.md` files before editing touched areas.
2. Preserve existing migrations; add new migrations only.
3. Use TDD where practical:
   - add failing tests for each decoder/parser/API behavior,
   - verify they fail for the expected reason,
   - implement the minimal code,
   - re-run targeted tests.
4. Add WebSocket transport scaffolding in `cmd/bootstrap` and register the `websocket` crawl strategy.
5. Implement AISstream message decoding and candidate emission in `internal/packs/maritime/aisstream`.
6. Register `parser:aisstream-json` in `internal/parser` and emit canonical candidates compatible with `worker-parse`.
7. Add the append-only ClickHouse schema in `migrations/clickhouse/0042_aisstream_ingestion.sql`:
   - AISstream source bronze table,
   - position-report ops table (ReplacingMergeTree keyed by MMSI + occurred_at),
   - MVs into `silver.dim_entity` and `silver.fact_track_point`,
   - any required `gold.api_v1_tracks` view adjustment for AISstream rows.
8. Add the opt-in `cmd/worker-aisstream` binary and Compose `live-stream` profile.
9. For live platform validation, verify the full path with ClickHouse evidence:
   - retained AIS messages in `bronze.raw_document`,
   - parser rows in the AISstream source bronze table,
   - current entities in `gold.api_v1_entities`,
   - point rows in `gold.api_v1_tracks`,
   - position-report rows in the AISstream ops table,
   - worker logs and message-rate counters when live ingestion stalls.
10. Add append-only repair migrations if existing applied migrations are insufficient; do not edit applied SQL in place.
11. Update source catalog, compiled catalog, generated docs, specifications, and workflow evidence.
12. Run validation commands and record exact outcomes in `workflow/Completion.md`.

## Validation Commands

```bash
go test ./internal/packs/maritime/aisstream ./internal/parser ./cmd/api ./cmd/bootstrap
go test ./...
docker compose config
./scripts/verify.sh
FULL=1 ./scripts/verify.sh
docker compose logs --tail=80 worker-aisstream
```
