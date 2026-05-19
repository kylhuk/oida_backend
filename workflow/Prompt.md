# Prompt

Integrate AISstream.io vessel data ingestion into the Go OSINT backend.

The deliverable must:

- Add source governance for `catalog:auto:maritime-ocean-and-coastal-sources-aisstream` with parser `parser:aisstream-json`, promote profile `promote:maritime`, crawl strategy `websocket`, and bronze table routing for position and static-data message types.
- Implement a deterministic AISstream JSON decoder in Go under `internal/packs/maritime/aisstream`, covering PositionReport, ShipStaticData, StandardClassBPositionReport, ExtendedClassBPositionReport, AidToNavigationReport, and BaseStationReport message types. Unknown or other message types received from the stream must be handled gracefully — logged and skipped, not panicked.
- Register `parser:aisstream-json` in `internal/parser`; valid position messages produce vessel entity and track-point candidates, valid static-data messages produce vessel entity candidates, and unrecognized or malformed messages return typed parse errors.
- Add a Go `cmd/worker-aisstream` runtime binary using WebSocket transport to `wss://stream.aisstream.io/v0/stream` with a global bounding-box subscription (`[[-90,-180],[90,180]]`), authenticated via `APIKey` in the subscribe message, persisting parsed output through ClickHouse/MinIO without introducing Python or SQLite.
- Add append-only ClickHouse migration `0042_aisstream_ingestion.sql` for the AISstream source bronze table, position-report ops table, materialized views into `silver.dim_entity` and `silver.fact_track_point`, and any required `gold.api_v1_tracks` view adjustments to include AISstream point rows.
- Implement cross-source identity deduplication automatically via ClickHouse ReplacingMergeTree with `record_version = occurred_at`; entity canonical IDs use `ent:vessel:<imo>` when IMO is present and `ent:vessel:mmsi:<mmsi>` otherwise, matching VesselFinder's identity scheme.
- Keep live streaming opt-in through an explicit Docker Compose profile; default compose and default verification must not contact AISstream.io.
- Add tests for JSON decoding, message-type dispatch, parser registration, source/catalog/bronze coverage, API track fields, and fixture-driven decode-to-API behavior where practical.
- Keep workflow and specification documentation aligned and leave concrete evidence in `workflow/Completion.md`.
- During live validation, verify the whole platform path: retained AIS messages, parser replay timestamps, source bronze rows, silver/gold entity and track rows, deduplication with VesselFinder-sourced entities where MMSI/IMO overlap exists, and worker logs.
- If live message ingestion produces unexpectedly few vessels, isolate whether the cause is platform logic or external reachability with concrete ClickHouse counts and connection/log evidence.

Non-goals:

- Do not modify existing applied migrations in place.
- Do not introduce Python or SQLite runtime dependencies.
- Do not make default verification perform live WebSocket streaming.
