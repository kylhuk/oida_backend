# Documentation Log

## 2026-05-05

- Established Go OSINT as the repository's active product.
- Recorded single-node Docker as the production target.
- Recorded hashed API keys as the first production authentication model.
- Noted that Kubernetes, OIDC/JWT, and custom frontend work are deferred.
- Removed the legacy Python runtime and the unused renderer surface from the production repository state.
- Added scoped API-key auth, rate limiting, `/metrics`, monitoring provisioning, and native ClickHouse backup/restore/retention jobs.
- Rewrote README, production readiness notes, and runbooks around the Go-only single-node topology.
- Default verification passed; full Compose verification is blocked by an existing `medallion-minio-1` container using host ports `9000-9001`.

## 2026-05-06

- Stopped the conflicting `medallion-minio-1` and `medallion-http-fixture-1` containers so the OIDA full Compose gate could bind `9000-9001` and `8079`.
- Added fresh-volume reset behavior to `scripts/verify.sh` for `FULL=1` runs.
- Fixed runtime image packaging by copying migrations, seed data, backup assets, and source catalog markdown into the distroless image.
- Added `osint_promote` privileges required by automatic discovery-candidate sync and verified them through bootstrap.
- Changed generated discovery-candidate writes from streaming `INSERT ... VALUES` to executable `INSERT ... SELECT` SQL to avoid ClickHouse HTTP hangs.
- Full verification passed with `FULL=1 ./scripts/verify.sh`, including fresh Compose startup, `bootstrap verify`, and E2E.

## 2026-05-06 Specifications Task

- Reframed the active workflow around a documentation-only system specifications deliverable.
- The intended output is a root `specifications/` folder for future implementation agents.
- Runtime behavior must be grounded in current Go code, ClickHouse migrations, seed files, Compose topology, and existing docs.
- Cataloged but not runtime-runnable sources must be labeled separately from implemented worker/job/API behavior.

## 2026-05-06 VesselFinder Ingestion Task

- Reframed the active workflow around adding VesselFinder browser-rendered ship ingestion.
- The runtime must stay Go/ClickHouse/MinIO based and must not introduce Python or SQLite.
- Live VesselFinder crawling must remain opt-in through an explicit Compose profile; default verification must avoid external crawling.
- Source governance, bronze schema, silver materialization, API exposure, workflow evidence, and specifications must stay aligned.
- Live service check found the initial Chromium runtime was not usable with `HOME=/nonexistent`; the worker now creates a writable temporary browser profile, disables crashpad, and sets `HOME=/tmp` in the browser image.
- Live service check found the default crawler user agent received `403 Forbidden`; the worker default now uses a browser-like user agent.
- Live VesselFinder markup uses `name="flag"` and hyphenated `advsearch-ship-type`; dimension extraction now covers those selector shapes.
- After the runtime fixes, the worker reached VesselFinder and inserted 24 successful `ops.vesselfinder_page_job` rows before the live run was stopped. It did not discover detail URLs, fetch retained detail HTML, or parse vessel/track rows; the generated `flag/type/page` query path returned no detail links in the sampled page.
- Stopped `worker-vesselfinder` after the live check to avoid an unbounded external crawl while the remaining discovery URL-shape issue is unresolved.
- Follow-up live debugging showed the query URL shape was valid; the crawler was wrong because it randomized individual pages and could start a sparse dimension at page 124 before checking page 1. `BuildPageJobs` now randomizes flag/type dimension groups while keeping pages ascending, and discovery treats a 200 page with zero detail links as terminal for that dimension.
- Discovery now queues detail URLs from populated VesselFinder pages. A bounded live run produced 66 page-job rows, 145 scan-queue rows, and page-job statuses including `empty` for sparse dimensions.
- Detail scanning initially failed because large rendered HTML was inlined into ClickHouse metadata. `worker-vesselfinder` now persists detail HTML through MinIO object storage and records object-store raw documents.
- `worker-parse` needed MinIO credentials in Compose to replay object-stored raw documents; those env vars are now wired.
- `worker-parse` also needed quoted ClickHouse identifiers for generated bronze table names containing hyphens. Bronze insert SQL now quotes the table identifier.
- Parser bot-page detection was too broad because it treated any `cloudflare` script reference as a bot page. Detection now keys on explicit challenge/captcha strings instead.
- Live evidence after fixes: 6 VesselFinder `bronze.raw_document` rows, 3 fresh VesselFinder source-bronze rows, and 3 `silver.dim_entity` vessel rows were produced. The sampled pages did not produce track points because no recognized coordinates were present in those detail pages.
- Follow-up replay of retained detail HTML showed current VesselFinder pages expose identifiers and position hints through title text, `h2`, script variables, and `#djson data-json`, not only `dt/dd` fields.
- `parser:vesselfinder-html` now extracts MMSI and vessel type from the current page shape, ignores invalid sentinel coordinates such as `91/181`, and emits track points from valid `djson` positions. The parser version is bumped to `1.0.1` so retained raw documents reparse.
- Live reparse evidence after the parser fix: 11 retained raw documents were replayed with 0 failures, producing 11 entity rows and 1 track-point row at parser version `1.0.1`; `silver.dim_entity` has 11 VesselFinder entities and `gold.api_v1_tracks` has 1 VesselFinder point row.
- VesselFinder entities currently do not have country/place graph linkage in `silver.dim_entity.primary_place_id`; verified count is 0 of 11 for this live sample.
- A 12-hour live check showed discovery kept running but only the manually scanned 11 raw documents existed. Root cause: service mode ran full discovery before scanning, so the detail scanner was starved by the large country/type/page space. A second issue was constant `record_version=1` writes to `ReplacingMergeTree(record_version)` queue tables, making replacement state nondeterministic.
- `worker-vesselfinder` service mode now runs discovery and scan loops concurrently; queue/page replacement writes use nanosecond timestamp record versions, and expired leases are claimable again.
- After rebuilding the live crawler from the fixed tree, retained VesselFinder raw documents increased from 11 to 63 and `silver.dim_entity` increased to 59 within minutes. `worker-parse` consumed new raw documents automatically.
- VesselFinder discovery context is now carried through scan-queue attrs, raw-document fetch metadata, parser input attrs, source bronze payloads, silver entity primary places, track-point places, and entity-place bridge rows.
- Added append-only migrations `0037_vesselfinder_geo_context.sql` and `0038_vesselfinder_mid_place_context.sql`; `0037` materializes discovery flag dimensions as places and bridges source-bronze entities to places, while `0038` materializes parser-derived flag/MID places from source-bronze rows.
- Because live discovery can time out before dimensions load, the parser now falls back to observed MMSI MID prefixes for currently observed live allocations: `451` -> Kyrgyz Republic, `533` -> Malaysia, `538` -> Marshall Islands, `548` -> Philippines, and `618` -> Crozet Archipelago.
- Live evidence after parser `1.0.3` reprocessing: 92 current VesselFinder silver entities, 74 with `primary_place_id`, 75 `flag_state` bridge rows, 4 VesselFinder-derived place rows, and all 46 VesselFinder track points have place IDs. The remaining 18 entities lack usable MMSI/flag context in the retained detail page.
- A follow-up platform check found `worker-parse` parsed ClickHouse `DateTime64` values like `2026-05-07 07:20:01.815` as unknown strings and silently fell back to parse time. This polluted replayed VesselFinder track rows with parser-time `observed_at` values.
- `worker-parse` now accepts ClickHouse `DateTime64` timestamps, and `parser:vesselfinder-html` is bumped to `1.0.4` so retained raw documents replay with correct observed times. Gold VesselFinder tracks now expose 60 point rows, all with place IDs, with observed times bounded by the actual raw fetch window (`2026-05-06 19:52:07` to `2026-05-07 07:20:01` UTC).
- Added append-only migrations `0039_vesselfinder_history_and_tracks_view.sql`, `0040_vesselfinder_dim_entity_record_version.sql`, and `zzzzzzz_vesselfinder_tracks_view_final.sql`. These wire VesselFinder bronze rows into ops vessel state and position history, ensure VesselFinder silver entity replacement versions are based on parse time, and keep `gold.api_v1_tracks` on the latest VesselFinder parser output after legacy final view migrations.
- Live replay after parser `1.0.4` produced 120 entity bronze rows and 60 track-point bronze rows; `ops.vesselfinder_vessel_state` has 92 state rows, `ops.vesselfinder_position_history` has 60 position rows, and `gold.api_v1_entities` has 91 current VesselFinder entities, 73 with `primary_place_id`.
- The reason the live vessel count is still not in the thousands is upstream reachability from this host, not parse/promotion capacity: `bronze.raw_document` remains at 95 VesselFinder retained detail pages, the scan queue has about 67k discovered pending detail URLs, and live worker logs show `tcp preflight www.vesselfinder.com:443 ... i/o timeout` before HTTP. `worker-vesselfinder` now classifies these as `connect_timeout` and logs `scan_loop_batch` counters.
- To avoid grinding through the discovered backlog while the host cannot reach VesselFinder, `worker-vesselfinder` now checks TCP reachability before claiming scan rows. Current live logs show `scan_loop_blocked` with the same TCP timeout, so pending detail URLs are preserved until connectivity returns.
- VesselFinder task is complete. All migrations are applied, worker is profile-gated, documentation and specifications are updated.

## 2026-05-19 AISstream Ingestion Task

- Reframed the active workflow around adding AISstream.io real-time WebSocket AIS ingestion.
- The runtime must stay Go/ClickHouse/MinIO based and must not introduce Python or SQLite.
- Live AISstream streaming must remain opt-in through the `live-stream` Compose profile; default verification must avoid external WebSocket connections.
- Source governance, bronze schema, silver materialization, API exposure, deduplication with VesselFinder-sourced entities, workflow evidence, and specifications must stay aligned.
- AISstream.io provides real-time global AIS data over WebSocket (`wss://stream.aisstream.io/v0/stream`); subscription is via a JSON `SubscribeMessage` containing `APIKey` and a `BoundingBoxes` array; the global subscription uses `[[-90,-180],[90,180]]`.
- Supported message types: PositionReport, ShipStaticData, StandardClassBPositionReport, ExtendedClassBPositionReport, AidToNavigationReport, BaseStationReport.
- Cross-source deduplication strategy: ReplacingMergeTree with `record_version = occurred_at`; entity canonical IDs use `ent:vessel:<imo>` (when IMO is non-zero) or `ent:vessel:mmsi:<mmsi>`, matching VesselFinder's identity scheme so that overlap collapses automatically in `silver.dim_entity`.

## Documents to Update When AISstream Task Completes

- `sources.md` — add AISstream source entry.
- `seed/source_catalog.json` — update and activate the existing AISstream entry at `catalog:concrete:maritime-ocean-and-coastal-sources:aisstream`.
- `seed/source_catalog_compiled.json` — regenerate via `cmd/bootstrap compile-catalog`.
- `migrations/clickhouse/0042_aisstream_ingestion.sql` — new append-only migration.
- `internal/packs/maritime/aisstream/` — new domain pack.
- `internal/parser/aisstream_json.go` — new parser.
- `internal/parser/registry.go` — register `parser:aisstream-json`.
- `cmd/worker-aisstream/` — new worker binary.
- `docker-compose.yml` — add `worker-aisstream` under `live-stream` profile.
- `.env.example` — add `AISSTREAM_API_KEY`.
- `docs/api-reference.md` — regenerate if new track fields are added.
- `docs/capability-matrix.md` — add AISstream to maritime source coverage.
- `testdata/fixtures/contracts/api_v1_schema.json` — regenerate if schema changes.
- `specifications/source-governance-and-catalog.md` — add AISstream source entry.
- `specifications/system-architecture.md` — document WebSocket transport path.
- `AGENTS.md` — note AISstream worker and decoder locations.
- `workflow/Completion.md` — fill evidence slots after each milestone.
