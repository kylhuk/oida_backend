# Extension Playbooks

## Source of Truth

- Repository map: `AGENTS.md`, `README.md`
- Specifications index: `specifications/README.md`
- Validation gate: `scripts/verify.sh`
- Package-specific instructions: child `AGENTS.md` files under `cmd/`, `internal/`, `migrations/`, `docs/runbooks/`, and `test/`

## Runtime Behavior

Use these recipes for common changes. Keep runtime changes, tests, migrations, and docs in the same change. Preserve append-only migrations, ClickHouse HTTP-only access, scoped hashed API keys, and typed API filter fields.

## Add A Runtime HTTP Source

1. Add or update the catalog entry in `seed/source_catalog.json`.
2. Add or synthesize a runtime seed with entrypoints, allowed hosts, auth mode/config, `transport_type`, `crawl_enabled`, rate limits, retention class, parser id, bronze table, and promote profile.
3. Regenerate/validate compiled catalog behavior through bootstrap tests.
4. Add source-specific bronze DDL through the generated migration path or append-only migration.
5. Ensure parser and promotion profile support the emitted rows.
6. Run `./scripts/verify.sh`; run full verification when worker behavior or e2e source flow changes.

## Add A Parser

1. Implement `parser.Parser` in `internal/parser/`.
2. Register it in `parser.DefaultRegistry()`.
3. Add tests for descriptor records, parser resolution, happy path, and parse errors.
4. Add source governance fields that route to the parser.
5. If it emits new canonical fields, update `internal/canonical` and promotion tests.

## Add A Control-Plane Job

1. Create or update `cmd/control-plane/jobs_<topic>.go`.
2. Register the job in `init()` with a stable job name and description.
3. Use `currentJobOptions(ctx)` for shared CLI options.
4. Write `ops.job_run` evidence for non-trivial jobs.
5. Add tests for registration, CLI errors, success, failure, and SQL generation.

## Add Metrics Or A Domain Pack

1. Add shared metric definitions in `internal/metrics` or domain definitions in `internal/packs/<domain>`.
2. Emit deterministic `metrics.Contribution` rows.
3. Use `metrics.UpsertMaterializationSQL()` unless the storage model changes.
4. Insert or update `meta.metric_registry`.
5. Add fixture tests that assert metric ids, values, ranks, and evidence.

## Add An API Route Or Field

1. Add or alter the backing `gold.api_v1_*` view through an append-only migration.
2. Update the relevant `resourceSpec` or route spec in `cmd/api`.
3. Keep fields and filters allowlisted.
4. Add handler/contract tests.
5. Run `go test ./cmd/api -run 'TestGeneratedDocsAreCurrent|TestRouteContract'`.

## Add A Migration

1. Create the next ordered file under `migrations/clickhouse/`.
2. Use idempotent DDL.
3. Keep applied migrations immutable.
4. Record schema compatibility metadata when the migration touches contract-bearing surfaces.
5. Update bootstrap verification when the new table or column becomes required.

## Add Deployment Or Operations Behavior

1. Update `docker-compose.yml` and related `infra/` files.
2. Keep images pinned and Go services hardened.
3. Update `cmd/bootstrap/main.go` if runtime assets, buckets, RBAC, or readiness change.
4. Put procedural operator details in `docs/runbooks/`.
5. Run `docker compose config` and `./scripts/verify.sh`.

## Extension Knobs

- Prefer local package patterns before adding abstractions or dependencies.
- Add tests near changed behavior; use e2e only when the behavior crosses Compose services.
- Update the matching specification page when extension points move.
- Record verification evidence in `workflow/Completion.md` for non-trivial work.
