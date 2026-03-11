# CLICKHOUSE MIGRATIONS

## OVERVIEW
Ordered schema history for the single-node stack. Bootstrap applies these files through `internal/migrate.HTTPRunner` and records checksums in `meta.schema_migrations`.

## WHERE TO LOOK

| Task | Location | Notes |
|------|----------|-------|
| Add migration | `migrations/clickhouse/000N_name.sql` | Preserve numeric ordering |
| Check frozen schema rules | `docs/schema-standards.md` | Update tests when rules change |
| Runtime application | `cmd/bootstrap/main.go` | `applyMigrations()` drives rollout |

## CONVENTIONS

- Files are append-only and numerically ordered.
- DDL should be idempotent where practical: prefer `IF NOT EXISTS` / compatible guards.
- The repo standard is lowercase snake_case, `DateTime64(3, 'UTC')`, typed hot fields, and `attrs` / `evidence` for extensibility.
- Registry and contract-bearing tables use `schema_version`, `record_version`, `api_contract_version`, and `updated_at`.

## GOTCHAS

- Applied migrations are checksum-locked; editing an old file breaks bootstrap verification.
- `meta.source_registry` retains a legacy `version` column for compatibility; new designs should still prefer `record_version`.
- Some operational / API behaviors are view-backed, so migration changes may require API or E2E test updates.

## COMMANDS

```bash
go test ./internal/migrate/...
docker compose run --rm bootstrap verify
```

## ANTI-PATTERNS

- Do not reorder existing migration files.
- Do not hide filterable columns inside JSON blobs.
- Do not introduce schema exceptions without updating `docs/schema-standards.md` and related tests.
