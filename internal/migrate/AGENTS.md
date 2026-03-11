# MIGRATION RUNTIME

## OVERVIEW
Small but critical HTTP-only ClickHouse migration layer. Owns schema ledger invariants and SQL statement splitting.

## WHERE TO LOOK

| Task | Location | Notes |
|------|----------|-------|
| HTTP execution | `internal/migrate/http_runner.go` | `Query()`, `ApplySQL()`, ledger helpers |
| SQL splitting | `internal/migrate/split.go` | Statement boundaries for migration batches |
| Standards tests | `internal/migrate/schema_standards_test.go` | Frozen schema convention checks |

## CONVENTIONS

- Use `HTTPRunner`; app code does not speak ClickHouse native protocol.
- `EnsureMigrationsTable()` and `VerifyMigrationsTableContract()` protect the bootstrap-owned ledger.
- Applied migration checks are checksum-based; files are immutable after rollout.
- `ApplySQL()` executes `SplitStatements()` output one statement at a time.

## GOTCHAS

- `validateAppliedMigrationChecksum()` errors on conflicting or changed checksums, not just missing rows.
- `Query()` uses HTTP POST with `?query=` and returns raw body text; callers own parsing.
- `split.go` behavior affects every multi-statement migration, so parser tweaks are high-risk.

## COMMANDS

```bash
go test ./internal/migrate/...
```

## ANTI-PATTERNS

- Do not silently ignore checksum drift.
- Do not bypass ledger recording for applied migrations.
- Do not add ClickHouse client dependencies when HTTP is sufficient.
