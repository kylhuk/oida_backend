# PROMOTE PIPELINE

## OVERVIEW
Pure planning layer that converts canonical stage inputs into deterministic `silver` / `ops` insert SQL.

## WHERE TO LOOK

| Task | Location | Notes |
|------|----------|-------|
| Plan building | `internal/promote/pipeline.go` | `Prepare()` and row builders |
| Contract tests | `internal/promote/pipeline_test.go` | Pin SQL and row semantics |

## CONVENTIONS

- `Prepare()` validates and normalizes input before any row generation.
- Resolved records go to observation / event / entity rows; unresolved records go to `ops.unresolved_location_queue`.
- Stable IDs are SHA-256 based and derived from business identity, not random UUIDs.
- `SQLStatements()` is pure assembly; DB execution happens elsewhere.
- Evidence and attrs are merged from discovery, fetch, parse, and location stages for explainability.

## GOTCHAS

- `LocationResolution` below confidence threshold is treated as unresolved even if `Resolved=true`.
- SQL builders deliberately left-join existing IDs to stay idempotent.
- This package duplicates some low-level SQL literal helpers on purpose to keep pipeline generation self-contained.

## COMMANDS

```bash
go test ./internal/promote/...
```

## ANTI-PATTERNS

- Do not add database calls here; keep this package deterministic and testable.
- Do not weaken input validation to "best effort" parsing; downstream tables assume normalized inputs.
