# METRICS CORE

## OVERVIEW
Shared metrics registry, contribution, rollup, and runtime-output plumbing used across domain packs.

## WHERE TO LOOK

| Task | Location | Notes |
|------|----------|-------|
| Metric registry records | `internal/metrics/registry.go` | Shared registry shapes |
| Contribution math | `internal/metrics/contribution.go` | Windowing and contribution helpers |
| Materialization SQL | `internal/metrics/materialization_sql.go` | Writes `silver` / `gold` state |
| Runtime analytics outputs | `internal/metrics/runtime_outputs.go` | Refresh / output SQL |
| Rollups | `internal/metrics/rollup.go` | Shared aggregation behavior |

## CONVENTIONS

- Packs feed contributions into shared materialization instead of hand-writing `gold` rollups independently.
- Registry metadata carries explainability and cadence details, not just IDs and units.
- Sort IDs and keys deterministically before SQL generation.

## GOTCHAS

- `UpsertMaterializationSQL()` deletes and rebuilds related materialization keys together; partial edits can break consistency.
- Window semantics (`day`, rolling windows) live in code and are reused broadly.

## COMMANDS

```bash
go test ./internal/metrics/...
```
