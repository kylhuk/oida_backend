# DOMAIN PACKS

## OVERVIEW
Domain-specific ingest and analytics modules for aviation, geopolitical, maritime, safety, and space. Shared pattern: normalize source records, emit entities/facts, register metrics, and produce materialization inputs.

## WHERE TO LOOK

| Task | Location | Notes |
|------|----------|-------|
| Aviation analytics | `internal/packs/aviation/` | Track reconstruction, military likelihood, airport interactions |
| Geopolitical ingest | `internal/packs/geopolitical/` | Multi-source event/entity plan with optional credentials |
| Maritime metrics | `internal/packs/maritime/` | Vessel signals, registry records, metric definitions |
| Safety ingest | `internal/packs/safety/` | Entity + observation plans and metric contributions |
| Space analysis | `internal/packs/space/` | Orbital propagation, overpass windows, conjunction metrics |

## SHARED CONVENTIONS

- Stable `SourceID` strings are part of the contract; control-plane jobs and tests reference them directly.
- Packs usually return a plan/result plus SQL or metric rows rather than performing side effects themselves.
- Metric registry records and metric contributions are produced beside facts so explainability ships with the data.
- Sort output deterministically before returning; tests expect stable ordering.

## PACK NOTES

- `aviation/` is analysis-heavy: long-running metric derivation from state vectors and registries.
- `geopolitical/` and `safety/` are ingest-plan builders with `ExecutedSources` / disabled-source reporting.
- `maritime/` keeps metric definitions explicit and explainable; formulas live in code.
- `space/` is geometry/orbit heavy and depends on `internal/location` types for distance work.

## COMMANDS

```bash
go test ./internal/packs/...
go test ./internal/packs/aviation/...
go test ./internal/packs/geopolitical/...
```

## ANTI-PATTERNS

- Do not mix pack-specific semantics into shared root docs; keep domain quirks here.
- Do not add non-deterministic ordering to returned slices.
- Do not invent metric IDs ad hoc; keep them explicit, documented, and stable.
