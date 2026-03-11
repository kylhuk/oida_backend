# PLACE GRAPH

## OVERVIEW
Fixture-driven place graph materialization, polygon validation, lineage building, and reverse-geocode support.

## WHERE TO LOOK

| Task | Location | Notes |
|------|----------|-------|
| Build graph bundle | `internal/place/materialize.go` | Places, hierarchies, polygons, reverse fixtures |
| Package contract notes | `internal/place/doc.go` | Place package intent |
| Validate geometry + lineage | `internal/place/materialize_test.go`, `internal/place/coverage_test.go`, `internal/place/fixtures_test.go` | Tests encode expected semantics |
| Consume in job | `cmd/control-plane/jobs_place_build.go` | Writes bundle into ClickHouse |

## CONVENTIONS

- `BuildBundle()` is deterministic from in-package fixtures plus the provided clock.
- Invalid polygons are filtered before bundle output; tests rely on that behavior.
- `Attrs` carry lineage and coverage side data such as `parent_chain_place_ids` and `h3_coverage_res7`.
- Reverse-geocode verification uses named fixtures like `Paris France` and `Paris Texas`.

## GOTCHAS

- `ApplyH3Coverage()` is a second step; `BuildBundle()` alone does not populate H3 cells.
- World / continent nodes are pseudo places and intentionally materialized.
- Geometry validity checks reject short, open, zero-area, or self-intersecting rings.

## COMMANDS

```bash
go test ./internal/place/...
```

## ANTI-PATTERNS

- Do not treat fixture geometry as disposable test data; production-like reverse-geocode behavior depends on it.
- Do not change place IDs casually; multiple tests and control-plane jobs key off them.
