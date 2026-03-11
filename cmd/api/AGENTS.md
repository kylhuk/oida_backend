# API SERVICE

## OVERVIEW
HTTP read surface over ClickHouse `gold.api_v1_*` views. Contracts live here, not in handlers spread across `internal/`.

## WHERE TO LOOK

| Task | Location | Notes |
|------|----------|-------|
| Wire route | `cmd/api/main.go` | Add route in `newAPIMuxWithServer()` |
| Change list/detail semantics | `cmd/api/handlers.go` | Most behavior is resource-spec driven |
| Expand API coverage/views | `cmd/api/handlers_expanded.go` | Extended handlers and query helpers |
| Validate contract behavior | `cmd/api/contract_test.go` | API-facing expectations |
| Validate mux/handler behavior | `cmd/api/handlers_test.go`, `cmd/api/main_test.go` | Fast package tests |

## CONVENTIONS

- Prefer extending `resourceSpec` over adding one-off handlers.
- List endpoints use `buildListQuery()` + `parseListOptions()`; keep filters whitelisted.
- Detail endpoints use `buildDetailQuery()` and return `not_found` envelopes instead of raw errors.
- Use `respond()` / `respondError()` for every response path.
- Query ClickHouse with `FORMAT JSONEachRow`; decode rows with `decodeJSONEachRow()`.
- JSON-ish columns such as `attrs`, `evidence`, `entrypoints`, and `parent_place_chain` are normalized in `normalizeRow()`.

## GOTCHAS

- `fields=` is allowlisted per resource; forgetting to add a field to `selectFields` makes it inaccessible even if the view has the column.
- Cursor pagination depends on ascending `idColumn`; changing sort semantics breaks `next_cursor`.
- Search uses `positionCaseInsensitiveUTF8(toString(column), ...)`; only add searchable columns that are safe to stringify.
- API tests assume the envelope shape from root conventions; changing response wrappers has wide fallout.

## COMMANDS

```bash
go test ./cmd/api/...
go run ./cmd/api
```

## ANTI-PATTERNS

- Do not query raw `silver` / `bronze` tables from handlers; stay on API-facing `gold.api_v1_*` views.
- Do not accept arbitrary query params; update `rejectUnsupportedQueryParams()` inputs.
- Do not bypass `resourceSpec` just to add another list/detail endpoint unless the route truly breaks the pattern.
