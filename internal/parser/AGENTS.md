# PARSERS

## OVERVIEW
Structured parsing layer for JSON, CSV, XML/HTML, and parser registry composition.

## WHERE TO LOOK

| Task | Location | Notes |
|------|----------|-------|
| Parser registration | `internal/parser/registry.go` | Registry and parser selection |
| JSON parsing | `internal/parser/json.go` | Structured JSON candidates |
| CSV parsing | `internal/parser/csv.go` | Delimited payload extraction |
| XML / HTML parsing | `internal/parser/xml.go` | Tree model, selectors, extraction |

## CONVENTIONS

- Parser packages return structured candidates plus evidence rather than mutating downstream rows directly.
- Registry composition happens through `NewRegistry(...)` and explicit parser registration.
- Parsing errors are typed and carry machine-readable codes where possible.

## GOTCHAS

- `xml.go` supports both XML and relaxed HTML tokenization; selector behavior lives here, not in an external DOM layer.
- Registry ordering and format matching affect which parser wins for a payload.

## COMMANDS

```bash
go test ./internal/parser/...
```
