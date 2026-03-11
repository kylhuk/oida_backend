# DISCOVERY

## OVERVIEW
URL discovery and crawl eligibility logic: frontier management, feeds, sitemaps, and robots semantics.

## WHERE TO LOOK

| Task | Location | Notes |
|------|----------|-------|
| Frontier behavior | `internal/discovery/frontier.go` | Discovery queue semantics |
| Robots logic | `internal/discovery/robots.go` | Policy parsing and path matching |
| Sitemap ingestion | `internal/discovery/sitemap.go` | XML sitemap handling |
| Feed parsing | `internal/discovery/feed.go` | Feed-derived discovery |

## CONVENTIONS

- Keep discovery output deterministic; tests rely on stable ordering and normalized URLs.
- Robots parsing is explicit and local, not delegated to an external library.
- Discovery helpers should preserve source-policy semantics rather than hardcoding crawler shortcuts.

## GOTCHAS

- `robots.go` contains pattern-specific matching behavior; small changes can widen crawl scope unintentionally.
- Sitemap and feed parsing both need tolerant parsing plus normalized output.

## COMMANDS

```bash
go test ./internal/discovery/...
```
