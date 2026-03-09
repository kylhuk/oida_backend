# PROJECT KNOWLEDGE BASE

**Generated:** 2025-03-09
**Commit:** dc4e7d4
**Branch:** main

## OVERVIEW
Global OSINT Backend - Phase A scaffold. Go 1.23 + ClickHouse-first architecture. Multi-service with 6 binaries.

## STRUCTURE
```
./
├── cmd/                    # Service entrypoints (6 binaries)
│   ├── api/                # HTTP API (port 8080)
│   ├── bootstrap/          # ClickHouse migrations + seeding
│   ├── control-plane/      # Orchestrator stub
│   ├── renderer/           # Report renderer stub (port 8090)
│   ├── worker-fetch/       # Crawler worker stub
│   └── worker-parse/       # Parser worker stub
├── internal/
│   └── migrate/            # ClickHouse HTTP migration runner
├── migrations/clickhouse/  # Ordered SQL migrations (0001_init.sql...)
├── build/                  # Multi-stage Dockerfiles
├── seed/                   # JSON seed data (source_registry.json)
└── docker-compose.yml      # Full stack orchestration
```

## WHERE TO LOOK

| Task | Location | Notes |
|------|----------|-------|
| Add API endpoint | `cmd/api/main.go` | Use `listStub()` pattern for v1 stubs |
| Add migration | `migrations/clickhouse/` | Named `000N_description.sql` |
| Migration logic | `internal/migrate/http_runner.go` | HTTPRunner for ClickHouse over HTTP |
| Service entrypoint | `cmd/{name}/main.go` | Standard `package main` |
| Seed data | `seed/source_registry.json` | Loaded by bootstrap idempotently |
| Docker build | `build/*.Dockerfile` | Multi-stage: golang:1.23 → distroless |

## CODE MAP

| Symbol | Type | Location | Role |
|--------|------|----------|------|
| `respond()` | func | `cmd/api/main.go:54` | JSON envelope wrapper |
| `listStub()` | func | `cmd/api/main.go:48` | Stub handler factory |
| `HTTPRunner` | struct | `internal/migrate/http_runner.go:12` | ClickHouse HTTP client |
| `ApplySQL()` | method | `internal/migrate/http_runner.go:55` | Execute migration statements |
| `SplitStatements()` | func | `internal/migrate/split.go:5` | Split SQL on `;` |
| `applyMigrations()` | func | `cmd/bootstrap/main.go:65` | Migration orchestration |
| `loadSourceSeed()` | func | `cmd/bootstrap/main.go:99` | JSON seed → ClickHouse |

## CONVENTIONS

**Go Patterns:**
- `getenv(k, d string)` helper for env vars with defaults
- `envelope map[string]any` for JSON responses
- Tests named `{Func}_test.go` alongside source

**API Response Format:**
```go
{
  "api_version": "v1",
  "schema_version": 1,
  "generated_at": "2025-01-01T00:00:00Z",
  "data": {...}
}
```

**Migrations:**
- Sequential naming: `0001_init.sql`, `0002_core_tables.sql`
- Checksums recorded in `meta.schema_migrations`
- Idempotent: `CREATE ... IF NOT EXISTS`

**ClickHouse Schema:**
- 5 logical DBs: `meta`, `ops`, `bronze`, `silver`, `gold`
- Partitioning: `toYYYYMM(timestamp)` for time-series tables
- Engines: `MergeTree`, `ReplacingMergeTree(version)`

## ANTI-PATTERNS (THIS PROJECT)

**DO NOT:**
- Add deps to `go.mod` without explicit need (keep minimal)
- Use CGO (disabled in Dockerfiles)
- Run migrations without bootstrap dependency (see docker-compose)
- Use ClickHouse native protocol (use HTTP 8123 only)
- Store secrets in code (use env vars)

## COMMANDS

```bash
# Start everything
docker compose up -d --build

# Check API health
curl http://localhost:8080/v1/health

# Run tests
go test ./...

# Build single service
CGO_ENABLED=0 go build -o out/api ./cmd/api
```

## NOTES

- **Bootstrap dependency:** All services wait for bootstrap completion
- **ClickHouse HTTP:** All DB interaction via port 8123, not 9000
- **Workers:** Currently stubs that log and sleep
- **Phase A:** API endpoints return stubs; real implementation in Phase B
- **MinIO:** Available at :9001 (console) and :9002 (API), unused in Phase A
