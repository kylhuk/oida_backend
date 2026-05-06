# API And Auth

## Source of Truth

- Router and middleware: `cmd/api/main.go`
- Route inventory: `cmd/api/route_contracts.go`
- Resource specs and query behavior: `cmd/api/handlers.go`, `cmd/api/handlers_expanded.go`
- Auth: `cmd/api/auth.go`, `cmd/bootstrap/api_clients.go`, `seed/api_clients.json`, `migrations/clickhouse/0035_api_clients.sql`
- Metrics endpoint: `cmd/api/metrics.go`
- Generated docs: `docs/api-reference.md`, generated block in `README.md`
- API views: `migrations/clickhouse/gold_api_views.sql`, `migrations/clickhouse/0007_api_expansion_views.sql`, later view migrations

## Runtime Behavior

The API is read-only. Public routes are:

- `GET /v1/health`
- `GET /v1/ready`
- `GET /v1/version`
- `GET /v1/schema`

All other `/v1/*` routes require `X-API-Key` and a matching scope. Standard data scopes use `read:*`; internal operational routes use `read:internal`; `admin:*` satisfies all API scopes.

Handlers return an envelope with `api_version`, `schema_version`, `generated_at`, and `data` for successful responses. List/detail handlers query ClickHouse `gold.api_v1_*` views with `FORMAT JSONEachRow`, enforce allowlisted filters and field projections, and use opaque cursors for pagination.

The route inventory is shared by router registration, `/v1/schema`, contract tests, and generated API docs. Do not hand-edit route docs without updating the route contracts.

## Auth Details

API clients are stored as SHA-256 hashes in `meta.api_clients`. Bootstrap loads seed rows from `seed/api_clients.json`. Runtime auth accepts raw keys with the `oida_` prefix, hashes them, queries enabled clients, checks expiry, and verifies scopes.

The `/metrics` endpoint is separate from `/v1/*` API auth. It requires `Authorization: Bearer $METRICS_SHARED_KEY` when the shared key is configured.

## Operational Middleware

API middleware provides:

- CORS for configured origins.
- Request IDs and structured observability logs.
- Per-client rate limiting from `API_RATE_LIMIT_RPS` and `API_RATE_LIMIT_BURST`.
- Prometheus counters/metrics through `/metrics`.

## Deferred Or Catalog-Only Behavior

The API exposes current ClickHouse gold views. It does not expose write endpoints, browser-facing auth, OIDC/JWT integration, or a frontend BFF.

## Extension Knobs

- Add routes in `buildRouteSpecs()` and wire handlers through `routeHandlerForSpec()`.
- Add list/detail resources by creating a `resourceSpec` with a `gold.api_v1_*` view, id column, selectable fields, filters, and search columns.
- Add protected scope rules in `cmd/api/route_contracts.go`.
- Add or rotate API clients in `seed/api_clients.json` with hashed keys; never commit raw production keys.
- Regenerate/check API docs through `go test ./cmd/api -run 'TestGeneratedDocsAreCurrent|TestRouteContract'`.
