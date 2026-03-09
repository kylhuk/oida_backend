# Global OSINT Backend (Scaffold)

This repository contains a Phase-A execution scaffold for a Go-first, ClickHouse-first backend.

## Included
- Docker Compose topology with required baseline services.
- Go entrypoints for `api`, `bootstrap`, `control-plane`, `worker-fetch`, `worker-parse`, `renderer`.
- Initial ClickHouse migration (`meta.schema_migrations` + logical DB zones).
- Seed source registry artifact.

## Quick start
```bash
docker compose up -d --build
```

API endpoints:
- `GET /v1/health`
- `GET /v1/ready`
- `GET /v1/version`
- `GET /v1/schema`


## Current execution status
- Phase A scaffold implemented.
- Bootstrap now applies ordered SQL migrations to ClickHouse over HTTP and records checksums in `meta.schema_migrations`.
- API exposes v1 core discovery endpoints as contract-shaped stubs.

- Added migrations through `0003_ops_bronze.sql` (meta source registry, ops job/frontier/fetch, bronze raw docs).
- Bootstrap now also seeds `meta.source_registry` idempotently from `seed/source_registry.json`.
