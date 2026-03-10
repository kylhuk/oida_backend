# Fresh Bootstrap

## Purpose

Start the system from zero so the Go services, ClickHouse schema, and MinIO buckets are all created in a repeatable way. This walk-through covers the default install-time path that `docker compose up` and `docker compose run --rm bootstrap` follow in Phase A.

## Prerequisites

- Docker Compose is installed and points at the repository root.
- No leftover Compose volumes remain from previous runs (`docker compose down --volumes` is a good safeguard).

## Steps

1. Bring up the base platform that the bootstrap service depends on:
   ```sh
   docker compose -f docker-compose.yml up -d clickhouse minio
   ```
   Wait 10 seconds for ClickHouse to accept HTTP connections.
2. Run the bootstrap command that applies migrations, seeds the source registry, and registers backup hooks:
   ```sh
   docker compose run --rm bootstrap
   ```
3. Verify that buckets, roles, migrations, and seeds exist:
   ```sh
   docker compose run --rm bootstrap verify
   ```
4. Confirm the HTTP API is healthy and marks itself ready only after the bootstrap ready marker exists:
   ```sh
   curl -fsS http://localhost:8080/v1/health
   curl -fsS http://localhost:8080/v1/ready
   ```
5. Query ClickHouse to ensure `meta.schema_migrations` and `meta.source_registry` hold the expected rows:
   ```sh
   curl -fsS "http://localhost:8123/?query=SELECT%20count()%20FROM%20meta.schema_migrations%20FORMAT%20TabSeparated"
   curl -fsS "http://localhost:8123/?query=SELECT%20count()%20FROM%20meta.source_registry%20FORMAT%20TabSeparated"
   ```

## Verification Notes

- Both `docker compose run --rm bootstrap` invocations exit with `0`.
- `meta.schema_migrations` reports at least one row and `meta.source_registry` is non-zero.
- The API health endpoint returns a JSON payload, and `/v1/ready` flips to `true` only after the ready file is created.

## Troubleshooting

- If buckets already exist, `bootstrap` will log that fact and continue; rerun `bootstrap verify` to inspect the seeds instead of deleting buckets.
- If ClickHouse rejects a migration, inspect `cmd/bootstrap` logs and the `meta.schema_migrations` table for the last applied checksum.
- Missing source rows usually point to JSON seed parsing errors; check `seed/source_registry.json` contents and rerun `bootstrap` after correcting the file.
