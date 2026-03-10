# Upgrade and Migration

## Purpose

Apply new migrations against an existing ClickHouse deployment while keeping the seed data, backups, and schema ledger intact. This runbook walks through the steps to exercise both the fresh path and the upgrade path so we can ship migration-only releases safely.

## Prerequisites

- A running ClickHouse and MinIO pair from a previous bootstrap.
- The `meta.schema_migrations` table filled with the prior migration history.
- An up-to-date `seed/source_registry.json` and any new migration files committed to `migrations/clickhouse`.

## Steps

1. Start the base services if they are not already running so the bootstrap command can reach ClickHouse and MinIO:
   ```sh
   docker compose -f docker-compose.yml up -d clickhouse minio
   ```
2. Take a backup manifest by listing the objects under `s3://backup/bootstrap/` (use `aws s3 ls` or `mc` against MinIO) so you can compare after the upgrade.
3. Run the bootstrap tool to apply the new migration set and record the checksum:
   ```sh
   docker compose run --rm bootstrap
   ```
4. Run the verification mode to ensure the new migration files and seeds ended up in the expected places:
   ```sh
   docker compose run --rm bootstrap verify
   ```
5. Confirm that `meta.schema_migrations` length increased by the number of new SQL files and that the newest row matches the newest migration checksum:
   ```sh
   curl -fsS "http://localhost:8123/?query=SELECT%20id%2C%20checksum%20FROM%20meta.schema_migrations%20ORDER%20BY%20id%20DESC%20LIMIT%205%20FORMAT%20TabSeparated"
   ```
6. Inspect `meta.source_registry` to ensure seeds still map to the current governance model, and check that the ready marker still exists so `/v1/ready` stays true.

## Verification

- New migration entries appear in `meta.schema_migrations` with the right `id` ordering and `checksum` values.
- `bootstrap verify` exits successfully and logs that buckets, roles, and manifests are still registered.
- The API readiness endpoint stays `true` because the marker file persists through the upgrade.

## Rollback / Mitigation

- If a migration fails, drop back to the previous `meta.schema_migrations` checkpoint and rerun with a subset of migrations. Keep the previous backup manifest handy to re-import if schema state is inconsistent.
- For manual rollbacks, restore the `backup` bucket from the manifest path indicated in `.sisyphus/evidence` and re-run `bootstrap` in restore mode (see the backup/restore runbook).
