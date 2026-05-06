# Backup and Restore

## Purpose

Capture and restore the single-node ClickHouse application databases plus the bootstrap assets in MinIO. The control-plane jobs use ClickHouse native `BACKUP` and `RESTORE` to an S3-compatible MinIO URL.

## Preconditions

- `clickhouse` and `minio` are online.
- `bootstrap verify` passes before the backup.
- `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, and `BACKUP_BUCKET` are configured for the control-plane container.

## Capture a Backup

1. Confirm the stack is healthy:
   ```sh
   docker compose run --rm bootstrap verify
   curl -fsS http://localhost:8080/v1/ready
   ```
2. Run the native ClickHouse backup job:
   ```sh
   docker compose run --rm control-plane run-once --job backup-clickhouse
   ```
3. Record the emitted backup URL from the `ops.job_run` record. By default it is under:
   ```text
   http://minio:9000/backup/clickhouse/<backup-name>
   ```
4. Copy `s3://backup/bootstrap/` and `s3://backup/clickhouse/<backup-name>` to durable external storage if this is more than a local drill.

## Restore Procedure

1. Make the backup path available in the `backup` MinIO bucket.
2. Run restore with an explicit URL:
   ```sh
   CLICKHOUSE_RESTORE_URL=http://minio:9000/backup/clickhouse/<backup-name> \
     docker compose run --rm control-plane run-once --job restore-clickhouse
   ```
3. Re-run bootstrap verification:
   ```sh
   docker compose run --rm bootstrap verify
   ```
4. Confirm API readiness:
   ```sh
   curl -fsS http://localhost:8080/v1/ready
   ```

## Verification

- `ops.job_run` contains a `backup-clickhouse` or `restore-clickhouse` row with `status='success'`.
- `meta.schema_migrations` shows the highest applied migration version and matching checksum set expected for the restored release.
- `meta.api_clients` contains at least one enabled scoped API client.
- `/v1/ready` returns `true`.

## Notes

- Restore intentionally requires `CLICKHOUSE_RESTORE_URL`; there is no implicit latest backup.
- The restore job uses `allow_non_empty_tables = 0` so accidental restores into populated tables fail instead of silently mixing data.
- Bootstrap assets remain separate from native ClickHouse backups. Keep both the ClickHouse backup path and `s3://backup/bootstrap/`.
