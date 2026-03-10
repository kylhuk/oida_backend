# Backup and Restore

## Purpose

Document how to snapshot the bootstrap assets that live in the `backup` MinIO bucket and how to rehydrate them if a ClickHouse or MinIO volume is lost.

## Preconditions

- ClickHouse and MinIO are online so `bootstrap` can reach them.
- You have the MinIO credentials from `docker-compose.yml` (`MINIO_ROOT_USER` and `MINIO_ROOT_PASSWORD`).

## Steps to Capture a Backup

1. Run the bootstrap service so it re-registers the hooks and manifest files:
   ```sh
   docker compose run --rm bootstrap
   ```
   This uploads `hooks/*.sql` and `manifests/*.json` under `s3://backup/bootstrap/`.
2. Copy the contents of that path to a durable external location. For example, use the AWS CLI against the MinIO endpoint:
   ```sh
   AWS_ACCESS_KEY_ID=minio AWS_SECRET_ACCESS_KEY=minio_change_me aws --endpoint-url http://localhost:9000 s3 sync s3://backup/bootstrap ./artifacts/bootstrap-backup
   ```
3. Record the manifest file names and checksums so you can match them later. The `backup` bucket always contains the `backup-manifest-v1.json` and `restore-manifest-v1.json` files.

## Restore Procedure

1. Seed the `backup` bucket from whichever archive you stored in `./artifacts/bootstrap-backup`:
   ```sh
   AWS_ACCESS_KEY_ID=minio AWS_SECRET_ACCESS_KEY=minio_change_me aws --endpoint-url http://localhost:9000 s3 sync ./artifacts/bootstrap-backup s3://backup/bootstrap
   ```
2. Run `docker compose run --rm bootstrap verify` to make sure the assets are present and the metadata tables still match the registered files:
   ```sh
   docker compose run --rm bootstrap verify
   ```
3. After restore verification, run the usual bootstrap install path if you need to reapply migrations:
   ```sh
   docker compose run --rm bootstrap
   ```

## Verification

- After the restore step, `backup-manifest-v1.json` and `restore-manifest-v1.json` appear again under `s3://backup/bootstrap/manifests/`.
- The `meta.schema_migrations` table shows the same highest migration ID as before the incident.
- `/v1/ready` returns `true` because the ready marker file was rewritten.

## Notes

- The `restore` manifest is a pointer, not a full dataset. Always keep the SQL hooks (`hooks/*.sql`) and the `seed` files that `bootstrap` depends on.
- If you need to refresh credentials, update the environment variables in the Compose file and rerun `bootstrap` so the new values are uploaded to the `backup` bucket.
