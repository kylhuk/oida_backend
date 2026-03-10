#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

mkdir -p "$backup_dir"

"$repo_root/infra/clickhouse/cluster/scripts/apply-cluster-schema.sh" >/dev/null
docker compose -f "$compose_file" exec -T clickhouse-01 sh -c "mkdir -p /var/lib/clickhouse/user_files/task-26 && rm -f /var/lib/clickhouse/user_files/task-26/*.parquet"
seed_fixture

run_query "INSERT INTO FUNCTION file('task-26/raw_document.parquet', 'Parquet') SELECT * FROM bronze.raw_document_all ORDER BY raw_id"
run_query "INSERT INTO FUNCTION file('task-26/fact_event.parquet', 'Parquet') SELECT * FROM silver.fact_event_all ORDER BY event_id"
run_query "INSERT INTO FUNCTION file('task-26/metric_snapshot.parquet', 'Parquet') SELECT * FROM gold.metric_snapshot_all ORDER BY snapshot_id"

printf 'backup_files\n'
ls -1 "$backup_dir"

truncate_fixture_tables

printf '\ncounts_after_truncate\n'
run_query "SELECT 'bronze.raw_document_all' AS table_name, count() AS rows FROM bronze.raw_document_all UNION ALL SELECT 'silver.fact_event_all' AS table_name, count() AS rows FROM silver.fact_event_all UNION ALL SELECT 'gold.metric_snapshot_all' AS table_name, count() AS rows FROM gold.metric_snapshot_all FORMAT TabSeparated"

run_query "INSERT INTO bronze.raw_document_all SELECT * FROM file('task-26/raw_document.parquet', 'Parquet')"
run_query "INSERT INTO silver.fact_event_all SELECT * FROM file('task-26/fact_event.parquet', 'Parquet')"
run_query "INSERT INTO gold.metric_snapshot_all SELECT * FROM file('task-26/metric_snapshot.parquet', 'Parquet')"

wait_for_replication

printf '\ncounts_after_restore\n'
run_query "SELECT 'bronze.raw_document_all' AS table_name, count() AS rows FROM bronze.raw_document_all UNION ALL SELECT 'silver.fact_event_all' AS table_name, count() AS rows FROM silver.fact_event_all UNION ALL SELECT 'gold.metric_snapshot_all' AS table_name, count() AS rows FROM gold.metric_snapshot_all FORMAT TabSeparated"

docker compose -f "$compose_file" stop clickhouse-03 clickhouse-04 >/dev/null

set +e
shard_error="$(curl -fsS --data-urlencode "query=SELECT count() FROM silver.fact_event_all FORMAT TabSeparated" "$cluster_http" 2>&1)"
shard_status=$?
set -e

printf '\nunavailable_shard_status\n'
printf '%s\n' "$shard_status"
printf '\nunavailable_shard_error\n'
printf '%s\n' "$shard_error"

docker compose -f "$compose_file" start clickhouse-03 clickhouse-04 >/dev/null
wait_for_http_port 39123
wait_for_http_port 49123
wait_for_replication
