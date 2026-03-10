#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

sql_dir="$repo_root/infra/clickhouse/cluster/sql"

apply_sql_file() {
    local sql_file="$1"

    for _ in $(seq 1 30); do
        if curl -fsS --data-binary @"$sql_file" "$cluster_http" >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done

    curl -fsS --data-binary @"$sql_file" "$cluster_http" >/dev/null
}

start_cluster

for sql_file in "$sql_dir"/*.sql; do
    printf 'apply\t%s\n' "$(basename "$sql_file")"
    apply_sql_file "$sql_file"
done

wait_for_replication

run_query "SELECT database, name, engine FROM system.tables WHERE database IN ('meta', 'bronze', 'silver', 'gold') AND name LIKE '%_local' ORDER BY database, name FORMAT TabSeparated"
