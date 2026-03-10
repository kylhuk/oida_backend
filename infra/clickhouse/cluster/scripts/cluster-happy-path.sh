#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

"$repo_root/infra/clickhouse/cluster/scripts/apply-cluster-schema.sh" >/dev/null
seed_fixture

printf 'cluster_members\n'
run_query "SELECT shard_num, replica_num, host_name FROM system.clusters WHERE cluster = '$cluster_name' ORDER BY shard_num, replica_num FORMAT TabSeparated"

printf '\nreplica_health\n'
run_query "SELECT hostName(), database, table, is_leader, is_readonly, is_session_expired, total_replicas, active_replicas, queue_size FROM clusterAllReplicas('$cluster_name', 'system', 'replicas') WHERE database IN ('meta', 'bronze', 'silver', 'gold') AND table IN ('source_registry_local', 'raw_document_local', 'fact_event_local', 'metric_state_local', 'metric_snapshot_local') ORDER BY hostName(), database, table FORMAT TabSeparated"

printf '\ndistributed_counts\n'
run_query "SELECT 'bronze.raw_document_all' AS table_name, count() AS rows FROM bronze.raw_document_all UNION ALL SELECT 'silver.fact_event_all' AS table_name, count() AS rows FROM silver.fact_event_all UNION ALL SELECT 'gold.metric_snapshot_all' AS table_name, count() AS rows FROM gold.metric_snapshot_all FORMAT TabSeparated"

printf '\nlocal_counts_by_replica\n'
run_query "SELECT hostName(), 'bronze.raw_document_local' AS table_name, count() AS rows FROM clusterAllReplicas('$cluster_name', 'bronze', 'raw_document_local') GROUP BY hostName() UNION ALL SELECT hostName(), 'silver.fact_event_local' AS table_name, count() AS rows FROM clusterAllReplicas('$cluster_name', 'silver', 'fact_event_local') GROUP BY hostName() UNION ALL SELECT hostName(), 'gold.metric_snapshot_local' AS table_name, count() AS rows FROM clusterAllReplicas('$cluster_name', 'gold', 'metric_snapshot_local') GROUP BY hostName() ORDER BY table_name, hostName() FORMAT TabSeparated"

printf '\ncluster_settings\n'
run_query "SELECT name, value FROM system.settings WHERE name IN ('insert_distributed_sync', 'load_balancing', 'skip_unavailable_shards') ORDER BY name FORMAT TabSeparated"

printf '\ncluster_cost_controls\n'
run_query "SELECT database, name, engine, if(positionCaseInsensitive(create_table_query, 'TTL') > 0, 'ttl', 'missing') AS ttl FROM system.tables WHERE database IN ('bronze', 'silver', 'gold') AND name IN ('raw_document_local', 'fact_event_local', 'metric_state_local', 'metric_snapshot_local') ORDER BY database, name FORMAT TabSeparated"

printf '\nprojection_count\n'
run_query "SELECT count() FROM system.tables WHERE database IN ('bronze', 'silver', 'gold') AND name IN ('raw_document_local', 'fact_event_local', 'metric_state_local', 'metric_snapshot_local') AND positionCaseInsensitive(create_table_query, 'PROJECTION') > 0 FORMAT TabSeparated"

printf '\nmaterialized_view_count\n'
run_query "SELECT count() FROM system.tables WHERE database IN ('meta', 'bronze', 'silver', 'gold') AND engine = 'MaterializedView' FORMAT TabSeparated"
