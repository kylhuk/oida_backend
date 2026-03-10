# ClickHouse Cluster Mode

## Purpose

This directory holds the optional scale-out topology for Task 26. The default single-node deployment stays on `docker-compose.yml`; cluster mode is activated only with `docker compose -f docker-compose.cluster.yml ...`.

## Topology

- Three ClickHouse Keeper nodes for quorum.
- Four ClickHouse servers arranged as two shards with two replicas each.
- `_local` tables use `Replicated*MergeTree` engines and remain the source of truth.
- `_all` tables use `Distributed` only for fan-out reads and routed writes.

## Cost Controls

- `bronze.raw_document_local` keeps a 180 day TTL for raw retention.
- `silver.fact_event_local` keeps a 1095 day TTL for long-lived fact retention.
- `gold.metric_state_local` keeps a 730 day TTL for aggregate state.
- `gold.metric_snapshot_local` keeps a 365 day TTL for read-optimized snapshots.
- No projections ship in the optional profile until query plans justify their cost.
- No `MaterializedView` objects ship in the optional profile; `metric_state_local` is written explicitly to keep MV spend at zero.
- `insert_distributed_sync=1` keeps distributed writes synchronous.
- `skip_unavailable_shards=0` keeps reads loud on full-shard loss instead of silently returning partial data.

## Backup and Restore Drill

- Cluster data is exported from `clickhouse-01` through the distributed tables into `infra/clickhouse/cluster/backups/task-26/`.
- Restore reads the same Parquet backups back through the distributed tables so shard routing still happens on replay.
- Schema recreation stays in repo under `infra/clickhouse/cluster/sql/`; data backup is intentionally separate from DDL.

## Scripts

- `infra/clickhouse/cluster/scripts/apply-cluster-schema.sh` waits for HTTP health and applies the cluster DDL.
- `infra/clickhouse/cluster/scripts/cluster-happy-path.sh` starts the topology, seeds fixture writes through distributed tables, and reports Keeper/replica health.
- `infra/clickhouse/cluster/scripts/cluster-edge-drill.sh` runs the backup/restore drill and proves full-shard loss fails loudly.
