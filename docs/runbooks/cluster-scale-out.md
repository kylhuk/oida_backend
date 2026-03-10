# Cluster Scale-Out

## Purpose

Run the optional two-shard, two-replica ClickHouse topology without disturbing the default single-node baseline.

## Prerequisites

- Docker Compose is installed.
- The repository root is the current working directory.
- Ports `19123`, `29123`, `39123`, and `49123` are free.

## Steps

1. Bring up the Keeper quorum and the four ClickHouse nodes:
   ```sh
   docker compose -f docker-compose.cluster.yml up -d
   ```
2. Apply the cluster DDL:
   ```sh
   infra/clickhouse/cluster/scripts/apply-cluster-schema.sh
   ```
3. Run the cluster happy-path verification:
   ```sh
   infra/clickhouse/cluster/scripts/cluster-happy-path.sh
   ```
4. Run the backup/restore plus unavailable-shard drill:
   ```sh
   infra/clickhouse/cluster/scripts/cluster-edge-drill.sh
   ```

## Verification Notes

- `system.clusters` shows four ClickHouse members under `osint_cluster_2s2r`.
- `system.replicas` reports `active_replicas = total_replicas` for each `_local` table.
- Distributed reads and writes go through `_all` tables while `_local` tables remain the source of truth.
- `insert_distributed_sync=1` keeps routed writes synchronous during the drill.
- Backup files appear in `infra/clickhouse/cluster/backups/task-26/`.
- Full-shard loss returns an error because `skip_unavailable_shards=0` is intentional.

## Troubleshooting

- If a node never reaches `/ping`, inspect the matching mounted XML file under `infra/clickhouse/cluster/config/` or `infra/clickhouse/cluster/keeper/`.
- If replication stays pending, check `system.replicas` output from `cluster-happy-path.sh` before rerunning inserts.
- If the backup drill fails because Parquet files already exist, clear `infra/clickhouse/cluster/backups/task-26/` and rerun the drill.
