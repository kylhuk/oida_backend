# INFRASTRUCTURE

## OVERVIEW
Runtime config, backup hooks, and optional ClickHouse cluster tooling. This subtree is operational support code, not application business logic.

## WHERE TO LOOK

| Task | Location | Notes |
|------|----------|-------|
| Default ClickHouse config | `infra/clickhouse/config/` | Single-node compose setup |
| Compose users | `infra/clickhouse/users/` | Service credentials wiring |
| Cluster topology | `infra/clickhouse/cluster/` | Keeper, shard, replica config |
| Backup hooks | `infra/backup/hooks/` | Pre/post backup and restore SQL |
| Backup manifests | `infra/backup/manifests/` | Asset inventory contracts |

## CONVENTIONS

- Keep single-node defaults under `infra/clickhouse/`; keep optional scale-out under `infra/clickhouse/cluster/`.
- Backup hook SQL should stay aligned with runbooks and bootstrap expectations.
- Cluster scripts are operator tooling and should remain explicit and auditable.

## GOTCHAS

- Cluster files are not active in the default compose stack unless the cluster compose path is selected.
- User / config XML changes can affect multiple services at once; treat them as shared infra contracts.
