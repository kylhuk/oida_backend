# RUNBOOKS

## OVERVIEW
Operational procedures for bootstrap, migration, backup/restore, kill-switches, unresolved triage, and cluster scale-out.

## WHERE TO LOOK

| Task | Location | Notes |
|------|----------|-------|
| Fresh install path | `docs/runbooks/fresh-bootstrap.md` | Single-node bootstrap steps |
| Migration upgrades | `docs/runbooks/upgrade-migration.md` | Upgrade and verify flow |
| Backup / restore | `docs/runbooks/backup-restore.md` | Asset and metadata recovery |
| Emergency source disable | `docs/runbooks/kill-switch.md` | Control-plane source shutdown |
| Unresolved queue operations | `docs/runbooks/unresolved-triage.md` | Triage and remediation workflow |
| Cluster expansion | `docs/runbooks/cluster-scale-out.md` | Optional scale-out topology |

## CONVENTIONS

- Runbooks should mirror actual commands in the repo and compose files.
- Include verification steps, not just action steps.
- Prefer referencing concrete binaries / commands over vague operator prose.

## ANTI-PATTERNS

- Do not document workflows that are no longer backed by the current code or compose files.
- Do not omit rollback or verification guidance for destructive operations.
