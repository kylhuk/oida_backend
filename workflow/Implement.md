# Implement

## Runbook

1. Read the current workflow files and the repository entry points before editing.
2. Use current code, migrations, seed files, and existing docs as the source of truth.
3. Keep the change documentation-only.
4. Create `specifications/` pages that tell future agents what exists, where it lives, and which extension points to use.
5. On every page, include:
   - `Source of Truth`
   - `Runtime Behavior`
   - `Extension Knobs`
6. Label catalog-only or deferred capability explicitly.
7. Update root `README.md` and root `AGENTS.md` to point to `specifications/README.md`.
8. Run the validation commands.
9. Record exact evidence in `workflow/Completion.md`.

## Validation Commands

Run the sentinel-word scan over `specifications` and `workflow`, then run:

```bash
docker compose config
./scripts/verify.sh
```
