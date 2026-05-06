# Plan

## Milestones

1. Workflow specification refresh
   - Rewrite `workflow/Prompt.md` as the documentation task specification.
   - Rewrite `workflow/Plan.md` with documentation milestones, acceptance criteria, and validation commands.
   - Rewrite `workflow/Implement.md` as the runbook for this task.
   - Reset `workflow/Completion.md` as an evidence ledger for the specifications deliverable.
   - Record the context shift in `workflow/Documentation.md`.
   - Status: complete.

2. System specifications folder
   - Add `specifications/README.md` as the index, reading order, and "where do I change X?" map.
   - Add focused specification pages for architecture, lifecycle, source governance, schema contracts, parsing/promotion, packs/metrics, orchestration, API/auth, operations, and extension playbooks.
   - Ensure each page includes `Source of Truth` and `Extension Knobs`.
   - Status: complete.

3. Entry point links
   - Update root `README.md` documentation links.
   - Update root `AGENTS.md` so future agents start at `specifications/README.md` for system specification context.
   - Status: complete.

4. Verification and evidence
   - Run sentinel-word scan over `specifications` and `workflow`.
   - Run `docker compose config`.
   - Run `./scripts/verify.sh`.
   - Update `workflow/Completion.md` with exact commands, changed files, and outcomes.
   - Status: complete.

## Acceptance Criteria

- Future agents can answer where to change sources, parsers, promotion, packs, jobs, schemas, API routes, auth, metrics, deployment, and verification from `specifications/` alone.
- Documentation matches current code, seed files, migrations, Compose topology, and existing docs.
- Runtime-backed behavior is clearly distinguished from deferred/catalog-only capability.
- Every specification page has `Source of Truth` and `Extension Knobs`.
- Sentinel-word scan over `specifications` and `workflow` returns no matches.
- `docker compose config` exits 0.
- `./scripts/verify.sh` exits 0.

## Validation Commands

Run the sentinel-word scan over `specifications` and `workflow`, then run:

```bash
docker compose config
./scripts/verify.sh
```
