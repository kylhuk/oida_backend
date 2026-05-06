# Prompt

Create a root-level `specifications/` folder that documents the current Go OSINT backend as an agent-facing system specification.

The deliverable must:

- Treat `specifications/` as a source map for future implementation agents, not as an operator runbook.
- Describe the system that exists today: Go binaries, ClickHouse HTTP storage, MinIO-backed bootstrap assets, source governance, crawl/fetch/parse/promote flow, metrics, API views, auth, and deployment topology.
- Add a clear index and reading order in `specifications/README.md`.
- Include focused pages for architecture, data lifecycle, source governance and catalog, ClickHouse schema contracts, parsers and promotion, domain packs and metrics, orchestration jobs, API and auth, operations and deployment, and extension playbooks.
- Put `Source of Truth` paths and `Extension Knobs` in every specification page.
- Separate runtime-backed behavior from cataloged, deferred, or roadmap behavior.
- Update `README.md` and root `AGENTS.md` so future agents know to start with `specifications/README.md`.
- Leave concrete evidence in `workflow/Completion.md`.

Non-goals:

- No runtime behavior changes.
- No API, migration, schema, seed, or Compose behavior changes.
- No new dependencies.
