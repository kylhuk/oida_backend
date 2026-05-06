# Prompt

Make the repository production-ready around the Go OSINT backend.

The deliverable must:

- Remove stale Python runtime ownership and documentation.
- Provide one authoritative Go verification command.
- Harden Docker Compose for single-node production use.
- Replace single shared-key API auth with hashed, scoped API keys.
- Add operational metrics, dashboards, alerts, and backup/restore runbooks.
- Keep migrations append-only and ClickHouse HTTP-only.
- Leave evidence in `workflow/Completion.md` before claiming completion.
