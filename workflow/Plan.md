# Plan

## Milestones

1. Repository ownership cleanup
   - Remove Python medallion source, tests, Dockerfile, dependencies, and compose services.
   - Remove tracked runtime binaries and ready/session artifacts.
   - Align `.gitignore`, `.dockerignore`, README, and Makefile around Go.
   - Status: complete.

2. Verification foundation
   - Add `scripts/verify.sh`.
   - Make `make verify` call the script.
   - Use Dockerized Go when local Go is unavailable.
   - Status: complete; default gate passed.

3. Production auth
   - Add `meta.api_clients`.
   - Seed hashed API clients during bootstrap.
   - Authorize protected routes by key hash and scopes.
   - Status: complete.

4. Operations hardening
   - Pin container images by digest.
   - Use non-root Go runtime images.
   - Add `/metrics`, Prometheus, Alertmanager, and Grafana.
   - Add backup/restore/retention jobs.
   - Status: complete.

5. Evidence gate
   - Run `./scripts/verify.sh`.
   - Run `FULL=1 ./scripts/verify.sh` when Docker services are available.
   - Status: default gate passed; full gate blocked by existing host MinIO on port `9000`.

## Acceptance Criteria

- `docker compose config` exits 0.
- `./scripts/verify.sh` exits 0.
- No Python runtime/test files remain.
- No tracked root binaries remain.
- No `:latest` compose images remain.
- Protected API routes reject missing/invalid keys and accept seeded scoped keys.
- `/metrics` emits Prometheus text format.
