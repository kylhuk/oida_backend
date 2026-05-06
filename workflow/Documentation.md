# Documentation Log

## 2026-05-05

- Established Go OSINT as the repository's active product.
- Recorded single-node Docker as the production target.
- Recorded hashed API keys as the first production authentication model.
- Noted that Kubernetes, OIDC/JWT, and custom frontend work are deferred.
- Removed the legacy Python runtime and the unused renderer surface from the production repository state.
- Added scoped API-key auth, rate limiting, `/metrics`, monitoring provisioning, and native ClickHouse backup/restore/retention jobs.
- Rewrote README, production readiness notes, and runbooks around the Go-only single-node topology.
- Default verification passed; full Compose verification is blocked by an existing `medallion-minio-1` container using host ports `9000-9001`.

## 2026-05-06

- Stopped the conflicting `medallion-minio-1` and `medallion-http-fixture-1` containers so the OIDA full Compose gate could bind `9000-9001` and `8079`.
- Added fresh-volume reset behavior to `scripts/verify.sh` for `FULL=1` runs.
- Fixed runtime image packaging by copying migrations, seed data, backup assets, and source catalog markdown into the distroless image.
- Added `osint_promote` privileges required by automatic discovery-candidate sync and verified them through bootstrap.
- Changed generated discovery-candidate writes from streaming `INSERT ... VALUES` to executable `INSERT ... SELECT` SQL to avoid ClickHouse HTTP hangs.
- Full verification passed with `FULL=1 ./scripts/verify.sh`, including fresh Compose startup, `bootstrap verify`, and E2E.

## 2026-05-06 Specifications Task

- Reframed the active workflow around a documentation-only system specifications deliverable.
- The intended output is a root `specifications/` folder for future implementation agents.
- Runtime behavior must be grounded in current Go code, ClickHouse migrations, seed files, Compose topology, and existing docs.
- Cataloged but not runtime-runnable sources must be labeled separately from implemented worker/job/API behavior.
