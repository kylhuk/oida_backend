# BOOTSTRAP SERVICE

## OVERVIEW
One-shot installer and verifier for ClickHouse, MinIO, RBAC, migrations, seeds, staged assets, and the ready marker.

## WHERE TO LOOK

| Task | Location | Notes |
|------|----------|-------|
| Change install/verify flow | `cmd/bootstrap/main.go` | Keep `install()` and `verify()` aligned |
| Change source governance seed logic | `cmd/bootstrap/source_registry.go` | Seed evolution preserves operational state |
| Change migration execution contract | `internal/migrate/http_runner.go` | Bootstrap depends on HTTP runner invariants |

## CONVENTIONS

- `main.go` supports `install` and `verify`; new bootstrap features usually need both paths.
- Bootstrap talks to ClickHouse over HTTP and MinIO via the in-repo SigV4 client.
- Seed loading is idempotent: unchanged seeds should not create new versions.
- `meta.source_registry` governance fields are not throwaway metadata; they drive runtime behavior.
- Ready state is a file marker written by `writeReadyMarker()` and read by API `/v1/ready`.

## GOTCHAS

- `source_registry.go` carries backward compatibility for legacy seeds and the old `version` column.
- `bundle_alias` versus `http` sources have different validation rules for `bronze_table` and `promote_profile`.
- `mergeSourceAttrs()` stores `seed_checksum`; losing it causes unnecessary version churn.
- RBAC setup includes revokes as well as grants; do not assume additive-only behavior.

## COMMANDS

```bash
go test ./cmd/bootstrap/...
go run ./cmd/bootstrap
go run ./cmd/bootstrap verify
docker compose run --rm bootstrap verify
```

## ANTI-PATTERNS

- Do not add install-only work without a verify path.
- Do not hardcode credentials in code or seed JSON.
- Do not skip optimization / governance compatibility steps in source registry evolution.
