# Schema Standards

This document freezes the ClickHouse schema conventions introduced through `migrations/clickhouse/0004_meta_registries.sql`.

Later tasks must use these standards instead of inventing table conventions ad hoc. If a new requirement needs an exception, land the exception in this document and the schema convention tests in the same change.

## Frozen conventions

- Naming uses lowercase snake_case for databases, tables, columns, views, and compatibility artifacts.
- Timestamps use `DateTime64(3, 'UTC')`. Use `Nullable(DateTime64(3, 'UTC'))` only when the timestamp can be intentionally absent.
- Versioning uses `schema_version`, `record_version`, and `api_contract_version` on registry and contract-bearing tables.
- JSON payloads use `attrs` for evolving non-hot fields and `evidence` for provenance or explainability payloads.
- Time-series tables partition monthly with `PARTITION BY toYYYYMM(<primary_timestamp>)`.
- `ORDER BY` keys are tuned to expected API filter patterns. Lead with common filter dimensions, then add a stable identifier.
- Enum-like strings use `LowCardinality(String)`.
- Engines follow a fixed default:
  - `MergeTree` for standard append-oriented tables.
  - `ReplacingMergeTree(record_version)` for dimensions and registries.
  - `AggregatingMergeTree` for metric state and rollup storage.

## Meta registry baseline

All `meta` registries use explicit engines and version fields.

| Table | Purpose | Engine | API-oriented order key | Required standard columns |
| --- | --- | --- | --- | --- |
| `meta.source_registry` | source definitions and parser routing | `ReplacingMergeTree(record_version)` | `(source_id)` | `schema_version`, `record_version`, `api_contract_version`, `updated_at`, `attrs`, `evidence` |
| `meta.parser_registry` | parser metadata and routing | `ReplacingMergeTree(record_version)` | `(source_class, route_scope, parser_id)` | `schema_version`, `record_version`, `api_contract_version`, `updated_at`, `attrs`, `evidence` |
| `meta.metric_registry` | metric definitions and rollup rules | `ReplacingMergeTree(record_version)` | `(metric_family, subject_grain, metric_id)` | `schema_version`, `record_version`, `api_contract_version`, `updated_at`, `attrs`, `evidence` |
| `meta.api_schema_registry` | API compatibility and deprecation metadata | `ReplacingMergeTree(record_version)` | `(api_contract_version, http_method, route_pattern)` | `schema_version`, `record_version`, `api_contract_version`, `updated_at`, `attrs`, `evidence` |

## Practical rules

- Keep hot filter fields typed; do not hide filterable fields inside `attrs` or `evidence`.
- Prefer non-nullable columns. Use `Nullable(...)` only when missing data carries meaning.
- New monthly event, log, or snapshot tables should use the dominant event timestamp in both retention policy design and `toYYYYMM(...)` partitioning.
- New registries should not use generic `version` column names; use `record_version`.
- New API compatibility metadata should record deprecation windows in `meta.api_schema_registry` instead of custom per-table flags.

## Enforcement

- `migrations/clickhouse/0004_meta_registries.sql` establishes the registry baseline.
- `internal/migrate/schema_standards_test.go` acts as the schema convention lint check for frozen standards.
