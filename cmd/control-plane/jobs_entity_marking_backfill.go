package main

import (
	"context"
	"fmt"
	"time"

	"global-osint-backend/internal/migrate"
)

const entityMarkingBackfillJobName = "entity-marking-backfill"

func init() {
	jobRegistry[entityMarkingBackfillJobName] = jobRunner{
		description: "Backfill silver.dim_entity.marking from meta.source_registry.marking_default. Idempotent.",
		run:         runEntityMarkingBackfill,
	}
}

func runEntityMarkingBackfill(ctx context.Context) error {
	runner := migrate.NewHTTPRunner(controlPlaneClickHouseURL())
	startedAt := time.Now().UTC()
	jobID := fmt.Sprintf("job:%s:%d", entityMarkingBackfillJobName, startedAt.UnixMilli())

	// ClickHouse ReplacingMergeTree: we cannot do UPDATE; instead we perform an
	// INSERT that re-emits all rows joined to their source's marking_default.
	// The record_version is incremented by 1 so FINAL selects the new copy.
	// Rows without a source registry entry retain the column default 'UNCLASSIFIED'.
	sql := `
INSERT INTO silver.dim_entity
SELECT
    e.entity_id,
    e.entity_type,
    e.canonical_name,
    e.status,
    e.risk_band,
    e.primary_place_id,
    e.source_entity_key,
    e.source_system,
    e.valid_from,
    e.valid_to,
    e.schema_version,
    e.record_version + 1 AS record_version,
    e.api_contract_version,
    now64(3) AS updated_at,
    e.attrs,
    e.evidence,
    coalesce(nullIf(trimBoth(s.marking_default), ''), 'UNCLASSIFIED') AS marking
FROM silver.dim_entity AS e FINAL
LEFT JOIN meta.source_registry AS s FINAL
    ON e.source_system = s.source_id
WHERE coalesce(e.marking, '') != coalesce(nullIf(trimBoth(s.marking_default), ''), 'UNCLASSIFIED')
`
	if err := runner.ApplySQL(ctx, sql); err != nil {
		return recordOpsJobFailure(ctx, runner, jobID, entityMarkingBackfillJobName, startedAt,
			"entity marking backfill failed", err, nil)
	}

	// Count how many entities now carry non-default markings.
	out, queryErr := runner.Query(ctx, `
SELECT count() AS n
FROM silver.dim_entity FINAL
WHERE marking != 'UNCLASSIFIED'
FORMAT JSONEachRow
`)
	stats := map[string]any{}
	if queryErr == nil {
		type row struct {
			N uint64 `json:"n"`
		}
		var r row
		fmt.Sscanf(out, `{"n":"%d"}`, &r.N)
		stats["non_default_marking_count"] = r.N
	}

	return recordJobRun(ctx, runner, jobID, entityMarkingBackfillJobName, "success",
		startedAt, time.Now().UTC().Truncate(time.Millisecond),
		"entity marking backfill completed", stats)
}
