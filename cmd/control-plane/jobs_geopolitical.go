package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"global-osint-backend/internal/migrate"
	"global-osint-backend/internal/packs/geopolitical"
)

const ingestGeopoliticalJobName = "ingest-geopolitical"

func init() {
	jobRegistry[ingestGeopoliticalJobName] = jobRunner{
		description: "Ingest geopolitical fixture feeds and compute pack metrics.",
		run:         runIngestGeopolitical,
	}
}

func runIngestGeopolitical(ctx context.Context) error {
	startedAt := time.Now().UTC().Truncate(time.Millisecond)
	runner := migrate.NewHTTPRunner(controlPlaneClickHouseURL())
	jobID := fmt.Sprintf("job:%s:%d", ingestGeopoliticalJobName, startedAt.UnixMilli())
	options := currentJobOptions(ctx)

	recordFailure := func(err error, message string, stats map[string]any) error {
		if recordErr := recordJobRun(ctx, runner, jobID, ingestGeopoliticalJobName, "failed", startedAt, time.Now().UTC().Truncate(time.Millisecond), message, stats); recordErr != nil {
			return fmt.Errorf("%w (job log failed: %v)", err, recordErr)
		}
		return err
	}

	plan, err := geopolitical.BuildIngestPlan(ctx, geopolitical.Options{
		SourceID: strings.TrimSpace(options.SourceID),
		ACLEDKey: strings.TrimSpace(os.Getenv(geopolitical.ACLEDKeyEnv)),
		Now:      startedAt,
	})
	if err != nil {
		return recordFailure(err, "build geopolitical ingest plan", map[string]any{"stage": "plan", "source_id": options.SourceID})
	}

	statements, err := plan.SQLStatements()
	if err != nil {
		return recordFailure(err, "build geopolitical ingest sql", map[string]any{"stage": "sql"})
	}
	for _, statement := range statements {
		if err := runner.ApplySQL(ctx, statement); err != nil {
			return recordFailure(err, "apply geopolitical ingest sql", map[string]any{"stage": "apply"})
		}
	}

	disabled := make([]map[string]any, 0, len(plan.DisabledSources))
	for _, item := range plan.DisabledSources {
		disabled = append(disabled, map[string]any{"source_id": item.SourceID, "reason": item.Reason})
	}
	stats := map[string]any{
		"source_id":            options.SourceID,
		"executed_sources":     plan.ExecutedSources,
		"disabled_sources":     disabled,
		"event_rows":           len(plan.Events),
		"entity_rows":          len(plan.Entities),
		"event_entity_links":   len(plan.EventEntities),
		"event_place_links":    len(plan.EventPlaces),
		"entity_place_links":   len(plan.EntityPlaces),
		"metric_registry_rows": len(plan.MetricRegistry),
		"metric_rows":          len(plan.Contributions),
		"snapshot_rows":        len(plan.Snapshots),
		"sql_statements":       len(statements),
	}
	if err := recordJobRun(ctx, runner, jobID, ingestGeopoliticalJobName, "success", startedAt, time.Now().UTC().Truncate(time.Millisecond), "ingested geopolitical fixtures", stats); err != nil {
		return err
	}
	return nil
}
