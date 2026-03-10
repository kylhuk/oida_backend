package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"global-osint-backend/internal/migrate"
	"global-osint-backend/internal/packs/safety"
)

const ingestSafetySecurityJobName = "ingest-safety-security"

func init() {
	jobRegistry[ingestSafetySecurityJobName] = jobRunner{
		description: "Ingest safety/security fixture feeds and compute pack metrics.",
		run:         runIngestSafetySecurity,
	}
}

func runIngestSafetySecurity(ctx context.Context) error {
	startedAt := time.Now().UTC().Truncate(time.Millisecond)
	runner := migrate.NewHTTPRunner(controlPlaneClickHouseURL())
	jobID := fmt.Sprintf("job:%s:%d", ingestSafetySecurityJobName, startedAt.UnixMilli())
	options := currentJobOptions(ctx)

	recordFailure := func(err error, message string, stats map[string]any) error {
		if recordErr := recordJobRun(ctx, runner, jobID, ingestSafetySecurityJobName, "failed", startedAt, time.Now().UTC().Truncate(time.Millisecond), message, stats); recordErr != nil {
			return fmt.Errorf("%w (job log failed: %v)", err, recordErr)
		}
		return err
	}

	plan, err := safety.BuildIngestPlan(ctx, safety.Options{
		SourceID: strings.TrimSpace(options.SourceID),
		Now:      startedAt,
	})
	if err != nil {
		return recordFailure(err, "build safety/security ingest plan", map[string]any{"stage": "plan", "source_id": options.SourceID})
	}

	statements, err := plan.SQLStatements()
	if err != nil {
		return recordFailure(err, "build safety/security ingest sql", map[string]any{"stage": "sql"})
	}
	for _, statement := range statements {
		if err := runner.ApplySQL(ctx, statement); err != nil {
			return recordFailure(err, "apply safety/security ingest sql", map[string]any{"stage": "apply"})
		}
	}

	stats := map[string]any{
		"source_id":            options.SourceID,
		"executed_sources":     plan.ExecutedSources,
		"entity_rows":          len(plan.Entities),
		"observation_rows":     len(plan.Observations),
		"entity_place_links":   len(plan.EntityPlaces),
		"metric_registry_rows": len(plan.MetricRegistry),
		"metric_rows":          len(plan.Contributions),
		"snapshot_rows":        len(plan.Snapshots),
		"sql_statements":       len(statements),
	}
	if err := recordJobRun(ctx, runner, jobID, ingestSafetySecurityJobName, "success", startedAt, time.Now().UTC().Truncate(time.Millisecond), "ingested safety/security fixtures", stats); err != nil {
		return err
	}
	return nil
}
