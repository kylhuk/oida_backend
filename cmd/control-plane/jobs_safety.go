package main

import (
	"context"
	"fmt"
	"time"

	"global-osint-backend/internal/migrate"
)

const ingestSafetySecurityJobName = "ingest-safety-security"

func init() {
	jobRegistry[ingestSafetySecurityJobName] = jobRunner{
		description: "Orchestrate safety/security HTTP sources through fetch, parse, and promote stages.",
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

	stats, err := orchestrateDomainSources(ctx, runner, ingestSafetySecurityJobName, options, safetyConcreteSources, startedAt, "")
	if err != nil {
		return recordFailure(err, "build safety/security ingest plan", map[string]any{"stage": "plan", "source_id": options.SourceID})
	}
	finalStats := map[string]any{
		"source_id":            options.SourceID,
		"selected_sources":     stats.SelectedSources,
		"executed_sources":     stats.ExecutedSources,
		"disabled_sources":     stats.DisabledSources,
		"frontier_seeded_rows": stats.FrontierSeededRows,
		"fetch_runs":           stats.FetchRuns,
		"parse_runs":           stats.ParseRuns,
		"promote_runs":         stats.PromoteRuns,
	}
	if err := recordJobRun(ctx, runner, jobID, ingestSafetySecurityJobName, "success", startedAt, time.Now().UTC().Truncate(time.Millisecond), "orchestrated safety/security http sources", finalStats); err != nil {
		return err
	}
	return nil
}
