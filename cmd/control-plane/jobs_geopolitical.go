package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"global-osint-backend/internal/migrate"
)

const ingestGeopoliticalJobName = "ingest-geopolitical"

func init() {
	jobRegistry[ingestGeopoliticalJobName] = jobRunner{
		description: "Orchestrate geopolitical HTTP sources through fetch, parse, and promote stages.",
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

	stats, err := orchestrateDomainSources(ctx, runner, ingestGeopoliticalJobName, options, geopoliticalConcreteSources, startedAt, strings.TrimSpace(os.Getenv("ACLED_API_KEY")))
	if err != nil {
		return recordFailure(err, "build geopolitical ingest plan", map[string]any{"stage": "plan", "source_id": options.SourceID})
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
	if err := recordJobRun(ctx, runner, jobID, ingestGeopoliticalJobName, "success", startedAt, time.Now().UTC().Truncate(time.Millisecond), "orchestrated geopolitical http sources", finalStats); err != nil {
		return err
	}
	return nil
}
