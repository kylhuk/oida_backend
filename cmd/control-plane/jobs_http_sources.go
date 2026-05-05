package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"global-osint-backend/internal/dashboardstats"
	"global-osint-backend/internal/discovery"
	"global-osint-backend/internal/migrate"
	"global-osint-backend/internal/observability"
)

var (
	geopoliticalConcreteSources = []string{"seed:gdelt", "fixture:reliefweb", "fixture:acled"}
	safetyConcreteSources       = []string{"fixture:opensanctions", "fixture:nasa-firms", "fixture:noaa-hazards", "fixture:kev"}
)

const automaticHTTPSyncJobName = "ingest-http-sources"

type sourceRuntimeRecord struct {
	SourceID        string   `json:"source_id"`
	CatalogKind     string   `json:"catalog_kind"`
	LifecycleState  string   `json:"lifecycle_state"`
	ReviewStatus    string   `json:"review_status"`
	Domain          string   `json:"domain"`
	Entrypoints     []string `json:"entrypoints"`
	AuthMode        string   `json:"auth_mode"`
	AuthConfigJSON  string   `json:"auth_config_json"`
	TransportType   string   `json:"transport_type"`
	CrawlEnabled    uint8    `json:"crawl_enabled"`
	RefreshStrategy string   `json:"refresh_strategy"`
	CrawlStrategy   string   `json:"crawl_strategy"`
	CrawlConfigJSON string   `json:"crawl_config_json"`
	BronzeTable     *string  `json:"bronze_table"`
	Enabled         uint8    `json:"enabled"`
	DisabledReason  *string  `json:"disabled_reason"`
}

type sourceAliasConfig struct {
	SourceAliases []string `json:"source_aliases"`
}

type disabledSource struct {
	SourceID string `json:"source_id"`
	Reason   string `json:"reason"`
}

type domainOrchestrationStats struct {
	SelectedSources    []string         `json:"selected_sources"`
	ExecutedSources    []string         `json:"executed_sources"`
	DisabledSources    []disabledSource `json:"disabled_sources"`
	FrontierSeededRows int              `json:"frontier_seeded_rows"`
	FetchRuns          int              `json:"fetch_runs"`
	ParseRuns          int              `json:"parse_runs"`
	PromoteRuns        int              `json:"promote_runs"`
}

func orchestrateDomainSources(ctx context.Context, runner *migrate.HTTPRunner, jobName string, options jobOptions, defaultSources []string, startedAt time.Time, acledKey string, recordPromoteStage bool) (domainOrchestrationStats, error) {
	requestedSources, err := resolveRequestedSourceIDs(ctx, runner, strings.TrimSpace(options.SourceID), defaultSources)
	if err != nil {
		return domainOrchestrationStats{}, err
	}
	records, err := loadSourceRuntimeRecords(ctx, runner, requestedSources)
	if err != nil {
		return domainOrchestrationStats{}, err
	}
	stats := domainOrchestrationStats{SelectedSources: requestedSources}
	for _, record := range records {
		if skip, reason := shouldSkipSource(record, acledKey); skip {
			stats.DisabledSources = append(stats.DisabledSources, disabledSource{SourceID: record.SourceID, Reason: reason})
			continue
		}
		if due, reason, err := sourceDueForSync(ctx, runner, record, startedAt); err != nil {
			return domainOrchestrationStats{}, err
		} else if !due {
			stats.DisabledSources = append(stats.DisabledSources, disabledSource{SourceID: record.SourceID, Reason: reason})
			continue
		}
		seeded, err := seedFrontier(ctx, runner, record, startedAt)
		if err != nil {
			return domainOrchestrationStats{}, err
		}
		stats.ExecutedSources = append(stats.ExecutedSources, record.SourceID)
		stats.FrontierSeededRows += seeded
		if err := recordPipelineStage(ctx, runner, jobName, record.SourceID, "fetch", startedAt, map[string]any{"source_id": record.SourceID, "entrypoints": len(record.Entrypoints)}); err != nil {
			return domainOrchestrationStats{}, err
		}
		stats.FetchRuns++
		if err := recordPipelineStage(ctx, runner, jobName, record.SourceID, "parse", startedAt, map[string]any{"source_id": record.SourceID, "bronze_table": record.BronzeTable}); err != nil {
			return domainOrchestrationStats{}, err
		}
		stats.ParseRuns++
		if recordPromoteStage {
			if err := recordPipelineStage(ctx, runner, jobName, record.SourceID, "promote", startedAt, map[string]any{"source_id": record.SourceID}); err != nil {
				return domainOrchestrationStats{}, err
			}
			stats.PromoteRuns++
		}
	}
	return stats, nil
}

func resolveRequestedSourceIDs(ctx context.Context, runner *migrate.HTTPRunner, requested string, defaults []string) ([]string, error) {
	if requested == "" {
		if len(defaults) == 0 {
			return listInScopeHTTPSources(ctx, runner)
		}
		return append([]string(nil), defaults...), nil
	}
	record, err := loadSingleSourceRuntimeRecord(ctx, runner, requested)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(record.TransportType) == "bundle_alias" {
		aliases, err := parseSourceAliases(record.CrawlConfigJSON)
		if err != nil {
			return nil, err
		}
		return aliases, nil
	}
	return []string{requested}, nil
}

func loadSingleSourceRuntimeRecord(ctx context.Context, runner *migrate.HTTPRunner, sourceID string) (sourceRuntimeRecord, error) {
	records, err := loadSourceRuntimeRecords(ctx, runner, []string{sourceID})
	if err != nil {
		return sourceRuntimeRecord{}, err
	}
	if len(records) != 1 {
		return sourceRuntimeRecord{}, fmt.Errorf("source %q not found in meta.source_registry", sourceID)
	}
	return records[0], nil
}

func loadSourceRuntimeRecords(ctx context.Context, runner *migrate.HTTPRunner, sourceIDs []string) ([]sourceRuntimeRecord, error) {
	if len(sourceIDs) == 0 {
		return []sourceRuntimeRecord{}, nil
	}
	query := fmt.Sprintf(`SELECT source_id, catalog_kind, lifecycle_state, review_status, domain, entrypoints, auth_mode, auth_config_json, transport_type, crawl_enabled, refresh_strategy, crawl_strategy, crawl_config_json, bronze_table, enabled, disabled_reason
FROM meta.source_registry FINAL
WHERE source_id IN (%s)
ORDER BY source_id
FORMAT JSONEachRow`, sqlStringList(sourceIDs))
	output, err := runner.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(strings.TrimSpace(output), "\n")
	indexed := make(map[string]sourceRuntimeRecord, len(lines))
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var record sourceRuntimeRecord
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			return nil, err
		}
		indexed[record.SourceID] = record
	}
	records := make([]sourceRuntimeRecord, 0, len(indexed))
	for _, sourceID := range sourceIDs {
		record, ok := indexed[sourceID]
		if !ok {
			continue
		}
		records = append(records, record)
	}
	return records, nil
}

func listInScopeHTTPSources(ctx context.Context, runner *migrate.HTTPRunner) ([]string, error) {
	query := `SELECT source_id
FROM meta.source_registry FINAL
WHERE catalog_kind = 'concrete'
	AND transport_type = 'http'
	AND bronze_table IS NOT NULL
ORDER BY source_id
FORMAT TabSeparated`
	output, err := runner.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	sourceIDs := make([]string, 0)
	for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
		sourceID := strings.TrimSpace(line)
		if sourceID == "" {
			continue
		}
		sourceIDs = append(sourceIDs, sourceID)
	}
	return sourceIDs, nil
}

func listAutomaticHTTPSyncSourceIDs(ctx context.Context, runner *migrate.HTTPRunner) ([]string, error) {
	return listInScopeHTTPSources(ctx, runner)
}

func runAutomaticHTTPSync(ctx context.Context) error {
	startedAt := time.Now().UTC().Truncate(time.Millisecond)
	runner := migrate.NewHTTPRunner(controlPlaneClickHouseURL())
	jobID := fmt.Sprintf("job:%s:%d", automaticHTTPSyncJobName, startedAt.UnixMilli())
	automaticSources, err := listAutomaticHTTPSyncSourceIDs(ctx, runner)
	if err != nil {
		if recordErr := recordJobRun(ctx, runner, jobID, automaticHTTPSyncJobName, "failed", startedAt, time.Now().UTC().Truncate(time.Millisecond), "enumerate automatic http source sync sources", map[string]any{"stage": "plan"}); recordErr != nil {
			return fmt.Errorf("%w (job log failed: %v)", err, recordErr)
		}
		return err
	}
	stats, err := orchestrateDomainSources(ctx, runner, automaticHTTPSyncJobName, jobOptions{}, automaticSources, startedAt, strings.TrimSpace(os.Getenv("ACLED_API_KEY")), false)
	if err != nil {
		if recordErr := recordJobRun(ctx, runner, jobID, automaticHTTPSyncJobName, "failed", startedAt, time.Now().UTC().Truncate(time.Millisecond), "orchestrate automatic http source sync", map[string]any{"stage": "plan"}); recordErr != nil {
			return fmt.Errorf("%w (job log failed: %v)", err, recordErr)
		}
		return err
	}
	if stats.ParseRuns > 0 {
		promoteStartedAt := time.Now().UTC().Truncate(time.Millisecond)
		promoteResult, err := runPromoteWithRunner(ctx, runner, promoteStartedAt)
		if err != nil {
			if recordErr := recordJobRun(ctx, runner, jobID, automaticHTTPSyncJobName, "failed", startedAt, time.Now().UTC().Truncate(time.Millisecond), "run automatic promote after parse", map[string]any{"stage": "promote"}); recordErr != nil {
				return fmt.Errorf("%w (job log failed: %v)", err, recordErr)
			}
			return err
		}
		for _, sourceID := range promoteResult.SourceIDs {
			if err := recordPipelineStage(ctx, runner, automaticHTTPSyncJobName, sourceID, "promote", startedAt, map[string]any{"source_id": sourceID, "input_rows": promoteResult.Stats["input_rows"]}); err != nil {
				if recordErr := recordJobRun(ctx, runner, jobID, automaticHTTPSyncJobName, "failed", startedAt, time.Now().UTC().Truncate(time.Millisecond), "record automatic promote stage", map[string]any{"stage": "promote", "source_id": sourceID}); recordErr != nil {
					return fmt.Errorf("%w (job log failed: %v)", err, recordErr)
				}
				return err
			}
		}
		stats.PromoteRuns = len(promoteResult.SourceIDs)
	}
	statsPayload := map[string]any{
		"selected_sources":     stats.SelectedSources,
		"executed_sources":     stats.ExecutedSources,
		"disabled_sources":     stats.DisabledSources,
		"frontier_seeded_rows": stats.FrontierSeededRows,
		"fetch_runs":           stats.FetchRuns,
		"parse_runs":           stats.ParseRuns,
		"promote_runs":         stats.PromoteRuns,
	}
	if err := addCatalogRolloutSummary(ctx, runner, statsPayload); err != nil {
		observability.LogEvent("control-plane", "rollout_summary_unavailable", observability.CorrelationID(ctx), map[string]any{"job": automaticHTTPSyncJobName, "error": err.Error()})
	}
	if err := recordJobRun(ctx, runner, jobID, automaticHTTPSyncJobName, "success", startedAt, time.Now().UTC().Truncate(time.Millisecond), "orchestrated automatic http source sync", statsPayload); err != nil {
		return err
	}
	return nil
}

func addCatalogRolloutSummary(ctx context.Context, runner *migrate.HTTPRunner, stats map[string]any) error {
	rolloutSummary, err := dashboardstats.CollectCatalogRolloutSummary(ctx, runner)
	if err != nil {
		return err
	}
	stats["catalog_runnable"] = rolloutSummary.CatalogRunnable
	stats["catalog_approved_runtime_linked"] = rolloutSummary.CatalogApprovedRuntime
	stats["catalog_credential_gated"] = rolloutSummary.CatalogCredentialGated
	stats["catalog_public_concrete"] = rolloutSummary.CatalogPublicConcrete
	stats["catalog_public_runtime_linked"] = rolloutSummary.CatalogPublicRuntime
	stats["catalog_public_deferred"] = rolloutSummary.CatalogPublicDeferred
	stats["catalog_runtime_credential_gated"] = rolloutSummary.CatalogRuntimeGated
	stats["catalog_deferred_credential_gated"] = rolloutSummary.CatalogDeferredGated
	return nil
}

func parseSourceAliases(raw string) ([]string, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, fmt.Errorf("bundle alias crawl_config_json is empty")
	}
	var config sourceAliasConfig
	if err := json.Unmarshal([]byte(raw), &config); err != nil {
		return nil, err
	}
	if len(config.SourceAliases) == 0 {
		return nil, fmt.Errorf("bundle alias crawl_config_json has no source_aliases")
	}
	return config.SourceAliases, nil
}

func shouldSkipSource(record sourceRuntimeRecord, acledKey string) (bool, string) {
	if strings.TrimSpace(record.CatalogKind) != "concrete" {
		return true, "source is not a concrete runtime source"
	}
	if strings.TrimSpace(record.LifecycleState) != "approved_enabled" {
		return true, fmt.Sprintf("source lifecycle %s not runnable", strings.TrimSpace(record.LifecycleState))
	}
	if reviewStatus := strings.TrimSpace(record.ReviewStatus); reviewStatus != "approved" {
		if reviewStatus == "" {
			return true, "source review status missing not runnable"
		}
		return true, fmt.Sprintf("source review status %s not runnable", reviewStatus)
	}
	if record.Enabled == 0 {
		if record.DisabledReason != nil && strings.TrimSpace(*record.DisabledReason) != "" {
			return true, strings.TrimSpace(*record.DisabledReason)
		}
		return true, "source disabled"
	}
	if reason := missingCredentialReason(record, acledKey); reason != "" {
		return true, reason
	}
	if record.CrawlEnabled == 0 {
		return true, "crawl disabled"
	}
	return false, ""
}

func missingCredentialReason(record sourceRuntimeRecord, acledKey string) string {
	authMode := strings.ToLower(strings.TrimSpace(record.AuthMode))
	if authMode == "" || authMode == "none" {
		if record.SourceID == "fixture:acled" && strings.TrimSpace(acledKey) == "" {
			return "missing credential ACLED_API_KEY"
		}
		return ""
	}
	var contract struct {
		EnvVar             string `json:"env_var"`
		ClientIDEnvVar     string `json:"client_id_env_var"`
		ClientSecretEnvVar string `json:"client_secret_env_var"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(record.AuthConfigJSON)), &contract); err != nil {
		return ""
	}
	switch authMode {
	case "user_supplied_key":
		envVar := strings.TrimSpace(contract.EnvVar)
		if envVar != "" && strings.TrimSpace(os.Getenv(envVar)) == "" {
			return fmt.Sprintf("missing credential %s", envVar)
		}
	case "oauth2_client_credentials":
		for _, envVar := range []string{strings.TrimSpace(contract.ClientIDEnvVar), strings.TrimSpace(contract.ClientSecretEnvVar)} {
			if envVar != "" && strings.TrimSpace(os.Getenv(envVar)) == "" {
				return fmt.Sprintf("missing credential %s", envVar)
			}
		}
	}
	return ""
}

func seedFrontier(ctx context.Context, runner *migrate.HTTPRunner, record sourceRuntimeRecord, now time.Time) (int, error) {
	seeded := 0
	for _, entrypoint := range record.Entrypoints {
		entry, err := discovery.NewFrontierEntrypoint(record.SourceID, record.Domain, entrypoint, "entrypoint", 100, now, plannedNextFetchAt(record, now))
		if err != nil || entry.CanonicalURL == "" {
			continue
		}
		exists, err := frontierEntryExists(ctx, runner, record.SourceID, entry.CanonicalURL)
		if err != nil {
			return seeded, err
		}
		if exists {
			query := fmt.Sprintf(`ALTER TABLE ops.crawl_frontier
	UPDATE url = %s,
	       state = %s,
	       lease_owner = NULL,
	       lease_expires_at = NULL,
	       discovery_kind = %s,
	       last_attempt_at = %s
	WHERE source_id = %s AND canonical_url = %s`,
				sqlString(entry.URL),
				sqlString(entry.State),
				sqlString(entry.DiscoveryKind),
				sqlTime(now.UTC()),
				sqlString(record.SourceID),
				sqlString(entry.CanonicalURL),
			)
			if err := runner.ApplySQL(ctx, query); err != nil {
				return seeded, err
			}
			seeded++
			continue
		}
		frontierID := hashStrings(record.SourceID, entry.CanonicalURL, "entrypoint")[:32]
		query := fmt.Sprintf(`INSERT INTO ops.crawl_frontier (frontier_id, source_id, domain, url, canonical_url, discovery_kind, url_hash, priority, state, discovered_at, next_fetch_at)
VALUES (%s,%s,%s,%s,%s,%s,%s,%d,%s,%s,%s)`,
			sqlString(frontierID),
			sqlString(record.SourceID),
			sqlString(entry.Domain),
			sqlString(entry.URL),
			sqlString(entry.CanonicalURL),
			sqlString(entry.DiscoveryKind),
			sqlString(entry.URLHash),
			entry.Priority,
			sqlString(entry.State),
			sqlTime(entry.DiscoveredAt),
			sqlTime(entry.NextFetchAt),
		)
		if err := runner.ApplySQL(ctx, query); err != nil {
			return seeded, err
		}
		seeded++
	}
	return seeded, nil
}

func frontierEntryExists(ctx context.Context, runner *migrate.HTTPRunner, sourceID, canonicalURL string) (bool, error) {
	query := fmt.Sprintf(`SELECT count() FROM ops.crawl_frontier WHERE source_id = %s AND canonical_url = %s FORMAT TabSeparated`, sqlString(sourceID), sqlString(canonicalURL))
	output, err := runner.Query(ctx, query)
	if err != nil {
		if strings.Contains(err.Error(), "UNKNOWN_TABLE") {
			return false, nil
		}
		return false, err
	}
	count, err := strconv.Atoi(strings.TrimSpace(output))
	if err != nil {
		return false, fmt.Errorf("decode crawl_frontier count: %w", err)
	}
	return count > 0, nil
}

func sourceDueForSync(ctx context.Context, runner *migrate.HTTPRunner, record sourceRuntimeRecord, now time.Time) (bool, string, error) {
	window := syncRefreshWindow(record.RefreshStrategy)
	if window <= 0 {
		return false, fmt.Sprintf("unsupported refresh strategy %s", strings.TrimSpace(record.RefreshStrategy)), nil
	}
	switch strings.TrimSpace(record.CrawlStrategy) {
	case "delta", "full":
	default:
		return false, fmt.Sprintf("unsupported crawl strategy %s", strings.TrimSpace(record.CrawlStrategy)), nil
	}
	for _, entrypoint := range record.Entrypoints {
		canonical, err := discovery.NormalizeURL(entrypoint)
		if err != nil || canonical == "" {
			continue
		}
		exists, err := frontierEntryExists(ctx, runner, record.SourceID, canonical)
		if err != nil {
			return false, "", err
		}
		if !exists {
			return true, "", nil
		}
	}
	query := fmt.Sprintf(`SELECT max(next_fetch_at), max(last_attempt_at) FROM ops.crawl_frontier WHERE source_id = %s FORMAT TabSeparated`, sqlString(record.SourceID))
	output, err := runner.Query(ctx, query)
	if err != nil {
		if strings.Contains(err.Error(), "UNKNOWN_TABLE") {
			return true, "", nil
		}
		return false, "", err
	}
	parts := strings.Split(strings.TrimSpace(output), "\t")
	if len(parts) == 0 {
		parts = []string{""}
	}
	nextFetchValue := strings.TrimSpace(parts[0])
	lastAttemptValue := ""
	if len(parts) > 1 {
		lastAttemptValue = strings.TrimSpace(parts[1])
	}
	if nextFetchValue == "" || nextFetchValue == "\\N" || nextFetchValue == "null" || nextFetchValue == "0000-00-00 00:00:00" {
		if lastAttemptValue == "" || lastAttemptValue == "\\N" || lastAttemptValue == "null" || lastAttemptValue == "0000-00-00 00:00:00" {
			return true, "", nil
		}
	}
	effectiveNextFetchAt := time.Time{}
	if nextFetchValue != "" && nextFetchValue != "\\N" && nextFetchValue != "null" && nextFetchValue != "0000-00-00 00:00:00" {
		parsedNextFetchAt, err := parseClickHouseTime(nextFetchValue)
		if err != nil {
			return false, "", err
		}
		effectiveNextFetchAt = parsedNextFetchAt
	}
	if lastAttemptValue != "" && lastAttemptValue != "\\N" && lastAttemptValue != "null" && lastAttemptValue != "0000-00-00 00:00:00" {
		parsedLastAttemptAt, err := parseClickHouseTime(lastAttemptValue)
		if err != nil {
			return false, "", err
		}
		candidateNextFetchAt := parsedLastAttemptAt.Add(window)
		if effectiveNextFetchAt.IsZero() || candidateNextFetchAt.After(effectiveNextFetchAt) {
			effectiveNextFetchAt = candidateNextFetchAt
		}
	}
	if effectiveNextFetchAt.IsZero() {
		return true, "", nil
	}
	if !effectiveNextFetchAt.After(now.UTC()) {
		return true, "", nil
	}
	if strings.TrimSpace(record.CrawlStrategy) == "full" && effectiveNextFetchAt.Sub(now.UTC()) <= window/2 {
		return true, "", nil
	}
	return false, fmt.Sprintf("not due until %s", effectiveNextFetchAt.UTC().Format(time.RFC3339)), nil
}

func syncRefreshWindow(refreshStrategy string) time.Duration {
	switch strings.TrimSpace(refreshStrategy) {
	case "frequent":
		return 15 * time.Minute
	case "scheduled":
		return 6 * time.Hour
	case "manual", "":
		return 0
	default:
		return 0
	}
}

func plannedNextFetchAt(record sourceRuntimeRecord, now time.Time) time.Time {
	window := syncRefreshWindow(record.RefreshStrategy)
	if window <= 0 {
		return now.UTC()
	}
	return now.UTC().Add(window)
}

func parseClickHouseTime(value string) (time.Time, error) {
	value = strings.TrimSpace(value)
	for _, layout := range []string{"2006-01-02 15:04:05.000", "2006-01-02 15:04:05", time.RFC3339Nano, time.RFC3339} {
		parsed, err := time.Parse(layout, value)
		if err == nil {
			return parsed.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("unsupported ClickHouse time %q", value)
}

func recordPipelineStage(ctx context.Context, runner *migrate.HTTPRunner, jobName, sourceID, stage string, startedAt time.Time, stats map[string]any) error {
	jobID := fmt.Sprintf("job:%s:%s:%s:%d", jobName, stage, sourceID, startedAt.UnixMilli())
	return recordJobRun(ctx, runner, jobID, stage, "success", startedAt, startedAt, fmt.Sprintf("orchestrated %s stage for %s", stage, sourceID), stats)
}

func sqlStringList(values []string) string {
	items := make([]string, 0, len(values))
	for _, value := range values {
		items = append(items, sqlString(strings.TrimSpace(value)))
	}
	return strings.Join(items, ",")
}

func hashStrings(values ...string) string {
	h := sha256.New()
	for _, value := range values {
		_, _ = h.Write([]byte(strings.TrimSpace(value)))
		_, _ = h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}
