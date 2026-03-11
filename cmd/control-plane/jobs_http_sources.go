package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"global-osint-backend/internal/discovery"
	"global-osint-backend/internal/migrate"
)

var (
	geopoliticalConcreteSources = []string{"seed:gdelt", "fixture:reliefweb", "fixture:acled"}
	safetyConcreteSources       = []string{"fixture:opensanctions", "fixture:nasa-firms", "fixture:noaa-hazards", "fixture:kev"}
)

type sourceRuntimeRecord struct {
	SourceID        string   `json:"source_id"`
	CatalogKind     string   `json:"catalog_kind"`
	LifecycleState  string   `json:"lifecycle_state"`
	ReviewStatus    string   `json:"review_status"`
	Domain          string   `json:"domain"`
	Entrypoints     []string `json:"entrypoints"`
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

func orchestrateDomainSources(ctx context.Context, runner *migrate.HTTPRunner, jobName string, options jobOptions, defaultSources []string, startedAt time.Time, acledKey string) (domainOrchestrationStats, error) {
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
		if err := recordPipelineStage(ctx, runner, jobName, record.SourceID, "promote", startedAt, map[string]any{"source_id": record.SourceID}); err != nil {
			return domainOrchestrationStats{}, err
		}
		stats.PromoteRuns++
	}
	return stats, nil
}

func resolveRequestedSourceIDs(ctx context.Context, runner *migrate.HTTPRunner, requested string, defaults []string) ([]string, error) {
	if requested == "" {
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
	query := fmt.Sprintf(`SELECT source_id, catalog_kind, lifecycle_state, review_status, domain, entrypoints, transport_type, crawl_enabled, refresh_strategy, crawl_strategy, crawl_config_json, bronze_table, enabled, disabled_reason
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
	if record.SourceID == "fixture:acled" && strings.TrimSpace(acledKey) == "" {
		return true, "missing credential ACLED_API_KEY"
	}
	if record.CrawlEnabled == 0 {
		return true, "crawl disabled"
	}
	return false, ""
}

func seedFrontier(ctx context.Context, runner *migrate.HTTPRunner, record sourceRuntimeRecord, now time.Time) (int, error) {
	seeded := 0
	for _, entrypoint := range record.Entrypoints {
		canonical, err := discovery.NormalizeURL(entrypoint)
		if err != nil || canonical == "" {
			continue
		}
		exists, err := frontierEntryExists(ctx, runner, record.SourceID, canonical)
		if err != nil {
			return seeded, err
		}
		if exists {
			continue
		}
		frontierID := hashStrings(record.SourceID, canonical, "entrypoint")[:32]
		urlHash := hashStrings(canonical)
		nextFetchAt := plannedNextFetchAt(record, now)
		query := fmt.Sprintf(`INSERT INTO ops.crawl_frontier (frontier_id, source_id, domain, url, canonical_url, discovery_kind, url_hash, priority, state, discovered_at, next_fetch_at)
VALUES (%s,%s,%s,%s,%s,%s,%s,%d,%s,%s,%s)`,
			sqlString(frontierID),
			sqlString(record.SourceID),
			sqlString(record.Domain),
			sqlString(canonical),
			sqlString(canonical),
			sqlString("entrypoint"),
			sqlString(urlHash),
			100,
			sqlString("pending"),
			sqlTime(now),
			sqlTime(nextFetchAt),
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
	query := fmt.Sprintf(`SELECT max(next_fetch_at) FROM ops.crawl_frontier WHERE source_id = %s FORMAT TabSeparated`, sqlString(record.SourceID))
	output, err := runner.Query(ctx, query)
	if err != nil {
		if strings.Contains(err.Error(), "UNKNOWN_TABLE") {
			return true, "", nil
		}
		return false, "", err
	}
	trimmed := strings.TrimSpace(output)
	if trimmed == "" || trimmed == "\\N" || trimmed == "null" || trimmed == "0000-00-00 00:00:00" {
		return true, "", nil
	}
	nextFetchAt, err := parseClickHouseTime(trimmed)
	if err != nil {
		return false, "", err
	}
	if !nextFetchAt.After(now.UTC()) {
		return true, "", nil
	}
	if strings.TrimSpace(record.CrawlStrategy) == "full" && nextFetchAt.Sub(now.UTC()) <= window/2 {
		return true, "", nil
	}
	return false, fmt.Sprintf("not due until %s", nextFetchAt.UTC().Format(time.RFC3339)), nil
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
