package dashboardstats

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

const FreshnessThresholdSeconds = 600

type Querier interface {
	Query(ctx context.Context, query string) (string, error)
}

type Report struct {
	GeneratedAt string   `json:"generated_at"`
	Warnings    []string `json:"warnings"`
	Summary     Summary  `json:"summary"`
	Storage     Storage  `json:"storage"`
	Quality     Quality  `json:"quality"`
	Outputs     Outputs  `json:"outputs"`
}

type Summary struct {
	SourcesTotal           uint64 `json:"sources_total"`
	SourcesEnabled         uint64 `json:"sources_enabled"`
	SourcesDisabled        uint64 `json:"sources_disabled"`
	CatalogTotal           uint64 `json:"catalog_total"`
	CatalogConcrete        uint64 `json:"catalog_concrete"`
	CatalogFingerprint     uint64 `json:"catalog_fingerprint"`
	CatalogFamily          uint64 `json:"catalog_family"`
	CatalogRunnable        uint64 `json:"catalog_runnable"`
	CatalogDeferred        uint64 `json:"catalog_deferred"`
	CatalogCredentialGated uint64 `json:"catalog_credential_gated"`
	JobsRunning            uint64 `json:"jobs_running"`
	FrontierPending        uint64 `json:"frontier_pending"`
	FrontierRetry          uint64 `json:"frontier_retry"`
	UnresolvedOpen         uint64 `json:"unresolved_open"`
	QualityOpen            uint64 `json:"quality_open"`
}

type TableRow struct {
	TableName string `json:"table_name"`
	Rows      uint64 `json:"rows"`
	CountMode string `json:"count_mode"`
}

type Storage struct {
	TableRows        []TableRow `json:"table_rows"`
	SourceBronzeRows []TableRow `json:"source_bronze_rows"`
}

type FreshnessSource struct {
	SourceID         string `json:"source_id"`
	FreshnessSeconds uint64 `json:"freshness_seconds"`
	LagReason        string `json:"lag_reason"`
}

type Freshness struct {
	ThresholdSeconds     uint64            `json:"threshold_seconds"`
	SourcesOverThreshold uint64            `json:"sources_over_threshold"`
	MedianLagSeconds     uint64            `json:"median_lag_seconds"`
	MaxLagSeconds        uint64            `json:"max_lag_seconds"`
	Sources              []FreshnessSource `json:"sources"`
}

type FailureBreakdown struct {
	ErrorClass    string `json:"error_class"`
	Count         uint64 `json:"count"`
	ExampleSource string `json:"example_source"`
}

type ParserSuccess struct {
	WindowMinutes uint64             `json:"window_minutes"`
	TotalRuns     uint64             `json:"total_runs"`
	SuccessRuns   uint64             `json:"success_runs"`
	SuccessRate   float64            `json:"success_rate"`
	Failures      []FailureBreakdown `json:"failures"`
}

type FetchQuality struct {
	WindowHours uint64 `json:"window_hours"`
	Success     uint64 `json:"success"`
	Failed      uint64 `json:"failed"`
}

type Quality struct {
	Freshness     Freshness     `json:"freshness"`
	ParserSuccess ParserSuccess `json:"parser_success"`
	Fetch         FetchQuality  `json:"fetch"`
}

type Outputs struct {
	MetricsTotal     uint64  `json:"metrics_total"`
	LatestSnapshotAt *string `json:"latest_snapshot_at"`
	HotspotsTotal    uint64  `json:"hotspots_total"`
	CrossDomainTotal uint64  `json:"cross_domain_total"`
}

func Collect(ctx context.Context, q Querier, now time.Time) (Report, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	report := Report{GeneratedAt: now.UTC().Format(time.RFC3339Nano), Warnings: []string{}}

	if err := collectSummary(ctx, q, &report); err != nil {
		return Report{}, err
	}
	if err := collectStorage(ctx, q, &report); err != nil {
		report.Warnings = append(report.Warnings, "storage_partial: "+err.Error())
	}
	if err := collectQuality(ctx, q, &report); err != nil {
		report.Warnings = append(report.Warnings, "quality_partial: "+err.Error())
	}
	if err := collectOutputs(ctx, q, &report); err != nil {
		report.Warnings = append(report.Warnings, "outputs_partial: "+err.Error())
	}
	return report, nil
}

func collectSummary(ctx context.Context, q Querier, report *Report) error {
	sourceQuery := `SELECT count() AS sources_total, countIf(enabled = 1) AS sources_enabled, countIf(enabled = 0) AS sources_disabled FROM meta.source_registry FORMAT JSONEachRow`
	row, err := queryOne(ctx, q, sourceQuery)
	if err != nil {
		return err
	}
	report.Summary.SourcesTotal = asUInt(row["sources_total"])
	report.Summary.SourcesEnabled = asUInt(row["sources_enabled"])
	report.Summary.SourcesDisabled = asUInt(row["sources_disabled"])

	catalogQuery := `SELECT count() AS catalog_total,
	countIf(catalog_kind = 'concrete') AS catalog_concrete,
	countIf(catalog_kind = 'fingerprint') AS catalog_fingerprint,
	countIf(catalog_kind = 'family') AS catalog_family,
	countIf(catalog_kind = 'concrete' AND runtime_source_id IS NOT NULL AND runtime_source_id != '') AS catalog_runnable,
	countIf(catalog_kind = 'concrete' AND (runtime_source_id IS NULL OR runtime_source_id = '')) AS catalog_deferred,
	countIf(catalog_kind = 'concrete' AND (
		JSONExtractBool(attrs, 'credential_requirement', 'requires_registration') = 1 OR
		JSONExtractBool(attrs, 'credential_requirement', 'requires_approval') = 1 OR
		JSONExtractBool(attrs, 'credential_requirement', 'commercial_terms') = 1 OR
		JSONExtractBool(attrs, 'credential_requirement', 'noncommercial_terms') = 1 OR
		JSONExtractBool(attrs, 'credential_requirement', 'restricted_access') = 1 OR
		JSONExtractString(attrs, 'auth_env_var') != ''
	)) AS catalog_credential_gated
FROM meta.source_catalog FORMAT JSONEachRow`
	catalogRow, err := queryOne(ctx, q, catalogQuery)
	if err != nil {
		return err
	}
	report.Summary.CatalogTotal = asUInt(catalogRow["catalog_total"])
	report.Summary.CatalogConcrete = asUInt(catalogRow["catalog_concrete"])
	report.Summary.CatalogFingerprint = asUInt(catalogRow["catalog_fingerprint"])
	report.Summary.CatalogFamily = asUInt(catalogRow["catalog_family"])
	report.Summary.CatalogRunnable = asUInt(catalogRow["catalog_runnable"])
	report.Summary.CatalogDeferred = asUInt(catalogRow["catalog_deferred"])
	report.Summary.CatalogCredentialGated = asUInt(catalogRow["catalog_credential_gated"])

	jobsQuery := `SELECT count() AS jobs_running FROM ops.job_run WHERE status IN ('running','in_progress','started') OR finished_at IS NULL FORMAT JSONEachRow`
	jobRow, err := queryOne(ctx, q, jobsQuery)
	if err != nil {
		return err
	}
	report.Summary.JobsRunning = asUInt(jobRow["jobs_running"])

	frontierQuery := `SELECT countIf(state = 'pending') AS frontier_pending, countIf(state = 'retry') AS frontier_retry FROM ops.crawl_frontier FORMAT JSONEachRow`
	frontierRow, err := queryOne(ctx, q, frontierQuery)
	if err != nil {
		return err
	}
	report.Summary.FrontierPending = asUInt(frontierRow["frontier_pending"])
	report.Summary.FrontierRetry = asUInt(frontierRow["frontier_retry"])

	unresolvedQuery := `SELECT countIf(state NOT IN ('resolved','discarded')) AS unresolved_open FROM ops.unresolved_location_queue FORMAT JSONEachRow`
	unresolvedRow, err := queryOne(ctx, q, unresolvedQuery)
	if err != nil {
		return err
	}
	report.Summary.UnresolvedOpen = asUInt(unresolvedRow["unresolved_open"])

	qualityQuery := `SELECT countIf(status NOT IN ('resolved','closed')) AS quality_open FROM ops.quality_incident FORMAT JSONEachRow`
	qualityRow, err := queryOne(ctx, q, qualityQuery)
	if err != nil {
		return err
	}
	report.Summary.QualityOpen = asUInt(qualityRow["quality_open"])
	return nil
}

func collectStorage(ctx context.Context, q Querier, report *Report) error {
	bronzeTables := make([]string, 0, len(sourceBronzeTables())+1)
	bronzeTables = append(bronzeTables, "'raw_document'")
	for _, table := range sourceBronzeTables() {
		bronzeTables = append(bronzeTables, fmt.Sprintf("'%s'", table))
	}
	query := `SELECT concat(database, '.', table) AS table_name, sum(rows) AS rows
FROM system.parts
WHERE active = 1
  AND (
    (database = 'bronze' AND table IN (` + strings.Join(bronzeTables, ",") + `))
    OR (database = 'silver' AND table IN ('fact_event','fact_observation'))
    OR (database = 'gold' AND table IN ('metric_snapshot','cross_domain_snapshot'))
  )
GROUP BY database, table
ORDER BY table_name ASC
FORMAT JSONEachRow`
	rows, err := queryRows(ctx, q, query)
	if err != nil {
		return err
	}
	tableRows := make([]TableRow, 0, len(rows))
	sourceRows := make([]TableRow, 0, len(rows))
	for _, row := range rows {
		name := asString(row["table_name"])
		item := TableRow{TableName: name, Rows: asUInt(row["rows"]), CountMode: "approximate"}
		tableRows = append(tableRows, item)
		if strings.HasPrefix(name, "bronze.src_") {
			sourceRows = append(sourceRows, item)
		}
	}
	report.Storage.TableRows = tableRows
	report.Storage.SourceBronzeRows = sourceRows
	return nil
}

func collectQuality(ctx context.Context, q Querier, report *Report) error {
	freshnessQuery := `SELECT s.source_id AS source_id, maxOrNull(r.fetched_at) AS last_fetched_at
FROM meta.source_registry s
LEFT JOIN bronze.raw_document r ON s.source_id = r.source_id AND r.status_code IN (200, 204)
GROUP BY s.source_id
ORDER BY s.source_id ASC
FORMAT JSONEachRow`
	rows, err := queryRows(ctx, q, freshnessQuery)
	if err != nil {
		return err
	}
	freshnessSources := make([]FreshnessSource, 0, len(rows))
	seconds := make([]uint64, 0, len(rows))
	var maxLag uint64
	var over uint64
	now := time.Now().UTC()
	for _, row := range rows {
		sourceID := asString(row["source_id"])
		lagReason := "ok"
		freshnessSeconds := uint64(0)
		lastFetched := asNullableTime(row["last_fetched_at"])
		if lastFetched == nil {
			lagReason = "never_fetched"
			freshnessSeconds = uint64(FreshnessThresholdSeconds + 1)
		} else {
			freshnessSeconds = uint64(now.Sub(lastFetched.UTC()).Seconds())
			if freshnessSeconds > FreshnessThresholdSeconds {
				lagReason = "stale"
			}
		}
		if freshnessSeconds > FreshnessThresholdSeconds {
			over++
		}
		if freshnessSeconds > maxLag {
			maxLag = freshnessSeconds
		}
		seconds = append(seconds, freshnessSeconds)
		freshnessSources = append(freshnessSources, FreshnessSource{SourceID: sourceID, FreshnessSeconds: freshnessSeconds, LagReason: lagReason})
	}
	sort.Slice(freshnessSources, func(i, j int) bool {
		if freshnessSources[i].FreshnessSeconds == freshnessSources[j].FreshnessSeconds {
			return freshnessSources[i].SourceID < freshnessSources[j].SourceID
		}
		return freshnessSources[i].FreshnessSeconds > freshnessSources[j].FreshnessSeconds
	})
	report.Quality.Freshness = Freshness{
		ThresholdSeconds:     FreshnessThresholdSeconds,
		SourcesOverThreshold: over,
		MedianLagSeconds:     median(seconds),
		MaxLagSeconds:        maxLag,
		Sources:              freshnessSources,
	}

	parserQuery := `SELECT count() AS total_runs, countIf(status = 'success') AS success_runs
FROM ops.parse_log
WHERE started_at >= now64(3) - INTERVAL 15 MINUTE
FORMAT JSONEachRow`
	parserRow, err := queryOne(ctx, q, parserQuery)
	if err != nil {
		return err
	}
	totalRuns := asUInt(parserRow["total_runs"])
	successRuns := asUInt(parserRow["success_runs"])
	rate := 0.0
	if totalRuns > 0 {
		rate = float64(successRuns) / float64(totalRuns)
	}

	failureQuery := `SELECT if(error_class = '', 'unknown', error_class) AS error_class, count() AS count, any(source_id) AS example_source
FROM ops.parse_log
WHERE started_at >= now64(3) - INTERVAL 15 MINUTE AND status != 'success'
GROUP BY error_class
ORDER BY count DESC, error_class ASC
LIMIT 5
FORMAT JSONEachRow`
	failureRows, err := queryRows(ctx, q, failureQuery)
	if err != nil {
		return err
	}
	failures := make([]FailureBreakdown, 0, len(failureRows))
	for _, row := range failureRows {
		failures = append(failures, FailureBreakdown{
			ErrorClass:    asString(row["error_class"]),
			Count:         asUInt(row["count"]),
			ExampleSource: asString(row["example_source"]),
		})
	}
	report.Quality.ParserSuccess = ParserSuccess{WindowMinutes: 15, TotalRuns: totalRuns, SuccessRuns: successRuns, SuccessRate: rate, Failures: failures}

	fetchQuery := `SELECT countIf(success = 1) AS success_count, countIf(success = 0) AS failed_count
FROM ops.fetch_log
WHERE fetched_at >= now64(3) - INTERVAL 24 HOUR
FORMAT JSONEachRow`
	fetchRow, err := queryOne(ctx, q, fetchQuery)
	if err != nil {
		return err
	}
	report.Quality.Fetch = FetchQuality{WindowHours: 24, Success: asUInt(fetchRow["success_count"]), Failed: asUInt(fetchRow["failed_count"])}
	return nil
}

func collectOutputs(ctx context.Context, q Querier, report *Report) error {
	metricsQuery := `SELECT countIf(enabled = 1) AS metrics_total FROM meta.metric_registry FORMAT JSONEachRow`
	metricsRow, err := queryOne(ctx, q, metricsQuery)
	if err != nil {
		return err
	}
	report.Outputs.MetricsTotal = asUInt(metricsRow["metrics_total"])

	latestQuery := `SELECT maxOrNull(snapshot_at) AS latest_snapshot_at FROM gold.metric_snapshot FORMAT JSONEachRow`
	latestRow, err := queryOne(ctx, q, latestQuery)
	if err != nil {
		return err
	}
	if parsed := asNullableTime(latestRow["latest_snapshot_at"]); parsed != nil {
		formatted := parsed.UTC().Format(time.RFC3339Nano)
		report.Outputs.LatestSnapshotAt = &formatted
	}

	hotspotQuery := `SELECT count() AS hotspots_total FROM gold.hotspot_snapshot FORMAT JSONEachRow`
	hotspotRow, err := queryOne(ctx, q, hotspotQuery)
	if err != nil {
		return err
	}
	report.Outputs.HotspotsTotal = asUInt(hotspotRow["hotspots_total"])

	crossQuery := `SELECT count() AS cross_domain_total FROM gold.cross_domain_snapshot FORMAT JSONEachRow`
	crossRow, err := queryOne(ctx, q, crossQuery)
	if err != nil {
		return err
	}
	report.Outputs.CrossDomainTotal = asUInt(crossRow["cross_domain_total"])
	return nil
}

func queryOne(ctx context.Context, q Querier, query string) (map[string]any, error) {
	rows, err := queryRows(ctx, q, query)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("no rows returned")
	}
	return rows[0], nil
}

func queryRows(ctx context.Context, q Querier, query string) ([]map[string]any, error) {
	out, err := q.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	trimmed := strings.TrimSpace(out)
	if trimmed == "" {
		return nil, nil
	}
	scanner := bufio.NewScanner(strings.NewReader(trimmed))
	scanner.Buffer(make([]byte, 0, 64*1024), 10*1024*1024)
	rows := make([]map[string]any, 0, 8)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var row map[string]any
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return rows, nil
}

func asUInt(value any) uint64 {
	switch typed := value.(type) {
	case float64:
		if typed < 0 {
			return 0
		}
		return uint64(typed)
	case int:
		if typed < 0 {
			return 0
		}
		return uint64(typed)
	case int64:
		if typed < 0 {
			return 0
		}
		return uint64(typed)
	case uint64:
		return typed
	case string:
		parsed, err := strconv.ParseUint(strings.TrimSpace(typed), 10, 64)
		if err != nil {
			return 0
		}
		return parsed
	default:
		return 0
	}
}

func asString(value any) string {
	if value == nil {
		return ""
	}
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	default:
		return strings.TrimSpace(fmt.Sprintf("%v", typed))
	}
}

func asNullableTime(value any) *time.Time {
	text := asString(value)
	if text == "" || strings.EqualFold(text, "null") {
		return nil
	}
	if parsed, err := time.Parse(time.RFC3339Nano, text); err == nil {
		v := parsed.UTC()
		return &v
	}
	if parsed, err := time.Parse(time.RFC3339, text); err == nil {
		v := parsed.UTC()
		return &v
	}
	if parsed, err := time.Parse("2006-01-02 15:04:05.999", text); err == nil {
		v := parsed.UTC()
		return &v
	}
	if parsed, err := time.Parse("2006-01-02 15:04:05", text); err == nil {
		v := parsed.UTC()
		return &v
	}
	return nil
}

func median(values []uint64) uint64 {
	if len(values) == 0 {
		return 0
	}
	cloned := append([]uint64(nil), values...)
	sort.Slice(cloned, func(i, j int) bool { return cloned[i] < cloned[j] })
	mid := len(cloned) / 2
	if len(cloned)%2 == 1 {
		return cloned[mid]
	}
	return (cloned[mid-1] + cloned[mid]) / 2
}
