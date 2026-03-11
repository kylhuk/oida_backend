package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"global-osint-backend/internal/migrate"
)

func TestIngestDomainJobOrchestratesSources(t *testing.T) {
	queries := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query().Get("query")
		queries = append(queries, query)
		if strings.Contains(query, "FROM meta.source_registry FINAL") {
			_, _ = w.Write([]byte(mockSourceRegistryJSONLines(extractQuotedValues(query))))
			return
		}
		if strings.Contains(query, "SELECT count() FROM ops.crawl_frontier") {
			_, _ = w.Write([]byte("0\n"))
			return
		}
		if strings.Contains(query, "SELECT max(next_fetch_at) FROM ops.crawl_frontier") {
			_, _ = w.Write([]byte("\\N\n"))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	t.Setenv("CLICKHOUSE_HTTP_URL", server.URL)
	stdout := &strings.Builder{}
	stderr := &strings.Builder{}
	if code := run([]string{"run-once", "--job", ingestGeopoliticalJobName}, stdout, stderr); code != 0 {
		t.Fatalf("expected zero exit code, got %d stderr=%s", code, stderr.String())
	}
	if code := run([]string{"run-once", "--job", ingestSafetySecurityJobName, "--source-id", "fixture:safety"}, stdout, stderr); code != 0 {
		t.Fatalf("expected zero exit code for safety alias, got %d stderr=%s", code, stderr.String())
	}
	joined := strings.Join(queries, "\n")
	for _, want := range []string{
		"SELECT source_id, catalog_kind, lifecycle_state, review_status, domain, entrypoints, transport_type, crawl_enabled, refresh_strategy, crawl_strategy, crawl_config_json, bronze_table, enabled, disabled_reason",
		"INSERT INTO ops.crawl_frontier",
		"orchestrated fetch stage",
		"orchestrated parse stage",
		"orchestrated promote stage",
		"orchestrated geopolitical http sources",
		"orchestrated safety/security http sources",
		"seed:gdelt",
		"fixture:reliefweb",
		"fixture:opensanctions",
		"fixture:nasa-firms",
		"fixture:noaa-hazards",
		"fixture:kev",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("expected query fragment %q, got %s", want, joined)
		}
	}
	if strings.Contains(joined, "fixture:safety','public-safety-fixtures.local") {
		t.Fatalf("fixture:safety alias should not seed frontier directly, got %s", joined)
	}
	if !strings.Contains(stdout.String(), ingestGeopoliticalJobName) || !strings.Contains(stdout.String(), ingestSafetySecurityJobName) {
		t.Fatalf("expected completion output for public job names, got %s", stdout.String())
	}
}

func TestGeopoliticalJobSkipsACLEDWithoutCredential(t *testing.T) {
	queries := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query().Get("query")
		queries = append(queries, query)
		if strings.Contains(query, "FROM meta.source_registry FINAL") {
			_, _ = w.Write([]byte(mockSourceRegistryJSONLines(extractQuotedValues(query))))
			return
		}
		if strings.Contains(query, "SELECT count() FROM ops.crawl_frontier") {
			_, _ = w.Write([]byte("0\n"))
			return
		}
		if strings.Contains(query, "SELECT max(next_fetch_at) FROM ops.crawl_frontier") {
			_, _ = w.Write([]byte("\\N\n"))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	t.Setenv("CLICKHOUSE_HTTP_URL", server.URL)
	t.Setenv("ACLED_API_KEY", "")
	ctx := context.WithValue(context.Background(), jobOptionsContextKey{}, jobOptions{})
	if err := runIngestGeopolitical(ctx); err != nil {
		t.Fatalf("runIngestGeopolitical: %v", err)
	}
	joined := strings.Join(queries, "\n")
	if strings.Contains(joined, "orchestrated fetch stage for fixture:acled") {
		t.Fatalf("expected ACLED fetch stage to be skipped without credential, got %s", joined)
	}
	if !strings.Contains(joined, `"source_id":"fixture:acled","reason":"missing credential ACLED_API_KEY"`) {
		t.Fatalf("expected disabled_sources stats to include gated ACLED, got %s", joined)
	}
	for _, want := range []string{"orchestrated fetch stage for seed:gdelt", "orchestrated fetch stage for fixture:reliefweb"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("expected active source stage %q, got %s", want, joined)
		}
	}
}

func TestFrontierDedupe(t *testing.T) {
	queries := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query().Get("query")
		queries = append(queries, query)
		switch {
		case strings.Contains(query, "SELECT count() FROM ops.crawl_frontier"):
			_, _ = w.Write([]byte("1\n"))
		case strings.Contains(query, "SELECT max(next_fetch_at) FROM ops.crawl_frontier"):
			_, _ = w.Write([]byte("2026-03-10 00:00:00.000\n"))
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()
	ctx := context.Background()
	runner := migrate.NewHTTPRunner(server.URL)
	record := sourceRuntimeRecord{SourceID: "seed:gdelt", Domain: "gdeltproject.org", Entrypoints: []string{"https://www.gdeltproject.org/data.html?utm_source=test"}, RefreshStrategy: "frequent"}
	seeded, err := seedFrontier(ctx, runner, record, time.Date(2026, time.March, 10, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("seedFrontier: %v", err)
	}
	if seeded != 0 {
		t.Fatalf("expected deduped frontier insert count 0, got %d", seeded)
	}
	joined := strings.Join(queries, "\n")
	if !strings.Contains(joined, "canonical_url = 'https://www.gdeltproject.org/data.html'") {
		t.Fatalf("expected dedupe query to use normalized canonical URL, got %s", joined)
	}
	if strings.Contains(joined, "INSERT INTO ops.crawl_frontier") {
		t.Fatalf("expected no frontier insert when row already exists, got %s", joined)
	}
}

func TestAutomaticSyncPlanner(t *testing.T) {
	queries := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query().Get("query")
		queries = append(queries, query)
		switch {
		case strings.Contains(query, "FROM meta.source_registry FINAL"):
			_, _ = w.Write([]byte(mockSourceRegistryJSONLines(extractQuotedValues(query))))
		case strings.Contains(query, "SELECT count() FROM ops.crawl_frontier"):
			_, _ = w.Write([]byte("0\n"))
		case strings.Contains(query, "SELECT max(next_fetch_at) FROM ops.crawl_frontier WHERE source_id = 'seed:gdelt'"):
			_, _ = w.Write([]byte("2026-03-10 11:30:00.000\n"))
		case strings.Contains(query, "SELECT max(next_fetch_at) FROM ops.crawl_frontier WHERE source_id = 'fixture:reliefweb'"):
			_, _ = w.Write([]byte("2099-03-10 18:00:00.000\n"))
		case strings.Contains(query, "SELECT max(next_fetch_at) FROM ops.crawl_frontier WHERE source_id = 'fixture:acled'"):
			_, _ = w.Write([]byte("\\N\n"))
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()
	t.Setenv("CLICKHOUSE_HTTP_URL", server.URL)
	t.Setenv("ACLED_API_KEY", "demo-key")
	ctx := context.WithValue(context.Background(), jobOptionsContextKey{}, jobOptions{})
	if err := runIngestGeopolitical(ctx); err != nil {
		t.Fatalf("runIngestGeopolitical: %v", err)
	}
	joined := strings.Join(queries, "\n")
	if !strings.Contains(joined, "orchestrated fetch stage for seed:gdelt") {
		t.Fatalf("expected due source seed:gdelt to execute, got %s", joined)
	}
	if !strings.Contains(joined, "orchestrated fetch stage for fixture:acled") {
		t.Fatalf("expected unscheduled source fixture:acled to execute, got %s", joined)
	}
	if strings.Contains(joined, "orchestrated fetch stage for fixture:reliefweb") {
		t.Fatalf("expected not-due source fixture:reliefweb to skip execution, got %s", joined)
	}
	if !strings.Contains(joined, `"source_id":"fixture:reliefweb","reason":"not due until 2099-03-10T18:00:00Z"`) {
		t.Fatalf("expected planner skip reason for reliefweb, got %s", joined)
	}
}

func mockSourceRegistryJSONLines(sourceIDs []string) string {
	records := map[string]sourceRuntimeRecord{
		"seed:gdelt": {
			SourceID:        "seed:gdelt",
			CatalogKind:     "concrete",
			LifecycleState:  "approved_enabled",
			ReviewStatus:    "approved",
			Domain:          "gdeltproject.org",
			Entrypoints:     []string{"https://www.gdeltproject.org/data.html"},
			TransportType:   "http",
			CrawlEnabled:    1,
			RefreshStrategy: "frequent",
			CrawlStrategy:   "delta",
			CrawlConfigJSON: `{}`,
			Enabled:         1,
			BronzeTable:     stringPointer("bronze.src_seed_gdelt_v1"),
		},
		"fixture:reliefweb": {
			SourceID:        "fixture:reliefweb",
			CatalogKind:     "concrete",
			LifecycleState:  "approved_enabled",
			ReviewStatus:    "approved",
			Domain:          "reliefweb.int",
			Entrypoints:     []string{"https://reliefweb.int/help/api"},
			TransportType:   "http",
			CrawlEnabled:    1,
			RefreshStrategy: "frequent",
			CrawlStrategy:   "delta",
			CrawlConfigJSON: `{}`,
			Enabled:         1,
			BronzeTable:     stringPointer("bronze.src_fixture_reliefweb_v1"),
		},
		"fixture:acled": {
			SourceID:        "fixture:acled",
			CatalogKind:     "concrete",
			LifecycleState:  "approved_enabled",
			ReviewStatus:    "approved",
			Domain:          "acleddata.com",
			Entrypoints:     []string{"https://acleddata.com/"},
			TransportType:   "http",
			CrawlEnabled:    1,
			RefreshStrategy: "scheduled",
			CrawlStrategy:   "delta",
			CrawlConfigJSON: `{"credential_gate":true}`,
			Enabled:         1,
			BronzeTable:     stringPointer("bronze.src_fixture_acled_v1"),
		},
		"fixture:safety": {
			SourceID:        "fixture:safety",
			CatalogKind:     "concrete",
			LifecycleState:  "approved_enabled",
			ReviewStatus:    "approved",
			Domain:          "public-safety-fixtures.local",
			Entrypoints:     []string{"https://fixtures.local/safety"},
			TransportType:   "bundle_alias",
			CrawlEnabled:    0,
			RefreshStrategy: "scheduled",
			CrawlStrategy:   "bundle_alias",
			CrawlConfigJSON: `{"source_aliases":["fixture:opensanctions","fixture:nasa-firms","fixture:noaa-hazards","fixture:kev"]}`,
			Enabled:         1,
		},
		"fixture:opensanctions": {
			SourceID:        "fixture:opensanctions",
			CatalogKind:     "concrete",
			LifecycleState:  "approved_enabled",
			ReviewStatus:    "approved",
			Domain:          "opensanctions.org",
			Entrypoints:     []string{"https://www.opensanctions.org/"},
			TransportType:   "http",
			CrawlEnabled:    1,
			RefreshStrategy: "scheduled",
			CrawlStrategy:   "delta",
			CrawlConfigJSON: `{}`,
			Enabled:         1,
			BronzeTable:     stringPointer("bronze.src_fixture_opensanctions_v1"),
		},
		"fixture:nasa-firms": {
			SourceID:        "fixture:nasa-firms",
			CatalogKind:     "concrete",
			LifecycleState:  "approved_enabled",
			ReviewStatus:    "approved",
			Domain:          "earthdata.nasa.gov",
			Entrypoints:     []string{"https://www.earthdata.nasa.gov/learn/find-data/near-real-time/firms"},
			TransportType:   "http",
			CrawlEnabled:    1,
			RefreshStrategy: "frequent",
			CrawlStrategy:   "delta",
			CrawlConfigJSON: `{}`,
			Enabled:         1,
			BronzeTable:     stringPointer("bronze.src_fixture_nasa_firms_v1"),
		},
		"fixture:noaa-hazards": {
			SourceID:        "fixture:noaa-hazards",
			CatalogKind:     "concrete",
			LifecycleState:  "approved_enabled",
			ReviewStatus:    "approved",
			Domain:          "weather.gov",
			Entrypoints:     []string{"https://www.weather.gov/"},
			TransportType:   "http",
			CrawlEnabled:    1,
			RefreshStrategy: "scheduled",
			CrawlStrategy:   "delta",
			CrawlConfigJSON: `{}`,
			Enabled:         1,
			BronzeTable:     stringPointer("bronze.src_fixture_noaa_hazards_v1"),
		},
		"fixture:kev": {
			SourceID:        "fixture:kev",
			CatalogKind:     "concrete",
			LifecycleState:  "approved_enabled",
			ReviewStatus:    "approved",
			Domain:          "cisa.gov",
			Entrypoints:     []string{"https://www.cisa.gov/known-exploited-vulnerabilities-catalog"},
			TransportType:   "http",
			CrawlEnabled:    1,
			RefreshStrategy: "scheduled",
			CrawlStrategy:   "delta",
			CrawlConfigJSON: `{}`,
			Enabled:         1,
			BronzeTable:     stringPointer("bronze.src_fixture_kev_v1"),
		},
	}
	filtered := make([]string, 0, len(sourceIDs))
	for _, sourceID := range sourceIDs {
		if _, ok := records[sourceID]; ok {
			filtered = append(filtered, sourceID)
		}
	}
	if len(filtered) == 0 {
		filtered = []string{"seed:gdelt", "fixture:reliefweb", "fixture:acled", "fixture:safety", "fixture:opensanctions", "fixture:nasa-firms", "fixture:noaa-hazards", "fixture:kev"}
	}
	lines := make([]string, 0, len(filtered))
	for _, sourceID := range filtered {
		payload, _ := json.Marshal(records[sourceID])
		lines = append(lines, string(payload))
	}
	return strings.Join(lines, "\n")
}

func TestAutomaticSyncPlannerSkipsNonRunnableGovernanceState(t *testing.T) {
	queries := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query().Get("query")
		queries = append(queries, query)
		if strings.Contains(query, "FROM meta.source_registry FINAL") {
			lines := mockSourceRegistryJSONLines([]string{"seed:gdelt"})
			lines = strings.Replace(lines, `"lifecycle_state":"approved_enabled"`, `"lifecycle_state":"review_required"`, 1)
			_, _ = w.Write([]byte(lines))
			return
		}
		if strings.Contains(query, "SELECT max(next_fetch_at) FROM ops.crawl_frontier") {
			_, _ = w.Write([]byte("\\N\n"))
			return
		}
		if strings.Contains(query, "SELECT count() FROM ops.crawl_frontier") {
			_, _ = w.Write([]byte("0\n"))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	t.Setenv("CLICKHOUSE_HTTP_URL", server.URL)
	ctx := context.WithValue(context.Background(), jobOptionsContextKey{}, jobOptions{SourceID: "seed:gdelt"})
	if err := runIngestGeopolitical(ctx); err != nil {
		t.Fatalf("runIngestGeopolitical: %v", err)
	}
	joined := strings.Join(queries, "\n")
	if strings.Contains(joined, "INSERT INTO ops.crawl_frontier") {
		t.Fatalf("expected non-runnable governance source to skip frontier seeding, got %s", joined)
	}
	if !strings.Contains(joined, `"source_id":"seed:gdelt","reason":"source lifecycle review_required not runnable"`) {
		t.Fatalf("expected governance skip reason, got %s", joined)
	}
}

func TestAutomaticSyncPlannerSkipsNonConcreteCatalogKind(t *testing.T) {
	queries := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query().Get("query")
		queries = append(queries, query)
		if strings.Contains(query, "FROM meta.source_registry FINAL") {
			lines := mockSourceRegistryJSONLines([]string{"seed:gdelt"})
			lines = strings.Replace(lines, `"catalog_kind":"concrete"`, `"catalog_kind":"fingerprint"`, 1)
			_, _ = w.Write([]byte(lines))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	t.Setenv("CLICKHOUSE_HTTP_URL", server.URL)
	ctx := context.WithValue(context.Background(), jobOptionsContextKey{}, jobOptions{SourceID: "seed:gdelt"})
	if err := runIngestGeopolitical(ctx); err != nil {
		t.Fatalf("runIngestGeopolitical: %v", err)
	}
	joined := strings.Join(queries, "\n")
	if strings.Contains(joined, "INSERT INTO ops.crawl_frontier") {
		t.Fatalf("expected non-concrete source to skip frontier seeding, got %s", joined)
	}
	if !strings.Contains(joined, `"source_id":"seed:gdelt","reason":"source is not a concrete runtime source"`) {
		t.Fatalf("expected catalog kind skip reason, got %s", joined)
	}
}

func TestAutomaticSyncPlannerSkipsMissingReviewStatus(t *testing.T) {
	queries := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query().Get("query")
		queries = append(queries, query)
		if strings.Contains(query, "FROM meta.source_registry FINAL") {
			lines := mockSourceRegistryJSONLines([]string{"seed:gdelt"})
			lines = strings.Replace(lines, `"review_status":"approved"`, `"review_status":""`, 1)
			_, _ = w.Write([]byte(lines))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	t.Setenv("CLICKHOUSE_HTTP_URL", server.URL)
	ctx := context.WithValue(context.Background(), jobOptionsContextKey{}, jobOptions{SourceID: "seed:gdelt"})
	if err := runIngestGeopolitical(ctx); err != nil {
		t.Fatalf("runIngestGeopolitical: %v", err)
	}
	joined := strings.Join(queries, "\n")
	if strings.Contains(joined, "INSERT INTO ops.crawl_frontier") {
		t.Fatalf("expected blank review status source to skip frontier seeding, got %s", joined)
	}
	if !strings.Contains(joined, `"source_id":"seed:gdelt","reason":"source review status missing not runnable"`) {
		t.Fatalf("expected review status skip reason, got %s", joined)
	}
}

func TestAutomaticSyncPlannerSkipsUnsupportedStrategy(t *testing.T) {
	queries := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query().Get("query")
		queries = append(queries, query)
		if strings.Contains(query, "FROM meta.source_registry FINAL") {
			lines := mockSourceRegistryJSONLines([]string{"seed:gdelt"})
			lines = strings.Replace(lines, `"refresh_strategy":"frequent"`, `"refresh_strategy":"manual"`, 1)
			_, _ = w.Write([]byte(lines))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	t.Setenv("CLICKHOUSE_HTTP_URL", server.URL)
	ctx := context.WithValue(context.Background(), jobOptionsContextKey{}, jobOptions{SourceID: "seed:gdelt"})
	if err := runIngestGeopolitical(ctx); err != nil {
		t.Fatalf("runIngestGeopolitical: %v", err)
	}
	joined := strings.Join(queries, "\n")
	if strings.Contains(joined, "INSERT INTO ops.crawl_frontier") {
		t.Fatalf("expected unsupported strategy source to skip frontier seeding, got %s", joined)
	}
	if !strings.Contains(joined, `"source_id":"seed:gdelt","reason":"unsupported refresh strategy manual"`) {
		t.Fatalf("expected unsupported strategy skip reason, got %s", joined)
	}
}

func TestAutomaticSyncPlannerSkipsUnsupportedCrawlStrategy(t *testing.T) {
	queries := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query().Get("query")
		queries = append(queries, query)
		if strings.Contains(query, "FROM meta.source_registry FINAL") {
			lines := mockSourceRegistryJSONLines([]string{"seed:gdelt"})
			lines = strings.Replace(lines, `"crawl_strategy":"delta"`, `"crawl_strategy":"snapshot"`, 1)
			_, _ = w.Write([]byte(lines))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	t.Setenv("CLICKHOUSE_HTTP_URL", server.URL)
	ctx := context.WithValue(context.Background(), jobOptionsContextKey{}, jobOptions{SourceID: "seed:gdelt"})
	if err := runIngestGeopolitical(ctx); err != nil {
		t.Fatalf("runIngestGeopolitical: %v", err)
	}
	joined := strings.Join(queries, "\n")
	if strings.Contains(joined, "INSERT INTO ops.crawl_frontier") {
		t.Fatalf("expected unsupported crawl strategy source to skip frontier seeding, got %s", joined)
	}
	if !strings.Contains(joined, `"source_id":"seed:gdelt","reason":"unsupported crawl strategy snapshot"`) {
		t.Fatalf("expected unsupported crawl strategy skip reason, got %s", joined)
	}
}

func TestAutomaticSyncPlannerRerunStaysIdempotent(t *testing.T) {
	type frontierState struct {
		exists    map[string]bool
		nextFetch map[string]string
	}
	state := frontierState{exists: map[string]bool{}, nextFetch: map[string]string{}}
	queries := []string{}
	reSource := regexp.MustCompile(`source_id = '([^']+)'`)
	reCanonical := regexp.MustCompile(`canonical_url = '([^']+)'`)
	reInsert := regexp.MustCompile(`VALUES \('([^']+)','([^']+)','([^']+)','([^']+)','([^']+)','([^']+)'`)
	reNextFetch := regexp.MustCompile(`,'([0-9:-]{19}\.[0-9]{3})'\)$`)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query().Get("query")
		queries = append(queries, query)
		switch {
		case strings.Contains(query, "FROM meta.source_registry FINAL"):
			_, _ = w.Write([]byte(mockSourceRegistryJSONLines(extractQuotedValues(query))))
		case strings.Contains(query, "SELECT count() FROM ops.crawl_frontier"):
			source := reSource.FindStringSubmatch(query)[1]
			canonical := reCanonical.FindStringSubmatch(query)[1]
			if state.exists[source+"|"+canonical] {
				_, _ = w.Write([]byte("1\n"))
			} else {
				_, _ = w.Write([]byte("0\n"))
			}
		case strings.Contains(query, "SELECT max(next_fetch_at) FROM ops.crawl_frontier"):
			source := reSource.FindStringSubmatch(query)[1]
			if next, ok := state.nextFetch[source]; ok {
				_, _ = w.Write([]byte(next + "\n"))
			} else {
				_, _ = w.Write([]byte("\\N\n"))
			}
		case strings.Contains(query, "INSERT INTO ops.crawl_frontier"):
			m := reInsert.FindStringSubmatch(query)
			state.exists[m[2]+"|"+m[5]] = true
			if next := reNextFetch.FindStringSubmatch(query); len(next) == 2 {
				state.nextFetch[m[2]] = next[1]
			}
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()
	t.Setenv("CLICKHOUSE_HTTP_URL", server.URL)
	t.Setenv("ACLED_API_KEY", "demo-key")
	ctx := context.WithValue(context.Background(), jobOptionsContextKey{}, jobOptions{})
	if err := runIngestGeopolitical(ctx); err != nil {
		t.Fatalf("first runIngestGeopolitical: %v", err)
	}
	if err := runIngestGeopolitical(ctx); err != nil {
		t.Fatalf("second runIngestGeopolitical: %v", err)
	}
	insertCount := 0
	for _, query := range queries {
		if strings.Contains(query, "INSERT INTO ops.crawl_frontier") {
			insertCount++
		}
	}
	if insertCount != 3 {
		t.Fatalf("expected exactly 3 frontier inserts across two planner runs, got %d", insertCount)
	}
}

func extractQuotedValues(query string) []string {
	re := regexp.MustCompile(`'([^']+)'`)
	matches := re.FindAllStringSubmatch(query, -1)
	values := make([]string, 0, len(matches))
	for _, match := range matches {
		if len(match) < 2 {
			continue
		}
		values = append(values, match[1])
	}
	return slices.Compact(values)
}

func stringPointer(value string) *string {
	return &value
}
