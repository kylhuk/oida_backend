package dashboardstats

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type stubQuerier struct{}

func (stubQuerier) Query(_ context.Context, query string) (string, error) {
	switch {
	case strings.Contains(query, "FROM meta.source_registry") && strings.Contains(query, "sources_total"):
		return `{"sources_total":7,"sources_enabled":6,"sources_disabled":1}` + "\n", nil
	case strings.Contains(query, "FROM meta.source_catalog"):
		return `{"catalog_total":240,"catalog_concrete":206,"catalog_fingerprint":16,"catalog_family":18,"catalog_runnable":7,"catalog_deferred":199,"catalog_credential_gated":16}` + "\n", nil
	case strings.Contains(query, "FROM ops.job_run"):
		return `{"jobs_running":2}` + "\n", nil
	case strings.Contains(query, "FROM ops.crawl_frontier"):
		return `{"frontier_pending":12,"frontier_retry":3}` + "\n", nil
	case strings.Contains(query, "FROM ops.unresolved_location_queue"):
		return `{"unresolved_open":4}` + "\n", nil
	case strings.Contains(query, "FROM ops.quality_incident"):
		return `{"quality_open":1}` + "\n", nil
	case strings.Contains(query, "FROM system.parts"):
		return strings.Join([]string{
			`{"table_name":"bronze.raw_document","rows":100}`,
			`{"table_name":"bronze.src_seed_gdelt_v1","rows":60}`,
			`{"table_name":"gold.metric_snapshot","rows":20}`,
		}, "\n") + "\n", nil
	case strings.Contains(query, "maxOrNull(r.fetched_at)"):
		now := time.Now().UTC()
		fresh := now.Add(-5 * time.Minute).Format(time.RFC3339Nano)
		stale := now.Add(-20 * time.Minute).Format(time.RFC3339Nano)
		return strings.Join([]string{
			fmt.Sprintf(`{"source_id":"seed:gdelt","last_fetched_at":"%s"}`, fresh),
			fmt.Sprintf(`{"source_id":"fixture:reliefweb","last_fetched_at":"%s"}`, stale),
			`{"source_id":"fixture:acled","last_fetched_at":null}`,
		}, "\n") + "\n", nil
	case strings.Contains(query, "FROM ops.parse_log") && strings.Contains(query, "total_runs"):
		return `{"total_runs":10,"success_runs":8}` + "\n", nil
	case strings.Contains(query, "FROM ops.parse_log") && strings.Contains(query, "error_class"):
		return `{"error_class":"schema_drift","count":2,"example_source":"fixture:reliefweb"}` + "\n", nil
	case strings.Contains(query, "FROM ops.fetch_log"):
		return `{"success_count":25,"failed_count":5}` + "\n", nil
	case strings.Contains(query, "FROM meta.metric_registry"):
		return `{"metrics_total":9}` + "\n", nil
	case strings.Contains(query, "FROM gold.metric_snapshot"):
		return `{"latest_snapshot_at":"2026-03-10T10:00:00Z"}` + "\n", nil
	case strings.Contains(query, "FROM gold.hotspot_snapshot"):
		return `{"hotspots_total":15}` + "\n", nil
	case strings.Contains(query, "FROM gold.cross_domain_snapshot"):
		return `{"cross_domain_total":5}` + "\n", nil
	default:
		return "", fmt.Errorf("unexpected query: %s", query)
	}
}

func TestCollect(t *testing.T) {
	report, err := Collect(context.Background(), stubQuerier{}, time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("collect: %v", err)
	}
	if report.Summary.SourcesTotal != 7 || report.Summary.FrontierPending != 12 {
		t.Fatalf("unexpected summary: %#v", report.Summary)
	}
	if report.Summary.CatalogTotal != 240 || report.Summary.CatalogDeferred != 199 || report.Summary.CatalogCredentialGated != 16 {
		t.Fatalf("unexpected catalog summary: %#v", report.Summary)
	}
	if report.Quality.ParserSuccess.WindowMinutes != 15 || report.Quality.ParserSuccess.SuccessRuns != 8 {
		t.Fatalf("unexpected parser success: %#v", report.Quality.ParserSuccess)
	}
	if len(report.Storage.TableRows) == 0 || report.Storage.TableRows[0].CountMode != "approximate" {
		t.Fatalf("expected approximate table rows, got %#v", report.Storage.TableRows)
	}
	if report.Outputs.MetricsTotal != 9 || report.Outputs.CrossDomainTotal != 5 {
		t.Fatalf("unexpected outputs: %#v", report.Outputs)
	}
}

func TestSourceBronzeTablesGeneratedList(t *testing.T) {
	tables := sourceBronzeTables()
	if len(tables) != 7 {
		t.Fatalf("expected 7 generated source bronze tables, got %d", len(tables))
	}
	for _, expected := range []string{"src_seed_gdelt_v1", "src_fixture_reliefweb_v1", "src_fixture_acled_v1", "src_fixture_opensanctions_v1", "src_fixture_nasa_firms_v1", "src_fixture_noaa_hazards_v1", "src_fixture_kev_v1"} {
		found := false
		for _, table := range tables {
			if table == expected {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("expected generated bronze table %q in list", expected)
		}
	}
}

func TestSourceBronzeTablesGeneratedListMatchesCompiledManifest(t *testing.T) {
	type bronzeManifestRow struct {
		BronzeTable string `json:"bronze_table"`
	}
	type compiledCatalog struct {
		BronzeDDLManifest []bronzeManifestRow `json:"bronze_ddl_manifest"`
	}
	b, err := os.ReadFile(filepath.Join("..", "..", "seed", "source_catalog_compiled.json"))
	if err != nil {
		t.Fatalf("read compiled source catalog: %v", err)
	}
	var compiled compiledCatalog
	if err := json.Unmarshal(b, &compiled); err != nil {
		t.Fatalf("decode compiled source catalog: %v", err)
	}
	want := map[string]struct{}{}
	for _, row := range compiled.BronzeDDLManifest {
		want[strings.TrimPrefix(row.BronzeTable, "bronze.")] = struct{}{}
	}
	got := map[string]struct{}{}
	for _, table := range sourceBronzeTables() {
		got[table] = struct{}{}
	}
	if len(got) != len(want) {
		t.Fatalf("expected %d generated bronze tables, got %d", len(want), len(got))
	}
	for table := range want {
		if _, ok := got[table]; !ok {
			t.Fatalf("missing generated bronze table %q", table)
		}
	}
}
