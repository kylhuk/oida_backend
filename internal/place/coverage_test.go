package place

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

const coverageArtifactPath = ".sisyphus/evidence/task-9-place-coverage.json"

type placeCoverageReport struct {
	SchemaVersion     int                 `json:"schema_version"`
	Countries         []countryCoverage   `json:"countries"`
	AmbiguousNames    map[string][]string `json:"ambiguous_names"`
	InvalidPolygonIDs []string            `json:"invalid_polygon_ids"`
}

type countryCoverage struct {
	CountryCode         string          `json:"country_code"`
	DeepestAdminLevel   int             `json:"deepest_admin_level"`
	DepthAvailable      map[string]bool `json:"depth_available"`
	PlaceCount          int             `json:"place_count"`
	InvalidPolygonCount int             `json:"invalid_polygon_count"`
}

func TestPlaceCoverageReport(t *testing.T) {
	resolver := mustFixtureResolver(t)
	report := buildCoverageReport(resolver)

	byCountry := map[string]countryCoverage{}
	for _, country := range report.Countries {
		byCountry[country.CountryCode] = country
	}

	assertDepths(t, byCountry["FR"], 2, map[string]bool{"adm0": true, "adm1": true, "adm2": true, "adm3": false, "adm4": false})
	assertDepths(t, byCountry["US"], 2, map[string]bool{"adm0": true, "adm1": true, "adm2": true, "adm3": false, "adm4": false})
	assertDepths(t, byCountry["NR"], 1, map[string]bool{"adm0": true, "adm1": true, "adm2": false, "adm3": false, "adm4": false})
	assertDepths(t, byCountry["OV"], 2, map[string]bool{"adm0": true, "adm1": true, "adm2": true, "adm3": false, "adm4": false})
	assertDepths(t, byCountry["IV"], 0, map[string]bool{"adm0": true, "adm1": false, "adm2": false, "adm3": false, "adm4": false})

	if got := byCountry["IV"].InvalidPolygonCount; got != 1 {
		t.Fatalf("expected IV to report 1 invalid polygon, got %d", got)
	}
	parisMatches, ok := report.AmbiguousNames["paris"]
	if !ok {
		t.Fatal("expected ambiguous name report to include paris")
	}
	if len(parisMatches) != 2 {
		t.Fatalf("expected paris ambiguity to include 2 matches, got %d", len(parisMatches))
	}
	if report.InvalidPolygonIDs[0] != "plc:ivl-bowtie" {
		t.Fatalf("expected invalid polygon list to include plc:ivl-bowtie, got %v", report.InvalidPolygonIDs)
	}

	artifactPath := filepath.Join(mustRepoRoot(t), coverageArtifactPath)
	if err := os.MkdirAll(filepath.Dir(artifactPath), 0o755); err != nil {
		t.Fatalf("mkdir artifact dir: %v", err)
	}
	b, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatalf("marshal coverage report: %v", err)
	}
	if err := os.WriteFile(artifactPath, b, 0o644); err != nil {
		t.Fatalf("write coverage artifact: %v", err)
	}
	if _, err := os.Stat(artifactPath); err != nil {
		t.Fatalf("stat coverage artifact: %v", err)
	}
}

func buildCoverageReport(resolver *fixtureResolver) placeCoverageReport {
	countries := map[string]*countryCoverage{}
	for _, place := range resolver.places {
		entry := countries[place.CountryCode]
		if entry == nil {
			entry = &countryCoverage{
				CountryCode:       place.CountryCode,
				DeepestAdminLevel: place.AdminLevel,
				DepthAvailable:    map[string]bool{"adm0": false, "adm1": false, "adm2": false, "adm3": false, "adm4": false},
			}
			countries[place.CountryCode] = entry
		}
		entry.PlaceCount++
		if place.AdminLevel > entry.DeepestAdminLevel {
			entry.DeepestAdminLevel = place.AdminLevel
		}
		entry.DepthAvailable[depthKey(place.AdminLevel)] = true
	}
	for countryCode, count := range resolver.invalidByCountryCode {
		entry := countries[countryCode]
		if entry == nil {
			entry = &countryCoverage{
				CountryCode:    countryCode,
				DepthAvailable: map[string]bool{"adm0": false, "adm1": false, "adm2": false, "adm3": false, "adm4": false},
			}
			countries[countryCode] = entry
		}
		entry.InvalidPolygonCount = count
	}

	report := placeCoverageReport{
		SchemaVersion:     1,
		AmbiguousNames:    map[string][]string{},
		InvalidPolygonIDs: append([]string(nil), resolver.invalidPolygonIDs...),
	}

	for name, matches := range resolver.nameIndex {
		if len(matches) < 2 {
			continue
		}
		ids := make([]string, len(matches))
		for i, match := range matches {
			ids[i] = match.ID
		}
		report.AmbiguousNames[name] = ids
	}

	for _, entry := range countries {
		report.Countries = append(report.Countries, *entry)
	}
	sort.Slice(report.Countries, func(i, j int) bool {
		return report.Countries[i].CountryCode < report.Countries[j].CountryCode
	})
	return report
}

func assertDepths(t *testing.T, country countryCoverage, wantDeepest int, want map[string]bool) {
	t.Helper()
	if country.DeepestAdminLevel != wantDeepest {
		t.Fatalf("expected %s deepest depth %d, got %d", country.CountryCode, wantDeepest, country.DeepestAdminLevel)
	}
	for level, wantPresent := range want {
		if got := country.DepthAvailable[level]; got != wantPresent {
			t.Fatalf("expected %s %s=%t, got %t", country.CountryCode, level, wantPresent, got)
		}
	}
}

func depthKey(level int) string {
	return "adm" + string(rune('0'+level))
}

func mustRepoRoot(tb testing.TB) string {
	tb.Helper()
	wd, err := os.Getwd()
	if err != nil {
		tb.Fatalf("getwd: %v", err)
	}
	for dir := wd; dir != filepath.Dir(dir); dir = filepath.Dir(dir) {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
	}
	tb.Fatal("unable to locate repo root from working directory")
	return ""
}
