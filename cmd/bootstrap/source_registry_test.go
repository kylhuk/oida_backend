package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestBuildSourceRegistryRecordsCreatesGovernedSeedRecord(t *testing.T) {
	now := time.Date(2026, time.March, 10, 12, 0, 0, 0, time.UTC)
	records, err := buildSourceRegistryRecords([]sourceSeed{sampleSourceSeed()}, map[string]sourceRegistryRecord{}, now)
	if err != nil {
		t.Fatalf("build records: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}

	record := records[0]
	if record.RecordVersion != 1 || record.Version != 1 {
		t.Fatalf("expected initial versions to be 1, got version=%d record_version=%d", record.Version, record.RecordVersion)
	}
	if record.SchemaVersion != sourceRegistrySchemaVersion {
		t.Fatalf("expected schema version %d, got %d", sourceRegistrySchemaVersion, record.SchemaVersion)
	}
	if record.RequestsPerMinute != 30 || record.BurstSize != 5 {
		t.Fatalf("expected governance rate limits from seed, got %d/%d", record.RequestsPerMinute, record.BurstSize)
	}
	if record.RetentionClass != "warm" {
		t.Fatalf("expected retention class warm, got %q", record.RetentionClass)
	}
	if record.ReviewStatus != "approved" {
		t.Fatalf("expected approved review status, got %q", record.ReviewStatus)
	}
	if seedChecksumFromAttrs(record.Attrs) == "" {
		t.Fatal("expected seed checksum to be stored in attrs")
	}

	var authConfig map[string]any
	if err := json.Unmarshal([]byte(record.AuthConfigJSON), &authConfig); err != nil {
		t.Fatalf("decode auth config: %v", err)
	}
}

func TestBuildSourceRegistryRecordsSkipsUnchangedSeed(t *testing.T) {
	now := time.Date(2026, time.March, 10, 12, 0, 0, 0, time.UTC)
	seed := sampleSourceSeed()
	checksum := mustSeedChecksum(t, seed)
	existing := mustNormalizedSeed(t, seed).toRecord(checksum, now.Add(-time.Hour))

	records, err := buildSourceRegistryRecords([]sourceSeed{seed}, map[string]sourceRegistryRecord{existing.SourceID: existing}, now)
	if err != nil {
		t.Fatalf("build records: %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("expected unchanged seed to produce no updates, got %d", len(records))
	}
}

func TestBuildSourceRegistryRecordsUpdatesExistingSeedWithoutDuplicate(t *testing.T) {
	now := time.Date(2026, time.March, 10, 12, 0, 0, 0, time.UTC)
	seed := sampleSourceSeed()
	checksum := mustSeedChecksum(t, seed)
	existing := mustNormalizedSeed(t, seed).toRecord(checksum, now.Add(-time.Hour))
	existing.Enabled = 0
	existing.ReviewStatus = "manual_hold"
	existing.ReviewNotes = "waiting for legal review"
	existing = existing.Disable("legal incident", "ops:user", now.Add(-30*time.Minute))

	updatedSeed := sampleSourceSeed()
	updatedSeed.RequestsPerMinute = 45

	records, err := buildSourceRegistryRecords([]sourceSeed{updatedSeed}, map[string]sourceRegistryRecord{existing.SourceID: existing}, now)
	if err != nil {
		t.Fatalf("build records: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected one versioned update, got %d", len(records))
	}

	next := records[0]
	if next.SourceID != existing.SourceID {
		t.Fatalf("expected source id %q, got %q", existing.SourceID, next.SourceID)
	}
	if next.RecordVersion != existing.RecordVersion+1 || next.Version != existing.Version+1 {
		t.Fatalf("expected version increment to %d, got version=%d record_version=%d", existing.RecordVersion+1, next.Version, next.RecordVersion)
	}
	if next.Enabled != 0 {
		t.Fatalf("expected kill switch to be preserved, got enabled=%d", next.Enabled)
	}
	if next.DisabledReason == nil || *next.DisabledReason != "legal incident" {
		t.Fatalf("expected disabled reason to be preserved, got %#v", next.DisabledReason)
	}
	if next.ReviewStatus != "manual_hold" {
		t.Fatalf("expected review status to be preserved, got %q", next.ReviewStatus)
	}
	if next.RequestsPerMinute != 45 {
		t.Fatalf("expected updated seed value to apply, got %d", next.RequestsPerMinute)
	}
	if seedChecksumFromAttrs(next.Attrs) != mustSeedChecksum(t, updatedSeed) {
		t.Fatal("expected attrs checksum to track updated seed")
	}
}

func TestSourceRegistryDisableReenableWorkflowBlocksAndAllowsFetch(t *testing.T) {
	now := time.Date(2026, time.March, 10, 12, 0, 0, 0, time.UTC)
	record := mustNormalizedSeed(t, sampleSourceSeed()).toRecord(mustSeedChecksum(t, sampleSourceSeed()), now)

	if err := record.CanFetch(); err != nil {
		t.Fatalf("expected fetch to be allowed before disable: %v", err)
	}

	disabled := record.Disable("safety review", "ops:user", now.Add(time.Minute))
	if err := disabled.CanFetch(); err == nil {
		t.Fatal("expected disabled source to block fetch")
	}

	reenabled := disabled.Reenable("review cleared", "ops:user", now.Add(2*time.Minute))
	if err := reenabled.CanFetch(); err != nil {
		t.Fatalf("expected fetch to be allowed after re-enable: %v", err)
	}
	if reenabled.DisabledReason != nil || reenabled.DisabledAt != nil || reenabled.DisabledBy != nil {
		t.Fatal("expected re-enable to clear kill-switch metadata")
	}
	if reenabled.RecordVersion != 3 {
		t.Fatalf("expected create/disable/re-enable workflow to end at record version 3, got %d", reenabled.RecordVersion)
	}
}

func TestSourceRateLimiterEnforcesBurstAndRefill(t *testing.T) {
	now := time.Date(2026, time.March, 10, 12, 0, 0, 0, time.UTC)
	record := sourceRegistryRecord{SourceID: "seed:gdelt", RequestsPerMinute: 60, BurstSize: 2}
	limiter := record.NewRateLimiter(now)

	if !limiter.Allow(now) {
		t.Fatal("expected first request to pass")
	}
	if !limiter.Allow(now) {
		t.Fatal("expected second request to consume burst")
	}
	if limiter.Allow(now) {
		t.Fatal("expected third immediate request to be throttled")
	}
	if !limiter.Allow(now.Add(time.Second)) {
		t.Fatal("expected one token to refill after one second")
	}
	if limiter.Allow(now.Add(time.Second)) {
		t.Fatal("expected refill to enforce one request per second")
	}
}

func TestLoadSourceSeedFallsBackToLegacySchema(t *testing.T) {
	tempDir := t.TempDir()
	seedPath := filepath.Join(tempDir, "source_registry.json")
	b, err := json.Marshal([]sourceSeed{sampleSourceSeed()})
	if err != nil {
		t.Fatalf("marshal seeds: %v", err)
	}
	if err := os.WriteFile(seedPath, b, 0o644); err != nil {
		t.Fatalf("write seed file: %v", err)
	}

	runner := &stubSourceRegistryStore{
		queryResults: map[string]string{
			"SELECT count() FROM system.columns WHERE database = 'meta' AND table = 'source_registry' AND name = 'auth_config_json' FORMAT TabSeparated": "0\n",
			"SELECT count() FROM meta.source_registry WHERE source_id='seed:gdelt' FORMAT TabSeparated":                                                  "0\n",
		},
	}

	if err := loadSourceSeed(context.Background(), runner, seedPath); err != nil {
		t.Fatalf("load source seed: %v", err)
	}
	if len(runner.appliedSQL) != 1 {
		t.Fatalf("expected one insert, got %d", len(runner.appliedSQL))
	}
	insert := runner.appliedSQL[0]
	if strings.Contains(insert, "auth_config_json") {
		t.Fatalf("expected legacy insert without governance columns, got %q", insert)
	}
	for _, fragment := range []string{"schema_version", "record_version", "api_contract_version", "now64(3)"} {
		if !strings.Contains(insert, fragment) {
			t.Fatalf("legacy insert missing fragment %q", fragment)
		}
	}
}

func sampleSourceSeed() sourceSeed {
	return sourceSeed{
		SourceID:            "seed:gdelt",
		Domain:              "gdeltproject.org",
		DomainFamily:        "general_web",
		SourceClass:         "broad_web_corpus",
		Entrypoints:         []string{"https://www.gdeltproject.org/data.html"},
		AuthMode:            "none",
		AuthConfig:          map[string]any{},
		FormatHint:          "csv",
		RobotsPolicy:        "respect",
		RefreshStrategy:     "frequent",
		RequestsPerMinute:   30,
		BurstSize:           5,
		RetentionClass:      "warm",
		License:             "public",
		TermsURL:            "https://www.gdeltproject.org/",
		AttributionRequired: true,
		GeoScope:            "global",
		Priority:            10,
		ParserID:            "parser:csv",
		EntityTypes:         []string{"event", "document"},
		ExpectedPlaceTypes:  []string{"admin0", "admin1"},
		SupportsHistorical:  true,
		SupportsDelta:       true,
		BackfillPriority:    90,
		ReviewStatus:        "approved",
		ReviewNotes:         "seed baseline",
		ConfidenceBaseline:  0.7,
	}
}

func mustNormalizedSeed(t *testing.T, seed sourceSeed) normalizedSourceSeed {
	t.Helper()
	normalized, err := normalizeSourceSeed(seed)
	if err != nil {
		t.Fatalf("normalize seed: %v", err)
	}
	return normalized
}

func mustSeedChecksum(t *testing.T, seed sourceSeed) string {
	t.Helper()
	checksum, err := sourceSeedFingerprint(mustNormalizedSeed(t, seed))
	if err != nil {
		t.Fatalf("seed checksum: %v", err)
	}
	return checksum
}

type stubSourceRegistryStore struct {
	queryResults map[string]string
	appliedSQL   []string
}

func (s *stubSourceRegistryStore) Query(_ context.Context, q string) (string, error) {
	if out, ok := s.queryResults[q]; ok {
		return out, nil
	}
	return "0\n", nil
}

func (s *stubSourceRegistryStore) ApplySQL(_ context.Context, sql string) error {
	s.appliedSQL = append(s.appliedSQL, sql)
	return nil
}
