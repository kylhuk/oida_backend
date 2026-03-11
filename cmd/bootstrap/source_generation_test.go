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

func TestSourceGenerationGovernance(t *testing.T) {
	runner := &stubSourceRegistryStore{}
	seedPath := "../../seed/source_catalog_compiled.json"
	if err := loadSourceCatalogGovernance(context.Background(), runner, seedPath); err != nil {
		t.Fatalf("load source catalog governance: %v", err)
	}
	joined := strings.Join(runner.appliedSQL, "\n")
	for _, fragment := range []string{
		"INSERT INTO meta.source_catalog",
		"INSERT INTO meta.source_family_template",
		"INSERT INTO meta.discovery_probe",
		"OPTIMIZE TABLE meta.source_catalog FINAL",
		"OPTIMIZE TABLE meta.source_family_template FINAL",
		"OPTIMIZE TABLE meta.discovery_probe FINAL",
	} {
		if !strings.Contains(joined, fragment) {
			t.Fatalf("expected governance load to include %q", fragment)
		}
	}
	if !strings.Contains(joined, "'review_required'") {
		t.Fatal("expected generated governance rows to remain review_required")
	}
	if !strings.Contains(joined, "review_pipeline:discovery_candidate") {
		t.Fatal("expected fingerprint governance rows to preserve generator relationships")
	}
	if !strings.Contains(joined, `"catalog_checksum"`) {
		t.Fatal("expected governance rows to persist catalog checksum attrs")
	}
}

func TestSourceGenerationGovernancePreservesOperatorState(t *testing.T) {
	runner := &stubSourceRegistryStore{
		queryResults: map[string]string{
			"SELECT catalog_id, review_status, materialized_source_id, record_version, schema_version, api_contract_version, attrs, evidence FROM meta.source_catalog FINAL FORMAT JSONEachRow": `{"catalog_id":"catalog:concrete:discovery-catalogs-platform-fingerprints-and-archives:gdelt","review_status":"manual_hold","materialized_source_id":"generated:locked","record_version":7,"schema_version":1,"api_contract_version":1,"attrs":"{\"catalog_checksum\":\"stale\"}","evidence":"[\"kept\"]"}` + "\n",
			"SELECT template_id, review_status, record_version, schema_version, api_contract_version, attrs, evidence FROM meta.source_family_template FINAL FORMAT JSONEachRow":                `{"template_id":"catalog:family:recurring-national-and-subnational-source-families:open-data-portals","review_status":"manual_hold","record_version":3,"schema_version":1,"api_contract_version":1,"attrs":"{\"catalog_checksum\":\"stale\"}","evidence":"[\"kept\"]"}` + "\n",
			"SELECT probe_id, review_status, record_version, schema_version, api_contract_version, attrs, evidence FROM meta.discovery_probe FINAL FORMAT JSONEachRow":                          `{"probe_id":"catalog:fingerprint:discovery-catalogs-platform-fingerprints-and-archives:ckan-action-api","review_status":"manual_hold","record_version":5,"schema_version":1,"api_contract_version":1,"attrs":"{\"catalog_checksum\":\"stale\"}","evidence":"[\"kept\"]"}` + "\n",
		},
	}
	seedPath := "../../seed/source_catalog_compiled.json"
	if err := loadSourceCatalogGovernance(context.Background(), runner, seedPath); err != nil {
		t.Fatalf("load source catalog governance with existing state: %v", err)
	}
	joined := strings.Join(runner.appliedSQL, "\n")
	for _, fragment := range []string{"'manual_hold'", "'generated:locked'", "'[\"kept\"]'", ",8,", ",4,", ",6,"} {
		if !strings.Contains(joined, fragment) {
			t.Fatalf("expected preserved governance fragment %q in SQL", fragment)
		}
	}
}

func TestSourceGenerationGovernanceSkipsUnchangedRows(t *testing.T) {
	compiled, err := loadCompiledSourceCatalog("../../seed/source_catalog_compiled.json")
	if err != nil {
		t.Fatalf("load compiled source catalog fixture: %v", err)
	}
	entry := compiled.Catalog.Entries[0]
	entryChecksum := governanceChecksum(entry, compiled.Catalog.SourceMarkdownChecksum)
	entryAttrs := mergeSourceCatalogAttrs("", entryChecksum, compiled.Catalog.SourceMarkdownChecksum, entry)
	template := compiled.FamilyTemplates[0]
	templateChecksum := governanceChecksum(template, "")
	probe := compiled.FingerprintProbes[0]
	probeChecksum := governanceChecksum(probe, "")
	if _, ok := buildSourceCatalogEntrySQL(entry, compiled.Catalog, map[string]sourceCatalogGovernanceRecord{
		entry.CatalogID: {CatalogID: entry.CatalogID, Attrs: entryAttrs},
	}, time.Now().UTC()); ok {
		t.Fatal("expected unchanged source catalog entry to skip insert")
	}
	if _, ok := buildSourceFamilyTemplateSQL(template, map[string]sourceFamilyTemplateRecord{
		template.CatalogID: {TemplateID: template.CatalogID, Attrs: `{"catalog_checksum":"` + templateChecksum + `"}`},
	}, time.Now().UTC()); ok {
		t.Fatal("expected unchanged family template to skip insert")
	}
	if _, ok := buildDiscoveryProbeSQL(probe, map[string]discoveryProbeGovernanceRecord{
		probe.CatalogID: {ProbeID: probe.CatalogID, Attrs: `{"catalog_checksum":"` + probeChecksum + `"}`},
	}, time.Now().UTC()); ok {
		t.Fatal("expected unchanged discovery probe to skip insert")
	}
}

func TestGeneratedSourceKillSwitch(t *testing.T) {
	candidate := discoveryCandidateRecord{
		CandidateID:          "candidate:ckan:city-example",
		CatalogID:            "catalog:fingerprint:discovery:ckan-action-api",
		CandidateName:        "Example CKAN Portal",
		CandidateURL:         "https://city.example.test/api/3/action/package_search",
		IntegrationArchetype: "catalog_ckan",
		DetectedPlatform:     "CKAN",
		ReviewStatus:         generatedChildApprovedReviewStatus,
		MaterializedSourceID: "generated:city-example:ckan",
	}
	seed := sampleSourceSeed()
	seed.SourceID = ""
	seed.ReviewStatus = "approved"
	seed.ReviewNotes = ""

	generated, err := materializeGeneratedChildSource(candidate, seed)
	if err != nil {
		t.Fatalf("materialize generated child source: %v", err)
	}
	if generated.SourceID != candidate.MaterializedSourceID {
		t.Fatalf("expected materialized source id %q, got %q", candidate.MaterializedSourceID, generated.SourceID)
	}
	if generated.LifecycleState != "approved_disabled" {
		t.Fatalf("expected generated child lifecycle approved_disabled, got %q", generated.LifecycleState)
	}
	if generated.ReviewStatus != generatedChildReviewRequiredStatus {
		t.Fatalf("expected generated child review status %q, got %q", generatedChildReviewRequiredStatus, generated.ReviewStatus)
	}
	normalized, err := normalizeSourceSeed(generated)
	if err != nil {
		t.Fatalf("normalize generated seed: %v", err)
	}
	record := normalized.toRecord("seed-checksum", time.Date(2026, time.March, 11, 0, 0, 0, 0, time.UTC))
	if record.Enabled != 0 {
		t.Fatalf("expected generated child source to stay disabled until further approval, got enabled=%d", record.Enabled)
	}
}

func TestGeneratedChildSourcesRequireApproval(t *testing.T) {
	candidate := discoveryCandidateRecord{
		CandidateID:          "candidate:city-example",
		ReviewStatus:         generatedChildReviewRequiredStatus,
		MaterializedSourceID: "generated:city-example",
	}
	if _, err := materializeGeneratedChildSource(candidate, sampleSourceSeed()); err == nil || !strings.Contains(err.Error(), "must be approved") {
		t.Fatalf("expected review-required candidate to be rejected, got %v", err)
	}
}

func TestLoadSourceCatalogGovernanceNoOpsForLegacySeedArray(t *testing.T) {
	tempDir := t.TempDir()
	seedPath := filepath.Join(tempDir, "source_registry.json")
	b, err := json.Marshal([]sourceSeed{sampleSourceSeed()})
	if err != nil {
		t.Fatalf("marshal legacy seed array: %v", err)
	}
	if err := os.WriteFile(seedPath, b, 0o644); err != nil {
		t.Fatalf("write legacy seed array: %v", err)
	}
	runner := &stubSourceRegistryStore{}
	if err := loadSourceCatalogGovernance(context.Background(), runner, seedPath); err != nil {
		t.Fatalf("expected governance load to ignore legacy seed array, got %v", err)
	}
	if len(runner.appliedSQL) != 0 {
		t.Fatalf("expected no governance SQL for legacy seed array, got %d statements", len(runner.appliedSQL))
	}
}
