package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

var numberedSectionPrefix = regexp.MustCompile(`^\d+(?:\.\d+)?\.?\s+`)

func TestCompileSourceCatalog(t *testing.T) {
	compiled, err := compileSourceCatalog(filepath.Join("..", "..", "seed", "source_catalog.json"), filepath.Join("..", "..", "seed", "source_registry.json"))
	if err != nil {
		t.Fatalf("compile source catalog: %v", err)
	}
	if compiled.CatalogChecksum == "" {
		t.Fatal("expected compiled catalog checksum")
	}
	if compiled.CatalogChecksum != sourceCatalogChecksum(compiled.Catalog) {
		t.Fatal("expected compiled catalog checksum to use deterministic source catalog checksum")
	}
	wantMarkdownChecksum := sum(readRepoBytes(t, "..", "..", "sources.md"))
	if compiled.Catalog.SourceMarkdownChecksum != wantMarkdownChecksum {
		t.Fatalf("expected markdown checksum %s, got %s", wantMarkdownChecksum, compiled.Catalog.SourceMarkdownChecksum)
	}
	if len(compiled.RunnableSeeds) != len(approvedRuntimeLinkedSourceIDs) {
		t.Fatalf("expected %d runnable seeds mapped from catalog, got %d", len(approvedRuntimeLinkedSourceIDs), len(compiled.RunnableSeeds))
	}
	if len(compiled.FingerprintProbes) != 16 {
		t.Fatalf("expected 16 fingerprint probes, got %d", len(compiled.FingerprintProbes))
	}
	if len(compiled.FamilyTemplates) != 26 {
		t.Fatalf("expected 26 family templates, got %d", len(compiled.FamilyTemplates))
	}
	if len(compiled.BronzeDDLManifest) != 269 {
		t.Fatalf("expected 269 bronze manifest rows, got %d", len(compiled.BronzeDDLManifest))
	}
	seedIDs := map[string]struct{}{}
	for _, seed := range compiled.RunnableSeeds {
		seedIDs[seed.SourceID] = struct{}{}
	}
	for _, expected := range []string{"seed:gdelt", "fixture:reliefweb", "fixture:acled", "fixture:opensanctions", "fixture:nasa-firms", "fixture:noaa-hazards", "fixture:kev"} {
		if _, ok := seedIDs[expected]; !ok {
			t.Fatalf("expected runnable seed %q in compiled output", expected)
		}
	}
	for _, template := range compiled.FamilyTemplates {
		if !strings.HasPrefix(template.CatalogID, "catalog:family:") {
			t.Fatalf("expected compiled family template catalog id to stay in family scope, got %q", template.CatalogID)
		}
		if strings.TrimSpace(template.Scope) == "" || strings.TrimSpace(template.IntegrationArchetype) == "" || strings.TrimSpace(template.ReviewStatusDefault) == "" {
			t.Fatalf("expected compiled family template %s to include scope/archetype/review defaults", template.CatalogID)
		}
		if strings.TrimSpace(template.TransportType) == "" || len(template.ScopeLevels) == 0 {
			t.Fatalf("expected compiled family template %s to include transport/scope levels", template.CatalogID)
		}
		if strings.TrimSpace(template.ChildSource.TransportType) == "" || strings.TrimSpace(template.ChildSource.IntegrationArchetype) == "" || strings.TrimSpace(template.ChildSource.ParserID) == "" {
			t.Fatalf("expected compiled family template %s to include child-source shape metadata", template.CatalogID)
		}
		if len(template.GeneratorRelationships) == 0 || template.GeneratorRelationships[0] != "review_pipeline:family_template_candidate" {
			t.Fatalf("expected compiled family template %s to preserve family review pipeline", template.CatalogID)
		}
	}
	for _, template := range compiled.FamilyTemplates {
		if template.CatalogID != "catalog:family:recurring-national-and-subnational-source-families:open-data-portals" {
			continue
		}
		if template.ChildSource.IntegrationArchetype != "catalog_ckan" {
			t.Fatalf("expected open-data portals child archetype catalog_ckan, got %q", template.ChildSource.IntegrationArchetype)
		}
		if template.ChildSource.ParserID != "parser:json" {
			t.Fatalf("expected open-data portals child parser parser:json, got %q", template.ChildSource.ParserID)
		}
	}
}

func TestCompiledSourceCounts(t *testing.T) {
	compiled, err := compileSourceCatalog(filepath.Join("..", "..", "seed", "source_catalog.json"), filepath.Join("..", "..", "seed", "source_registry.json"))
	if err != nil {
		t.Fatalf("compile source catalog: %v", err)
	}
	if len(compiled.Catalog.Entries) != 311 {
		t.Fatalf("expected 311 catalog entries, got %d", len(compiled.Catalog.Entries))
	}
	counts := map[string]int{}
	nonFamilyCount := 0
	byCategoryName := map[string]sourceCatalogEntry{}
	for _, entry := range compiled.Catalog.Entries {
		counts[entry.CatalogKind]++
		if entry.CatalogKind != "family" {
			nonFamilyCount++
		}
		byCategoryName[entry.Category+"\x00"+entry.Name] = entry
	}
	if nonFamilyCount != 285 {
		t.Fatalf("expected 285 non-family rows from sources.md + sources2.md, got %d", nonFamilyCount)
	}
	if counts["concrete"] != 269 || counts["fingerprint"] != 16 || counts["family"] != 26 {
		t.Fatalf("unexpected catalog kind counts: %#v", counts)
	}
	if entry := byCategoryName["Discovery, catalogs, platform fingerprints, and archives\x00CKAN Action API"]; entry.CatalogKind != "fingerprint" {
		t.Fatalf("expected CKAN Action API to compile as fingerprint, got %q", entry.CatalogKind)
	}
	if entry := byCategoryName["Recurring national and subnational source families\x00Open-data portals"]; entry.CatalogKind != "family" {
		t.Fatalf("expected Open-data portals to compile as family, got %q", entry.CatalogKind)
	}
	if entry := byCategoryName["Discovery, catalogs, platform fingerprints, and archives\x00DataPortals.org"]; entry.CatalogKind != "concrete" {
		t.Fatalf("expected DataPortals.org to compile as concrete, got %q", entry.CatalogKind)
	}
	manifestBySourceID := map[string]sourceBronzeDDLManifest{}
	manifestTables := map[string]string{}
	for _, row := range compiled.BronzeDDLManifest {
		if prior, ok := manifestBySourceID[row.SourceID]; ok {
			t.Fatalf("duplicate bronze manifest source %q: %#v and %#v", row.SourceID, prior, row)
		}
		manifestBySourceID[row.SourceID] = row
		if priorSourceID, ok := manifestTables[row.BronzeTable]; ok {
			t.Fatalf("duplicate bronze manifest table %q for %s and %s", row.BronzeTable, priorSourceID, row.SourceID)
		}
		manifestTables[row.BronzeTable] = row.SourceID
	}
	concreteEntries := 0
	for _, entry := range compiled.Catalog.Entries {
		manifestSourceID := bronzeManifestSourceID(entry)
		if manifestSourceID == "" {
			continue
		}
		concreteEntries++
		row, ok := manifestBySourceID[manifestSourceID]
		if !ok {
			t.Fatalf("missing bronze manifest source %q", manifestSourceID)
		}
		if strings.TrimSpace(entry.RuntimeSourceID) == "" && row.BronzeTable != bronzeTableForSourceID(manifestSourceID) {
			t.Fatalf("expected bronze manifest source %q to use %q, got %q", manifestSourceID, bronzeTableForSourceID(manifestSourceID), row.BronzeTable)
		}
	}
	if concreteEntries != len(compiled.BronzeDDLManifest) {
		t.Fatalf("expected bronze manifest count %d to equal concrete entry count %d", len(compiled.BronzeDDLManifest), concreteEntries)
	}

	markdownRows := parseCatalogMarkdownRows(t,
		filepath.Join("..", "..", "sources.md"),
		filepath.Join("..", "..", "sources2.md"),
	)
	if len(markdownRows) != len(compiled.Catalog.Entries) {
		t.Fatalf("expected markdown row count %d to equal catalog entries %d", len(markdownRows), len(compiled.Catalog.Entries))
	}
	for key := range markdownRows {
		if _, ok := byCategoryName[key]; !ok {
			t.Fatalf("expected catalog to cover markdown row %q", key)
		}
	}
}

func TestCompileSourceCatalogRejectsDuplicateIDs(t *testing.T) {
	tempDir := t.TempDir()
	catalogPath := filepath.Join(tempDir, "source_catalog.json")
	registryPath := filepath.Join("..", "..", "seed", "source_registry.json")
	catalog := mustLoadSourceCatalogFixture(t)
	catalog.Entries[1].CatalogID = catalog.Entries[0].CatalogID
	b, err := json.Marshal(catalog)
	if err != nil {
		t.Fatalf("marshal duplicate catalog: %v", err)
	}
	if err := os.WriteFile(catalogPath, b, 0o644); err != nil {
		t.Fatalf("write duplicate catalog: %v", err)
	}
	if _, err := compileSourceCatalog(catalogPath, registryPath); err == nil || !strings.Contains(err.Error(), "duplicate catalog_id") {
		t.Fatalf("expected duplicate catalog id to be rejected, got %v", err)
	}
}

func TestCompileSourceCatalogRejectsDuplicateRuntimeSourceIDs(t *testing.T) {
	tempDir := t.TempDir()
	catalogPath := filepath.Join(tempDir, "source_catalog.json")
	registryPath := filepath.Join("..", "..", "seed", "source_registry.json")
	catalog := mustLoadSourceCatalogFixture(t)
	firstIdx := -1
	secondIdx := -1
	for idx, entry := range catalog.Entries {
		if entry.RuntimeSourceID == "" {
			continue
		}
		if firstIdx == -1 {
			firstIdx = idx
			continue
		}
		secondIdx = idx
		break
	}
	if firstIdx == -1 || secondIdx == -1 {
		t.Fatal("expected fixture catalog to contain at least two runtime-mapped entries")
	}
	catalog.Entries[secondIdx].RuntimeSourceID = catalog.Entries[firstIdx].RuntimeSourceID
	b, err := json.Marshal(catalog)
	if err != nil {
		t.Fatalf("marshal duplicate runtime source catalog: %v", err)
	}
	if err := os.WriteFile(catalogPath, b, 0o644); err != nil {
		t.Fatalf("write duplicate runtime source catalog: %v", err)
	}
	if _, err := compileSourceCatalog(catalogPath, registryPath); err == nil || !strings.Contains(err.Error(), "runtime_source_id") {
		t.Fatalf("expected duplicate runtime_source_id to be rejected, got %v", err)
	}
}

func TestLoadCompiledSourceCatalogVerifiesChecksumAndMarkdown(t *testing.T) {
	compiled, err := loadCompiledSourceCatalog(filepath.Join("..", "..", "seed", "source_catalog_compiled.json"))
	if err != nil {
		t.Fatalf("load compiled source catalog: %v", err)
	}
	if compiled.CatalogChecksum != sourceCatalogChecksum(compiled.Catalog) {
		t.Fatal("expected runtime compiled catalog validation to preserve catalog checksum")
	}
	if len(compiled.BronzeDDLManifest) != 269 {
		t.Fatalf("expected compiled bronze manifest to preserve 269 concrete source rows, got %d", len(compiled.BronzeDDLManifest))
	}
}

func TestRenderSourceBronzeMigrationMatchesCheckedInArtifact(t *testing.T) {
	compiled, err := loadCompiledSourceCatalog(filepath.Join("..", "..", "seed", "source_catalog_compiled.json"))
	if err != nil {
		t.Fatalf("load compiled source catalog: %v", err)
	}
	rendered, err := renderSourceBronzeMigration(compiled)
	if err != nil {
		t.Fatalf("render source bronze migration: %v", err)
	}
	want := string(readRepoBytes(t, "..", "..", "migrations", "clickhouse", "0025_source_bronze_tables_expanded.sql"))
	if rendered != want {
		t.Fatal("expected rendered source bronze migration to match checked-in artifact")
	}
}

func TestCompileCatalogArtifactWritesBronzeMigration(t *testing.T) {
	tempDir := t.TempDir()
	compiledPath := filepath.Join(tempDir, "seed", "source_catalog_compiled.json")
	migrationPath := filepath.Join(tempDir, "migrations", "clickhouse", "0025_source_bronze_tables_expanded.sql")
	t.Setenv("SOURCE_CATALOG_PATH", filepath.Join("..", "..", "seed", "source_catalog.json"))
	t.Setenv("SOURCE_REGISTRY_PATH", filepath.Join("..", "..", "seed", "source_registry.json"))
	t.Setenv("SOURCE_CATALOG_COMPILED_PATH", compiledPath)
	t.Setenv("SOURCE_BRONZE_MIGRATION_PATH", migrationPath)

	if err := compileCatalogArtifact(); err != nil {
		t.Fatalf("compile catalog artifact: %v", err)
	}

	compiled, err := compileSourceCatalog(filepath.Join("..", "..", "seed", "source_catalog.json"), filepath.Join("..", "..", "seed", "source_registry.json"))
	if err != nil {
		t.Fatalf("compile source catalog for expected migration: %v", err)
	}
	rendered, err := renderSourceBronzeMigration(compiled)
	if err != nil {
		t.Fatalf("render expected source bronze migration: %v", err)
	}
	got, err := os.ReadFile(migrationPath)
	if err != nil {
		t.Fatalf("read written source bronze migration: %v", err)
	}
	if string(got) != rendered {
		t.Fatal("expected compile-catalog to write rendered source bronze migration")
	}
	if _, err := os.ReadFile(compiledPath); err != nil {
		t.Fatalf("read written compiled source catalog: %v", err)
	}
}

func TestLoadCompiledSourceCatalogRejectsChecksumMismatch(t *testing.T) {
	tempDir := t.TempDir()
	compiledPath := filepath.Join(tempDir, "source_catalog_compiled.json")
	compiled, err := loadCompiledSourceCatalog(filepath.Join("..", "..", "seed", "source_catalog_compiled.json"))
	if err != nil {
		t.Fatalf("load compiled source catalog fixture: %v", err)
	}
	compiled.CatalogChecksum = "bad-checksum"
	b, err := json.Marshal(compiled)
	if err != nil {
		t.Fatalf("marshal checksum mismatch catalog: %v", err)
	}
	if err := os.WriteFile(compiledPath, b, 0o644); err != nil {
		t.Fatalf("write checksum mismatch compiled catalog: %v", err)
	}
	if _, err := loadCompiledSourceCatalog(compiledPath); err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("expected checksum mismatch to be rejected, got %v", err)
	}
}

func TestCompiledSourceCatalogBronzeTableSetsSplitManifestFromRunnableRegistry(t *testing.T) {
	compiled, err := compileSourceCatalog(filepath.Join("..", "..", "seed", "source_catalog.json"), filepath.Join("..", "..", "seed", "source_registry.json"))
	if err != nil {
		t.Fatalf("compile source catalog: %v", err)
	}
	manifestTables := manifestBronzeTableSet(compiled)
	runnableTables := runnableSeedBronzeTableSet(compiled)
	if len(manifestTables) != len(compiled.BronzeDDLManifest) {
		t.Fatalf("expected manifest bronze table set size %d, got %d", len(compiled.BronzeDDLManifest), len(manifestTables))
	}
	if len(runnableTables) != len(compiled.RunnableSeeds) {
		t.Fatalf("expected runnable bronze table set size %d, got %d", len(compiled.RunnableSeeds), len(runnableTables))
	}
	if len(runnableTables) >= len(manifestTables) {
		t.Fatalf("expected runnable bronze table set to stay smaller than full manifest: runnable=%d manifest=%d", len(runnableTables), len(manifestTables))
	}
	for table := range runnableTables {
		if _, ok := manifestTables[table]; !ok {
			t.Fatalf("expected runnable bronze table %q to be present in manifest", table)
		}
	}
}

func TestLoadRunnableSourceSeedsPreservesCompiledCatalogValidationError(t *testing.T) {
	tempDir := t.TempDir()
	compiledPath := filepath.Join(tempDir, "source_catalog_compiled.json")
	compiled, err := loadCompiledSourceCatalog(filepath.Join("..", "..", "seed", "source_catalog_compiled.json"))
	if err != nil {
		t.Fatalf("load compiled source catalog fixture: %v", err)
	}
	compiled.CatalogChecksum = "bad-checksum"
	b, err := json.Marshal(compiled)
	if err != nil {
		t.Fatalf("marshal invalid compiled source catalog: %v", err)
	}
	if err := os.WriteFile(compiledPath, b, 0o644); err != nil {
		t.Fatalf("write invalid compiled source catalog: %v", err)
	}
	if _, err := loadRunnableSourceSeeds(compiledPath); err == nil || !strings.Contains(err.Error(), "compiled source catalog checksum mismatch") {
		t.Fatalf("expected bootstrap load path to preserve compiled catalog validation error, got %v", err)
	}
}

func TestCatalogArchetypeCoverage(t *testing.T) {
	catalog := mustLoadSourceCatalogFixture(t)
	concreteCount := 0
	deferredCount := 0
	explicitDeferredCount := 0
	for _, entry := range catalog.Entries {
		if entry.CatalogKind != "concrete" {
			continue
		}
		concreteCount++
		if strings.TrimSpace(entry.IntegrationArchetype) == "" {
			t.Fatalf("expected concrete entry %s to declare integration_archetype", entry.CatalogID)
		}
		if !supportedIntegrationArchetype(entry.IntegrationArchetype) {
			t.Fatalf("expected concrete entry %s to use supported archetype, got %q", entry.CatalogID, entry.IntegrationArchetype)
		}
		if strings.TrimSpace(entry.IntegrationArchetype) == "deferred_transport" {
			deferredCount++
			if strings.TrimSpace(entry.DeferredReason) == "" {
				t.Fatalf("expected deferred concrete entry %s to include deferred_reason", entry.CatalogID)
			}
			if strings.TrimSpace(entry.ParserID) != "" {
				t.Fatalf("expected deferred concrete entry %s to omit parser_id", entry.CatalogID)
			}
			continue
		}
		if strings.TrimSpace(entry.RuntimeSourceID) == "" {
			explicitDeferredCount++
			if strings.TrimSpace(entry.DeferredReason) == "" {
				t.Fatalf("expected non-runtime concrete entry %s to include deferred_reason", entry.CatalogID)
			}
			if strings.TrimSpace(entry.ParserID) == "" {
				t.Fatalf("expected non-runtime concrete entry %s to still include parser_id", entry.CatalogID)
			}
			continue
		}
		if strings.TrimSpace(entry.ParserID) == "" {
			t.Fatalf("expected runnable concrete entry %s to include parser_id", entry.CatalogID)
		}
		if strings.TrimSpace(entry.DeferredReason) != "" {
			t.Fatalf("expected runnable concrete entry %s to omit deferred_reason", entry.CatalogID)
		}
	}
	if concreteCount != 269 {
		t.Fatalf("expected 269 concrete catalog entries, got %d", concreteCount)
	}
	if deferredCount != 0 {
		t.Fatalf("expected 0 deferred concrete entries in current catalog snapshot, got %d", deferredCount)
	}
	if explicitDeferredCount != 259 {
		t.Fatalf("expected raw catalog fixture to still include 259 non-runtime concrete entries prior to compile/runtime synthesis, got %d", explicitDeferredCount)
	}
}

func TestCompileSourceCatalogKeepsFutureRuntimeExpansionDeferredByArchetypeWave(t *testing.T) {
	compiled, err := compileSourceCatalog(filepath.Join("..", "..", "seed", "source_catalog.json"), filepath.Join("..", "..", "seed", "source_registry.json"))
	if err != nil {
		t.Fatalf("compile source catalog: %v", err)
	}
	byID := map[string]sourceCatalogEntry{}
	for _, entry := range compiled.Catalog.Entries {
		byID[entry.CatalogID] = entry
	}

	deferredIDs := map[string]string{
		"catalog:concrete:aviation-airports-drones-and-mobility:opensky-network":      deferredReasonByArchetype["http_json"],
		"catalog:concrete:aviation-airports-drones-and-mobility:airplanes-live":       deferredReasonByArchetype["http_json"],
		"catalog:concrete:aviation-airports-drones-and-mobility:openaip-core-api":     deferredReasonByArchetype["http_json"],
		"catalog:concrete:maritime-ocean-and-coastal-sources:aishub":                  deferredReasonByArchetype["http_json"],
		"catalog:concrete:security-addendum:air-adsblol-api":                          deferredReasonByArchetype["http_json"],
		"catalog:concrete:aviation-airports-drones-and-mobility:aviationweather-api":  deferredReasonByArchetype["http_json"],
		"catalog:concrete:aviation-airports-drones-and-mobility:faa-nms-notam":        deferredReasonByArchetype["http_json"],
		"catalog:concrete:maritime-ocean-and-coastal-sources:marine-cadastre-u-s-ais": deferredReasonByArchetype["html_profile"],
		"catalog:concrete:maritime-ocean-and-coastal-sources:noaa-co-ops-erddap":      deferredReasonByArchetype["http_json"],
		"catalog:concrete:aviation-airports-drones-and-mobility:ads-b-exchange":       deferredReasonByArchetype["http_json"],
		"catalog:concrete:maritime-ocean-and-coastal-sources:marinetraffic-apis":      deferredReasonByArchetype["http_json"],
		"catalog:concrete:maritime-ocean-and-coastal-sources:global-fishing-watch":    deferredReasonByArchetype["html_profile"],
		"catalog:concrete:maritime-ocean-and-coastal-sources:equasis":                 deferredReasonByArchetype["html_profile"],
		"catalog:concrete:maritime-ocean-and-coastal-sources:imo-gisis":               deferredReasonByArchetype["html_profile"],
	}
	for catalogID, wantReason := range deferredIDs {
		entry, ok := byID[catalogID]
		if !ok {
			t.Fatalf("expected deferred catalog entry %s", catalogID)
		}
		if strings.TrimSpace(entry.RuntimeSourceID) != "" {
			t.Fatalf("expected deferred catalog entry %s to omit runtime_source_id, got %q", catalogID, entry.RuntimeSourceID)
		}
		if strings.TrimSpace(entry.DeferredReason) != wantReason {
			t.Fatalf("expected deferred catalog entry %s reason %q, got %q", catalogID, wantReason, entry.DeferredReason)
		}
	}
}

func assertEntrypointSet(t *testing.T, got []string, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("entrypoint length mismatch: got=%d want=%d", len(got), len(want))
	}
	wantSet := make(map[string]struct{}, len(want))
	for _, entry := range want {
		wantSet[entry] = struct{}{}
	}
	for _, entry := range got {
		if _, ok := wantSet[entry]; !ok {
			t.Fatalf("unexpected entrypoint %q", entry)
		}
		delete(wantSet, entry)
	}
	for entry := range wantSet {
		t.Fatalf("missing entrypoint %q", entry)
	}
}

var approvedRuntimeLinkedSourceIDs = map[string]struct{}{
	"seed:gdelt":            {},
	"fixture:nasa-firms":    {},
	"fixture:acled":         {},
	"fixture:reliefweb":     {},
	"fixture:noaa-hazards":  {},
	"fixture:opensanctions": {},
	"fixture:kev":           {},
	"catalog:auto:maritime-ocean-and-coastal-sources-aisstream":           {},
	"catalog:auto:maritime-ocean-and-coastal-sources-vesselfinder":        {},
	"catalog:auto:maritime-ocean-and-coastal-sources-vesselfinder-routes": {},
}

var deferredReasonByArchetype = map[string]string{
	"arcgis_rest":   "deferred to future arcgis_rest onboarding wave",
	"bulk_file":     "deferred to future bulk_file onboarding wave",
	"discovery_web": "deferred to future discovery_web onboarding wave",
	"html_profile":  "deferred to future html_profile onboarding wave",
	"http_json":     "deferred to future http_json onboarding wave",
	"ogc_records":   "deferred to future ogc_records onboarding wave",
	"rss_atom":      "deferred to future rss_atom onboarding wave",
	"stac_api":      "deferred to future stac_api onboarding wave",
}

func TestPhase1TelemetryLandingTargetsAreExplicitAndComplete(t *testing.T) {
	if len(phase1TelemetryLandingTargets) != 8 {
		t.Fatalf("expected 8 phase-1 landing targets, got %d", len(phase1TelemetryLandingTargets))
	}
	expectedTargets := map[string]string{
		"catalog:auto:aviation-airports-drones-and-mobility-opensky-network":  "silver.fact_track_point",
		"catalog:auto:aviation-airports-drones-and-mobility-airplanes-live":   "silver.fact_track_point",
		"catalog:auto:security-addendum-air-adsblol-api":                      "silver.fact_track_point",
		"catalog:auto:maritime-ocean-and-coastal-sources-aishub":              "silver.fact_track_point",
		"catalog:auto:maritime-ocean-and-coastal-sources-aisstream":           "silver.fact_track_point",
		"catalog:auto:maritime-ocean-and-coastal-sources-vesselfinder":        "silver.fact_track_point",
		"catalog:auto:maritime-ocean-and-coastal-sources-vesselfinder-routes": "ops.vesselfinder_route_plan",
		"catalog:auto:aviation-airports-drones-and-mobility-openaip-core-api": "silver.dim_entity",
	}
	for sourceID, want := range expectedTargets {
		got, ok := phase1TelemetryLandingTargets[sourceID]
		if !ok {
			t.Fatalf("expected source %s in phase-1 landing targets", sourceID)
		}
		if got != want {
			t.Fatalf("expected source %s to target %s, got %s", sourceID, want, got)
		}
	}
}

func TestArchetypeParserCompatibility(t *testing.T) {
	catalog := mustLoadSourceCatalogFixture(t)
	for _, entry := range catalog.Entries {
		if entry.CatalogKind != "concrete" || strings.TrimSpace(entry.IntegrationArchetype) == "deferred_transport" {
			continue
		}
		if !parserCompatibleWithArchetype(entry.IntegrationArchetype, entry.ParserID) {
			t.Fatalf("expected parser %q to be compatible with archetype %q for %s", entry.ParserID, entry.IntegrationArchetype, entry.CatalogID)
		}
	}
}

func TestDeferredTransportClassification(t *testing.T) {
	entry := sourceCatalogEntry{
		CatalogID:            "catalog:concrete:test:interactive-source",
		CatalogKind:          "concrete",
		Name:                 "Interactive Source",
		Category:             "Test",
		IntegrationArchetype: "deferred_transport",
		DeferredReason:       "requires browser or interactive workflow",
		GeneratorKind:        "direct",
	}
	if err := validateCatalogEntryExecutionContract(entry); err != nil {
		t.Fatalf("expected valid deferred transport contract, got %v", err)
	}
	entry.ParserID = "parser:json"
	if err := validateCatalogEntryExecutionContract(entry); err == nil || !strings.Contains(err.Error(), "must not declare parser_id") {
		t.Fatalf("expected deferred transport parser rejection, got %v", err)
	}
}

func TestConcreteSourceCoverage(t *testing.T) {
	catalog := mustLoadSourceCatalogFixture(t)
	publicConcrete := 0
	runtimeLinked := 0
	explicitlyDeferred := 0
	deferredByArchetype := map[string]int{}
	for _, entry := range catalog.Entries {
		if entry.CatalogKind != "concrete" || concreteRequiresCredential(entry) {
			continue
		}
		publicConcrete++
		if strings.TrimSpace(entry.RuntimeSourceID) != "" {
			runtimeLinked++
			if _, ok := approvedRuntimeLinkedSourceIDs[strings.TrimSpace(entry.RuntimeSourceID)]; !ok {
				t.Fatalf("expected runtime-linked public concrete entry %s to stay inside approved subset, got %q", entry.CatalogID, entry.RuntimeSourceID)
			}
			if strings.TrimSpace(entry.DeferredReason) != "" {
				t.Fatalf("expected runnable concrete entry %s to omit deferred_reason", entry.CatalogID)
			}
			continue
		}
		wantReason, ok := deferredReasonByArchetype[strings.TrimSpace(entry.IntegrationArchetype)]
		if !ok {
			t.Fatalf("expected public concrete entry %s archetype %q to have explicit deferred-wave mapping", entry.CatalogID, entry.IntegrationArchetype)
		}
		if strings.TrimSpace(entry.DeferredReason) != wantReason {
			t.Fatalf("expected public concrete entry %s deferred_reason %q, got %q", entry.CatalogID, wantReason, entry.DeferredReason)
		}
		deferredByArchetype[strings.TrimSpace(entry.IntegrationArchetype)]++
		explicitlyDeferred++
	}
	if publicConcrete != 245 {
		t.Fatalf("expected 245 public concrete entries, got %d", publicConcrete)
	}
	if runtimeLinked != 8 {
		t.Fatalf("expected 8 runtime-linked public concrete entries, got %d", runtimeLinked)
	}
	if runtimeLinked+explicitlyDeferred != publicConcrete {
		t.Fatalf("expected runtime-linked + deferred to equal public concrete count, got runtime=%d deferred=%d public=%d", runtimeLinked, explicitlyDeferred, publicConcrete)
	}
	if len(deferredByArchetype) != len(deferredReasonByArchetype) {
		t.Fatalf("expected deferred public concrete coverage across %d archetype waves, got %d", len(deferredReasonByArchetype), len(deferredByArchetype))
	}
}

func TestApprovedRunnableSourceCoverage(t *testing.T) {
	compiled, err := compileSourceCatalog(filepath.Join("..", "..", "seed", "source_catalog.json"), filepath.Join("..", "..", "seed", "source_registry.json"))
	if err != nil {
		t.Fatalf("compile source catalog: %v", err)
	}
	if len(compiled.RunnableSeeds) != len(approvedRuntimeLinkedSourceIDs) {
		t.Fatalf("expected %d approved runnable seeds, got %d", len(approvedRuntimeLinkedSourceIDs), len(compiled.RunnableSeeds))
	}
	for _, seed := range compiled.RunnableSeeds {
		if _, ok := approvedRuntimeLinkedSourceIDs[strings.TrimSpace(seed.SourceID)]; !ok {
			t.Fatalf("expected runnable seed %s to stay inside approved runtime-linked subset", seed.SourceID)
		}
		if strings.TrimSpace(seed.BronzeTable) == "" {
			t.Fatalf("expected runnable seed %s to include bronze_table", seed.SourceID)
		}
		if seed.BronzeSchemaVersion == 0 {
			t.Fatalf("expected runnable seed %s to include bronze_schema_version", seed.SourceID)
		}
		if strings.TrimSpace(seed.PromoteProfile) == "" {
			t.Fatalf("expected runnable seed %s to include promote_profile", seed.SourceID)
		}
		if strings.TrimSpace(seed.RefreshStrategy) == "" || strings.TrimSpace(seed.CrawlStrategy) == "" {
			t.Fatalf("expected runnable seed %s to include sync metadata", seed.SourceID)
		}
		if seed.RequestsPerMinute <= 0 {
			t.Fatalf("expected runnable seed %s to include requests_per_minute", seed.SourceID)
		}
	}
}

func TestCredentialedSourcesAreDisabledByDefault(t *testing.T) {
	catalog := mustLoadSourceCatalogFixture(t)
	credentialed := 0
	wantEnvVars := map[string]string{
		"catalog:concrete:global-official-statistics-economics-and-institutional-data:iea-api-data-services": "SOURCE_IEA_API_DATA_SERVICES_API_KEY",
		"catalog:concrete:aviation-airports-drones-and-mobility:openaip-core-api":                            "SOURCE_OPENAIP_CORE_API_KEY",
		"catalog:concrete:weather-climate-environment-biodiversity-and-energy:noaa-climate-data-online":      "SOURCE_NOAA_CLIMATE_DATA_ONLINE_TOKEN",
		"catalog:concrete:weather-climate-environment-biodiversity-and-energy:ebird-data-products":           "SOURCE_EBIRD_DATA_PRODUCTS_API_KEY",
		"catalog:concrete:corporate-ownership-sanctions-procurement-legal-and-ip:sam-gov-data-services":      "SOURCE_SAM_GOV_DATA_SERVICES_API_KEY",
	}
	for _, entry := range catalog.Entries {
		if entry.CatalogKind != "concrete" || !concreteRequiresCredential(entry) {
			continue
		}
		credentialed++
		if strings.TrimSpace(entry.AuthConfig.EnvVar) == "" {
			t.Fatalf("expected credential-gated concrete entry %s to include auth env var", entry.CatalogID)
		}
		if !validSourceEnvVarName(entry.AuthConfig.EnvVar) {
			t.Fatalf("expected credential-gated concrete entry %s to use deterministic env-var naming, got %q", entry.CatalogID, entry.AuthConfig.EnvVar)
		}
		runtimeSourceID := effectiveRuntimeSourceID(entry)
		if strings.TrimSpace(runtimeSourceID) == "" {
			if strings.TrimSpace(entry.DeferredReason) == "" {
				t.Fatalf("expected deferred credential-gated concrete entry %s to include deferred_reason", entry.CatalogID)
			}
			continue
		}
		if entry.RuntimeSourceID == "fixture:acled" && entry.AuthConfig.EnvVar != "ACLED_API_KEY" {
			t.Fatalf("expected ACLED runtime-linked catalog row to preserve ACLED_API_KEY contract, got %q", entry.AuthConfig.EnvVar)
		}
		if wantEnvVar, ok := wantEnvVars[entry.CatalogID]; ok && entry.AuthConfig.EnvVar != wantEnvVar {
			t.Fatalf("expected credential-gated catalog row %s to use env var %q, got %q", entry.CatalogID, wantEnvVar, entry.AuthConfig.EnvVar)
		}
	}
	if credentialed != 24 {
		t.Fatalf("expected 24 credential-gated concrete entries, got %d", credentialed)
	}
}

func TestCompileSourceCatalogRejectsDeferredRuntimeMapping(t *testing.T) {
	tempDir := t.TempDir()
	catalogPath := filepath.Join(tempDir, "source_catalog.json")
	registryPath := filepath.Join("..", "..", "seed", "source_registry.json")
	catalog := mustLoadSourceCatalogFixture(t)
	idx := -1
	for i, entry := range catalog.Entries {
		if entry.CatalogKind == "concrete" && strings.TrimSpace(entry.RuntimeSourceID) != "" {
			idx = i
			break
		}
	}
	if idx == -1 {
		t.Fatal("expected runtime-linked concrete catalog entry")
	}
	catalog.Entries[idx].IntegrationArchetype = "deferred_transport"
	catalog.Entries[idx].ParserID = ""
	catalog.Entries[idx].DeferredReason = "requires browser workflow"
	b, err := json.Marshal(catalog)
	if err != nil {
		t.Fatalf("marshal deferred runtime catalog: %v", err)
	}
	if err := os.WriteFile(catalogPath, b, 0o644); err != nil {
		t.Fatalf("write deferred runtime catalog: %v", err)
	}
	compiled, err := compileSourceCatalog(catalogPath, registryPath)
	if err != nil {
		t.Fatalf("compile source catalog: %v", err)
	}
	if strings.TrimSpace(compiled.Catalog.Entries[idx].RuntimeSourceID) != "" {
		t.Fatalf("expected deferred transport entry to remain non-runtime, got %q", compiled.Catalog.Entries[idx].RuntimeSourceID)
	}
}

func TestCompileSourceCatalogRuntimeSeedParity(t *testing.T) {
	compiled, err := compileSourceCatalog(filepath.Join("..", "..", "seed", "source_catalog.json"), filepath.Join("..", "..", "seed", "source_registry.json"))
	if err != nil {
		t.Fatalf("compile source catalog: %v", err)
	}
	entryByRuntimeSourceID := map[string]sourceCatalogEntry{}
	for _, entry := range compiled.Catalog.Entries {
		if entry.CatalogKind == "concrete" && strings.TrimSpace(entry.RuntimeSourceID) != "" {
			entryByRuntimeSourceID[entry.RuntimeSourceID] = entry
		}
	}
	for _, seed := range compiled.RunnableSeeds {
		entry, ok := entryByRuntimeSourceID[seed.SourceID]
		if !ok {
			t.Fatalf("expected runtime-linked catalog entry for seed %s", seed.SourceID)
		}
		if err := validateRuntimeSeedParity(entry, seed); err != nil {
			t.Fatalf("expected runtime seed parity for %s, got %v", seed.SourceID, err)
		}
	}
}

func TestBronzeManifestSourceIDDerivesCatalogAutoIDs(t *testing.T) {
	entry := sourceCatalogEntry{
		CatalogID:            "catalog:concrete:discovery-catalogs-platform-fingerprints-and-archives:dataportals-org",
		CatalogKind:          "concrete",
		IntegrationArchetype: "html_profile",
	}
	if got := bronzeManifestSourceID(entry); got != "catalog:auto:discovery-catalogs-platform-fingerprints-and-archives-dataportals-org" {
		t.Fatalf("expected derived bronze manifest source id, got %q", got)
	}
	entry.RuntimeSourceID = "fixture:reliefweb"
	if got := bronzeManifestSourceID(entry); got != "fixture:reliefweb" {
		t.Fatalf("expected explicit runtime source id to win, got %q", got)
	}
}

func mustLoadSourceCatalogFixture(t *testing.T) sourceCatalogFile {
	t.Helper()
	catalog, err := loadSourceCatalog(filepath.Join("..", "..", "seed", "source_catalog.json"))
	if err != nil {
		t.Fatalf("load source catalog fixture: %v", err)
	}
	return catalog
}

func parseCatalogMarkdownRows(t *testing.T, paths ...string) map[string]struct{} {
	t.Helper()
	rows := map[string]struct{}{}
	for _, path := range paths {
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read markdown rows: %v", err)
		}
		section := ""
		tableMode := false
		nameIndex := 0
		for _, line := range strings.Split(string(b), "\n") {
			if strings.HasPrefix(line, "# ") {
				section = strings.TrimSpace(strings.TrimPrefix(line, "# "))
				section = strings.TrimSpace(numberedSectionPrefix.ReplaceAllString(section, ""))
				tableMode = false
				continue
			}
			if strings.HasPrefix(line, "## ") {
				section = strings.TrimSpace(strings.TrimPrefix(line, "## "))
				section = strings.TrimSpace(numberedSectionPrefix.ReplaceAllString(section, ""))
				tableMode = false
				continue
			}
			if strings.HasPrefix(line, "| Source |") || strings.HasPrefix(line, "| Source family |") {
				tableMode = true
				nameIndex = 0
				continue
			}
			if strings.HasPrefix(line, "| source_id |") {
				tableMode = true
				nameIndex = 1
				continue
			}
			if strings.HasPrefix(line, "| family_id |") {
				tableMode = true
				nameIndex = 1
				continue
			}
			if !tableMode || !strings.HasPrefix(line, "|") || strings.Contains(line, "|---") {
				continue
			}
			parts := strings.Split(strings.Trim(line, "|"), "|")
			if len(parts) <= nameIndex {
				continue
			}
			name := strings.TrimSpace(parts[nameIndex])
			if name == "" {
				continue
			}
			rows[section+"\x00"+name] = struct{}{}
		}
	}
	return rows
}

func readRepoBytes(t *testing.T, parts ...string) []byte {
	t.Helper()
	path := filepath.Join(parts...)
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read repo bytes %s: %v", path, err)
	}
	return b
}

func TestCompileSourceCatalogDoesNotImplicitlyRuntimeLinkDeferredConcreteEntries(t *testing.T) {
	compiled, err := compileSourceCatalog(filepath.Join("..", "..", "seed", "source_catalog.json"), filepath.Join("..", "..", "seed", "source_registry.json"))
	if err != nil {
		t.Fatalf("compile source catalog: %v", err)
	}
	runtimeLinkedEntries := 0
	deferredEntries := 0
	for _, entry := range compiled.Catalog.Entries {
		if entry.CatalogKind != "concrete" {
			continue
		}
		if strings.TrimSpace(entry.RuntimeSourceID) != "" {
			runtimeLinkedEntries++
			continue
		}
		if strings.TrimSpace(entry.IntegrationArchetype) == "deferred_transport" {
			continue
		}
		deferredEntries++
		if strings.TrimSpace(entry.DeferredReason) == "" {
			t.Fatalf("expected deferred concrete catalog entry %s to remain explicitly deferred in compiled output", entry.CatalogID)
		}
	}
	if runtimeLinkedEntries != len(compiled.RunnableSeeds) {
		t.Fatalf("expected compiled runtime-linked entry count %d to equal runnable seed count %d", runtimeLinkedEntries, len(compiled.RunnableSeeds))
	}
	if deferredEntries == 0 {
		t.Fatal("expected compiled catalog to preserve explicitly deferred concrete entries")
	}
}

func TestBronzeTableForSourceIDLongSimilarIDsDoNotCollide(t *testing.T) {
	left := "catalog:auto:discovery-catalogs-platform-fingerprints-and-archives-dataportals-org-aaaaaaaaaaaaaaaaaaaa"
	right := "catalog:auto:discovery-catalogs-platform-fingerprints-and-archives-dataportals-org-bbbbbbbbbbbbbbbbbbbb"
	leftTable := bronzeTableForSourceID(left)
	rightTable := bronzeTableForSourceID(right)
	if leftTable == rightTable {
		t.Fatalf("expected distinct bronze tables, got %q", leftTable)
	}
	if !strings.HasPrefix(leftTable, "bronze.src_") || !strings.HasSuffix(leftTable, "_v1") {
		t.Fatalf("unexpected left bronze table format %q", leftTable)
	}
	if !strings.HasPrefix(rightTable, "bronze.src_") || !strings.HasSuffix(rightTable, "_v1") {
		t.Fatalf("unexpected right bronze table format %q", rightTable)
	}
}
