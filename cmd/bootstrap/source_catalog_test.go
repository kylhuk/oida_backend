package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

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
	if len(compiled.RunnableSeeds) != 7 {
		t.Fatalf("expected 7 runnable seeds mapped from catalog, got %d", len(compiled.RunnableSeeds))
	}
	if len(compiled.FingerprintProbes) != 16 {
		t.Fatalf("expected 16 fingerprint probes, got %d", len(compiled.FingerprintProbes))
	}
	if len(compiled.FamilyTemplates) != 18 {
		t.Fatalf("expected 18 family templates, got %d", len(compiled.FamilyTemplates))
	}
	if len(compiled.BronzeDDLManifest) != 7 {
		t.Fatalf("expected 7 bronze manifest rows, got %d", len(compiled.BronzeDDLManifest))
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
		if len(template.GeneratorRelationships) == 0 || template.GeneratorRelationships[0] != "review_pipeline:family_template_candidate" {
			t.Fatalf("expected compiled family template %s to preserve family review pipeline", template.CatalogID)
		}
	}
}

func TestCompiledSourceCounts(t *testing.T) {
	compiled, err := compileSourceCatalog(filepath.Join("..", "..", "seed", "source_catalog.json"), filepath.Join("..", "..", "seed", "source_registry.json"))
	if err != nil {
		t.Fatalf("compile source catalog: %v", err)
	}
	if len(compiled.Catalog.Entries) != 240 {
		t.Fatalf("expected 240 catalog entries, got %d", len(compiled.Catalog.Entries))
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
	if nonFamilyCount != 222 {
		t.Fatalf("expected 222 non-family rows from sources.md, got %d", nonFamilyCount)
	}
	if counts["concrete"] != 206 || counts["fingerprint"] != 16 || counts["family"] != 18 {
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
	bronzeSources := map[string]struct{}{}
	for _, seed := range compiled.RunnableSeeds {
		if strings.TrimSpace(seed.BronzeTable) != "" {
			bronzeSources[seed.SourceID] = struct{}{}
		}
	}
	if len(bronzeSources) != len(compiled.BronzeDDLManifest) {
		t.Fatalf("expected bronze manifest count %d to equal runnable bronze source count %d", len(compiled.BronzeDDLManifest), len(bronzeSources))
	}
	for _, row := range compiled.BronzeDDLManifest {
		if _, ok := bronzeSources[row.SourceID]; !ok {
			t.Fatalf("unexpected bronze manifest source %q", row.SourceID)
		}
	}

	markdownRows := parseSourcesMarkdownRows(t, filepath.Join("..", "..", "sources.md"))
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
	if len(compiled.BronzeDDLManifest) != len(compiled.RunnableSeeds) {
		t.Fatalf("expected one bronze manifest row per runnable seed, got %d manifests for %d seeds", len(compiled.BronzeDDLManifest), len(compiled.RunnableSeeds))
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
	if concreteCount != 206 {
		t.Fatalf("expected 206 concrete catalog entries, got %d", concreteCount)
	}
	if deferredCount != 0 {
		t.Fatalf("expected 0 deferred concrete entries in current catalog snapshot, got %d", deferredCount)
	}
	if explicitDeferredCount != 199 {
		t.Fatalf("expected 199 non-runtime concrete entries in current catalog snapshot, got %d", explicitDeferredCount)
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
	for _, entry := range catalog.Entries {
		if entry.CatalogKind != "concrete" || entry.CredentialRequirement.RestrictedAccess {
			continue
		}
		publicConcrete++
		if strings.TrimSpace(entry.RuntimeSourceID) != "" {
			runtimeLinked++
			if strings.TrimSpace(entry.DeferredReason) != "" {
				t.Fatalf("expected runnable concrete entry %s to omit deferred_reason", entry.CatalogID)
			}
			continue
		}
		if strings.TrimSpace(entry.DeferredReason) == "" {
			t.Fatalf("expected public concrete entry %s to be explicitly deferred", entry.CatalogID)
		}
		explicitlyDeferred++
	}
	if publicConcrete != 191 {
		t.Fatalf("expected 191 public concrete entries, got %d", publicConcrete)
	}
	if runtimeLinked != 7 || explicitlyDeferred != 184 {
		t.Fatalf("expected public concrete coverage runtime=7 deferred=184, got runtime=%d deferred=%d", runtimeLinked, explicitlyDeferred)
	}
}

func TestApprovedRunnableSourceCoverage(t *testing.T) {
	compiled, err := compileSourceCatalog(filepath.Join("..", "..", "seed", "source_catalog.json"), filepath.Join("..", "..", "seed", "source_registry.json"))
	if err != nil {
		t.Fatalf("compile source catalog: %v", err)
	}
	if len(compiled.RunnableSeeds) != 7 {
		t.Fatalf("expected 7 approved runnable seeds, got %d", len(compiled.RunnableSeeds))
	}
	for _, seed := range compiled.RunnableSeeds {
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
	}
}

func TestCredentialedSourcesAreDisabledByDefault(t *testing.T) {
	catalog := mustLoadSourceCatalogFixture(t)
	credentialed := 0
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
		if strings.TrimSpace(entry.RuntimeSourceID) == "" {
			if strings.TrimSpace(entry.DeferredReason) == "" {
				t.Fatalf("expected credential-gated concrete entry %s to stay non-runnable by default", entry.CatalogID)
			}
			continue
		}
		if entry.RuntimeSourceID == "fixture:acled" && entry.AuthConfig.EnvVar != "ACLED_API_KEY" {
			t.Fatalf("expected ACLED runtime-linked catalog row to preserve ACLED_API_KEY contract, got %q", entry.AuthConfig.EnvVar)
		}
	}
	if credentialed != 16 {
		t.Fatalf("expected 16 credential-gated concrete entries, got %d", credentialed)
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
	if _, err := compileSourceCatalog(catalogPath, registryPath); err == nil || !strings.Contains(err.Error(), "must not map deferred_transport") {
		t.Fatalf("expected deferred runtime mapping rejection, got %v", err)
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

func mustLoadSourceCatalogFixture(t *testing.T) sourceCatalogFile {
	t.Helper()
	catalog, err := loadSourceCatalog(filepath.Join("..", "..", "seed", "source_catalog.json"))
	if err != nil {
		t.Fatalf("load source catalog fixture: %v", err)
	}
	return catalog
}

func parseSourcesMarkdownRows(t *testing.T, path string) map[string]struct{} {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read markdown rows: %v", err)
	}
	rows := map[string]struct{}{}
	section := ""
	tableMode := false
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, "## ") {
			section = strings.TrimSpace(strings.TrimPrefix(line, "## "))
			tableMode = false
			continue
		}
		if strings.HasPrefix(line, "| Source |") || strings.HasPrefix(line, "| Source family |") {
			tableMode = true
			continue
		}
		if !tableMode || !strings.HasPrefix(line, "|") || strings.Contains(line, "|---") {
			continue
		}
		parts := strings.Split(strings.Trim(line, "|"), "|")
		if len(parts) == 0 {
			continue
		}
		name := strings.TrimSpace(parts[0])
		if name == "" {
			continue
		}
		rows[section+"\x00"+name] = struct{}{}
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
