package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"global-osint-backend/internal/parser"
)

type sourceCatalogFile struct {
	SchemaVersion          int                  `json:"schema_version"`
	SourceMarkdownPath     string               `json:"source_markdown_path"`
	SourceMarkdownChecksum string               `json:"source_markdown_checksum"`
	Entries                []sourceCatalogEntry `json:"entries"`
}

type sourceCatalogEntry struct {
	CatalogID              string                             `json:"catalog_id"`
	CatalogKind            string                             `json:"catalog_kind"`
	Name                   string                             `json:"name"`
	Category               string                             `json:"category"`
	Scope                  string                             `json:"scope"`
	Produces               string                             `json:"produces"`
	Tags                   []string                           `json:"tags"`
	AccessNotes            string                             `json:"access_notes"`
	OfficialDocsURL        string                             `json:"official_docs_url"`
	IntegrationArchetype   string                             `json:"integration_archetype"`
	AuthConfig             sourceCatalogAuthConfig            `json:"auth_config_json,omitempty"`
	ParserID               string                             `json:"parser_id,omitempty"`
	DeferredReason         string                             `json:"deferred_reason,omitempty"`
	GeneratorKind          string                             `json:"generator_kind"`
	RuntimeSourceID        string                             `json:"runtime_source_id"`
	CredentialRequirement  sourceCatalogCredentialRequirement `json:"credential_requirement"`
	GeneratorRelationships []string                           `json:"generator_relationships"`
	ProbePatterns          []string                           `json:"probe_patterns,omitempty"`
	SourceMarkdownLine     int                                `json:"source_markdown_line"`
}

type sourceCatalogCredentialRequirement struct {
	RequiresRegistration bool `json:"requires_registration"`
	RequiresApproval     bool `json:"requires_approval"`
	CommercialTerms      bool `json:"commercial_terms"`
	NoncommercialTerms   bool `json:"noncommercial_terms"`
	RestrictedAccess     bool `json:"restricted_access"`
}

type sourceCatalogAuthConfig struct {
	EnvVar    string `json:"env_var,omitempty"`
	Placement string `json:"placement,omitempty"`
	Name      string `json:"name,omitempty"`
	Prefix    string `json:"prefix,omitempty"`
}

type sourceFingerprintProbe struct {
	CatalogID            string   `json:"catalog_id"`
	Name                 string   `json:"name"`
	IntegrationArchetype string   `json:"integration_archetype"`
	ProbePatterns        []string `json:"probe_patterns"`
}

type sourceFamilyTemplate struct {
	CatalogID              string                          `json:"catalog_id"`
	Name                   string                          `json:"name"`
	Scope                  string                          `json:"scope"`
	Outputs                string                          `json:"outputs"`
	IntegrationArchetype   string                          `json:"integration_archetype"`
	TransportType          string                          `json:"transport_type"`
	ScopeLevels            []string                        `json:"scope_levels"`
	ReviewStatusDefault    string                          `json:"review_status_default"`
	GeneratorRelationships []string                        `json:"generator_relationships"`
	Tags                   []string                        `json:"tags"`
	ChildSource            sourceFamilyChildSourceTemplate `json:"child_source"`
}

type sourceFamilyChildSourceTemplate struct {
	TransportType        string   `json:"transport_type"`
	IntegrationArchetype string   `json:"integration_archetype"`
	FormatHint           string   `json:"format_hint"`
	ParserID             string   `json:"parser_id"`
	SourceClass          string   `json:"source_class"`
	RefreshStrategy      string   `json:"refresh_strategy"`
	CrawlStrategy        string   `json:"crawl_strategy"`
	ExpectedPlaceTypes   []string `json:"expected_place_types"`
}

type compiledSourceCatalog struct {
	Catalog           sourceCatalogFile         `json:"catalog"`
	CatalogChecksum   string                    `json:"catalog_checksum"`
	RunnableSeeds     []sourceSeed              `json:"runnable_seeds"`
	FingerprintProbes []sourceFingerprintProbe  `json:"fingerprint_probes"`
	FamilyTemplates   []sourceFamilyTemplate    `json:"family_templates"`
	BronzeDDLManifest []sourceBronzeDDLManifest `json:"bronze_ddl_manifest"`
}

type sourceBronzeDDLManifest struct {
	SourceID            string `json:"source_id"`
	BronzeTable         string `json:"bronze_table"`
	BronzeSchemaVersion int    `json:"bronze_schema_version"`
	PromoteProfile      string `json:"promote_profile"`
}

type runtimeSourceOverride struct {
	Entrypoints        []string
	AuthMode           string
	AuthConfig         map[string]any
	RequestsPerMinute  int
	BurstSize          int
	RefreshStrategy    string
	CrawlStrategy      string
	LifecycleState     string
	CrawlEnabled       *bool
	PromoteProfile     string
	EntityTypes        []string
	ExpectedPlaceTypes []string
	SupportsHistorical *bool
	BackfillPriority   *int
	ReviewStatus       string
	ReviewNotes        string
}

var phase1TelemetryLandingTargets = map[string]string{
	"catalog:auto:aviation-airports-drones-and-mobility-opensky-network":  "silver.fact_track_point",
	"catalog:auto:aviation-airports-drones-and-mobility-airplanes-live":   "silver.fact_track_point",
	"catalog:auto:security-addendum-air-adsblol-api":                      "silver.fact_track_point",
	"catalog:auto:maritime-ocean-and-coastal-sources-aishub":              "silver.fact_track_point",
	"catalog:auto:maritime-ocean-and-coastal-sources-aisstream":           "silver.fact_track_point",
	"catalog:auto:maritime-ocean-and-coastal-sources-vesselfinder":        "silver.fact_track_point",
	"catalog:auto:maritime-ocean-and-coastal-sources-vesselfinder-routes": "ops.vesselfinder_route_plan",
	"catalog:auto:aviation-airports-drones-and-mobility-openaip-core-api": "silver.dim_entity",
}

func intPtr(value int) *int {
	v := value
	return &v
}

func loadSourceCatalog(path string) (sourceCatalogFile, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return sourceCatalogFile{}, err
	}
	var catalog sourceCatalogFile
	if err := json.Unmarshal(b, &catalog); err != nil {
		return sourceCatalogFile{}, err
	}
	if err := validateSourceCatalog(catalog); err != nil {
		return sourceCatalogFile{}, err
	}
	return catalog, nil
}

func compileSourceCatalog(catalogPath, registrySeedPath string) (compiledSourceCatalog, error) {
	catalog, err := loadSourceCatalog(catalogPath)
	if err != nil {
		return compiledSourceCatalog{}, err
	}
	registrySeeds, err := loadSourceSeedFile(registrySeedPath)
	if err != nil {
		return compiledSourceCatalog{}, err
	}
	registryByID := make(map[string]sourceSeed, len(registrySeeds))
	for _, seed := range registrySeeds {
		registryByID[strings.TrimSpace(seed.SourceID)] = seed
	}

	compiled := compiledSourceCatalog{
		Catalog:           catalog,
		RunnableSeeds:     make([]sourceSeed, 0),
		FingerprintProbes: make([]sourceFingerprintProbe, 0),
		FamilyTemplates:   make([]sourceFamilyTemplate, 0),
		BronzeDDLManifest: make([]sourceBronzeDDLManifest, 0),
	}

	for idx, rawEntry := range catalog.Entries {
		entry := rawEntry
		entry.RuntimeSourceID = effectiveRuntimeSourceID(rawEntry)
		catalog.Entries[idx] = entry
		switch entry.CatalogKind {
		case "concrete":
			if strings.TrimSpace(entry.IntegrationArchetype) == "deferred_transport" && strings.TrimSpace(entry.RuntimeSourceID) != "" {
				return compiledSourceCatalog{}, fmt.Errorf("catalog entry %s must not map deferred_transport into runtime_source_id %q", entry.CatalogID, entry.RuntimeSourceID)
			}
			if entry.RuntimeSourceID != "" {
				seed, ok := registryByID[entry.RuntimeSourceID]
				if ok {
					if err := validateRuntimeSeedParity(entry, seed); err != nil {
						return compiledSourceCatalog{}, fmt.Errorf("catalog entry %s runtime parity: %w", entry.CatalogID, err)
					}
				} else {
					seed, err = synthesizedRuntimeSeed(entry)
					if err != nil {
						return compiledSourceCatalog{}, fmt.Errorf("catalog entry %s runtime synthesis: %w", entry.CatalogID, err)
					}
				}
				compiled.RunnableSeeds = append(compiled.RunnableSeeds, seed)
			}
			manifestRow, ok, err := bronzeManifestRowForCatalogEntry(entry, registryByID)
			if err != nil {
				return compiledSourceCatalog{}, fmt.Errorf("catalog entry %s bronze manifest: %w", entry.CatalogID, err)
			}
			if ok {
				compiled.BronzeDDLManifest = append(compiled.BronzeDDLManifest, manifestRow)
			}
		case "fingerprint":
			compiled.FingerprintProbes = append(compiled.FingerprintProbes, sourceFingerprintProbe{
				CatalogID:            entry.CatalogID,
				Name:                 entry.Name,
				IntegrationArchetype: entry.IntegrationArchetype,
				ProbePatterns:        cloneStrings(entry.ProbePatterns),
			})
		case "family":
			childTemplate := familyChildSourceTemplate(entry)
			compiled.FamilyTemplates = append(compiled.FamilyTemplates, sourceFamilyTemplate{
				CatalogID:              entry.CatalogID,
				Name:                   entry.Name,
				Scope:                  entry.Scope,
				Outputs:                entry.Produces,
				IntegrationArchetype:   entry.IntegrationArchetype,
				TransportType:          familyTemplateTransportType(entry),
				ScopeLevels:            familyTemplateScopeLevels(entry),
				ReviewStatusDefault:    "review_required",
				GeneratorRelationships: cloneStrings(entry.GeneratorRelationships),
				Tags:                   cloneStrings(entry.Tags),
				ChildSource:            childTemplate,
			})
		}
	}

	compiled.Catalog = catalog
	compiled.CatalogChecksum = sourceCatalogChecksum(catalog)
	return compiled, nil
}

func loadCompiledSourceCatalog(path string) (compiledSourceCatalog, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return compiledSourceCatalog{}, err
	}
	var compiled compiledSourceCatalog
	if err := json.Unmarshal(b, &compiled); err != nil {
		return compiledSourceCatalog{}, err
	}
	hydrateCompiledFamilyTemplates(&compiled)
	if err := validateCompiledSourceCatalog(path, compiled); err != nil {
		return compiledSourceCatalog{}, err
	}
	return compiled, nil
}

func hydrateCompiledFamilyTemplates(compiled *compiledSourceCatalog) {
	if compiled == nil {
		return
	}
	entries := map[string]sourceCatalogEntry{}
	for _, entry := range compiled.Catalog.Entries {
		if entry.CatalogKind == "family" {
			entries[strings.TrimSpace(entry.CatalogID)] = entry
		}
	}
	for i := range compiled.FamilyTemplates {
		entry, ok := entries[strings.TrimSpace(compiled.FamilyTemplates[i].CatalogID)]
		if !ok {
			continue
		}
		if strings.TrimSpace(compiled.FamilyTemplates[i].TransportType) == "" {
			compiled.FamilyTemplates[i].TransportType = familyTemplateTransportType(entry)
		}
		if len(compiled.FamilyTemplates[i].ScopeLevels) == 0 {
			compiled.FamilyTemplates[i].ScopeLevels = familyTemplateScopeLevels(entry)
		}
		if strings.TrimSpace(compiled.FamilyTemplates[i].ChildSource.TransportType) == "" || strings.TrimSpace(compiled.FamilyTemplates[i].ChildSource.IntegrationArchetype) == "" || strings.TrimSpace(compiled.FamilyTemplates[i].ChildSource.ParserID) == "" {
			compiled.FamilyTemplates[i].ChildSource = familyChildSourceTemplate(entry)
		}
	}
}

func loadRunnableSourceSeeds(path string) ([]sourceSeed, error) {
	compiled, err := loadCompiledSourceCatalog(path)
	if err == nil {
		return expandRunnableSeedsFromCompiled(compiled)
	}
	compiledCandidate, probeErr := looksLikeCompiledSourceCatalog(path)
	if probeErr != nil {
		return nil, probeErr
	}
	if compiledCandidate {
		return nil, err
	}
	return loadSourceSeedFile(path)
}

func expandRunnableSeedsFromCompiled(compiled compiledSourceCatalog) ([]sourceSeed, error) {
	seeds := make([]sourceSeed, 0, len(compiled.Catalog.Entries))
	bySourceID := make(map[string]sourceSeed, len(compiled.RunnableSeeds))
	for _, seed := range compiled.RunnableSeeds {
		bySourceID[strings.TrimSpace(seed.SourceID)] = seed
	}
	for _, rawEntry := range compiled.Catalog.Entries {
		entry := rawEntry
		runtimeSourceID := effectiveRuntimeSourceID(entry)
		if runtimeSourceID == "" {
			continue
		}
		entry.RuntimeSourceID = runtimeSourceID
		if seed, ok := bySourceID[runtimeSourceID]; ok {
			if err := validateRuntimeSeedParity(entry, seed); err != nil {
				return nil, fmt.Errorf("catalog entry %s runtime parity: %w", entry.CatalogID, err)
			}
			seeds = append(seeds, seed)
			continue
		}
		seed, err := synthesizedRuntimeSeed(entry)
		if err != nil {
			return nil, fmt.Errorf("catalog entry %s runtime synthesis: %w", entry.CatalogID, err)
		}
		seeds = append(seeds, seed)
	}
	return seeds, nil
}

func looksLikeCompiledSourceCatalog(path string) (bool, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return false, err
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(b, &raw); err != nil {
		return false, nil
	}
	_, hasCatalog := raw["catalog"]
	_, hasCatalogChecksum := raw["catalog_checksum"]
	_, hasRunnableSeeds := raw["runnable_seeds"]
	return hasCatalog || hasCatalogChecksum || hasRunnableSeeds, nil
}

func loadSourceSeedFile(path string) ([]sourceSeed, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var seeds []sourceSeed
	if err := json.Unmarshal(b, &seeds); err != nil {
		return nil, err
	}
	return seeds, nil
}

func validateSourceCatalog(catalog sourceCatalogFile) error {
	if catalog.SchemaVersion != 1 {
		return fmt.Errorf("unsupported source catalog schema_version %d", catalog.SchemaVersion)
	}
	if strings.TrimSpace(catalog.SourceMarkdownChecksum) == "" {
		return fmt.Errorf("source_markdown_checksum is required")
	}
	if strings.TrimSpace(catalog.SourceMarkdownPath) == "" {
		return fmt.Errorf("source_markdown_path is required")
	}
	if len(catalog.Entries) == 0 {
		return fmt.Errorf("source catalog must contain entries")
	}
	seenCatalogIDs := map[string]struct{}{}
	seenRuntimeSourceIDs := map[string]string{}
	for _, entry := range catalog.Entries {
		entryID := strings.TrimSpace(entry.CatalogID)
		if entryID == "" {
			return fmt.Errorf("source catalog entry missing catalog_id")
		}
		if _, ok := seenCatalogIDs[entryID]; ok {
			return fmt.Errorf("duplicate catalog_id %q", entryID)
		}
		seenCatalogIDs[entryID] = struct{}{}
		if strings.TrimSpace(entry.Name) == "" {
			return fmt.Errorf("source catalog entry %s missing name", entryID)
		}
		if strings.TrimSpace(entry.Category) == "" {
			return fmt.Errorf("source catalog entry %s missing category", entryID)
		}
		switch entry.CatalogKind {
		case "concrete", "fingerprint", "family":
		default:
			return fmt.Errorf("source catalog entry %s has unsupported catalog_kind %q", entryID, entry.CatalogKind)
		}
		switch entry.GeneratorKind {
		case "direct", "fingerprint_probe", "family_template":
		default:
			return fmt.Errorf("source catalog entry %s has unsupported generator_kind %q", entryID, entry.GeneratorKind)
		}
		if entry.IntegrationArchetype == "" {
			return fmt.Errorf("source catalog entry %s missing integration_archetype", entryID)
		}
		if err := validateCatalogEntryExecutionContract(entry); err != nil {
			return fmt.Errorf("source catalog entry %s invalid execution contract: %w", entryID, err)
		}
		switch entry.CatalogKind {
		case "concrete":
			if entry.GeneratorKind != "direct" {
				return fmt.Errorf("source catalog concrete entry %s must use generator_kind direct", entryID)
			}
		case "fingerprint":
			if entry.GeneratorKind != "fingerprint_probe" {
				return fmt.Errorf("source catalog fingerprint entry %s must use generator_kind fingerprint_probe", entryID)
			}
			if len(entry.GeneratorRelationships) == 0 {
				return fmt.Errorf("source catalog fingerprint %s must declare generator_relationships", entryID)
			}
		case "family":
			if entry.GeneratorKind != "family_template" {
				return fmt.Errorf("source catalog family entry %s must use generator_kind family_template", entryID)
			}
			if len(entry.GeneratorRelationships) == 0 {
				return fmt.Errorf("source catalog family %s must declare generator_relationships", entryID)
			}
		}
		if entry.CatalogKind == "fingerprint" && len(entry.ProbePatterns) == 0 {
			return fmt.Errorf("source catalog fingerprint %s must declare probe_patterns", entryID)
		}
		runtimeSourceID := effectiveRuntimeSourceID(entry)
		if runtimeSourceID != "" {
			if entry.CatalogKind != "concrete" {
				return fmt.Errorf("source catalog entry %s may only assign runtime_source_id on concrete entries", entryID)
			}
			if previous, ok := seenRuntimeSourceIDs[runtimeSourceID]; ok {
				return fmt.Errorf("runtime_source_id %q reused by %s and %s", runtimeSourceID, previous, entryID)
			}
			seenRuntimeSourceIDs[runtimeSourceID] = entryID
		}
	}
	return nil
}

func validateCompiledSourceCatalog(path string, compiled compiledSourceCatalog) error {
	if err := validateSourceCatalog(compiled.Catalog); err != nil {
		return fmt.Errorf("compiled source catalog invalid: %w", err)
	}
	if compiled.CatalogChecksum != sourceCatalogChecksum(compiled.Catalog) {
		return fmt.Errorf("compiled source catalog checksum mismatch")
	}
	markdownPath := compiled.Catalog.SourceMarkdownPath
	if !filepath.IsAbs(markdownPath) {
		markdownPath = filepath.Clean(filepath.Join(filepath.Dir(path), "..", markdownPath))
	}
	markdownBytes, err := os.ReadFile(markdownPath)
	if err != nil {
		return fmt.Errorf("compiled source catalog markdown verification: %w", err)
	}
	if compiled.Catalog.SourceMarkdownChecksum != sum(markdownBytes) {
		return fmt.Errorf("compiled source catalog markdown checksum mismatch")
	}
	manifestBySourceID := make(map[string]sourceBronzeDDLManifest, len(compiled.BronzeDDLManifest))
	manifestBronzeTables := make(map[string]string, len(compiled.BronzeDDLManifest))
	for _, manifest := range compiled.BronzeDDLManifest {
		sourceID := strings.TrimSpace(manifest.SourceID)
		if sourceID == "" {
			return fmt.Errorf("compiled source catalog bronze manifest contains empty source_id")
		}
		if _, ok := manifestBySourceID[sourceID]; ok {
			return fmt.Errorf("compiled source catalog bronze manifest duplicates source_id %q", sourceID)
		}
		manifestBySourceID[sourceID] = manifest
		bronzeTable := strings.TrimSpace(manifest.BronzeTable)
		if bronzeTable == "" {
			return fmt.Errorf("compiled source catalog bronze manifest %s missing bronze_table", sourceID)
		}
		if priorSourceID, ok := manifestBronzeTables[bronzeTable]; ok {
			return fmt.Errorf("compiled source catalog bronze manifest duplicates bronze_table %q for %s and %s", bronzeTable, priorSourceID, sourceID)
		}
		manifestBronzeTables[bronzeTable] = sourceID
	}
	for _, entry := range compiled.Catalog.Entries {
		manifestSourceID := bronzeManifestSourceID(entry)
		if manifestSourceID == "" {
			continue
		}
		manifest, ok := manifestBySourceID[manifestSourceID]
		if !ok {
			return fmt.Errorf("compiled source catalog missing bronze manifest for %s", manifestSourceID)
		}
		if strings.TrimSpace(entry.RuntimeSourceID) == "" {
			seed, err := synthesizedRuntimeSeedForSourceID(entry, manifestSourceID)
			if err != nil {
				return fmt.Errorf("compiled source catalog bronze manifest synthesis for %s: %w", manifestSourceID, err)
			}
			if manifest.BronzeTable != seed.BronzeTable || manifest.BronzeSchemaVersion != seed.BronzeSchemaVersion || manifest.PromoteProfile != seed.PromoteProfile {
				return fmt.Errorf("compiled source catalog bronze manifest mismatch for %s", manifestSourceID)
			}
		}
	}
	for _, seed := range compiled.RunnableSeeds {
		if strings.TrimSpace(seed.BronzeTable) == "" {
			continue
		}
		manifest, ok := manifestBySourceID[strings.TrimSpace(seed.SourceID)]
		if !ok {
			return fmt.Errorf("compiled source catalog missing bronze manifest for %s", seed.SourceID)
		}
		if manifest.BronzeTable != seed.BronzeTable || manifest.BronzeSchemaVersion != seed.BronzeSchemaVersion || manifest.PromoteProfile != seed.PromoteProfile {
			return fmt.Errorf("compiled source catalog bronze manifest mismatch for %s", seed.SourceID)
		}
	}
	familyCatalogIDs := map[string]sourceCatalogEntry{}
	for _, entry := range compiled.Catalog.Entries {
		if entry.CatalogKind == "family" {
			familyCatalogIDs[entry.CatalogID] = entry
		}
	}
	if len(compiled.FamilyTemplates) != len(familyCatalogIDs) {
		return fmt.Errorf("compiled source catalog family template count mismatch: got %d want %d", len(compiled.FamilyTemplates), len(familyCatalogIDs))
	}
	for _, template := range compiled.FamilyTemplates {
		entry, ok := familyCatalogIDs[strings.TrimSpace(template.CatalogID)]
		if !ok {
			return fmt.Errorf("compiled family template %s does not map to a family catalog entry", template.CatalogID)
		}
		if strings.TrimSpace(template.Name) != strings.TrimSpace(entry.Name) || strings.TrimSpace(template.Outputs) != strings.TrimSpace(entry.Produces) {
			return fmt.Errorf("compiled family template %s name/outputs mismatch", template.CatalogID)
		}
		if strings.TrimSpace(template.Scope) != strings.TrimSpace(entry.Scope) {
			return fmt.Errorf("compiled family template %s scope mismatch", template.CatalogID)
		}
		if strings.TrimSpace(template.IntegrationArchetype) != strings.TrimSpace(entry.IntegrationArchetype) {
			return fmt.Errorf("compiled family template %s integration_archetype mismatch", template.CatalogID)
		}
		if strings.TrimSpace(template.TransportType) == "" || len(template.ScopeLevels) == 0 {
			return fmt.Errorf("compiled family template %s missing transport/scope metadata", template.CatalogID)
		}
		if strings.TrimSpace(template.ReviewStatusDefault) != "review_required" {
			return fmt.Errorf("compiled family template %s invalid review default %q", template.CatalogID, template.ReviewStatusDefault)
		}
		if strings.TrimSpace(template.ChildSource.TransportType) == "" || strings.TrimSpace(template.ChildSource.IntegrationArchetype) == "" || strings.TrimSpace(template.ChildSource.ParserID) == "" {
			return fmt.Errorf("compiled family template %s missing child-source shape", template.CatalogID)
		}
		if strings.Join(template.Tags, ",") != strings.Join(entry.Tags, ",") {
			return fmt.Errorf("compiled family template %s tags mismatch", template.CatalogID)
		}
		if strings.Join(template.GeneratorRelationships, ",") != strings.Join(entry.GeneratorRelationships, ",") {
			return fmt.Errorf("compiled family template %s generator relationship mismatch", template.CatalogID)
		}
	}
	return nil
}

func sourceCatalogChecksum(catalog sourceCatalogFile) string {
	var b strings.Builder
	b.WriteString(strconv.Itoa(catalog.SchemaVersion))
	b.WriteString("\n")
	b.WriteString(strings.TrimSpace(catalog.SourceMarkdownPath))
	b.WriteString("\n")
	b.WriteString(strings.TrimSpace(catalog.SourceMarkdownChecksum))
	for _, entry := range catalog.Entries {
		b.WriteString("\n--\n")
		b.WriteString(strings.TrimSpace(entry.CatalogID))
		b.WriteString("\n")
		b.WriteString(strings.TrimSpace(entry.CatalogKind))
		b.WriteString("\n")
		b.WriteString(strings.TrimSpace(entry.Name))
		b.WriteString("\n")
		b.WriteString(strings.TrimSpace(entry.Category))
		b.WriteString("\n")
		b.WriteString(strings.TrimSpace(entry.Scope))
		b.WriteString("\n")
		b.WriteString(strings.TrimSpace(entry.Produces))
		b.WriteString("\n")
		b.WriteString(strings.Join(entry.Tags, ","))
		b.WriteString("\n")
		b.WriteString(strings.TrimSpace(entry.AccessNotes))
		b.WriteString("\n")
		b.WriteString(strings.TrimSpace(entry.OfficialDocsURL))
		b.WriteString("\n")
		b.WriteString(strings.TrimSpace(entry.IntegrationArchetype))
		b.WriteString("\n")
		b.WriteString(strings.TrimSpace(entry.AuthConfig.EnvVar))
		b.WriteString("\n")
		b.WriteString(strings.TrimSpace(entry.AuthConfig.Placement))
		b.WriteString("\n")
		b.WriteString(strings.TrimSpace(entry.AuthConfig.Name))
		b.WriteString("\n")
		b.WriteString(strings.TrimSpace(entry.AuthConfig.Prefix))
		b.WriteString("\n")
		b.WriteString(strings.TrimSpace(entry.ParserID))
		b.WriteString("\n")
		b.WriteString(strings.TrimSpace(entry.DeferredReason))
		b.WriteString("\n")
		b.WriteString(strings.TrimSpace(entry.GeneratorKind))
		b.WriteString("\n")
		b.WriteString(strings.TrimSpace(entry.RuntimeSourceID))
		b.WriteString("\n")
		b.WriteString(strings.Join(entry.GeneratorRelationships, ","))
		b.WriteString("\n")
		b.WriteString(strings.Join(entry.ProbePatterns, ","))
		b.WriteString("\n")
		b.WriteString(strconv.Itoa(entry.SourceMarkdownLine))
		b.WriteString("\n")
		b.WriteString(strconv.FormatBool(entry.CredentialRequirement.RequiresRegistration))
		b.WriteString(",")
		b.WriteString(strconv.FormatBool(entry.CredentialRequirement.RequiresApproval))
		b.WriteString(",")
		b.WriteString(strconv.FormatBool(entry.CredentialRequirement.CommercialTerms))
		b.WriteString(",")
		b.WriteString(strconv.FormatBool(entry.CredentialRequirement.NoncommercialTerms))
		b.WriteString(",")
		b.WriteString(strconv.FormatBool(entry.CredentialRequirement.RestrictedAccess))
	}
	return sum([]byte(b.String()))
}

func validateCatalogEntryExecutionContract(entry sourceCatalogEntry) error {
	if entry.CatalogKind != "concrete" {
		return nil
	}
	archetype := strings.TrimSpace(entry.IntegrationArchetype)
	if !supportedIntegrationArchetype(archetype) {
		return fmt.Errorf("unsupported integration_archetype %q", archetype)
	}
	parserID := strings.TrimSpace(entry.ParserID)
	deferredReason := strings.TrimSpace(entry.DeferredReason)
	if concreteRequiresCredential(entry) {
		if strings.TrimSpace(entry.AuthConfig.EnvVar) == "" {
			return fmt.Errorf("credential-gated concrete source must declare auth_config_json.env_var")
		}
		if !validSourceEnvVarName(entry.AuthConfig.EnvVar) {
			return fmt.Errorf("credential-gated concrete source has invalid auth env var %q", entry.AuthConfig.EnvVar)
		}
	}
	if archetype == "deferred_transport" {
		if deferredReason == "" {
			return fmt.Errorf("deferred transport must declare deferred_reason")
		}
		if parserID != "" {
			return fmt.Errorf("deferred transport must not declare parser_id")
		}
		return nil
	}
	if parserID == "" {
		return fmt.Errorf("non-deferred concrete source must declare parser_id")
	}
	if !parserCompatibleWithArchetype(archetype, parserID) {
		return fmt.Errorf("parser_id %q is not compatible with integration_archetype %q", parserID, archetype)
	}
	return nil
}

func effectiveRuntimeSourceID(entry sourceCatalogEntry) string {
	if strings.TrimSpace(entry.CatalogKind) != "concrete" {
		return ""
	}
	if strings.TrimSpace(entry.IntegrationArchetype) == "deferred_transport" {
		return ""
	}
	return strings.TrimSpace(entry.RuntimeSourceID)
}

func bronzeManifestSourceID(entry sourceCatalogEntry) string {
	runtimeSourceID := effectiveRuntimeSourceID(entry)
	if runtimeSourceID != "" {
		return runtimeSourceID
	}
	if strings.TrimSpace(entry.CatalogKind) != "concrete" {
		return ""
	}
	if strings.TrimSpace(entry.IntegrationArchetype) == "deferred_transport" {
		return ""
	}
	catalogID := strings.TrimSpace(entry.CatalogID)
	const concretePrefix = "catalog:concrete:"
	if !strings.HasPrefix(catalogID, concretePrefix) {
		return ""
	}
	autoSuffix := strings.Trim(strings.ReplaceAll(strings.TrimPrefix(catalogID, concretePrefix), ":", "-"), "-")
	if autoSuffix == "" {
		return ""
	}
	return "catalog:auto:" + autoSuffix
}

func familyTemplateTransportType(entry sourceCatalogEntry) string {
	if archetype := strings.TrimSpace(familyTemplateChildArchetype(entry)); archetype != "" {
		switch archetype {
		case "bulk_file", "http_json", "http_csv", "http_xml", "rss_atom", "html_profile", "stac_api", "catalog_ckan", "catalog_socrata", "catalog_opendatasoft", "arcgis_rest", "ogc_features", "ogc_records", "discovery_web":
			return "http"
		}
	}
	return "http"
}

func familyTemplateScopeLevels(entry sourceCatalogEntry) []string {
	scope := strings.ToLower(strings.TrimSpace(entry.Scope))
	levels := make([]string, 0, 3)
	if strings.Contains(scope, "global") {
		levels = append(levels, "global")
	}
	if strings.Contains(scope, "national") {
		levels = append(levels, "admin0")
	}
	if strings.Contains(scope, "subnational") {
		levels = append(levels, "admin1", "admin2")
	}
	if len(levels) == 0 {
		levels = append(levels, "admin0")
	}
	return dedupeStrings(levels)
}

func familyChildSourceTemplate(entry sourceCatalogEntry) sourceFamilyChildSourceTemplate {
	archetype := familyTemplateChildArchetype(entry)
	formatHint, parserID := familyChildFormatAndParser(archetype)
	return sourceFamilyChildSourceTemplate{
		TransportType:        familyTemplateTransportType(entry),
		IntegrationArchetype: archetype,
		FormatHint:           formatHint,
		ParserID:             parserID,
		SourceClass:          "family_generated",
		RefreshStrategy:      "scheduled",
		CrawlStrategy:        "delta",
		ExpectedPlaceTypes:   familyTemplateScopeLevels(entry),
	}
}

func familyTemplateChildArchetype(entry sourceCatalogEntry) string {
	if archetype := strings.TrimSpace(entry.IntegrationArchetype); archetype != "" && archetype != "deferred_transport" {
		return archetype
	}
	name := strings.ToLower(strings.TrimSpace(entry.Name))
	tags := make(map[string]struct{}, len(entry.Tags))
	for _, tag := range entry.Tags {
		trimmed := strings.ToLower(strings.TrimSpace(tag))
		if trimmed != "" {
			tags[trimmed] = struct{}{}
		}
	}
	hasTag := func(tag string) bool {
		_, ok := tags[tag]
		return ok
	}
	switch {
	case hasTag("catalog"):
		return "catalog_ckan"
	case hasTag("geospatial") || hasTag("boundaries"):
		return "ogc_features"
	case hasTag("media") && (hasTag("feeds") || hasTag("local")):
		return "rss_atom"
	case hasTag("social") || hasTag("community"):
		return "discovery_web"
	case hasTag("official-stats") || hasTag("weather") || hasTag("hydrology") || hasTag("health") || hasTag("surveillance") || hasTag("transport") || hasTag("mobility") || hasTag("outages") || hasTag("utilities") || hasTag("safety") || hasTag("alerts") || hasTag("procurement") || hasTag("elections"):
		return "http_json"
	case strings.Contains(name, "feeds") || strings.Contains(name, "rss"):
		return "rss_atom"
	case strings.Contains(name, "portal") || strings.Contains(name, "portals") || strings.Contains(name, "registries") || strings.Contains(name, "gazette") || strings.Contains(name, "parliament") || strings.Contains(name, "court") || strings.Contains(name, "media") || strings.Contains(name, "repository"):
		return "html_profile"
	default:
		return "discovery_web"
	}
}

func familyChildFormatAndParser(archetype string) (string, string) {
	switch strings.TrimSpace(archetype) {
	case "http_json", "catalog_ckan", "catalog_socrata", "catalog_opendatasoft", "arcgis_rest", "ogc_features", "ogc_records", "stac_api":
		return "json", "parser:json"
	case "http_csv":
		return "csv", "parser:csv"
	case "http_xml":
		return "xml", "parser:xml"
	case "rss_atom":
		return "rss", "parser:rss"
	case "html_profile", "discovery_web", "bulk_file":
		return "html", "parser:html-profile"
	default:
		return "html", "parser:html-profile"
	}
}

func dedupeStrings(values []string) []string {
	seen := map[string]struct{}{}
	result := make([]string, 0, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		result = append(result, trimmed)
	}
	return result
}

func synthesizedRuntimeSeed(entry sourceCatalogEntry) (sourceSeed, error) {
	runtimeSourceID := effectiveRuntimeSourceID(entry)
	if runtimeSourceID == "" {
		return sourceSeed{}, fmt.Errorf("runtime source id is empty")
	}
	return synthesizedRuntimeSeedForSourceID(entry, runtimeSourceID)
}

func synthesizedRuntimeSeedForSourceID(entry sourceCatalogEntry, runtimeSourceID string) (sourceSeed, error) {
	entry.RuntimeSourceID = strings.TrimSpace(runtimeSourceID)
	if entry.RuntimeSourceID == "" {
		return sourceSeed{}, fmt.Errorf("runtime source id is empty")
	}
	domain, entrypoint := entrypointFromCatalog(entry)
	entrypoints := []string{entrypoint}
	authMode, authConfig := authConfigForCatalogEntry(entry)
	formatHint, err := inferFormatHint(entry)
	if err != nil {
		return sourceSeed{}, err
	}
	priority := 200
	if concreteRequiresCredential(entry) {
		priority = 220
	}
	requestsPerMinute := 1
	burstSize := 1
	refreshStrategy := "scheduled"
	crawlStrategy := "delta"
	lifecycleState := "approved_enabled"
	crawlEnabled := true
	promoteProfile := "promote:catalog"
	entityTypes := []string{"document"}
	expectedPlaceTypes := []string{"admin0"}
	supportsHistorical := true
	backfillPriority := 100
	reviewStatus := "approved"
	reviewNotes := "auto-generated runtime seed from source catalog"
	if override, ok := runtimeSourceOverrideForID(runtimeSourceID); ok {
		if len(override.Entrypoints) > 0 {
			entrypoints = cloneStrings(override.Entrypoints)
			if host, err := hostFromURL(entrypoints[0]); err == nil && host != "" {
				domain = host
			}
		}
		if strings.TrimSpace(override.AuthMode) != "" {
			authMode = strings.TrimSpace(override.AuthMode)
			authConfig = cloneAnyMap(override.AuthConfig)
		}
		if override.RequestsPerMinute > 0 {
			requestsPerMinute = override.RequestsPerMinute
		}
		if override.BurstSize > 0 {
			burstSize = override.BurstSize
		}
		if strings.TrimSpace(override.RefreshStrategy) != "" {
			refreshStrategy = strings.TrimSpace(override.RefreshStrategy)
		}
		if strings.TrimSpace(override.CrawlStrategy) != "" {
			crawlStrategy = strings.TrimSpace(override.CrawlStrategy)
		}
		if strings.TrimSpace(override.LifecycleState) != "" {
			lifecycleState = strings.TrimSpace(override.LifecycleState)
		}
		if override.CrawlEnabled != nil {
			crawlEnabled = *override.CrawlEnabled
		}
		if strings.TrimSpace(override.PromoteProfile) != "" {
			promoteProfile = strings.TrimSpace(override.PromoteProfile)
		}
		if len(override.EntityTypes) > 0 {
			entityTypes = cloneStrings(override.EntityTypes)
		}
		if len(override.ExpectedPlaceTypes) > 0 {
			expectedPlaceTypes = cloneStrings(override.ExpectedPlaceTypes)
		}
		if override.SupportsHistorical != nil {
			supportsHistorical = *override.SupportsHistorical
		}
		if override.BackfillPriority != nil {
			backfillPriority = *override.BackfillPriority
		}
		if strings.TrimSpace(override.ReviewStatus) != "" {
			reviewStatus = strings.TrimSpace(override.ReviewStatus)
		}
		if strings.TrimSpace(override.ReviewNotes) != "" {
			reviewNotes = strings.TrimSpace(override.ReviewNotes)
		}
	}
	allowedHosts, err := hostsFromEntrypoints(entrypoints)
	if err != nil {
		return sourceSeed{}, err
	}
	if len(allowedHosts) == 0 && domain != "" {
		allowedHosts = []string{domain}
	}
	return sourceSeed{
		SourceID:        runtimeSourceID,
		CatalogKind:     "concrete",
		LifecycleState:  lifecycleState,
		Domain:          domain,
		DomainFamily:    slugify(entry.Category),
		SourceClass:     "catalog_source",
		Entrypoints:     entrypoints,
		AuthMode:        authMode,
		AuthConfig:      authConfig,
		TransportType:   "http",
		CrawlEnabled:    crawlEnabled,
		AllowedHosts:    allowedHosts,
		FormatHint:      formatHint,
		RobotsPolicy:    "respect",
		RefreshStrategy: refreshStrategy,
		CrawlStrategy:   crawlStrategy,
		CrawlConfig: map[string]any{
			"catalog_archetype": strings.TrimSpace(entry.IntegrationArchetype),
		},
		RequestsPerMinute:   requestsPerMinute,
		BurstSize:           burstSize,
		RetentionClass:      "warm",
		License:             "public",
		TermsURL:            strings.TrimSpace(entry.OfficialDocsURL),
		AttributionRequired: true,
		GeoScope:            "global",
		Priority:            priority,
		ParserID:            strings.TrimSpace(entry.ParserID),
		ParseConfig:         map[string]any{},
		BronzeTable:         bronzeTableForSourceID(runtimeSourceID),
		BronzeSchemaVersion: 1,
		PromoteProfile:      promoteProfile,
		EntityTypes:         entityTypes,
		ExpectedPlaceTypes:  expectedPlaceTypes,
		SupportsHistorical:  supportsHistorical,
		SupportsDelta:       true,
		BackfillPriority:    backfillPriority,
		ReviewStatus:        reviewStatus,
		ReviewNotes:         reviewNotes,
		ConfidenceBaseline:  0.5,
	}, nil
}

func bronzeManifestRowForCatalogEntry(entry sourceCatalogEntry, registryByID map[string]sourceSeed) (sourceBronzeDDLManifest, bool, error) {
	manifestSourceID := bronzeManifestSourceID(entry)
	if manifestSourceID == "" {
		return sourceBronzeDDLManifest{}, false, nil
	}
	seed, ok := registryByID[manifestSourceID]
	if ok {
		if err := validateRuntimeSeedParity(entry, seed); err != nil {
			return sourceBronzeDDLManifest{}, false, err
		}
	} else {
		var err error
		seed, err = synthesizedRuntimeSeedForSourceID(entry, manifestSourceID)
		if err != nil {
			return sourceBronzeDDLManifest{}, false, err
		}
	}
	if strings.TrimSpace(seed.BronzeTable) == "" {
		return sourceBronzeDDLManifest{}, false, nil
	}
	return sourceBronzeDDLManifest{
		SourceID:            seed.SourceID,
		BronzeTable:         seed.BronzeTable,
		BronzeSchemaVersion: seed.BronzeSchemaVersion,
		PromoteProfile:      seed.PromoteProfile,
	}, true, nil
}

func authConfigForCatalogEntry(entry sourceCatalogEntry) (string, map[string]any) {
	envVar := strings.TrimSpace(entry.AuthConfig.EnvVar)
	if envVar == "" {
		return "none", map[string]any{}
	}
	placement := strings.TrimSpace(entry.AuthConfig.Placement)
	if placement == "" {
		placement = "header"
	}
	name := strings.TrimSpace(entry.AuthConfig.Name)
	if name == "" {
		name = "Authorization"
	}
	return "user_supplied_key", map[string]any{
		"env_var":   envVar,
		"placement": placement,
		"name":      name,
		"prefix":    strings.TrimSpace(entry.AuthConfig.Prefix),
	}
}

func entrypointFromCatalog(entry sourceCatalogEntry) (string, string) {
	fallbackHost := "example.invalid"
	return fallbackHost, "https://" + fallbackHost + "/" + slugify(entry.CatalogID)
}

func runtimeSourceOverrideForID(sourceID string) (runtimeSourceOverride, bool) {
	falseValue := false
	trueValue := true
	switch strings.TrimSpace(sourceID) {
	case "catalog:auto:aviation-airports-drones-and-mobility-opensky-network":
		return runtimeSourceOverride{
			Entrypoints: []string{"https://opensky-network.org/api/states/all?extended=1"},
			AuthMode:    "user_supplied_key",
			AuthConfig: map[string]any{
				"env_var":   "SOURCE_OPENSKY_NETWORK_API_KEY",
				"placement": "header",
				"name":      "Authorization",
				"prefix":    "Bearer ",
			},
			RequestsPerMinute:  1,
			BurstSize:          1,
			RefreshStrategy:    "scheduled",
			PromoteProfile:     "promote:aviation",
			EntityTypes:        []string{"aircraft"},
			ExpectedPlaceTypes: []string{"admin0", "admin1", "admin2", "waterbody"},
			SupportsHistorical: &falseValue,
			BackfillPriority:   intPtr(0),
		}, true
	case "catalog:auto:aviation-airports-drones-and-mobility-airplanes-live":
		return runtimeSourceOverride{
			Entrypoints:        adsbSupplementEntrypoints("https://api.airplanes.live"),
			AuthMode:           "none",
			AuthConfig:         map[string]any{},
			RequestsPerMinute:  60,
			BurstSize:          1,
			RefreshStrategy:    "scheduled",
			PromoteProfile:     "promote:aviation",
			EntityTypes:        []string{"aircraft"},
			ExpectedPlaceTypes: []string{"admin0", "admin1", "admin2", "waterbody"},
			SupportsHistorical: &falseValue,
			BackfillPriority:   intPtr(0),
		}, true
	case "catalog:auto:security-addendum-air-adsblol-api":
		return runtimeSourceOverride{
			Entrypoints:        adsbSupplementEntrypoints("https://api.adsb.lol"),
			AuthMode:           "none",
			AuthConfig:         map[string]any{},
			RequestsPerMinute:  60,
			BurstSize:          1,
			RefreshStrategy:    "scheduled",
			PromoteProfile:     "promote:aviation",
			EntityTypes:        []string{"aircraft"},
			ExpectedPlaceTypes: []string{"admin0", "admin1", "admin2", "waterbody"},
			SupportsHistorical: &falseValue,
			BackfillPriority:   intPtr(0),
		}, true
	case "catalog:auto:maritime-ocean-and-coastal-sources-aishub":
		return runtimeSourceOverride{
			Entrypoints: []string{"https://data.aishub.net/ws.php?format=1&output=json&compress=2&latmin=-90&latmax=90&lonmin=-180&lonmax=180&interval=5"},
			AuthMode:    "user_supplied_key",
			AuthConfig: map[string]any{
				"env_var":   "SOURCE_AISHUB_USERNAME",
				"placement": "query",
				"name":      "username",
				"prefix":    "",
			},
			RequestsPerMinute:  1,
			BurstSize:          1,
			RefreshStrategy:    "scheduled",
			PromoteProfile:     "promote:maritime",
			EntityTypes:        []string{"vessel"},
			ExpectedPlaceTypes: []string{"admin0", "admin1", "admin2", "waterbody"},
			SupportsHistorical: &falseValue,
			BackfillPriority:   intPtr(0),
		}, true
	case "catalog:auto:maritime-ocean-and-coastal-sources-vesselfinder":
		return runtimeSourceOverride{
			Entrypoints:        []string{"https://www.vesselfinder.com/vessels"},
			AuthMode:           "none",
			AuthConfig:         map[string]any{},
			RequestsPerMinute:  18,
			BurstSize:          3,
			RefreshStrategy:    "scheduled",
			CrawlStrategy:      "browser_rendered",
			PromoteProfile:     "promote:maritime",
			EntityTypes:        []string{"vessel"},
			ExpectedPlaceTypes: []string{"admin0", "admin1", "admin2", "waterbody"},
			SupportsHistorical: &falseValue,
			BackfillPriority:   intPtr(0),
			ReviewNotes:        "browser-rendered VesselFinder ingestion; live crawling is opt-in through the live-crawl Compose profile",
		}, true
	case "catalog:auto:maritime-ocean-and-coastal-sources-vesselfinder-routes":
		return runtimeSourceOverride{
			Entrypoints:        []string{"https://www.vesselfinder.com/api/pub/dm3/"},
			AuthMode:           "none",
			AuthConfig:         map[string]any{},
			RequestsPerMinute:  60,
			BurstSize:          8,
			RefreshStrategy:    "scheduled",
			CrawlStrategy:      "browser_rendered",
			PromoteProfile:     "promote:maritime",
			EntityTypes:        []string{"vessel"},
			ExpectedPlaceTypes: []string{"waterbody"},
			SupportsHistorical: &falseValue,
			BackfillPriority:   intPtr(0),
			ReviewNotes:        "browser-rendered VesselFinder route/waypoint API; worker-vesselfinder drives the queue, not the frontier",
		}, true
	case "catalog:auto:aviation-airports-drones-and-mobility-openaip-core-api":
		return runtimeSourceOverride{
			Entrypoints: []string{
				"https://api.core.openaip.net/api/airports",
				"https://api.core.openaip.net/api/airspaces",
				"https://api.core.openaip.net/api/navaids",
				"https://api.core.openaip.net/api/reporting-points",
			},
			AuthMode: "user_supplied_key",
			AuthConfig: map[string]any{
				"env_var":   "SOURCE_OPENAIP_CORE_API_KEY",
				"placement": "header",
				"name":      "x-openaip-api-key",
				"prefix":    "",
			},
			RequestsPerMinute:  10,
			BurstSize:          1,
			RefreshStrategy:    "scheduled",
			PromoteProfile:     "promote:aviation",
			EntityTypes:        []string{"airport", "airspace", "navaid", "reporting_point"},
			ExpectedPlaceTypes: []string{"admin0", "admin1", "admin2"},
			SupportsHistorical: &falseValue,
			BackfillPriority:   intPtr(0),
		}, true
	case "catalog:auto:aviation-airports-drones-and-mobility-aviationweather-api":
		return runtimeSourceOverride{
			Entrypoints:    []string{"https://aviationweather.gov/api/data/metar"},
			LifecycleState: "approved_disabled",
			CrawlEnabled:   &falseValue,
			ReviewStatus:   "review_required",
			ReviewNotes:    "deferred in urgent phase-1: provider contract not implemented",
		}, true
	case "catalog:auto:aviation-airports-drones-and-mobility-faa-nms-notam":
		return runtimeSourceOverride{
			Entrypoints:    []string{"https://nms.aim.faa.gov/"},
			LifecycleState: "approved_disabled",
			CrawlEnabled:   &falseValue,
			ReviewStatus:   "review_required",
			ReviewNotes:    "deferred in urgent phase-1: NOTAM transport contract not implemented",
		}, true
	case "catalog:auto:maritime-ocean-and-coastal-sources-marine-cadastre-u-s-ais":
		return runtimeSourceOverride{
			Entrypoints:    []string{"https://hub.marinecadastre.gov/datasets/marinecadastre::vessel-traffic-density/about"},
			LifecycleState: "approved_disabled",
			CrawlEnabled:   &falseValue,
			ReviewStatus:   "review_required",
			ReviewNotes:    "deferred in urgent phase-1: marine cadastre ingest contract not implemented",
		}, true
	case "catalog:auto:maritime-ocean-and-coastal-sources-noaa-co-ops-erddap", "catalog:auto:maritime-ocean-and-coastal-sources-noaa-co-ops-data-api", "catalog:auto:maritime-ocean-and-coastal-sources-noaa-co-ops-metadata-api":
		return runtimeSourceOverride{
			Entrypoints:    []string{"https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"},
			LifecycleState: "approved_disabled",
			CrawlEnabled:   &falseValue,
			ReviewStatus:   "review_required",
			ReviewNotes:    "deferred in urgent phase-1: NOAA CO-OPS contract not implemented",
		}, true
	case "catalog:auto:aviation-airports-drones-and-mobility-ads-b-exchange":
		return runtimeSourceOverride{
			Entrypoints:    []string{"https://api.adsbexchange.com/v2/lat/0/lon/0/dist/250"},
			LifecycleState: "approved_disabled",
			CrawlEnabled:   &falseValue,
			ReviewStatus:   "review_required",
			ReviewNotes:    "deferred in urgent phase-1: ADS-B Exchange key contract not implemented",
		}, true
	case "catalog:auto:maritime-ocean-and-coastal-sources-marinetraffic-apis":
		return runtimeSourceOverride{
			Entrypoints:    []string{"https://services.marinetraffic.com/api/exportvessel/v:8"},
			LifecycleState: "approved_disabled",
			CrawlEnabled:   &falseValue,
			ReviewStatus:   "review_required",
			ReviewNotes:    "deferred in urgent phase-1: MarineTraffic contract not implemented",
		}, true
	case "catalog:auto:maritime-ocean-and-coastal-sources-global-fishing-watch":
		return runtimeSourceOverride{
			Entrypoints:    []string{"https://gateway.api.globalfishingwatch.org/v3/events"},
			LifecycleState: "approved_disabled",
			CrawlEnabled:   &falseValue,
			ReviewStatus:   "review_required",
			ReviewNotes:    "deferred in urgent phase-1: Global Fishing Watch contract not implemented",
		}, true
	case "catalog:auto:maritime-ocean-and-coastal-sources-aisstream":
		aistreamEnabled := strings.TrimSpace(os.Getenv("SOURCE_AISSTREAM_API_KEY")) != ""
		aistreamLifecycle := "blocked_missing_credential"
		aistreamCrawl := &falseValue
		if aistreamEnabled {
			aistreamLifecycle = "approved_enabled"
			aistreamCrawl = &trueValue
		}
		return runtimeSourceOverride{
			Entrypoints: []string{"wss://stream.aisstream.io/v0/stream"},
			AuthMode:    "user_supplied_key",
			AuthConfig: map[string]any{
				"env_var":   "SOURCE_AISSTREAM_API_KEY",
				"placement": "websocket_subscribe_message",
				"name":      "APIKey",
			},
			CrawlStrategy:      "websocket_stream",
			LifecycleState:     aistreamLifecycle,
			CrawlEnabled:       aistreamCrawl,
			PromoteProfile:     "promote:maritime",
			EntityTypes:        []string{"vessel"},
			ExpectedPlaceTypes: []string{"admin0", "admin1", "admin2", "waterbody"},
			SupportsHistorical: &falseValue,
			BackfillPriority:   intPtr(0),
			ReviewStatus:       "approved",
			ReviewNotes:        "websocket AIS stream; worker-aisstream manages connection lifecycle",
		}, true
	case "catalog:auto:maritime-ocean-and-coastal-sources-equasis":
		return runtimeSourceOverride{
			Entrypoints:    []string{"https://www.equasis.org/"},
			LifecycleState: "approved_disabled",
			CrawlEnabled:   &falseValue,
			ReviewStatus:   "review_required",
			ReviewNotes:    "deferred in urgent phase-1: login-gated contract not implemented",
		}, true
	case "catalog:auto:maritime-ocean-and-coastal-sources-imo-gisis":
		return runtimeSourceOverride{
			Entrypoints:    []string{"https://gisis.imo.org/Public/Default.aspx"},
			LifecycleState: "approved_disabled",
			CrawlEnabled:   &falseValue,
			ReviewStatus:   "review_required",
			ReviewNotes:    "deferred in urgent phase-1: interactive contract not implemented",
		}, true
	default:
		return runtimeSourceOverride{}, false
	}
}

func adsbSupplementEntrypoints(baseURL string) []string {
	base := strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if base == "" {
		return nil
	}
	return []string{
		base + "/v2/mil",
		base + "/v2/ladd",
		base + "/v2/pia",
		base + "/v2/point/40.7128/-74.0060/250",
		base + "/v2/point/34.0522/-118.2437/250",
		base + "/v2/point/51.5072/-0.1276/250",
		base + "/v2/point/50.1109/8.6821/250",
		base + "/v2/point/25.2048/55.2708/250",
		base + "/v2/point/1.3521/103.8198/250",
	}
}

func hostsFromEntrypoints(entrypoints []string) ([]string, error) {
	hostSet := map[string]struct{}{}
	hosts := make([]string, 0, len(entrypoints))
	for _, raw := range entrypoints {
		host, err := hostFromURL(raw)
		if err != nil {
			return nil, err
		}
		if host == "" {
			continue
		}
		if _, ok := hostSet[host]; ok {
			continue
		}
		hostSet[host] = struct{}{}
		hosts = append(hosts, host)
	}
	return hosts, nil
}

func hostFromURL(rawURL string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return "", fmt.Errorf("parse runtime entrypoint %q: %w", rawURL, err)
	}
	return strings.ToLower(strings.TrimSpace(parsed.Hostname())), nil
}

func cloneAnyMap(input map[string]any) map[string]any {
	if len(input) == 0 {
		return map[string]any{}
	}
	out := make(map[string]any, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}

func inferFormatHint(entry sourceCatalogEntry) (string, error) {
	switch strings.TrimSpace(entry.ParserID) {
	case "parser:json":
		return "json", nil
	case "parser:csv":
		return "csv", nil
	case "parser:xml":
		return "xml", nil
	case "parser:rss":
		return "rss", nil
	case "parser:atom":
		return "atom", nil
	case "parser:html-profile":
		return "html", nil
	case "parser:vesselfinder-html":
		return "vesselfinder-html", nil
	case "parser:vesselfinder-route-json":
		return "vesselfinder-route-json", nil
	case "parser:aisstream-json":
		return "aisstream-json", nil
	default:
		return "", fmt.Errorf("unsupported parser id %q for synthesized runtime seed", entry.ParserID)
	}
}

func bronzeTableForSourceID(sourceID string) string {
	slug := slugify(sourceID)
	if slug == "" {
		slug = "source"
	}
	if len(slug) > 40 {
		slug = slug[:40]
	}
	sum := sha256.Sum256([]byte(strings.TrimSpace(sourceID)))
	hash8 := hex.EncodeToString(sum[:])[:8]
	return "bronze.src_" + slug + "_" + hash8 + "_v1"
}

func slugify(input string) string {
	input = strings.ToLower(strings.TrimSpace(input))
	if input == "" {
		return ""
	}
	var b strings.Builder
	b.Grow(len(input))
	lastDash := false
	for _, r := range input {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			lastDash = false
			continue
		}
		if !lastDash {
			b.WriteByte('-')
			lastDash = true
		}
	}
	out := strings.Trim(b.String(), "-")
	return out
}

func validSourceEnvVarName(envVar string) bool {
	envVar = strings.TrimSpace(envVar)
	if envVar == "ACLED_API_KEY" {
		return true
	}
	if !strings.HasPrefix(envVar, "SOURCE_") {
		return false
	}
	if !(strings.HasSuffix(envVar, "_API_KEY") || strings.HasSuffix(envVar, "_TOKEN")) {
		return false
	}
	for _, r := range envVar {
		if (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
			continue
		}
		return false
	}
	return true
}

func concreteRequiresCredential(entry sourceCatalogEntry) bool {
	if entry.CatalogKind != "concrete" {
		return false
	}
	if strings.TrimSpace(entry.AuthConfig.EnvVar) != "" {
		return true
	}
	req := entry.CredentialRequirement
	return req.RequiresRegistration || req.RequiresApproval || req.CommercialTerms || req.NoncommercialTerms || req.RestrictedAccess
}

func supportedIntegrationArchetype(archetype string) bool {
	return parser.SupportedCatalogArchetype(archetype)
}

func parserCompatibleWithArchetype(archetype, parserID string) bool {
	return parser.ArchetypeParserCompatible(archetype, parserID)
}

func validateRuntimeSeedParity(entry sourceCatalogEntry, seed sourceSeed) error {
	wantParserID := strings.TrimSpace(entry.ParserID)
	gotParserID := strings.TrimSpace(seed.ParserID)
	if wantParserID != gotParserID {
		return fmt.Errorf("parser mismatch: catalog=%q seed=%q", wantParserID, gotParserID)
	}
	wantArchetype := strings.TrimSpace(entry.IntegrationArchetype)
	gotArchetype, err := integrationArchetypeForSeed(seed)
	if err != nil {
		return err
	}
	if wantArchetype != gotArchetype {
		return fmt.Errorf("integration_archetype mismatch: catalog=%q seed=%q", wantArchetype, gotArchetype)
	}
	return nil
}

func integrationArchetypeForSeed(seed sourceSeed) (string, error) {
	if inferred := strings.TrimSpace(stringValue(seed.CrawlConfig["catalog_archetype"])); inferred != "" {
		if supportedIntegrationArchetype(inferred) {
			return inferred, nil
		}
	}
	transport := strings.TrimSpace(seed.TransportType)
	formatHint := strings.ToLower(strings.TrimSpace(seed.FormatHint))
	parserID := strings.TrimSpace(seed.ParserID)
	switch transport {
	case "http":
		if parserID == "parser:rss" || parserID == "parser:atom" {
			return "rss_atom", nil
		}
		switch formatHint {
		case "json":
			return "http_json", nil
		case "csv", "tsv":
			return "http_csv", nil
		case "xml":
			return "http_xml", nil
		case "rss", "atom":
			return "rss_atom", nil
		case "html":
			return "html_profile", nil
		default:
			if parserID == "parser:html-profile" {
				return "html_profile", nil
			}
			return "", fmt.Errorf("unable to infer runtime archetype for seed %s transport=%q format=%q parser=%q", seed.SourceID, transport, formatHint, parserID)
		}
	case "bundle_alias":
		return "deferred_transport", nil
	case "websocket":
		return "websocket_stream", nil
	default:
		return "", fmt.Errorf("unsupported runtime transport %q for seed %s", transport, seed.SourceID)
	}
}
