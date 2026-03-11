package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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
	CatalogID              string   `json:"catalog_id"`
	Name                   string   `json:"name"`
	Scope                  string   `json:"scope"`
	Outputs                string   `json:"outputs"`
	IntegrationArchetype   string   `json:"integration_archetype"`
	ReviewStatusDefault    string   `json:"review_status_default"`
	GeneratorRelationships []string `json:"generator_relationships"`
	Tags                   []string `json:"tags"`
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

	for _, entry := range catalog.Entries {
		switch entry.CatalogKind {
		case "concrete":
			if strings.TrimSpace(entry.IntegrationArchetype) == "deferred_transport" && strings.TrimSpace(entry.RuntimeSourceID) != "" {
				return compiledSourceCatalog{}, fmt.Errorf("catalog entry %s must not map deferred_transport into runtime_source_id %q", entry.CatalogID, entry.RuntimeSourceID)
			}
			if entry.RuntimeSourceID != "" {
				seed, ok := registryByID[entry.RuntimeSourceID]
				if !ok {
					return compiledSourceCatalog{}, fmt.Errorf("catalog entry %s references unknown runtime_source_id %q", entry.CatalogID, entry.RuntimeSourceID)
				}
				if err := validateRuntimeSeedParity(entry, seed); err != nil {
					return compiledSourceCatalog{}, fmt.Errorf("catalog entry %s runtime parity: %w", entry.CatalogID, err)
				}
				compiled.RunnableSeeds = append(compiled.RunnableSeeds, seed)
				if strings.TrimSpace(seed.BronzeTable) != "" {
					compiled.BronzeDDLManifest = append(compiled.BronzeDDLManifest, sourceBronzeDDLManifest{
						SourceID:            seed.SourceID,
						BronzeTable:         seed.BronzeTable,
						BronzeSchemaVersion: seed.BronzeSchemaVersion,
						PromoteProfile:      seed.PromoteProfile,
					})
				}
			}
		case "fingerprint":
			compiled.FingerprintProbes = append(compiled.FingerprintProbes, sourceFingerprintProbe{
				CatalogID:            entry.CatalogID,
				Name:                 entry.Name,
				IntegrationArchetype: entry.IntegrationArchetype,
				ProbePatterns:        cloneStrings(entry.ProbePatterns),
			})
		case "family":
			compiled.FamilyTemplates = append(compiled.FamilyTemplates, sourceFamilyTemplate{
				CatalogID:              entry.CatalogID,
				Name:                   entry.Name,
				Scope:                  entry.Scope,
				Outputs:                entry.Produces,
				IntegrationArchetype:   entry.IntegrationArchetype,
				ReviewStatusDefault:    "review_required",
				GeneratorRelationships: cloneStrings(entry.GeneratorRelationships),
				Tags:                   cloneStrings(entry.Tags),
			})
		}
	}

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
	if err := validateCompiledSourceCatalog(path, compiled); err != nil {
		return compiledSourceCatalog{}, err
	}
	return compiled, nil
}

func loadRunnableSourceSeeds(path string) ([]sourceSeed, error) {
	compiled, err := loadCompiledSourceCatalog(path)
	if err == nil {
		return compiled.RunnableSeeds, nil
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
		runtimeSourceID := strings.TrimSpace(entry.RuntimeSourceID)
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
	for _, manifest := range compiled.BronzeDDLManifest {
		manifestBySourceID[strings.TrimSpace(manifest.SourceID)] = manifest
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
		if strings.TrimSpace(template.ReviewStatusDefault) != "review_required" {
			return fmt.Errorf("compiled family template %s invalid review default %q", template.CatalogID, template.ReviewStatusDefault)
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
	runtimeLinked := strings.TrimSpace(entry.RuntimeSourceID) != ""
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
	if !runtimeLinked {
		if deferredReason == "" {
			return fmt.Errorf("non-runtime concrete source must declare deferred_reason")
		}
		if parserID == "" {
			return fmt.Errorf("non-runtime concrete source must still declare parser_id")
		}
		if !parserCompatibleWithArchetype(archetype, parserID) {
			return fmt.Errorf("parser_id %q is not compatible with integration_archetype %q", parserID, archetype)
		}
		return nil
	}
	if parserID == "" {
		return fmt.Errorf("non-deferred concrete source must declare parser_id")
	}
	if deferredReason != "" {
		return fmt.Errorf("non-deferred concrete source must not declare deferred_reason")
	}
	if !parserCompatibleWithArchetype(archetype, parserID) {
		return fmt.Errorf("parser_id %q is not compatible with integration_archetype %q", parserID, archetype)
	}
	return nil
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
	return entry.CredentialRequirement.RestrictedAccess
}

func supportedIntegrationArchetype(archetype string) bool {
	switch strings.TrimSpace(archetype) {
	case "http_json", "http_csv", "http_xml", "rss_atom", "html_profile", "bulk_file", "stac_api", "catalog_ckan", "catalog_socrata", "catalog_opendatasoft", "arcgis_rest", "ogc_features", "ogc_records", "discovery_web", "deferred_transport":
		return true
	default:
		return false
	}
}

func parserCompatibleWithArchetype(archetype, parserID string) bool {
	parserID = strings.TrimSpace(parserID)
	switch strings.TrimSpace(archetype) {
	case "http_json", "stac_api", "catalog_ckan", "catalog_socrata", "catalog_opendatasoft", "arcgis_rest", "ogc_features":
		return parserID == "parser:json"
	case "http_csv":
		return parserID == "parser:csv"
	case "http_xml", "ogc_records":
		return parserID == "parser:xml"
	case "rss_atom":
		return parserID == "parser:rss" || parserID == "parser:atom"
	case "html_profile", "discovery_web":
		return parserID == "parser:html-profile"
	case "bulk_file":
		return parserID == "parser:csv" || parserID == "parser:json" || parserID == "parser:xml"
	default:
		return false
	}
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
	if strings.TrimSpace(entry.DeferredReason) != "" {
		return fmt.Errorf("runtime-linked catalog entry must not declare deferred_reason")
	}
	return nil
}

func integrationArchetypeForSeed(seed sourceSeed) (string, error) {
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
	default:
		return "", fmt.Errorf("unsupported runtime transport %q for seed %s", transport, seed.SourceID)
	}
}
