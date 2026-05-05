package sourcecatalog

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type File struct {
	SchemaVersion          int     `json:"schema_version"`
	SourceMarkdownPath     string  `json:"source_markdown_path"`
	SourceMarkdownChecksum string  `json:"source_markdown_checksum"`
	Entries                []Entry `json:"entries"`
}

type Entry struct {
	CatalogID              string                `json:"catalog_id"`
	CatalogKind            string                `json:"catalog_kind"`
	Name                   string                `json:"name"`
	Category               string                `json:"category"`
	Scope                  string                `json:"scope"`
	Produces               string                `json:"produces"`
	Tags                   []string              `json:"tags"`
	AccessNotes            string                `json:"access_notes"`
	OfficialDocsURL        string                `json:"official_docs_url"`
	IntegrationArchetype   string                `json:"integration_archetype"`
	AuthConfig             AuthConfig            `json:"auth_config_json,omitempty"`
	ParserID               string                `json:"parser_id,omitempty"`
	DeferredReason         string                `json:"deferred_reason,omitempty"`
	GeneratorKind          string                `json:"generator_kind"`
	RuntimeSourceID        string                `json:"runtime_source_id"`
	CredentialRequirement  CredentialRequirement `json:"credential_requirement"`
	GeneratorRelationships []string              `json:"generator_relationships"`
	ProbePatterns          []string              `json:"probe_patterns,omitempty"`
	SourceMarkdownLine     int                   `json:"source_markdown_line"`
}

type CredentialRequirement struct {
	RequiresRegistration bool `json:"requires_registration"`
	RequiresApproval     bool `json:"requires_approval"`
	CommercialTerms      bool `json:"commercial_terms"`
	NoncommercialTerms   bool `json:"noncommercial_terms"`
	RestrictedAccess     bool `json:"restricted_access"`
}

type AuthConfig struct {
	EnvVar    string `json:"env_var,omitempty"`
	Placement string `json:"placement,omitempty"`
	Name      string `json:"name,omitempty"`
	Prefix    string `json:"prefix,omitempty"`
}

type FingerprintProbe struct {
	CatalogID            string   `json:"catalog_id"`
	Name                 string   `json:"name"`
	IntegrationArchetype string   `json:"integration_archetype"`
	ProbePatterns        []string `json:"probe_patterns"`
}

type FamilyTemplate struct {
	CatalogID              string   `json:"catalog_id"`
	Name                   string   `json:"name"`
	Scope                  string   `json:"scope"`
	Outputs                string   `json:"outputs"`
	IntegrationArchetype   string   `json:"integration_archetype"`
	TransportType          string   `json:"transport_type"`
	ScopeLevels            []string `json:"scope_levels"`
	ReviewStatusDefault    string   `json:"review_status_default"`
	GeneratorRelationships []string `json:"generator_relationships"`
	Tags                   []string `json:"tags"`
	ChildSource            FamilyChildSourceTemplate `json:"child_source"`
}

type FamilyChildSourceTemplate struct {
	TransportType        string   `json:"transport_type"`
	IntegrationArchetype string   `json:"integration_archetype"`
	FormatHint           string   `json:"format_hint"`
	ParserID             string   `json:"parser_id"`
	SourceClass          string   `json:"source_class"`
	RefreshStrategy      string   `json:"refresh_strategy"`
	CrawlStrategy        string   `json:"crawl_strategy"`
	ExpectedPlaceTypes   []string `json:"expected_place_types"`
}

type BronzeDDLManifest struct {
	SourceID            string `json:"source_id"`
	BronzeTable         string `json:"bronze_table"`
	BronzeSchemaVersion int    `json:"bronze_schema_version"`
	PromoteProfile      string `json:"promote_profile"`
}

type RuntimeSeed struct {
	SourceID            string          `json:"source_id"`
	CatalogKind         string          `json:"catalog_kind"`
	LifecycleState      string          `json:"lifecycle_state"`
	FormatHint          string          `json:"format_hint"`
	ParserID            string          `json:"parser_id"`
	TransportType       string          `json:"transport_type"`
	ReviewStatus        string          `json:"review_status"`
	CrawlEnabled        bool            `json:"crawl_enabled"`
	BronzeSchemaVersion int             `json:"bronze_schema_version"`
	BronzeTable         string          `json:"bronze_table"`
	PromoteProfile      string          `json:"promote_profile"`
	OfficialDocsURL     string          `json:"official_docs_url,omitempty"`
	Raw                 json.RawMessage `json:"-"`
}

type Compiled struct {
	Catalog           File                `json:"catalog"`
	CatalogChecksum   string              `json:"catalog_checksum"`
	RunnableSeeds     []RuntimeSeed       `json:"runnable_seeds"`
	FingerprintProbes []FingerprintProbe  `json:"fingerprint_probes"`
	FamilyTemplates   []FamilyTemplate    `json:"family_templates"`
	BronzeDDLManifest []BronzeDDLManifest `json:"bronze_ddl_manifest"`
}

func LoadCompiled(path string) (Compiled, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Compiled{}, err
	}
	var compiled Compiled
	if err := json.Unmarshal(b, &compiled); err != nil {
		return Compiled{}, err
	}
	hydrateCompiledFamilyTemplates(&compiled)
	if err := ValidateCompiled(path, compiled); err != nil {
		return Compiled{}, err
	}
	return compiled, nil
}

func hydrateCompiledFamilyTemplates(compiled *Compiled) {
	if compiled == nil {
		return
	}
	entries := map[string]Entry{}
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

func ValidateCompiled(path string, compiled Compiled) error {
	if compiled.CatalogChecksum != Checksum(compiled.Catalog) {
		return fmt.Errorf("compiled source catalog checksum mismatch")
	}
	markdownPath := compiled.Catalog.SourceMarkdownPath
	if !filepath.IsAbs(markdownPath) {
		markdownPath = filepath.Clean(filepath.Join(filepath.Dir(path), "..", markdownPath))
	}
	b, err := os.ReadFile(markdownPath)
	if err != nil {
		return fmt.Errorf("compiled source catalog markdown verification: %w", err)
	}
	if compiled.Catalog.SourceMarkdownChecksum != hashBytes(b) {
		return fmt.Errorf("compiled source catalog markdown checksum mismatch")
	}
	familyEntries := map[string]Entry{}
	for _, entry := range compiled.Catalog.Entries {
		if entry.CatalogKind == "family" {
			familyEntries[strings.TrimSpace(entry.CatalogID)] = entry
		}
	}
	if len(compiled.FamilyTemplates) != len(familyEntries) {
		return fmt.Errorf("compiled source catalog family template count mismatch: got %d want %d", len(compiled.FamilyTemplates), len(familyEntries))
	}
	for _, template := range compiled.FamilyTemplates {
		entry, ok := familyEntries[strings.TrimSpace(template.CatalogID)]
		if !ok {
			return fmt.Errorf("compiled family template %s does not map to a family catalog entry", template.CatalogID)
		}
		if strings.TrimSpace(template.Name) != strings.TrimSpace(entry.Name) || strings.TrimSpace(template.Outputs) != strings.TrimSpace(entry.Produces) {
			return fmt.Errorf("compiled family template %s name/outputs mismatch", template.CatalogID)
		}
		if strings.TrimSpace(template.Scope) != strings.TrimSpace(entry.Scope) || strings.TrimSpace(template.IntegrationArchetype) != strings.TrimSpace(entry.IntegrationArchetype) {
			return fmt.Errorf("compiled family template %s scope/archetype mismatch", template.CatalogID)
		}
		if strings.TrimSpace(template.TransportType) == "" {
			return fmt.Errorf("compiled family template %s missing transport_type", template.CatalogID)
		}
		if len(template.ScopeLevels) == 0 {
			return fmt.Errorf("compiled family template %s missing scope_levels", template.CatalogID)
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

func Checksum(catalog File) string {
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
	return hashBytes([]byte(b.String()))
}

func hashBytes(b []byte) string {
	d := sha256.Sum256(b)
	return fmt.Sprintf("%x", d[:])
}

func familyTemplateTransportType(entry Entry) string {
	if archetype := strings.TrimSpace(familyTemplateChildArchetype(entry)); archetype != "" {
		switch archetype {
		case "bulk_file", "http_json", "http_csv", "http_xml", "rss_atom", "html_profile", "stac_api", "catalog_ckan", "catalog_socrata", "catalog_opendatasoft", "arcgis_rest", "ogc_features", "ogc_records", "discovery_web":
			return "http"
		}
	}
	return "http"
}

func familyTemplateScopeLevels(entry Entry) []string {
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

func familyChildSourceTemplate(entry Entry) FamilyChildSourceTemplate {
	archetype := familyTemplateChildArchetype(entry)
	formatHint, parserID := familyChildFormatAndParser(archetype)
	return FamilyChildSourceTemplate{
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

func familyTemplateChildArchetype(entry Entry) string {
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
