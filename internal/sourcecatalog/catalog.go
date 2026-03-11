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
	ReviewStatusDefault    string   `json:"review_status_default"`
	GeneratorRelationships []string `json:"generator_relationships"`
	Tags                   []string `json:"tags"`
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
	if err := ValidateCompiled(path, compiled); err != nil {
		return Compiled{}, err
	}
	return compiled, nil
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
