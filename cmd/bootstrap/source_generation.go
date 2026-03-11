package main

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"
)

const (
	sourceCatalogSchemaVersion         = 1
	sourceCatalogAPIContractVersion    = 1
	generatedChildReviewRequiredStatus = "review_required"
	generatedChildApprovedReviewStatus = "approved"
)

type sourceCatalogGovernanceRecord struct {
	CatalogID            string
	ReviewStatus         string
	MaterializedSourceID *string
	RecordVersion        uint64
	SchemaVersion        uint32
	APIContractVersion   uint32
	Attrs                string
	Evidence             string
}

type sourceFamilyTemplateRecord struct {
	TemplateID         string
	ReviewStatus       string
	RecordVersion      uint64
	SchemaVersion      uint32
	APIContractVersion uint32
	Attrs              string
	Evidence           string
}

type discoveryProbeGovernanceRecord struct {
	ProbeID            string
	ReviewStatus       string
	RecordVersion      uint64
	SchemaVersion      uint32
	APIContractVersion uint32
	Attrs              string
	Evidence           string
}

type discoveryCandidateRecord struct {
	CandidateID          string
	CatalogID            string
	CandidateName        string
	CandidateURL         string
	IntegrationArchetype string
	DetectedPlatform     string
	ReviewStatus         string
	MaterializedSourceID string
}

func loadSourceCatalogGovernance(ctx context.Context, runner sourceRegistryStore, path string) error {
	compiledCandidate, err := looksLikeCompiledSourceCatalog(path)
	if err != nil {
		return err
	}
	if !compiledCandidate {
		return nil
	}

	compiled, err := loadCompiledSourceCatalog(path)
	if err != nil {
		return err
	}
	existingCatalog, err := latestSourceCatalogRecords(ctx, runner)
	if err != nil {
		return err
	}
	existingFamilyTemplates, err := latestSourceFamilyTemplateRecords(ctx, runner)
	if err != nil {
		return err
	}
	existingDiscoveryProbes, err := latestDiscoveryProbeRecords(ctx, runner)
	if err != nil {
		return err
	}

	now := time.Now().UTC()
	insertedCatalog := false
	for _, entry := range compiled.Catalog.Entries {
		sql, ok := buildSourceCatalogEntrySQL(entry, compiled.Catalog, existingCatalog, now)
		if !ok {
			continue
		}
		if err := runner.ApplySQL(ctx, sql); err != nil {
			return fmt.Errorf("insert source catalog entry %s: %w", entry.CatalogID, err)
		}
		insertedCatalog = true
	}
	insertedFamilies := false
	for _, template := range compiled.FamilyTemplates {
		sql, ok := buildSourceFamilyTemplateSQL(template, existingFamilyTemplates, now)
		if !ok {
			continue
		}
		if err := runner.ApplySQL(ctx, sql); err != nil {
			return fmt.Errorf("insert family template %s: %w", template.CatalogID, err)
		}
		insertedFamilies = true
	}
	insertedProbes := false
	for _, probe := range compiled.FingerprintProbes {
		sql, ok := buildDiscoveryProbeSQL(probe, existingDiscoveryProbes, now)
		if !ok {
			continue
		}
		if err := runner.ApplySQL(ctx, sql); err != nil {
			return fmt.Errorf("insert discovery probe %s: %w", probe.CatalogID, err)
		}
		insertedProbes = true
	}
	if insertedCatalog {
		if err := runner.ApplySQL(ctx, "OPTIMIZE TABLE meta.source_catalog FINAL"); err != nil {
			return fmt.Errorf("optimize source catalog: %w", err)
		}
	}
	if insertedFamilies {
		if err := runner.ApplySQL(ctx, "OPTIMIZE TABLE meta.source_family_template FINAL"); err != nil {
			return fmt.Errorf("optimize source family template: %w", err)
		}
	}
	if insertedProbes {
		if err := runner.ApplySQL(ctx, "OPTIMIZE TABLE meta.discovery_probe FINAL"); err != nil {
			return fmt.Errorf("optimize discovery probe: %w", err)
		}
	}
	return nil
}

func materializeGeneratedChildSource(candidate discoveryCandidateRecord, seed sourceSeed) (sourceSeed, error) {
	if strings.TrimSpace(candidate.ReviewStatus) != generatedChildApprovedReviewStatus {
		return sourceSeed{}, fmt.Errorf("candidate %s must be approved before materialization", candidate.CandidateID)
	}
	if strings.TrimSpace(candidate.MaterializedSourceID) == "" {
		return sourceSeed{}, fmt.Errorf("candidate %s missing materialized_source_id", candidate.CandidateID)
	}
	seed.SourceID = strings.TrimSpace(candidate.MaterializedSourceID)
	seed.CatalogKind = defaultCatalogKind
	seed.LifecycleState = "approved_disabled"
	seed.ReviewStatus = generatedChildReviewRequiredStatus
	seed.ReviewNotes = fmt.Sprintf("generated from catalog candidate %s", candidate.CandidateID)
	return seed, nil
}

func buildSourceCatalogEntrySQL(entry sourceCatalogEntry, catalog sourceCatalogFile, existing map[string]sourceCatalogGovernanceRecord, now time.Time) (string, bool) {
	checksum := governanceChecksum(entry, catalog.SourceMarkdownChecksum)
	current, ok := existing[entry.CatalogID]
	attrs := mergeSourceCatalogAttrs(current.Attrs, checksum, catalog.SourceMarkdownChecksum, entry)
	if ok && governanceChecksumFromAttrs(current.Attrs) == checksum && sourceCatalogAttrsEquivalent(current.Attrs, attrs) {
		return "", false
	}
	reviewStatus := generatedChildReviewRequiredStatus
	if entry.CatalogKind == "concrete" {
		reviewStatus = generatedChildApprovedReviewStatus
	}
	materializedSourceID := stringPtr(entry.RuntimeSourceID)
	evidence := "[]"
	recordVersion := uint64(1)
	schemaVersion := uint32(sourceCatalogSchemaVersion)
	apiContractVersion := uint32(sourceCatalogAPIContractVersion)
	if ok {
		reviewStatus = fallbackString(current.ReviewStatus, reviewStatus)
		materializedSourceID = current.MaterializedSourceID
		evidence = fallbackString(current.Evidence, evidence)
		recordVersion = current.RecordVersion + 1
		schemaVersion = maxUint32(current.SchemaVersion, sourceCatalogSchemaVersion)
		apiContractVersion = maxUint32(current.APIContractVersion, sourceCatalogAPIContractVersion)
	}
	return fmt.Sprintf(`INSERT INTO meta.source_catalog
	(catalog_id, catalog_kind, name, category, scope, produces, tags, access_notes, official_docs_url, integration_archetype, generator_kind, runtime_source_id, generator_relationships, source_markdown_line, source_markdown_path, source_markdown_checksum, review_status, materialized_source_id, schema_version, record_version, api_contract_version, updated_at, attrs, evidence)
	VALUES ('%s','%s','%s','%s','%s','%s',%s,'%s','%s','%s','%s',%s,%s,%d,'%s','%s','%s',%s,%d,%d,%d,'%s','%s','%s')`,
		esc(entry.CatalogID),
		esc(entry.CatalogKind),
		esc(entry.Name),
		esc(entry.Category),
		esc(entry.Scope),
		esc(entry.Produces),
		arr(entry.Tags),
		esc(entry.AccessNotes),
		esc(entry.OfficialDocsURL),
		esc(entry.IntegrationArchetype),
		esc(entry.GeneratorKind),
		sqlNullableString(stringPtr(entry.RuntimeSourceID)),
		arr(entry.GeneratorRelationships),
		entry.SourceMarkdownLine,
		esc(catalog.SourceMarkdownPath),
		esc(catalog.SourceMarkdownChecksum),
		esc(reviewStatus),
		sqlNullableString(materializedSourceID),
		schemaVersion,
		recordVersion,
		apiContractVersion,
		esc(formatClickHouseTime(now)),
		esc(attrs),
		esc(evidence),
	), true
}

func buildSourceFamilyTemplateSQL(template sourceFamilyTemplate, existing map[string]sourceFamilyTemplateRecord, now time.Time) (string, bool) {
	checksum := governanceChecksum(template, "")
	current, ok := existing[template.CatalogID]
	if ok && governanceChecksumFromAttrs(current.Attrs) == checksum {
		return "", false
	}
	reviewStatus := generatedChildReviewRequiredStatus
	evidence := "[]"
	recordVersion := uint64(1)
	schemaVersion := uint32(sourceCatalogSchemaVersion)
	apiContractVersion := uint32(sourceCatalogAPIContractVersion)
	if ok {
		reviewStatus = fallbackString(current.ReviewStatus, reviewStatus)
		evidence = fallbackString(current.Evidence, evidence)
		recordVersion = current.RecordVersion + 1
		schemaVersion = maxUint32(current.SchemaVersion, sourceCatalogSchemaVersion)
		apiContractVersion = maxUint32(current.APIContractVersion, sourceCatalogAPIContractVersion)
	}
	return fmt.Sprintf(`INSERT INTO meta.source_family_template
	(template_id, catalog_id, family_name, scope, outputs, integration_archetype, review_status_default, generator_relationships, tags, review_status, schema_version, record_version, api_contract_version, updated_at, attrs, evidence)
	VALUES ('%s','%s','%s','%s','%s','%s','%s',%s,%s,'%s',%d,%d,%d,'%s','%s','%s')`,
		esc(template.CatalogID),
		esc(template.CatalogID),
		esc(template.Name),
		esc(template.Scope),
		esc(template.Outputs),
		esc(template.IntegrationArchetype),
		esc(template.ReviewStatusDefault),
		arr(template.GeneratorRelationships),
		arr(template.Tags),
		esc(reviewStatus),
		schemaVersion,
		recordVersion,
		apiContractVersion,
		esc(formatClickHouseTime(now)),
		esc(mergeGovernanceAttrs(current.Attrs, checksum, "")),
		esc(evidence),
	), true
}

func buildDiscoveryProbeSQL(probe sourceFingerprintProbe, existing map[string]discoveryProbeGovernanceRecord, now time.Time) (string, bool) {
	checksum := governanceChecksum(probe, "")
	current, ok := existing[probe.CatalogID]
	if ok && governanceChecksumFromAttrs(current.Attrs) == checksum {
		return "", false
	}
	reviewStatus := generatedChildReviewRequiredStatus
	evidence := "[]"
	recordVersion := uint64(1)
	schemaVersion := uint32(sourceCatalogSchemaVersion)
	apiContractVersion := uint32(sourceCatalogAPIContractVersion)
	if ok {
		reviewStatus = fallbackString(current.ReviewStatus, reviewStatus)
		evidence = fallbackString(current.Evidence, evidence)
		recordVersion = current.RecordVersion + 1
		schemaVersion = maxUint32(current.SchemaVersion, sourceCatalogSchemaVersion)
		apiContractVersion = maxUint32(current.APIContractVersion, sourceCatalogAPIContractVersion)
	}
	return fmt.Sprintf(`INSERT INTO meta.discovery_probe
	(probe_id, catalog_id, probe_name, integration_archetype, probe_patterns, review_status, schema_version, record_version, api_contract_version, updated_at, attrs, evidence)
	VALUES ('%s','%s','%s','%s',%s,'%s',%d,%d,%d,'%s','%s','%s')`,
		esc(probe.CatalogID),
		esc(probe.CatalogID),
		esc(probe.Name),
		esc(probe.IntegrationArchetype),
		arr(probe.ProbePatterns),
		esc(reviewStatus),
		schemaVersion,
		recordVersion,
		apiContractVersion,
		esc(formatClickHouseTime(now)),
		esc(mergeGovernanceAttrs(current.Attrs, checksum, "")),
		esc(evidence),
	), true
}

func latestSourceCatalogRecords(ctx context.Context, runner sourceRegistryStore) (map[string]sourceCatalogGovernanceRecord, error) {
	out, err := runner.Query(ctx, "SELECT catalog_id, review_status, materialized_source_id, record_version, schema_version, api_contract_version, attrs, evidence FROM meta.source_catalog FINAL FORMAT JSONEachRow")
	if err != nil {
		if strings.Contains(err.Error(), "UNKNOWN_TABLE") {
			return map[string]sourceCatalogGovernanceRecord{}, nil
		}
		return nil, err
	}
	records := map[string]sourceCatalogGovernanceRecord{}
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if !strings.HasPrefix(line, "{") {
			continue
		}
		var raw map[string]json.RawMessage
		if err := json.Unmarshal([]byte(line), &raw); err != nil {
			return nil, err
		}
		id, _ := decodeJSONString(raw["catalog_id"])
		reviewStatus, _ := decodeJSONString(raw["review_status"])
		materializedSourceID, _ := decodeNullableString(raw["materialized_source_id"])
		recordVersion, _ := decodeUint64(raw["record_version"])
		schemaVersion, _ := decodeUint32(raw["schema_version"])
		apiContractVersion, _ := decodeUint32(raw["api_contract_version"])
		attrs, _ := decodeJSONString(raw["attrs"])
		evidence, _ := decodeJSONString(raw["evidence"])
		records[id] = sourceCatalogGovernanceRecord{CatalogID: id, ReviewStatus: reviewStatus, MaterializedSourceID: materializedSourceID, RecordVersion: recordVersion, SchemaVersion: schemaVersion, APIContractVersion: apiContractVersion, Attrs: attrs, Evidence: evidence}
	}
	return records, nil
}

func latestSourceFamilyTemplateRecords(ctx context.Context, runner sourceRegistryStore) (map[string]sourceFamilyTemplateRecord, error) {
	out, err := runner.Query(ctx, "SELECT template_id, review_status, record_version, schema_version, api_contract_version, attrs, evidence FROM meta.source_family_template FINAL FORMAT JSONEachRow")
	if err != nil {
		if strings.Contains(err.Error(), "UNKNOWN_TABLE") {
			return map[string]sourceFamilyTemplateRecord{}, nil
		}
		return nil, err
	}
	records := map[string]sourceFamilyTemplateRecord{}
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if !strings.HasPrefix(line, "{") {
			continue
		}
		var raw map[string]json.RawMessage
		if err := json.Unmarshal([]byte(line), &raw); err != nil {
			return nil, err
		}
		id, _ := decodeJSONString(raw["template_id"])
		reviewStatus, _ := decodeJSONString(raw["review_status"])
		recordVersion, _ := decodeUint64(raw["record_version"])
		schemaVersion, _ := decodeUint32(raw["schema_version"])
		apiContractVersion, _ := decodeUint32(raw["api_contract_version"])
		attrs, _ := decodeJSONString(raw["attrs"])
		evidence, _ := decodeJSONString(raw["evidence"])
		records[id] = sourceFamilyTemplateRecord{TemplateID: id, ReviewStatus: reviewStatus, RecordVersion: recordVersion, SchemaVersion: schemaVersion, APIContractVersion: apiContractVersion, Attrs: attrs, Evidence: evidence}
	}
	return records, nil
}

func latestDiscoveryProbeRecords(ctx context.Context, runner sourceRegistryStore) (map[string]discoveryProbeGovernanceRecord, error) {
	out, err := runner.Query(ctx, "SELECT probe_id, review_status, record_version, schema_version, api_contract_version, attrs, evidence FROM meta.discovery_probe FINAL FORMAT JSONEachRow")
	if err != nil {
		if strings.Contains(err.Error(), "UNKNOWN_TABLE") {
			return map[string]discoveryProbeGovernanceRecord{}, nil
		}
		return nil, err
	}
	records := map[string]discoveryProbeGovernanceRecord{}
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if !strings.HasPrefix(line, "{") {
			continue
		}
		var raw map[string]json.RawMessage
		if err := json.Unmarshal([]byte(line), &raw); err != nil {
			return nil, err
		}
		id, _ := decodeJSONString(raw["probe_id"])
		reviewStatus, _ := decodeJSONString(raw["review_status"])
		recordVersion, _ := decodeUint64(raw["record_version"])
		schemaVersion, _ := decodeUint32(raw["schema_version"])
		apiContractVersion, _ := decodeUint32(raw["api_contract_version"])
		attrs, _ := decodeJSONString(raw["attrs"])
		evidence, _ := decodeJSONString(raw["evidence"])
		records[id] = discoveryProbeGovernanceRecord{ProbeID: id, ReviewStatus: reviewStatus, RecordVersion: recordVersion, SchemaVersion: schemaVersion, APIContractVersion: apiContractVersion, Attrs: attrs, Evidence: evidence}
	}
	return records, nil
}

func governanceChecksum(value any, markdownChecksum string) string {
	b, err := json.Marshal(struct {
		Value            any    `json:"value"`
		MarkdownChecksum string `json:"markdown_checksum,omitempty"`
	}{Value: value, MarkdownChecksum: markdownChecksum})
	if err != nil {
		return ""
	}
	return sum(b)
}

func governanceChecksumFromAttrs(attrs string) string {
	decoded, ok := decodeJSONObject(attrs)
	if !ok {
		return ""
	}
	checksum, _ := decoded["catalog_checksum"].(string)
	return checksum
}

func mergeGovernanceAttrs(existingAttrs, checksum, markdownChecksum string) string {
	attrs, ok := decodeJSONObject(existingAttrs)
	if !ok {
		attrs = map[string]any{}
	}
	attrs["catalog_checksum"] = checksum
	if strings.TrimSpace(markdownChecksum) != "" {
		attrs["source_markdown_checksum"] = markdownChecksum
	}
	b, err := json.Marshal(attrs)
	if err != nil {
		return fmt.Sprintf(`{"catalog_checksum":%q}`, checksum)
	}
	return string(b)
}

func mergeSourceCatalogAttrs(existingAttrs, checksum, markdownChecksum string, entry sourceCatalogEntry) string {
	attrs, ok := decodeJSONObject(existingAttrs)
	if !ok {
		attrs = map[string]any{}
	}
	attrs["catalog_checksum"] = checksum
	if strings.TrimSpace(markdownChecksum) != "" {
		attrs["source_markdown_checksum"] = markdownChecksum
	}
	attrs["credential_requirement"] = map[string]any{
		"requires_registration": entry.CredentialRequirement.RequiresRegistration,
		"requires_approval":     entry.CredentialRequirement.RequiresApproval,
		"commercial_terms":      entry.CredentialRequirement.CommercialTerms,
		"noncommercial_terms":   entry.CredentialRequirement.NoncommercialTerms,
		"restricted_access":     entry.CredentialRequirement.RestrictedAccess,
	}
	if envVar := strings.TrimSpace(entry.AuthConfig.EnvVar); envVar != "" {
		attrs["auth_env_var"] = envVar
	} else {
		delete(attrs, "auth_env_var")
	}
	b, err := json.Marshal(attrs)
	if err != nil {
		return mergeGovernanceAttrs(existingAttrs, checksum, markdownChecksum)
	}
	return string(b)
}

func sourceCatalogAttrsEquivalent(existingAttrs, desiredAttrs string) bool {
	existingDecoded, existingOK := decodeJSONObject(existingAttrs)
	desiredDecoded, desiredOK := decodeJSONObject(desiredAttrs)
	if !existingOK || !desiredOK {
		return false
	}
	return reflect.DeepEqual(existingDecoded, desiredDecoded)
}
