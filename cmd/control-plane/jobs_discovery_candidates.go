package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"global-osint-backend/internal/discovery"
	"global-osint-backend/internal/migrate"
	"global-osint-backend/internal/sourcecatalog"
)

type persistedDiscoveryCandidate struct {
	CandidateID          string
	ReviewStatus         string
	MaterializedSourceID *string
	RecordVersion        uint64
	SchemaVersion        uint32
	APIContractVersion   uint32
	Attrs                string
	Evidence             string
}

const controlPlaneCompiledCatalogPathEnv = "SOURCE_CATALOG_COMPILED_PATH"

type generatedDiscoveryCandidate struct {
	CandidateID          string
	CatalogID            string
	CandidateName        string
	CandidateURL         string
	IntegrationArchetype string
	DetectedPlatform     string
	ReviewStatus         string
	MaterializedSourceID string
}

func persistFingerprintCandidates(ctx context.Context, runner *migrate.HTTPRunner, candidates []discovery.FingerprintCandidate, now time.Time) error {
	generated := make([]generatedDiscoveryCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		generated = append(generated, generatedDiscoveryCandidate{
			CandidateID:          candidate.CandidateID,
			CatalogID:            candidate.CatalogID,
			CandidateName:        candidate.CandidateName,
			CandidateURL:         candidate.CandidateURL,
			IntegrationArchetype: candidate.IntegrationArchetype,
			DetectedPlatform:     candidate.DetectedPlatform,
			ReviewStatus:         candidate.ReviewStatus,
			MaterializedSourceID: candidate.MaterializedSourceID,
		})
	}
	return persistGeneratedDiscoveryCandidates(ctx, runner, generated, now)
}

func persistFamilyCandidates(ctx context.Context, runner *migrate.HTTPRunner, candidates []discovery.FamilyCandidate, now time.Time) error {
	generated := make([]generatedDiscoveryCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		generated = append(generated, generatedDiscoveryCandidate{
			CandidateID:          candidate.CandidateID,
			CatalogID:            candidate.CatalogID,
			CandidateName:        candidate.CandidateName,
			CandidateURL:         candidate.CandidateURL,
			IntegrationArchetype: candidate.IntegrationArchetype,
			DetectedPlatform:     candidate.DetectedPlatform,
			ReviewStatus:         candidate.ReviewStatus,
			MaterializedSourceID: candidate.MaterializedSourceID,
		})
	}
	return persistGeneratedDiscoveryCandidates(ctx, runner, generated, now)
}

func runFamilyTemplateGenerationTick(ctx context.Context) error {
	path, err := controlPlaneCompiledCatalogPath()
	if err != nil {
		return nil
	}
	families, members, err := loadCompiledFamilyTemplateInputs(path)
	if err != nil {
		return err
	}
	generated := make([]discovery.FamilyCandidate, 0)
	now := time.Now().UTC()
	for _, template := range families {
		generated = append(generated, discovery.GenerateFamilyCandidatesFromMembers(template, members, now)...)
	}
	if len(generated) == 0 {
		return nil
	}
	return persistFamilyCandidates(ctx, migrate.NewHTTPRunner(controlPlaneClickHouseURL()), generated, now)
}

func controlPlaneCompiledCatalogPath() (string, error) {
	if path := strings.TrimSpace(os.Getenv(controlPlaneCompiledCatalogPathEnv)); path != "" {
		return path, nil
	}
	defaultPath := "/app/seed/source_catalog_compiled.json"
	if _, err := os.Stat(defaultPath); err == nil {
		return defaultPath, nil
	}
	return "", fmt.Errorf("compiled catalog not configured")
}

func loadCompiledFamilyTemplateInputs(path string) ([]discovery.FamilyTemplate, []discovery.FamilyMember, error) {
	compiled, err := sourcecatalog.LoadCompiled(path)
	if err != nil {
		return nil, nil, err
	}
	families := make([]discovery.FamilyTemplate, 0, len(compiled.FamilyTemplates))
	for _, template := range compiled.FamilyTemplates {
		families = append(families, discovery.FamilyTemplate{
			CatalogID:            template.CatalogID,
			Name:                 template.Name,
			Scope:                template.Scope,
			Outputs:              template.Outputs,
			IntegrationArchetype: template.IntegrationArchetype,
			Tags:                 append([]string(nil), template.Tags...),
		})
	}
	members := make([]discovery.FamilyMember, 0)
	for _, entry := range compiled.Catalog.Entries {
		if entry.CatalogKind != "concrete" {
			continue
		}
		candidateURL := strings.TrimSpace(entry.OfficialDocsURL)
		if candidateURL == "" {
			continue
		}
		members = append(members, discovery.FamilyMember{
			CatalogID:    entry.CatalogID,
			Name:         entry.Name,
			Scope:        entry.Scope,
			Tags:         append([]string(nil), entry.Tags...),
			CandidateURL: candidateURL,
		})
	}
	return families, members, nil
}

func persistGeneratedDiscoveryCandidates(ctx context.Context, runner *migrate.HTTPRunner, candidates []generatedDiscoveryCandidate, now time.Time) error {
	existing, err := latestDiscoveryCandidates(ctx, runner)
	if err != nil {
		return err
	}
	inserted := false
	for _, candidate := range candidates {
		sql, ok := buildDiscoveryCandidateSQL(candidate, existing, now)
		if !ok {
			continue
		}
		if err := runner.ApplySQL(ctx, sql); err != nil {
			return err
		}
		inserted = true
	}
	if inserted {
		if err := runner.ApplySQL(ctx, "OPTIMIZE TABLE meta.discovery_candidate FINAL"); err != nil {
			return err
		}
	}
	return nil
}

func latestDiscoveryCandidates(ctx context.Context, runner *migrate.HTTPRunner) (map[string]persistedDiscoveryCandidate, error) {
	out, err := runner.Query(ctx, "SELECT candidate_id, review_status, materialized_source_id, record_version, schema_version, api_contract_version, attrs, evidence FROM meta.discovery_candidate FINAL FORMAT JSONEachRow")
	if err != nil {
		if strings.Contains(err.Error(), "UNKNOWN_TABLE") {
			return map[string]persistedDiscoveryCandidate{}, nil
		}
		return nil, err
	}
	results := map[string]persistedDiscoveryCandidate{}
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || !strings.HasPrefix(line, "{") {
			continue
		}
		var raw map[string]json.RawMessage
		if err := json.Unmarshal([]byte(line), &raw); err != nil {
			return nil, err
		}
		candidateID, _ := decodeJSONString(raw["candidate_id"])
		reviewStatus, _ := decodeJSONString(raw["review_status"])
		materializedSourceID, _ := decodeNullableString(raw["materialized_source_id"])
		recordVersion, _ := decodeUint64(raw["record_version"])
		schemaVersion, _ := decodeUint32(raw["schema_version"])
		apiContractVersion, _ := decodeUint32(raw["api_contract_version"])
		attrs, _ := decodeJSONString(raw["attrs"])
		evidence, _ := decodeJSONString(raw["evidence"])
		results[candidateID] = persistedDiscoveryCandidate{CandidateID: candidateID, ReviewStatus: reviewStatus, MaterializedSourceID: materializedSourceID, RecordVersion: recordVersion, SchemaVersion: schemaVersion, APIContractVersion: apiContractVersion, Attrs: attrs, Evidence: evidence}
	}
	return results, nil
}

func buildDiscoveryCandidateSQL(candidate generatedDiscoveryCandidate, existing map[string]persistedDiscoveryCandidate, now time.Time) (string, bool) {
	checksumPayload := map[string]any{
		"catalog_id":            strings.TrimSpace(candidate.CatalogID),
		"candidate_name":        strings.TrimSpace(candidate.CandidateName),
		"candidate_url":         strings.TrimSpace(candidate.CandidateURL),
		"integration_archetype": strings.TrimSpace(candidate.IntegrationArchetype),
		"detected_platform":     strings.TrimSpace(candidate.DetectedPlatform),
	}
	checksumBytes, _ := json.Marshal(checksumPayload)
	checksum := hashDiscoveryCandidateChecksum(checksumBytes)
	current, ok := existing[strings.TrimSpace(candidate.CandidateID)]
	if ok {
		attrs, decoded := decodeJSONObject(current.Attrs)
		if decoded {
			if existingChecksum, _ := attrs["candidate_checksum"].(string); existingChecksum == checksum {
				return "", false
			}
		}
	}
	reviewStatus := "review_required"
	evidence := "[]"
	materializedSourceID := (*string)(nil)
	recordVersion := uint64(1)
	schemaVersion := uint32(1)
	apiContractVersion := uint32(1)
	if ok {
		reviewStatus = controlPlaneFallbackString(current.ReviewStatus, reviewStatus)
		evidence = controlPlaneFallbackString(current.Evidence, evidence)
		materializedSourceID = current.MaterializedSourceID
		recordVersion = current.RecordVersion + 1
		schemaVersion = controlPlaneMaxUint32(current.SchemaVersion, schemaVersion)
		apiContractVersion = controlPlaneMaxUint32(current.APIContractVersion, apiContractVersion)
	}
	attrs := fmt.Sprintf(`{"candidate_checksum":%q}`, checksum)
	return fmt.Sprintf(`INSERT INTO meta.discovery_candidate
	(candidate_id, catalog_id, candidate_name, candidate_url, integration_archetype, detected_platform, review_status, materialized_source_id, schema_version, record_version, api_contract_version, updated_at, attrs, evidence)
	VALUES ('%s','%s','%s','%s','%s','%s','%s',%s,%d,%d,%d,toDateTime64('%s', 3, 'UTC'),'%s','%s')`,
		controlPlaneEsc(candidate.CandidateID),
		controlPlaneEsc(candidate.CatalogID),
		controlPlaneEsc(candidate.CandidateName),
		controlPlaneEsc(candidate.CandidateURL),
		controlPlaneEsc(candidate.IntegrationArchetype),
		controlPlaneEsc(candidate.DetectedPlatform),
		controlPlaneEsc(reviewStatus),
		controlPlaneNullableString(materializedSourceID),
		schemaVersion,
		recordVersion,
		apiContractVersion,
		controlPlaneEsc(now.UTC().Format(time.RFC3339Nano)),
		controlPlaneEsc(attrs),
		controlPlaneEsc(evidence),
	), true
}

func hashDiscoveryCandidateChecksum(payload []byte) string {
	digest := sha256.Sum256(payload)
	return fmt.Sprintf("%x", digest[:])
}

func decodeJSONString(raw json.RawMessage) (string, error) {
	if len(raw) == 0 {
		return "", nil
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", err
	}
	return value, nil
}

func decodeNullableString(raw json.RawMessage) (*string, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	value, err := decodeJSONString(raw)
	if err != nil {
		return nil, err
	}
	return &value, nil
}

func decodeUint64(raw json.RawMessage) (uint64, error) {
	if len(raw) == 0 {
		return 0, nil
	}
	var asNumber json.Number
	if err := json.Unmarshal(raw, &asNumber); err == nil {
		return strconv.ParseUint(asNumber.String(), 10, 64)
	}
	var value uint64
	if err := json.Unmarshal(raw, &value); err != nil {
		return 0, err
	}
	return value, nil
}

func decodeUint32(raw json.RawMessage) (uint32, error) {
	value, err := decodeUint64(raw)
	return uint32(value), err
}

func decodeJSONObject(raw string) (map[string]any, bool) {
	decoded := map[string]any{}
	if strings.TrimSpace(raw) == "" {
		return decoded, false
	}
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		return map[string]any{}, false
	}
	return decoded, true
}

func controlPlaneFallbackString(current, fallback string) string {
	if strings.TrimSpace(current) != "" {
		return strings.TrimSpace(current)
	}
	return strings.TrimSpace(fallback)
}

func controlPlaneMaxUint32(left, right uint32) uint32 {
	if left > right {
		return left
	}
	return right
}

func controlPlaneEsc(value string) string {
	return strings.ReplaceAll(strings.TrimSpace(value), "'", "''")
}

func controlPlaneNullableString(value *string) string {
	if value == nil || strings.TrimSpace(*value) == "" {
		return "NULL"
	}
	return fmt.Sprintf("'%s'", controlPlaneEsc(*value))
}
