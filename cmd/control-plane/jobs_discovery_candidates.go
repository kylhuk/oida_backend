package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"sort"
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
	ClassifierKind       string
	ClassifierSignals    []string
	ObservedFrom         []string
	Geography            string
	AdminLevel           string
	ChildSource          *discovery.GeneratedChildSource
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
			ClassifierKind:       candidate.ClassifierKind,
			ClassifierSignals:    append([]string(nil), candidate.ClassifierSignals...),
			ObservedFrom:         append([]string(nil), candidate.ObservedFrom...),
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
			ClassifierKind:       "family_template",
			Geography:            candidate.Geography,
			AdminLevel:           candidate.AdminLevel,
			ChildSource:          &candidate.ChildSource,
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

func runFingerprintProbeGenerationTick(ctx context.Context) error {
	path, err := controlPlaneCompiledCatalogPath()
	if err != nil {
		return nil
	}
	probes, observedURLs, err := loadCompiledFingerprintProbeInputs(path)
	if err != nil {
		return err
	}
	generated := make([]discovery.FingerprintCandidate, 0)
	now := time.Now().UTC()
	for _, probe := range probes {
		generated = append(generated, discovery.GenerateFingerprintCandidates(probe, observedURLs, now)...)
	}
	if len(generated) == 0 {
		return nil
	}
	return persistFingerprintCandidates(ctx, migrate.NewHTTPRunner(controlPlaneClickHouseURL()), generated, now)
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
			IntegrationArchetype: template.ChildSource.IntegrationArchetype,
			TransportType:        template.TransportType,
			ScopeLevels:          append([]string(nil), template.ScopeLevels...),
			Tags:                 append([]string(nil), template.Tags...),
			ChildSource: discovery.FamilyChildSourceTemplate{
				TransportType:        template.ChildSource.TransportType,
				IntegrationArchetype: template.ChildSource.IntegrationArchetype,
				FormatHint:           template.ChildSource.FormatHint,
				ParserID:             template.ChildSource.ParserID,
				SourceClass:          template.ChildSource.SourceClass,
				RefreshStrategy:      template.ChildSource.RefreshStrategy,
				CrawlStrategy:        template.ChildSource.CrawlStrategy,
				ExpectedPlaceTypes:   append([]string(nil), template.ChildSource.ExpectedPlaceTypes...),
			},
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
			Geography:    discovery.InferGeographyFromScope(entry.Scope),
			AdminLevel:   discovery.InferAdminLevelFromScope(entry.Scope),
		})
	}
	return families, members, nil
}

func loadCompiledFingerprintProbeInputs(path string) ([]discovery.FingerprintProbe, []string, error) {
	compiled, err := sourcecatalog.LoadCompiled(path)
	if err != nil {
		return nil, nil, err
	}
	probes := make([]discovery.FingerprintProbe, 0, len(compiled.FingerprintProbes))
	for _, probe := range compiled.FingerprintProbes {
		probes = append(probes, discovery.FingerprintProbe{
			CatalogID:            probe.CatalogID,
			ProbeName:            probe.Name,
			IntegrationArchetype: probe.IntegrationArchetype,
			ProbePatterns:        append([]string(nil), probe.ProbePatterns...),
		})
	}
	observedSet := map[string]struct{}{}
	for _, entry := range compiled.Catalog.Entries {
		if entry.CatalogKind != "concrete" {
			continue
		}
		candidateURL := strings.TrimSpace(entry.OfficialDocsURL)
		if candidateURL == "" {
			continue
		}
		canonical, err := discovery.NormalizeURL(candidateURL)
		if err != nil || canonical == "" {
			continue
		}
		observedSet[canonical] = struct{}{}
	}
	observedURLs := make([]string, 0, len(observedSet))
	for candidateURL := range observedSet {
		observedURLs = append(observedURLs, candidateURL)
	}
	sort.Strings(observedURLs)
	return probes, observedURLs, nil
}

type discoveryCandidateAttrs struct {
	CandidateChecksum string                               `json:"candidate_checksum"`
	Classifier        discoveryCandidateClassifierMetadata `json:"classifier"`
	Family            *discoveryCandidateFamilyMetadata    `json:"family,omitempty"`
}

type discoveryCandidateClassifierMetadata struct {
	Kind                 string   `json:"kind"`
	CatalogID            string   `json:"catalog_id"`
	Platform             string   `json:"platform"`
	IntegrationArchetype string   `json:"integration_archetype"`
	Signals              []string `json:"signals,omitempty"`
	ObservedFrom         []string `json:"observed_from,omitempty"`
}

type discoveryCandidateFamilyMetadata struct {
	Geography   string                                `json:"geography"`
	AdminLevel  string                                `json:"admin_level"`
	ChildSource discoveryCandidateChildSourceMetadata `json:"child_source"`
}

type discoveryCandidateChildSourceMetadata struct {
	SourceID             string   `json:"source_id"`
	Domain               string   `json:"domain"`
	Entrypoints          []string `json:"entrypoints"`
	TransportType        string   `json:"transport_type"`
	IntegrationArchetype string   `json:"integration_archetype"`
	FormatHint           string   `json:"format_hint"`
	ParserID             string   `json:"parser_id"`
	SourceClass          string   `json:"source_class"`
	RefreshStrategy      string   `json:"refresh_strategy"`
	CrawlStrategy        string   `json:"crawl_strategy"`
	ExpectedPlaceTypes   []string `json:"expected_place_types"`
	Geography            string   `json:"geography"`
	AdminLevel           string   `json:"admin_level"`
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
		if err := runner.ApplySQLBody(ctx, sql); err != nil {
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
	classifierSignals := append([]string(nil), candidate.ClassifierSignals...)
	sort.Strings(classifierSignals)
	observedFrom := append([]string(nil), candidate.ObservedFrom...)
	sort.Strings(observedFrom)
	checksumPayload := map[string]any{
		"catalog_id":            strings.TrimSpace(candidate.CatalogID),
		"candidate_name":        strings.TrimSpace(candidate.CandidateName),
		"candidate_url":         strings.TrimSpace(candidate.CandidateURL),
		"integration_archetype": strings.TrimSpace(candidate.IntegrationArchetype),
		"detected_platform":     strings.TrimSpace(candidate.DetectedPlatform),
		"classifier_kind":       strings.TrimSpace(candidate.ClassifierKind),
		"classifier_signals":    classifierSignals,
		"observed_from":         observedFrom,
		"geography":             strings.TrimSpace(candidate.Geography),
		"admin_level":           strings.TrimSpace(candidate.AdminLevel),
		"child_source":          candidate.ChildSource,
	}
	checksumBytes, _ := json.Marshal(checksumPayload)
	checksum := hashDiscoveryCandidateChecksum(checksumBytes)
	attrsPayload := discoveryCandidateAttrs{
		CandidateChecksum: checksum,
		Classifier: discoveryCandidateClassifierMetadata{
			Kind:                 strings.TrimSpace(candidate.ClassifierKind),
			CatalogID:            strings.TrimSpace(candidate.CatalogID),
			Platform:             strings.TrimSpace(candidate.DetectedPlatform),
			IntegrationArchetype: strings.TrimSpace(candidate.IntegrationArchetype),
			Signals:              classifierSignals,
			ObservedFrom:         observedFrom,
		},
	}
	if candidate.ChildSource != nil {
		attrsPayload.Family = &discoveryCandidateFamilyMetadata{
			Geography:  strings.TrimSpace(candidate.Geography),
			AdminLevel: strings.TrimSpace(candidate.AdminLevel),
			ChildSource: discoveryCandidateChildSourceMetadata{
				SourceID:             strings.TrimSpace(candidate.ChildSource.SourceID),
				Domain:               strings.TrimSpace(candidate.ChildSource.Domain),
				Entrypoints:          append([]string(nil), candidate.ChildSource.Entrypoints...),
				TransportType:        strings.TrimSpace(candidate.ChildSource.TransportType),
				IntegrationArchetype: strings.TrimSpace(candidate.ChildSource.IntegrationArchetype),
				FormatHint:           strings.TrimSpace(candidate.ChildSource.FormatHint),
				ParserID:             strings.TrimSpace(candidate.ChildSource.ParserID),
				SourceClass:          strings.TrimSpace(candidate.ChildSource.SourceClass),
				RefreshStrategy:      strings.TrimSpace(candidate.ChildSource.RefreshStrategy),
				CrawlStrategy:        strings.TrimSpace(candidate.ChildSource.CrawlStrategy),
				ExpectedPlaceTypes:   append([]string(nil), candidate.ChildSource.ExpectedPlaceTypes...),
				Geography:            strings.TrimSpace(candidate.ChildSource.Geography),
				AdminLevel:           strings.TrimSpace(candidate.ChildSource.AdminLevel),
			},
		}
	}
	attrsBytes, _ := json.Marshal(attrsPayload)
	attrsJSON := string(attrsBytes)
	current, ok := existing[strings.TrimSpace(candidate.CandidateID)]
	if ok {
		attrs, decoded := decodeJSONObject(current.Attrs)
		if decoded {
			if existingChecksum, _ := attrs["candidate_checksum"].(string); existingChecksum == checksum && strings.TrimSpace(current.Attrs) == attrsJSON {
				return "", false
			}
		}
	}
	reviewStatus := "review_required"
	evidence := "[]"
	materializedSourceID := controlPlaneStringPtr(candidate.MaterializedSourceID)
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
	return fmt.Sprintf(`INSERT INTO meta.discovery_candidate
	(candidate_id, catalog_id, candidate_name, candidate_url, integration_archetype, detected_platform, review_status, materialized_source_id, schema_version, record_version, api_contract_version, updated_at, attrs, evidence)
	SELECT '%s' AS candidate_id, '%s' AS catalog_id, '%s' AS candidate_name, '%s' AS candidate_url, '%s' AS integration_archetype, '%s' AS detected_platform, '%s' AS review_status, %s AS materialized_source_id, %d AS schema_version, %d AS record_version, %d AS api_contract_version, toDateTime64('%s', 3, 'UTC') AS updated_at, '%s' AS attrs, '%s' AS evidence`,
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
		controlPlaneEsc(now.UTC().Format("2006-01-02 15:04:05.000")),
		controlPlaneEsc(attrsJSON),
		controlPlaneEsc(evidence),
	), true
}

func controlPlaneStringPtr(value string) *string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	v := strings.TrimSpace(value)
	return &v
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
