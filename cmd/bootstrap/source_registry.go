package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	sourceRegistrySchemaVersion      = 2
	sourceRegistryAPIContractVersion = 1
	defaultRequestsPerMinute         = 60
	defaultBurstSize                 = 10
	defaultRetentionClass            = "warm"
	defaultBackfillPriority          = 100
	defaultReviewStatus              = "approved"
)

type sourceRegistryStore interface {
	Query(ctx context.Context, q string) (string, error)
	ApplySQL(ctx context.Context, sql string) error
}

type normalizedSourceSeed struct {
	SourceID            string   `json:"source_id"`
	Domain              string   `json:"domain"`
	DomainFamily        string   `json:"domain_family"`
	SourceClass         string   `json:"source_class"`
	Entrypoints         []string `json:"entrypoints"`
	AuthMode            string   `json:"auth_mode"`
	AuthConfigJSON      string   `json:"auth_config_json"`
	FormatHint          string   `json:"format_hint"`
	RobotsPolicy        string   `json:"robots_policy"`
	RefreshStrategy     string   `json:"refresh_strategy"`
	RequestsPerMinute   uint32   `json:"requests_per_minute"`
	BurstSize           uint16   `json:"burst_size"`
	RetentionClass      string   `json:"retention_class"`
	License             string   `json:"license"`
	TermsURL            string   `json:"terms_url"`
	AttributionRequired bool     `json:"attribution_required"`
	GeoScope            string   `json:"geo_scope"`
	Priority            uint16   `json:"priority"`
	ParserID            string   `json:"parser_id"`
	EntityTypes         []string `json:"entity_types"`
	ExpectedPlaceTypes  []string `json:"expected_place_types"`
	SupportsHistorical  bool     `json:"supports_historical"`
	SupportsDelta       bool     `json:"supports_delta"`
	BackfillPriority    uint16   `json:"backfill_priority"`
	ConfidenceBaseline  float32  `json:"confidence_baseline"`
	InitialReviewStatus string   `json:"initial_review_status"`
	InitialReviewNotes  string   `json:"initial_review_notes"`
}

type sourceRegistryRecord struct {
	SourceID            string   `json:"source_id"`
	Domain              string   `json:"domain"`
	DomainFamily        string   `json:"domain_family"`
	SourceClass         string   `json:"source_class"`
	Entrypoints         []string `json:"entrypoints"`
	AuthMode            string   `json:"auth_mode"`
	AuthConfigJSON      string   `json:"auth_config_json"`
	FormatHint          string   `json:"format_hint"`
	RobotsPolicy        string   `json:"robots_policy"`
	RefreshStrategy     string   `json:"refresh_strategy"`
	RequestsPerMinute   uint32   `json:"requests_per_minute"`
	BurstSize           uint16   `json:"burst_size"`
	RetentionClass      string   `json:"retention_class"`
	License             string   `json:"license"`
	TermsURL            string   `json:"terms_url"`
	AttributionRequired uint8    `json:"attribution_required"`
	GeoScope            string   `json:"geo_scope"`
	Priority            uint16   `json:"priority"`
	ParserID            string   `json:"parser_id"`
	EntityTypes         []string `json:"entity_types"`
	ExpectedPlaceTypes  []string `json:"expected_place_types"`
	SupportsHistorical  uint8    `json:"supports_historical"`
	SupportsDelta       uint8    `json:"supports_delta"`
	BackfillPriority    uint16   `json:"backfill_priority"`
	ConfidenceBaseline  float32  `json:"confidence_baseline"`
	Enabled             uint8    `json:"enabled"`
	DisabledReason      *string  `json:"disabled_reason"`
	DisabledAt          *string  `json:"disabled_at"`
	DisabledBy          *string  `json:"disabled_by"`
	ReviewStatus        string   `json:"review_status"`
	ReviewNotes         string   `json:"review_notes"`
	Version             uint64   `json:"version"`
	SchemaVersion       uint32   `json:"schema_version"`
	RecordVersion       uint64   `json:"record_version"`
	APIContractVersion  uint32   `json:"api_contract_version"`
	Attrs               string   `json:"attrs"`
	Evidence            string   `json:"evidence"`
	UpdatedAt           string   `json:"updated_at"`
}

type sourceRateLimiter struct {
	tokens          float64
	burst           float64
	refillPerSecond float64
	lastRefill      time.Time
}

func loadSourceSeed(ctx context.Context, runner sourceRegistryStore, path string) error {
	supportsGovernance, err := sourceRegistrySupportsGovernance(ctx, runner)
	if err != nil {
		return err
	}
	if !supportsGovernance {
		return loadLegacySourceSeed(ctx, runner, path)
	}

	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	var seeds []sourceSeed
	if err := json.Unmarshal(b, &seeds); err != nil {
		return err
	}

	existing, err := latestSourceRegistryRecords(ctx, runner)
	if err != nil {
		return err
	}

	records, err := buildSourceRegistryRecords(seeds, existing, time.Now().UTC())
	if err != nil {
		return err
	}

	for _, record := range records {
		if err := runner.ApplySQL(ctx, insertSourceRegistryRecordSQL(record)); err != nil {
			return err
		}
	}

	return nil
}

func sourceRegistrySupportsGovernance(ctx context.Context, runner sourceRegistryStore) (bool, error) {
	out, err := runner.Query(ctx, "SELECT count() FROM system.columns WHERE database = 'meta' AND table = 'source_registry' AND name = 'auth_config_json' FORMAT TabSeparated")
	if err != nil {
		return false, err
	}
	return strings.TrimSpace(out) != "0", nil
}

func loadLegacySourceSeed(ctx context.Context, runner sourceRegistryStore, path string) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	var seeds []sourceSeed
	if err := json.Unmarshal(b, &seeds); err != nil {
		return err
	}

	for _, seed := range seeds {
		check := fmt.Sprintf("SELECT count() FROM meta.source_registry WHERE source_id='%s' FORMAT TabSeparated", esc(seed.SourceID))
		out, err := runner.Query(ctx, check)
		if err != nil {
			return err
		}
		if strings.TrimSpace(out) != "0" {
			continue
		}
		if err := runner.ApplySQL(ctx, legacySourceRegistryInsertSQL(seed)); err != nil {
			return err
		}
	}

	return nil
}

func legacySourceRegistryInsertSQL(seed sourceSeed) string {
	return fmt.Sprintf(`INSERT INTO meta.source_registry
	(source_id, domain, domain_family, source_class, entrypoints, auth_mode, format_hint, robots_policy, refresh_strategy, license, terms_url, geo_scope, priority, parser_id, entity_types, expected_place_types, supports_historical, supports_delta, confidence_baseline, enabled, version, schema_version, record_version, api_contract_version, attrs, evidence, updated_at)
	VALUES ('%s','%s','%s','%s',%s,'%s','%s','%s','%s','%s','%s','%s',%d,'%s',%s,%s,%d,%d,%f,1,1,1,1,1,'{}','[]',now64(3))`,
		esc(seed.SourceID), esc(seed.Domain), esc(seed.DomainFamily), esc(seed.SourceClass), arr(seed.Entrypoints), esc(seed.AuthMode), esc(seed.FormatHint), esc(seed.RobotsPolicy), esc(seed.RefreshStrategy), esc(seed.License), esc(seed.TermsURL), esc(seed.GeoScope), seed.Priority, esc(seed.ParserID), arr(seed.EntityTypes), arr(seed.ExpectedPlaceTypes), btoi(seed.SupportsHistorical), btoi(seed.SupportsDelta), seed.ConfidenceBaseline)
}

func latestSourceRegistryRecords(ctx context.Context, runner sourceRegistryStore) (map[string]sourceRegistryRecord, error) {
	query := `SELECT
		source_id,
		domain,
		domain_family,
		source_class,
		entrypoints,
		auth_mode,
		auth_config_json,
		format_hint,
		robots_policy,
		refresh_strategy,
		requests_per_minute,
		burst_size,
		retention_class,
		license,
		terms_url,
		attribution_required,
		geo_scope,
		priority,
		parser_id,
		entity_types,
		expected_place_types,
		supports_historical,
		supports_delta,
		backfill_priority,
		confidence_baseline,
		enabled,
		disabled_reason,
		disabled_at,
		disabled_by,
		review_status,
		review_notes,
		version,
		schema_version,
		record_version,
		api_contract_version,
		attrs,
		evidence,
		updated_at
	FROM meta.source_registry FINAL
	FORMAT JSONEachRow`

	out, err := runner.Query(ctx, query)
	if err != nil {
		if strings.Contains(err.Error(), "UNKNOWN_TABLE") {
			return map[string]sourceRegistryRecord{}, nil
		}
		return nil, err
	}

	records := make(map[string]sourceRegistryRecord)
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		record, err := decodeSourceRegistryRecord(line)
		if err != nil {
			return nil, fmt.Errorf("decode source registry row: %w", err)
		}
		records[record.SourceID] = record
	}

	return records, nil
}

func buildSourceRegistryRecords(seeds []sourceSeed, existing map[string]sourceRegistryRecord, now time.Time) ([]sourceRegistryRecord, error) {
	records := make([]sourceRegistryRecord, 0, len(seeds))
	for _, raw := range seeds {
		seed, err := normalizeSourceSeed(raw)
		if err != nil {
			return nil, err
		}
		checksum, err := sourceSeedFingerprint(seed)
		if err != nil {
			return nil, err
		}

		current, ok := existing[seed.SourceID]
		if ok && current.SchemaVersion >= sourceRegistrySchemaVersion && seedChecksumFromAttrs(current.Attrs) == checksum {
			continue
		}

		next := seed.toRecord(checksum, now)
		if ok {
			next.Enabled = current.Enabled
			next.DisabledReason = current.DisabledReason
			next.DisabledAt = current.DisabledAt
			next.DisabledBy = current.DisabledBy
			next.ReviewStatus = fallbackString(current.ReviewStatus, next.ReviewStatus)
			next.ReviewNotes = fallbackString(current.ReviewNotes, next.ReviewNotes)
			next.Evidence = fallbackString(current.Evidence, next.Evidence)
			next.Attrs = mergeSourceAttrs(current.Attrs, checksum)
			version := maxUint64(current.Version, current.RecordVersion) + 1
			next.Version = version
			next.RecordVersion = version
			next.APIContractVersion = maxUint32(current.APIContractVersion, sourceRegistryAPIContractVersion)
		}

		records = append(records, next)
	}

	return records, nil
}

func normalizeSourceSeed(seed sourceSeed) (normalizedSourceSeed, error) {
	authConfigJSON, err := normalizeJSON(seed.AuthConfig)
	if err != nil {
		return normalizedSourceSeed{}, fmt.Errorf("normalize auth config for %s: %w", seed.SourceID, err)
	}

	requestsPerMinute := seed.RequestsPerMinute
	if requestsPerMinute <= 0 {
		requestsPerMinute = defaultRequestsPerMinute
	}

	burstSize := seed.BurstSize
	if burstSize <= 0 {
		burstSize = defaultBurstSize
	}

	backfillPriority := seed.BackfillPriority
	if backfillPriority <= 0 {
		backfillPriority = defaultBackfillPriority
	}

	retentionClass := strings.TrimSpace(seed.RetentionClass)
	if retentionClass == "" {
		retentionClass = defaultRetentionClass
	}

	initialReviewStatus := strings.TrimSpace(seed.ReviewStatus)
	if initialReviewStatus == "" {
		initialReviewStatus = defaultReviewStatus
	}

	return normalizedSourceSeed{
		SourceID:            strings.TrimSpace(seed.SourceID),
		Domain:              strings.TrimSpace(seed.Domain),
		DomainFamily:        strings.TrimSpace(seed.DomainFamily),
		SourceClass:         strings.TrimSpace(seed.SourceClass),
		Entrypoints:         cloneStrings(seed.Entrypoints),
		AuthMode:            strings.TrimSpace(seed.AuthMode),
		AuthConfigJSON:      authConfigJSON,
		FormatHint:          strings.TrimSpace(seed.FormatHint),
		RobotsPolicy:        strings.TrimSpace(seed.RobotsPolicy),
		RefreshStrategy:     strings.TrimSpace(seed.RefreshStrategy),
		RequestsPerMinute:   uint32(requestsPerMinute),
		BurstSize:           uint16(burstSize),
		RetentionClass:      retentionClass,
		License:             strings.TrimSpace(seed.License),
		TermsURL:            strings.TrimSpace(seed.TermsURL),
		AttributionRequired: seed.AttributionRequired,
		GeoScope:            strings.TrimSpace(seed.GeoScope),
		Priority:            uint16(seed.Priority),
		ParserID:            strings.TrimSpace(seed.ParserID),
		EntityTypes:         cloneStrings(seed.EntityTypes),
		ExpectedPlaceTypes:  cloneStrings(seed.ExpectedPlaceTypes),
		SupportsHistorical:  seed.SupportsHistorical,
		SupportsDelta:       seed.SupportsDelta,
		BackfillPriority:    uint16(backfillPriority),
		ConfidenceBaseline:  float32(seed.ConfidenceBaseline),
		InitialReviewStatus: initialReviewStatus,
		InitialReviewNotes:  strings.TrimSpace(seed.ReviewNotes),
	}, nil
}

func sourceSeedFingerprint(seed normalizedSourceSeed) (string, error) {
	b, err := json.Marshal(seed)
	if err != nil {
		return "", err
	}
	return sum(b), nil
}

func (seed normalizedSourceSeed) toRecord(checksum string, now time.Time) sourceRegistryRecord {
	return sourceRegistryRecord{
		SourceID:            seed.SourceID,
		Domain:              seed.Domain,
		DomainFamily:        seed.DomainFamily,
		SourceClass:         seed.SourceClass,
		Entrypoints:         seed.Entrypoints,
		AuthMode:            seed.AuthMode,
		AuthConfigJSON:      seed.AuthConfigJSON,
		FormatHint:          seed.FormatHint,
		RobotsPolicy:        seed.RobotsPolicy,
		RefreshStrategy:     seed.RefreshStrategy,
		RequestsPerMinute:   seed.RequestsPerMinute,
		BurstSize:           seed.BurstSize,
		RetentionClass:      seed.RetentionClass,
		License:             seed.License,
		TermsURL:            seed.TermsURL,
		AttributionRequired: uint8(btoi(seed.AttributionRequired)),
		GeoScope:            seed.GeoScope,
		Priority:            seed.Priority,
		ParserID:            seed.ParserID,
		EntityTypes:         seed.EntityTypes,
		ExpectedPlaceTypes:  seed.ExpectedPlaceTypes,
		SupportsHistorical:  uint8(btoi(seed.SupportsHistorical)),
		SupportsDelta:       uint8(btoi(seed.SupportsDelta)),
		BackfillPriority:    seed.BackfillPriority,
		ConfidenceBaseline:  seed.ConfidenceBaseline,
		Enabled:             1,
		ReviewStatus:        seed.InitialReviewStatus,
		ReviewNotes:         seed.InitialReviewNotes,
		Version:             1,
		SchemaVersion:       sourceRegistrySchemaVersion,
		RecordVersion:       1,
		APIContractVersion:  sourceRegistryAPIContractVersion,
		Attrs:               mergeSourceAttrs("{}", checksum),
		Evidence:            "[]",
		UpdatedAt:           formatClickHouseTime(now),
	}
}

func (record sourceRegistryRecord) CanFetch() error {
	if record.Enabled == 0 {
		reason := "source disabled"
		if record.DisabledReason != nil && strings.TrimSpace(*record.DisabledReason) != "" {
			reason = strings.TrimSpace(*record.DisabledReason)
		}
		return fmt.Errorf("source %s fetch blocked: %s", record.SourceID, reason)
	}
	return nil
}

func (record sourceRegistryRecord) Disable(reason, by string, at time.Time) sourceRegistryRecord {
	next := record
	next.Enabled = 0
	next.DisabledReason = stringPtr(reason)
	next.DisabledBy = stringPtr(by)
	disabledAt := formatClickHouseTime(at)
	next.DisabledAt = &disabledAt
	next.bumpVersion(at)
	return next
}

func (record sourceRegistryRecord) Reenable(note, by string, at time.Time) sourceRegistryRecord {
	next := record
	next.Enabled = 1
	next.DisabledReason = nil
	next.DisabledAt = nil
	next.DisabledBy = nil
	if strings.TrimSpace(note) != "" {
		next.ReviewNotes = strings.TrimSpace(note)
	}
	if strings.TrimSpace(by) != "" {
		next.ReviewStatus = fallbackString(next.ReviewStatus, defaultReviewStatus)
	}
	next.bumpVersion(at)
	return next
}

func (record sourceRegistryRecord) NewRateLimiter(now time.Time) *sourceRateLimiter {
	requestsPerMinute := int(record.RequestsPerMinute)
	if requestsPerMinute <= 0 {
		requestsPerMinute = defaultRequestsPerMinute
	}
	burstSize := int(record.BurstSize)
	if burstSize <= 0 {
		burstSize = defaultBurstSize
	}
	return newSourceRateLimiter(requestsPerMinute, burstSize, now)
}

func (record *sourceRegistryRecord) bumpVersion(at time.Time) {
	nextVersion := maxUint64(record.Version, record.RecordVersion) + 1
	record.Version = nextVersion
	record.RecordVersion = nextVersion
	record.SchemaVersion = maxUint32(record.SchemaVersion, sourceRegistrySchemaVersion)
	record.APIContractVersion = maxUint32(record.APIContractVersion, sourceRegistryAPIContractVersion)
	record.UpdatedAt = formatClickHouseTime(at)
	if record.Attrs == "" {
		record.Attrs = "{}"
	}
	if record.Evidence == "" {
		record.Evidence = "[]"
	}
}

func newSourceRateLimiter(requestsPerMinute, burstSize int, now time.Time) *sourceRateLimiter {
	if requestsPerMinute <= 0 {
		requestsPerMinute = defaultRequestsPerMinute
	}
	if burstSize <= 0 {
		burstSize = defaultBurstSize
	}
	return &sourceRateLimiter{
		tokens:          float64(burstSize),
		burst:           float64(burstSize),
		refillPerSecond: float64(requestsPerMinute) / 60.0,
		lastRefill:      now,
	}
}

func (limiter *sourceRateLimiter) Allow(at time.Time) bool {
	if limiter == nil {
		return true
	}
	if limiter.lastRefill.IsZero() {
		limiter.lastRefill = at
	}
	if at.Before(limiter.lastRefill) {
		at = limiter.lastRefill
	}
	elapsed := at.Sub(limiter.lastRefill).Seconds()
	if elapsed > 0 {
		limiter.tokens += elapsed * limiter.refillPerSecond
		if limiter.tokens > limiter.burst {
			limiter.tokens = limiter.burst
		}
		limiter.lastRefill = at
	}
	if limiter.tokens < 1 {
		return false
	}
	limiter.tokens--
	return true
}

func insertSourceRegistryRecordSQL(record sourceRegistryRecord) string {
	return fmt.Sprintf(`INSERT INTO meta.source_registry
	(source_id, domain, domain_family, source_class, entrypoints, auth_mode, auth_config_json, format_hint, robots_policy, refresh_strategy, requests_per_minute, burst_size, retention_class, license, terms_url, attribution_required, geo_scope, priority, parser_id, entity_types, expected_place_types, supports_historical, supports_delta, backfill_priority, confidence_baseline, enabled, disabled_reason, disabled_at, disabled_by, review_status, review_notes, version, schema_version, record_version, api_contract_version, attrs, evidence, updated_at)
	VALUES ('%s','%s','%s','%s',%s,'%s','%s','%s','%s','%s',%s,%s,'%s','%s','%s',%d,'%s',%s,'%s',%s,%s,%d,%d,%s,%s,%d,%s,%s,%s,'%s','%s',%s,%d,%s,%d,'%s','%s','%s')`,
		esc(record.SourceID),
		esc(record.Domain),
		esc(record.DomainFamily),
		esc(record.SourceClass),
		arr(record.Entrypoints),
		esc(record.AuthMode),
		esc(record.AuthConfigJSON),
		esc(record.FormatHint),
		esc(record.RobotsPolicy),
		esc(record.RefreshStrategy),
		strconv.FormatUint(uint64(record.RequestsPerMinute), 10),
		strconv.FormatUint(uint64(record.BurstSize), 10),
		esc(record.RetentionClass),
		esc(record.License),
		esc(record.TermsURL),
		record.AttributionRequired,
		esc(record.GeoScope),
		strconv.FormatUint(uint64(record.Priority), 10),
		esc(record.ParserID),
		arr(record.EntityTypes),
		arr(record.ExpectedPlaceTypes),
		record.SupportsHistorical,
		record.SupportsDelta,
		strconv.FormatUint(uint64(record.BackfillPriority), 10),
		strconv.FormatFloat(float64(record.ConfidenceBaseline), 'g', -1, 32),
		record.Enabled,
		sqlNullableString(record.DisabledReason),
		sqlNullableDateTime(record.DisabledAt),
		sqlNullableString(record.DisabledBy),
		esc(record.ReviewStatus),
		esc(record.ReviewNotes),
		strconv.FormatUint(record.Version, 10),
		record.SchemaVersion,
		strconv.FormatUint(record.RecordVersion, 10),
		record.APIContractVersion,
		esc(fallbackString(record.Attrs, "{}")),
		esc(fallbackString(record.Evidence, "[]")),
		esc(fallbackString(record.UpdatedAt, formatClickHouseTime(time.Now().UTC()))),
	)
}

func seedChecksumFromAttrs(attrs string) string {
	decoded, ok := decodeJSONObject(attrs)
	if !ok {
		return ""
	}
	checksum, _ := decoded["seed_checksum"].(string)
	return checksum
}

func mergeSourceAttrs(existingAttrs, checksum string) string {
	attrs, ok := decodeJSONObject(existingAttrs)
	if !ok {
		attrs = map[string]any{}
	}
	attrs["seed_checksum"] = checksum
	b, err := json.Marshal(attrs)
	if err != nil {
		return fmt.Sprintf(`{"seed_checksum":%q}`, checksum)
	}
	return string(b)
}

func normalizeJSON(v any) (string, error) {
	if v == nil {
		return "{}", nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	if string(b) == "null" {
		return "{}", nil
	}
	return string(b), nil
}

func decodeSourceRegistryRecord(line string) (sourceRegistryRecord, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal([]byte(line), &raw); err != nil {
		return sourceRegistryRecord{}, err
	}

	var record sourceRegistryRecord
	var err error
	if record.SourceID, err = decodeJSONString(raw["source_id"]); err != nil {
		return sourceRegistryRecord{}, err
	}
	if record.Domain, err = decodeJSONString(raw["domain"]); err != nil {
		return sourceRegistryRecord{}, err
	}
	if record.DomainFamily, err = decodeJSONString(raw["domain_family"]); err != nil {
		return sourceRegistryRecord{}, err
	}
	if record.SourceClass, err = decodeJSONString(raw["source_class"]); err != nil {
		return sourceRegistryRecord{}, err
	}
	if record.Entrypoints, err = decodeStringSlice(raw["entrypoints"]); err != nil {
		return sourceRegistryRecord{}, err
	}
	if record.AuthMode, err = decodeJSONString(raw["auth_mode"]); err != nil {
		return sourceRegistryRecord{}, err
	}
	if record.AuthConfigJSON, err = decodeJSONString(raw["auth_config_json"]); err != nil {
		return sourceRegistryRecord{}, err
	}
	if record.FormatHint, err = decodeJSONString(raw["format_hint"]); err != nil {
		return sourceRegistryRecord{}, err
	}
	if record.RobotsPolicy, err = decodeJSONString(raw["robots_policy"]); err != nil {
		return sourceRegistryRecord{}, err
	}
	if record.RefreshStrategy, err = decodeJSONString(raw["refresh_strategy"]); err != nil {
		return sourceRegistryRecord{}, err
	}
	if record.RequestsPerMinute, err = decodeUint32(raw["requests_per_minute"]); err != nil {
		return sourceRegistryRecord{}, err
	}
	if record.BurstSize, err = decodeUint16(raw["burst_size"]); err != nil {
		return sourceRegistryRecord{}, err
	}
	if record.RetentionClass, err = decodeJSONString(raw["retention_class"]); err != nil {
		return sourceRegistryRecord{}, err
	}
	if record.License, err = decodeJSONString(raw["license"]); err != nil {
		return sourceRegistryRecord{}, err
	}
	if record.TermsURL, err = decodeJSONString(raw["terms_url"]); err != nil {
		return sourceRegistryRecord{}, err
	}
	if record.AttributionRequired, err = decodeUint8(raw["attribution_required"]); err != nil {
		return sourceRegistryRecord{}, err
	}
	if record.GeoScope, err = decodeJSONString(raw["geo_scope"]); err != nil {
		return sourceRegistryRecord{}, err
	}
	if record.Priority, err = decodeUint16(raw["priority"]); err != nil {
		return sourceRegistryRecord{}, err
	}
	if record.ParserID, err = decodeJSONString(raw["parser_id"]); err != nil {
		return sourceRegistryRecord{}, err
	}
	if record.EntityTypes, err = decodeStringSlice(raw["entity_types"]); err != nil {
		return sourceRegistryRecord{}, err
	}
	if record.ExpectedPlaceTypes, err = decodeStringSlice(raw["expected_place_types"]); err != nil {
		return sourceRegistryRecord{}, err
	}
	if record.SupportsHistorical, err = decodeUint8(raw["supports_historical"]); err != nil {
		return sourceRegistryRecord{}, err
	}
	if record.SupportsDelta, err = decodeUint8(raw["supports_delta"]); err != nil {
		return sourceRegistryRecord{}, err
	}
	if record.BackfillPriority, err = decodeUint16(raw["backfill_priority"]); err != nil {
		return sourceRegistryRecord{}, err
	}
	if record.ConfidenceBaseline, err = decodeFloat32(raw["confidence_baseline"]); err != nil {
		return sourceRegistryRecord{}, err
	}
	if record.Enabled, err = decodeUint8(raw["enabled"]); err != nil {
		return sourceRegistryRecord{}, err
	}
	if record.DisabledReason, err = decodeNullableString(raw["disabled_reason"]); err != nil {
		return sourceRegistryRecord{}, err
	}
	if record.DisabledAt, err = decodeNullableString(raw["disabled_at"]); err != nil {
		return sourceRegistryRecord{}, err
	}
	if record.DisabledBy, err = decodeNullableString(raw["disabled_by"]); err != nil {
		return sourceRegistryRecord{}, err
	}
	if record.ReviewStatus, err = decodeJSONString(raw["review_status"]); err != nil {
		return sourceRegistryRecord{}, err
	}
	if record.ReviewNotes, err = decodeJSONString(raw["review_notes"]); err != nil {
		return sourceRegistryRecord{}, err
	}
	if record.Version, err = decodeUint64(raw["version"]); err != nil {
		return sourceRegistryRecord{}, err
	}
	if record.SchemaVersion, err = decodeUint32(raw["schema_version"]); err != nil {
		return sourceRegistryRecord{}, err
	}
	if record.RecordVersion, err = decodeUint64(raw["record_version"]); err != nil {
		return sourceRegistryRecord{}, err
	}
	if record.APIContractVersion, err = decodeUint32(raw["api_contract_version"]); err != nil {
		return sourceRegistryRecord{}, err
	}
	if record.Attrs, err = decodeJSONString(raw["attrs"]); err != nil {
		return sourceRegistryRecord{}, err
	}
	if record.Evidence, err = decodeJSONString(raw["evidence"]); err != nil {
		return sourceRegistryRecord{}, err
	}
	if record.UpdatedAt, err = decodeJSONString(raw["updated_at"]); err != nil {
		return sourceRegistryRecord{}, err
	}

	return record, nil
}

func decodeJSONString(raw json.RawMessage) (string, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return "", nil
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", err
	}
	return value, nil
}

func decodeStringSlice(raw json.RawMessage) ([]string, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return []string{}, nil
	}
	var value []string
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, err
	}
	if value == nil {
		return []string{}, nil
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
	value, err := decodeNumericString(raw)
	if err != nil {
		return 0, err
	}
	if value == "" {
		return 0, nil
	}
	return strconv.ParseUint(value, 10, 64)
}

func decodeUint32(raw json.RawMessage) (uint32, error) {
	value, err := decodeNumericString(raw)
	if err != nil {
		return 0, err
	}
	if value == "" {
		return 0, nil
	}
	parsed, err := strconv.ParseUint(value, 10, 32)
	return uint32(parsed), err
}

func decodeUint16(raw json.RawMessage) (uint16, error) {
	value, err := decodeNumericString(raw)
	if err != nil {
		return 0, err
	}
	if value == "" {
		return 0, nil
	}
	parsed, err := strconv.ParseUint(value, 10, 16)
	return uint16(parsed), err
}

func decodeUint8(raw json.RawMessage) (uint8, error) {
	value, err := decodeNumericString(raw)
	if err != nil {
		return 0, err
	}
	if value == "" {
		return 0, nil
	}
	parsed, err := strconv.ParseUint(value, 10, 8)
	return uint8(parsed), err
}

func decodeFloat32(raw json.RawMessage) (float32, error) {
	value, err := decodeNumericString(raw)
	if err != nil {
		return 0, err
	}
	if value == "" {
		return 0, nil
	}
	parsed, err := strconv.ParseFloat(value, 32)
	return float32(parsed), err
}

func decodeNumericString(raw json.RawMessage) (string, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return "", nil
	}
	var number json.Number
	if err := json.Unmarshal(raw, &number); err == nil {
		return number.String(), nil
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", err
	}
	return value, nil
}

func decodeJSONObject(raw string) (map[string]any, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return map[string]any{}, true
	}
	var decoded map[string]any
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		return nil, false
	}
	if decoded == nil {
		decoded = map[string]any{}
	}
	return decoded, true
}

func cloneStrings(items []string) []string {
	if len(items) == 0 {
		return []string{}
	}
	cloned := make([]string, 0, len(items))
	for _, item := range items {
		cloned = append(cloned, strings.TrimSpace(item))
	}
	return cloned
}

func fallbackString(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

func sqlNullableString(value *string) string {
	if value == nil || strings.TrimSpace(*value) == "" {
		return "NULL"
	}
	return fmt.Sprintf("'%s'", esc(*value))
}

func sqlNullableDateTime(value *string) string {
	if value == nil || strings.TrimSpace(*value) == "" {
		return "NULL"
	}
	return fmt.Sprintf("'%s'", esc(*value))
}

func stringPtr(value string) *string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil
	}
	return &trimmed
}

func formatClickHouseTime(at time.Time) string {
	return at.UTC().Format("2006-01-02 15:04:05.000")
}

func maxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func maxUint32(a, b uint32) uint32 {
	if a > b {
		return a
	}
	return b
}
