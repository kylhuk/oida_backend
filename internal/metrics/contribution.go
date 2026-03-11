package metrics

import (
	"fmt"
	"math"
	"sort"
	"time"

	"global-osint-backend/internal/canonical"
)

const worldPlaceID = "plc:world"

type InputRecord struct {
	RecordID              string
	RecordType            string
	EntityID              string
	PlaceID               string
	Admin0PlaceID         string
	ContinentPlaceID      string
	SourceID              string
	OccurredAt            time.Time
	PublishedAt           *time.Time
	GeolocationSucceeded  bool
	Deduplicated          bool
	SchemaDriftDetected   bool
	ConfirmingSourceCount int
	Confidence            float64
	EvidenceCount         int
	BurstScore            float64
	RiskScore             float64
	Acceleration7dVs30d   float64
	AnomalyZScore30d      float64
	Evidence              []canonical.Evidence
}

type Contribution struct {
	ContributionID     string               `json:"contribution_id"`
	MetricID           string               `json:"metric_id"`
	SubjectGrain       string               `json:"subject_grain"`
	SubjectID          string               `json:"subject_id"`
	SourceRecordType   string               `json:"source_record_type"`
	SourceRecordID     string               `json:"source_record_id"`
	SourceID           string               `json:"source_id"`
	PlaceID            string               `json:"place_id"`
	WindowGrain        string               `json:"window_grain"`
	WindowStart        time.Time            `json:"window_start"`
	WindowEnd          time.Time            `json:"window_end"`
	MaterializationKey string               `json:"materialization_key"`
	ContributionType   string               `json:"contribution_type"`
	ContributionValue  float64              `json:"contribution_value"`
	ContributionWeight float64              `json:"contribution_weight"`
	SchemaVersion      uint32               `json:"schema_version"`
	Attrs              map[string]any       `json:"attrs"`
	Evidence           []canonical.Evidence `json:"evidence"`
}

type subjectScope struct {
	grain string
	id    string
}

type emissionContext struct {
	seenDistinct map[string]struct{}
}

func EmitCoreMetricContributions(records []InputRecord) []Contribution {
	contributions := make([]Contribution, 0, len(records)*16)
	ctx := emissionContext{seenDistinct: map[string]struct{}{}}
	for _, record := range records {
		record.OccurredAt = record.OccurredAt.UTC().Truncate(time.Millisecond)
		for _, scope := range scopesForRecord(record) {
			placeID := scope.id
			contributions = append(contributions, emitActivityContributions(&ctx, record, scope, placeID)...)
			contributions = append(contributions, emitQualityContributions(&ctx, record, scope, placeID)...)
			contributions = append(contributions, emitRiskContributions(record, scope, placeID)...)
		}
	}
	sort.Slice(contributions, func(i, j int) bool {
		if contributions[i].ContributionID != contributions[j].ContributionID {
			return contributions[i].ContributionID < contributions[j].ContributionID
		}
		if contributions[i].MetricID != contributions[j].MetricID {
			return contributions[i].MetricID < contributions[j].MetricID
		}
		return contributions[i].SubjectID < contributions[j].SubjectID
	})
	return contributions
}

func emitActivityContributions(ctx *emissionContext, record InputRecord, scope subjectScope, placeID string) []Contribution {
	dayStart, dayEnd := dayWindow(record.OccurredAt)
	rollingDayStart, rollingDayEnd := rolling24HourWindow(record.OccurredAt)
	weekStart, weekEnd := rollingWeekWindow(record.OccurredAt)
	monthStart, monthEnd := rolling30DayWindow(record.OccurredAt)
	confidence := sanitizeWeight(record.Confidence)
	activeRecord := record.RecordType == "observation" || record.RecordType == "event"
	var out []Contribution
	if record.RecordType == "observation" {
		out = append(out, newContribution(record, scope, placeID, "obs_count", "count", "day", dayStart, dayEnd, 1, 1))
	}
	if record.RecordType == "event" {
		out = append(out, newContribution(record, scope, placeID, "event_count", "count", "day", dayStart, dayEnd, 1, 1))
	}
	if activeRecord {
		out = append(out,
			newContribution(record, scope, placeID, "confidence_weighted_activity", "weighted_activity", "day", dayStart, dayEnd, confidence, 1),
			newContribution(record, scope, placeID, "source_diversity_score", "distinct_source", "day", dayStart, dayEnd, 1, 1),
			newContribution(record, scope, placeID, "trend_24h", "activity", "24h", rollingDayStart, rollingDayEnd, 1, 1),
			newContribution(record, scope, placeID, "trend_7d", "activity", "7d", weekStart, weekEnd, 1, 1),
			newContribution(record, scope, placeID, "acceleration_7d_vs_30d", "trend_acceleration", "30d", monthStart, monthEnd, record.Acceleration7dVs30d, 1),
		)
		if ctx.markDistinctMetric("entity_count_approx", scope, "day", dayStart, entityKey(record)) {
			out = append(out, newContribution(record, scope, placeID, "entity_count_approx", "approx_distinct_entity", "day", dayStart, dayEnd, 1, 1))
		}
	}
	return out
}

func emitQualityContributions(ctx *emissionContext, record InputRecord, scope subjectScope, placeID string) []Contribution {
	dayStart, dayEnd := dayWindow(record.OccurredAt)
	out := []Contribution{
		newContribution(record, scope, placeID, "geolocation_success_rate", "ratio_component", "day", dayStart, dayEnd, boolToFloat(record.GeolocationSucceeded), 1),
		newContribution(record, scope, placeID, "evidence_density", "evidence_density", "day", dayStart, dayEnd, float64(effectiveEvidenceCount(record)), 1),
		newContribution(record, scope, placeID, "dedup_rate", "ratio_component", "day", dayStart, dayEnd, boolToFloat(record.Deduplicated), 1),
		newContribution(record, scope, placeID, "schema_drift_rate", "ratio_component", "day", dayStart, dayEnd, boolToFloat(record.SchemaDriftDetected), 1),
		newContribution(record, scope, placeID, "cross_source_confirmation_rate", "ratio_component", "day", dayStart, dayEnd, boolToFloat(record.ConfirmingSourceCount > 1), 1),
	}
	if ctx.markDistinctMetric("source_count_approx", scope, "day", dayStart, record.SourceID) {
		out = append(out, newContribution(record, scope, placeID, "source_count_approx", "approx_distinct_source", "day", dayStart, dayEnd, 1, 1))
	}
	if record.PublishedAt != nil {
		lag := record.OccurredAt.Sub(record.PublishedAt.UTC()).Minutes()
		if lag < 0 {
			lag = 0
		}
		out = append(out, newContribution(record, scope, placeID, "freshness_lag_minutes", "lag_minutes", "day", dayStart, dayEnd, lag, 1))
	}
	return out
}

func emitRiskContributions(record InputRecord, scope subjectScope, placeID string) []Contribution {
	dayStart, dayEnd := dayWindow(record.OccurredAt)
	monthStart, monthEnd := rolling30DayWindow(record.OccurredAt)
	confidence := sanitizeWeight(record.Confidence)
	if confidence == 0 {
		confidence = 1
	}
	burst := record.BurstScore
	if burst <= 0 {
		burst = confidence
	}
	risk := record.RiskScore
	if risk < 0 {
		risk = 0
	}
	return []Contribution{
		newContribution(record, scope, placeID, "burst_score", "weighted_signal", "day", dayStart, dayEnd, burst*confidence, confidence),
		newContribution(record, scope, placeID, "risk_composite_global", "weighted_signal", "day", dayStart, dayEnd, risk*confidence, confidence),
		newContribution(record, scope, placeID, "anomaly_zscore_30d", "anomaly_zscore", "30d", monthStart, monthEnd, record.AnomalyZScore30d, 1),
	}
}

func newContribution(record InputRecord, scope subjectScope, placeID, metricID, contributionType, windowGrain string, windowStart, windowEnd time.Time, value, weight float64) Contribution {
	confidence := sanitizeWeight(record.Confidence)
	features := []map[string]any{{
		"feature": "record_type",
		"value":   record.RecordType,
		"weight":  confidence,
	}}
	if metricID == "source_diversity_score" {
		features = append(features, map[string]any{"feature": "source_id", "value": record.SourceID, "weight": 1.0})
	}
	if metricID == "geolocation_success_rate" {
		features = append(features, map[string]any{"feature": "geolocation_succeeded", "value": record.GeolocationSucceeded, "weight": 1.0})
	}
	return Contribution{
		ContributionID:     fmt.Sprintf("mc:%s:%s:%s:%s:%d", metricID, scope.grain, scope.id, record.RecordID, windowStart.Unix()),
		MetricID:           metricID,
		SubjectGrain:       scope.grain,
		SubjectID:          scope.id,
		SourceRecordType:   record.RecordType,
		SourceRecordID:     record.RecordID,
		SourceID:           record.SourceID,
		PlaceID:            placeID,
		WindowGrain:        windowGrain,
		WindowStart:        windowStart,
		WindowEnd:          windowEnd,
		MaterializationKey: materializationKey(metricID, scope.grain, scope.id, placeID, windowGrain, windowStart),
		ContributionType:   contributionType,
		ContributionValue:  roundMetric(value),
		ContributionWeight: roundMetric(weight),
		SchemaVersion:      SchemaVersion,
		Attrs: map[string]any{
			"source_id":      record.SourceID,
			"confidence":     roundMetric(confidence),
			"evidence_count": record.EvidenceCount,
			"explainability": map[string]any{
				"confidence":            roundMetric(confidence),
				"feature_contributions": features,
				"evidence_refs":         evidenceRefs(record.Evidence),
			},
		},
		Evidence: append([]canonical.Evidence(nil), record.Evidence...),
	}
}

func scopesForRecord(record InputRecord) []subjectScope {
	seen := map[string]struct{}{}
	add := func(scopes *[]subjectScope, grain, id string) {
		if id == "" {
			return
		}
		key := grain + ":" + id
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		*scopes = append(*scopes, subjectScope{grain: grain, id: id})
	}
	var scopes []subjectScope
	add(&scopes, "place", record.PlaceID)
	add(&scopes, "admin0", record.Admin0PlaceID)
	add(&scopes, "continent", record.ContinentPlaceID)
	add(&scopes, "world", worldPlaceID)
	return scopes
}

func dayWindow(ts time.Time) (time.Time, time.Time) {
	start := time.Date(ts.Year(), ts.Month(), ts.Day(), 0, 0, 0, 0, time.UTC)
	return start, start.Add(24 * time.Hour)
}

func rollingWeekWindow(ts time.Time) (time.Time, time.Time) {
	end := time.Date(ts.Year(), ts.Month(), ts.Day(), 0, 0, 0, 0, time.UTC).Add(24 * time.Hour)
	return end.Add(-7 * 24 * time.Hour), end
}

func rolling24HourWindow(ts time.Time) (time.Time, time.Time) {
	end := ts.UTC().Truncate(time.Hour).Add(time.Hour)
	return end.Add(-24 * time.Hour), end
}

func rolling30DayWindow(ts time.Time) (time.Time, time.Time) {
	end := time.Date(ts.Year(), ts.Month(), ts.Day(), 0, 0, 0, 0, time.UTC).Add(24 * time.Hour)
	return end.Add(-30 * 24 * time.Hour), end
}

func evidenceRefs(evidence []canonical.Evidence) []string {
	refs := make([]string, 0, len(evidence))
	for _, item := range evidence {
		ref := item.Ref
		if ref == "" {
			ref = item.RawID
		}
		if ref == "" {
			continue
		}
		refs = append(refs, ref)
	}
	return refs
}

func sanitizeWeight(v float64) float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) || v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}

func effectiveEvidenceCount(record InputRecord) int {
	if record.EvidenceCount > 0 {
		return record.EvidenceCount
	}
	return len(record.Evidence)
}

func entityKey(record InputRecord) string {
	if record.EntityID != "" {
		return record.EntityID
	}
	if record.RecordID != "" {
		return record.RecordID
	}
	return record.SourceID
}

func (ctx *emissionContext) markDistinctMetric(metricID string, scope subjectScope, windowGrain string, windowStart time.Time, distinctKey string) bool {
	if ctx == nil || distinctKey == "" {
		return false
	}
	key := fmt.Sprintf("%s:%s:%s:%s:%d:%s", metricID, scope.grain, scope.id, windowGrain, windowStart.UnixMilli(), distinctKey)
	if _, ok := ctx.seenDistinct[key]; ok {
		return false
	}
	ctx.seenDistinct[key] = struct{}{}
	return true
}

func roundMetric(v float64) float64 {
	return math.Round(v*10000) / 10000
}

func boolToFloat(v bool) float64 {
	if v {
		return 1
	}
	return 0
}
