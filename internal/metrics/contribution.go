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
	RecordID             string
	RecordType           string
	PlaceID              string
	Admin0PlaceID        string
	ContinentPlaceID     string
	SourceID             string
	OccurredAt           time.Time
	PublishedAt          *time.Time
	GeolocationSucceeded bool
	Confidence           float64
	EvidenceCount        int
	BurstScore           float64
	RiskScore            float64
	Evidence             []canonical.Evidence
}

type Contribution struct {
	ContributionID     string               `json:"contribution_id"`
	MetricID           string               `json:"metric_id"`
	SubjectGrain       string               `json:"subject_grain"`
	SubjectID          string               `json:"subject_id"`
	SourceRecordType   string               `json:"source_record_type"`
	SourceRecordID     string               `json:"source_record_id"`
	PlaceID            string               `json:"place_id"`
	WindowGrain        string               `json:"window_grain"`
	WindowStart        time.Time            `json:"window_start"`
	WindowEnd          time.Time            `json:"window_end"`
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

func EmitCoreMetricContributions(records []InputRecord) []Contribution {
	contributions := make([]Contribution, 0, len(records)*16)
	for _, record := range records {
		record.OccurredAt = record.OccurredAt.UTC().Truncate(time.Millisecond)
		for _, scope := range scopesForRecord(record) {
			placeID := scope.id
			contributions = append(contributions, emitActivityContributions(record, scope, placeID)...)
			contributions = append(contributions, emitQualityContributions(record, scope, placeID)...)
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

func emitActivityContributions(record InputRecord, scope subjectScope, placeID string) []Contribution {
	dayStart, dayEnd := dayWindow(record.OccurredAt)
	weekStart, weekEnd := rollingWeekWindow(record.OccurredAt)
	var out []Contribution
	if record.RecordType == "observation" {
		out = append(out, newContribution(record, scope, placeID, "obs_count", "count", "day", dayStart, dayEnd, 1, 1))
	}
	if record.RecordType == "event" {
		out = append(out, newContribution(record, scope, placeID, "event_count", "count", "day", dayStart, dayEnd, 1, 1))
	}
	if record.RecordType == "observation" || record.RecordType == "event" {
		out = append(out,
			newContribution(record, scope, placeID, "source_diversity_score", "distinct_source", "day", dayStart, dayEnd, 1, 1),
			newContribution(record, scope, placeID, "trend_7d", "activity", "7d", weekStart, weekEnd, 1, 1),
		)
	}
	return out
}

func emitQualityContributions(record InputRecord, scope subjectScope, placeID string) []Contribution {
	dayStart, dayEnd := dayWindow(record.OccurredAt)
	out := []Contribution{
		newContribution(record, scope, placeID, "geolocation_success_rate", "ratio_component", "day", dayStart, dayEnd, boolToFloat(record.GeolocationSucceeded), 1),
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
		PlaceID:            placeID,
		WindowGrain:        windowGrain,
		WindowStart:        windowStart,
		WindowEnd:          windowEnd,
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

func roundMetric(v float64) float64 {
	return math.Round(v*10000) / 10000
}

func boolToFloat(v bool) float64 {
	if v {
		return 1
	}
	return 0
}
