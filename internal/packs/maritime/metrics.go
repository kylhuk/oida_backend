package maritime

import (
	"math"
	"sort"
	"strings"
	"time"

	"global-osint-backend/internal/canonical"
	coremetrics "global-osint-backend/internal/metrics"
)

type MetricReading struct {
	MetricID      string               `json:"metric_id"`
	MetricFamily  string               `json:"metric_family"`
	SubjectGrain  string               `json:"subject_grain"`
	SubjectID     string               `json:"subject_id"`
	WindowGrain   string               `json:"window_grain"`
	CalculatedAt  time.Time            `json:"calculated_at"`
	MetricValue   float64              `json:"metric_value"`
	Unit          string               `json:"unit"`
	SchemaVersion uint32               `json:"schema_version"`
	Attrs         map[string]any       `json:"attrs,omitempty"`
	Evidence      []canonical.Evidence `json:"evidence,omitempty"`
}

type ShadowFleetSignals struct {
	AISDarkHours         float64
	AISGapFrequency      int
	FlagChanges90d       int
	OwnershipChanges180d int
	SanctionsExposure    float64
	HighRiskPortCalls    int
	STSSuspicionScore    float64
	RouteDeviationScore  float64
	VesselAgeYears       int
	Evidence             []canonical.Evidence
}

type FlagRegistrySignals struct {
	RegistryFlagState string
	ObservedFlagState string
	FlagChanges90d    int
	Evidence          []canonical.Evidence
}

func MetricDefinitions() []coremetrics.MetricDefinition {
	definitions := []coremetrics.MetricDefinition{
		{
			MetricID:       "ais_dark_hours_sum",
			MetricFamily:   domainFamily,
			SubjectGrain:   "entity",
			Unit:           "hours",
			ValueType:      "gauge",
			RollupEngine:   "AggregatingMergeTree",
			RollupRule:     "sum",
			RefreshCadence: "15 MINUTE",
			Description:    "Summed AIS silence duration from closed dark-activity gaps for a vessel.",
			Formula:        "sum(dateDiff('minute', gap_start, gap_end)) / 60.0",
			Windows:        []string{"day", "7d"},
		},
		{
			MetricID:       "anchorage_dwell_hours",
			MetricFamily:   domainFamily,
			SubjectGrain:   "entity",
			Unit:           "hours",
			ValueType:      "gauge",
			RollupEngine:   "AggregatingMergeTree",
			RollupRule:     "sum",
			RefreshCadence: "1 HOUR",
			Description:    "Summed dwell duration from anchorage-class port calls tied to a vessel.",
			Formula:        "sum(dateDiff('minute', port_call_start, port_call_end)) / 60.0 where anchorage_match = 1",
			Windows:        []string{"day"},
		},
		{
			MetricID:       "flag_registry_mismatch_score",
			MetricFamily:   domainFamily,
			SubjectGrain:   "entity",
			Unit:           "score",
			ValueType:      "ratio",
			RollupEngine:   "AggregatingMergeTree",
			RollupRule:     "weighted_avg",
			RefreshCadence: "1 HOUR",
			Description:    "Normalized inconsistency score for registry flag versus observed flag state, tempered by recent flag churn.",
			Formula:        "0.7 * flag_mismatch_indicator + 0.3 * min(flag_changes_90d / 3.0, 1)",
			Windows:        []string{"day"},
		},
		{
			MetricID:       "port_gap_hours",
			MetricFamily:   domainFamily,
			SubjectGrain:   "entity",
			Unit:           "hours",
			ValueType:      "gauge",
			RollupEngine:   "AggregatingMergeTree",
			RollupRule:     "sum",
			RefreshCadence: "1 HOUR",
			Description:    "Summed hours between a closed AIS gap and the next confirmed arrival at the expected port.",
			Formula:        "sum(dateDiff('minute', gap_end, port_call_start)) / 60.0 for matched gap-to-port transitions",
			Windows:        []string{"day"},
		},
		{
			MetricID:       "route_deviation_score",
			MetricFamily:   domainFamily,
			SubjectGrain:   "entity",
			Unit:           "score",
			ValueType:      "ratio",
			RollupEngine:   "AggregatingMergeTree",
			RollupRule:     "weighted_avg",
			RefreshCadence: "1 HOUR",
			Description:    "Normalized route deviation signal derived from vessel routing anomalies.",
			Formula:        "route_deviation_signal",
			Windows:        []string{"day"},
		},
		{
			MetricID:       "shadow_fleet_score",
			MetricFamily:   domainFamily,
			SubjectGrain:   "entity",
			Unit:           "score",
			ValueType:      "ratio",
			RollupEngine:   "AggregatingMergeTree",
			RollupRule:     "weighted_avg",
			RefreshCadence: "1 HOUR",
			Description:    "Composite vessel risk score combining AIS darkness, sanctions, ownership churn, and route anomalies.",
			Formula:        "0.22*dark_hours + 0.10*gap_freq + 0.12*flag_churn + 0.12*owner_churn + 0.18*sanctions + 0.10*high_risk_ports + 0.08*sts + 0.05*route_deviation + 0.03*age_profile",
			Windows:        []string{"day"},
		},
	}
	sort.Slice(definitions, func(i, j int) bool {
		return definitions[i].MetricID < definitions[j].MetricID
	})
	return definitions
}

func BuildMetricRegistryRecords(now time.Time) []coremetrics.RegistryRecord {
	now = now.UTC().Truncate(time.Millisecond)
	definitions := MetricDefinitions()
	records := make([]coremetrics.RegistryRecord, 0, len(definitions))
	for idx, def := range definitions {
		attrs := map[string]any{
			"description":     def.Description,
			"formula":         def.Formula,
			"refresh_cadence": def.RefreshCadence,
			"window_grains":   copyStringSlice(def.Windows),
			"domain_family":   domainFamily,
			"core_metric":     false,
			"explainability": map[string]any{
				"includes_confidence":            true,
				"includes_feature_contributions": true,
				"includes_evidence_refs":         true,
			},
		}
		if def.MetricID == "ais_dark_hours_sum" {
			attrs["compatibility_notes"] = []string{"metric_id renamed from ais_dark_hours; semantics unchanged"}
		}
		records = append(records, coremetrics.RegistryRecord{
			MetricID:     def.MetricID,
			MetricFamily: def.MetricFamily,
			SubjectGrain: def.SubjectGrain,
			Unit:         def.Unit,
			ValueType:    def.ValueType,
			RollupEngine: def.RollupEngine,
			RollupRule:   def.RollupRule,
			Attrs:        attrs,
			Evidence: []canonical.Evidence{{
				Kind:  "metric_spec",
				Ref:   def.MetricID,
				Value: def.Formula,
				Attrs: map[string]any{
					"domain_family": domainFamily,
					"window_grains": copyStringSlice(def.Windows),
				},
			}},
			SchemaVersion:      coremetrics.SchemaVersion,
			RecordVersion:      uint64(idx + 1),
			APIContractVersion: coremetrics.APIContractVersion,
			Enabled:            true,
			UpdatedAt:          now,
		})
	}
	return records
}

func AISDarkHours(subjectID string, gaps []AISGap, calculatedAt time.Time) MetricReading {
	return AISDarkHoursSum(subjectID, gaps, calculatedAt)
}

func AISDarkHoursSum(subjectID string, gaps []AISGap, calculatedAt time.Time) MetricReading {
	calculatedAt = calculatedAt.UTC().Truncate(time.Millisecond)
	features := make([]map[string]any, 0, len(gaps))
	evidence := []canonical.Evidence{{
		Kind:  "metric_formula",
		Ref:   "ais_dark_hours_sum",
		Value: "sum(ais_gap_hours)",
		Attrs: map[string]any{
			"compatibility_notes": []string{"metric_id renamed from ais_dark_hours; semantics unchanged"},
		},
	}}
	totalHours := 0.0
	longestGap := 0.0
	for _, gap := range gaps {
		hours := gap.DurationHours()
		if hours <= 0 {
			continue
		}
		totalHours += hours
		if hours > longestGap {
			longestGap = hours
		}
		gapEvent := gap.EventEnvelope()
		features = append(features, map[string]any{
			"feature": "ais_gap_hours",
			"ref":     firstNonEmpty(gapEvent.NativeID, gap.TrackID),
			"value":   hours,
			"weight":  1.0,
		})
		evidence = mergeEvidence(evidence, gapEvent.Evidence)
	}
	return MetricReading{
		MetricID:      "ais_dark_hours_sum",
		MetricFamily:  domainFamily,
		SubjectGrain:  "entity",
		SubjectID:     subjectID,
		WindowGrain:   "day",
		CalculatedAt:  calculatedAt,
		MetricValue:   roundMetric(totalHours),
		Unit:          "hours",
		SchemaVersion: schemaVersion,
		Attrs: map[string]any{
			"gap_count":           len(features),
			"longest_gap_hours":   roundMetric(longestGap),
			"compatibility_notes": []string{"metric_id renamed from ais_dark_hours; semantics unchanged"},
			"explainability": map[string]any{
				"feature_contributions": features,
				"evidence_refs":         evidenceRefs(evidence),
			},
		},
		Evidence: evidence,
	}
}

func RouteDeviationScore(subjectID string, score float64, evidence []canonical.Evidence, calculatedAt time.Time) MetricReading {
	calculatedAt = calculatedAt.UTC().Truncate(time.Millisecond)
	score = roundMetric(clamp01(score))
	evidence = mergeEvidence([]canonical.Evidence{{
		Kind:  "metric_formula",
		Ref:   "route_deviation_score",
		Value: "route_deviation_signal",
	}}, evidence)
	features := []map[string]any{{
		"feature": "route_deviation_signal",
		"value":   score,
		"weight":  1.0,
	}}
	return MetricReading{
		MetricID:      "route_deviation_score",
		MetricFamily:  domainFamily,
		SubjectGrain:  "entity",
		SubjectID:     subjectID,
		WindowGrain:   "day",
		CalculatedAt:  calculatedAt,
		MetricValue:   score,
		Unit:          "score",
		SchemaVersion: schemaVersion,
		Attrs: map[string]any{
			"raw_route_deviation_score": score,
			"explainability": map[string]any{
				"feature_contributions": features,
				"evidence_refs":         evidenceRefs(evidence),
			},
		},
		Evidence: evidence,
	}
}

func PortGapHours(subjectID string, gaps []AISGap, calls []PortCall, calculatedAt time.Time) MetricReading {
	calculatedAt = calculatedAt.UTC().Truncate(time.Millisecond)
	evidence := []canonical.Evidence{{
		Kind:  "metric_formula",
		Ref:   "port_gap_hours",
		Value: "sum(port_call_start - gap_end)",
	}}
	features := []map[string]any{}
	totalHours := 0.0
	for _, call := range calls {
		gap, gapHours, ok := matchPortGap(call, gaps)
		if !ok {
			continue
		}
		totalHours += gapHours
		callEvent := call.EventEnvelope()
		features = append(features, map[string]any{
			"feature":        "gap_to_port_transition_hours",
			"ref":            firstNonEmpty(callEvent.NativeID, call.PlaceID),
			"value":          roundMetric(gapHours),
			"gap_event_id":   firstNonEmpty(gap.EventEnvelope().NativeID, gap.TrackID),
			"next_port_id":   gap.NextKnownPortID,
			"port_call_type": firstNonEmpty(call.CallType, "turnaround"),
			"weight":         1.0,
		})
		evidence = mergeEvidence(evidence, gap.EventEnvelope().Evidence, callEvent.Evidence)
	}
	return MetricReading{
		MetricID:      "port_gap_hours",
		MetricFamily:  domainFamily,
		SubjectGrain:  "entity",
		SubjectID:     subjectID,
		WindowGrain:   "day",
		CalculatedAt:  calculatedAt,
		MetricValue:   roundMetric(totalHours),
		Unit:          "hours",
		SchemaVersion: schemaVersion,
		Attrs: map[string]any{
			"matched_port_calls": len(features),
			"explainability": map[string]any{
				"feature_contributions": features,
				"evidence_refs":         evidenceRefs(evidence),
			},
		},
		Evidence: evidence,
	}
}

func AnchorageDwellHours(subjectID string, calls []PortCall, calculatedAt time.Time) MetricReading {
	calculatedAt = calculatedAt.UTC().Truncate(time.Millisecond)
	evidence := []canonical.Evidence{{
		Kind:  "metric_formula",
		Ref:   "anchorage_dwell_hours",
		Value: "sum(port_call_dwell_hours where anchorage_match = 1)",
	}}
	features := []map[string]any{}
	totalHours := 0.0
	for _, call := range calls {
		if !isAnchorageCall(call) {
			continue
		}
		hours := roundMetric(durationHours(call.StartedAt, call.EndedAt))
		if hours <= 0 {
			continue
		}
		totalHours += hours
		callEvent := call.EventEnvelope()
		features = append(features, map[string]any{
			"feature":   "anchorage_port_call_hours",
			"ref":       firstNonEmpty(callEvent.NativeID, call.PlaceID),
			"value":     hours,
			"terminal":  call.Terminal,
			"port_name": call.PortName,
			"call_type": firstNonEmpty(call.CallType, "turnaround"),
			"weight":    1.0,
		})
		evidence = mergeEvidence(evidence, callEvent.Evidence)
	}
	return MetricReading{
		MetricID:      "anchorage_dwell_hours",
		MetricFamily:  domainFamily,
		SubjectGrain:  "entity",
		SubjectID:     subjectID,
		WindowGrain:   "day",
		CalculatedAt:  calculatedAt,
		MetricValue:   roundMetric(totalHours),
		Unit:          "hours",
		SchemaVersion: schemaVersion,
		Attrs: map[string]any{
			"anchorage_calls": len(features),
			"explainability": map[string]any{
				"feature_contributions": features,
				"evidence_refs":         evidenceRefs(evidence),
			},
		},
		Evidence: evidence,
	}
}

func FlagRegistryMismatchScore(subjectID string, signals FlagRegistrySignals, calculatedAt time.Time) MetricReading {
	calculatedAt = calculatedAt.UTC().Truncate(time.Millisecond)
	registryFlag := strings.ToUpper(strings.TrimSpace(signals.RegistryFlagState))
	observedFlag := strings.ToUpper(strings.TrimSpace(signals.ObservedFlagState))
	mismatch := 0.0
	if registryFlag != "" && observedFlag != "" && registryFlag != observedFlag {
		mismatch = 1.0
	}
	flagChurn := clamp01(float64(maxInt(signals.FlagChanges90d, 0)) / 3.0)
	score := roundMetric(clamp01((mismatch * 0.7) + (flagChurn * 0.3)))
	evidence := mergeEvidence([]canonical.Evidence{{
		Kind:  "metric_formula",
		Ref:   "flag_registry_mismatch_score",
		Value: "0.7*flag_mismatch_indicator + 0.3*normalized_flag_churn",
	}}, signals.Evidence)
	features := []map[string]any{
		{
			"feature": "flag_mismatch_indicator",
			"value":   mismatch,
			"weight":  0.7,
		},
		{
			"feature": "flag_changes_90d",
			"value":   float64(maxInt(signals.FlagChanges90d, 0)),
			"weight":  0.3,
		},
	}
	return MetricReading{
		MetricID:      "flag_registry_mismatch_score",
		MetricFamily:  domainFamily,
		SubjectGrain:  "entity",
		SubjectID:     subjectID,
		WindowGrain:   "day",
		CalculatedAt:  calculatedAt,
		MetricValue:   score,
		Unit:          "score",
		SchemaVersion: schemaVersion,
		Attrs: map[string]any{
			"registry_flag_state": registryFlag,
			"observed_flag_state": observedFlag,
			"flag_changes_90d":    maxInt(signals.FlagChanges90d, 0),
			"flag_mismatch":       mismatch > 0,
			"explainability": map[string]any{
				"feature_contributions": features,
				"evidence_refs":         evidenceRefs(evidence),
			},
		},
		Evidence: evidence,
	}
}

func ShadowFleetScore(subjectID string, signals ShadowFleetSignals, calculatedAt time.Time) MetricReading {
	calculatedAt = calculatedAt.UTC().Truncate(time.Millisecond)
	components := []struct {
		name       string
		raw        float64
		normalized float64
		weight     float64
	}{
		{name: "ais_dark_hours_sum", raw: roundMetric(signals.AISDarkHours), normalized: clamp01(signals.AISDarkHours / 24.0), weight: 0.22},
		{name: "ais_gap_frequency", raw: float64(signals.AISGapFrequency), normalized: clamp01(float64(signals.AISGapFrequency) / 6.0), weight: 0.10},
		{name: "flag_changes_90d", raw: float64(signals.FlagChanges90d), normalized: clamp01(float64(signals.FlagChanges90d) / 3.0), weight: 0.12},
		{name: "ownership_changes_180d", raw: float64(signals.OwnershipChanges180d), normalized: clamp01(float64(signals.OwnershipChanges180d) / 2.0), weight: 0.12},
		{name: "sanctions_exposure", raw: roundMetric(signals.SanctionsExposure), normalized: clamp01(signals.SanctionsExposure), weight: 0.18},
		{name: "high_risk_port_calls", raw: float64(signals.HighRiskPortCalls), normalized: clamp01(float64(signals.HighRiskPortCalls) / 4.0), weight: 0.10},
		{name: "sts_transfer_suspicion", raw: roundMetric(signals.STSSuspicionScore), normalized: clamp01(signals.STSSuspicionScore), weight: 0.08},
		{name: "route_deviation_score", raw: roundMetric(signals.RouteDeviationScore), normalized: clamp01(signals.RouteDeviationScore), weight: 0.05},
		{name: "age_profile", raw: float64(signals.VesselAgeYears), normalized: clamp01(float64(maxInt(signals.VesselAgeYears-15, 0)) / 20.0), weight: 0.03},
	}

	features := make([]map[string]any, 0, len(components))
	score := 0.0
	for _, component := range components {
		score += component.normalized * component.weight
		features = append(features, map[string]any{
			"feature":            component.name,
			"raw_value":          component.raw,
			"normalized_value":   roundMetric(component.normalized),
			"weight":             component.weight,
			"weighted_component": roundMetric(component.normalized * component.weight),
		})
	}
	evidence := mergeEvidence([]canonical.Evidence{{
		Kind:  "metric_formula",
		Ref:   "shadow_fleet_score",
		Value: "weighted_signal_blend",
	}}, signals.Evidence)
	return MetricReading{
		MetricID:      "shadow_fleet_score",
		MetricFamily:  domainFamily,
		SubjectGrain:  "entity",
		SubjectID:     subjectID,
		WindowGrain:   "day",
		CalculatedAt:  calculatedAt,
		MetricValue:   roundMetric(score),
		Unit:          "score",
		SchemaVersion: schemaVersion,
		Attrs: map[string]any{
			"normalized_factors": map[string]any{
				"ais_dark_hours_sum":     roundMetric(clamp01(signals.AISDarkHours / 24.0)),
				"ais_gap_frequency":      roundMetric(clamp01(float64(signals.AISGapFrequency) / 6.0)),
				"flag_changes_90d":       roundMetric(clamp01(float64(signals.FlagChanges90d) / 3.0)),
				"ownership_changes_180d": roundMetric(clamp01(float64(signals.OwnershipChanges180d) / 2.0)),
				"sanctions_exposure":     roundMetric(clamp01(signals.SanctionsExposure)),
				"high_risk_port_calls":   roundMetric(clamp01(float64(signals.HighRiskPortCalls) / 4.0)),
				"sts_transfer_suspicion": roundMetric(clamp01(signals.STSSuspicionScore)),
				"route_deviation_score":  roundMetric(clamp01(signals.RouteDeviationScore)),
				"age_profile":            roundMetric(clamp01(float64(maxInt(signals.VesselAgeYears-15, 0)) / 20.0)),
			},
			"explainability": map[string]any{
				"feature_contributions": features,
				"evidence_refs":         evidenceRefs(evidence),
			},
		},
		Evidence: evidence,
	}
}

func matchPortGap(call PortCall, gaps []AISGap) (AISGap, float64, bool) {
	bestHours := 0.0
	bestGap := AISGap{}
	found := false
	for _, gap := range gaps {
		if gap.EndsAt.IsZero() || call.StartedAt.IsZero() || !call.StartedAt.After(gap.EndsAt) {
			continue
		}
		if gap.NextKnownPortID != "" && gap.NextKnownPortID != call.PlaceID {
			continue
		}
		hours := roundMetric(durationHours(gap.EndsAt, call.StartedAt))
		if !found || gap.EndsAt.After(bestGap.EndsAt) {
			bestGap = gap
			bestHours = hours
			found = true
		}
	}
	return bestGap, bestHours, found
}

func isAnchorageCall(call PortCall) bool {
	joined := strings.ToLower(strings.Join([]string{call.PortName, call.Terminal, call.Berth}, " "))
	return strings.Contains(joined, "anchorage")
}

func clamp01(value float64) float64 {
	if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
		return 0
	}
	if value > 1 {
		return 1
	}
	return value
}

func roundMetric(value float64) float64 {
	return math.Round(value*10000) / 10000
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
