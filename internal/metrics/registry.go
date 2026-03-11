package metrics

import (
	"sort"
	"time"

	"global-osint-backend/internal/canonical"
)

const (
	SchemaVersion      uint32 = 1
	APIContractVersion uint32 = 1
)

type MetricDefinition struct {
	MetricID       string
	MetricFamily   string
	SubjectGrain   string
	Unit           string
	ValueType      string
	RollupEngine   string
	RollupRule     string
	RefreshCadence string
	Description    string
	Formula        string
	Windows        []string
}

type RegistryRecord struct {
	MetricID           string               `json:"metric_id"`
	MetricFamily       string               `json:"metric_family"`
	SubjectGrain       string               `json:"subject_grain"`
	Unit               string               `json:"unit"`
	ValueType          string               `json:"value_type"`
	RollupEngine       string               `json:"rollup_engine"`
	RollupRule         string               `json:"rollup_rule"`
	Attrs              map[string]any       `json:"attrs"`
	Evidence           []canonical.Evidence `json:"evidence"`
	SchemaVersion      uint32               `json:"schema_version"`
	RecordVersion      uint64               `json:"record_version"`
	APIContractVersion uint32               `json:"api_contract_version"`
	Enabled            bool                 `json:"enabled"`
	UpdatedAt          time.Time            `json:"updated_at"`
}

func CoreMetricDefinitions() []MetricDefinition {
	metrics := []MetricDefinition{
		{
			MetricID:       "acceleration_7d_vs_30d",
			MetricFamily:   "trend",
			SubjectGrain:   "place_hierarchy",
			Unit:           "activity_rate_delta",
			ValueType:      "gauge",
			RollupEngine:   "AggregatingMergeTree",
			RollupRule:     "sum",
			RefreshCadence: "1 HOUR",
			Description:    "Per-record 7d-vs-30d acceleration signal accumulated across the place tree.",
			Formula:        "sum(contribution_value)",
			Windows:        []string{"30d"},
		},
		{
			MetricID:       "anomaly_zscore_30d",
			MetricFamily:   "risk",
			SubjectGrain:   "place_hierarchy",
			Unit:           "zscore",
			ValueType:      "gauge",
			RollupEngine:   "AggregatingMergeTree",
			RollupRule:     "weighted_avg",
			RefreshCadence: "1 HOUR",
			Description:    "Average 30-day anomaly z-score supplied by upstream record scoring.",
			Formula:        "sum(contribution_value) / nullIf(sum(contribution_weight), 0)",
			Windows:        []string{"30d"},
		},
		{
			MetricID:       "burst_score",
			MetricFamily:   "activity",
			SubjectGrain:   "place_hierarchy",
			Unit:           "score",
			ValueType:      "ratio",
			RollupEngine:   "AggregatingMergeTree",
			RollupRule:     "weighted_avg",
			RefreshCadence: "15 MINUTE",
			Description:    "Weighted burst intensity from per-record anomaly hints.",
			Formula:        "sum(contribution_value) / nullIf(sum(contribution_weight), 0)",
			Windows:        []string{"day"},
		},
		{
			MetricID:       "confidence_weighted_activity",
			MetricFamily:   "activity",
			SubjectGrain:   "place_hierarchy",
			Unit:           "weighted_activity",
			ValueType:      "gauge",
			RollupEngine:   "AggregatingMergeTree",
			RollupRule:     "sum",
			RefreshCadence: "15 MINUTE",
			Description:    "Confidence-weighted activity volume across observations and events.",
			Formula:        "sum(contribution_value)",
			Windows:        []string{"day"},
		},
		{
			MetricID:       "cross_source_confirmation_rate",
			MetricFamily:   "quality",
			SubjectGrain:   "place_hierarchy",
			Unit:           "ratio",
			ValueType:      "ratio",
			RollupEngine:   "AggregatingMergeTree",
			RollupRule:     "weighted_avg",
			RefreshCadence: "15 MINUTE",
			Description:    "Share of records that are confirmed by more than one source.",
			Formula:        "sum(contribution_value) / nullIf(sum(contribution_weight), 0)",
			Windows:        []string{"day"},
		},
		{
			MetricID:       "dedup_rate",
			MetricFamily:   "quality",
			SubjectGrain:   "place_hierarchy",
			Unit:           "ratio",
			ValueType:      "ratio",
			RollupEngine:   "AggregatingMergeTree",
			RollupRule:     "weighted_avg",
			RefreshCadence: "15 MINUTE",
			Description:    "Share of records that were deduplicated during canonicalization.",
			Formula:        "sum(contribution_value) / nullIf(sum(contribution_weight), 0)",
			Windows:        []string{"day"},
		},
		{
			MetricID:       "entity_count_approx",
			MetricFamily:   "activity",
			SubjectGrain:   "place_hierarchy",
			Unit:           "entities",
			ValueType:      "count",
			RollupEngine:   "AggregatingMergeTree",
			RollupRule:     "sum",
			RefreshCadence: "15 MINUTE",
			Description:    "Approximate count of distinct entities represented in canonical records.",
			Formula:        "sum(contribution_value)",
			Windows:        []string{"day"},
		},
		{
			MetricID:       "event_count",
			MetricFamily:   "activity",
			SubjectGrain:   "place_hierarchy",
			Unit:           "events",
			ValueType:      "count",
			RollupEngine:   "AggregatingMergeTree",
			RollupRule:     "sum",
			RefreshCadence: "15 MINUTE",
			Description:    "Count of canonical events by place lineage.",
			Formula:        "sum(contribution_value)",
			Windows:        []string{"day"},
		},
		{
			MetricID:       "evidence_density",
			MetricFamily:   "quality",
			SubjectGrain:   "place_hierarchy",
			Unit:           "references_per_record",
			ValueType:      "gauge",
			RollupEngine:   "AggregatingMergeTree",
			RollupRule:     "weighted_avg",
			RefreshCadence: "15 MINUTE",
			Description:    "Average evidence-reference count attached to canonical records.",
			Formula:        "sum(contribution_value) / nullIf(sum(contribution_weight), 0)",
			Windows:        []string{"day"},
		},
		{
			MetricID:       "freshness_lag_minutes",
			MetricFamily:   "quality",
			SubjectGrain:   "place_hierarchy",
			Unit:           "minutes",
			ValueType:      "gauge",
			RollupEngine:   "AggregatingMergeTree",
			RollupRule:     "weighted_avg",
			RefreshCadence: "15 MINUTE",
			Description:    "Average observed-to-published lag for metric inputs.",
			Formula:        "sum(contribution_value) / nullIf(sum(contribution_weight), 0)",
			Windows:        []string{"day"},
		},
		{
			MetricID:       "geolocation_success_rate",
			MetricFamily:   "quality",
			SubjectGrain:   "place_hierarchy",
			Unit:           "ratio",
			ValueType:      "ratio",
			RollupEngine:   "AggregatingMergeTree",
			RollupRule:     "weighted_avg",
			RefreshCadence: "15 MINUTE",
			Description:    "Share of records that resolved to a usable place.",
			Formula:        "sum(contribution_value) / nullIf(sum(contribution_weight), 0)",
			Windows:        []string{"day"},
		},
		{
			MetricID:       "obs_count",
			MetricFamily:   "activity",
			SubjectGrain:   "place_hierarchy",
			Unit:           "observations",
			ValueType:      "count",
			RollupEngine:   "AggregatingMergeTree",
			RollupRule:     "sum",
			RefreshCadence: "15 MINUTE",
			Description:    "Count of canonical observations by place lineage.",
			Formula:        "sum(contribution_value)",
			Windows:        []string{"day"},
		},
		{
			MetricID:       "risk_composite_global",
			MetricFamily:   "risk",
			SubjectGrain:   "place_hierarchy",
			Unit:           "score",
			ValueType:      "ratio",
			RollupEngine:   "AggregatingMergeTree",
			RollupRule:     "weighted_avg",
			RefreshCadence: "15 MINUTE",
			Description:    "Confidence-weighted risk composite rolled up across the place tree.",
			Formula:        "sum(contribution_value) / nullIf(sum(contribution_weight), 0)",
			Windows:        []string{"day"},
		},
		{
			MetricID:       "schema_drift_rate",
			MetricFamily:   "quality",
			SubjectGrain:   "place_hierarchy",
			Unit:           "ratio",
			ValueType:      "ratio",
			RollupEngine:   "AggregatingMergeTree",
			RollupRule:     "weighted_avg",
			RefreshCadence: "15 MINUTE",
			Description:    "Share of records that indicate a schema field drift or contract mismatch.",
			Formula:        "sum(contribution_value) / nullIf(sum(contribution_weight), 0)",
			Windows:        []string{"day"},
		},
		{
			MetricID:       "source_count_approx",
			MetricFamily:   "quality",
			SubjectGrain:   "place_hierarchy",
			Unit:           "sources",
			ValueType:      "count",
			RollupEngine:   "AggregatingMergeTree",
			RollupRule:     "sum",
			RefreshCadence: "15 MINUTE",
			Description:    "Approximate count of distinct contributing sources in the current slice.",
			Formula:        "sum(contribution_value)",
			Windows:        []string{"day"},
		},
		{
			MetricID:       "source_diversity_score",
			MetricFamily:   "quality",
			SubjectGrain:   "place_hierarchy",
			Unit:           "ratio",
			ValueType:      "ratio",
			RollupEngine:   "AggregatingMergeTree",
			RollupRule:     "distinct_sources_per_contribution",
			RefreshCadence: "15 MINUTE",
			Description:    "Distinct-source coverage normalized by contribution volume.",
			Formula:        "uniq(source_id) / count()",
			Windows:        []string{"day"},
		},
		{
			MetricID:       "trend_24h",
			MetricFamily:   "trend",
			SubjectGrain:   "place_hierarchy",
			Unit:           "activity_delta",
			ValueType:      "count",
			RollupEngine:   "AggregatingMergeTree",
			RollupRule:     "sum",
			RefreshCadence: "1 HOUR",
			Description:    "Twenty-four-hour rolling activity total used for short-horizon trend snapshots.",
			Formula:        "sum(contribution_value)",
			Windows:        []string{"24h"},
		},
		{
			MetricID:       "trend_7d",
			MetricFamily:   "trend",
			SubjectGrain:   "place_hierarchy",
			Unit:           "activity_delta",
			ValueType:      "count",
			RollupEngine:   "AggregatingMergeTree",
			RollupRule:     "sum",
			RefreshCadence: "1 HOUR",
			Description:    "Seven-day rolling activity total used for delta snapshots.",
			Formula:        "sum(contribution_value)",
			Windows:        []string{"7d"},
		},
	}
	sort.Slice(metrics, func(i, j int) bool { return metrics[i].MetricID < metrics[j].MetricID })
	return metrics
}

func BuildRegistryRecords(now time.Time) []RegistryRecord {
	now = now.UTC().Truncate(time.Millisecond)
	definitions := CoreMetricDefinitions()
	records := make([]RegistryRecord, 0, len(definitions))
	for idx, def := range definitions {
		records = append(records, RegistryRecord{
			MetricID:     def.MetricID,
			MetricFamily: def.MetricFamily,
			SubjectGrain: def.SubjectGrain,
			Unit:         def.Unit,
			ValueType:    def.ValueType,
			RollupEngine: def.RollupEngine,
			RollupRule:   def.RollupRule,
			Attrs: map[string]any{
				"description":     def.Description,
				"formula":         def.Formula,
				"refresh_cadence": def.RefreshCadence,
				"window_grains":   append([]string(nil), def.Windows...),
				"core_metric":     true,
				"explainability": map[string]any{
					"includes_confidence":            true,
					"includes_feature_contributions": true,
					"includes_evidence_refs":         true,
				},
			},
			Evidence: []canonical.Evidence{{
				Kind:  "metric_spec",
				Ref:   def.MetricID,
				Value: def.Formula,
				Attrs: map[string]any{
					"metric_family": def.MetricFamily,
					"window_grains": append([]string(nil), def.Windows...),
				},
			}},
			SchemaVersion:      SchemaVersion,
			RecordVersion:      uint64(idx + 1),
			APIContractVersion: APIContractVersion,
			Enabled:            true,
			UpdatedAt:          now,
		})
	}
	return records
}
