package metrics

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"global-osint-backend/internal/canonical"
)

type StateRow struct {
	MetricID              string               `json:"metric_id"`
	SubjectGrain          string               `json:"subject_grain"`
	SubjectID             string               `json:"subject_id"`
	PlaceID               string               `json:"place_id"`
	WindowGrain           string               `json:"window_grain"`
	WindowStart           time.Time            `json:"window_start"`
	WindowEnd             time.Time            `json:"window_end"`
	MaterializationKey    string               `json:"materialization_key"`
	ContributionCount     uint64               `json:"contribution_count"`
	ContributionValueSum  float64              `json:"contribution_value_sum"`
	ContributionWeightSum float64              `json:"contribution_weight_sum"`
	PeakValue             float64              `json:"peak_value"`
	LastContributionAt    time.Time            `json:"last_contribution_at"`
	DistinctSourceCount   int                  `json:"distinct_source_count"`
	UpdatedAt             time.Time            `json:"updated_at"`
	Explainability        map[string]any       `json:"explainability"`
	Evidence              []canonical.Evidence `json:"evidence"`
}

type SnapshotRow struct {
	SnapshotID         string               `json:"snapshot_id"`
	MetricID           string               `json:"metric_id"`
	SubjectGrain       string               `json:"subject_grain"`
	SubjectID          string               `json:"subject_id"`
	PlaceID            string               `json:"place_id"`
	WindowGrain        string               `json:"window_grain"`
	WindowStart        time.Time            `json:"window_start"`
	WindowEnd          time.Time            `json:"window_end"`
	MaterializationKey string               `json:"materialization_key"`
	SnapshotAt         time.Time            `json:"snapshot_at"`
	MetricValue        float64              `json:"metric_value"`
	MetricDelta        float64              `json:"metric_delta"`
	Rank               uint32               `json:"rank"`
	SchemaVersion      uint32               `json:"schema_version"`
	Attrs              map[string]any       `json:"attrs"`
	Evidence           []canonical.Evidence `json:"evidence"`
}

type stateAccumulator struct {
	row       StateRow
	sourceIDs map[string]struct{}
	refs      []string
	seenRefs  map[string]struct{}
}

func BuildMetricState(contributions []Contribution, updatedAt time.Time) []StateRow {
	updatedAt = updatedAt.UTC().Truncate(time.Millisecond)
	accumulators := map[string]*stateAccumulator{}
	for _, contribution := range contributions {
		contribution = normalizeContribution(contribution)
		key := stateKey(contribution)
		acc, ok := accumulators[key]
		if !ok {
			acc = &stateAccumulator{
				row: StateRow{
					MetricID:           contribution.MetricID,
					SubjectGrain:       contribution.SubjectGrain,
					SubjectID:          contribution.SubjectID,
					PlaceID:            contribution.PlaceID,
					WindowGrain:        contribution.WindowGrain,
					WindowStart:        contribution.WindowStart,
					WindowEnd:          contribution.WindowEnd,
					MaterializationKey: normalizeContribution(contribution).MaterializationKey,
					PeakValue:          contribution.ContributionValue,
					UpdatedAt:          updatedAt,
					Explainability:     map[string]any{},
				},
				sourceIDs: map[string]struct{}{},
				seenRefs:  map[string]struct{}{},
			}
			accumulators[key] = acc
		}
		acc.row.ContributionCount++
		acc.row.ContributionValueSum += contribution.ContributionValue
		acc.row.ContributionWeightSum += contribution.ContributionWeight
		if contribution.ContributionValue > acc.row.PeakValue {
			acc.row.PeakValue = contribution.ContributionValue
		}
		if contribution.WindowEnd.After(acc.row.LastContributionAt) {
			acc.row.LastContributionAt = contribution.WindowEnd
		}
		sourceID := contribution.SourceID
		if sourceID == "" {
			sourceID, _ = contribution.Attrs["source_id"].(string)
		}
		if sourceID != "" {
			acc.sourceIDs[sourceID] = struct{}{}
		}
		for _, ref := range evidenceRefs(contribution.Evidence) {
			if _, seen := acc.seenRefs[ref]; seen {
				continue
			}
			acc.seenRefs[ref] = struct{}{}
			acc.refs = append(acc.refs, ref)
		}
		acc.row.Evidence = mergeEvidence(acc.row.Evidence, contribution.Evidence)
	}

	rows := make([]StateRow, 0, len(accumulators))
	for _, acc := range accumulators {
		acc.row.DistinctSourceCount = len(acc.sourceIDs)
		acc.row.Explainability = map[string]any{
			"confidence_weight_sum": roundMetric(acc.row.ContributionWeightSum),
			"distinct_sources":      acc.row.DistinctSourceCount,
			"evidence_refs":         append([]string(nil), acc.refs...),
			"feature_contributions": []map[string]any{{
				"feature": "contribution_count",
				"value":   acc.row.ContributionCount,
				"weight":  roundMetric(acc.row.ContributionWeightSum),
			}},
		}
		rows = append(rows, acc.row)
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].MetricID != rows[j].MetricID {
			return rows[i].MetricID < rows[j].MetricID
		}
		if rows[i].WindowStart != rows[j].WindowStart {
			return rows[i].WindowStart.Before(rows[j].WindowStart)
		}
		if rows[i].SubjectGrain != rows[j].SubjectGrain {
			return rows[i].SubjectGrain < rows[j].SubjectGrain
		}
		return rows[i].SubjectID < rows[j].SubjectID
	})
	return rows
}

func BuildMetricSnapshots(states []StateRow, snapshotAt time.Time) []SnapshotRow {
	snapshotAt = snapshotAt.UTC().Truncate(time.Millisecond)
	sorted := append([]StateRow(nil), states...)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].MetricID != sorted[j].MetricID {
			return sorted[i].MetricID < sorted[j].MetricID
		}
		if sorted[i].SubjectGrain != sorted[j].SubjectGrain {
			return sorted[i].SubjectGrain < sorted[j].SubjectGrain
		}
		if sorted[i].SubjectID != sorted[j].SubjectID {
			return sorted[i].SubjectID < sorted[j].SubjectID
		}
		if sorted[i].WindowGrain != sorted[j].WindowGrain {
			return sorted[i].WindowGrain < sorted[j].WindowGrain
		}
		return sorted[i].WindowStart.Before(sorted[j].WindowStart)
	})

	previous := map[string]float64{}
	snapshots := make([]SnapshotRow, 0, len(sorted))
	for _, state := range sorted {
		value := finalizeMetricValue(state)
		chainKey := strings.Join([]string{state.MetricID, state.SubjectGrain, state.SubjectID, state.WindowGrain}, "|")
		delta := value - previous[chainKey]
		previous[chainKey] = value
		key := state.MaterializationKey
		if key == "" {
			key = materializationKey(state.MetricID, state.SubjectGrain, state.SubjectID, state.PlaceID, state.WindowGrain, state.WindowStart)
		}
		snapshots = append(snapshots, SnapshotRow{
			SnapshotID:         snapshotIDForMaterializationKey(key),
			MetricID:           state.MetricID,
			SubjectGrain:       state.SubjectGrain,
			SubjectID:          state.SubjectID,
			PlaceID:            state.PlaceID,
			WindowGrain:        state.WindowGrain,
			WindowStart:        state.WindowStart,
			WindowEnd:          state.WindowEnd,
			MaterializationKey: key,
			SnapshotAt:         snapshotAt,
			MetricValue:        roundMetric(value),
			MetricDelta:        roundMetric(delta),
			SchemaVersion:      SchemaVersion,
			Attrs: map[string]any{
				"distinct_source_count": state.DistinctSourceCount,
				"explainability":        cloneAnyMap(state.Explainability),
			},
			Evidence: append([]canonical.Evidence(nil), state.Evidence...),
		})
	}
	assignRanks(snapshots)
	return snapshots
}

func MetricStateTableSQL() string {
	return `CREATE TABLE IF NOT EXISTS gold.metric_state
(
    metric_id String,
    subject_grain LowCardinality(String),
    subject_id String,
    place_id String,
    window_grain LowCardinality(String),
    window_start DateTime64(3, 'UTC'),
    window_end DateTime64(3, 'UTC'),
    materialization_key String,
    contribution_count_state AggregateFunction(count),
    contribution_value_state AggregateFunction(sum, Float64),
    contribution_weight_state AggregateFunction(sum, Float64),
    peak_value_state AggregateFunction(max, Float64),
    last_contribution_at_state AggregateFunction(max, DateTime64(3, 'UTC')),
    distinct_source_count_state AggregateFunction(uniqExact, String),
    latest_value_state AggregateFunction(argMax, Float64, DateTime64(3, 'UTC')),
    updated_at DateTime64(3, 'UTC')
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(window_start)
ORDER BY (metric_id, subject_grain, subject_id, window_grain, window_start, materialization_key)
TTL toDateTime(window_start) + INTERVAL 730 DAY DELETE;`
}

func MetricStateMaterializedViewSQL(viewName string) string {
	return fmt.Sprintf(`CREATE MATERIALIZED VIEW IF NOT EXISTS gold.%s
TO gold.metric_state AS
SELECT
    metric_id,
    subject_grain,
    subject_id,
    place_id,
    window_grain,
    window_start,
    window_end,
    materialization_key,
    countState() AS contribution_count_state,
    sumState(contribution_value) AS contribution_value_state,
    sumState(toFloat64(contribution_weight)) AS contribution_weight_state,
    maxState(contribution_value) AS peak_value_state,
    maxState(window_end) AS last_contribution_at_state,
    uniqExactState(source_id) AS distinct_source_count_state,
    argMaxState(contribution_value, window_end) AS latest_value_state,
    now64(3) AS updated_at
FROM silver.metric_contribution
GROUP BY metric_id, subject_grain, subject_id, place_id, window_grain, window_start, window_end, materialization_key;`, viewName)
}

func RefreshableMetricSnapshotViewSQL(viewName, cadence, windowGrain string) string {
	return fmt.Sprintf(`CREATE MATERIALIZED VIEW IF NOT EXISTS gold.%s
REFRESH EVERY %s TO gold.metric_snapshot AS
SELECT
    concat('snapshot:', materialization_key) AS snapshot_id,
    metric_id,
    subject_grain,
    subject_id,
    place_id,
    window_grain,
    window_start,
    window_end,
    materialization_key,
    now64(3) AS snapshot_at,
    multiIf(
        metric_id = 'source_diversity_score', if(countMerge(contribution_count_state) = 0, 0.0, uniqExactMerge(distinct_source_count_state) / toFloat64(countMerge(contribution_count_state))),
        metric_id IN ('anomaly_zscore_30d', 'burst_score', 'cross_source_confirmation_rate', 'dedup_rate', 'evidence_density', 'freshness_lag_minutes', 'geolocation_success_rate', 'risk_composite_global', 'schema_drift_rate'), if(sumMerge(contribution_weight_state) = 0, 0.0, sumMerge(contribution_value_state) / sumMerge(contribution_weight_state)),
        sumMerge(contribution_value_state)
    ) AS metric_value,
    metric_value - ifNull(lagInFrame(metric_value) OVER (PARTITION BY metric_id, subject_grain, subject_id, window_grain ORDER BY window_start), 0.0) AS metric_delta,
    row_number() OVER (PARTITION BY metric_id, subject_grain, window_grain, window_start ORDER BY metric_value DESC, subject_id ASC) AS rank,
    toUInt32(1) AS schema_version,
    '{}' AS attrs,
    '[]' AS evidence
FROM gold.metric_state
WHERE window_grain = '%s'
GROUP BY metric_id, subject_grain, subject_id, place_id, window_grain, window_start, window_end, materialization_key;`, viewName, cadence, windowGrain)
}

func RefreshableMetricSnapshotViews() map[string]string {
	return map[string]string{
		"metric_snapshot_day_mv": RefreshableMetricSnapshotViewSQL("metric_snapshot_day_mv", "15 MINUTE", "day"),
		"metric_snapshot_24h_mv": RefreshableMetricSnapshotViewSQL("metric_snapshot_24h_mv", "1 HOUR", "24h"),
		"metric_snapshot_7d_mv":  RefreshableMetricSnapshotViewSQL("metric_snapshot_7d_mv", "1 HOUR", "7d"),
		"metric_snapshot_30d_mv": RefreshableMetricSnapshotViewSQL("metric_snapshot_30d_mv", "1 HOUR", "30d"),
	}
}

func finalizeMetricValue(state StateRow) float64 {
	switch state.MetricID {
	case "source_diversity_score":
		if state.ContributionCount == 0 {
			return 0
		}
		return float64(state.DistinctSourceCount) / float64(state.ContributionCount)
	case "anomaly_zscore_30d", "burst_score", "cross_source_confirmation_rate", "dedup_rate", "evidence_density", "freshness_lag_minutes", "geolocation_success_rate", "risk_composite_global", "schema_drift_rate":
		if state.ContributionWeightSum == 0 {
			return 0
		}
		return state.ContributionValueSum / state.ContributionWeightSum
	default:
		return state.ContributionValueSum
	}
}

func assignRanks(snapshots []SnapshotRow) {
	groups := map[string][]int{}
	for idx, snapshot := range snapshots {
		key := strings.Join([]string{snapshot.MetricID, snapshot.SubjectGrain, snapshot.WindowGrain, snapshot.WindowStart.Format(time.RFC3339)}, "|")
		groups[key] = append(groups[key], idx)
	}
	for _, indexes := range groups {
		sort.Slice(indexes, func(i, j int) bool {
			left := snapshots[indexes[i]]
			right := snapshots[indexes[j]]
			if left.MetricValue != right.MetricValue {
				return left.MetricValue > right.MetricValue
			}
			return left.SubjectID < right.SubjectID
		})
		for rank, idx := range indexes {
			snapshots[idx].Rank = uint32(rank + 1)
		}
	}
}

func stateKey(contribution Contribution) string {
	contribution = normalizeContribution(contribution)
	return strings.Join([]string{
		contribution.MetricID,
		contribution.SubjectGrain,
		contribution.SubjectID,
		contribution.PlaceID,
		contribution.WindowGrain,
		contribution.WindowStart.Format(time.RFC3339),
	}, "|")
}

func normalizeContribution(contribution Contribution) Contribution {
	if contribution.WindowStart.Location() != time.UTC {
		contribution.WindowStart = contribution.WindowStart.UTC()
	}
	if contribution.WindowEnd.Location() != time.UTC {
		contribution.WindowEnd = contribution.WindowEnd.UTC()
	}
	if contribution.MaterializationKey == "" {
		contribution.MaterializationKey = materializationKey(contribution.MetricID, contribution.SubjectGrain, contribution.SubjectID, contribution.PlaceID, contribution.WindowGrain, contribution.WindowStart)
	}
	return contribution
}

func materializationKey(metricID, subjectGrain, subjectID, placeID, windowGrain string, windowStart time.Time) string {
	windowStart = windowStart.UTC().Truncate(time.Millisecond)
	parts := []string{metricID, subjectGrain, subjectID, placeID, windowGrain, fmt.Sprintf("%d", windowStart.UnixMilli())}
	return strings.Join(parts, "|")
}

func snapshotIDForMaterializationKey(key string) string {
	return "snapshot:" + key
}

func mergeEvidence(existing, incoming []canonical.Evidence) []canonical.Evidence {
	if len(incoming) == 0 {
		return existing
	}
	seen := map[string]struct{}{}
	for _, item := range existing {
		seen[item.Kind+"|"+item.Ref+"|"+item.RawID] = struct{}{}
	}
	out := append([]canonical.Evidence(nil), existing...)
	for _, item := range incoming {
		key := item.Kind + "|" + item.Ref + "|" + item.RawID
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, item)
	}
	return out
}

func cloneAnyMap(input map[string]any) map[string]any {
	if len(input) == 0 {
		return nil
	}
	out := make(map[string]any, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}
