package metrics

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

type materializationSubject struct {
	MetricID     string
	SubjectGrain string
	SubjectID    string
	WindowGrain  string
}

func UpsertMaterializationSQL(contributions []Contribution, snapshotAt time.Time) ([]string, error) {
	if len(contributions) == 0 {
		return nil, nil
	}
	snapshotAt = snapshotAt.UTC().Truncate(time.Millisecond)
	normalized := make([]Contribution, 0, len(contributions))
	contributionIDs := make([]string, 0, len(contributions))
	materializationKeys := make([]string, 0, len(contributions))
	metricIDs := make([]string, 0, len(contributions))
	subjects := make([]materializationSubject, 0, len(contributions))

	seenContributionIDs := map[string]struct{}{}
	seenKeys := map[string]struct{}{}
	seenMetricIDs := map[string]struct{}{}
	seenSubjects := map[string]struct{}{}

	for _, row := range contributions {
		row = normalizeContribution(row)
		normalized = append(normalized, row)
		if _, ok := seenContributionIDs[row.ContributionID]; !ok {
			seenContributionIDs[row.ContributionID] = struct{}{}
			contributionIDs = append(contributionIDs, row.ContributionID)
		}
		if _, ok := seenKeys[row.MaterializationKey]; !ok {
			seenKeys[row.MaterializationKey] = struct{}{}
			materializationKeys = append(materializationKeys, row.MaterializationKey)
		}
		if _, ok := seenMetricIDs[row.MetricID]; !ok {
			seenMetricIDs[row.MetricID] = struct{}{}
			metricIDs = append(metricIDs, row.MetricID)
		}
		subjectKey := strings.Join([]string{row.MetricID, row.SubjectGrain, row.SubjectID, row.WindowGrain}, "|")
		if _, ok := seenSubjects[subjectKey]; !ok {
			seenSubjects[subjectKey] = struct{}{}
			subjects = append(subjects, materializationSubject{
				MetricID:     row.MetricID,
				SubjectGrain: row.SubjectGrain,
				SubjectID:    row.SubjectID,
				WindowGrain:  row.WindowGrain,
			})
		}
	}

	sort.Strings(contributionIDs)
	sort.Strings(materializationKeys)
	sort.Strings(metricIDs)
	sort.Slice(subjects, func(i, j int) bool {
		left := subjects[i]
		right := subjects[j]
		if left.MetricID != right.MetricID {
			return left.MetricID < right.MetricID
		}
		if left.SubjectGrain != right.SubjectGrain {
			return left.SubjectGrain < right.SubjectGrain
		}
		if left.SubjectID != right.SubjectID {
			return left.SubjectID < right.SubjectID
		}
		return left.WindowGrain < right.WindowGrain
	})

	insertSQL, err := BuildContributionInsertSQL(normalized)
	if err != nil {
		return nil, err
	}
	statements := []string{
		deleteByStringListSQL("silver.metric_contribution", "contribution_id", contributionIDs),
		insertSQL,
		deleteByStringListSQL("gold.metric_state", "materialization_key", materializationKeys),
		insertMetricStateSQL(materializationKeys),
		deleteByStringListSQL("gold.metric_snapshot", "materialization_key", materializationKeys),
		insertMetricSnapshotSQL(metricIDs, materializationKeys, subjects, snapshotAt),
	}
	statements = append(statements, RuntimeAnalyticsRefreshSQL()...)
	return statements, nil
}

func BuildContributionInsertSQL(rows []Contribution) (string, error) {
	if len(rows) == 0 {
		return "", nil
	}
	values := make([]string, 0, len(rows))
	for _, row := range rows {
		row = normalizeContribution(row)
		attrs, err := json.Marshal(row.Attrs)
		if err != nil {
			return "", err
		}
		evidence, err := json.Marshal(row.Evidence)
		if err != nil {
			return "", err
		}
		values = append(values, fmt.Sprintf("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%d,%s,%s)",
			sqlString(row.ContributionID),
			sqlString(row.MetricID),
			sqlString(row.SubjectGrain),
			sqlString(row.SubjectID),
			sqlString(row.SourceRecordType),
			sqlString(row.SourceRecordID),
			sqlString(row.SourceID),
			sqlString(row.PlaceID),
			sqlString(row.WindowGrain),
			sqlTime(row.WindowStart),
			sqlTime(row.WindowEnd),
			sqlString(row.MaterializationKey),
			sqlString(row.ContributionType),
			formatFloat64(row.ContributionValue),
			formatFloat64(row.ContributionWeight),
			row.SchemaVersion,
			sqlString(string(attrs)),
			sqlString(string(evidence)),
		))
	}
	return "INSERT INTO silver.metric_contribution (contribution_id, metric_id, subject_grain, subject_id, source_record_type, source_record_id, source_id, place_id, window_grain, window_start, window_end, materialization_key, contribution_type, contribution_value, contribution_weight, schema_version, attrs, evidence) VALUES " + strings.Join(values, ","), nil
}

func deleteByStringListSQL(tableName, columnName string, values []string) string {
	return fmt.Sprintf("ALTER TABLE %s DELETE WHERE %s IN (%s) SETTINGS mutations_sync = 1", tableName, columnName, sqlStringList(values))
}

func insertMetricStateSQL(materializationKeys []string) string {
	return fmt.Sprintf(`INSERT INTO gold.metric_state (metric_id, subject_grain, subject_id, place_id, window_grain, window_start, window_end, materialization_key, contribution_count_state, contribution_value_state, contribution_weight_state, peak_value_state, last_contribution_at_state, distinct_source_count_state, latest_value_state, updated_at)
SELECT
    metric_id,
    subject_grain,
    subject_id,
    place_id,
    window_grain,
    window_start,
    bucket_window_end AS window_end,
    materialization_key,
    contribution_count_state,
    contribution_value_state,
    contribution_weight_state,
    peak_value_state,
    last_contribution_at_state,
    distinct_source_count_state,
    latest_value_state,
    updated_at
FROM (
    SELECT
        metric_id,
        subject_grain,
        subject_id,
        place_id,
        window_grain,
        window_start,
        any(window_end) AS bucket_window_end,
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
    WHERE materialization_key IN (%s)
    GROUP BY metric_id, subject_grain, subject_id, place_id, window_grain, window_start, materialization_key
) AS finalized_state`, sqlStringList(materializationKeys))
}

func insertMetricSnapshotSQL(metricIDs, materializationKeys []string, subjects []materializationSubject, snapshotAt time.Time) string {
	return fmt.Sprintf(`INSERT INTO gold.metric_snapshot (snapshot_id, metric_id, subject_grain, subject_id, place_id, window_grain, window_start, window_end, materialization_key, snapshot_at, metric_value, metric_delta, rank, schema_version, attrs, evidence)
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
    %s AS snapshot_at,
    metric_value,
    metric_value - ifNull(lagInFrame(metric_value) OVER (PARTITION BY metric_id, subject_grain, subject_id, window_grain ORDER BY window_start), 0.0) AS metric_delta,
    row_number() OVER (PARTITION BY metric_id, subject_grain, window_grain, window_start ORDER BY metric_value DESC, subject_id ASC) AS rank,
    toUInt32(%d) AS schema_version,
    concat('{"materialization_path":"registry->silver.metric_contribution->gold.metric_state->gold.metric_snapshot","rollup_rule":"', rollup_rule, '"}') AS attrs,
    '[]' AS evidence
FROM (
    SELECT
        s.metric_id,
        s.subject_grain,
        s.subject_id,
        s.place_id,
        s.window_grain,
        s.window_start,
        any(s.window_end) AS window_end,
        any(s.materialization_key) AS materialization_key,
        any(registry.rollup_rule) AS rollup_rule,
        multiIf(
            any(registry.rollup_rule) = 'weighted_avg', if(sumMerge(s.contribution_weight_state) = 0, 0.0, sumMerge(s.contribution_value_state) / sumMerge(s.contribution_weight_state)),
            any(registry.rollup_rule) = 'distinct_sources_per_contribution', if(countMerge(s.contribution_count_state) = 0, 0.0, uniqExactMerge(s.distinct_source_count_state) / toFloat64(countMerge(s.contribution_count_state))),
            any(registry.rollup_rule) = 'latest', argMaxMerge(s.latest_value_state),
            any(registry.rollup_rule) = 'latest_daily_pack_score', argMaxMerge(s.latest_value_state),
            sumMerge(s.contribution_value_state)
        ) AS metric_value
    FROM gold.metric_state AS s
    INNER JOIN (
        SELECT
            metric_id,
            argMax(rollup_rule, record_version) AS rollup_rule
        FROM meta.metric_registry
        WHERE metric_id IN (%s)
        GROUP BY metric_id
    ) AS registry USING (metric_id)
    WHERE (s.metric_id, s.subject_grain, s.subject_id, s.window_grain) IN (%s)
    GROUP BY s.metric_id, s.subject_grain, s.subject_id, s.place_id, s.window_grain, s.window_start
) AS finalized
WHERE materialization_key IN (%s)`, sqlTime(snapshotAt), SchemaVersion, sqlStringList(metricIDs), sqlSubjectList(subjects), sqlStringList(materializationKeys))
}

func sqlSubjectList(subjects []materializationSubject) string {
	parts := make([]string, 0, len(subjects))
	for _, subject := range subjects {
		parts = append(parts, fmt.Sprintf("(%s,%s,%s,%s)", sqlString(subject.MetricID), sqlString(subject.SubjectGrain), sqlString(subject.SubjectID), sqlString(subject.WindowGrain)))
	}
	return strings.Join(parts, ",")
}

func sqlStringList(values []string) string {
	parts := make([]string, 0, len(values))
	for _, value := range values {
		parts = append(parts, sqlString(value))
	}
	return strings.Join(parts, ",")
}

func sqlString(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func sqlTime(value time.Time) string {
	return sqlString(value.UTC().Truncate(time.Millisecond).Format("2006-01-02 15:04:05.000"))
}

func formatFloat64(value float64) string {
	return strconv.FormatFloat(value, 'f', -1, 64)
}
