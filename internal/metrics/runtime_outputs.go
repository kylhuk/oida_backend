package metrics

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"global-osint-backend/internal/canonical"
)

type HotspotRow struct {
	HotspotID     string               `json:"hotspot_id"`
	MetricID      string               `json:"metric_id"`
	ScopeType     string               `json:"scope_type"`
	ScopeID       string               `json:"scope_id"`
	PlaceID       string               `json:"place_id"`
	SnapshotAt    time.Time            `json:"snapshot_at"`
	WindowGrain   string               `json:"window_grain"`
	WindowStart   time.Time            `json:"window_start"`
	WindowEnd     time.Time            `json:"window_end"`
	Rank          uint32               `json:"rank"`
	HotspotScore  float64              `json:"hotspot_score"`
	SchemaVersion uint32               `json:"schema_version"`
	Attrs         map[string]any       `json:"attrs"`
	Evidence      []canonical.Evidence `json:"evidence"`
}

type CrossDomainRow struct {
	CrossDomainID  string               `json:"cross_domain_id"`
	SubjectGrain   string               `json:"subject_grain"`
	SubjectID      string               `json:"subject_id"`
	PlaceID        string               `json:"place_id"`
	WindowGrain    string               `json:"window_grain"`
	WindowStart    time.Time            `json:"window_start"`
	Domains        []string             `json:"domains"`
	CompositeScore float64              `json:"composite_score"`
	SnapshotAt     time.Time            `json:"snapshot_at"`
	MetricIDs      []string             `json:"metric_ids"`
	Attrs          map[string]any       `json:"attrs"`
	Evidence       []canonical.Evidence `json:"evidence"`
}

type metricSurfaceSummary struct {
	maxValue         float64
	maxPositiveDelta float64
}

type crossDomainAccumulator struct {
	SubjectGrain string
	SubjectID    string
	PlaceID      string
	WindowGrain  string
	WindowStart  time.Time
	SnapshotAt   time.Time
	Domains      map[string]struct{}
	MetricIDs    map[string]struct{}
	Evidence     []canonical.Evidence
	Components   []map[string]any
	ScoreTotal   float64
	ScoreCount   int
}

func BuildHotspotRows(snapshots []SnapshotRow, metricFamilies map[string]string) []HotspotRow {
	latest := latestSnapshots(snapshots)
	groups := map[string][]SnapshotRow{}
	for _, snapshot := range latest {
		if snapshot.MetricValue <= 0 && snapshot.MetricDelta <= 0 {
			continue
		}
		key := strings.Join([]string{snapshot.MetricID, snapshot.SubjectGrain, snapshot.WindowGrain, snapshot.WindowStart.UTC().Format(time.RFC3339Nano)}, "|")
		groups[key] = append(groups[key], snapshot)
	}

	rows := make([]HotspotRow, 0, len(latest))
	for _, group := range groups {
		sort.Slice(group, func(i, j int) bool {
			if group[i].MetricValue != group[j].MetricValue {
				return group[i].MetricValue > group[j].MetricValue
			}
			return group[i].SubjectID < group[j].SubjectID
		})
		maxValue := 0.0
		maxPositiveDelta := 0.0
		for _, snapshot := range group {
			if snapshot.MetricValue > maxValue {
				maxValue = snapshot.MetricValue
			}
			if snapshot.MetricDelta > maxPositiveDelta {
				maxPositiveDelta = snapshot.MetricDelta
			}
		}
		populationSize := len(group)
		for idx, snapshot := range group {
			rank := idx + 1
			valueComponent := roundMetric(normalizedRatio(snapshot.MetricValue, maxValue) * 60)
			deltaComponent := roundMetric(normalizedRatio(maxFloat64(snapshot.MetricDelta, 0), maxPositiveDelta) * 25)
			rankComponent := roundMetric(rankWeight(rank, populationSize) * 15)
			hotspotScore := roundMetric(valueComponent + deltaComponent + rankComponent)
			metricFamily := metricFamilyFor(snapshot.MetricID, metricFamilies)
			rows = append(rows, HotspotRow{
				HotspotID:     hotspotIDForSnapshot(snapshot),
				MetricID:      snapshot.MetricID,
				ScopeType:     snapshot.SubjectGrain,
				ScopeID:       snapshot.SubjectID,
				PlaceID:       snapshot.PlaceID,
				SnapshotAt:    snapshot.SnapshotAt,
				WindowGrain:   snapshot.WindowGrain,
				WindowStart:   snapshot.WindowStart,
				WindowEnd:     snapshot.WindowEnd,
				Rank:          uint32(rank),
				HotspotScore:  hotspotScore,
				SchemaVersion: SchemaVersion,
				Attrs: map[string]any{
					"metric_family":      metricFamily,
					"source_snapshot_id": snapshot.SnapshotID,
					"population_size":    populationSize,
					"value_component":    valueComponent,
					"delta_component":    deltaComponent,
					"rank_component":     rankComponent,
				},
				Evidence: append([]canonical.Evidence(nil), snapshot.Evidence...),
			})
		}
	}

	sort.Slice(rows, func(i, j int) bool {
		if rows[i].MetricID != rows[j].MetricID {
			return rows[i].MetricID < rows[j].MetricID
		}
		if rows[i].ScopeType != rows[j].ScopeType {
			return rows[i].ScopeType < rows[j].ScopeType
		}
		if rows[i].WindowStart != rows[j].WindowStart {
			return rows[i].WindowStart.Before(rows[j].WindowStart)
		}
		if rows[i].Rank != rows[j].Rank {
			return rows[i].Rank < rows[j].Rank
		}
		return rows[i].ScopeID < rows[j].ScopeID
	})
	return rows
}

func BuildCrossDomainRows(snapshots []SnapshotRow, metricFamilies map[string]string) []CrossDomainRow {
	latest := latestSnapshots(snapshots)
	surface := metricSurfaceSummaries(latest)
	accumulators := map[string]*crossDomainAccumulator{}
	for _, snapshot := range latest {
		if snapshot.MetricValue <= 0 && snapshot.MetricDelta <= 0 {
			continue
		}
		summary := surface[metricSurfaceKey(snapshot)]
		valueComponent := normalizedRatio(snapshot.MetricValue, summary.maxValue)
		deltaComponent := normalizedRatio(maxFloat64(snapshot.MetricDelta, 0), summary.maxPositiveDelta)
		componentScore := roundMetric((valueComponent*0.7 + deltaComponent*0.3) * 100)
		key := strings.Join([]string{snapshot.SubjectGrain, snapshot.SubjectID, snapshot.PlaceID, snapshot.WindowGrain, snapshot.WindowStart.UTC().Format(time.RFC3339Nano)}, "|")
		acc, ok := accumulators[key]
		if !ok {
			acc = &crossDomainAccumulator{
				SubjectGrain: snapshot.SubjectGrain,
				SubjectID:    snapshot.SubjectID,
				PlaceID:      snapshot.PlaceID,
				WindowGrain:  snapshot.WindowGrain,
				WindowStart:  snapshot.WindowStart,
				SnapshotAt:   snapshot.SnapshotAt,
				Domains:      map[string]struct{}{},
				MetricIDs:    map[string]struct{}{},
			}
			accumulators[key] = acc
		}
		if snapshot.SnapshotAt.After(acc.SnapshotAt) {
			acc.SnapshotAt = snapshot.SnapshotAt
		}
		metricFamily := metricFamilyFor(snapshot.MetricID, metricFamilies)
		acc.Domains[metricFamily] = struct{}{}
		acc.MetricIDs[snapshot.MetricID] = struct{}{}
		acc.ScoreTotal += componentScore
		acc.ScoreCount++
		acc.Components = append(acc.Components, map[string]any{
			"metric_id":          snapshot.MetricID,
			"metric_family":      metricFamily,
			"source_snapshot_id": snapshot.SnapshotID,
			"component_score":    componentScore,
			"metric_value":       roundMetric(snapshot.MetricValue),
			"metric_delta":       roundMetric(snapshot.MetricDelta),
		})
		acc.Evidence = mergeEvidence(acc.Evidence, append([]canonical.Evidence{{Kind: "metric_snapshot", Ref: snapshot.SnapshotID, Value: snapshot.MetricID}}, snapshot.Evidence...))
	}

	rows := make([]CrossDomainRow, 0, len(accumulators))
	for _, acc := range accumulators {
		domains := sortedSet(acc.Domains)
		metricIDs := sortedSet(acc.MetricIDs)
		sort.Slice(acc.Components, func(i, j int) bool {
			left := acc.Components[i]["component_score"].(float64)
			right := acc.Components[j]["component_score"].(float64)
			if left != right {
				return left > right
			}
			return acc.Components[i]["metric_id"].(string) < acc.Components[j]["metric_id"].(string)
		})
		diversityBonus := crossDomainDiversityBonus(len(domains))
		compositeScore := 0.0
		if acc.ScoreCount > 0 {
			compositeScore = roundMetric(acc.ScoreTotal/float64(acc.ScoreCount) + diversityBonus)
		}
		rows = append(rows, CrossDomainRow{
			CrossDomainID:  crossDomainID(acc.SubjectGrain, acc.SubjectID, acc.PlaceID, acc.WindowGrain, acc.WindowStart),
			SubjectGrain:   acc.SubjectGrain,
			SubjectID:      acc.SubjectID,
			PlaceID:        acc.PlaceID,
			WindowGrain:    acc.WindowGrain,
			WindowStart:    acc.WindowStart,
			Domains:        domains,
			CompositeScore: compositeScore,
			SnapshotAt:     acc.SnapshotAt,
			MetricIDs:      metricIDs,
			Attrs: map[string]any{
				"window_grain":    acc.WindowGrain,
				"window_start":    acc.WindowStart.UTC().Format(time.RFC3339Nano),
				"domain_count":    len(domains),
				"metric_count":    len(metricIDs),
				"diversity_bonus": diversityBonus,
				"components":      acc.Components,
			},
			Evidence: acc.Evidence,
		})
	}

	sort.Slice(rows, func(i, j int) bool {
		if rows[i].CompositeScore != rows[j].CompositeScore {
			return rows[i].CompositeScore > rows[j].CompositeScore
		}
		if rows[i].SubjectGrain != rows[j].SubjectGrain {
			return rows[i].SubjectGrain < rows[j].SubjectGrain
		}
		if rows[i].WindowStart != rows[j].WindowStart {
			return rows[i].WindowStart.Before(rows[j].WindowStart)
		}
		return rows[i].SubjectID < rows[j].SubjectID
	})
	return rows
}

func RuntimeAnalyticsRefreshSQL() []string {
	return []string{
		"TRUNCATE TABLE gold.hotspot_snapshot",
		insertHotspotSnapshotSQL(),
		"TRUNCATE TABLE gold.cross_domain_snapshot",
		insertCrossDomainSnapshotSQL(),
	}
}

func insertHotspotSnapshotSQL() string {
	return fmt.Sprintf(`INSERT INTO gold.hotspot_snapshot (hotspot_id, metric_id, scope_type, scope_id, place_id, snapshot_at, window_grain, window_start, window_end, rank, hotspot_score, schema_version, attrs, evidence)
SELECT
    concat('hotspot:', metric_id, ':', scope_type, ':', scope_id, ':', window_grain, ':', toString(toUnixTimestamp64Milli(window_start))) AS hotspot_id,
    metric_id,
    scope_type,
    scope_id,
    place_id,
    snapshot_at,
    window_grain,
    window_start,
    window_end,
    rank,
    round(value_component + delta_component + rank_component, 4) AS hotspot_score,
    toUInt32(%d) AS schema_version,
    concat(
        '{"metric_family":"', replaceAll(metric_family, '"', '\\"'),
        '","source_snapshot_id":"', replaceAll(snapshot_id, '"', '\\"'),
        '","population_size":', toString(population_size),
        ',"value_component":', toString(round(value_component, 4)),
        ',"delta_component":', toString(round(delta_component, 4)),
        ',"rank_component":', toString(round(rank_component, 4)),
        '}'
    ) AS attrs,
    evidence
FROM (
    SELECT
        ranked.snapshot_id,
        ranked.metric_id,
        ranked.scope_type,
        ranked.scope_id,
        ranked.place_id,
        ranked.snapshot_at,
        ranked.window_grain,
        ranked.window_start,
        ranked.window_end,
        ranked.rank,
        ranked.metric_family,
        ranked.population_size,
        ranked.evidence,
        if(ranked.max_metric_value <= 0, 0.0, least(ranked.metric_value / ranked.max_metric_value, 1.0) * 60.0) AS value_component,
        if(ranked.max_positive_delta <= 0, 0.0, least(ranked.positive_delta / ranked.max_positive_delta, 1.0) * 25.0) AS delta_component,
        if(ranked.population_size <= 1, 15.0, (1.0 - (toFloat64(ranked.rank - 1) / toFloat64(ranked.population_size - 1))) * 15.0) AS rank_component
    FROM (
        SELECT
            surface.snapshot_id,
            surface.metric_id,
            surface.subject_grain AS scope_type,
            surface.subject_id AS scope_id,
            surface.place_id,
            surface.snapshot_at,
            surface.window_grain,
            surface.window_start,
            surface.window_end,
            surface.metric_value,
            greatest(surface.metric_delta, 0.0) AS positive_delta,
            row_number() OVER (PARTITION BY surface.metric_id, surface.subject_grain, surface.window_grain, surface.window_start ORDER BY surface.metric_value DESC, surface.subject_id ASC) AS rank,
            max(surface.metric_value) OVER (PARTITION BY surface.metric_id, surface.subject_grain, surface.window_grain, surface.window_start) AS max_metric_value,
            max(greatest(surface.metric_delta, 0.0)) OVER (PARTITION BY surface.metric_id, surface.subject_grain, surface.window_grain, surface.window_start) AS max_positive_delta,
            count() OVER (PARTITION BY surface.metric_id, surface.subject_grain, surface.window_grain, surface.window_start) AS population_size,
            ifNull(registry.metric_family, surface.metric_id) AS metric_family,
            surface.evidence
        FROM (%s) AS surface
        LEFT JOIN (
            SELECT
                metric_id,
                argMax(metric_family, record_version) AS metric_family
            FROM meta.metric_registry
            GROUP BY metric_id
        ) AS registry USING (metric_id)
        WHERE surface.metric_value > 0 OR surface.metric_delta > 0
    ) AS ranked
) AS scored`, SchemaVersion, latestMetricSurfaceSQL())
}

func insertCrossDomainSnapshotSQL() string {
	return fmt.Sprintf(`INSERT INTO gold.cross_domain_snapshot (cross_domain_id, subject_grain, subject_id, place_id, domains, composite_score, snapshot_at, metric_ids, attrs, evidence)
SELECT
    concat('cross:', subject_grain, ':', subject_id, ':', place_id, ':', window_grain, ':', toString(toUnixTimestamp64Milli(window_start))) AS cross_domain_id,
    subject_grain,
    subject_id,
    place_id,
    domains,
    round(avg_component_score + diversity_bonus, 4) AS composite_score,
    snapshot_at,
    metric_ids,
    concat(
        '{"window_grain":"', replaceAll(window_grain, '"', '\\"'),
		'","window_start":"', formatDateTime(window_start, '%%Y-%%m-%%dT%%H:%%i:%%SZ'),
        '","domain_count":', toString(length(domains)),
        ',"metric_count":', toString(length(metric_ids)),
        ',"diversity_bonus":', toString(round(diversity_bonus, 4)),
        ',"domains":', toJSONString(domains),
        ',"metric_ids":', toJSONString(metric_ids),
        '}'
    ) AS attrs,
    concat('[', arrayStringConcat(arrayMap(id -> concat('{"kind":"metric_snapshot","ref":"', replaceAll(id, '"', '\\"'), '","value":"runtime_metric_surface"}'), metric_snapshot_ids), ','), ']') AS evidence
FROM (
    SELECT
        subject_grain,
        subject_id,
        place_id,
        window_grain,
        window_start,
        max(snapshot_at) AS snapshot_at,
        arraySort(groupUniqArray(metric_family)) AS domains,
        round(avg(component_score), 4) AS avg_component_score,
        if(length(arraySort(groupUniqArray(metric_family))) <= 1, 0.0, least(toFloat64(length(arraySort(groupUniqArray(metric_family))) - 1) * 5.0, 10.0)) AS diversity_bonus,
        arraySort(groupUniqArray(metric_id)) AS metric_ids,
        arraySort(groupUniqArray(snapshot_id)) AS metric_snapshot_ids
    FROM (
        SELECT
            scored.snapshot_id,
            scored.metric_id,
            scored.subject_grain,
            scored.subject_id,
            scored.place_id,
            scored.window_grain,
            scored.window_start,
            scored.snapshot_at,
            scored.metric_family,
            round(((if(scored.max_metric_value <= 0, 0.0, least(scored.metric_value / scored.max_metric_value, 1.0)) * 0.7) + (if(scored.max_positive_delta <= 0, 0.0, least(scored.positive_delta / scored.max_positive_delta, 1.0)) * 0.3)) * 100.0, 4) AS component_score
        FROM (
            SELECT
                surface.snapshot_id,
                surface.metric_id,
                surface.subject_grain,
                surface.subject_id,
                surface.place_id,
                surface.window_grain,
                surface.window_start,
                surface.snapshot_at,
                surface.metric_value,
                greatest(surface.metric_delta, 0.0) AS positive_delta,
                max(surface.metric_value) OVER (PARTITION BY surface.metric_id, surface.window_grain, surface.window_start) AS max_metric_value,
                max(greatest(surface.metric_delta, 0.0)) OVER (PARTITION BY surface.metric_id, surface.window_grain, surface.window_start) AS max_positive_delta,
                ifNull(registry.metric_family, surface.metric_id) AS metric_family
            FROM (%s) AS surface
            LEFT JOIN (
                SELECT
                    metric_id,
                    argMax(metric_family, record_version) AS metric_family
                FROM meta.metric_registry
                GROUP BY metric_id
            ) AS registry USING (metric_id)
            WHERE surface.metric_value > 0 OR surface.metric_delta > 0
        ) AS scored
    ) AS grouped
    GROUP BY subject_grain, subject_id, place_id, window_grain, window_start
) AS cross_domain`, latestMetricSurfaceSQL())
}

func latestMetricSurfaceSQL() string {
	return `SELECT
    snapshot_id,
    metric_id,
    subject_grain,
    subject_id,
    place_id,
    window_grain,
    window_start,
    window_end,
    materialization_key,
    snapshot_at,
    metric_value,
    metric_delta,
    rank,
    schema_version,
    attrs,
    evidence
FROM (
    SELECT
        snapshot_id,
        metric_id,
        subject_grain,
        subject_id,
        place_id,
        window_grain,
        window_start,
        window_end,
        materialization_key,
        snapshot_at,
        metric_value,
        metric_delta,
        rank,
        schema_version,
        attrs,
        evidence,
        row_number() OVER (PARTITION BY metric_id, subject_grain, subject_id, place_id, window_grain, window_start ORDER BY snapshot_at DESC, snapshot_id DESC) AS snapshot_version_rank
    FROM gold.metric_snapshot
) AS latest_metric_surface
WHERE snapshot_version_rank = 1`
}

func latestSnapshots(snapshots []SnapshotRow) []SnapshotRow {
	latest := map[string]SnapshotRow{}
	for _, snapshot := range snapshots {
		key := strings.Join([]string{snapshot.MetricID, snapshot.SubjectGrain, snapshot.SubjectID, snapshot.PlaceID, snapshot.WindowGrain, snapshot.WindowStart.UTC().Format(time.RFC3339Nano)}, "|")
		existing, ok := latest[key]
		if !ok || snapshot.SnapshotAt.After(existing.SnapshotAt) || (snapshot.SnapshotAt.Equal(existing.SnapshotAt) && snapshot.SnapshotID > existing.SnapshotID) {
			latest[key] = snapshot
		}
	}
	rows := make([]SnapshotRow, 0, len(latest))
	for _, row := range latest {
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].MetricID != rows[j].MetricID {
			return rows[i].MetricID < rows[j].MetricID
		}
		if rows[i].SubjectGrain != rows[j].SubjectGrain {
			return rows[i].SubjectGrain < rows[j].SubjectGrain
		}
		if rows[i].SubjectID != rows[j].SubjectID {
			return rows[i].SubjectID < rows[j].SubjectID
		}
		if rows[i].WindowGrain != rows[j].WindowGrain {
			return rows[i].WindowGrain < rows[j].WindowGrain
		}
		return rows[i].WindowStart.Before(rows[j].WindowStart)
	})
	return rows
}

func metricSurfaceSummaries(snapshots []SnapshotRow) map[string]metricSurfaceSummary {
	surface := map[string]metricSurfaceSummary{}
	for _, snapshot := range snapshots {
		key := metricSurfaceKey(snapshot)
		summary := surface[key]
		if snapshot.MetricValue > summary.maxValue {
			summary.maxValue = snapshot.MetricValue
		}
		if snapshot.MetricDelta > summary.maxPositiveDelta {
			summary.maxPositiveDelta = snapshot.MetricDelta
		}
		surface[key] = summary
	}
	return surface
}

func metricSurfaceKey(snapshot SnapshotRow) string {
	return strings.Join([]string{snapshot.MetricID, snapshot.WindowGrain, snapshot.WindowStart.UTC().Format(time.RFC3339Nano)}, "|")
}

func hotspotIDForSnapshot(snapshot SnapshotRow) string {
	if snapshot.MaterializationKey != "" {
		return "hotspot:" + snapshot.MaterializationKey
	}
	return "hotspot:" + snapshot.SnapshotID
}

func crossDomainID(subjectGrain, subjectID, placeID, windowGrain string, windowStart time.Time) string {
	return fmt.Sprintf("cross:%s:%s:%s:%s:%d", subjectGrain, subjectID, placeID, windowGrain, windowStart.UTC().UnixMilli())
}

func metricFamilyFor(metricID string, metricFamilies map[string]string) string {
	metricFamily := strings.TrimSpace(metricFamilies[metricID])
	if metricFamily == "" {
		return metricID
	}
	return metricFamily
}

func normalizedRatio(value, maxValue float64) float64 {
	if value <= 0 || maxValue <= 0 {
		return 0
	}
	ratio := value / maxValue
	if ratio > 1 {
		return 1
	}
	if ratio < 0 {
		return 0
	}
	return ratio
}

func rankWeight(rank, populationSize int) float64 {
	if populationSize <= 1 {
		return 1
	}
	weight := 1 - (float64(rank-1) / float64(populationSize-1))
	if weight < 0 {
		return 0
	}
	return weight
}

func sortedSet(items map[string]struct{}) []string {
	values := make([]string, 0, len(items))
	for item := range items {
		values = append(values, item)
	}
	sort.Strings(values)
	return values
}

func crossDomainDiversityBonus(domainCount int) float64 {
	if domainCount <= 1 {
		return 0
	}
	bonus := float64(domainCount-1) * 5
	if bonus > 10 {
		return 10
	}
	return bonus
}

func maxFloat64(left, right float64) float64 {
	if left > right {
		return left
	}
	return right
}
