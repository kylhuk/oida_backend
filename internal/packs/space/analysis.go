package space

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"global-osint-backend/internal/canonical"
	"global-osint-backend/internal/location"
)

const (
	earthMuKM3PerSecond2 = 398600.4418
	earthRadiusKM        = 6378.137
	daySeconds           = 86400.0
)

func Analyze(input Input) (Result, error) {
	start := input.Start.UTC()
	end := input.End.UTC()
	if start.IsZero() || end.IsZero() || !end.After(start) {
		return Result{}, fmt.Errorf("invalid analysis window")
	}
	step := input.Step
	if step <= 0 {
		step = 5 * time.Minute
	}
	result := Result{Start: start, End: end}
	for idx, element := range input.Catalog {
		if err := element.Validate(); err != nil {
			return Result{}, fmt.Errorf("catalog %d: %w", idx+1, err)
		}
		track, err := PropagateGroundTrack(element, start, end, step)
		if err != nil {
			return Result{}, fmt.Errorf("catalog %d: %w", idx+1, err)
		}
		windows, intersections := DetectPlaceIntersections(track, input.Places)
		report := SatelliteReport{
			SatelliteID:   element.SatelliteID(),
			Element:       element,
			Track:         track,
			Windows:       windows,
			Intersections: intersections,
		}
		for _, advisory := range input.Conjunctions {
			if advisory.SatelliteID == report.SatelliteID {
				report.Conjunctions = append(report.Conjunctions, normalizeConjunction(advisory))
			}
		}
		result.Satellites = append(result.Satellites, report)
	}
	result.Metrics = append(result.Metrics, buildOverpassDensityMetrics(result, input.Places)...)
	result.Metrics = append(result.Metrics, buildRevisitCapabilityMetrics(result, input.Places)...)
	result.Metrics = append(result.Metrics, buildCoverageGapMetrics(result, input.Places)...)
	result.Metrics = append(result.Metrics, buildConjunctionRiskMetrics(result)...)
	result.Metrics = append(result.Metrics, buildOrbitalDecayMetrics(result)...)
	result.Metrics = append(result.Metrics, buildManeuverFrequencyMetrics(result)...)
	result.Metrics = append(result.Metrics, buildSatelliteHealthMetrics(result)...)
	sort.Slice(result.Satellites, func(i, j int) bool { return result.Satellites[i].SatelliteID < result.Satellites[j].SatelliteID })
	sort.Slice(result.Metrics, func(i, j int) bool {
		if result.Metrics[i].MetricID != result.Metrics[j].MetricID {
			return result.Metrics[i].MetricID < result.Metrics[j].MetricID
		}
		if result.Metrics[i].SubjectType != result.Metrics[j].SubjectType {
			return result.Metrics[i].SubjectType < result.Metrics[j].SubjectType
		}
		return result.Metrics[i].SubjectID < result.Metrics[j].SubjectID
	})
	return result, nil
}

type placeMetricAggregate struct {
	placeName  string
	passCount  int
	satellites map[string]struct{}
	windowIDs  []string
	windows    []OverpassWindow
	evidence   []canonical.Evidence
}

func PropagateGroundTrack(element ElementSet, start, end time.Time, step time.Duration) ([]TrackPoint, error) {
	if err := element.Validate(); err != nil {
		return nil, err
	}
	if !end.After(start) {
		return nil, fmt.Errorf("end must be after start")
	}
	if step <= 0 {
		return nil, fmt.Errorf("step must be positive")
	}
	satID := element.SatelliteID()
	points := make([]TrackPoint, 0, int(end.Sub(start)/step)+1)
	var alongTrack float64
	var previous *TrackPoint
	for ts := start; !ts.After(end); ts = ts.Add(step) {
		lat, lon, altitude, err := propagatePoint(element, ts)
		if err != nil {
			return nil, err
		}
		point := TrackPoint{
			PointID:       fmt.Sprintf("trk:%s:%d", satID, ts.Unix()),
			SatelliteID:   satID,
			ObservedAt:    ts.UTC(),
			Latitude:      roundMetric(lat),
			Longitude:     roundMetric(lon),
			AltitudeKM:    roundMetric(altitude),
			SchemaVersion: SchemaVersion,
			Attrs: map[string]any{
				"source_format": element.SourceFormat,
				"object_name":   element.ObjectName,
			},
			Evidence: mergeEvidence(element.Evidence, []canonical.Evidence{sampleEvidence(element, ts, lat, lon)}),
		}
		if previous != nil {
			alongTrack += haversineKM(
				location.Coordinate{Lat: previous.Latitude, Lon: previous.Longitude},
				location.Coordinate{Lat: point.Latitude, Lon: point.Longitude},
			)
		}
		point.AlongTrackKM = roundMetric(alongTrack)
		points = append(points, point)
		previous = &points[len(points)-1]
	}
	return points, nil
}

func DetectPlaceIntersections(track []TrackPoint, places []Place) ([]OverpassWindow, []PlaceIntersection) {
	if len(track) == 0 || len(places) == 0 {
		return nil, nil
	}
	sampleStep := time.Duration(0)
	if len(track) > 1 {
		sampleStep = track[1].ObservedAt.Sub(track[0].ObservedAt)
	}
	if sampleStep <= 0 {
		sampleStep = time.Minute
	}
	var windows []OverpassWindow
	var intersections []PlaceIntersection
	for _, place := range places {
		ref := placeReference(place)
		insideWindow := false
		current := OverpassWindow{}
		for _, point := range track {
			coord := location.Coordinate{Lat: point.Latitude, Lon: point.Longitude}
			distance := haversineKM(coord, ref)
			inside := placeContains(place, coord, distance)
			if inside {
				intersections = append(intersections, PlaceIntersection{
					IntersectionID: fmt.Sprintf("ix:%s:%s:%d", point.SatelliteID, place.PlaceID, point.ObservedAt.Unix()),
					SatelliteID:    point.SatelliteID,
					PlaceID:        place.PlaceID,
					ObservedAt:     point.ObservedAt,
					Latitude:       point.Latitude,
					Longitude:      point.Longitude,
					DistanceKM:     roundMetric(distance),
					Inside:         true,
					SchemaVersion:  SchemaVersion,
					Attrs: map[string]any{
						"place_name": place.Name,
						"tags":       append([]string(nil), place.Tags...),
					},
					Evidence: append([]canonical.Evidence(nil), point.Evidence...),
				})
				if !insideWindow {
					insideWindow = true
					current = OverpassWindow{
						WindowID:          fmt.Sprintf("pass:%s:%s:%d", point.SatelliteID, place.PlaceID, point.ObservedAt.Unix()),
						SatelliteID:       point.SatelliteID,
						PlaceID:           place.PlaceID,
						StartedAt:         point.ObservedAt,
						EndedAt:           point.ObservedAt,
						ClosestApproachKM: roundMetric(distance),
						PeakAltitudeKM:    point.AltitudeKM,
						SchemaVersion:     SchemaVersion,
						Evidence:          append([]canonical.Evidence(nil), point.Evidence...),
					}
				}
				current.EndedAt = point.ObservedAt
				current.SampleCount++
				if distance < current.ClosestApproachKM {
					current.ClosestApproachKM = roundMetric(distance)
				}
				if point.AltitudeKM > current.PeakAltitudeKM {
					current.PeakAltitudeKM = point.AltitudeKM
				}
				current.Evidence = mergeEvidence(current.Evidence, point.Evidence)
				continue
			}
			if insideWindow {
				finalizeWindow(&current, place, sampleStep)
				windows = append(windows, current)
				insideWindow = false
			}
		}
		if insideWindow {
			finalizeWindow(&current, place, sampleStep)
			windows = append(windows, current)
		}
	}
	sort.Slice(windows, func(i, j int) bool {
		if windows[i].SatelliteID != windows[j].SatelliteID {
			return windows[i].SatelliteID < windows[j].SatelliteID
		}
		if windows[i].PlaceID != windows[j].PlaceID {
			return windows[i].PlaceID < windows[j].PlaceID
		}
		return windows[i].StartedAt.Before(windows[j].StartedAt)
	})
	sort.Slice(intersections, func(i, j int) bool {
		if intersections[i].SatelliteID != intersections[j].SatelliteID {
			return intersections[i].SatelliteID < intersections[j].SatelliteID
		}
		if intersections[i].PlaceID != intersections[j].PlaceID {
			return intersections[i].PlaceID < intersections[j].PlaceID
		}
		return intersections[i].ObservedAt.Before(intersections[j].ObservedAt)
	})
	return windows, intersections
}

func propagatePoint(element ElementSet, observedAt time.Time) (float64, float64, float64, error) {
	n := element.MeanMotionRevPerDay * 2 * math.Pi / daySeconds
	if n <= 0 {
		return 0, 0, 0, fmt.Errorf("invalid mean motion")
	}
	a := math.Cbrt(earthMuKM3PerSecond2 / (n * n))
	e := element.Eccentricity
	meanAnomaly := degreesToRadians(element.MeanAnomalyDeg) + n*observedAt.Sub(element.Epoch).Seconds()
	eccentricAnomaly := solveKepler(meanAnomaly, e)
	trueAnomaly := 2 * math.Atan2(
		math.Sqrt(1+e)*math.Sin(eccentricAnomaly/2),
		math.Sqrt(1-e)*math.Cos(eccentricAnomaly/2),
	)
	radius := a * (1 - e*math.Cos(eccentricAnomaly))
	xOrb := radius * math.Cos(trueAnomaly)
	yOrb := radius * math.Sin(trueAnomaly)
	inc := degreesToRadians(element.InclinationDeg)
	raan := degreesToRadians(element.RAANDeg)
	argPerigee := degreesToRadians(element.ArgPerigeeDeg)
	cosO := math.Cos(raan)
	sinO := math.Sin(raan)
	cosI := math.Cos(inc)
	sinI := math.Sin(inc)
	cosW := math.Cos(argPerigee)
	sinW := math.Sin(argPerigee)
	xECI := xOrb*(cosO*cosW-sinO*sinW*cosI) - yOrb*(cosO*sinW+sinO*cosW*cosI)
	yECI := xOrb*(sinO*cosW+cosO*sinW*cosI) + yOrb*(cosO*cosW*cosI-sinO*sinW)
	zECI := xOrb*(sinW*sinI) + yOrb*(cosW*sinI)
	gmst := greenwichMeanSidereal(observedAt)
	xECEF := xECI*math.Cos(gmst) + yECI*math.Sin(gmst)
	yECEF := -xECI*math.Sin(gmst) + yECI*math.Cos(gmst)
	zECEF := zECI
	longitude := radiansToDegrees(math.Atan2(yECEF, xECEF))
	horizontal := math.Sqrt(xECEF*xECEF + yECEF*yECEF)
	latitude := radiansToDegrees(math.Atan2(zECEF, horizontal))
	altitude := math.Sqrt(xECEF*xECEF+yECEF*yECEF+zECEF*zECEF) - earthRadiusKM
	return latitude, wrapLongitude(longitude), altitude, nil
}

func buildOverpassDensityMetrics(result Result, places []Place) []Metric {
	aggregates, hours := buildPlaceMetricAggregates(result, places)
	if hours <= 0 {
		return nil
	}
	metrics := make([]Metric, 0, len(aggregates))
	for placeID, agg := range aggregates {
		metrics = append(metrics, Metric{
			MetricID:      "overpass_density_score",
			SubjectType:   "place",
			SubjectID:     placeID,
			WindowStart:   result.Start,
			WindowEnd:     result.End,
			Value:         roundMetric(float64(agg.passCount) * 24 / hours),
			SchemaVersion: SchemaVersion,
			Attrs: map[string]any{
				"place_name":      agg.placeName,
				"pass_count":      agg.passCount,
				"satellite_count": len(agg.satellites),
				"horizon_hours":   roundMetric(hours),
				"window_ids":      append([]string(nil), agg.windowIDs...),
				"explainability": map[string]any{
					"feature_contributions": []map[string]any{{
						"feature": "pass_count",
						"value":   agg.passCount,
						"weight":  roundMetric(hours),
					}},
					"evidence_refs": evidenceRefs(agg.evidence),
				},
			},
			Evidence: agg.evidence,
		})
	}
	return metrics
}

func buildRevisitCapabilityMetrics(result Result, places []Place) []Metric {
	aggregates, hours := buildPlaceMetricAggregates(result, places)
	if hours <= 0 {
		return nil
	}
	metrics := make([]Metric, 0, len(aggregates))
	for placeID, agg := range aggregates {
		coverageGapHours, averageGapHours := windowGapHours(result.Start, result.End, agg.windows)
		value := clamp((1-coverageGapHours/hours)*70+math.Min(float64(len(agg.satellites))*15, 30), 0, 100)
		metrics = append(metrics, Metric{
			MetricID:      "revisit_capability_index",
			SubjectType:   "place",
			SubjectID:     placeID,
			WindowStart:   result.Start,
			WindowEnd:     result.End,
			Value:         roundMetric(value),
			SchemaVersion: SchemaVersion,
			Attrs: map[string]any{
				"place_name":         agg.placeName,
				"coverage_gap_hours": roundMetric(coverageGapHours),
				"average_gap_hours":  roundMetric(averageGapHours),
				"satellite_count":    len(agg.satellites),
				"window_ids":         append([]string(nil), agg.windowIDs...),
				"explainability": map[string]any{
					"feature_contributions": []map[string]any{
						{"feature": "coverage_gap_hours", "value": roundMetric(coverageGapHours), "weight": roundMetric(hours)},
						{"feature": "satellite_count", "value": len(agg.satellites), "weight": 15.0},
					},
					"evidence_refs": evidenceRefs(agg.evidence),
				},
			},
			Evidence: agg.evidence,
		})
	}
	return metrics
}

func buildCoverageGapMetrics(result Result, places []Place) []Metric {
	aggregates, hours := buildPlaceMetricAggregates(result, places)
	if hours <= 0 {
		return nil
	}
	metrics := make([]Metric, 0, len(aggregates))
	for placeID, agg := range aggregates {
		coverageGapHours, averageGapHours := windowGapHours(result.Start, result.End, agg.windows)
		metrics = append(metrics, Metric{
			MetricID:      "coverage_gap_hours",
			SubjectType:   "place",
			SubjectID:     placeID,
			WindowStart:   result.Start,
			WindowEnd:     result.End,
			Value:         roundMetric(coverageGapHours),
			SchemaVersion: SchemaVersion,
			Attrs: map[string]any{
				"place_name":        agg.placeName,
				"average_gap_hours": roundMetric(averageGapHours),
				"pass_count":        agg.passCount,
				"satellite_count":   len(agg.satellites),
				"window_ids":        append([]string(nil), agg.windowIDs...),
				"explainability": map[string]any{
					"feature_contributions": []map[string]any{{
						"feature": "largest_gap_hours",
						"value":   roundMetric(coverageGapHours),
						"weight":  roundMetric(hours),
					}},
					"evidence_refs": evidenceRefs(agg.evidence),
				},
			},
			Evidence: agg.evidence,
		})
	}
	return metrics
}

func buildConjunctionRiskMetrics(result Result) []Metric {
	metrics := make([]Metric, 0, len(result.Satellites))
	for _, satellite := range result.Satellites {
		value := 0.0
		closestMiss := 0.0
		meanProbability := 0.0
		evidence := satellite.Element.Evidence
		refs := evidenceRefs(evidence)
		if len(satellite.Conjunctions) > 0 {
			closestMiss = satellite.Conjunctions[0].MissDistanceKM
			for _, advisory := range satellite.Conjunctions {
				value += conjunctionSeverity(advisory)
				meanProbability += advisory.Probability
				if advisory.MissDistanceKM < closestMiss {
					closestMiss = advisory.MissDistanceKM
				}
				evidence = mergeEvidence(evidence, advisory.Evidence)
			}
			value /= float64(len(satellite.Conjunctions))
			meanProbability /= float64(len(satellite.Conjunctions))
			refs = evidenceRefs(evidence)
		}
		metrics = append(metrics, Metric{
			MetricID:      "conjunction_risk_score",
			SubjectType:   "satellite",
			SubjectID:     satellite.SatelliteID,
			WindowStart:   result.Start,
			WindowEnd:     result.End,
			Value:         roundMetric(value),
			SchemaVersion: SchemaVersion,
			Attrs: map[string]any{
				"advisory_count":           len(satellite.Conjunctions),
				"closest_miss_distance_km": roundMetric(closestMiss),
				"mean_probability":         roundProbability(meanProbability),
				"explainability": map[string]any{
					"feature_contributions": []map[string]any{{
						"feature": "advisory_count",
						"value":   len(satellite.Conjunctions),
						"weight":  1.0,
					}},
					"evidence_refs": refs,
				},
			},
			Evidence: evidence,
		})
	}
	return metrics
}

func buildOrbitalDecayMetrics(result Result) []Metric {
	metrics := make([]Metric, 0, len(result.Satellites))
	for _, satellite := range result.Satellites {
		meanAltitude, dragComponent, altitudeComponent, meanMotionComponent := orbitalDecayComponents(satellite)
		value := roundMetric(clamp(dragComponent+altitudeComponent+meanMotionComponent, 0, 100))
		evidence := append([]canonical.Evidence(nil), satellite.Element.Evidence...)
		metrics = append(metrics, Metric{
			MetricID:      "orbital_decay_indicator",
			SubjectType:   "satellite",
			SubjectID:     satellite.SatelliteID,
			WindowStart:   result.Start,
			WindowEnd:     result.End,
			Value:         value,
			SchemaVersion: SchemaVersion,
			Attrs: map[string]any{
				"mean_altitude_km":        roundMetric(meanAltitude),
				"bstar":                   roundMetric(math.Abs(satellite.Element.BStar)),
				"mean_motion_rev_per_day": roundMetric(satellite.Element.MeanMotionRevPerDay),
				"explainability": map[string]any{
					"feature_contributions": []map[string]any{
						{"feature": "drag_component", "value": roundMetric(dragComponent), "weight": 1.0},
						{"feature": "altitude_component", "value": roundMetric(altitudeComponent), "weight": 1.0},
						{"feature": "mean_motion_component", "value": roundMetric(meanMotionComponent), "weight": 1.0},
					},
					"evidence_refs": evidenceRefs(evidence),
				},
			},
			Evidence: evidence,
		})
	}
	return metrics
}

func buildManeuverFrequencyMetrics(result Result) []Metric {
	metrics := make([]Metric, 0, len(result.Satellites))
	for _, satellite := range result.Satellites {
		maneuverCount := metricAttrFloat(satellite.Element.Attrs, "maneuver_count_30d", "maneuvers_30d", "planned_maneuver_count")
		recentFirings := metricAttrFloat(satellite.Element.Attrs, "thruster_firings_7d", "recent_thruster_firings")
		value := roundMetric(clamp(maneuverCount*18+recentFirings*4, 0, 100))
		evidence := append([]canonical.Evidence(nil), satellite.Element.Evidence...)
		metrics = append(metrics, Metric{
			MetricID:      "maneuver_frequency_score",
			SubjectType:   "satellite",
			SubjectID:     satellite.SatelliteID,
			WindowStart:   result.Start,
			WindowEnd:     result.End,
			Value:         value,
			SchemaVersion: SchemaVersion,
			Attrs: map[string]any{
				"maneuver_count_30d":  maneuverCount,
				"thruster_firings_7d": recentFirings,
				"explainability": map[string]any{
					"feature_contributions": []map[string]any{
						{"feature": "maneuver_count_30d", "value": maneuverCount, "weight": 18.0},
						{"feature": "thruster_firings_7d", "value": recentFirings, "weight": 4.0},
					},
					"evidence_refs": evidenceRefs(evidence),
				},
			},
			Evidence: evidence,
		})
	}
	return metrics
}

func buildSatelliteHealthMetrics(result Result) []Metric {
	metrics := make([]Metric, 0, len(result.Satellites))
	for _, satellite := range result.Satellites {
		decayIndicator := roundMetric(clamp(orbitalDecayValue(satellite), 0, 100))
		declaredHealth := metricAttrFloat(satellite.Element.Attrs, "health_score", "satellite_health_index")
		uptime := metricAttrFloat(satellite.Element.Attrs, "uptime_pct")
		if uptime == 0 {
			uptime = 92
		}
		batteryMargin := metricAttrFloat(satellite.Element.Attrs, "battery_margin_pct", "power_margin_pct")
		if batteryMargin == 0 {
			batteryMargin = 80
		}
		anomalyCount := metricAttrFloat(satellite.Element.Attrs, "anomaly_count_30d", "anomalies_30d")
		activeTransmitters := activeTransmitterCount(satellite.Element.Transmitters)
		transmitterWeight := 0.0
		if len(satellite.Element.Transmitters) > 0 {
			transmitterWeight = (float64(activeTransmitters) / float64(len(satellite.Element.Transmitters))) * 10
		}
		value := declaredHealth
		if value == 0 {
			value = uptime*0.55 + batteryMargin*0.25 + transmitterWeight + 10 - anomalyCount*8 - decayIndicator*0.2
		} else {
			value = value + transmitterWeight - anomalyCount*6 - decayIndicator*0.15
		}
		value = roundMetric(clamp(value, 0, 100))
		evidence := append([]canonical.Evidence(nil), satellite.Element.Evidence...)
		metrics = append(metrics, Metric{
			MetricID:      "satellite_health_index",
			SubjectType:   "satellite",
			SubjectID:     satellite.SatelliteID,
			WindowStart:   result.Start,
			WindowEnd:     result.End,
			Value:         value,
			SchemaVersion: SchemaVersion,
			Attrs: map[string]any{
				"declared_health_score":   declaredHealth,
				"uptime_pct":              roundMetric(uptime),
				"battery_margin_pct":      roundMetric(batteryMargin),
				"anomaly_count_30d":       anomalyCount,
				"active_transmitters":     activeTransmitters,
				"transmitter_count":       len(satellite.Element.Transmitters),
				"orbital_decay_indicator": decayIndicator,
				"explainability": map[string]any{
					"feature_contributions": []map[string]any{
						{"feature": "uptime_pct", "value": roundMetric(uptime), "weight": 0.55},
						{"feature": "battery_margin_pct", "value": roundMetric(batteryMargin), "weight": 0.25},
						{"feature": "anomaly_count_30d", "value": anomalyCount, "weight": -8.0},
						{"feature": "orbital_decay_indicator", "value": decayIndicator, "weight": -0.2},
					},
					"evidence_refs": evidenceRefs(evidence),
				},
			},
			Evidence: evidence,
		})
	}
	return metrics
}

func buildPlaceMetricAggregates(result Result, places []Place) (map[string]*placeMetricAggregate, float64) {
	hours := result.End.Sub(result.Start).Hours()
	if hours <= 0 {
		return nil, hours
	}
	placeNames := map[string]string{}
	for _, place := range places {
		placeNames[place.PlaceID] = place.Name
	}
	aggregates := map[string]*placeMetricAggregate{}
	for _, satellite := range result.Satellites {
		for _, window := range satellite.Windows {
			agg, ok := aggregates[window.PlaceID]
			if !ok {
				agg = &placeMetricAggregate{placeName: placeNames[window.PlaceID], satellites: map[string]struct{}{}}
				aggregates[window.PlaceID] = agg
			}
			agg.passCount++
			agg.satellites[satellite.SatelliteID] = struct{}{}
			agg.windowIDs = append(agg.windowIDs, window.WindowID)
			agg.windows = append(agg.windows, window)
			agg.evidence = mergeEvidence(agg.evidence, window.Evidence)
		}
	}
	return aggregates, hours
}

func windowGapHours(start, end time.Time, windows []OverpassWindow) (float64, float64) {
	if !end.After(start) {
		return 0, 0
	}
	if len(windows) == 0 {
		hours := end.Sub(start).Hours()
		return roundMetric(hours), roundMetric(hours)
	}
	sorted := append([]OverpassWindow(nil), windows...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].StartedAt.Before(sorted[j].StartedAt) })
	maxGap := math.Max(0, sorted[0].StartedAt.Sub(start).Hours())
	totalGap := maxGap
	gapCount := 1
	lastCoveredAt := sorted[0].EndedAt
	if lastCoveredAt.Before(sorted[0].StartedAt) {
		lastCoveredAt = sorted[0].StartedAt
	}
	for _, window := range sorted[1:] {
		gap := window.StartedAt.Sub(lastCoveredAt).Hours()
		if gap < 0 {
			gap = 0
		}
		if gap > maxGap {
			maxGap = gap
		}
		totalGap += gap
		gapCount++
		if window.EndedAt.After(lastCoveredAt) {
			lastCoveredAt = window.EndedAt
		}
	}
	tailGap := end.Sub(lastCoveredAt).Hours()
	if tailGap < 0 {
		tailGap = 0
	}
	if tailGap > maxGap {
		maxGap = tailGap
	}
	totalGap += tailGap
	gapCount++
	averageGap := totalGap / float64(gapCount)
	return roundMetric(maxGap), roundMetric(averageGap)
}

func meanTrackAltitude(track []TrackPoint) float64 {
	if len(track) == 0 {
		return 0
	}
	total := 0.0
	for _, point := range track {
		total += point.AltitudeKM
	}
	return total / float64(len(track))
}

func orbitalDecayComponents(report SatelliteReport) (float64, float64, float64, float64) {
	meanAltitude := meanTrackAltitude(report.Track)
	dragComponent := math.Min(math.Abs(report.Element.BStar)*1e6, 60)
	altitudeComponent := math.Max(0, 450-meanAltitude) * 0.12
	meanMotionComponent := clamp((report.Element.MeanMotionRevPerDay-12)*5, 0, 20)
	return meanAltitude, dragComponent, altitudeComponent, meanMotionComponent
}

func orbitalDecayValue(report SatelliteReport) float64 {
	_, dragComponent, altitudeComponent, meanMotionComponent := orbitalDecayComponents(report)
	return dragComponent + altitudeComponent + meanMotionComponent
}

func metricAttrFloat(attrs map[string]any, keys ...string) float64 {
	for _, key := range keys {
		value, ok := attrs[key]
		if !ok {
			continue
		}
		switch candidate := value.(type) {
		case float64:
			return candidate
		case float32:
			return float64(candidate)
		case int:
			return float64(candidate)
		case int64:
			return float64(candidate)
		case string:
			parsed, err := strconv.ParseFloat(strings.TrimSpace(candidate), 64)
			if err == nil {
				return parsed
			}
		}
	}
	return 0
}

func activeTransmitterCount(transmitters []Transmitter) int {
	count := 0
	for _, transmitter := range transmitters {
		status := strings.ToLower(strings.TrimSpace(transmitter.Status))
		if status == "" || status == "active" || status == "nominal" || status == "online" {
			count++
		}
	}
	return count
}

func finalizeWindow(window *OverpassWindow, place Place, sampleStep time.Duration) {
	window.Duration = window.EndedAt.Sub(window.StartedAt) + sampleStep
	window.ClosestApproachKM = roundMetric(window.ClosestApproachKM)
	window.Attrs = map[string]any{
		"place_name":          place.Name,
		"tags":                append([]string(nil), place.Tags...),
		"sample_count":        window.SampleCount,
		"duration_minutes":    roundMetric(window.Duration.Minutes()),
		"peak_altitude_km":    roundMetric(window.PeakAltitudeKM),
		"closest_approach_km": roundMetric(window.ClosestApproachKM),
	}
}

func normalizeConjunction(advisory ConjunctionAdvisory) ConjunctionAdvisory {
	advisory.ClosestApproachAt = advisory.ClosestApproachAt.UTC()
	advisory.MissDistanceKM = roundMetric(advisory.MissDistanceKM)
	advisory.Probability = roundProbability(advisory.Probability)
	if advisory.Evidence == nil {
		advisory.Evidence = []canonical.Evidence{{
			Kind:  "conjunction",
			Ref:   advisory.AdvisoryID,
			Value: advisory.SatelliteID,
		}}
	}
	return advisory
}

func placeContains(place Place, point location.Coordinate, distanceKM float64) bool {
	if place.BBox != nil && place.BBox.Contains(point) {
		return true
	}
	if place.RadiusKM > 0 && distanceKM <= place.RadiusKM {
		return true
	}
	return false
}

func placeReference(place Place) location.Coordinate {
	if place.Center != (location.Coordinate{}) {
		return place.Center
	}
	if place.BBox != nil {
		return place.BBox.Centroid()
	}
	return location.Coordinate{}
}

func sampleEvidence(element ElementSet, observedAt time.Time, lat, lon float64) canonical.Evidence {
	return canonical.Evidence{
		Kind:  "orbit_sample",
		Ref:   fmt.Sprintf("%s:%d", element.SatelliteID(), observedAt.Unix()),
		Value: fmt.Sprintf("%.4f,%.4f", lat, lon),
		Attrs: map[string]any{
			"observed_at": observedAt.UTC().Format(time.RFC3339),
			"source":      element.SourceFormat,
		},
	}
}

func evidenceRefs(evidence []canonical.Evidence) []string {
	refs := make([]string, 0, len(evidence))
	seen := map[string]struct{}{}
	for _, item := range evidence {
		ref := strings.TrimSpace(item.Ref)
		if ref == "" {
			ref = strings.TrimSpace(item.RawID)
		}
		if ref == "" {
			continue
		}
		if _, ok := seen[ref]; ok {
			continue
		}
		seen[ref] = struct{}{}
		refs = append(refs, ref)
	}
	return refs
}

func mergeEvidence(base, extra []canonical.Evidence) []canonical.Evidence {
	if len(extra) == 0 {
		return append([]canonical.Evidence(nil), base...)
	}
	out := append([]canonical.Evidence(nil), base...)
	seen := map[string]struct{}{}
	for _, item := range out {
		seen[evidenceKey(item)] = struct{}{}
	}
	for _, item := range extra {
		key := evidenceKey(item)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, item)
	}
	return out
}

func evidenceKey(item canonical.Evidence) string {
	return strings.Join([]string{item.Kind, item.Ref, item.RawID, item.Value}, "|")
}

func conjunctionSeverity(advisory ConjunctionAdvisory) float64 {
	probabilityComponent := clamp(advisory.Probability/0.001, 0, 1)
	distanceComponent := 1 - clamp(advisory.MissDistanceKM/25, 0, 1)
	return roundMetric(0.6*probabilityComponent + 0.4*distanceComponent)
}

func solveKepler(meanAnomaly, eccentricity float64) float64 {
	value := math.Mod(meanAnomaly, 2*math.Pi)
	if value < 0 {
		value += 2 * math.Pi
	}
	current := value
	for i := 0; i < 10; i++ {
		delta := (current - eccentricity*math.Sin(current) - value) / (1 - eccentricity*math.Cos(current))
		current -= delta
		if math.Abs(delta) < 1e-10 {
			break
		}
	}
	return current
}

func greenwichMeanSidereal(ts time.Time) float64 {
	seconds := float64(ts.Unix()) + float64(ts.Nanosecond())/1e9
	jd := seconds/daySeconds + 2440587.5
	centuries := (jd - 2451545.0) / 36525.0
	degrees := 280.46061837 + 360.98564736629*(jd-2451545.0) + 0.000387933*centuries*centuries - centuries*centuries*centuries/38710000.0
	return degreesToRadians(math.Mod(degrees, 360))
}

func wrapLongitude(value float64) float64 {
	for value > 180 {
		value -= 360
	}
	for value <= -180 {
		value += 360
	}
	return value
}

func degreesToRadians(value float64) float64 {
	return value * math.Pi / 180
}

func radiansToDegrees(value float64) float64 {
	return value * 180 / math.Pi
}

func haversineKM(a, b location.Coordinate) float64 {
	dLat := degreesToRadians(b.Lat - a.Lat)
	dLon := degreesToRadians(b.Lon - a.Lon)
	lat1 := degreesToRadians(a.Lat)
	lat2 := degreesToRadians(b.Lat)
	h := math.Sin(dLat/2)*math.Sin(dLat/2) + math.Cos(lat1)*math.Cos(lat2)*math.Sin(dLon/2)*math.Sin(dLon/2)
	return 2 * earthRadiusKM * math.Asin(math.Sqrt(h))
}

func roundMetric(value float64) float64 {
	return math.Round(value*10000) / 10000
}

func roundProbability(value float64) float64 {
	return math.Round(value*1e8) / 1e8
}

func clamp(value, min, max float64) float64 {
	if value < min {
		return min
	}
	if value > max {
		return max
	}
	return value
}
