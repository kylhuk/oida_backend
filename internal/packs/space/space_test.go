package space

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"global-osint-backend/internal/canonical"
	"global-osint-backend/internal/location"
)

const (
	spaceEvidencePath     = ".sisyphus/evidence/task-24-space.txt"
	spaceEdgeEvidencePath = ".sisyphus/evidence/task-24-space-edge.txt"
)

func TestSpacePackHappyPath(t *testing.T) {
	start := time.Date(2026, time.March, 10, 0, 0, 0, 0, time.UTC)
	tles, err := ParseTLEFeed([]byte(sampleTLEFeed()))
	if err != nil {
		t.Fatalf("parse TLE: %v", err)
	}
	omm, err := ParseOMMFeed([]byte(sampleOMMFeed()))
	if err != nil {
		t.Fatalf("parse OMM: %v", err)
	}
	if len(tles) != 1 || len(omm) != 1 {
		t.Fatalf("expected 1 TLE and 1 OMM record, got %d and %d", len(tles), len(omm))
	}
	if got := len(omm[0].Transmitters); got != 2 {
		t.Fatalf("expected 2 transmitters from OMM envelope, got %d", got)
	}
	if got, ok := omm[0].Attrs["maneuver_count_30d"].(float64); !ok || got != 3 {
		t.Fatalf("expected maneuver_count_30d=3, got %#v", omm[0].Attrs["maneuver_count_30d"])
	}
	sourceEnvelope, ok := omm[0].Attrs["source_envelope"].(map[string]any)
	if !ok {
		t.Fatalf("expected source envelope attrs, got %#v", omm[0].Attrs)
	}
	metadata, ok := sourceEnvelope["metadata"].(map[string]any)
	if !ok {
		t.Fatalf("expected source envelope metadata, got %#v", sourceEnvelope)
	}
	if metadata["source_id"] != "fixture:space:catalog" {
		t.Fatalf("expected source envelope source_id fixture:space:catalog, got %#v", metadata["source_id"])
	}
	input := Input{
		Catalog:      append(tles, omm...),
		Places:       samplePlaces(),
		Conjunctions: sampleConjunctions(start),
		Start:        start,
		End:          start.Add(4 * time.Hour),
		Step:         5 * time.Minute,
	}
	result, err := Analyze(input)
	if err != nil {
		t.Fatalf("analyze: %v", err)
	}
	if len(result.Satellites) != 2 {
		t.Fatalf("expected 2 satellite reports, got %d", len(result.Satellites))
	}
	if len(result.Metrics) != 14 {
		t.Fatalf("expected 14 exact metrics, got %d", len(result.Metrics))
	}
	equatorial := mustSatellite(t, result, "sat:99991")
	inclined := mustSatellite(t, result, "sat:25544")
	if len(equatorial.Track) < 10 || len(inclined.Track) < 10 {
		t.Fatal("expected dense ground track samples")
	}
	if equatorial.Track[0].AltitudeKM <= 100 || inclined.Track[0].AltitudeKM <= 100 {
		t.Fatal("expected low-earth orbit altitude")
	}
	if countWindows(equatorial.Windows, "plc:eq-belt") == 0 {
		t.Fatal("expected equatorial overpass window for plc:eq-belt")
	}
	if countWindows(inclined.Windows, "plc:polar-watch") == 0 {
		t.Fatal("expected inclined overpass window for plc:polar-watch")
	}
	if countIntersections(equatorial.Intersections, "plc:eq-belt") == 0 {
		t.Fatal("expected equatorial place intersections")
	}
	if countIntersections(inclined.Intersections, "plc:polar-watch") == 0 {
		t.Fatal("expected inclined place intersections")
	}
	for _, metricID := range []string{
		"conjunction_risk_score",
		"coverage_gap_hours",
		"maneuver_frequency_score",
		"orbital_decay_indicator",
		"overpass_density_score",
		"revisit_capability_index",
		"satellite_health_index",
	} {
		if !hasMetricID(result.Metrics, metricID) {
			t.Fatalf("missing exact metric %q", metricID)
		}
	}
	for _, legacyID := range []string{"overpass_density", "conjunction_risk"} {
		if hasMetricID(result.Metrics, legacyID) {
			t.Fatalf("did not expect legacy metric %q in analysis output", legacyID)
		}
	}
	eqDensity := metricValue(result.Metrics, "overpass_density_score", "place", "plc:eq-belt")
	polarDensity := metricValue(result.Metrics, "overpass_density_score", "place", "plc:polar-watch")
	if eqDensity <= 0 || polarDensity <= 0 {
		t.Fatalf("expected positive overpass density metrics, got %v and %v", eqDensity, polarDensity)
	}
	eqRevisit := metricValue(result.Metrics, "revisit_capability_index", "place", "plc:eq-belt")
	eqCoverageGap := metricValue(result.Metrics, "coverage_gap_hours", "place", "plc:eq-belt")
	if eqRevisit <= 0 || eqCoverageGap <= 0 {
		t.Fatalf("expected revisit and coverage gap metrics, got %v and %v", eqRevisit, eqCoverageGap)
	}
	eqRisk := metricValue(result.Metrics, "conjunction_risk_score", "satellite", "sat:99991")
	inclinedRisk := metricValue(result.Metrics, "conjunction_risk_score", "satellite", "sat:25544")
	if eqRisk <= inclinedRisk {
		t.Fatalf("expected higher conjunction risk for sat:99991, got %v <= %v", eqRisk, inclinedRisk)
	}
	if got := metricValue(result.Metrics, "orbital_decay_indicator", "satellite", "sat:25544"); got <= 0 {
		t.Fatalf("expected positive orbital_decay_indicator for sat:25544, got %v", got)
	}
	if got := metricValue(result.Metrics, "maneuver_frequency_score", "satellite", "sat:25544"); got <= 0 {
		t.Fatalf("expected positive maneuver_frequency_score for sat:25544, got %v", got)
	}
	if got := metricValue(result.Metrics, "satellite_health_index", "satellite", "sat:25544"); got <= 0 {
		t.Fatalf("expected positive satellite_health_index for sat:25544, got %v", got)
	}

	var evidence strings.Builder
	evidence.WriteString("satellite_id\ttrack_points\twindows\tintersections\tconjunctions\n")
	for _, satellite := range result.Satellites {
		evidence.WriteString(fmt.Sprintf("%s\t%d\t%d\t%d\t%d\n", satellite.SatelliteID, len(satellite.Track), len(satellite.Windows), len(satellite.Intersections), len(satellite.Conjunctions)))
	}
	evidence.WriteString("\nmetric_id\tsubject_type\tsubject_id\tvalue\n")
	for _, metric := range result.Metrics {
		evidence.WriteString(fmt.Sprintf("%s\t%s\t%s\t%.4f\n", metric.MetricID, metric.SubjectType, metric.SubjectID, metric.Value))
	}
	writeEvidenceFile(t, spaceEvidencePath, []byte(evidence.String()))
}

func TestSpacePackNearThresholdStaysDeterministic(t *testing.T) {
	start := time.Date(2026, time.March, 10, 0, 0, 0, 0, time.UTC)
	omm, err := ParseOMMFeed([]byte(sampleOMMFeed()))
	if err != nil {
		t.Fatalf("parse OMM: %v", err)
	}
	input := Input{
		Catalog: []ElementSet{omm[0]},
		Places: []Place{{
			PlaceID: "plc:near-threshold",
			Name:    "Near Threshold",
			BBox:    &location.BBox{MinLat: 52.2, MinLon: -180, MaxLat: 54.0, MaxLon: 180},
		}},
		Conjunctions: []ConjunctionAdvisory{{
			AdvisoryID:        "cdm:near-threshold",
			SatelliteID:       "sat:25544",
			SecondaryNORADID:  "43210",
			ClosestApproachAt: start.Add(2 * time.Hour),
			MissDistanceKM:    24.9,
			Probability:       0.00001,
			SourceID:          "fixture:cdm",
			Evidence:          []canonical.Evidence{{Kind: "conjunction", Ref: "cdm:near-threshold", Value: "edge"}},
		}},
		Start: start,
		End:   start.Add(3 * time.Hour),
		Step:  5 * time.Minute,
	}
	result, err := Analyze(input)
	if err != nil {
		t.Fatalf("analyze: %v", err)
	}
	report := mustSatellite(t, result, "sat:25544")
	if len(report.Windows) != 0 {
		t.Fatalf("expected no windows near threshold, got %d", len(report.Windows))
	}
	if len(report.Intersections) != 0 {
		t.Fatalf("expected no intersections near threshold, got %d", len(report.Intersections))
	}
	risk := metricValue(result.Metrics, "conjunction_risk_score", "satellite", "sat:25544")
	if risk != 0.0076 {
		t.Fatalf("expected deterministic conjunction risk 0.0076, got %v", risk)
	}
	evidence := fmt.Sprintf("satellite_id\twindows\tintersections\tconjunction_risk_score\n%s\t%d\t%d\t%.4f\n", report.SatelliteID, len(report.Windows), len(report.Intersections), risk)
	writeEvidenceFile(t, spaceEdgeEvidencePath, []byte(evidence))
}

func sampleTLEFeed() string {
	return strings.Join([]string{
		"EQUATOR-OBS-1",
		"1 99991U 26069A   26069.00000000  .00000000  00000-0  00000-0 0  9991",
		"2 99991   0.0000  15.0000 0001000   0.0000   0.0000 15.00000000   101",
	}, "\n")
}

func sampleOMMFeed() string {
	return `{
	  "metadata": {
	    "source_id": "fixture:space:catalog",
	    "supported_metrics": [
	      "overpass_density_score",
	      "conjunction_risk_score",
	      "revisit_capability_index",
	      "orbital_decay_indicator",
	      "maneuver_frequency_score",
	      "coverage_gap_hours",
	      "satellite_health_index"
	    ]
	  },
	  "objects": [
	    {
	      "OBJECT_NAME": "ISS-LIKE-1",
	      "OBJECT_ID": "1998-067A",
	      "NORAD_CAT_ID": "25544",
	      "CLASSIFICATION_TYPE": "U",
	      "EPOCH": "2026-03-10T00:00:00Z",
	      "MEAN_MOTION": "15.50000000",
	      "ECCENTRICITY": "0.0007000",
	      "INCLINATION": "51.6400",
	      "RA_OF_ASC_NODE": "120.0000",
	      "ARG_OF_PERICENTER": "80.0000",
	      "MEAN_ANOMALY": "30.0000",
	      "BSTAR": "0.00002100",
	      "MANEUVER_COUNT_30D": 3,
	      "THRUSTER_FIRINGS_7D": 2,
	      "HEALTH_SCORE": 91,
	      "BATTERY_MARGIN_PCT": 88,
	      "UPTIME_PCT": 97,
	      "ANOMALY_COUNT_30D": 1,
	      "TRANSMITTERS": [
	        {"callsign": "ISS-DL1", "mode": "s-band", "downlink_mhz": 2210.0, "status": "active"},
	        {"callsign": "ISS-DL2", "mode": "uhf", "downlink_mhz": 437.8, "status": "nominal"}
	      ]
	    }
	  ]
	}`
}

func samplePlaces() []Place {
	return []Place{
		{
			PlaceID:  "plc:eq-belt",
			Name:     "Equatorial Belt",
			Center:   location.Coordinate{Lat: 0, Lon: 0},
			BBox:     &location.BBox{MinLat: -3, MinLon: -40, MaxLat: 3, MaxLon: 40},
			RadiusKM: 4500,
			Tags:     []string{"critical_infrastructure"},
		},
		{
			PlaceID:  "plc:polar-watch",
			Name:     "Polar Watch",
			Center:   location.Coordinate{Lat: 50, Lon: 0},
			BBox:     &location.BBox{MinLat: 45, MinLon: -180, MaxLat: 55, MaxLon: 180},
			RadiusKM: 2500,
			Tags:     []string{"maritime"},
		},
	}
}

func sampleConjunctions(start time.Time) []ConjunctionAdvisory {
	return []ConjunctionAdvisory{
		{
			AdvisoryID:        "cdm:99991:1",
			SatelliteID:       "sat:99991",
			SecondaryNORADID:  "44001",
			ClosestApproachAt: start.Add(90 * time.Minute),
			MissDistanceKM:    5,
			Probability:       0.0008,
			SourceID:          "fixture:cdm",
			Evidence:          []canonical.Evidence{{Kind: "conjunction", Ref: "cdm:99991:1", Value: "high"}},
		},
		{
			AdvisoryID:        "cdm:25544:1",
			SatelliteID:       "sat:25544",
			SecondaryNORADID:  "44002",
			ClosestApproachAt: start.Add(2 * time.Hour),
			MissDistanceKM:    30,
			Probability:       0.0002,
			SourceID:          "fixture:cdm",
			Evidence:          []canonical.Evidence{{Kind: "conjunction", Ref: "cdm:25544:1", Value: "low"}},
		},
	}
}

func mustSatellite(t *testing.T, result Result, satelliteID string) SatelliteReport {
	t.Helper()
	for _, satellite := range result.Satellites {
		if satellite.SatelliteID == satelliteID {
			return satellite
		}
	}
	t.Fatalf("missing satellite %s", satelliteID)
	return SatelliteReport{}
}

func countWindows(windows []OverpassWindow, placeID string) int {
	count := 0
	for _, window := range windows {
		if window.PlaceID == placeID {
			count++
		}
	}
	return count
}

func countIntersections(intersections []PlaceIntersection, placeID string) int {
	count := 0
	for _, intersection := range intersections {
		if intersection.PlaceID == placeID {
			count++
		}
	}
	return count
}

func metricValue(metrics []Metric, metricID, subjectType, subjectID string) float64 {
	for _, metric := range metrics {
		if metric.MetricID == metricID && metric.SubjectType == subjectType && metric.SubjectID == subjectID {
			return metric.Value
		}
	}
	return 0
}

func hasMetricID(metrics []Metric, metricID string) bool {
	for _, metric := range metrics {
		if metric.MetricID == metricID {
			return true
		}
	}
	return false
}

func writeEvidenceFile(tb testing.TB, relativePath string, content []byte) {
	tb.Helper()
	artifactPath := filepath.Join(mustRepoRoot(tb), relativePath)
	if err := os.MkdirAll(filepath.Dir(artifactPath), 0o755); err != nil {
		tb.Fatalf("mkdir evidence dir: %v", err)
	}
	if err := os.WriteFile(artifactPath, content, 0o644); err != nil {
		tb.Fatalf("write evidence file: %v", err)
	}
}

func mustRepoRoot(tb testing.TB) string {
	tb.Helper()
	wd, err := os.Getwd()
	if err != nil {
		tb.Fatalf("getwd: %v", err)
	}
	for dir := wd; dir != filepath.Dir(dir); dir = filepath.Dir(dir) {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
	}
	tb.Fatal("unable to locate repo root")
	return ""
}
