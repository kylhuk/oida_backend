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
	eqDensity := metricValue(result.Metrics, "overpass_density", "place", "plc:eq-belt")
	polarDensity := metricValue(result.Metrics, "overpass_density", "place", "plc:polar-watch")
	if eqDensity <= 0 || polarDensity <= 0 {
		t.Fatalf("expected positive overpass density metrics, got %v and %v", eqDensity, polarDensity)
	}
	eqRisk := metricValue(result.Metrics, "conjunction_risk", "satellite", "sat:99991")
	inclinedRisk := metricValue(result.Metrics, "conjunction_risk", "satellite", "sat:25544")
	if eqRisk <= inclinedRisk {
		t.Fatalf("expected higher conjunction risk for sat:99991, got %v <= %v", eqRisk, inclinedRisk)
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
	risk := metricValue(result.Metrics, "conjunction_risk", "satellite", "sat:25544")
	if risk != 0.0076 {
		t.Fatalf("expected deterministic conjunction risk 0.0076, got %v", risk)
	}
	evidence := fmt.Sprintf("satellite_id\twindows\tintersections\tconjunction_risk\n%s\t%d\t%d\t%.4f\n", report.SatelliteID, len(report.Windows), len(report.Intersections), risk)
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
	return `[
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
	    "BSTAR": "0.00002100"
	  }
	]`
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
