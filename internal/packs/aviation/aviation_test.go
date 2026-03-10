package aviation

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const (
	aviationEvidencePath     = ".sisyphus/evidence/task-23-aviation.txt"
	aviationEdgeEvidencePath = ".sisyphus/evidence/task-23-aviation-edge.txt"
)

func TestLoadFixtureBundleDecodesAdapters(t *testing.T) {
	bundle, err := LoadFixtureBundle(DefaultFixtureSourceID)
	if err != nil {
		t.Fatalf("load fixture bundle: %v", err)
	}
	if got := len(bundle.StateVectors); got != 7 {
		t.Fatalf("expected 7 state vectors, got %d", got)
	}
	if got := len(bundle.Registry); got != 1 {
		t.Fatalf("expected 1 registry record, got %d", got)
	}
	if bundle.Registry[0].ModeSCodeHex != "ae5c0f" {
		t.Fatalf("unexpected registry modes code %q", bundle.Registry[0].ModeSCodeHex)
	}
	if bundle.StateVectors[0].ICAO24 != "ae5c0f" {
		t.Fatalf("unexpected icao24 %q", bundle.StateVectors[0].ICAO24)
	}
}

func TestAnalyzeFixtureHappyPath(t *testing.T) {
	bundle, err := LoadFixtureBundle(DefaultFixtureSourceID)
	if err != nil {
		t.Fatalf("load fixture bundle: %v", err)
	}
	plan, err := Analyze(bundle, Options{Now: fixedNow})
	if err != nil {
		t.Fatalf("analyze: %v", err)
	}
	if plan.Stats.AircraftEntities != 1 {
		t.Fatalf("expected 1 aircraft entity, got %d", plan.Stats.AircraftEntities)
	}
	if plan.Stats.TrackPoints != 7 {
		t.Fatalf("expected 7 track points, got %d", plan.Stats.TrackPoints)
	}
	if plan.Stats.FlightSegments != 2 {
		t.Fatalf("expected 2 flight segments, got %d", plan.Stats.FlightSegments)
	}
	if plan.Stats.TransponderGaps != 1 {
		t.Fatalf("expected 1 transponder gap, got %d", plan.Stats.TransponderGaps)
	}
	if plan.Stats.AirportInteractions != 2 {
		t.Fatalf("expected 2 airport interactions, got %d", plan.Stats.AirportInteractions)
	}
	entity := plan.Aircraft[0]
	if entity.MilitaryStatus != "likely_military" {
		t.Fatalf("expected likely_military status, got %q", entity.MilitaryStatus)
	}
	if entity.MilitaryLikelihood < 0.85 {
		t.Fatalf("expected military likelihood >= 0.85, got %.4f", entity.MilitaryLikelihood)
	}
	routeMetric := metricByID(plan.Metrics, MetricRouteIrregularity)
	if routeMetric.MetricValue <= 0.5 {
		t.Fatalf("expected route irregularity > 0.5, got %.4f", routeMetric.MetricValue)
	}
	writeEvidenceFile(t, aviationEvidencePath, renderEvidence(plan))
}

func TestAnalyzeLowEvidenceMilitaryGuardrail(t *testing.T) {
	bundle, err := LoadFixtureBundle("fixture:aviation-low-evidence")
	if err != nil {
		t.Fatalf("load edge fixture bundle: %v", err)
	}
	plan, err := Analyze(bundle, Options{Now: fixedNow})
	if err != nil {
		t.Fatalf("analyze edge: %v", err)
	}
	entity := plan.Aircraft[0]
	if entity.MilitaryStatus != "unknown" {
		t.Fatalf("expected unknown military status, got %q", entity.MilitaryStatus)
	}
	if entity.MilitaryLikelihood >= 0.55 {
		t.Fatalf("expected low-evidence score below 0.55, got %.4f", entity.MilitaryLikelihood)
	}
	writeEvidenceFile(t, aviationEdgeEvidencePath, renderEvidence(plan))
}

func metricByID(rows []MetricSnapshot, metricID string) MetricSnapshot {
	for _, row := range rows {
		if row.MetricID == metricID {
			return row
		}
	}
	return MetricSnapshot{}
}

func renderEvidence(plan Bundle) []byte {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("stats aircraft=%d track_points=%d segments=%d gaps=%d airport_interactions=%d metrics=%d\n",
		plan.Stats.AircraftEntities,
		plan.Stats.TrackPoints,
		plan.Stats.FlightSegments,
		plan.Stats.TransponderGaps,
		plan.Stats.AirportInteractions,
		plan.Stats.Metrics,
	))
	for _, entity := range plan.Aircraft {
		b.WriteString(fmt.Sprintf("entity %s icao24=%s callsign=%s military_score=%.4f military_status=%s route_irregularity=%.4f risk_band=%s primary_place=%s\n",
			entity.EntityID,
			entity.ICAO24,
			entity.Callsign,
			entity.MilitaryLikelihood,
			entity.MilitaryStatus,
			entity.RouteIrregularity,
			entity.RiskBand,
			entity.PrimaryPlaceID,
		))
	}
	for _, segment := range plan.Segments {
		b.WriteString(fmt.Sprintf("segment %s from=%s to=%s points=%d distance_km=%.4f avg_speed_kph=%.4f irregularity=%.4f gaps=%d\n",
			segment.SegmentID,
			segment.FromAirportID,
			segment.ToAirportID,
			segment.PointCount,
			segment.DistanceKM,
			segment.AvgSpeedKPH,
			segment.RouteIrregularity,
			segment.GapCount,
		))
	}
	for _, gap := range plan.GapEvents {
		b.WriteString(fmt.Sprintf("gap %s hours=%.4f in_flight=%t place=%s\n", gap.EventID, gap.GapHours, gap.InFlight, gap.PlaceID))
	}
	for _, event := range plan.AirportInteractions {
		b.WriteString(fmt.Sprintf("airport_interaction %s airport=%s type=%s place=%s observed_at=%s\n", event.EventID, event.AirportID, event.InteractionType, event.PlaceID, event.ObservedAt.Format(time.RFC3339)))
	}
	for _, metric := range plan.Metrics {
		b.WriteString(fmt.Sprintf("metric %s subject=%s value=%.4f rank=%d place=%s\n", metric.MetricID, metric.SubjectID, metric.MetricValue, metric.Rank, metric.PlaceID))
	}
	return []byte(b.String())
}

func writeEvidenceFile(tb testing.TB, relativePath string, content []byte) {
	tb.Helper()
	artifactPath := filepath.Join(repoRoot(tb), relativePath)
	if err := os.MkdirAll(filepath.Dir(artifactPath), 0o755); err != nil {
		tb.Fatalf("mkdir evidence dir: %v", err)
	}
	if err := os.WriteFile(artifactPath, content, 0o644); err != nil {
		tb.Fatalf("write evidence file: %v", err)
	}
}

func repoRoot(tb testing.TB) string {
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

func fixedNow() time.Time {
	return time.Date(2026, time.March, 10, 16, 0, 0, 0, time.UTC)
}
