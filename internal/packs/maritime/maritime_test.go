package maritime

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"global-osint-backend/internal/canonical"
)

const (
	maritimePackEvidencePath    = ".sisyphus/evidence/task-22-maritime-pack.json"
	maritimeSummaryEvidencePath = ".sisyphus/evidence/task-22-maritime-summary.txt"
)

func TestDefaultAdaptersExposeAISAndMetadataCoverage(t *testing.T) {
	now := time.Date(2026, time.March, 10, 12, 0, 0, 0, time.UTC)
	adapters := DefaultAdapters(now)
	if len(adapters) != 4 {
		t.Fatalf("expected 4 maritime adapters, got %d", len(adapters))
	}
	for _, adapterID := range []string{"maritime:ais:community", "maritime:port:unlocode", "maritime:registry:vessel", "maritime:sanctions:entity_graph"} {
		if _, ok := AdapterByID(adapters, adapterID); !ok {
			t.Fatalf("missing adapter %q", adapterID)
		}
	}
	aisAdapter, ok := AdapterByID(adapters, "maritime:ais:community")
	if !ok {
		t.Fatal("expected AIS adapter")
	}
	if aisAdapter.AuthMode != "user_supplied_key" {
		t.Fatalf("expected AIS auth mode user_supplied_key, got %q", aisAdapter.AuthMode)
	}
	if got := aisAdapter.AuthConfig["parameter_name"]; got != "key" {
		t.Fatalf("expected AIS key parameter name, got %#v", got)
	}
}

func TestMaritimePackArtifacts(t *testing.T) {
	now := time.Date(2026, time.March, 10, 12, 0, 0, 0, time.UTC)
	rawEvidence := canonical.NewRawDocumentEvidence("maritime:registry:vessel", "raw:vessel-1", "https://example.test/vessels/9303801")
	registryEvidence := canonical.Evidence{Kind: "registry_page", Ref: "imo:9303801", URL: "https://example.test/vessels/9303801", Value: "Northern Light"}

	vessel := Vessel{
		SourceID:       "maritime:registry:vessel",
		NativeID:       "imo:9303801",
		Name:           "MV Northern Light",
		Aliases:        []string{"Northern Light"},
		IMO:            "9303801",
		MMSI:           "538009877",
		CallSign:       "V7NL8",
		VesselType:     "oil_tanker",
		FlagState:      "PA",
		Status:         "active",
		RiskBand:       "high",
		PrimaryPlaceID: "plc:port:ae-fjr",
		OwnerName:      "Northern Light Maritime Ltd",
		OperatorName:   "Gulf Route Shipping",
		BuildYear:      2007,
		DeadweightTons: 105432,
		GrossTonnage:   58521,
		ValidFrom:      time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC),
		Evidence:       []canonical.Evidence{rawEvidence, registryEvidence},
	}
	entity := vessel.EntityEnvelope()
	if entity.EntityType != "vessel" {
		t.Fatalf("expected vessel entity type, got %q", entity.EntityType)
	}
	if entity.CanonicalName != "MV Northern Light" {
		t.Fatalf("unexpected canonical name %q", entity.CanonicalName)
	}

	speed := float32(22.4)
	course := float32(134.2)
	trackPoint := VesselTrackPoint{
		SourceID:       "maritime:ais:community",
		TrackID:        "trk:northern-light",
		EntityID:       entity.ID,
		PlaceID:        "plc:sea:arabian-gulf",
		ObservedAt:     now.Add(-2 * time.Hour),
		Latitude:       25.2472,
		Longitude:      56.3575,
		SpeedKPH:       &speed,
		CourseDeg:      &course,
		Status:         "under_way_using_engine",
		ParentPlaceIDs: []string{"plc:ae", "plc:continent:as"},
		Evidence:       []canonical.Evidence{canonical.NewRawDocumentEvidence("maritime:ais:community", "raw:ais-point-1", "https://example.test/ais/point-1")},
	}
	pointEnvelope := trackPoint.Envelope()
	if pointEnvelope.TrackType != "maritime_position" {
		t.Fatalf("expected maritime_position track type, got %q", pointEnvelope.TrackType)
	}

	avgSpeed := float32(21.1)
	trackSegment := VesselTrackSegment{
		SourceID:       "maritime:ais:community",
		TrackID:        "trk:northern-light",
		EntityID:       entity.ID,
		PlaceID:        "plc:sea:gulf-of-oman",
		FromPlaceID:    "plc:port:iruqn",
		ToPlaceID:      "plc:port:ae-fjr",
		StartedAt:      now.Add(-36 * time.Hour),
		EndedAt:        now.Add(-6 * time.Hour),
		DistanceKM:     612.4,
		PointCount:     48,
		AvgSpeedKPH:    &avgSpeed,
		ParentPlaceIDs: []string{"plc:ae", "plc:continent:as"},
		Evidence:       []canonical.Evidence{canonical.NewRawDocumentEvidence("maritime:ais:community", "raw:ais-segment-1", "https://example.test/ais/segment-1")},
	}
	segmentEnvelope := trackSegment.Envelope()
	if segmentEnvelope.TrackType != "maritime_segment" {
		t.Fatalf("expected maritime_segment track type, got %q", segmentEnvelope.TrackType)
	}

	portCall := PortCall{
		SourceID:         "maritime:port:unlocode",
		EntityID:         entity.ID,
		PlaceID:          "plc:port:ae-fjr",
		PortName:         "Fujairah Anchorage",
		Terminal:         "Outer Anchorage",
		Berth:            "A-12",
		CallType:         "turnaround",
		StartedAt:        now.Add(-10 * time.Hour),
		EndedAt:          now.Add(-2 * time.Hour),
		NextPlaceID:      "plc:port:pkkhi",
		ParentPlaceChain: []string{"plc:ae-fuj", "plc:ae", "plc:continent:as"},
		Evidence:         []canonical.Evidence{canonical.NewRawDocumentEvidence("maritime:port:unlocode", "raw:port-call-1", "https://example.test/port-calls/1")},
	}
	portCallEnvelope := portCall.EventEnvelope()
	if portCallEnvelope.EventType != "port_call" {
		t.Fatalf("expected port_call event type, got %q", portCallEnvelope.EventType)
	}

	gapOne := AISGap{
		SourceID:         "maritime:ais:community",
		TrackID:          trackSegment.TrackID,
		EntityID:         entity.ID,
		PlaceID:          "plc:sea:arabian-gulf",
		StartsAt:         now.Add(-72 * time.Hour),
		EndsAt:           now.Add(-54 * time.Hour),
		Reason:           "dark_activity",
		LastKnownPortID:  "plc:port:iruqn",
		NextKnownPortID:  "plc:sea:arabian-gulf",
		ParentPlaceChain: []string{"plc:ae", "plc:continent:as"},
		Evidence:         []canonical.Evidence{canonical.NewRawDocumentEvidence("maritime:ais:community", "raw:gap-1", "https://example.test/ais/gap-1")},
	}
	gapTwo := AISGap{
		SourceID:         "maritime:ais:community",
		TrackID:          trackSegment.TrackID,
		EntityID:         entity.ID,
		PlaceID:          "plc:sea:gulf-of-oman",
		StartsAt:         now.Add(-30 * time.Hour),
		EndsAt:           now.Add(-23*time.Hour - 30*time.Minute),
		Reason:           "dark_activity",
		LastKnownPortID:  "plc:sea:arabian-gulf",
		NextKnownPortID:  "plc:port:ae-fjr",
		ParentPlaceChain: []string{"plc:ae", "plc:continent:as"},
		Evidence:         []canonical.Evidence{canonical.NewRawDocumentEvidence("maritime:ais:community", "raw:gap-2", "https://example.test/ais/gap-2")},
	}
	gapEvents := []any{gapOne.EventEnvelope(), gapTwo.EventEnvelope()}
	darkHours := AISDarkHours(entity.ID, []AISGap{gapOne, gapTwo}, now)
	if darkHours.MetricValue != 24.5 {
		t.Fatalf("expected 24.5 dark hours, got %v", darkHours.MetricValue)
	}

	shadowScore := ShadowFleetScore(entity.ID, ShadowFleetSignals{
		AISDarkHours:         darkHours.MetricValue,
		AISGapFrequency:      2,
		FlagChanges90d:       2,
		OwnershipChanges180d: 1,
		SanctionsExposure:    1,
		HighRiskPortCalls:    2,
		STSSuspicionScore:    0.7,
		RouteDeviationScore:  0.8,
		VesselAgeYears:       19,
		Evidence:             mergeEvidence(darkHours.Evidence, portCallEnvelope.Evidence, []canonical.Evidence{{Kind: "watchlist_match", Ref: "sanctions:northern-light", Value: "matched_sanctioned_operator"}}),
	}, now)
	if shadowScore.MetricValue != 0.7253 {
		t.Fatalf("expected 0.7253 shadow fleet score, got %v", shadowScore.MetricValue)
	}

	registryRecords := BuildMetricRegistryRecords(now)
	if len(registryRecords) != 2 {
		t.Fatalf("expected 2 registry records, got %d", len(registryRecords))
	}
	ids := []string{registryRecords[0].MetricID, registryRecords[1].MetricID}
	if strings.Join(ids, ",") != "ais_dark_hours,shadow_fleet_score" {
		t.Fatalf("unexpected metric registry order %v", ids)
	}

	artifact := map[string]any{
		"adapters":        DefaultAdapters(now),
		"entity":          entity,
		"track_point":     pointEnvelope,
		"track_segment":   segmentEnvelope,
		"port_call":       portCallEnvelope,
		"ais_gap_events":  gapEvents,
		"metric_registry": registryRecords,
		"metric_readings": []MetricReading{darkHours, shadowScore},
		"generated_at":    now,
		"evidence_refs":   evidenceRefs(mergeEvidence(entity.Evidence, pointEnvelope.Evidence, segmentEnvelope.Evidence, portCallEnvelope.Evidence, darkHours.Evidence, shadowScore.Evidence)),
	}
	b, err := json.MarshalIndent(artifact, "", "  ")
	if err != nil {
		t.Fatalf("marshal artifact: %v", err)
	}
	writeEvidenceFile(t, maritimePackEvidencePath, b)

	var summary strings.Builder
	summary.WriteString("kind\tid\tkey\n")
	for _, adapter := range DefaultAdapters(now) {
		summary.WriteString(fmt.Sprintf("adapter\t%s\t%s\n", adapter.AdapterID, adapter.AuthMode))
	}
	summary.WriteString(fmt.Sprintf("entity\t%s\t%s\n", entity.ID, vessel.IMO))
	summary.WriteString(fmt.Sprintf("track\t%s\t%s\n", pointEnvelope.ID, pointEnvelope.TrackType))
	summary.WriteString(fmt.Sprintf("track\t%s\t%s\n", segmentEnvelope.ID, segmentEnvelope.TrackType))
	summary.WriteString(fmt.Sprintf("event\t%s\t%s\n", portCallEnvelope.ID, portCallEnvelope.EventType))
	for _, gapEvent := range []AISGap{gapOne, gapTwo} {
		summary.WriteString(fmt.Sprintf("event\t%s\t%s\n", gapEvent.EventEnvelope().ID, gapEvent.EventEnvelope().EventType))
	}
	summary.WriteString(fmt.Sprintf("metric\t%s\t%.4f\n", darkHours.MetricID, darkHours.MetricValue))
	summary.WriteString(fmt.Sprintf("metric\t%s\t%.4f\n", shadowScore.MetricID, shadowScore.MetricValue))
	writeEvidenceFile(t, maritimeSummaryEvidencePath, []byte(summary.String()))
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
