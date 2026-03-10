package canonical

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const (
	canonicalEvidencePath   = ".sisyphus/evidence/task-13-canonical.json"
	canonicalIDEvidencePath = ".sisyphus/evidence/task-13-id.txt"
)

func TestNewRecordEnvelopePrefersSourceNativeID(t *testing.T) {
	record := NewRecordEnvelope("event", map[string]any{"title": "Port strike"}, EnvelopeOptions{
		SourceID:      "fixture:site",
		RawID:         "raw:event-1",
		NativeID:      "evt/42",
		ParserID:      "parser:rss",
		ParserVersion: "2026.03.10",
	})

	if record.SchemaVersion != SchemaVersion {
		t.Fatalf("expected schema version %d, got %d", SchemaVersion, record.SchemaVersion)
	}
	if record.RecordVersion != InitialRecordVersion {
		t.Fatalf("expected record version %d, got %d", InitialRecordVersion, record.RecordVersion)
	}
	if record.IDStrategy != IDStrategySourceNative {
		t.Fatalf("expected source-native strategy, got %s", record.IDStrategy)
	}
	if record.ID != "event:fixture_site:evt_42" {
		t.Fatalf("unexpected source-native id %q", record.ID)
	}
	if record.NativeID != "evt/42" {
		t.Fatalf("expected native id retention, got %q", record.NativeID)
	}
}

func TestNewRecordEnvelopeFallsBackToContentHash(t *testing.T) {
	data := map[string]any{"name": "Harbor", "status": "active"}
	record := NewRecordEnvelope("entity", data, EnvelopeOptions{SourceID: "fixture:site"})
	identity := NewIdentity(IDOptions{
		Namespace: "entity",
		SourceID:  "fixture:site",
		Content: map[string]any{
			"kind":         "entity",
			"source_id":    "fixture:site",
			"raw_id":       "",
			"content_hash": record.ContentHash,
			"data":         data,
		},
	})

	if record.IDStrategy != IDStrategyContentHash {
		t.Fatalf("expected content-hash strategy, got %s", record.IDStrategy)
	}
	if record.ID != identity.ID {
		t.Fatalf("expected deterministic fallback id %q, got %q", identity.ID, record.ID)
	}
	if !strings.HasPrefix(record.ID, "entity:fixture_site:") {
		t.Fatalf("expected source-scoped fallback id, got %q", record.ID)
	}
	if record.ContentHash == "" {
		t.Fatal("expected content hash to be populated")
	}
}

func TestCanonicalEnvelopeContracts(t *testing.T) {
	now := time.Date(2026, time.March, 10, 12, 0, 0, 0, time.UTC)
	measurementValue := 91.4
	impactScore := float32(72.5)
	latitude := 1.25
	longitude := 103.8
	speed := float32(18.2)
	pointCount := uint32(4)

	rawEvidence := NewRawDocumentEvidence("fixture:site", "raw:obs-1", "https://example.test/obs-1")
	parserEvidence := NewParserVersionEvidence("parser:rss", "2026.03.10")

	observation := ObservationEnvelope{
		RecordEnvelope: NewRecordEnvelope("observation", map[string]any{"headline": "Harbor congestion"}, EnvelopeOptions{
			SourceID:      "fixture:site",
			RawID:         "raw:obs-1",
			NativeID:      "obs-1",
			ParserID:      "parser:rss",
			ParserVersion: "2026.03.10",
			Evidence:      []Evidence{rawEvidence, parserEvidence},
		}),
		SubjectType:      "place",
		SubjectID:        "place:sg:port",
		ObservationType:  "port_congestion",
		PlaceID:          "place:sg:port",
		ParentPlaceChain: []string{"place:sg", "place:asia"},
		ObservedAt:       now,
		ConfidenceBand:   "high",
		MeasurementUnit:  "percent",
		MeasurementValue: &measurementValue,
		Payload:          map[string]any{"berth_utilization": 91.4},
	}

	event := EventEnvelope{
		RecordEnvelope: NewRecordEnvelope("event", map[string]any{"headline": "Strike announced"}, EnvelopeOptions{
			SourceID:      "fixture:site",
			RawID:         "raw:event-1",
			NativeID:      "event-1",
			ParserID:      "parser:rss",
			ParserVersion: "2026.03.10",
			Evidence:      []Evidence{rawEvidence, parserEvidence},
		}),
		EventType:        "labor_disruption",
		EventSubtype:     "strike",
		PlaceID:          "place:sg:port",
		ParentPlaceChain: []string{"place:sg", "place:asia"},
		StartsAt:         now,
		Status:           "open",
		ConfidenceBand:   "medium",
		ImpactScore:      &impactScore,
		Payload:          map[string]any{"affected_terminals": []string{"T1", "T2"}},
	}

	entity := EntityEnvelope{
		RecordEnvelope: NewRecordEnvelope("entity", map[string]any{"registry_status": "active"}, EnvelopeOptions{
			SourceID:      "fixture:site",
			RawID:         "raw:entity-1",
			NativeID:      "entity-1",
			ParserID:      "parser:rss",
			ParserVersion: "2026.03.10",
			Evidence:      []Evidence{rawEvidence, parserEvidence},
		}),
		EntityType:     "organization",
		CanonicalName:  "Example Shipping Ltd",
		Status:         "active",
		RiskBand:       "watch",
		PrimaryPlaceID: "place:sg:port",
		ValidFrom:      &now,
		Aliases:        []string{"Example Shipping", "ESL"},
		Payload:        map[string]any{"imo_company_number": "1234567"},
	}

	track := TrackEnvelope{
		RecordEnvelope: NewRecordEnvelope("track", map[string]any{"sensor": "ais"}, EnvelopeOptions{
			SourceID:      "fixture:site",
			RawID:         "raw:track-1",
			NativeID:      "track-1",
			ParserID:      "parser:rss",
			ParserVersion: "2026.03.10",
			Evidence:      []Evidence{rawEvidence, parserEvidence},
		}),
		TrackID:    "track-1",
		TrackType:  "vessel_position",
		EntityID:   "entity:fixture_site:entity-1",
		PlaceID:    "place:sg:port",
		ObservedAt: &now,
		Latitude:   &latitude,
		Longitude:  &longitude,
		SpeedKPH:   &speed,
		PointCount: &pointCount,
		Payload:    map[string]any{"course_source": "reported"},
	}

	evidenceEnvelope := EvidenceEnvelope{
		RecordEnvelope: NewRecordEnvelope("evidence", map[string]any{"kind": rawEvidence.Kind}, EnvelopeOptions{
			SourceID:      "fixture:site",
			RawID:         "raw:obs-1",
			NativeID:      "evidence-1",
			ParserID:      "parser:rss",
			ParserVersion: "2026.03.10",
			Evidence:      []Evidence{rawEvidence, parserEvidence},
		}),
		EvidenceType: rawEvidence.Kind,
		CapturedAt:   &now,
		Payload:      rawEvidence,
	}

	for _, record := range []RecordEnvelope{observation.RecordEnvelope, event.RecordEnvelope, entity.RecordEnvelope, track.RecordEnvelope, evidenceEnvelope.RecordEnvelope} {
		if record.SchemaVersion != SchemaVersion {
			t.Fatalf("expected schema version %d, got %d", SchemaVersion, record.SchemaVersion)
		}
		if record.RecordVersion != InitialRecordVersion {
			t.Fatalf("expected record version %d, got %d", InitialRecordVersion, record.RecordVersion)
		}
	}
	hashFallback := NewRecordEnvelope("observation", map[string]any{"headline": "No native identifier"}, EnvelopeOptions{SourceID: "fixture:site"})
	if hashFallback.IDStrategy != IDStrategyContentHash {
		t.Fatalf("expected fallback strategy %s, got %s", IDStrategyContentHash, hashFallback.IDStrategy)
	}

	payload := map[string]any{
		"observation":   observation,
		"event":         event,
		"entity":        entity,
		"track":         track,
		"evidence":      evidenceEnvelope,
		"hash_fallback": hashFallback,
	}
	encoded, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		t.Fatalf("marshal evidence payload: %v", err)
	}
	writeEvidenceFile(t, canonicalEvidencePath, encoded)

	idEvidence := strings.Join([]string{
		"kind\tstrategy\tid\tnative_id\tcontent_hash",
		observation.Kind + "\t" + string(observation.IDStrategy) + "\t" + observation.ID + "\t" + observation.NativeID + "\t" + observation.ContentHash,
		event.Kind + "\t" + string(event.IDStrategy) + "\t" + event.ID + "\t" + event.NativeID + "\t" + event.ContentHash,
		entity.Kind + "\t" + string(entity.IDStrategy) + "\t" + entity.ID + "\t" + entity.NativeID + "\t" + entity.ContentHash,
		track.Kind + "\t" + string(track.IDStrategy) + "\t" + track.ID + "\t" + track.NativeID + "\t" + track.ContentHash,
		evidenceEnvelope.Kind + "\t" + string(evidenceEnvelope.IDStrategy) + "\t" + evidenceEnvelope.ID + "\t" + evidenceEnvelope.NativeID + "\t" + evidenceEnvelope.ContentHash,
		hashFallback.Kind + "\t" + string(hashFallback.IDStrategy) + "\t" + hashFallback.ID + "\t" + hashFallback.NativeID + "\t" + hashFallback.ContentHash,
	}, "\n") + "\n"
	writeEvidenceFile(t, canonicalIDEvidencePath, []byte(idEvidence))
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
