package parser

import (
	"context"
	"strings"
	"testing"
	"time"
)

// Inline fixtures — mimic real AISstream WebSocket messages.

const aisstreamPositionReportFixture = `{
	"MessageType": "PositionReport",
	"MetaData": {
		"MMSI": 123456789,
		"ShipName": "OCEAN TITAN",
		"latitude": 51.5,
		"longitude": -0.12,
		"time_utc": "2026-05-19 12:34:56.789 +0000 UTC",
		"MMSI_String": "123456789"
	},
	"Message": {
		"PositionReport": {
			"Mmsi": 123456789,
			"Sog": 12.5,
			"Cog": 270.0,
			"TrueHeading": 268,
			"NavigationalStatus": 0,
			"Latitude": 51.5,
			"Longitude": -0.12,
			"RateOfTurn": 0,
			"PositionAccuracy": false,
			"Timestamp": 45
		}
	}
}`

const aisstreamShipStaticDataFixture = `{
	"MessageType": "ShipStaticData",
	"MetaData": {
		"MMSI": 987654321,
		"ShipName": "AURORA",
		"latitude": 0,
		"longitude": 0,
		"time_utc": "2026-05-19 08:00:00 +0000 UTC",
		"MMSI_String": "987654321"
	},
	"Message": {
		"ShipStaticData": {
			"Mmsi": 987654321,
			"ImoNumber": 9876543,
			"Name": "AURORA",
			"CallSign": "XYZW",
			"Type": 70,
			"Dimension": {"A": 100, "B": 20, "C": 10, "D": 10}
		}
	}
}`

const aisstreamUnknownTypeFixture = `{
	"MessageType": "SafetyBroadcastMessage",
	"MetaData": {
		"MMSI": 111222333,
		"ShipName": "",
		"latitude": 0,
		"longitude": 0,
		"time_utc": "2026-05-19 10:00:00 +0000 UTC",
		"MMSI_String": "111222333"
	},
	"Message": {
		"SafetyBroadcastMessage": {"Text": "TEST BROADCAST"}
	}
}`

const aisstreamShipStaticNoIMOFixture = `{
	"MessageType": "ShipStaticData",
	"MetaData": {
		"MMSI": 555666777,
		"ShipName": "NOIMO",
		"latitude": 0,
		"longitude": 0,
		"time_utc": "2026-05-19 00:00:00 +0000 UTC",
		"MMSI_String": "555666777"
	},
	"Message": {
		"ShipStaticData": {
			"Mmsi": 555666777,
			"ImoNumber": 0,
			"Name": "NOIMO",
			"CallSign": "XY",
			"Type": 60,
			"Dimension": {"A": 0, "B": 0, "C": 0, "D": 0}
		}
	}
}`

func makeBatch(msgs ...string) []byte {
	return []byte("[" + strings.Join(msgs, ",") + "]")
}

func newAISstreamInput(body []byte) Input {
	return Input{
		ParserID:  "parser:aisstream-json",
		SourceID:  "catalog:auto:maritime-aisstream",
		RawID:     "raw:ais:batch:001",
		FetchedAt: time.Date(2026, 5, 19, 12, 0, 0, 0, time.UTC),
		Body:      body,
	}
}

// TestAISstreamParser_TwoMessageBatch tests a batch with one PositionReport and
// one ShipStaticData, asserting correct candidate count, kinds and NativeIDs.
func TestAISstreamParser_TwoMessageBatch(t *testing.T) {
	body := makeBatch(aisstreamPositionReportFixture, aisstreamShipStaticDataFixture)
	result, parseErr := DefaultRegistry().Parse(context.Background(), newAISstreamInput(body))
	if parseErr != nil {
		t.Fatalf("Parse returned error: %v", parseErr)
	}
	if len(result.Candidates) != 2 {
		t.Fatalf("expected 2 candidates, got %d", len(result.Candidates))
	}

	trackPoint := result.Candidates[0]
	if trackPoint.Kind != "track_point" {
		t.Errorf("candidates[0].Kind = %q, want track_point", trackPoint.Kind)
	}
	if trackPoint.NativeID != "123456789" {
		t.Errorf("candidates[0].NativeID = %q, want 123456789", trackPoint.NativeID)
	}

	lat, ok := trackPoint.Data["lat"]
	if !ok {
		t.Error("expected lat in track_point data")
	}
	lon, ok := trackPoint.Data["lon"]
	if !ok {
		t.Error("expected lon in track_point data")
	}
	if latF, _ := lat.(float64); latF == 0 {
		t.Errorf("expected non-zero lat, got %v", lat)
	}
	if lonF, _ := lon.(float64); lonF == 0 {
		t.Errorf("expected non-zero lon, got %v", lon)
	}

	entity := result.Candidates[1]
	if entity.Kind != "entity" {
		t.Errorf("candidates[1].Kind = %q, want entity", entity.Kind)
	}
	if entity.NativeID != "987654321" {
		t.Errorf("candidates[1].NativeID = %q, want 987654321", entity.NativeID)
	}
}

// TestAISstreamParser_UnknownMessageType asserts that an unrecognised MessageType
// produces a "raw" candidate.
func TestAISstreamParser_UnknownMessageType(t *testing.T) {
	body := makeBatch(aisstreamUnknownTypeFixture)
	result, parseErr := DefaultRegistry().Parse(context.Background(), newAISstreamInput(body))
	if parseErr != nil {
		t.Fatalf("Parse returned error: %v", parseErr)
	}
	if len(result.Candidates) != 1 {
		t.Fatalf("expected 1 candidate, got %d", len(result.Candidates))
	}
	c := result.Candidates[0]
	if c.Kind != "raw" {
		t.Errorf("candidate.Kind = %q, want raw", c.Kind)
	}
}

// TestAISstreamParser_EmptyBodyReturnsParseError asserts that an empty/object
// body causes a ParseError, not a panic.
func TestAISstreamParser_EmptyBodyReturnsParseError(t *testing.T) {
	_, parseErr := DefaultRegistry().Parse(context.Background(), Input{
		ParserID: "parser:aisstream-json",
		SourceID: "catalog:auto:maritime-aisstream",
		Body:     []byte("{}"),
	})
	if parseErr == nil {
		t.Fatal("expected ParseError for object body, got nil")
	}
}

// TestAISstreamParser_ShipStaticEntityIDFormat asserts that a ShipStaticData
// candidate carries an EntityID following the "ent:vessel:" pattern used by
// VesselFinder.
func TestAISstreamParser_ShipStaticEntityIDFormat(t *testing.T) {
	t.Run("with IMO", func(t *testing.T) {
		body := makeBatch(aisstreamShipStaticDataFixture)
		result, parseErr := DefaultRegistry().Parse(context.Background(), newAISstreamInput(body))
		if parseErr != nil {
			t.Fatalf("Parse returned error: %v", parseErr)
		}
		if len(result.Candidates) != 1 {
			t.Fatalf("expected 1 candidate, got %d", len(result.Candidates))
		}
		c := result.Candidates[0]
		entityID, _ := c.Data["entity_id"].(string)
		if !strings.HasPrefix(entityID, "ent:vessel:") {
			t.Errorf("entity_id = %q, want prefix ent:vessel:", entityID)
		}
		// With a known IMO the ID should be "ent:vessel:9876543"
		if entityID != "ent:vessel:9876543" {
			t.Errorf("entity_id = %q, want ent:vessel:9876543", entityID)
		}
	})

	t.Run("without IMO falls back to mmsi", func(t *testing.T) {
		body := makeBatch(aisstreamShipStaticNoIMOFixture)
		result, parseErr := DefaultRegistry().Parse(context.Background(), newAISstreamInput(body))
		if parseErr != nil {
			t.Fatalf("Parse returned error: %v", parseErr)
		}
		c := result.Candidates[0]
		entityID, _ := c.Data["entity_id"].(string)
		if !strings.HasPrefix(entityID, "ent:vessel:") {
			t.Errorf("entity_id = %q, want prefix ent:vessel:", entityID)
		}
	})
}

// TestAISstreamParser_SourceRecordKeyDeterministic asserts that parsing the same
// batch twice yields identical source_record_key values.
func TestAISstreamParser_SourceRecordKeyDeterministic(t *testing.T) {
	body := makeBatch(aisstreamPositionReportFixture, aisstreamShipStaticDataFixture)

	result1, err1 := DefaultRegistry().Parse(context.Background(), newAISstreamInput(body))
	result2, err2 := DefaultRegistry().Parse(context.Background(), newAISstreamInput(body))

	if err1 != nil || err2 != nil {
		t.Fatalf("Parse returned errors: %v / %v", err1, err2)
	}
	if len(result1.Candidates) != len(result2.Candidates) {
		t.Fatalf("candidate counts differ: %d vs %d", len(result1.Candidates), len(result2.Candidates))
	}
	for i := range result1.Candidates {
		key1, _ := result1.Candidates[i].Data["source_record_key"].(string)
		key2, _ := result2.Candidates[i].Data["source_record_key"].(string)
		if key1 == "" {
			t.Errorf("candidates[%d] missing source_record_key", i)
			continue
		}
		if key1 != key2 {
			t.Errorf("candidates[%d] source_record_key not deterministic: %q vs %q", i, key1, key2)
		}
	}
}

// TestDefaultRegistryIncludesAISstreamJSONParser verifies that the parser is
// properly registered and its descriptor fields are correct.
func TestDefaultRegistryIncludesAISstreamJSONParser(t *testing.T) {
	p, ok := DefaultRegistry().Lookup("parser:aisstream-json")
	if !ok {
		t.Fatal("expected parser:aisstream-json to be registered")
	}
	desc := p.Descriptor()
	if desc.Family != "aisstream" {
		t.Errorf("Family = %q, want aisstream", desc.Family)
	}
	if desc.SourceClass != "websocket_stream_ais" {
		t.Errorf("SourceClass = %q, want websocket_stream_ais", desc.SourceClass)
	}
	if desc.Version != "1.0.0" {
		t.Errorf("Version = %q, want 1.0.0", desc.Version)
	}
}
