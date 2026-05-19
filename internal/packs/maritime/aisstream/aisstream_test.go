package aisstream

import (
	"encoding/json"
	"testing"
	"time"
)

// positionReportFixture is a single PositionReport envelope.
const positionReportFixture = `{
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

// shipStaticDataFixture is a single ShipStaticData envelope with IMO.
const shipStaticDataFixture = `{
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

// unknownTypeFixture is an envelope with a message type not explicitly handled.
const unknownTypeFixture = `{
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

func TestParseBatch_PositionAndStatic(t *testing.T) {
	body := "[" + positionReportFixture + "," + shipStaticDataFixture + "]"

	envelopes, err := ParseBatch([]byte(body))
	if err != nil {
		t.Fatalf("ParseBatch error: %v", err)
	}
	if len(envelopes) != 2 {
		t.Fatalf("expected 2 envelopes, got %d", len(envelopes))
	}

	// --- first: PositionReport ---
	pr := envelopes[0]
	if pr.MessageType != "PositionReport" {
		t.Errorf("env[0].MessageType = %q, want PositionReport", pr.MessageType)
	}
	if pr.MetaData.MMSI != 123456789 {
		t.Errorf("env[0].MetaData.MMSI = %d, want 123456789", pr.MetaData.MMSI)
	}
	if pr.MetaData.ShipName != "OCEAN TITAN" {
		t.Errorf("env[0].MetaData.ShipName = %q, want OCEAN TITAN", pr.MetaData.ShipName)
	}

	// Decode the inner PositionReport.
	var innerMsg struct {
		PositionReport PositionReport `json:"PositionReport"`
	}
	if err := json.Unmarshal(pr.Message, &innerMsg); err != nil {
		t.Fatalf("unmarshal PositionReport: %v", err)
	}
	if innerMsg.PositionReport.Sog != 12.5 {
		t.Errorf("PositionReport.Sog = %v, want 12.5", innerMsg.PositionReport.Sog)
	}
	if innerMsg.PositionReport.TrueHeading != 268 {
		t.Errorf("PositionReport.TrueHeading = %d, want 268", innerMsg.PositionReport.TrueHeading)
	}

	// PositionData check.
	lat, lon, ts, ok := pr.PositionData()
	if !ok {
		t.Error("PositionData() ok = false, want true")
	}
	if lat != 51.5 || lon != -0.12 {
		t.Errorf("PositionData() lat/lon = %v/%v, want 51.5/-0.12", lat, lon)
	}
	wantTime := time.Date(2026, 5, 19, 12, 34, 56, 789_000_000, time.UTC)
	if !ts.Equal(wantTime) {
		t.Errorf("PositionData() ts = %v, want %v", ts, wantTime)
	}

	// EntityID — no IMO in PositionReport, so MMSI-based.
	if id := pr.EntityID(); id != "ent:vessel:mmsi:123456789" {
		t.Errorf("pr.EntityID() = %q, want ent:vessel:mmsi:123456789", id)
	}

	// --- second: ShipStaticData ---
	ss := envelopes[1]
	if ss.MessageType != "ShipStaticData" {
		t.Errorf("env[1].MessageType = %q, want ShipStaticData", ss.MessageType)
	}

	var innerStatic struct {
		ShipStaticData ShipStaticData `json:"ShipStaticData"`
	}
	if err := json.Unmarshal(ss.Message, &innerStatic); err != nil {
		t.Fatalf("unmarshal ShipStaticData: %v", err)
	}
	if innerStatic.ShipStaticData.IMO != 9876543 {
		t.Errorf("ShipStaticData.IMO = %d, want 9876543", innerStatic.ShipStaticData.IMO)
	}
	if innerStatic.ShipStaticData.DimToBow != 100 {
		t.Errorf("ShipStaticData.DimToBow = %d, want 100", innerStatic.ShipStaticData.DimToBow)
	}
}

func TestMMSIStringIntFormat(t *testing.T) {
	// Real AISstream API sends MMSI_String as a JSON integer, not a quoted string.
	fixture := `[{"Message":{"ShipStaticData":{"ImoNumber":0,"Name":"FORENSO","CallSign":"PD3587","Type":79,"Dimension":{"A":120,"B":15,"C":5,"D":7}}},"MessageType":"ShipStaticData","MetaData":{"MMSI":244650889,"MMSI_String":244650889,"ShipName":"FORENSO","latitude":51.85557,"longitude":6.08148,"time_utc":"2026-05-19 15:33:05.865837165 +0000 UTC"}}]`
	envelopes, err := ParseBatch([]byte(fixture))
	if err != nil {
		t.Fatalf("ParseBatch with integer MMSI_String failed: %v", err)
	}
	if envelopes[0].MetaData.MMSI != 244650889 {
		t.Errorf("MMSI = %d, want 244650889", envelopes[0].MetaData.MMSI)
	}
	if id := envelopes[0].EntityID(); id != "ent:vessel:mmsi:244650889" {
		t.Errorf("EntityID() = %q, want ent:vessel:mmsi:244650889", id)
	}
	// Nanosecond-precision timestamp from real API.
	if envelopes[0].MetaData.TimeUTC.Year() != 2026 {
		t.Errorf("TimeUTC year = %d, want 2026", envelopes[0].MetaData.TimeUTC.Year())
	}
}

func TestMetaDataTimeParsing(t *testing.T) {
	tests := []struct {
		name      string
		timeUTC   string
		wantYear  int
		wantMs    int // millisecond component
	}{
		{
			name:     "with milliseconds",
			timeUTC:  "2026-05-19 12:34:56.789 +0000 UTC",
			wantYear: 2026,
			wantMs:   789,
		},
		{
			name:     "without milliseconds",
			timeUTC:  "2026-05-19 08:00:00 +0000 UTC",
			wantYear: 2026,
			wantMs:   0,
		},
		{
			name:     "with nanoseconds (real API format)",
			timeUTC:  "2026-05-19 15:33:05.865837165 +0000 UTC",
			wantYear: 2026,
			wantMs:   865,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ts, err := parseMetaDataTime(tc.timeUTC)
			if err != nil {
				t.Fatalf("parseMetaDataTime(%q) error: %v", tc.timeUTC, err)
			}
			if ts.Year() != tc.wantYear {
				t.Errorf("year = %d, want %d", ts.Year(), tc.wantYear)
			}
			gotMs := ts.Nanosecond() / 1_000_000
			if gotMs != tc.wantMs {
				t.Errorf("ms = %d, want %d", gotMs, tc.wantMs)
			}
		})
	}
}

func TestEntityID(t *testing.T) {
	tests := []struct {
		name     string
		envelope string
		wantID   string
	}{
		{
			name:     "IMO-based ID from ShipStaticData",
			envelope: shipStaticDataFixture,
			wantID:   "ent:vessel:9876543",
		},
		{
			name:     "MMSI-based ID from PositionReport",
			envelope: positionReportFixture,
			wantID:   "ent:vessel:mmsi:123456789",
		},
		{
			name:     "MMSI-based ID for unknown type",
			envelope: unknownTypeFixture,
			wantID:   "ent:vessel:mmsi:111222333",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			envelopes, err := ParseBatch([]byte("[" + tc.envelope + "]"))
			if err != nil {
				t.Fatalf("ParseBatch error: %v", err)
			}
			if len(envelopes) != 1 {
				t.Fatalf("expected 1 envelope, got %d", len(envelopes))
			}
			if id := envelopes[0].EntityID(); id != tc.wantID {
				t.Errorf("EntityID() = %q, want %q", id, tc.wantID)
			}
		})
	}

	// ShipStaticData with IMO=0 should fall back to MMSI.
	t.Run("ShipStaticData IMO=0 falls back to MMSI", func(t *testing.T) {
		fixture := `[{
			"MessageType": "ShipStaticData",
			"MetaData": {
				"MMSI": 555666777,
				"ShipName": "NOIMO",
				"latitude": 0, "longitude": 0,
				"time_utc": "2026-05-19 00:00:00 +0000 UTC",
				"MMSI_String": "555666777"
			},
			"Message": {
				"ShipStaticData": {"Mmsi": 555666777, "ImoNumber": 0, "Name": "NOIMO", "CallSign": "XY", "Type": 60}
			}
		}]`
		envelopes, err := ParseBatch([]byte(fixture))
		if err != nil {
			t.Fatalf("ParseBatch error: %v", err)
		}
		if id := envelopes[0].EntityID(); id != "ent:vessel:mmsi:555666777" {
			t.Errorf("EntityID() = %q, want ent:vessel:mmsi:555666777", id)
		}
	})
}

func TestParseBatch_UnknownType(t *testing.T) {
	body := "[" + unknownTypeFixture + "]"
	envelopes, err := ParseBatch([]byte(body))
	if err != nil {
		t.Fatalf("ParseBatch with unknown type should not error, got: %v", err)
	}
	if len(envelopes) != 1 {
		t.Fatalf("expected 1 envelope, got %d", len(envelopes))
	}
	env := envelopes[0]
	if env.MessageType != "SafetyBroadcastMessage" {
		t.Errorf("MessageType = %q, want SafetyBroadcastMessage", env.MessageType)
	}
	// Raw message should be preserved and parseable.
	if len(env.Message) == 0 {
		t.Error("Message should be non-empty raw JSON")
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(env.Message, &raw); err != nil {
		t.Errorf("Message is not valid JSON: %v", err)
	}
}

func TestParseBatch_MalformedJSON(t *testing.T) {
	_, err := ParseBatch([]byte(`[{invalid json`))
	if err == nil {
		t.Error("ParseBatch with malformed JSON should return error")
	}
}

func TestParseBatch_EmptyBody(t *testing.T) {
	_, err := ParseBatch([]byte{})
	if err == nil {
		t.Error("ParseBatch with empty body should return error")
	}
}

func TestPositionData_ZeroCoords(t *testing.T) {
	// ShipStaticData with lat=0, lon=0 → PositionData ok=false
	envelopes, err := ParseBatch([]byte("[" + shipStaticDataFixture + "]"))
	if err != nil {
		t.Fatalf("ParseBatch error: %v", err)
	}
	_, _, _, ok := envelopes[0].PositionData()
	if ok {
		t.Error("PositionData() should return ok=false for zero lat/lon")
	}
}
