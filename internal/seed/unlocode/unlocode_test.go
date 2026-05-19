package unlocode_test

import (
	"strings"
	"testing"

	"global-osint-backend/internal/seed/unlocode"
)

func TestParseCoordinates(t *testing.T) {
	tests := []struct {
		raw     string
		wantLat float64
		wantLon float64
		wantOK  bool
	}{
		{"0117N 10351E", 1.0 + 17.0/60, 103.0 + 51.0/60, true},   // Singapore
		{"5130N 00007W", 51.0 + 30.0/60, -(0.0 + 7.0/60), true},  // London area
		{"3347S 07040W", -(33.0 + 47.0/60), -(70.0 + 40.0/60), true}, // Chile
		{"0000N 00000E", 0.0, 0.0, true},
		{"2529N 05308E", 25.0 + 29.0/60, 53.0 + 8.0/60, true},    // Abu Dhabi area
		{"", 0, 0, false},
		{"INVALID", 0, 0, false},
		{"12", 0, 0, false},
	}
	for _, tt := range tests {
		lat, lon, ok := unlocode.ParseCoordinates(tt.raw)
		if ok != tt.wantOK {
			t.Errorf("ParseCoordinates(%q) ok=%v, want %v", tt.raw, ok, tt.wantOK)
			continue
		}
		if !ok {
			continue
		}
		const eps = 0.0001
		if diff := lat - tt.wantLat; diff < -eps || diff > eps {
			t.Errorf("ParseCoordinates(%q) lat=%.6f, want %.6f", tt.raw, lat, tt.wantLat)
		}
		if diff := lon - tt.wantLon; diff < -eps || diff > eps {
			t.Errorf("ParseCoordinates(%q) lon=%.6f, want %.6f", tt.raw, lon, tt.wantLon)
		}
	}
}

func TestLoad(t *testing.T) {
	const csvData = `Country,Location,Name,Coordinates
SG,SIN,Singapore,0117N 10351E
GB,LON,London,5130N 00007W
US,NYK,New York,4042N 07400W
XX,BAD,No Coords,
`
	ports, err := unlocode.Load(strings.NewReader(csvData))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(ports) != 3 {
		t.Fatalf("expected 3 ports (excluding no-coords), got %d", len(ports))
	}
	p := ports[0]
	if p.Code != "SGSIN" {
		t.Errorf("Code=%q, want SGSIN", p.Code)
	}
	if p.CountryCode != "SG" {
		t.Errorf("CountryCode=%q, want SG", p.CountryCode)
	}
	if p.Name != "Singapore" {
		t.Errorf("Name=%q, want Singapore", p.Name)
	}
	if p.Lat < 1.28 || p.Lat > 1.29 {
		t.Errorf("Lat=%.4f, want ~1.2833", p.Lat)
	}
}
