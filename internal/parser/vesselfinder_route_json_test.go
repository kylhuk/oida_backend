package parser

import (
	"context"
	"testing"
	"time"
)

const vesselFinderRouteJSONFixture = `{"reta":1718000000,"dest":"NLRTM","dname":"Rotterdam","wps":[[51.9067,4.4822,1717990000],[51.85,4.35]]}`

func TestDefaultRegistryIncludesVesselFinderRouteJSONParser(t *testing.T) {
	parser, ok := DefaultRegistry().Lookup("parser:vesselfinder-route-json")
	if !ok {
		t.Fatal("expected parser:vesselfinder-route-json to be registered")
	}
	desc := parser.Descriptor()
	if desc.Family != "vesselfinder" || desc.SourceClass != "browser_rendered_vessel_route" {
		t.Fatalf("unexpected descriptor: %#v", desc)
	}
	if desc.Version != "1.0.0" {
		t.Fatalf("unexpected parser version %q", desc.Version)
	}
}

func TestVesselFinderRouteJSONParserEmitsPlanAndWaypoints(t *testing.T) {
	result, parseErr := vesselFinderRouteJSONParser{}.Parse(context.Background(), Input{
		Body:      []byte(vesselFinderRouteJSONFixture),
		FetchedAt: time.Date(2024, 6, 10, 12, 0, 0, 0, time.UTC),
		Attrs: map[string]any{
			"vesselfinder": `{"mmsi":"247379500"}`,
		},
		URL: "https://www.vesselfinder.com/api/pub/dm3/247379500?wp=1",
	})
	if parseErr != nil {
		t.Fatalf("Parse returned error: %v", parseErr)
	}
	if len(result.Candidates) != 3 {
		t.Fatalf("expected 3 candidates (1 route_plan + 2 route_waypoints), got %d", len(result.Candidates))
	}

	// First candidate: route_plan
	plan := result.Candidates[0]
	if plan.Kind != "route_plan" {
		t.Errorf("first candidate kind=%q, want route_plan", plan.Kind)
	}
	if plan.NativeID != "mmsi:247379500" {
		t.Errorf("first candidate NativeID=%q, want mmsi:247379500", plan.NativeID)
	}

	// Second candidate: first route_waypoint (3-element, has ETA)
	wp1 := result.Candidates[1]
	if wp1.Kind != "route_waypoint" {
		t.Errorf("second candidate kind=%q, want route_waypoint", wp1.Kind)
	}
	if wp1.Data["record_kind"] != "route_waypoint" {
		t.Errorf("second candidate record_kind=%q, want route_waypoint", wp1.Data["record_kind"])
	}
	// SourceRecordIndex corresponds to the loop index (sequence+1 = 1 for first waypoint)
	if wp1.Data["sequence"] != 0 {
		t.Errorf("second candidate sequence=%v, want 0", wp1.Data["sequence"])
	}
	if wp1.Data["lat"] == nil || wp1.Data["lon"] == nil {
		t.Errorf("second candidate missing lat/lon: data=%#v", wp1.Data)
	}
	if lat, _ := wp1.Data["lat"].(float64); lat == 0 {
		t.Errorf("second candidate lat is zero, expected non-zero")
	}
	if lon, _ := wp1.Data["lon"].(float64); lon == 0 {
		t.Errorf("second candidate lon is zero, expected non-zero")
	}

	// Third candidate: second route_waypoint (2-element, no ETA)
	wp2 := result.Candidates[2]
	if wp2.Kind != "route_waypoint" {
		t.Errorf("third candidate kind=%q, want route_waypoint", wp2.Kind)
	}
	if wp2.Data["sequence"] != 1 {
		t.Errorf("third candidate sequence=%v, want 1", wp2.Data["sequence"])
	}
}

func TestExtractMMSIFromAttrs(t *testing.T) {
	cases := []struct {
		name string
		attrs map[string]any
		want  string
	}{
		{
			name:  "vesselfinder JSON string with mmsi",
			attrs: map[string]any{"vesselfinder": `{"mmsi":"247379500"}`},
			want:  "247379500",
		},
		{
			name:  "vesselfinder map with mmsi",
			attrs: map[string]any{"vesselfinder": map[string]any{"mmsi": "247379500"}},
			want:  "247379500",
		},
		{
			name:  "flat attrs mmsi fallback",
			attrs: map[string]any{"mmsi": "247379500"},
			want:  "247379500",
		},
		{
			name:  "vesselfinder present but no mmsi inside",
			attrs: map[string]any{"vesselfinder": map[string]any{"foo": "bar"}},
			want:  "",
		},
		{
			name:  "nil attrs",
			attrs: nil,
			want:  "",
		},
		{
			name:  "vesselfinder JSON string with empty mmsi",
			attrs: map[string]any{"vesselfinder": `{"mmsi":""}`},
			want:  "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := extractMMSIFromAttrs(tc.attrs)
			if got != tc.want {
				t.Errorf("extractMMSIFromAttrs(%v) = %q, want %q", tc.attrs, got, tc.want)
			}
		})
	}
}
