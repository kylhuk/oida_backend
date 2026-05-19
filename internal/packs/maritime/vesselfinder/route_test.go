package vesselfinder

import (
	"testing"
	"time"
)

var routeNow = time.Date(2026, 5, 19, 12, 0, 0, 0, time.UTC)

// TestParseDM3HappyPath exercises a well-formed response with all fields present.
func TestParseDM3HappyPath(t *testing.T) {
	// reta=1718000000, wps have 3 elements (lat, lon, eta_unix).
	body := []byte(`{
		"reta": 1718000000,
		"dest": "NLRTM",
		"dname": "Rotterdam",
		"wps": [
			[51.9, 4.1, 1718001000],
			[52.0, 4.2, 1718002000]
		]
	}`)

	fetchedAt := routeNow
	plan, err := ParseDM3(body, fetchedAt)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	// MMSI must always be empty string from ParseDM3.
	if plan.MMSI != "" {
		t.Errorf("MMSI: got %q, want empty string", plan.MMSI)
	}

	wantRETA := time.Unix(1718000000, 0).UTC()
	if !plan.RETA.Equal(wantRETA) {
		t.Errorf("RETA: got %v, want %v", plan.RETA, wantRETA)
	}

	if plan.DestinationLOCODE != "NLRTM" {
		t.Errorf("DestinationLOCODE: got %q, want NLRTM", plan.DestinationLOCODE)
	}
	if plan.DestinationName != "Rotterdam" {
		t.Errorf("DestinationName: got %q, want Rotterdam", plan.DestinationName)
	}

	if len(plan.Waypoints) != 2 {
		t.Fatalf("Waypoints: got %d, want 2", len(plan.Waypoints))
	}

	wp0 := plan.Waypoints[0]
	if wp0.Sequence != 0 || wp0.Lat != 51.9 || wp0.Lon != 4.1 {
		t.Errorf("Waypoints[0]: got seq=%d lat=%f lon=%f, want seq=0 lat=51.9 lon=4.1",
			wp0.Sequence, wp0.Lat, wp0.Lon)
	}
	wantETA0 := time.Unix(1718001000, 0).UTC()
	if !wp0.ETA.Equal(wantETA0) {
		t.Errorf("Waypoints[0].ETA: got %v, want %v", wp0.ETA, wantETA0)
	}

	wp1 := plan.Waypoints[1]
	if wp1.Sequence != 1 || wp1.Lat != 52.0 || wp1.Lon != 4.2 {
		t.Errorf("Waypoints[1]: got seq=%d lat=%f lon=%f, want seq=1 lat=52.0 lon=4.2",
			wp1.Sequence, wp1.Lat, wp1.Lon)
	}
	wantETA1 := time.Unix(1718002000, 0).UTC()
	if !wp1.ETA.Equal(wantETA1) {
		t.Errorf("Waypoints[1].ETA: got %v, want %v", wp1.ETA, wantETA1)
	}

	if !plan.FetchedAt.Equal(fetchedAt.UTC()) {
		t.Errorf("FetchedAt: got %v, want %v", plan.FetchedAt, fetchedAt.UTC())
	}
}

// TestParseDM3HandlesMissingRETA covers reta=0 and absent reta key.
func TestParseDM3HandlesMissingRETA(t *testing.T) {
	cases := []struct {
		name string
		body []byte
	}{
		{
			name: "reta zero",
			body: []byte(`{"reta": 0, "dest": "NLRTM", "dname": "Rotterdam", "wps": []}`),
		},
		{
			name: "reta absent",
			body: []byte(`{"dest": "NLRTM", "dname": "Rotterdam", "wps": []}`),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plan, err := ParseDM3(tc.body, routeNow)
			if err != nil {
				t.Fatalf("expected no error, got: %v", err)
			}
			if !plan.RETA.IsZero() {
				t.Errorf("RETA: got %v, want zero time", plan.RETA)
			}
		})
	}
}

// TestParseDM3HandlesEmptyWaypoints covers wps=[] and absent wps key.
func TestParseDM3HandlesEmptyWaypoints(t *testing.T) {
	cases := []struct {
		name string
		body []byte
	}{
		{
			name: "empty wps array",
			body: []byte(`{"reta": 1718000000, "dest": "NLRTM", "dname": "Rotterdam", "wps": []}`),
		},
		{
			name: "wps key absent",
			body: []byte(`{"reta": 1718000000, "dest": "NLRTM", "dname": "Rotterdam"}`),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plan, err := ParseDM3(tc.body, routeNow)
			if err != nil {
				t.Fatalf("expected no error, got: %v", err)
			}
			if plan.Waypoints == nil {
				t.Fatal("Waypoints: got nil, want empty slice")
			}
			if len(plan.Waypoints) != 0 {
				t.Errorf("Waypoints: got %d elements, want 0", len(plan.Waypoints))
			}
		})
	}
}

// TestParseDM3HandlesTwoElementWaypoints ensures ETA is zero for [lat, lon] waypoints.
func TestParseDM3HandlesTwoElementWaypoints(t *testing.T) {
	body := []byte(`{
		"reta": 1718000000,
		"dest": "NLRTM",
		"dname": "Rotterdam",
		"wps": [
			[51.9, 4.1],
			[52.0, 4.2]
		]
	}`)

	plan, err := ParseDM3(body, routeNow)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if len(plan.Waypoints) != 2 {
		t.Fatalf("Waypoints: got %d, want 2", len(plan.Waypoints))
	}
	for i, wp := range plan.Waypoints {
		if !wp.ETA.IsZero() {
			t.Errorf("Waypoints[%d].ETA: got %v, want zero time", i, wp.ETA)
		}
		if wp.Sequence != i {
			t.Errorf("Waypoints[%d].Sequence: got %d, want %d", i, wp.Sequence, i)
		}
	}
}

// TestApplyRouteResultSuccess verifies that a successful fetch sets status='success'
// and schedules the next fetch at now+refreshInterval.
func TestApplyRouteResultSuccess(t *testing.T) {
	refreshInterval := 6 * time.Hour
	item := RouteQueueItem{
		MMSI:         "123456789",
		Status:       "leased",
		AttemptCount: 2,
	}
	result := RouteResult{StatusCode: 200, Success: true}

	updated := ApplyRouteResult(item, result, routeNow, refreshInterval)

	if updated.Status != "success" {
		t.Errorf("Status: got %q, want success", updated.Status)
	}
	wantNext := routeNow.UTC().Add(refreshInterval)
	if !updated.NextFetchAt.Equal(wantNext) {
		t.Errorf("NextFetchAt: got %v, want %v", updated.NextFetchAt, wantNext)
	}
	// AttemptCount must be reset to 0 on success.
	if updated.AttemptCount != 0 {
		t.Errorf("AttemptCount: got %d, want 0 (reset on success)", updated.AttemptCount)
	}
	if !updated.LastFetchedAt.Equal(routeNow.UTC()) {
		t.Errorf("LastFetchedAt: got %v, want %v", updated.LastFetchedAt, routeNow.UTC())
	}
}

// TestApplyRouteResultFailure checks backoff math for attempt_count=2 (2²=4 min).
func TestApplyRouteResultFailure(t *testing.T) {
	refreshInterval := 6 * time.Hour
	item := RouteQueueItem{
		MMSI:         "123456789",
		Status:       "leased",
		AttemptCount: 2,
	}
	result := RouteResult{StatusCode: 503, Success: false, ErrorCode: "browser_timeout"}

	updated := ApplyRouteResult(item, result, routeNow, refreshInterval)

	if updated.Status != "failed" {
		t.Errorf("Status: got %q, want failed", updated.Status)
	}
	// AttemptCount must be incremented by 1.
	if updated.AttemptCount != 3 {
		t.Errorf("AttemptCount: got %d, want 3", updated.AttemptCount)
	}
	if updated.LastErrorCode != "browser_timeout" {
		t.Errorf("LastErrorCode: got %q, want browser_timeout", updated.LastErrorCode)
	}
	// Backoff = 3² = 9 minutes (after increment).
	wantNext := routeNow.UTC().Add(9 * time.Minute)
	if !updated.NextFetchAt.Equal(wantNext) {
		t.Errorf("NextFetchAt: got %v, want %v", updated.NextFetchAt, wantNext)
	}
}

// TestApplyRouteResultFailureClampsBackoff checks that large attempt counts are
// clamped to 1 hour.
func TestApplyRouteResultFailureClampsBackoff(t *testing.T) {
	refreshInterval := 6 * time.Hour
	item := RouteQueueItem{
		MMSI:         "123456789",
		Status:       "leased",
		AttemptCount: 10, // 11² = 121 minutes > 1 hour after increment
	}
	result := RouteResult{StatusCode: 503, Success: false, ErrorCode: "browser_timeout"}

	updated := ApplyRouteResult(item, result, routeNow, refreshInterval)

	wantNext := routeNow.UTC().Add(time.Hour)
	if !updated.NextFetchAt.Equal(wantNext) {
		t.Errorf("NextFetchAt: got %v, want %v (1h clamp)", updated.NextFetchAt, wantNext)
	}
	if updated.AttemptCount != 11 {
		t.Errorf("AttemptCount: got %d, want 11", updated.AttemptCount)
	}
}

func TestParseDM3NilBody(t *testing.T) {
	_, err := ParseDM3(nil, routeNow)
	if err == nil {
		t.Fatal("expected error for nil body, got nil")
	}
}

func TestApplyRouteResultSoftTerminal404(t *testing.T) {
	item := RouteQueueItem{AttemptCount: 1}
	result := RouteResult{StatusCode: 404, Success: false, ErrorCode: "http_status"}
	got := ApplyRouteResult(item, result, routeNow, time.Hour)
	if got.Status != "failed" {
		t.Errorf("status: want failed, got %s", got.Status)
	}
	want404 := routeNow.Add(24 * time.Hour)
	if !got.NextFetchAt.Equal(want404) {
		t.Errorf("NextFetchAt: want %v, got %v", want404, got.NextFetchAt)
	}
}
