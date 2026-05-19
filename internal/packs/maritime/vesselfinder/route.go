package vesselfinder

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// RouteWaypoint is a single point in a vessel's planned route.
// Sequence is 0-based. ETA is zero if not reported (2-element waypoint).
type RouteWaypoint struct {
	Sequence int
	Lat      float64
	Lon      float64
	ETA      time.Time
}

// RoutePlan holds the decoded route plan returned by the VesselFinder DM3 API.
// MMSI is always "" when returned by ParseDM3 — the caller must fill it in.
type RoutePlan struct {
	MMSI              string
	DestinationLOCODE string
	DestinationName   string
	RETA              time.Time // zero means not reported
	Waypoints         []RouteWaypoint
	FetchedAt         time.Time
}

// RouteQueueItem mirrors ScanQueueItem but is scoped to the route-fetch queue.
// It omits discovery context fields (CountryCode, TypeCode, PlaceID, LeaseID).
type RouteQueueItem struct {
	MMSI          string
	DetailID      string
	Status        string
	NextFetchAt   time.Time
	LastFetchedAt time.Time
	AttemptCount  int
	LastErrorCode string
	StatusCode    int
}

// RouteResult is the outcome of a single DM3 fetch attempt, analogous to ScanResult.
type RouteResult struct {
	StatusCode int
	Success    bool
	ErrorCode  string
}

// ParseDM3 decodes the JSON body returned by the VesselFinder DM3 API endpoint
// (https://www.vesselfinder.com/api/pub/dm3/{MMSI}?wp=1).
//
// The function is defensive: it always returns a partial RoutePlan even when an
// error is returned, so callers can log whatever was successfully decoded.
//
// MMSI is always set to "" — the caller knows which MMSI it requested.
func ParseDM3(body []byte, fetchedAt time.Time) (RoutePlan, error) {
	if len(body) == 0 {
		return RoutePlan{FetchedAt: fetchedAt}, fmt.Errorf("parsedm3: empty body")
	}

	plan := RoutePlan{
		FetchedAt: fetchedAt.UTC(),
	}

	// Use a raw map first so we can handle reta as either int or float without
	// relying on json.Unmarshal's strict integer handling.
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		return plan, fmt.Errorf("parsedm3: invalid json: %w", err)
	}

	// reta — may be absent, 0, or a float/int timestamp.
	if retaRaw, ok := raw["reta"]; ok {
		var retaF float64
		if err := json.Unmarshal(retaRaw, &retaF); err == nil && retaF > 0 {
			plan.RETA = time.Unix(int64(retaF), 0).UTC()
		}
		// if retaF == 0 or parse fails, RETA stays as zero time — that's correct.
	}

	// dest and dname — simple strings; absent or null → "".
	if destRaw, ok := raw["dest"]; ok {
		var dest string
		if err := json.Unmarshal(destRaw, &dest); err == nil {
			plan.DestinationLOCODE = strings.TrimSpace(dest)
		}
	}
	if dnameRaw, ok := raw["dname"]; ok {
		var dname string
		if err := json.Unmarshal(dnameRaw, &dname); err == nil {
			plan.DestinationName = strings.TrimSpace(dname)
		}
	}

	// wps — may be absent, null, or an array of [lat, lon] or [lat, lon, eta_unix].
	if wpsRaw, ok := raw["wps"]; ok {
		var wps [][]float64
		if json.Unmarshal(wpsRaw, &wps) == nil && len(wps) > 0 {
			waypoints := make([]RouteWaypoint, 0, len(wps))
			for i, wp := range wps {
				if len(wp) < 2 {
					// Malformed waypoint — skip silently.
					continue
				}
				w := RouteWaypoint{
					Sequence: i,
					Lat:      wp[0],
					Lon:      wp[1],
				}
				if len(wp) >= 3 && wp[2] > 0 {
					w.ETA = time.Unix(int64(wp[2]), 0).UTC()
				}
				waypoints = append(waypoints, w)
			}
			plan.Waypoints = waypoints
		}
	}

	if plan.Waypoints == nil {
		plan.Waypoints = []RouteWaypoint{}
	}

	return plan, nil
}

// ApplyRouteResult returns an updated RouteQueueItem after a fetch attempt.
//
// Mirrors ApplyScanResult from vesselfinder.go with one difference for terminal
// 404 errors: instead of parking the item permanently, it schedules a retry in
// 24 hours so routes that reappear are eventually picked up again.
//
//   - success → status='success', next_fetch_at = now + refreshInterval, attempt unchanged
//   - failure → status='failed', backoff = attempt² minutes clamped to [1m, 1h]
//   - 404 failure → status='failed', next_fetch_at = now + 24h (soft terminal)
func ApplyRouteResult(item RouteQueueItem, result RouteResult, now time.Time, refreshInterval time.Duration) RouteQueueItem {
	item.StatusCode = result.StatusCode
	item.LastFetchedAt = now.UTC()

	if result.Success {
		item.Status = "success"
		item.LastErrorCode = ""
		item.AttemptCount = 0
		item.NextFetchAt = now.UTC().Add(refreshInterval)
		return item
	}

	item.Status = "failed"
	item.AttemptCount++
	item.LastErrorCode = strings.TrimSpace(result.ErrorCode)
	if item.LastErrorCode == "" {
		item.LastErrorCode = "fetch_failed"
	}

	if result.StatusCode == 404 {
		// Soft terminal: try again tomorrow so routes that reappear are re-fetched.
		item.NextFetchAt = now.UTC().Add(24 * time.Hour)
		return item
	}

	backoff := max(time.Minute, min(time.Duration(item.AttemptCount*item.AttemptCount)*time.Minute, time.Hour))
	item.NextFetchAt = now.UTC().Add(backoff)
	return item
}
