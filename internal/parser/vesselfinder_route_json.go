package parser

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	vf "global-osint-backend/internal/packs/maritime/vesselfinder"
)

type vesselFinderRouteJSONParser struct{}

func (vesselFinderRouteJSONParser) Descriptor() Descriptor {
	return Descriptor{
		ID:               "parser:vesselfinder-route-json",
		Family:           "vesselfinder",
		Version:          "1.0.0",
		RouteScope:       "raw_document",
		SourceClass:      "browser_rendered_vessel_route",
		HandlerRef:       "internal/parser.vesselFinderRouteJSONParser",
		SupportedFormats: []string{"vesselfinder-route-json"},
	}
}

func (p vesselFinderRouteJSONParser) Parse(_ context.Context, input Input) (Result, *ParseError) {
	desc := p.Descriptor()

	if len(bytes.TrimSpace(input.Body)) == 0 {
		return Result{}, &ParseError{Code: CodeEmptyPayload, Message: "empty body"}
	}

	// extract MMSI from attrs.
	mmsi := extractMMSIFromAttrs(input.Attrs)
	if mmsi == "" {
		// No MMSI — emit empty result rather than error.
		return newResult(desc, nil), nil
	}

	plan, parseErr := vf.ParseDM3(input.Body, input.FetchedAt)
	if parseErr != nil && len(plan.Waypoints) == 0 && plan.RETA.IsZero() {
		// Complete parse failure — no usable data.
		return Result{}, &ParseError{
			Code:    CodeInvalidJSON,
			Message: "vesselfinder route json parse failed: " + parseErr.Error(),
		}
	}

	candidates := make([]Candidate, 0, 1+len(plan.Waypoints))

	occurredAt := plan.RETA
	if occurredAt.IsZero() {
		occurredAt = input.FetchedAt
	}
	var retaUnix int64
	if !plan.RETA.IsZero() {
		retaUnix = plan.RETA.Unix()
	}
	planPayload := map[string]any{
		"mmsi":               mmsi,
		"destination_locode": plan.DestinationLOCODE,
		"destination_name":   plan.DestinationName,
		"reta_unix":          retaUnix,
		"waypoint_count":     len(plan.Waypoints),
		"source_url":         input.URL,
	}
	planPayloadJSON, _ := json.Marshal(planPayload)
	planNativeID := "mmsi:" + mmsi
	planData := map[string]any{
		"record_kind":        "route_plan",
		"mmsi":               mmsi,
		"destination_locode": plan.DestinationLOCODE,
		"destination_name":   plan.DestinationName,
		"reta_unix":          retaUnix,
		"waypoint_count":     len(plan.Waypoints),
		"source_url":         input.URL,
		"occurred_at":        formatRFC3339(occurredAt),
		"payload_json":       string(planPayloadJSON),
	}
	planEnvelope := newCandidate(input, desc, "route_plan", planNativeID, planData, nil, nil)
	candidates = append(candidates, planEnvelope)

	for _, wp := range plan.Waypoints {
		wpOccurredAt := wp.ETA
		if wpOccurredAt.IsZero() {
			wpOccurredAt = input.FetchedAt
		}
		var etaUnix int64
		if !wp.ETA.IsZero() {
			etaUnix = wp.ETA.Unix()
		}
		lat := wp.Lat
		lon := wp.Lon
		wpNativeID := fmt.Sprintf("mmsi:%s:wp:%d", mmsi, wp.Sequence)
		wpPayload := map[string]any{
			"mmsi":      mmsi,
			"sequence":  wp.Sequence,
			"latitude":  wp.Lat,
			"longitude": wp.Lon,
			"eta_unix":  etaUnix,
		}
		wpPayloadJSON, _ := json.Marshal(wpPayload)
		wpData := map[string]any{
			"record_kind":  "route_waypoint",
			"mmsi":         mmsi,
			"sequence":     wp.Sequence,
			"lat":          lat,
			"lon":          lon,
			"latitude":     wp.Lat,
			"longitude":    wp.Lon,
			"eta_unix":     etaUnix,
			"occurred_at":  formatRFC3339(wpOccurredAt),
			"payload_json": string(wpPayloadJSON),
		}
		wpEnvelope := newCandidate(input, desc, "route_waypoint", wpNativeID, wpData, nil, nil)
		candidates = append(candidates, wpEnvelope)
	}

	return newResult(desc, candidates), nil
}

// extractMMSIFromAttrs reads MMSI from attrs["vesselfinder"]["mmsi"] with a
// fallback to attrs["mmsi"].
func extractMMSIFromAttrs(attrs map[string]any) string {
	if attrs == nil {
		return ""
	}
	// Primary: attrs["vesselfinder"] is a JSON string or map.
	if raw, ok := attrs["vesselfinder"]; ok {
		switch v := raw.(type) {
		case map[string]any:
			if mmsi, _ := v["mmsi"].(string); mmsi != "" {
				return mmsi
			}
		case string:
			var nested map[string]json.RawMessage
			if json.Unmarshal([]byte(v), &nested) == nil {
				if mmsiRaw, ok := nested["mmsi"]; ok {
					var mmsi string
					if json.Unmarshal(mmsiRaw, &mmsi) == nil && mmsi != "" {
						return mmsi
					}
				}
			}
		}
	}
	// Fallback: attrs["mmsi"] directly.
	if mmsi, _ := attrs["mmsi"].(string); mmsi != "" {
		return mmsi
	}
	return ""
}

