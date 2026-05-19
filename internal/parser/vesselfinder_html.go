package parser

import (
	"context"
	"fmt"
	"strings"
	"time"

	vf "global-osint-backend/internal/packs/maritime/vesselfinder"
)

const (
	CodeBotOrCaptchaPage = "bot_or_captcha_page"
	CodeMissingRecord    = "missing_record"
	CodeInvalidHTMLPage  = "invalid_html_page"
)

type vesselFinderHTMLParser struct{}

func (vesselFinderHTMLParser) Descriptor() Descriptor {
	return Descriptor{
		ID:               "parser:vesselfinder-html",
		Family:           "vesselfinder",
		Version:          "1.1.0",
		RouteScope:       "raw_document",
		SourceClass:      "browser_rendered_vessel_detail",
		HandlerRef:       "internal/parser.vesselFinderHTMLParser",
		SupportedFormats: []string{"vesselfinder-html"},
	}
}

func (p vesselFinderHTMLParser) Parse(_ context.Context, input Input) (Result, *ParseError) {
	desc := p.Descriptor()
	if trimBody(input.Body) == "" {
		return Result{}, &ParseError{Code: CodeEmptyPayload, Message: "vesselfinder html payload is empty"}
	}
	body := string(input.Body)
	if len(vf.ExtractDetailLinks(body, input.URL)) > 0 && !strings.Contains(strings.ToLower(input.URL), "/vessels/details/") {
		return newResult(desc, nil), nil
	}
	meta, err := vf.ParseDetail(body, input.URL, input.FetchedAt)
	if err != nil {
		return Result{}, mapVesselFinderParseError(err)
	}
	discovery := vesselFinderDiscoveryContext(input.Attrs, meta.MMSI)
	flagState := firstNonEmptyString(meta.Flag, discovery.CountryLabel)
	vesselType := firstNonEmptyString(meta.VesselType, discovery.TypeLabel)

	entityID := vesselEntityID(meta)
	nativeEntityID := "detail:" + meta.DetailID
	if meta.IMO != "" {
		nativeEntityID = "imo:" + meta.IMO
	} else if meta.MMSI != "" {
		nativeEntityID = "mmsi:" + meta.MMSI
	}
	entityData := map[string]any{
		"record_kind":      "entity",
		"entity_id":        entityID,
		"entity_type":      "vessel",
		"canonical_name":   meta.Name,
		"status":           firstNonEmptyString(meta.Status, "active"),
		"risk_band":        "watch",
		"source_system":    input.SourceID,
		"source_url":       input.URL,
		"detail_id":        meta.DetailID,
		"imo":              meta.IMO,
		"mmsi":             meta.MMSI,
		"call_sign":        meta.CallSign,
		"flag_state":       flagState,
		"flag_state_code":  discovery.CountryCode,
		"vessel_type":      vesselType,
		"vessel_type_code": discovery.TypeCode,
		"place_id":         discovery.PlaceID,
		"place_hint":       discovery.PlaceID,
		"observed_at":      formatRFC3339(meta.ObservedAt),
	}
	entityAttrs := map[string]any{
		"source_id":            input.SourceID,
		"metadata_fingerprint": vf.MetadataFingerprint(meta),
		"detail_url":           meta.DetailURL,
		"discovery_context":    discovery.Attrs(),
	}
	entity := newCandidate(input, desc, "entity", nativeEntityID, entityData, entityAttrs, nil)

	candidates := []Candidate{entity}
	if meta.Latitude != nil && meta.Longitude != nil {
		nativePointID := fmt.Sprintf("%s:%s", firstNonEmptyString(meta.IMO, meta.MMSI, meta.DetailID), meta.ObservedAt.UTC().Format("20060102150405"))
		pointData := map[string]any{
			"record_kind": "track_point",
			"track_id":    "trk:vessel:" + firstNonEmptyString(meta.IMO, meta.MMSI, meta.DetailID),
			"track_type":  "vessel",
			"entity_id":   entityID,
			"observed_at": formatRFC3339(meta.ObservedAt),
			"lat":         *meta.Latitude,
			"lon":         *meta.Longitude,
			"latitude":    *meta.Latitude,
			"longitude":   *meta.Longitude,
			"source_id":   input.SourceID,
			"detail_id":   meta.DetailID,
			"imo":         meta.IMO,
			"mmsi":        meta.MMSI,
			"status":      meta.Status,
			"place_id":    discovery.PlaceID,
		}
		if meta.SpeedKPH != nil {
			pointData["speed_kph"] = *meta.SpeedKPH
		}
		if meta.CourseDeg != nil {
			pointData["course_deg"] = *meta.CourseDeg
		}
		pointAttrs := map[string]any{
			"source_id":            input.SourceID,
			"metadata_fingerprint": vf.MetadataFingerprint(meta),
			"detail_url":           meta.DetailURL,
			"discovery_context":    discovery.Attrs(),
		}
		candidates = append(candidates, newCandidate(input, desc, "track_point", nativePointID, pointData, pointAttrs, nil))
	}

	entityKey := firstNonEmptyString(meta.IMO, meta.MMSI, meta.DetailID)
	for _, call := range meta.PortCalls {
		if call.UNLOCODE == "" || call.ArrivedAt.IsZero() {
			continue
		}
		placeID := "plc:port:" + strings.ToLower(call.UNLOCODE)
		nativeCallID := fmt.Sprintf("portcall:%s:%s", entityKey, call.ArrivedAt.UTC().Format("20060102150405"))
		callData := map[string]any{
			"record_kind": "track_point",
			"track_id":    "trk:vessel:" + entityKey,
			"track_type":  "vessel",
			"entity_id":   entityID,
			"observed_at": formatRFC3339(call.ArrivedAt),
			"place_id":    placeID,
			"place_hint":  call.Name,
			"source_id":   input.SourceID,
			"detail_id":   meta.DetailID,
			"imo":         meta.IMO,
			"mmsi":        meta.MMSI,
			"origin":      "port_call",
		}
		if !call.DepartedAt.IsZero() {
			callData["departed_at"] = formatRFC3339(call.DepartedAt)
		}
		callAttrs := map[string]any{
			"source_id":         input.SourceID,
			"detail_url":        meta.DetailURL,
			"discovery_context": discovery.Attrs(),
			"port_call": map[string]any{
				"raw_locode":   call.RawLOCODE,
				"unlocode":     call.UNLOCODE,
				"country_name": call.CountryName,
			},
		}
		candidates = append(candidates, newCandidate(input, desc, "track_point", nativeCallID, callData, callAttrs, nil))
	}

	return newResult(desc, candidates), nil
}

func mapVesselFinderParseError(err error) *ParseError {
	switch {
	case vf.IsBotPageError(err):
		return &ParseError{Code: CodeBotOrCaptchaPage, Message: err.Error(), Retryable: true}
	case vf.IsMissingVesselError(err):
		return &ParseError{Code: CodeMissingRecord, Message: err.Error(), Retryable: false}
	default:
		return &ParseError{Code: CodeInvalidHTMLPage, Message: err.Error(), Retryable: false}
	}
}

func vesselEntityID(meta vf.VesselMetadata) string {
	return "ent:vessel:" + firstNonEmptyString(meta.IMO, meta.MMSI, meta.DetailID)
}

type vesselFinderDiscovery struct {
	CountryCode  string
	CountryLabel string
	TypeCode     string
	TypeLabel    string
	PlaceID      string
}

type maritimeID struct {
	MID         string
	CountryCode string
	Label       string
	PlaceID     string
}

var maritimeIDs = map[string]maritimeID{
	"451": {MID: "451", CountryCode: "KG", Label: "Kyrgyz Republic", PlaceID: "plc:flag:kg"},
	"533": {MID: "533", CountryCode: "MY", Label: "Malaysia", PlaceID: "plc:flag:my"},
	"538": {MID: "538", CountryCode: "MH", Label: "Marshall Islands", PlaceID: "plc:flag:mh"},
	"548": {MID: "548", CountryCode: "PH", Label: "Philippines", PlaceID: "plc:flag:ph"},
	"618": {MID: "618", CountryCode: "TF", Label: "Crozet Archipelago", PlaceID: "plc:mid:618"},
}

func (d vesselFinderDiscovery) Attrs() map[string]any {
	return map[string]any{
		"country_code":  d.CountryCode,
		"country_label": d.CountryLabel,
		"type_code":     d.TypeCode,
		"type_label":    d.TypeLabel,
		"place_id":      d.PlaceID,
	}
}

func maritimeIDFromMMSI(mmsi string) (maritimeID, bool) {
	mmsi = strings.TrimSpace(mmsi)
	if len(mmsi) < 3 {
		return maritimeID{}, false
	}
	mid, ok := maritimeIDs[mmsi[:3]]
	return mid, ok
}

func vesselFinderDiscoveryContext(attrs map[string]any, mmsi string) vesselFinderDiscovery {
	raw, _ := attrs["vesselfinder"].(map[string]any)
	countryCode := firstNonEmptyString(stringAttr(raw, "country_code"), stringAttr(attrs, "country_code"))
	mid := maritimeID{}
	if countryCode == "" {
		if derived, ok := maritimeIDFromMMSI(mmsi); ok {
			mid = derived
			countryCode = derived.CountryCode
		}
	}
	return vesselFinderDiscovery{
		CountryCode:  countryCode,
		CountryLabel: firstNonEmptyString(stringAttr(raw, "country_label"), stringAttr(attrs, "country_label"), mid.Label),
		TypeCode:     firstNonEmptyString(stringAttr(raw, "type_code"), stringAttr(attrs, "type_code")),
		TypeLabel:    firstNonEmptyString(stringAttr(raw, "type_label"), stringAttr(attrs, "type_label")),
		PlaceID:      firstNonEmptyString(stringAttr(raw, "place_id"), stringAttr(attrs, "place_id"), mid.PlaceID, flagPlaceID(countryCode)),
	}
}

func stringAttr(attrs map[string]any, key string) string {
	if attrs == nil {
		return ""
	}
	switch value := attrs[key].(type) {
	case string:
		return strings.TrimSpace(value)
	default:
		return ""
	}
}

func flagPlaceID(countryCode string) string {
	countryCode = strings.ToLower(strings.TrimSpace(countryCode))
	if countryCode == "" {
		return ""
	}
	return "plc:flag:" + countryCode
}

func formatRFC3339(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format(time.RFC3339)
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
