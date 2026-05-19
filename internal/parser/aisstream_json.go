package parser

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"global-osint-backend/internal/packs/maritime/aisstream"
)

type aisStreamJSONParser struct{}

func (p aisStreamJSONParser) Descriptor() Descriptor {
	return Descriptor{
		ID:               "parser:aisstream-json",
		Family:           "aisstream",
		Version:          "1.0.0",
		RouteScope:       "raw_document",
		SourceClass:      "websocket_stream_ais",
		SupportedFormats: []string{"aisstream-json"},
		HandlerRef:       "internal/parser.aisStreamJSONParser",
	}
}

func (p aisStreamJSONParser) Parse(ctx context.Context, input Input) (Result, *ParseError) {
	desc := p.Descriptor()

	if trimBody(input.Body) == "" {
		return Result{}, &ParseError{
			Code:    CodeEmptyPayload,
			Message: "aisstream-json payload is empty",
		}
	}

	envelopes, err := aisstream.ParseBatch(input.Body)
	if err != nil {
		return Result{}, &ParseError{
			Code:      CodeInvalidJSON,
			Message:   "aisstream-json: failed to parse batch: " + err.Error(),
			Retryable: false,
		}
	}

	candidates := make([]Candidate, 0, len(envelopes))
	for i, env := range envelopes {
		sourceRecordKey := env.MessageType + ":" + fmt.Sprintf("%d", env.MetaData.MMSI) + ":" + fmt.Sprintf("%d", env.MetaData.TimeUTC.UnixMilli())

		var candidate Candidate
		switch env.MessageType {
		case "PositionReport", "StandardClassBPositionReport", "ExtendedClassBEquipmentPositionReport":
			candidate = p.buildTrackPointCandidate(input, desc, env, sourceRecordKey, i)
		case "ShipStaticData":
			candidate = p.buildShipStaticCandidate(input, desc, env, sourceRecordKey, i)
		case "AidToNavigationReport":
			candidate = p.buildAtonCandidate(input, desc, env, sourceRecordKey, i)
		case "BaseStationReport":
			candidate = p.buildBaseStationCandidate(input, desc, env, sourceRecordKey, i)
		default:
			candidate = p.buildRawCandidate(input, desc, env, sourceRecordKey, i)
		}
		candidates = append(candidates, candidate)
	}

	return newResult(desc, candidates), nil
}

// decodedPosition holds the common positional fields extracted from the three
// track-point message types.
type decodedPosition struct {
	Sog         float64
	Cog         float64
	TrueHeading int
	NavStatus   int
	Latitude    float64
	Longitude   float64
}

// decodePositionFields switches on env.MessageType, decodes the inner message,
// and returns a decodedPosition. An error is returned only when json.Unmarshal
// fails; callers should treat a non-nil error as "skip extra fields".
func decodePositionFields(env aisstream.Envelope) (decodedPosition, error) {
	switch env.MessageType {
	case "PositionReport":
		var msg struct {
			PositionReport aisstream.PositionReport `json:"PositionReport"`
		}
		if err := json.Unmarshal(env.Message, &msg); err != nil {
			return decodedPosition{}, err
		}
		pr := msg.PositionReport
		return decodedPosition{
			Sog:         pr.Sog,
			Cog:         pr.Cog,
			TrueHeading: pr.TrueHeading,
			NavStatus:   pr.NavigationalStatus,
			Latitude:    pr.Latitude,
			Longitude:   pr.Longitude,
		}, nil

	case "StandardClassBPositionReport":
		var msg struct {
			StandardClassBPositionReport aisstream.StandardClassBPositionReport `json:"StandardClassBPositionReport"`
		}
		if err := json.Unmarshal(env.Message, &msg); err != nil {
			return decodedPosition{}, err
		}
		pr := msg.StandardClassBPositionReport
		return decodedPosition{
			Sog:         pr.Sog,
			Cog:         pr.Cog,
			TrueHeading: pr.TrueHeading,
			NavStatus:   pr.NavigationalStatus,
			Latitude:    pr.Latitude,
			Longitude:   pr.Longitude,
		}, nil

	case "ExtendedClassBEquipmentPositionReport":
		var msg struct {
			ExtendedClassBEquipmentPositionReport aisstream.ExtendedClassBEquipmentPositionReport `json:"ExtendedClassBEquipmentPositionReport"`
		}
		if err := json.Unmarshal(env.Message, &msg); err != nil {
			return decodedPosition{}, err
		}
		pr := msg.ExtendedClassBEquipmentPositionReport
		return decodedPosition{
			Sog:         pr.Sog,
			Cog:         pr.Cog,
			TrueHeading: pr.TrueHeading,
			NavStatus:   pr.NavigationalStatus,
			Latitude:    pr.Latitude,
			Longitude:   pr.Longitude,
		}, nil

	default:
		return decodedPosition{}, fmt.Errorf("unsupported message type: %s", env.MessageType)
	}
}

func (p aisStreamJSONParser) buildTrackPointCandidate(input Input, desc Descriptor, env aisstream.Envelope, sourceRecordKey string, idx int) Candidate {
	mmsiStr := strconv.Itoa(env.MetaData.MMSI)

	occurredAt := env.MetaData.TimeUTC
	if occurredAt.IsZero() {
		occurredAt = input.FetchedAt
	}

	data := map[string]any{
		"record_kind": "track_point",
		"mmsi":        mmsiStr,
	}

	lat, lon, _, ok := env.PositionData()
	if ok {
		data["lat"] = lat
		data["lon"] = lon
	}

	if pos, err := decodePositionFields(env); err == nil {
		data["sog"] = pos.Sog
		data["cog"] = pos.Cog
		data["heading"] = pos.TrueHeading
		data["nav_status"] = pos.NavStatus
		if pos.Latitude != 0 || pos.Longitude != 0 {
			data["lat"] = pos.Latitude
			data["lon"] = pos.Longitude
		}
	}

	if !occurredAt.IsZero() {
		data["observed_at"] = formatRFC3339(occurredAt)
	}

	attrs := map[string]any{
		"source_record_key":   sourceRecordKey,
		"source_record_index": idx,
	}

	return newCandidate(input, desc, "track_point", mmsiStr, data, attrs, nil)
}

func (p aisStreamJSONParser) buildShipStaticCandidate(input Input, desc Descriptor, env aisstream.Envelope, sourceRecordKey string, idx int) Candidate {
	mmsiStr := strconv.Itoa(env.MetaData.MMSI)

	occurredAt := env.MetaData.TimeUTC
	if occurredAt.IsZero() {
		occurredAt = input.FetchedAt
	}

	entityID := env.EntityID()

	data := map[string]any{
		"record_kind": "entity",
		"entity_type": "vessel",
		"entity_id":   entityID,
		"mmsi":        mmsiStr,
	}
	if env.MetaData.ShipName != "" {
		data["name"] = env.MetaData.ShipName
	}
	if !occurredAt.IsZero() {
		data["observed_at"] = formatRFC3339(occurredAt)
	}

	var msg struct {
		ShipStaticData aisstream.ShipStaticData `json:"ShipStaticData"`
	}
	if err := json.Unmarshal(env.Message, &msg); err == nil {
		ssd := msg.ShipStaticData
		if ssd.IMO != 0 {
			data["imo"] = ssd.IMO
		}
		if ssd.Name != "" {
			data["name"] = ssd.Name
		}
		if ssd.CallSign != "" {
			data["callsign"] = ssd.CallSign
		}
		data["ship_type"] = ssd.ShipType
		data["dim_bow"] = ssd.DimToBow
		data["dim_stern"] = ssd.DimToStern
		data["dim_port"] = ssd.DimToPort
		data["dim_starboard"] = ssd.DimToStarboard
	}

	attrs := map[string]any{
		"source_record_key":   sourceRecordKey,
		"source_record_index": idx,
	}

	return newCandidate(input, desc, "entity", mmsiStr, data, attrs, nil)
}

func (p aisStreamJSONParser) buildAtonCandidate(input Input, desc Descriptor, env aisstream.Envelope, sourceRecordKey string, idx int) Candidate {
	mmsiStr := strconv.Itoa(env.MetaData.MMSI)

	occurredAt := env.MetaData.TimeUTC
	if occurredAt.IsZero() {
		occurredAt = input.FetchedAt
	}

	data := map[string]any{
		"record_kind": "entity",
		"entity_type": "navaid",
		"mmsi":        mmsiStr,
	}
	if !occurredAt.IsZero() {
		data["observed_at"] = formatRFC3339(occurredAt)
	}

	var msg struct {
		AidToNavigationReport aisstream.AidToNavigationReport `json:"AidToNavigationReport"`
	}
	if err := json.Unmarshal(env.Message, &msg); err == nil {
		aton := msg.AidToNavigationReport
		if aton.Name != "" {
			data["name"] = aton.Name
		}
		if aton.Latitude != 0 || aton.Longitude != 0 {
			data["lat"] = aton.Latitude
			data["lon"] = aton.Longitude
		}
	}

	attrs := map[string]any{
		"source_record_key":   sourceRecordKey,
		"source_record_index": idx,
	}

	return newCandidate(input, desc, "entity", mmsiStr, data, attrs, nil)
}

func (p aisStreamJSONParser) buildBaseStationCandidate(input Input, desc Descriptor, env aisstream.Envelope, sourceRecordKey string, idx int) Candidate {
	mmsiStr := strconv.Itoa(env.MetaData.MMSI)

	occurredAt := env.MetaData.TimeUTC
	if occurredAt.IsZero() {
		occurredAt = input.FetchedAt
	}

	data := map[string]any{
		"record_kind": "entity",
		"entity_type": "basestation",
		"mmsi":        mmsiStr,
	}
	if !occurredAt.IsZero() {
		data["observed_at"] = formatRFC3339(occurredAt)
	}

	var msg struct {
		BaseStationReport aisstream.BaseStationReport `json:"BaseStationReport"`
	}
	if err := json.Unmarshal(env.Message, &msg); err == nil {
		bsr := msg.BaseStationReport
		if bsr.Latitude != 0 || bsr.Longitude != 0 {
			data["lat"] = bsr.Latitude
			data["lon"] = bsr.Longitude
		}
	}

	attrs := map[string]any{
		"source_record_key":   sourceRecordKey,
		"source_record_index": idx,
	}

	return newCandidate(input, desc, "entity", mmsiStr, data, attrs, nil)
}

func (p aisStreamJSONParser) buildRawCandidate(input Input, desc Descriptor, env aisstream.Envelope, sourceRecordKey string, idx int) Candidate {
	mmsiStr := strconv.Itoa(env.MetaData.MMSI)

	rawMsg, _ := json.Marshal(env.Message)
	data := map[string]any{
		"record_kind":  "raw",
		"message_type": env.MessageType,
		"mmsi":         mmsiStr,
		"raw_message":  string(rawMsg),
	}

	attrs := map[string]any{
		"source_record_key":   sourceRecordKey,
		"source_record_index": idx,
	}

	return newCandidate(input, desc, "raw", mmsiStr, data, attrs, nil)
}
