package canonical

import "time"

const (
	SchemaVersion        uint32 = 1
	InitialRecordVersion uint64 = 1
)

type EnvelopeOptions struct {
	SourceID      string
	RawID         string
	NativeID      string
	ParserID      string
	ParserVersion string
	RecordVersion uint64
	Attrs         map[string]any
	Evidence      []Evidence
}

type RecordEnvelope struct {
	Kind          string         `json:"kind"`
	ID            string         `json:"id"`
	IDStrategy    IDStrategy     `json:"id_strategy"`
	SchemaVersion uint32         `json:"schema_version"`
	RecordVersion uint64         `json:"record_version"`
	SourceID      string         `json:"source_id,omitempty"`
	RawID         string         `json:"raw_id,omitempty"`
	NativeID      string         `json:"native_id,omitempty"`
	ContentHash   string         `json:"content_hash"`
	ParserID      string         `json:"parser_id,omitempty"`
	ParserVersion string         `json:"parser_version,omitempty"`
	Data          map[string]any `json:"data"`
	Attrs         map[string]any `json:"attrs,omitempty"`
	Evidence      []Evidence     `json:"evidence,omitempty"`
}

type ObservationEnvelope struct {
	RecordEnvelope
	SubjectType      string         `json:"subject_type"`
	SubjectID        string         `json:"subject_id"`
	ObservationType  string         `json:"observation_type"`
	PlaceID          string         `json:"place_id,omitempty"`
	ParentPlaceChain []string       `json:"parent_place_chain,omitempty"`
	ObservedAt       time.Time      `json:"observed_at"`
	PublishedAt      *time.Time     `json:"published_at,omitempty"`
	ConfidenceBand   string         `json:"confidence_band,omitempty"`
	MeasurementUnit  string         `json:"measurement_unit,omitempty"`
	MeasurementValue *float64       `json:"measurement_value,omitempty"`
	Payload          map[string]any `json:"payload,omitempty"`
}

type EventEnvelope struct {
	RecordEnvelope
	EventType        string         `json:"event_type"`
	EventSubtype     string         `json:"event_subtype,omitempty"`
	PlaceID          string         `json:"place_id,omitempty"`
	ParentPlaceChain []string       `json:"parent_place_chain,omitempty"`
	StartsAt         time.Time      `json:"starts_at"`
	EndsAt           *time.Time     `json:"ends_at,omitempty"`
	Status           string         `json:"status,omitempty"`
	ConfidenceBand   string         `json:"confidence_band,omitempty"`
	ImpactScore      *float32       `json:"impact_score,omitempty"`
	Payload          map[string]any `json:"payload,omitempty"`
}

type EntityEnvelope struct {
	RecordEnvelope
	EntityType     string         `json:"entity_type"`
	CanonicalName  string         `json:"canonical_name"`
	Status         string         `json:"status,omitempty"`
	RiskBand       string         `json:"risk_band,omitempty"`
	PrimaryPlaceID string         `json:"primary_place_id,omitempty"`
	ValidFrom      *time.Time     `json:"valid_from,omitempty"`
	ValidTo        *time.Time     `json:"valid_to,omitempty"`
	Aliases        []string       `json:"aliases,omitempty"`
	Payload        map[string]any `json:"payload,omitempty"`
}

type TrackEnvelope struct {
	RecordEnvelope
	TrackID     string         `json:"track_id"`
	TrackType   string         `json:"track_type"`
	EntityID    string         `json:"entity_id,omitempty"`
	PlaceID     string         `json:"place_id,omitempty"`
	FromPlaceID string         `json:"from_place_id,omitempty"`
	ToPlaceID   string         `json:"to_place_id,omitempty"`
	ObservedAt  *time.Time     `json:"observed_at,omitempty"`
	StartedAt   *time.Time     `json:"started_at,omitempty"`
	EndedAt     *time.Time     `json:"ended_at,omitempty"`
	Latitude    *float64       `json:"latitude,omitempty"`
	Longitude   *float64       `json:"longitude,omitempty"`
	AltitudeM   *float64       `json:"altitude_m,omitempty"`
	SpeedKPH    *float32       `json:"speed_kph,omitempty"`
	CourseDeg   *float32       `json:"course_deg,omitempty"`
	DistanceKM  *float64       `json:"distance_km,omitempty"`
	PointCount  *uint32        `json:"point_count,omitempty"`
	Payload     map[string]any `json:"payload,omitempty"`
}

type EvidenceEnvelope struct {
	RecordEnvelope
	EvidenceType string     `json:"evidence_type"`
	CapturedAt   *time.Time `json:"captured_at,omitempty"`
	Payload      Evidence   `json:"payload"`
}

func NewRecordEnvelope(kind string, data map[string]any, opts EnvelopeOptions) RecordEnvelope {
	if data == nil {
		data = map[string]any{}
	}
	if opts.RecordVersion == 0 {
		opts.RecordVersion = InitialRecordVersion
	}
	contentHash := HashContent(map[string]any{
		"kind":      kind,
		"source_id": opts.SourceID,
		"native_id": opts.NativeID,
		"data":      data,
	})
	identity := NewIdentity(IDOptions{
		Namespace: kind,
		SourceID:  opts.SourceID,
		NativeID:  opts.NativeID,
		Content: map[string]any{
			"kind":         kind,
			"source_id":    opts.SourceID,
			"raw_id":       opts.RawID,
			"content_hash": contentHash,
			"data":         data,
		},
	})
	return RecordEnvelope{
		Kind:          kind,
		ID:            identity.ID,
		IDStrategy:    identity.Strategy,
		SchemaVersion: SchemaVersion,
		RecordVersion: opts.RecordVersion,
		SourceID:      opts.SourceID,
		RawID:         opts.RawID,
		NativeID:      opts.NativeID,
		ContentHash:   contentHash,
		ParserID:      opts.ParserID,
		ParserVersion: opts.ParserVersion,
		Data:          cloneRequiredMap(data),
		Attrs:         cloneMap(opts.Attrs),
		Evidence:      append([]Evidence(nil), opts.Evidence...),
	}
}

func cloneRequiredMap(input map[string]any) map[string]any {
	if input == nil {
		return map[string]any{}
	}
	out := make(map[string]any, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}

func cloneMap(input map[string]any) map[string]any {
	if len(input) == 0 {
		return nil
	}
	out := make(map[string]any, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}
