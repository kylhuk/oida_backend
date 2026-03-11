package promote

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"global-osint-backend/internal/parser"
)

const (
	DefaultMinLocationConfidence = 0.75
	pipelineSchemaVersion        = 1
	pipelineAPIContractVersion   = 1
)

type Options struct {
	Now                   func() time.Time
	MinLocationConfidence float64
}

type Pipeline struct {
	now                   func() time.Time
	minLocationConfidence float64
}

type Input struct {
	SourceID  string             `json:"source_id"`
	Discovery DiscoveryRecord    `json:"discovery"`
	Fetch     FetchRecord        `json:"fetch"`
	Parse     ParseRecord        `json:"parse"`
	Location  LocationResolution `json:"location"`
}

type DiscoveryRecord struct {
	FrontierID   string    `json:"frontier_id"`
	URL          string    `json:"url"`
	CanonicalURL string    `json:"canonical_url"`
	DiscoveredAt time.Time `json:"discovered_at"`
}

type FetchRecord struct {
	RawID       string    `json:"raw_id"`
	URL         string    `json:"url"`
	ContentType string    `json:"content_type"`
	ContentHash string    `json:"content_hash"`
	StatusCode  uint16    `json:"status_code"`
	FetchedAt   time.Time `json:"fetched_at"`
}

type ParseRecord struct {
	ParseID   string           `json:"parse_id"`
	Candidate parser.Candidate `json:"candidate"`
}

type LocationResolution struct {
	Resolved         bool           `json:"resolved"`
	PlaceID          string         `json:"place_id"`
	ParentPlaceChain []string       `json:"parent_place_chain"`
	Confidence       float64        `json:"confidence"`
	Method           string         `json:"method"`
	ResolvedAt       time.Time      `json:"resolved_at"`
	FailureReason    string         `json:"failure_reason"`
	LocationHint     string         `json:"location_hint"`
	Attrs            map[string]any `json:"attrs"`
}

type Plan struct {
	Observations []ObservationRow
	Events       []EventRow
	Entities     []EntityRow
	Unresolved   []UnresolvedLocationRow
	Stats        Stats
}

type Stats struct {
	Inputs             int `json:"inputs"`
	ObservationRows    int `json:"observation_rows"`
	EventRows          int `json:"event_rows"`
	EntityRows         int `json:"entity_rows"`
	UnresolvedRows     int `json:"unresolved_rows"`
	ResolvedCandidates int `json:"resolved_candidates"`
}

type ObservationRow struct {
	ObservationID    string
	SourceID         string
	SubjectType      string
	SubjectID        string
	ObservationType  string
	PlaceID          string
	ParentPlaceChain []string
	ObservedAt       time.Time
	PublishedAt      *time.Time
	ConfidenceBand   string
	MeasurementUnit  string
	MeasurementValue float64
	SchemaVersion    uint32
	Attrs            map[string]any
	Evidence         []parser.Evidence
}

type EventRow struct {
	EventID          string
	SourceID         string
	EventType        string
	EventSubtype     string
	PlaceID          string
	ParentPlaceChain []string
	StartsAt         time.Time
	EndsAt           *time.Time
	Status           string
	ConfidenceBand   string
	ImpactScore      float32
	SchemaVersion    uint32
	Attrs            map[string]any
	Evidence         []parser.Evidence
}

type EntityRow struct {
	EntityID           string
	EntityType         string
	CanonicalName      string
	Status             string
	RiskBand           string
	PrimaryPlaceID     string
	SourceEntityKey    string
	SourceSystem       string
	ValidFrom          time.Time
	SchemaVersion      uint32
	RecordVersion      uint64
	APIContractVersion uint32
	UpdatedAt          time.Time
	Attrs              map[string]any
	Evidence           []parser.Evidence
}

type UnresolvedLocationRow struct {
	QueueID       string
	SubjectKind   string
	SubjectID     string
	SourceID      string
	RawID         string
	ResolverStage string
	FailureReason string
	State         string
	Priority      int16
	RetryCount    uint16
	FirstFailedAt time.Time
	LastFailedAt  time.Time
	NextRetryAt   time.Time
	LocationHint  string
	Attrs         map[string]any
	Evidence      []parser.Evidence
}

type entityRef struct {
	EntityID        string
	EntityType      string
	CanonicalName   string
	Status          string
	RiskBand        string
	SourceEntityKey string
	SourceSystem    string
	Attrs           map[string]any
	Evidence        []parser.Evidence
}

func NewPipeline(options Options) *Pipeline {
	now := options.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC().Truncate(time.Millisecond) }
	}
	minConfidence := options.MinLocationConfidence
	if minConfidence <= 0 {
		minConfidence = DefaultMinLocationConfidence
	}
	return &Pipeline{now: now, minLocationConfidence: minConfidence}
}

func (p *Pipeline) Prepare(inputs []Input) (Plan, error) {
	plan := Plan{Stats: Stats{Inputs: len(inputs)}}
	obsByID := map[string]ObservationRow{}
	eventByID := map[string]EventRow{}
	entityByID := map[string]EntityRow{}
	queueByID := map[string]UnresolvedLocationRow{}

	for idx, input := range inputs {
		normalized, err := normalizeInput(input)
		if err != nil {
			return Plan{}, fmt.Errorf("input %d: %w", idx+1, err)
		}

		kind := canonicalKind(normalized.Parse.Candidate)
		if kind == "" {
			return Plan{}, fmt.Errorf("input %d: canonical record_kind is required", idx+1)
		}

		recordID, err := canonicalRecordID(kind, normalized)
		if err != nil {
			return Plan{}, fmt.Errorf("input %d: %w", idx+1, err)
		}

		if !locationResolved(normalized.Location, p.minLocationConfidence) {
			queue := buildUnresolvedRow(kind, recordID, normalized, p.now())
			queueByID[queue.QueueID] = queue
			continue
		}

		stageAttrs := stageAttrs(normalized)
		stageEvidence := stageEvidence(normalized)
		chain := resolvedParentChain(normalized.Location)

		switch kind {
		case "observation":
			row, entities, err := buildObservationRow(normalized, chain, stageAttrs, stageEvidence)
			if err != nil {
				return Plan{}, fmt.Errorf("input %d: %w", idx+1, err)
			}
			obsByID[row.ObservationID] = row
			for _, entity := range entities {
				entityByID[entity.EntityID] = entity
			}
			plan.Stats.ResolvedCandidates++
		case "event":
			row, entities, err := buildEventRow(normalized, chain, stageAttrs, stageEvidence, p.now())
			if err != nil {
				return Plan{}, fmt.Errorf("input %d: %w", idx+1, err)
			}
			eventByID[row.EventID] = row
			for _, entity := range entities {
				entityByID[entity.EntityID] = entity
			}
			plan.Stats.ResolvedCandidates++
		case "entity":
			entity, err := buildEntityOnlyRow(normalized, stageAttrs, stageEvidence, p.now())
			if err != nil {
				return Plan{}, fmt.Errorf("input %d: %w", idx+1, err)
			}
			entityByID[entity.EntityID] = entity
			plan.Stats.ResolvedCandidates++
		default:
			return Plan{}, fmt.Errorf("input %d: unsupported record_kind %q", idx+1, kind)
		}
	}

	plan.Observations = sortObservationRows(obsByID)
	plan.Events = sortEventRows(eventByID)
	plan.Entities = sortEntityRows(entityByID)
	plan.Unresolved = sortUnresolvedRows(queueByID)
	plan.Stats.ObservationRows = len(plan.Observations)
	plan.Stats.EventRows = len(plan.Events)
	plan.Stats.EntityRows = len(plan.Entities)
	plan.Stats.UnresolvedRows = len(plan.Unresolved)
	return plan, nil
}

func (p Plan) SQLStatements() ([]string, error) {
	statements := []string{}
	if sql, err := buildEntityInsertSQL(p.Entities); err != nil {
		return nil, err
	} else if sql != "" {
		statements = append(statements, sql)
	}
	if sql, err := buildObservationInsertSQL(p.Observations); err != nil {
		return nil, err
	} else if sql != "" {
		statements = append(statements, sql)
	}
	if sql, err := buildEventInsertSQL(p.Events); err != nil {
		return nil, err
	} else if sql != "" {
		statements = append(statements, sql)
	}
	if sql, err := buildUnresolvedInsertSQL(p.Unresolved); err != nil {
		return nil, err
	} else if sql != "" {
		statements = append(statements, sql)
	}
	return statements, nil
}

func SampleInputs() []Input {
	observedAt := time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC)
	publishedAt := observedAt.Add(15 * time.Minute)
	startsAt := observedAt.Add(2 * time.Hour)
	endsAt := startsAt.Add(90 * time.Minute)
	resolvedAt := observedAt.Add(20 * time.Minute)
	return []Input{
		{
			SourceID:  "fixture:newsroom",
			Discovery: DiscoveryRecord{FrontierID: "frontier:1", URL: "https://example.com/reports/obs-1", CanonicalURL: "https://example.com/reports/obs-1", DiscoveredAt: observedAt.Add(-1 * time.Hour)},
			Fetch:     FetchRecord{RawID: "raw:obs-1", URL: "https://example.com/reports/obs-1", ContentType: "application/json", ContentHash: "rawhash-obs-1", StatusCode: 200, FetchedAt: observedAt.Add(-30 * time.Minute)},
			Parse: ParseRecord{
				ParseID: "parse:obs-1",
				Candidate: parser.Candidate{
					Kind:          "json_row",
					SchemaVersion: 1,
					RecordVersion: 1,
					ParserID:      "parser:json",
					ParserVersion: "1.0.0",
					SourceID:      "fixture:newsroom",
					RawID:         "raw:obs-1",
					NativeID:      "obs-1",
					ContentHash:   "candidatehash-obs-1",
					Data: map[string]any{
						"record_kind":       "observation",
						"observation_type":  "incident_count",
						"subject_type":      "organization",
						"subject_name":      "Northwatch",
						"measurement_unit":  "count",
						"measurement_value": 4,
						"observed_at":       observedAt.Format(time.RFC3339),
						"published_at":      publishedAt.Format(time.RFC3339),
						"confidence_band":   "high",
					},
					Attrs:    map[string]any{"pack": "general-web"},
					Evidence: []parser.Evidence{{Kind: "parser_field", Ref: "subject_name", Value: "Northwatch"}},
				},
			},
			Location: LocationResolution{Resolved: true, PlaceID: "plc:us-tx-paris", ParentPlaceChain: []string{"plc:us-tx", "plc:us", "plc:na"}, Confidence: 0.93, Method: "reverse_geocode", ResolvedAt: resolvedAt, LocationHint: "Paris, Texas"},
		},
		{
			SourceID:  "fixture:newsroom",
			Discovery: DiscoveryRecord{FrontierID: "frontier:2", URL: "https://example.com/reports/event-1", CanonicalURL: "https://example.com/reports/event-1", DiscoveredAt: observedAt.Add(-2 * time.Hour)},
			Fetch:     FetchRecord{RawID: "raw:event-1", URL: "https://example.com/reports/event-1", ContentType: "application/json", ContentHash: "rawhash-event-1", StatusCode: 200, FetchedAt: startsAt.Add(-45 * time.Minute)},
			Parse: ParseRecord{
				ParseID: "parse:event-1",
				Candidate: parser.Candidate{
					Kind:          "json_row",
					SchemaVersion: 1,
					RecordVersion: 1,
					ParserID:      "parser:json",
					ParserVersion: "1.0.0",
					SourceID:      "fixture:newsroom",
					RawID:         "raw:event-1",
					NativeID:      "event-1",
					ContentHash:   "candidatehash-event-1",
					Data: map[string]any{
						"record_kind":     "event",
						"event_type":      "protest",
						"event_subtype":   "labor",
						"status":          "active",
						"starts_at":       startsAt.Format(time.RFC3339),
						"ends_at":         endsAt.Format(time.RFC3339),
						"impact_score":    7.5,
						"confidence_band": "medium",
						"entities": []any{
							map[string]any{"entity_type": "organization", "canonical_name": "Dockworkers Union", "source_entity_key": "union-9"},
						},
					},
				},
			},
			Location: LocationResolution{Resolved: true, PlaceID: "plc:fr-idf-paris", ParentPlaceChain: []string{"plc:fr-idf", "plc:fr", "plc:eu"}, Confidence: 0.89, Method: "place_name", ResolvedAt: resolvedAt.Add(30 * time.Minute), LocationHint: "Paris, France"},
		},
		{
			SourceID:  "fixture:registry",
			Discovery: DiscoveryRecord{FrontierID: "frontier:3", URL: "https://example.com/entities/entity-1", CanonicalURL: "https://example.com/entities/entity-1", DiscoveredAt: observedAt.Add(-3 * time.Hour)},
			Fetch:     FetchRecord{RawID: "raw:entity-1", URL: "https://example.com/entities/entity-1", ContentType: "application/json", ContentHash: "rawhash-entity-1", StatusCode: 200, FetchedAt: observedAt.Add(-2 * time.Hour)},
			Parse: ParseRecord{
				ParseID: "parse:entity-1",
				Candidate: parser.Candidate{
					Kind:          "json_object",
					SchemaVersion: 1,
					RecordVersion: 1,
					ParserID:      "parser:json",
					ParserVersion: "1.0.0",
					SourceID:      "fixture:registry",
					RawID:         "raw:entity-1",
					NativeID:      "entity-1",
					ContentHash:   "candidatehash-entity-1",
					Data: map[string]any{
						"record_kind":       "entity",
						"entity_type":       "vessel",
						"canonical_name":    "MV Meridian",
						"status":            "active",
						"risk_band":         "medium",
						"source_entity_key": "imo-1234567",
					},
				},
			},
			Location: LocationResolution{Resolved: true, PlaceID: "plc:sg", ParentPlaceChain: []string{"plc:asia"}, Confidence: 0.88, Method: "registry_port", ResolvedAt: resolvedAt.Add(45 * time.Minute), LocationHint: "Singapore"},
		},
		{
			SourceID:  "fixture:newsroom",
			Discovery: DiscoveryRecord{FrontierID: "frontier:4", URL: "https://example.com/reports/unresolved-1", CanonicalURL: "https://example.com/reports/unresolved-1", DiscoveredAt: observedAt.Add(-40 * time.Minute)},
			Fetch:     FetchRecord{RawID: "raw:unresolved-1", URL: "https://example.com/reports/unresolved-1", ContentType: "application/json", ContentHash: "rawhash-unresolved-1", StatusCode: 200, FetchedAt: observedAt.Add(-20 * time.Minute)},
			Parse: ParseRecord{
				ParseID: "parse:unresolved-1",
				Candidate: parser.Candidate{
					Kind:          "json_row",
					SchemaVersion: 1,
					RecordVersion: 1,
					ParserID:      "parser:json",
					ParserVersion: "1.0.0",
					SourceID:      "fixture:newsroom",
					RawID:         "raw:unresolved-1",
					NativeID:      "obs-unresolved-1",
					ContentHash:   "candidatehash-unresolved-1",
					Data: map[string]any{
						"record_kind":       "observation",
						"observation_type":  "incident_count",
						"subject_type":      "organization",
						"subject_name":      "Blue Harbor",
						"measurement_unit":  "count",
						"measurement_value": 2,
						"observed_at":       observedAt.Format(time.RFC3339),
					},
				},
			},
			Location: LocationResolution{Resolved: false, Confidence: 0.41, Method: "place_name", FailureReason: "ambiguous_place_name", LocationHint: "Paris", ResolvedAt: resolvedAt.Add(10 * time.Minute)},
		},
	}
}

func normalizeInput(input Input) (Input, error) {
	sourceID := firstNonEmpty(input.SourceID, input.Parse.Candidate.SourceID)
	if sourceID == "" {
		return Input{}, fmt.Errorf("source_id is required")
	}
	input.SourceID = sourceID
	input.Parse.Candidate.SourceID = sourceID

	if strings.TrimSpace(input.Discovery.FrontierID) == "" {
		return Input{}, fmt.Errorf("discovery.frontier_id is required")
	}
	if firstNonEmpty(input.Discovery.CanonicalURL, input.Discovery.URL) == "" {
		return Input{}, fmt.Errorf("discovery canonical/url is required")
	}
	if strings.TrimSpace(input.Fetch.RawID) == "" {
		return Input{}, fmt.Errorf("fetch.raw_id is required")
	}
	if input.Fetch.FetchedAt.IsZero() {
		return Input{}, fmt.Errorf("fetch.fetched_at is required")
	}
	if strings.TrimSpace(input.Parse.ParseID) == "" {
		return Input{}, fmt.Errorf("parse.parse_id is required")
	}
	if strings.TrimSpace(input.Parse.Candidate.ParserID) == "" {
		return Input{}, fmt.Errorf("parse.candidate.parser_id is required")
	}
	if input.Parse.Candidate.RawID == "" {
		input.Parse.Candidate.RawID = input.Fetch.RawID
	}
	if input.Parse.Candidate.RawID != input.Fetch.RawID {
		return Input{}, fmt.Errorf("parse candidate raw_id %q does not match fetch.raw_id %q", input.Parse.Candidate.RawID, input.Fetch.RawID)
	}
	if input.Parse.Candidate.SchemaVersion == 0 {
		input.Parse.Candidate.SchemaVersion = pipelineSchemaVersion
	}
	if input.Parse.Candidate.RecordVersion == 0 {
		input.Parse.Candidate.RecordVersion = 1
	}
	if input.Fetch.URL == "" {
		input.Fetch.URL = firstNonEmpty(input.Discovery.CanonicalURL, input.Discovery.URL)
	}
	if input.Location.Attrs == nil {
		input.Location.Attrs = map[string]any{}
	}
	return input, nil
}

func canonicalKind(candidate parser.Candidate) string {
	if candidate.Kind == "observation" || candidate.Kind == "event" || candidate.Kind == "entity" {
		return candidate.Kind
	}
	return strings.ToLower(strings.TrimSpace(stringValue(candidate.Data["record_kind"])))
}

func canonicalRecordID(kind string, input Input) (string, error) {
	data := input.Parse.Candidate.Data
	switch kind {
	case "observation":
		observationType := firstNonEmpty(stringValue(data["observation_type"]), input.Parse.Candidate.Kind)
		if observationType == "" {
			return "", fmt.Errorf("observation_type is required")
		}
		observedAt, err := timeValue(data["observed_at"], input.Fetch.FetchedAt)
		if err != nil {
			return "", fmt.Errorf("invalid observed_at: %w", err)
		}
		subject := entityFromObservation(input.Parse.Candidate, input.SourceID)
		return stableID("observation", input.SourceID, firstNonEmpty(input.Parse.Candidate.NativeID, input.Parse.Candidate.ContentHash), observationType, subject.EntityID, observedAt.UTC().Format(time.RFC3339Nano)), nil
	case "event":
		eventType := stringValue(data["event_type"])
		if eventType == "" {
			return "", fmt.Errorf("event_type is required")
		}
		startsAt, err := timeValue(data["starts_at"], input.Fetch.FetchedAt)
		if err != nil {
			return "", fmt.Errorf("invalid starts_at: %w", err)
		}
		return stableID("event", input.SourceID, firstNonEmpty(input.Parse.Candidate.NativeID, input.Parse.Candidate.ContentHash), eventType, startsAt.UTC().Format(time.RFC3339Nano)), nil
	case "entity":
		entity, err := entityFromCandidate(input.Parse.Candidate, input.SourceID)
		if err != nil {
			return "", err
		}
		return entity.EntityID, nil
	default:
		return "", fmt.Errorf("unsupported record_kind %q", kind)
	}
}

func locationResolved(location LocationResolution, minConfidence float64) bool {
	if !location.Resolved {
		return false
	}
	if strings.TrimSpace(location.PlaceID) == "" {
		return false
	}
	return location.Confidence >= minConfidence
}

func buildObservationRow(input Input, chain []string, stageAttrs map[string]any, stageEvidence []parser.Evidence) (ObservationRow, []EntityRow, error) {
	data := input.Parse.Candidate.Data
	observationType := firstNonEmpty(stringValue(data["observation_type"]), input.Parse.Candidate.Kind)
	if observationType == "" {
		return ObservationRow{}, nil, fmt.Errorf("observation_type is required")
	}
	observedAt, err := timeValue(data["observed_at"], input.Fetch.FetchedAt)
	if err != nil {
		return ObservationRow{}, nil, fmt.Errorf("invalid observed_at: %w", err)
	}
	publishedAt, err := nullableTimeValue(data["published_at"])
	if err != nil {
		return ObservationRow{}, nil, fmt.Errorf("invalid published_at: %w", err)
	}
	measureValue, err := floatValue(data["measurement_value"], 0)
	if err != nil {
		return ObservationRow{}, nil, fmt.Errorf("invalid measurement_value: %w", err)
	}
	subject := entityFromObservation(input.Parse.Candidate, input.SourceID)
	row := ObservationRow{
		ObservationID:    stableID("observation", input.SourceID, firstNonEmpty(input.Parse.Candidate.NativeID, input.Parse.Candidate.ContentHash), observationType, subject.EntityID, observedAt.UTC().Format(time.RFC3339Nano)),
		SourceID:         input.SourceID,
		SubjectType:      subject.EntityType,
		SubjectID:        subject.EntityID,
		ObservationType:  observationType,
		PlaceID:          input.Location.PlaceID,
		ParentPlaceChain: chain,
		ObservedAt:       observedAt.UTC(),
		PublishedAt:      publishedAt,
		ConfidenceBand:   confidenceBand(stringValue(data["confidence_band"]), input.Location.Confidence),
		MeasurementUnit:  defaultString(stringValue(data["measurement_unit"]), "count"),
		MeasurementValue: measureValue,
		SchemaVersion:    pipelineSchemaVersion,
		Attrs:            mergeMaps(stageAttrs, map[string]any{"canonical": cloneMap(data), "parser_attrs": cloneMap(input.Parse.Candidate.Attrs)}),
		Evidence:         appendEvidence(stageEvidence, input.Parse.Candidate.Evidence...),
	}
	entity := entityRow(subject, input.Location.PlaceID, observedAt.UTC(), row.Attrs, row.Evidence)
	return row, []EntityRow{entity}, nil
}

func buildEventRow(input Input, chain []string, stageAttrs map[string]any, stageEvidence []parser.Evidence, now time.Time) (EventRow, []EntityRow, error) {
	data := input.Parse.Candidate.Data
	eventType := stringValue(data["event_type"])
	if eventType == "" {
		return EventRow{}, nil, fmt.Errorf("event_type is required")
	}
	startsAt, err := timeValue(data["starts_at"], input.Fetch.FetchedAt)
	if err != nil {
		return EventRow{}, nil, fmt.Errorf("invalid starts_at: %w", err)
	}
	endsAt, err := nullableTimeValue(data["ends_at"])
	if err != nil {
		return EventRow{}, nil, fmt.Errorf("invalid ends_at: %w", err)
	}
	impactScore, err := floatValue(data["impact_score"], 0)
	if err != nil {
		return EventRow{}, nil, fmt.Errorf("invalid impact_score: %w", err)
	}
	refs, err := entitiesFromList(data["entities"], input.SourceID)
	if err != nil {
		return EventRow{}, nil, fmt.Errorf("invalid entities: %w", err)
	}
	row := EventRow{
		EventID:          stableID("event", input.SourceID, firstNonEmpty(input.Parse.Candidate.NativeID, input.Parse.Candidate.ContentHash), eventType, startsAt.UTC().Format(time.RFC3339Nano)),
		SourceID:         input.SourceID,
		EventType:        eventType,
		EventSubtype:     defaultString(stringValue(data["event_subtype"]), "general"),
		PlaceID:          input.Location.PlaceID,
		ParentPlaceChain: chain,
		StartsAt:         startsAt.UTC(),
		EndsAt:           endsAt,
		Status:           defaultString(stringValue(data["status"]), "observed"),
		ConfidenceBand:   confidenceBand(stringValue(data["confidence_band"]), input.Location.Confidence),
		ImpactScore:      float32(impactScore),
		SchemaVersion:    pipelineSchemaVersion,
		Attrs:            mergeMaps(stageAttrs, map[string]any{"canonical": cloneMap(data), "parser_attrs": cloneMap(input.Parse.Candidate.Attrs)}),
		Evidence:         appendEvidence(stageEvidence, input.Parse.Candidate.Evidence...),
	}
	entities := make([]EntityRow, 0, len(refs))
	for _, ref := range refs {
		entities = append(entities, entityRow(ref, input.Location.PlaceID, now, mergeMaps(stageAttrs, ref.Attrs), appendEvidence(stageEvidence, ref.Evidence...)))
	}
	return row, entities, nil
}

func buildEntityOnlyRow(input Input, stageAttrs map[string]any, stageEvidence []parser.Evidence, now time.Time) (EntityRow, error) {
	ref, err := entityFromCandidate(input.Parse.Candidate, input.SourceID)
	if err != nil {
		return EntityRow{}, err
	}
	return entityRow(ref, input.Location.PlaceID, now, mergeMaps(stageAttrs, map[string]any{"canonical": cloneMap(input.Parse.Candidate.Data), "parser_attrs": cloneMap(input.Parse.Candidate.Attrs)}), appendEvidence(stageEvidence, input.Parse.Candidate.Evidence...)), nil
}

func buildUnresolvedRow(kind, recordID string, input Input, now time.Time) UnresolvedLocationRow {
	failureReason := defaultString(strings.TrimSpace(input.Location.FailureReason), "location_unresolved")
	if input.Location.Resolved && input.Location.Confidence < DefaultMinLocationConfidence {
		failureReason = "location_confidence_below_threshold"
	}
	attrs := mergeMaps(stageAttrs(input), map[string]any{
		"canonical":           cloneMap(input.Parse.Candidate.Data),
		"parser_attrs":        cloneMap(input.Parse.Candidate.Attrs),
		"location_confidence": input.Location.Confidence,
	})
	evidence := appendEvidence(stageEvidence(input), input.Parse.Candidate.Evidence...)
	failedAt := now.UTC()
	if !input.Location.ResolvedAt.IsZero() {
		failedAt = input.Location.ResolvedAt.UTC()
	}
	return UnresolvedLocationRow{
		QueueID:       stableID("unresolved", kind, recordID, input.SourceID, input.Fetch.RawID, failureReason),
		SubjectKind:   kind,
		SubjectID:     recordID,
		SourceID:      input.SourceID,
		RawID:         input.Fetch.RawID,
		ResolverStage: "location",
		FailureReason: failureReason,
		State:         "pending",
		Priority:      unresolvedPriority(input.Location.Confidence),
		RetryCount:    0,
		FirstFailedAt: failedAt,
		LastFailedAt:  failedAt,
		NextRetryAt:   failedAt.Add(6 * time.Hour),
		LocationHint:  defaultString(input.Location.LocationHint, stringValue(input.Parse.Candidate.Data["location_hint"])),
		Attrs:         attrs,
		Evidence:      evidence,
	}
}

func entityRow(ref entityRef, placeID string, validFrom time.Time, attrs map[string]any, evidence []parser.Evidence) EntityRow {
	return EntityRow{
		EntityID:           ref.EntityID,
		EntityType:         defaultString(ref.EntityType, "unknown"),
		CanonicalName:      defaultString(ref.CanonicalName, ref.EntityID),
		Status:             defaultString(ref.Status, "active"),
		RiskBand:           defaultString(ref.RiskBand, "unknown"),
		PrimaryPlaceID:     placeID,
		SourceEntityKey:    defaultString(ref.SourceEntityKey, ref.EntityID),
		SourceSystem:       defaultString(ref.SourceSystem, "canonical_pipeline"),
		ValidFrom:          validFrom.UTC(),
		SchemaVersion:      pipelineSchemaVersion,
		RecordVersion:      1,
		APIContractVersion: pipelineAPIContractVersion,
		UpdatedAt:          validFrom.UTC(),
		Attrs:              attrs,
		Evidence:           evidence,
	}
}

func entityFromObservation(candidate parser.Candidate, sourceID string) entityRef {
	if nested, ok := mapValue(candidate.Data["subject"]); ok {
		ref, err := entityFromMap(nested, sourceID, candidate.NativeID)
		if err == nil {
			return ref
		}
	}
	ref, _ := entityFromMap(map[string]any{
		"entity_id":         candidate.Data["subject_id"],
		"entity_type":       candidate.Data["subject_type"],
		"canonical_name":    candidate.Data["subject_name"],
		"status":            candidate.Data["subject_status"],
		"risk_band":         candidate.Data["subject_risk_band"],
		"source_entity_key": candidate.Data["subject_key"],
		"source_system":     candidate.Data["source_system"],
	}, sourceID, candidate.NativeID)
	return ref
}

func entityFromCandidate(candidate parser.Candidate, sourceID string) (entityRef, error) {
	return entityFromMap(candidate.Data, sourceID, candidate.NativeID)
}

func entityFromMap(data map[string]any, sourceID, fallbackKey string) (entityRef, error) {
	entityType := defaultString(stringValue(data["entity_type"]), "unknown")
	canonicalName := defaultString(stringValue(data["canonical_name"]), stringValue(data["subject_name"]))
	sourceKey := firstNonEmpty(stringValue(data["source_entity_key"]), stringValue(data["subject_key"]), fallbackKey, canonicalName)
	entityID := stringValue(data["entity_id"])
	if entityID == "" {
		entityID = stableID("entity", sourceID, entityType, sourceKey)
	}
	if entityID == "" {
		return entityRef{}, fmt.Errorf("entity_id could not be derived")
	}
	return entityRef{
		EntityID:        entityID,
		EntityType:      entityType,
		CanonicalName:   defaultString(canonicalName, entityID),
		Status:          defaultString(stringValue(data["status"]), "active"),
		RiskBand:        defaultString(stringValue(data["risk_band"]), "unknown"),
		SourceEntityKey: sourceKey,
		SourceSystem:    defaultString(stringValue(data["source_system"]), "canonical_pipeline"),
		Attrs:           cloneMap(data),
	}, nil
}

func entitiesFromList(raw any, sourceID string) ([]entityRef, error) {
	if raw == nil {
		return nil, nil
	}
	items, ok := raw.([]any)
	if !ok {
		return nil, fmt.Errorf("expected array, got %T", raw)
	}
	refs := make([]entityRef, 0, len(items))
	for idx, item := range items {
		mapped, ok := mapValue(item)
		if !ok {
			return nil, fmt.Errorf("item %d is %T", idx, item)
		}
		ref, err := entityFromMap(mapped, sourceID, fmt.Sprintf("entity-%d", idx+1))
		if err != nil {
			return nil, err
		}
		refs = append(refs, ref)
	}
	return refs, nil
}

func stageAttrs(input Input) map[string]any {
	return map[string]any{
		"source_id": input.SourceID,
		"discovery": map[string]any{
			"frontier_id":   input.Discovery.FrontierID,
			"url":           input.Discovery.URL,
			"canonical_url": input.Discovery.CanonicalURL,
		},
		"fetch": map[string]any{
			"raw_id":       input.Fetch.RawID,
			"url":          input.Fetch.URL,
			"content_type": input.Fetch.ContentType,
			"content_hash": input.Fetch.ContentHash,
			"status_code":  input.Fetch.StatusCode,
		},
		"parse": map[string]any{
			"parse_id":         input.Parse.ParseID,
			"parser_id":        input.Parse.Candidate.ParserID,
			"parser_version":   input.Parse.Candidate.ParserVersion,
			"candidate_kind":   input.Parse.Candidate.Kind,
			"candidate_native": input.Parse.Candidate.NativeID,
		},
		"location": map[string]any{
			"resolved":           input.Location.Resolved,
			"place_id":           input.Location.PlaceID,
			"parent_place_chain": resolvedParentChain(input.Location),
			"confidence":         input.Location.Confidence,
			"method":             input.Location.Method,
			"failure_reason":     input.Location.FailureReason,
			"hint":               input.Location.LocationHint,
			"attrs":              cloneMap(input.Location.Attrs),
		},
	}
}

func stageEvidence(input Input) []parser.Evidence {
	evidence := []parser.Evidence{
		{Kind: "frontier", Ref: input.Discovery.FrontierID, Value: firstNonEmpty(input.Discovery.CanonicalURL, input.Discovery.URL)},
		{Kind: "raw_document", Ref: input.Fetch.RawID, Value: input.Fetch.URL},
		{Kind: "parse", Ref: input.Parse.ParseID, Value: input.Parse.Candidate.ParserID},
	}
	if input.Location.PlaceID != "" || input.Location.Method != "" || input.Location.FailureReason != "" {
		evidence = append(evidence, parser.Evidence{Kind: "location", Ref: input.Location.PlaceID, Value: firstNonEmpty(input.Location.Method, input.Location.FailureReason)})
	}
	return evidence
}

func resolvedParentChain(location LocationResolution) []string {
	if len(location.ParentPlaceChain) > 0 {
		return append([]string(nil), location.ParentPlaceChain...)
	}
	if attrsChain, ok := stringSlice(location.Attrs["parent_chain_place_ids"]); ok {
		return attrsChain
	}
	return []string{}
}

func confidenceBand(existing string, score float64) string {
	if existing != "" {
		return existing
	}
	switch {
	case score >= 0.9:
		return "high"
	case score >= 0.75:
		return "medium"
	default:
		return "low"
	}
}

func unresolvedPriority(score float64) int16 {
	switch {
	case score >= 0.7:
		return 90
	case score >= 0.5:
		return 70
	default:
		return 50
	}
}

func buildEntityInsertSQL(rows []EntityRow) (string, error) {
	if len(rows) == 0 {
		return "", nil
	}
	var b strings.Builder
	b.WriteString("INSERT INTO silver.dim_entity (entity_id, entity_type, canonical_name, status, risk_band, primary_place_id, source_entity_key, source_system, valid_from, valid_to, schema_version, record_version, api_contract_version, updated_at, attrs, evidence) SELECT input.entity_id, input.entity_type, input.canonical_name, input.status, input.risk_band, input.primary_place_id, input.source_entity_key, input.source_system, input.valid_from, NULL, input.schema_version, input.record_version, input.api_contract_version, input.updated_at, input.attrs, input.evidence FROM (")
	for idx, row := range rows {
		if idx > 0 {
			b.WriteString(" UNION ALL ")
		}
		attrs, err := jsonString(row.Attrs)
		if err != nil {
			return "", err
		}
		evidence, err := jsonString(row.Evidence)
		if err != nil {
			return "", err
		}
		fmt.Fprintf(&b, "SELECT %s AS entity_id, %s AS entity_type, %s AS canonical_name, %s AS status, %s AS risk_band, %s AS primary_place_id, %s AS source_entity_key, %s AS source_system, %s AS valid_from, %d AS schema_version, %d AS record_version, %d AS api_contract_version, %s AS updated_at, %s AS attrs, %s AS evidence",
			sqlString(row.EntityID),
			sqlString(row.EntityType),
			sqlString(row.CanonicalName),
			sqlString(row.Status),
			sqlString(row.RiskBand),
			nullableSQLString(row.PrimaryPlaceID),
			sqlString(row.SourceEntityKey),
			sqlString(row.SourceSystem),
			sqlTime(row.ValidFrom),
			row.SchemaVersion,
			row.RecordVersion,
			row.APIContractVersion,
			sqlTime(row.UpdatedAt),
			sqlString(attrs),
			sqlString(evidence),
		)
	}
	b.WriteString(") AS input LEFT JOIN (SELECT DISTINCT entity_id, record_version FROM silver.dim_entity WHERE (")
	for idx, row := range rows {
		if idx > 0 {
			b.WriteString(" OR ")
		}
		fmt.Fprintf(&b, "(entity_id = %s AND record_version = %d)", sqlString(row.EntityID), row.RecordVersion)
	}
	b.WriteString(")) AS existing ON existing.entity_id = input.entity_id AND existing.record_version = input.record_version WHERE existing.entity_id = ''")
	return b.String(), nil
}

func buildObservationInsertSQL(rows []ObservationRow) (string, error) {
	if len(rows) == 0 {
		return "", nil
	}
	var b strings.Builder
	b.WriteString("INSERT INTO silver.fact_observation (observation_id, source_id, subject_type, subject_id, observation_type, place_id, parent_place_chain, observed_at, published_at, confidence_band, measurement_unit, measurement_value, schema_version, attrs, evidence) SELECT input.observation_id, input.source_id, input.subject_type, input.subject_id, input.observation_type, input.place_id, input.parent_place_chain, input.observed_at, input.published_at, input.confidence_band, input.measurement_unit, input.measurement_value, input.schema_version, input.attrs, input.evidence FROM (")
	for idx, row := range rows {
		if idx > 0 {
			b.WriteString(" UNION ALL ")
		}
		attrs, err := jsonString(row.Attrs)
		if err != nil {
			return "", err
		}
		evidence, err := jsonString(row.Evidence)
		if err != nil {
			return "", err
		}
		fmt.Fprintf(&b, "SELECT %s AS observation_id, %s AS source_id, %s AS subject_type, %s AS subject_id, %s AS observation_type, %s AS place_id, %s AS parent_place_chain, %s AS observed_at, %s AS published_at, %s AS confidence_band, %s AS measurement_unit, %s AS measurement_value, %d AS schema_version, %s AS attrs, %s AS evidence",
			sqlString(row.ObservationID),
			sqlString(row.SourceID),
			sqlString(row.SubjectType),
			sqlString(row.SubjectID),
			sqlString(row.ObservationType),
			sqlString(row.PlaceID),
			sqlStringArray(row.ParentPlaceChain),
			sqlTime(row.ObservedAt),
			nullableSQLTime(row.PublishedAt),
			sqlString(row.ConfidenceBand),
			sqlString(row.MeasurementUnit),
			formatFloat(row.MeasurementValue),
			row.SchemaVersion,
			sqlString(attrs),
			sqlString(evidence),
		)
	}
	b.WriteString(") AS input LEFT JOIN (SELECT DISTINCT observation_id FROM silver.fact_observation WHERE observation_id IN (")
	for idx, row := range rows {
		if idx > 0 {
			b.WriteString(",")
		}
		b.WriteString(sqlString(row.ObservationID))
	}
	b.WriteString(")) AS existing ON existing.observation_id = input.observation_id WHERE existing.observation_id = ''")
	return b.String(), nil
}

func buildEventInsertSQL(rows []EventRow) (string, error) {
	if len(rows) == 0 {
		return "", nil
	}
	var b strings.Builder
	b.WriteString("INSERT INTO silver.fact_event (event_id, source_id, event_type, event_subtype, place_id, parent_place_chain, starts_at, ends_at, status, confidence_band, impact_score, schema_version, attrs, evidence) SELECT input.event_id, input.source_id, input.event_type, input.event_subtype, input.place_id, input.parent_place_chain, input.starts_at, input.ends_at, input.status, input.confidence_band, input.impact_score, input.schema_version, input.attrs, input.evidence FROM (")
	for idx, row := range rows {
		if idx > 0 {
			b.WriteString(" UNION ALL ")
		}
		attrs, err := jsonString(row.Attrs)
		if err != nil {
			return "", err
		}
		evidence, err := jsonString(row.Evidence)
		if err != nil {
			return "", err
		}
		fmt.Fprintf(&b, "SELECT %s AS event_id, %s AS source_id, %s AS event_type, %s AS event_subtype, %s AS place_id, %s AS parent_place_chain, %s AS starts_at, %s AS ends_at, %s AS status, %s AS confidence_band, %s AS impact_score, %d AS schema_version, %s AS attrs, %s AS evidence",
			sqlString(row.EventID),
			sqlString(row.SourceID),
			sqlString(row.EventType),
			sqlString(row.EventSubtype),
			sqlString(row.PlaceID),
			sqlStringArray(row.ParentPlaceChain),
			sqlTime(row.StartsAt),
			nullableSQLTime(row.EndsAt),
			sqlString(row.Status),
			sqlString(row.ConfidenceBand),
			formatFloat(float64(row.ImpactScore)),
			row.SchemaVersion,
			sqlString(attrs),
			sqlString(evidence),
		)
	}
	b.WriteString(") AS input LEFT JOIN (SELECT DISTINCT event_id FROM silver.fact_event WHERE event_id IN (")
	for idx, row := range rows {
		if idx > 0 {
			b.WriteString(",")
		}
		b.WriteString(sqlString(row.EventID))
	}
	b.WriteString(")) AS existing ON existing.event_id = input.event_id WHERE existing.event_id = ''")
	return b.String(), nil
}

func buildUnresolvedInsertSQL(rows []UnresolvedLocationRow) (string, error) {
	if len(rows) == 0 {
		return "", nil
	}
	var b strings.Builder
	b.WriteString("INSERT INTO ops.unresolved_location_queue (queue_id, subject_kind, subject_id, source_id, raw_id, resolver_stage, failure_reason, state, priority, retry_count, first_failed_at, last_failed_at, next_retry_at, location_hint, attrs, evidence) SELECT input.queue_id, input.subject_kind, input.subject_id, input.source_id, input.raw_id, input.resolver_stage, input.failure_reason, input.state, input.priority, input.retry_count, input.first_failed_at, input.last_failed_at, input.next_retry_at, input.location_hint, input.attrs, input.evidence FROM (")
	for idx, row := range rows {
		if idx > 0 {
			b.WriteString(" UNION ALL ")
		}
		attrs, err := jsonString(row.Attrs)
		if err != nil {
			return "", err
		}
		evidence, err := jsonString(row.Evidence)
		if err != nil {
			return "", err
		}
		fmt.Fprintf(&b, "SELECT %s AS queue_id, %s AS subject_kind, %s AS subject_id, %s AS source_id, %s AS raw_id, %s AS resolver_stage, %s AS failure_reason, %s AS state, %d AS priority, %d AS retry_count, %s AS first_failed_at, %s AS last_failed_at, %s AS next_retry_at, %s AS location_hint, %s AS attrs, %s AS evidence",
			sqlString(row.QueueID),
			sqlString(row.SubjectKind),
			sqlString(row.SubjectID),
			sqlString(row.SourceID),
			sqlString(row.RawID),
			sqlString(row.ResolverStage),
			sqlString(row.FailureReason),
			sqlString(row.State),
			row.Priority,
			row.RetryCount,
			sqlTime(row.FirstFailedAt),
			sqlTime(row.LastFailedAt),
			sqlTime(row.NextRetryAt),
			sqlString(row.LocationHint),
			sqlString(attrs),
			sqlString(evidence),
		)
	}
	b.WriteString(") AS input LEFT JOIN (SELECT DISTINCT queue_id FROM ops.unresolved_location_queue WHERE queue_id IN (")
	for idx, row := range rows {
		if idx > 0 {
			b.WriteString(",")
		}
		b.WriteString(sqlString(row.QueueID))
	}
	b.WriteString(")) AS existing ON existing.queue_id = input.queue_id WHERE existing.queue_id = ''")
	return b.String(), nil
}

func sortObservationRows(source map[string]ObservationRow) []ObservationRow {
	rows := make([]ObservationRow, 0, len(source))
	for _, row := range source {
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].ObservationID < rows[j].ObservationID })
	return rows
}

func sortEventRows(source map[string]EventRow) []EventRow {
	rows := make([]EventRow, 0, len(source))
	for _, row := range source {
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].EventID < rows[j].EventID })
	return rows
}

func sortEntityRows(source map[string]EntityRow) []EntityRow {
	rows := make([]EntityRow, 0, len(source))
	for _, row := range source {
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].EntityID < rows[j].EntityID })
	return rows
}

func sortUnresolvedRows(source map[string]UnresolvedLocationRow) []UnresolvedLocationRow {
	rows := make([]UnresolvedLocationRow, 0, len(source))
	for _, row := range source {
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].QueueID < rows[j].QueueID })
	return rows
}

func stableID(parts ...string) string {
	h := sha256.New()
	for _, part := range parts {
		h.Write([]byte(strings.TrimSpace(part)))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

func appendEvidence(base []parser.Evidence, extra ...parser.Evidence) []parser.Evidence {
	merged := append([]parser.Evidence{}, base...)
	merged = append(merged, extra...)
	return merged
}

func mergeMaps(base map[string]any, extras ...map[string]any) map[string]any {
	merged := map[string]any{}
	for key, value := range base {
		merged[key] = value
	}
	for _, extra := range extras {
		for key, value := range extra {
			merged[key] = value
		}
	}
	return merged
}

func cloneMap(source map[string]any) map[string]any {
	if source == nil {
		return map[string]any{}
	}
	cloned := make(map[string]any, len(source))
	for key, value := range source {
		cloned[key] = value
	}
	return cloned
}

func jsonString(v any) (string, error) {
	encoded, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

func sqlString(v string) string {
	return "'" + strings.ReplaceAll(v, "'", "''") + "'"
}

func nullableSQLString(v string) string {
	if strings.TrimSpace(v) == "" {
		return "NULL"
	}
	return sqlString(v)
}

func sqlStringArray(values []string) string {
	if len(values) == 0 {
		return "[]"
	}
	parts := make([]string, 0, len(values))
	for _, value := range values {
		parts = append(parts, sqlString(value))
	}
	return "[" + strings.Join(parts, ",") + "]"
}

func sqlTime(v time.Time) string {
	return fmt.Sprintf("toDateTime64(%s, 3, 'UTC')", sqlString(v.UTC().Format("2006-01-02 15:04:05.000")))
}

func nullableSQLTime(v *time.Time) string {
	if v == nil {
		return "CAST(NULL, 'Nullable(DateTime64(3, ''UTC''))')"
	}
	return sqlTime(v.UTC())
}

func formatFloat(v float64) string {
	formatted := strconv.FormatFloat(v, 'f', -1, 64)
	if !strings.ContainsAny(formatted, ".eE") {
		return formatted + ".0"
	}
	return formatted
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func defaultString(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

func stringValue(raw any) string {
	switch typed := raw.(type) {
	case string:
		return strings.TrimSpace(typed)
	case fmt.Stringer:
		return strings.TrimSpace(typed.String())
	case json.Number:
		return typed.String()
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(typed), 'f', -1, 64)
	case int:
		return strconv.Itoa(typed)
	case int64:
		return strconv.FormatInt(typed, 10)
	case uint64:
		return strconv.FormatUint(typed, 10)
	default:
		return ""
	}
}

func floatValue(raw any, fallback float64) (float64, error) {
	if raw == nil {
		return fallback, nil
	}
	switch typed := raw.(type) {
	case float64:
		return typed, nil
	case float32:
		return float64(typed), nil
	case int:
		return float64(typed), nil
	case int64:
		return float64(typed), nil
	case json.Number:
		return typed.Float64()
	case string:
		if strings.TrimSpace(typed) == "" {
			return fallback, nil
		}
		return strconv.ParseFloat(strings.TrimSpace(typed), 64)
	default:
		return 0, fmt.Errorf("unsupported float type %T", raw)
	}
}

func timeValue(raw any, fallback time.Time) (time.Time, error) {
	if raw == nil || stringValue(raw) == "" {
		return fallback.UTC(), nil
	}
	if value, ok := raw.(time.Time); ok {
		return value.UTC(), nil
	}
	parsed, err := time.Parse(time.RFC3339, stringValue(raw))
	if err != nil {
		return time.Time{}, err
	}
	return parsed.UTC(), nil
}

func nullableTimeValue(raw any) (*time.Time, error) {
	if raw == nil || stringValue(raw) == "" {
		return nil, nil
	}
	parsed, err := timeValue(raw, time.Time{})
	if err != nil {
		return nil, err
	}
	return &parsed, nil
}

func mapValue(raw any) (map[string]any, bool) {
	if raw == nil {
		return nil, false
	}
	mapped, ok := raw.(map[string]any)
	return mapped, ok
}

func stringSlice(raw any) ([]string, bool) {
	items, ok := raw.([]any)
	if !ok {
		return nil, false
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		if value := stringValue(item); value != "" {
			out = append(out, value)
		}
	}
	return out, true
}
