package geopolitical

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"global-osint-backend/internal/canonical"
	"global-osint-backend/internal/metrics"
)

const (
	SourceGDELT     = "seed:gdelt"
	SourceReliefWeb = "fixture:reliefweb"
	SourceACLED     = "fixture:acled"
	ACLEDKeyEnv     = "ACLED_API_KEY"

	defaultEventSchemaVersion uint32 = 1
)

type Options struct {
	SourceID string
	ACLEDKey string
	Now      time.Time
}

type Plan struct {
	ExecutedSources []string
	DisabledSources []DisabledSource
	Events          []EventRecord
	Entities        []EntityRecord
	EventEntities   []EventEntityLink
	EventPlaces     []EventPlaceLink
	EntityPlaces    []EntityPlaceLink
	MetricRegistry  []metrics.RegistryRecord
	Contributions   []metrics.Contribution
	Snapshots       []metrics.SnapshotRow
}

type DisabledSource struct {
	SourceID string `json:"source_id"`
	Reason   string `json:"reason"`
}

type EventRecord struct {
	EventID        string
	SourceID       string
	EventType      string
	EventSubtype   string
	PlaceID        string
	ParentChain    []string
	StartsAt       time.Time
	EndsAt         *time.Time
	Status         string
	ConfidenceBand string
	ImpactScore    float32
	Attrs          map[string]any
	Evidence       []canonical.Evidence
}

type EntityRecord struct {
	EntityID        string
	EntityType      string
	CanonicalName   string
	Status          string
	RiskBand        string
	PrimaryPlaceID  string
	SourceEntityKey string
	SourceSystem    string
	ValidFrom       time.Time
	UpdatedAt       time.Time
	RecordVersion   uint64
	Attrs           map[string]any
	Evidence        []canonical.Evidence
}

type EventEntityLink struct {
	BridgeID       string
	EventID        string
	EntityID       string
	RoleType       string
	ConfidenceBand string
	LinkedAt       time.Time
	SchemaVersion  uint32
	Attrs          map[string]any
	Evidence       []canonical.Evidence
}

type EventPlaceLink struct {
	BridgeID      string
	EventID       string
	PlaceID       string
	RelationType  string
	LinkedAt      time.Time
	SchemaVersion uint32
	Attrs         map[string]any
	Evidence      []canonical.Evidence
}

type EntityPlaceLink struct {
	BridgeID      string
	EntityID      string
	PlaceID       string
	RelationType  string
	LinkedAt      time.Time
	SchemaVersion uint32
	Attrs         map[string]any
	Evidence      []canonical.Evidence
}

type rawActor struct {
	Name string
	Type string
	Role string
}

type rawCrossLink struct {
	SourceID   string
	ExternalID string
	URL        string
	Relation   string
}

type rawEvent struct {
	SourceID         string
	NativeID         string
	Title            string
	Category         string
	Subtype          string
	PrimaryPlaceID   string
	RelatedPlaceIDs  []string
	OccurredAt       time.Time
	PublishedAt      time.Time
	Status           string
	ImpactScore      float64
	MediaMentions    float64
	Actors           []rawActor
	CrossSourceLinks []rawCrossLink
	SourceURL        string
}

type adapter struct {
	SourceID          string
	RequiresKey       bool
	DisabledByDefault bool
	Load              func(context.Context, Options) ([]rawEvent, error)
}

type metricAccumulator struct {
	metricID      string
	placeID       string
	windowStart   time.Time
	windowEnd     time.Time
	value         float64
	eventIDs      []string
	evidence      []canonical.Evidence
	sourceIDs     map[string]struct{}
	featureValues map[string]float64
}

type placeMeta struct {
	admin0 string
	chain  []string
}

func BuildIngestPlan(ctx context.Context, opts Options) (Plan, error) {
	_ = ctx
	now := opts.Now.UTC().Truncate(time.Millisecond)
	if now.IsZero() {
		now = time.Now().UTC().Truncate(time.Millisecond)
	}

	selected := strings.TrimSpace(opts.SourceID)
	var raws []rawEvent
	disabled := []DisabledSource{}
	executed := []string{}
	for _, item := range adapters() {
		if selected != "" && item.SourceID != selected {
			continue
		}
		if item.RequiresKey && strings.TrimSpace(opts.ACLEDKey) == "" {
			disabled = append(disabled, DisabledSource{SourceID: item.SourceID, Reason: "missing credentials: " + ACLEDKeyEnv})
			continue
		}
		records, err := item.Load(ctx, opts)
		if err != nil {
			return Plan{}, err
		}
		if len(records) == 0 && item.DisabledByDefault {
			disabled = append(disabled, DisabledSource{SourceID: item.SourceID, Reason: "disabled by default"})
			continue
		}
		raws = append(raws, records...)
		executed = append(executed, item.SourceID)
	}
	if selected != "" && len(executed) == 0 && len(disabled) == 0 {
		return Plan{}, fmt.Errorf("unknown geopolitical source %q", selected)
	}

	events, entities, eventEntities, eventPlaces, entityPlaces := normalizeRecords(raws, now)
	registry := buildMetricRegistry(now)
	contributions := buildMetricContributions(events)
	snapshots := metrics.BuildMetricSnapshots(metrics.BuildMetricState(contributions, now), now)

	return Plan{
		ExecutedSources: executed,
		DisabledSources: disabled,
		Events:          events,
		Entities:        entities,
		EventEntities:   eventEntities,
		EventPlaces:     eventPlaces,
		EntityPlaces:    entityPlaces,
		MetricRegistry:  registry,
		Contributions:   contributions,
		Snapshots:       snapshots,
	}, nil
}

func (p Plan) SQLStatements() ([]string, error) {
	statements := []string{}
	if sql, err := buildMetricRegistryInsertSQL(p.MetricRegistry); err != nil {
		return nil, err
	} else if sql != "" {
		statements = append(statements, sql)
	}
	if sql, err := buildEntityInsertSQL(p.Entities); err != nil {
		return nil, err
	} else if sql != "" {
		statements = append(statements, sql)
	}
	if sql, err := buildEventInsertSQL(p.Events); err != nil {
		return nil, err
	} else if sql != "" {
		statements = append(statements, sql)
	}
	if sql, err := buildEventEntityInsertSQL(p.EventEntities); err != nil {
		return nil, err
	} else if sql != "" {
		statements = append(statements, sql)
	}
	if sql, err := buildEventPlaceInsertSQL(p.EventPlaces); err != nil {
		return nil, err
	} else if sql != "" {
		statements = append(statements, sql)
	}
	if sql, err := buildEntityPlaceInsertSQL(p.EntityPlaces); err != nil {
		return nil, err
	} else if sql != "" {
		statements = append(statements, sql)
	}
	if sql, err := buildContributionInsertSQL(p.Contributions); err != nil {
		return nil, err
	} else if sql != "" {
		statements = append(statements, sql)
	}
	if sql, err := buildSnapshotInsertSQL(p.Snapshots); err != nil {
		return nil, err
	} else if sql != "" {
		statements = append(statements, sql)
	}
	return statements, nil
}

func adapters() []adapter {
	return []adapter{
		{SourceID: SourceGDELT, Load: loadGDELTFixtures},
		{SourceID: SourceReliefWeb, Load: loadReliefWebFixtures},
		{SourceID: SourceACLED, RequiresKey: true, DisabledByDefault: true, Load: loadACLEDFixtures},
	}
}

func loadGDELTFixtures(_ context.Context, _ Options) ([]rawEvent, error) {
	return []rawEvent{
		{
			SourceID:       SourceGDELT,
			NativeID:       "gdelt:1001",
			Title:          "Paris protest draws wide coverage",
			Category:       "protest",
			Subtype:        "civil_unrest",
			PrimaryPlaceID: "plc:fr-idf-paris",
			OccurredAt:     time.Date(2026, 3, 10, 8, 0, 0, 0, time.UTC),
			PublishedAt:    time.Date(2026, 3, 10, 8, 45, 0, 0, time.UTC),
			Status:         "active",
			ImpactScore:    0.72,
			MediaMentions:  16,
			Actors: []rawActor{
				{Name: "Civic Coalition", Type: "organization", Role: "protester"},
				{Name: "National Police", Type: "organization", Role: "security_force"},
			},
			CrossSourceLinks: []rawCrossLink{{SourceID: SourceReliefWeb, ExternalID: "reliefweb:2001", URL: "https://reliefweb.int/report/example-2001", Relation: "same_incident"}},
			SourceURL:        "https://www.gdeltproject.org/data.html#gdelt1001",
		},
		{
			SourceID:        SourceGDELT,
			NativeID:        "gdelt:1002",
			Title:           "Cross-border escalation follow-up",
			Category:        "conflict",
			Subtype:         "border_escalation",
			PrimaryPlaceID:  "plc:fr-idf-paris",
			RelatedPlaceIDs: []string{"plc:us-tx-paris"},
			OccurredAt:      time.Date(2026, 3, 10, 9, 30, 0, 0, time.UTC),
			PublishedAt:     time.Date(2026, 3, 10, 10, 0, 0, 0, time.UTC),
			Status:          "active",
			ImpactScore:     0.88,
			MediaMentions:   22,
			Actors: []rawActor{
				{Name: "Border Guard Unit", Type: "organization", Role: "security_force"},
				{Name: "Regional Militia", Type: "organization", Role: "armed_group"},
			},
			SourceURL: "https://www.gdeltproject.org/data.html#gdelt1002",
		},
	}, nil
}

func loadReliefWebFixtures(_ context.Context, _ Options) ([]rawEvent, error) {
	return []rawEvent{
		{
			SourceID:       SourceReliefWeb,
			NativeID:       "reliefweb:2001",
			Title:          "Aid agencies monitor protest disruption",
			Category:       "protest",
			Subtype:        "humanitarian_access_disruption",
			PrimaryPlaceID: "plc:fr-idf-paris",
			OccurredAt:     time.Date(2026, 3, 10, 8, 15, 0, 0, time.UTC),
			PublishedAt:    time.Date(2026, 3, 10, 9, 0, 0, 0, time.UTC),
			Status:         "active",
			ImpactScore:    0.64,
			MediaMentions:  8,
			Actors: []rawActor{
				{Name: "Relief Coordination Cell", Type: "organization", Role: "humanitarian_actor"},
			},
			CrossSourceLinks: []rawCrossLink{{SourceID: SourceGDELT, ExternalID: "gdelt:1001", URL: "https://www.gdeltproject.org/data.html#gdelt1001", Relation: "same_incident"}},
			SourceURL:        "https://reliefweb.int/report/example-2001",
		},
		{
			SourceID:       SourceReliefWeb,
			NativeID:       "reliefweb:2002",
			Title:          "Nauru logistics disruption drives media attention",
			Category:       "media",
			Subtype:        "logistics_disruption",
			PrimaryPlaceID: "plc:nr-yaren",
			OccurredAt:     time.Date(2026, 3, 10, 11, 0, 0, 0, time.UTC),
			PublishedAt:    time.Date(2026, 3, 10, 11, 30, 0, 0, time.UTC),
			Status:         "active",
			ImpactScore:    0.41,
			MediaMentions:  12,
			Actors: []rawActor{
				{Name: "Port Authority", Type: "organization", Role: "operator"},
			},
			SourceURL: "https://reliefweb.int/report/example-2002",
		},
	}, nil
}

func loadACLEDFixtures(_ context.Context, opts Options) ([]rawEvent, error) {
	if strings.TrimSpace(opts.ACLEDKey) == "" {
		return nil, nil
	}
	return []rawEvent{
		{
			SourceID:       SourceACLED,
			NativeID:       "acled:3001",
			Title:          "Demonstration recorded by ACLED fixture",
			Category:       "protest",
			Subtype:        "demonstration",
			PrimaryPlaceID: "plc:fr-idf-paris",
			OccurredAt:     time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC),
			PublishedAt:    time.Date(2026, 3, 10, 12, 15, 0, 0, time.UTC),
			Status:         "active",
			ImpactScore:    0.57,
			MediaMentions:  6,
			Actors: []rawActor{
				{Name: "Municipal Protest Network", Type: "organization", Role: "protester"},
			},
			SourceURL: "https://acleddata.com/fixture/3001",
		},
	}, nil
}

func normalizeRecords(raws []rawEvent, now time.Time) ([]EventRecord, []EntityRecord, []EventEntityLink, []EventPlaceLink, []EntityPlaceLink) {
	entityByID := map[string]EntityRecord{}
	entityPlaceByID := map[string]EntityPlaceLink{}
	eventEntityByID := map[string]EventEntityLink{}
	eventPlaceByID := map[string]EventPlaceLink{}
	events := make([]EventRecord, 0, len(raws))

	for _, raw := range raws {
		meta := lookupPlace(raw.PrimaryPlaceID)
		eventID := eventID(raw.SourceID, raw.NativeID)
		evidence := []canonical.Evidence{canonical.NewRawDocumentEvidence(raw.SourceID, raw.NativeID, raw.SourceURL)}
		for _, link := range raw.CrossSourceLinks {
			evidence = append(evidence, canonical.Evidence{Kind: "cross_source_link", SourceID: link.SourceID, Ref: link.ExternalID, URL: link.URL, Value: link.Relation})
		}
		actorIDs := []string{}
		for _, actor := range raw.Actors {
			entityID := entityID(actor.Type, actor.Name)
			actorIDs = append(actorIDs, entityID)
			entityByID[entityID] = EntityRecord{
				EntityID:        entityID,
				EntityType:      actor.Type,
				CanonicalName:   actor.Name,
				Status:          "active",
				RiskBand:        riskBand(raw.ImpactScore),
				PrimaryPlaceID:  raw.PrimaryPlaceID,
				SourceEntityKey: raw.SourceID + ":" + slug(actor.Name),
				SourceSystem:    raw.SourceID,
				ValidFrom:       raw.OccurredAt,
				UpdatedAt:       now,
				RecordVersion:   uint64(now.UnixMilli()),
				Attrs: map[string]any{
					"role":      actor.Role,
					"source_id": raw.SourceID,
				},
				Evidence: append([]canonical.Evidence(nil), evidence...),
			}
			bridgeID := fmt.Sprintf("bee:%s:%s", eventID, entityID)
			eventEntityByID[bridgeID] = EventEntityLink{
				BridgeID:       bridgeID,
				EventID:        eventID,
				EntityID:       entityID,
				RoleType:       actor.Role,
				ConfidenceBand: confidenceBand(raw.ImpactScore),
				LinkedAt:       raw.OccurredAt,
				SchemaVersion:  defaultEventSchemaVersion,
				Attrs:          map[string]any{"source_id": raw.SourceID},
				Evidence:       append([]canonical.Evidence(nil), evidence...),
			}
			entityPlaceID := fmt.Sprintf("bep:%s:%s", entityID, raw.PrimaryPlaceID)
			entityPlaceByID[entityPlaceID] = EntityPlaceLink{
				BridgeID:      entityPlaceID,
				EntityID:      entityID,
				PlaceID:       raw.PrimaryPlaceID,
				RelationType:  "primary_presence",
				LinkedAt:      raw.OccurredAt,
				SchemaVersion: defaultEventSchemaVersion,
				Attrs:         map[string]any{"source_id": raw.SourceID},
				Evidence:      append([]canonical.Evidence(nil), evidence...),
			}
		}

		attrs := map[string]any{
			"title":              raw.Title,
			"category":           raw.Category,
			"source_event_id":    raw.NativeID,
			"actor_ids":          actorIDs,
			"cross_source_links": rawCrossLinksToMaps(raw.CrossSourceLinks),
			"related_place_ids":  append([]string(nil), raw.RelatedPlaceIDs...),
			"source_url":         raw.SourceURL,
			"media_mentions":     raw.MediaMentions,
			"admin0_place_id":    meta.admin0,
		}
		events = append(events, EventRecord{
			EventID:        eventID,
			SourceID:       raw.SourceID,
			EventType:      raw.Category,
			EventSubtype:   raw.Subtype,
			PlaceID:        raw.PrimaryPlaceID,
			ParentChain:    meta.chain,
			StartsAt:       raw.OccurredAt,
			Status:         raw.Status,
			ConfidenceBand: confidenceBand(raw.ImpactScore),
			ImpactScore:    float32(raw.ImpactScore),
			Attrs:          attrs,
			Evidence:       evidence,
		})

		primaryPlaceID := fmt.Sprintf("bepv:%s:%s", eventID, raw.PrimaryPlaceID)
		eventPlaceByID[primaryPlaceID] = EventPlaceLink{
			BridgeID:      primaryPlaceID,
			EventID:       eventID,
			PlaceID:       raw.PrimaryPlaceID,
			RelationType:  "primary",
			LinkedAt:      raw.OccurredAt,
			SchemaVersion: defaultEventSchemaVersion,
			Attrs:         map[string]any{"source_id": raw.SourceID},
			Evidence:      append([]canonical.Evidence(nil), evidence...),
		}
		for _, relatedPlaceID := range raw.RelatedPlaceIDs {
			bridgeID := fmt.Sprintf("bepv:%s:%s", eventID, relatedPlaceID)
			eventPlaceByID[bridgeID] = EventPlaceLink{
				BridgeID:      bridgeID,
				EventID:       eventID,
				PlaceID:       relatedPlaceID,
				RelationType:  "related",
				LinkedAt:      raw.OccurredAt,
				SchemaVersion: defaultEventSchemaVersion,
				Attrs:         map[string]any{"source_id": raw.SourceID},
				Evidence:      append([]canonical.Evidence(nil), evidence...),
			}
		}
	}

	sort.Slice(events, func(i, j int) bool { return events[i].EventID < events[j].EventID })
	entities := sortEntities(entityByID)
	eventEntities := sortEventEntities(eventEntityByID)
	eventPlaces := sortEventPlaces(eventPlaceByID)
	entityPlaces := sortEntityPlaces(entityPlaceByID)
	return events, entities, eventEntities, eventPlaces, entityPlaces
}

func buildMetricRegistry(now time.Time) []metrics.RegistryRecord {
	types := []struct {
		metricID    string
		description string
		formula     string
	}{
		{metricID: "conflict_intensity_score", description: "Daily normalized conflict intensity from geopolitical fixtures.", formula: "sum(conflict_impact * 100) capped at 100"},
		{metricID: "cross_border_spillover_score", description: "Daily cross-border spillover signal from linked places in other admin0 regions.", formula: "sum(related_foreign_place_edges * 35) capped at 100"},
		{metricID: "media_attention_score", description: "Daily media attention score derived from media mentions and corroborating links.", formula: "sum(media_mentions * 4 + corroboration_links * 5) capped at 100"},
		{metricID: "protest_activity_score", description: "Daily protest activity score from protest-class geopolitical events.", formula: "sum(protest_events * 25) capped at 100"},
	}
	records := make([]metrics.RegistryRecord, 0, len(types))
	for idx, item := range types {
		records = append(records, metrics.RegistryRecord{
			MetricID:           item.metricID,
			MetricFamily:       "geopolitical",
			SubjectGrain:       "place",
			Unit:               "score",
			ValueType:          "gauge",
			RollupEngine:       "MergeTree",
			RollupRule:         "latest_daily_pack_score",
			Enabled:            true,
			UpdatedAt:          now,
			SchemaVersion:      metrics.SchemaVersion,
			RecordVersion:      uint64(now.UnixMilli()) + uint64(idx) + 1,
			APIContractVersion: metrics.APIContractVersion,
			Attrs: map[string]any{
				"description":     item.description,
				"formula":         item.formula,
				"refresh_cadence": "run_once",
				"pack":            "geopolitical",
				"window_grains":   []string{"day"},
			},
			Evidence: []canonical.Evidence{{Kind: "metric_spec", Ref: item.metricID, Value: item.formula, Attrs: map[string]any{"pack": "geopolitical"}}},
		})
	}
	sort.Slice(records, func(i, j int) bool { return records[i].MetricID < records[j].MetricID })
	return records
}

func buildMetricContributions(events []EventRecord) []metrics.Contribution {
	accumulators := map[string]*metricAccumulator{}
	for _, event := range events {
		windowStart := time.Date(event.StartsAt.Year(), event.StartsAt.Month(), event.StartsAt.Day(), 0, 0, 0, 0, time.UTC)
		windowEnd := windowStart.Add(24 * time.Hour)
		attrs := event.Attrs
		mediaMentions, _ := asFloat64(attrs["media_mentions"])
		crossLinks := 0.0
		if items, ok := attrs["cross_source_links"].([]map[string]any); ok {
			crossLinks = float64(len(items))
		}
		accumulate(accumulators, "media_attention_score", event.PlaceID, event.SourceID, event.EventID, windowStart, windowEnd, clamp(mediaMentions*4+crossLinks*5), event.Evidence, map[string]float64{"media_mentions": mediaMentions, "cross_links": crossLinks})
		switch event.EventType {
		case "conflict":
			accumulate(accumulators, "conflict_intensity_score", event.PlaceID, event.SourceID, event.EventID, windowStart, windowEnd, clamp(float64(event.ImpactScore)*100), event.Evidence, map[string]float64{"impact_score": float64(event.ImpactScore)})
		case "protest":
			accumulate(accumulators, "protest_activity_score", event.PlaceID, event.SourceID, event.EventID, windowStart, windowEnd, 25, event.Evidence, map[string]float64{"protest_events": 1})
		}
		if hasCrossBorder(attrs, event.PlaceID) {
			accumulate(accumulators, "cross_border_spillover_score", event.PlaceID, event.SourceID, event.EventID, windowStart, windowEnd, 35, event.Evidence, map[string]float64{"cross_border_edges": 1})
		}
	}

	rows := make([]metrics.Contribution, 0, len(accumulators))
	for _, acc := range accumulators {
		sources := make([]string, 0, len(acc.sourceIDs))
		for sourceID := range acc.sourceIDs {
			sources = append(sources, sourceID)
		}
		sort.Strings(sources)
		sort.Strings(acc.eventIDs)
		featureContributions := make([]map[string]any, 0, len(acc.featureValues))
		for key, value := range acc.featureValues {
			featureContributions = append(featureContributions, map[string]any{"feature": key, "value": value, "weight": 1.0})
		}
		sort.Slice(featureContributions, func(i, j int) bool {
			return featureContributions[i]["feature"].(string) < featureContributions[j]["feature"].(string)
		})
		rows = append(rows, metrics.Contribution{
			ContributionID:     fmt.Sprintf("mc:%s:%s:%d", acc.metricID, acc.placeID, acc.windowStart.Unix()),
			MetricID:           acc.metricID,
			SubjectGrain:       "place",
			SubjectID:          acc.placeID,
			SourceRecordType:   "event",
			SourceRecordID:     strings.Join(acc.eventIDs, ","),
			PlaceID:            acc.placeID,
			WindowGrain:        "day",
			WindowStart:        acc.windowStart,
			WindowEnd:          acc.windowEnd,
			ContributionType:   "pack_score",
			ContributionValue:  clamp(acc.value),
			ContributionWeight: 1,
			SchemaVersion:      metrics.SchemaVersion,
			Attrs: map[string]any{
				"source_ids": sources,
				"explainability": map[string]any{
					"event_ids":             acc.eventIDs,
					"feature_contributions": featureContributions,
					"evidence_refs":         evidenceRefs(acc.evidence),
					"source_count":          len(sources),
				},
			},
			Evidence: acc.evidence,
		})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].MetricID+rows[i].PlaceID < rows[j].MetricID+rows[j].PlaceID })
	return rows
}

func accumulate(target map[string]*metricAccumulator, metricID, placeID, sourceID, eventID string, start, end time.Time, value float64, evidence []canonical.Evidence, features map[string]float64) {
	key := metricID + "|" + placeID + "|" + start.Format(time.RFC3339)
	acc, ok := target[key]
	if !ok {
		acc = &metricAccumulator{metricID: metricID, placeID: placeID, windowStart: start, windowEnd: end, sourceIDs: map[string]struct{}{}, featureValues: map[string]float64{}}
		target[key] = acc
	}
	acc.value = clamp(acc.value + value)
	acc.sourceIDs[sourceID] = struct{}{}
	acc.eventIDs = append(acc.eventIDs, eventID)
	acc.evidence = mergeEvidence(acc.evidence, evidence)
	for key, featureValue := range features {
		acc.featureValues[key] += featureValue
	}
}

func hasCrossBorder(attrs map[string]any, primaryPlaceID string) bool {
	primary := lookupPlace(primaryPlaceID).admin0
	if primary == "" {
		return false
	}
	if related, ok := attrs["related_place_ids"].([]string); ok {
		for _, placeID := range related {
			if admin0 := lookupPlace(placeID).admin0; admin0 != "" && admin0 != primary {
				return true
			}
		}
	}
	return false
}

func rawCrossLinksToMaps(items []rawCrossLink) []map[string]any {
	out := make([]map[string]any, 0, len(items))
	for _, item := range items {
		out = append(out, map[string]any{"source_id": item.SourceID, "external_id": item.ExternalID, "url": item.URL, "relation": item.Relation})
	}
	return out
}

func lookupPlace(placeID string) placeMeta {
	meta, ok := placeIndex()[placeID]
	if !ok {
		return placeMeta{}
	}
	return meta
}

func placeIndex() map[string]placeMeta {
	return map[string]placeMeta{
		"plc:fr":           {admin0: "plc:fr", chain: []string{"plc:world", "plc:continent:eu"}},
		"plc:fr-idf":       {admin0: "plc:fr", chain: []string{"plc:world", "plc:continent:eu", "plc:fr"}},
		"plc:fr-idf-paris": {admin0: "plc:fr", chain: []string{"plc:world", "plc:continent:eu", "plc:fr", "plc:fr-idf"}},
		"plc:us":           {admin0: "plc:us", chain: []string{"plc:world", "plc:continent:na"}},
		"plc:us-tx":        {admin0: "plc:us", chain: []string{"plc:world", "plc:continent:na", "plc:us"}},
		"plc:us-tx-paris":  {admin0: "plc:us", chain: []string{"plc:world", "plc:continent:na", "plc:us", "plc:us-tx"}},
		"plc:nr":           {admin0: "plc:nr", chain: []string{"plc:world", "plc:continent:oc"}},
		"plc:nr-yaren":     {admin0: "plc:nr", chain: []string{"plc:world", "plc:continent:oc", "plc:nr"}},
		"plc:continent:eu": {chain: []string{"plc:world"}},
		"plc:continent:na": {chain: []string{"plc:world"}},
		"plc:continent:oc": {chain: []string{"plc:world"}},
		"plc:world":        {chain: []string{}},
	}
}

func buildMetricRegistryInsertSQL(rows []metrics.RegistryRecord) (string, error) {
	if len(rows) == 0 {
		return "", nil
	}
	values := make([]string, 0, len(rows))
	for _, row := range rows {
		attrs, err := marshalString(row.Attrs)
		if err != nil {
			return "", err
		}
		evidence, err := marshalString(row.Evidence)
		if err != nil {
			return "", err
		}
		values = append(values, fmt.Sprintf("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%d,%d,%d,%d,%s)",
			sqlString(row.MetricID),
			sqlString(row.MetricFamily),
			sqlString(row.SubjectGrain),
			sqlString(row.Unit),
			sqlString(row.ValueType),
			sqlString(row.RollupEngine),
			sqlString(row.RollupRule),
			sqlString(attrs),
			sqlString(evidence),
			row.SchemaVersion,
			row.RecordVersion,
			row.APIContractVersion,
			boolToUInt8(row.Enabled),
			sqlTime(row.UpdatedAt),
		))
	}
	return "INSERT INTO meta.metric_registry (metric_id, metric_family, subject_grain, unit, value_type, rollup_engine, rollup_rule, attrs, evidence, schema_version, record_version, api_contract_version, enabled, updated_at) VALUES " + strings.Join(values, ","), nil
}

func buildEntityInsertSQL(rows []EntityRecord) (string, error) {
	if len(rows) == 0 {
		return "", nil
	}
	values := make([]string, 0, len(rows))
	for _, row := range rows {
		attrs, err := marshalString(row.Attrs)
		if err != nil {
			return "", err
		}
		evidence, err := marshalString(row.Evidence)
		if err != nil {
			return "", err
		}
		values = append(values, fmt.Sprintf("(%s,%s,%s,%s,%s,%s,%s,%s,%s,NULL,%d,%d,%d,%s,%s,%s)",
			sqlString(row.EntityID),
			sqlString(row.EntityType),
			sqlString(row.CanonicalName),
			sqlString(row.Status),
			sqlString(row.RiskBand),
			nullableSQLString(row.PrimaryPlaceID),
			sqlString(row.SourceEntityKey),
			sqlString(row.SourceSystem),
			sqlTime(row.ValidFrom),
			metrics.SchemaVersion,
			row.RecordVersion,
			metrics.APIContractVersion,
			sqlTime(row.UpdatedAt),
			sqlString(attrs),
			sqlString(evidence),
		))
	}
	return "INSERT INTO silver.dim_entity (entity_id, entity_type, canonical_name, status, risk_band, primary_place_id, source_entity_key, source_system, valid_from, valid_to, schema_version, record_version, api_contract_version, updated_at, attrs, evidence) VALUES " + strings.Join(values, ","), nil
}

func buildEventInsertSQL(rows []EventRecord) (string, error) {
	if len(rows) == 0 {
		return "", nil
	}
	values := make([]string, 0, len(rows))
	for _, row := range rows {
		attrs, err := marshalString(row.Attrs)
		if err != nil {
			return "", err
		}
		evidence, err := marshalString(row.Evidence)
		if err != nil {
			return "", err
		}
		values = append(values, fmt.Sprintf("(%s,%s,%s,%s,%s,%s,%s,NULL,%s,%s,%s,%d,%s,%s)",
			sqlString(row.EventID),
			sqlString(row.SourceID),
			sqlString(row.EventType),
			sqlString(row.EventSubtype),
			sqlString(row.PlaceID),
			stringArray(row.ParentChain),
			sqlTime(row.StartsAt),
			sqlString(row.Status),
			sqlString(row.ConfidenceBand),
			formatFloat64(float64(row.ImpactScore)),
			defaultEventSchemaVersion,
			sqlString(attrs),
			sqlString(evidence),
		))
	}
	return "INSERT INTO silver.fact_event (event_id, source_id, event_type, event_subtype, place_id, parent_place_chain, starts_at, ends_at, status, confidence_band, impact_score, schema_version, attrs, evidence) VALUES " + strings.Join(values, ","), nil
}

func buildEventEntityInsertSQL(rows []EventEntityLink) (string, error) {
	if len(rows) == 0 {
		return "", nil
	}
	values := make([]string, 0, len(rows))
	for _, row := range rows {
		attrs, err := marshalString(row.Attrs)
		if err != nil {
			return "", err
		}
		evidence, err := marshalString(row.Evidence)
		if err != nil {
			return "", err
		}
		values = append(values, fmt.Sprintf("(%s,%s,%s,%s,%s,%s,%d,%s,%s)",
			sqlString(row.BridgeID),
			sqlString(row.EventID),
			sqlString(row.EntityID),
			sqlString(row.RoleType),
			sqlString(row.ConfidenceBand),
			sqlTime(row.LinkedAt),
			row.SchemaVersion,
			sqlString(attrs),
			sqlString(evidence),
		))
	}
	return "INSERT INTO silver.bridge_event_entity (bridge_id, event_id, entity_id, role_type, confidence_band, linked_at, schema_version, attrs, evidence) VALUES " + strings.Join(values, ","), nil
}

func buildEventPlaceInsertSQL(rows []EventPlaceLink) (string, error) {
	if len(rows) == 0 {
		return "", nil
	}
	values := make([]string, 0, len(rows))
	for _, row := range rows {
		attrs, err := marshalString(row.Attrs)
		if err != nil {
			return "", err
		}
		evidence, err := marshalString(row.Evidence)
		if err != nil {
			return "", err
		}
		values = append(values, fmt.Sprintf("(%s,%s,%s,%s,%s,%d,%s,%s)",
			sqlString(row.BridgeID),
			sqlString(row.EventID),
			sqlString(row.PlaceID),
			sqlString(row.RelationType),
			sqlTime(row.LinkedAt),
			row.SchemaVersion,
			sqlString(attrs),
			sqlString(evidence),
		))
	}
	return "INSERT INTO silver.bridge_event_place (bridge_id, event_id, place_id, relation_type, linked_at, schema_version, attrs, evidence) VALUES " + strings.Join(values, ","), nil
}

func buildEntityPlaceInsertSQL(rows []EntityPlaceLink) (string, error) {
	if len(rows) == 0 {
		return "", nil
	}
	values := make([]string, 0, len(rows))
	for _, row := range rows {
		attrs, err := marshalString(row.Attrs)
		if err != nil {
			return "", err
		}
		evidence, err := marshalString(row.Evidence)
		if err != nil {
			return "", err
		}
		values = append(values, fmt.Sprintf("(%s,%s,%s,%s,%s,%d,%s,%s)",
			sqlString(row.BridgeID),
			sqlString(row.EntityID),
			sqlString(row.PlaceID),
			sqlString(row.RelationType),
			sqlTime(row.LinkedAt),
			row.SchemaVersion,
			sqlString(attrs),
			sqlString(evidence),
		))
	}
	return "INSERT INTO silver.bridge_entity_place (bridge_id, entity_id, place_id, relation_type, linked_at, schema_version, attrs, evidence) VALUES " + strings.Join(values, ","), nil
}

func buildContributionInsertSQL(rows []metrics.Contribution) (string, error) {
	if len(rows) == 0 {
		return "", nil
	}
	values := make([]string, 0, len(rows))
	for _, row := range rows {
		attrs, err := marshalString(row.Attrs)
		if err != nil {
			return "", err
		}
		evidence, err := marshalString(row.Evidence)
		if err != nil {
			return "", err
		}
		values = append(values, fmt.Sprintf("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%d,%s,%s)",
			sqlString(row.ContributionID),
			sqlString(row.MetricID),
			sqlString(row.SubjectGrain),
			sqlString(row.SubjectID),
			sqlString(row.SourceRecordType),
			sqlString(row.SourceRecordID),
			sqlString(row.PlaceID),
			sqlString(row.WindowGrain),
			sqlTime(row.WindowStart),
			sqlTime(row.WindowEnd),
			sqlString(row.ContributionType),
			formatFloat64(row.ContributionValue),
			formatFloat64(row.ContributionWeight),
			row.SchemaVersion,
			sqlString(attrs),
			sqlString(evidence),
		))
	}
	return "INSERT INTO silver.metric_contribution (contribution_id, metric_id, subject_grain, subject_id, source_record_type, source_record_id, place_id, window_grain, window_start, window_end, contribution_type, contribution_value, contribution_weight, schema_version, attrs, evidence) VALUES " + strings.Join(values, ","), nil
}

func buildSnapshotInsertSQL(rows []metrics.SnapshotRow) (string, error) {
	if len(rows) == 0 {
		return "", nil
	}
	values := make([]string, 0, len(rows))
	for _, row := range rows {
		attrs, err := marshalString(row.Attrs)
		if err != nil {
			return "", err
		}
		evidence, err := marshalString(row.Evidence)
		if err != nil {
			return "", err
		}
		values = append(values, fmt.Sprintf("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%d,%d,%s,%s)",
			sqlString(row.SnapshotID),
			sqlString(row.MetricID),
			sqlString(row.SubjectGrain),
			sqlString(row.SubjectID),
			sqlString(row.PlaceID),
			sqlString(row.WindowGrain),
			sqlTime(row.WindowStart),
			sqlTime(row.WindowEnd),
			sqlTime(row.SnapshotAt),
			formatFloat64(row.MetricValue),
			formatFloat64(row.MetricDelta),
			row.Rank,
			row.SchemaVersion,
			sqlString(attrs),
			sqlString(evidence),
		))
	}
	return "INSERT INTO gold.metric_snapshot (snapshot_id, metric_id, subject_grain, subject_id, place_id, window_grain, window_start, window_end, snapshot_at, metric_value, metric_delta, rank, schema_version, attrs, evidence) VALUES " + strings.Join(values, ","), nil
}

func sortEntities(items map[string]EntityRecord) []EntityRecord {
	out := make([]EntityRecord, 0, len(items))
	for _, item := range items {
		out = append(out, item)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].EntityID < out[j].EntityID })
	return out
}

func sortEventEntities(items map[string]EventEntityLink) []EventEntityLink {
	out := make([]EventEntityLink, 0, len(items))
	for _, item := range items {
		out = append(out, item)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].BridgeID < out[j].BridgeID })
	return out
}

func sortEventPlaces(items map[string]EventPlaceLink) []EventPlaceLink {
	out := make([]EventPlaceLink, 0, len(items))
	for _, item := range items {
		out = append(out, item)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].BridgeID < out[j].BridgeID })
	return out
}

func sortEntityPlaces(items map[string]EntityPlaceLink) []EntityPlaceLink {
	out := make([]EntityPlaceLink, 0, len(items))
	for _, item := range items {
		out = append(out, item)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].BridgeID < out[j].BridgeID })
	return out
}

func eventID(sourceID, nativeID string) string {
	return "evt:geo:" + slug(sourceID) + ":" + slug(nativeID)
}

func entityID(entityType, name string) string {
	return "ent:geo:" + slug(entityType) + ":" + slug(name)
}

func slug(v string) string {
	v = strings.ToLower(strings.TrimSpace(v))
	replacer := strings.NewReplacer(":", "-", "/", "-", " ", "-", ".", "-", ",", "-", "_", "-", "#", "", "'", "")
	v = replacer.Replace(v)
	parts := strings.FieldsFunc(v, func(r rune) bool {
		return (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '-'
	})
	clean := strings.Join(parts, "-")
	for strings.Contains(clean, "--") {
		clean = strings.ReplaceAll(clean, "--", "-")
	}
	clean = strings.Trim(clean, "-")
	if clean == "" {
		return "unknown"
	}
	return clean
}

func confidenceBand(score float64) string {
	switch {
	case score >= 0.8:
		return "high"
	case score >= 0.5:
		return "medium"
	default:
		return "low"
	}
}

func riskBand(score float64) string {
	switch {
	case score >= 0.8:
		return "severe"
	case score >= 0.5:
		return "elevated"
	default:
		return "moderate"
	}
}

func clamp(v float64) float64 {
	if v < 0 {
		return 0
	}
	if v > 100 {
		return 100
	}
	return metricsValue(v)
}

func metricsValue(v float64) float64 {
	return float64(int(v*10000+0.5)) / 10000
}

func mergeEvidence(left, right []canonical.Evidence) []canonical.Evidence {
	if len(right) == 0 {
		return append([]canonical.Evidence(nil), left...)
	}
	seen := map[string]struct{}{}
	out := make([]canonical.Evidence, 0, len(left)+len(right))
	for _, item := range append(append([]canonical.Evidence(nil), left...), right...) {
		key := item.Kind + "|" + item.SourceID + "|" + item.Ref + "|" + item.RawID
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, item)
	}
	return out
}

func evidenceRefs(items []canonical.Evidence) []string {
	refs := make([]string, 0, len(items))
	for _, item := range items {
		ref := item.Ref
		if ref == "" {
			ref = item.RawID
		}
		if ref == "" {
			continue
		}
		refs = append(refs, ref)
	}
	sort.Strings(refs)
	return refs
}

func asFloat64(v any) (float64, bool) {
	switch value := v.(type) {
	case float64:
		return value, true
	case float32:
		return float64(value), true
	case int:
		return float64(value), true
	case int64:
		return float64(value), true
	default:
		return 0, false
	}
}

func marshalString(v any) (string, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(b), nil
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

func sqlTime(v time.Time) string {
	return sqlString(v.UTC().Format("2006-01-02 15:04:05.000"))
}

func stringArray(items []string) string {
	quoted := make([]string, 0, len(items))
	for _, item := range items {
		quoted = append(quoted, sqlString(item))
	}
	return "[" + strings.Join(quoted, ",") + "]"
}

func formatFloat64(v float64) string {
	formatted := fmt.Sprintf("%.4f", v)
	formatted = strings.TrimRight(formatted, "0")
	formatted = strings.TrimRight(formatted, ".")
	if formatted == "" || formatted == "-" {
		return "0"
	}
	return formatted
}

func boolToUInt8(v bool) uint8 {
	if v {
		return 1
	}
	return 0
}
