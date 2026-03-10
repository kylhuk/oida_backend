package safety

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
	SourceSafetyFixture = "fixture:safety"
	SourceOpenSanctions = "fixture:opensanctions"
	SourceNASAFIRMS     = "fixture:nasa-firms"
	SourceNOAAHazards   = "fixture:noaa-hazards"
	SourceKEV           = "fixture:kev"

	defaultSchemaVersion uint32 = 1
)

type Options struct {
	SourceID string
	Now      time.Time
}

type Plan struct {
	ExecutedSources []string
	Entities        []EntityRecord
	Observations    []ObservationRecord
	EntityPlaces    []EntityPlaceLink
	MetricRegistry  []metrics.RegistryRecord
	Contributions   []metrics.Contribution
	Snapshots       []metrics.SnapshotRow
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

type ObservationRecord struct {
	ObservationID    string
	SourceID         string
	SubjectType      string
	SubjectID        string
	ObservationType  string
	PlaceID          string
	ParentChain      []string
	ObservedAt       time.Time
	PublishedAt      *time.Time
	ConfidenceBand   string
	MeasurementUnit  string
	MeasurementValue float64
	Attrs            map[string]any
	Evidence         []canonical.Evidence
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

type rawRelation struct {
	TargetRef string
	Type      string
	Value     string
	URL       string
}

type rawEntity struct {
	SourceID       string
	Ref            string
	NativeID       string
	Name           string
	EntityType     string
	PrimaryPlaceID string
	OccurredAt     time.Time
	Status         string
	RiskScore      float64
	SourceURL      string
	Relations      []rawRelation
	Attrs          map[string]any
}

type rawObservation struct {
	SourceID         string
	NativeID         string
	SubjectType      string
	SubjectRef       string
	ObservationType  string
	PlaceID          string
	ObservedAt       time.Time
	PublishedAt      *time.Time
	MeasurementUnit  string
	MeasurementValue float64
	Confidence       float64
	SourceURL        string
	Attrs            map[string]any
}

type adapter struct {
	SourceID string
	Load     func(context.Context, Options) ([]rawEntity, []rawObservation, error)
}

type metricAccumulator struct {
	metricID       string
	placeID        string
	windowStart    time.Time
	windowEnd      time.Time
	value          float64
	observationIDs []string
	evidence       []canonical.Evidence
	sourceIDs      map[string]struct{}
	featureValues  map[string]float64
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
	loadAll := selected == "" || selected == SourceSafetyFixture
	rawEntities := []rawEntity{}
	rawObservations := []rawObservation{}
	executed := []string{}
	for _, item := range adapters() {
		if !loadAll && item.SourceID != selected {
			continue
		}
		entities, observations, err := item.Load(ctx, opts)
		if err != nil {
			return Plan{}, err
		}
		rawEntities = append(rawEntities, entities...)
		rawObservations = append(rawObservations, observations...)
		executed = append(executed, item.SourceID)
	}
	if !loadAll && len(executed) == 0 {
		return Plan{}, fmt.Errorf("unknown safety source %q", selected)
	}

	entities, observations, entityPlaces := normalizeRecords(rawEntities, rawObservations, now)
	registry := buildMetricRegistry(now)
	contributions := buildMetricContributions(observations)
	snapshots := metrics.BuildMetricSnapshots(metrics.BuildMetricState(contributions, now), now)

	return Plan{
		ExecutedSources: executed,
		Entities:        entities,
		Observations:    observations,
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
	if sql, err := buildObservationInsertSQL(p.Observations); err != nil {
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
		{SourceID: SourceOpenSanctions, Load: loadOpenSanctionsFixtures},
		{SourceID: SourceNASAFIRMS, Load: loadFIRMSFixtures},
		{SourceID: SourceNOAAHazards, Load: loadNOAAHazardFixtures},
		{SourceID: SourceKEV, Load: loadKEVFixtures},
	}
}

func loadOpenSanctionsFixtures(_ context.Context, _ Options) ([]rawEntity, []rawObservation, error) {
	publishedOne := time.Date(2026, 3, 10, 7, 0, 0, 0, time.UTC)
	publishedTwo := time.Date(2026, 3, 10, 7, 5, 0, 0, time.UTC)
	entities := []rawEntity{
		{
			SourceID:       SourceOpenSanctions,
			Ref:            "atlas-maritime-holdings",
			NativeID:       "opensanctions:atlas-maritime-holdings",
			Name:           "Atlas Maritime Holdings Ltd",
			EntityType:     "organization",
			PrimaryPlaceID: "plc:ae-du-dubai",
			OccurredAt:     time.Date(2026, 3, 10, 6, 45, 0, 0, time.UTC),
			Status:         "active",
			RiskScore:      0.92,
			SourceURL:      "https://www.opensanctions.org/entities/atlas-maritime-holdings/",
			Relations: []rawRelation{{
				TargetRef: "blue-gulf-bunkering",
				Type:      "beneficial_owner",
				Value:     "owns 78%",
				URL:       "https://www.opensanctions.org/entities/atlas-maritime-holdings/",
			}},
			Attrs: map[string]any{
				"opensanctions_id": "atlas-maritime-holdings",
				"topics":           []string{"sanction", "transport"},
				"programs":         []string{"EU-RUS-2026", "OFAC-SSI"},
			},
		},
		{
			SourceID:       SourceOpenSanctions,
			Ref:            "blue-gulf-bunkering",
			NativeID:       "opensanctions:blue-gulf-bunkering",
			Name:           "Blue Gulf Bunkering DMCC",
			EntityType:     "organization",
			PrimaryPlaceID: "plc:ae-du-dubai",
			OccurredAt:     time.Date(2026, 3, 10, 6, 50, 0, 0, time.UTC),
			Status:         "active",
			RiskScore:      0.71,
			SourceURL:      "https://www.opensanctions.org/entities/blue-gulf-bunkering/",
			Relations: []rawRelation{{
				TargetRef: "atlas-maritime-holdings",
				Type:      "owned_by",
				Value:     "majority-owned affiliate",
				URL:       "https://www.opensanctions.org/entities/blue-gulf-bunkering/",
			}},
			Attrs: map[string]any{
				"opensanctions_id": "blue-gulf-bunkering",
				"topics":           []string{"company", "marine-fuels"},
			},
		},
	}
	observations := []rawObservation{
		{
			SourceID:         SourceOpenSanctions,
			NativeID:         "opensanctions:atlas-maritime-holdings:sanctions",
			SubjectType:      "entity",
			SubjectRef:       "atlas-maritime-holdings",
			ObservationType:  "sanctions_match",
			PlaceID:          "plc:ae-du-dubai",
			ObservedAt:       time.Date(2026, 3, 10, 6, 45, 0, 0, time.UTC),
			PublishedAt:      &publishedOne,
			MeasurementUnit:  "score",
			MeasurementValue: 91,
			Confidence:       0.96,
			SourceURL:        "https://www.opensanctions.org/entities/atlas-maritime-holdings/",
			Attrs: map[string]any{
				"programs": []string{"EU-RUS-2026", "OFAC-SSI"},
				"entity_graph": []map[string]any{{
					"target_ref": "blue-gulf-bunkering",
					"relation":   "beneficial_owner",
					"weight":     0.78,
				}},
				"sector_mapping": map[string]any{"sector": "shipping", "confidence": 0.91, "status": "resolved"},
			},
		},
		{
			SourceID:         SourceOpenSanctions,
			NativeID:         "opensanctions:blue-gulf-bunkering:exposure",
			SubjectType:      "entity",
			SubjectRef:       "blue-gulf-bunkering",
			ObservationType:  "sanctions_match",
			PlaceID:          "plc:ae-du-dubai",
			ObservedAt:       time.Date(2026, 3, 10, 6, 50, 0, 0, time.UTC),
			PublishedAt:      &publishedTwo,
			MeasurementUnit:  "score",
			MeasurementValue: 64,
			Confidence:       0.82,
			SourceURL:        "https://www.opensanctions.org/entities/blue-gulf-bunkering/",
			Attrs: map[string]any{
				"exposure_basis":    "ownership_chain",
				"linked_entity_ref": "atlas-maritime-holdings",
				"sector_mapping":    map[string]any{"sector": "marine-fuels", "confidence": 0.76, "status": "resolved"},
			},
		},
	}
	return entities, observations, nil
}

func loadFIRMSFixtures(_ context.Context, _ Options) ([]rawEntity, []rawObservation, error) {
	publishedOne := time.Date(2026, 3, 10, 3, 30, 0, 0, time.UTC)
	publishedTwo := time.Date(2026, 3, 10, 4, 0, 0, 0, time.UTC)
	observations := []rawObservation{
		{
			SourceID:         SourceNASAFIRMS,
			NativeID:         "firms:tokyo-bay:20260310T0300Z",
			SubjectType:      "place",
			SubjectRef:       "plc:jp-13-tokyo",
			ObservationType:  "fire_hotspot",
			PlaceID:          "plc:jp-13-tokyo",
			ObservedAt:       time.Date(2026, 3, 10, 3, 0, 0, 0, time.UTC),
			PublishedAt:      &publishedOne,
			MeasurementUnit:  "hotspots",
			MeasurementValue: 7,
			Confidence:       0.84,
			SourceURL:        "https://www.earthdata.nasa.gov/learn/find-data/near-real-time/firms",
			Attrs: map[string]any{
				"satellite":        "VIIRS",
				"frp_mw":           19.4,
				"confidence_label": "nominal",
			},
		},
		{
			SourceID:         SourceNASAFIRMS,
			NativeID:         "firms:new-orleans:20260310T0330Z",
			SubjectType:      "place",
			SubjectRef:       "plc:us-la-new-orleans",
			ObservationType:  "fire_hotspot",
			PlaceID:          "plc:us-la-new-orleans",
			ObservedAt:       time.Date(2026, 3, 10, 3, 30, 0, 0, time.UTC),
			PublishedAt:      &publishedTwo,
			MeasurementUnit:  "hotspots",
			MeasurementValue: 14,
			Confidence:       0.91,
			SourceURL:        "https://www.earthdata.nasa.gov/learn/find-data/near-real-time/firms",
			Attrs: map[string]any{
				"satellite":        "MODIS",
				"frp_mw":           28.2,
				"confidence_label": "high",
			},
		},
	}
	return nil, observations, nil
}

func loadNOAAHazardFixtures(_ context.Context, _ Options) ([]rawEntity, []rawObservation, error) {
	published := time.Date(2026, 3, 10, 9, 20, 0, 0, time.UTC)
	observations := []rawObservation{{
		SourceID:         SourceNOAAHazards,
		NativeID:         "noaa:limassol-coastal-bulletin:20260310",
		SubjectType:      "place",
		SubjectRef:       "plc:cy-02-limassol",
		ObservationType:  "coastal_hazard_bulletin",
		PlaceID:          "plc:cy-02-limassol",
		ObservedAt:       time.Date(2026, 3, 10, 9, 0, 0, 0, time.UTC),
		PublishedAt:      &published,
		MeasurementUnit:  "severity_score",
		MeasurementValue: 58,
		Confidence:       0.73,
		SourceURL:        "https://www.weather.gov/",
		Attrs: map[string]any{
			"bulletin_type":    "coastal_flood_statement",
			"expected_impacts": []string{"dock flooding", "port access disruption"},
			"mapping_status":   "resolved",
		},
	}}
	return nil, observations, nil
}

func loadKEVFixtures(_ context.Context, _ Options) ([]rawEntity, []rawObservation, error) {
	publishedOne := time.Date(2026, 3, 10, 10, 15, 0, 0, time.UTC)
	publishedTwo := time.Date(2026, 3, 10, 10, 20, 0, 0, time.UTC)
	entities := []rawEntity{
		{
			SourceID:       SourceKEV,
			Ref:            "portstack-terminal-gateway",
			NativeID:       "kev:portstack-terminal-gateway",
			Name:           "PortStack Terminal Gateway",
			EntityType:     "software_product",
			PrimaryPlaceID: "plc:us-la-new-orleans",
			OccurredAt:     time.Date(2026, 3, 10, 10, 0, 0, 0, time.UTC),
			Status:         "active",
			RiskScore:      0.68,
			SourceURL:      "https://www.cisa.gov/known-exploited-vulnerabilities-catalog",
			Attrs: map[string]any{
				"vendor":  "PortStack",
				"product": "Terminal Gateway",
			},
		},
		{
			SourceID:       SourceKEV,
			Ref:            "harborcontrol-edge",
			NativeID:       "kev:harborcontrol-edge",
			Name:           "HarborControl Edge Appliance",
			EntityType:     "software_product",
			PrimaryPlaceID: "plc:us",
			OccurredAt:     time.Date(2026, 3, 10, 10, 5, 0, 0, time.UTC),
			Status:         "active",
			RiskScore:      0.57,
			SourceURL:      "https://www.cisa.gov/known-exploited-vulnerabilities-catalog",
			Attrs: map[string]any{
				"vendor":  "HarborControl",
				"product": "Edge Appliance",
			},
		},
	}
	observations := []rawObservation{
		{
			SourceID:         SourceKEV,
			NativeID:         "kev:CVE-2025-1001",
			SubjectType:      "entity",
			SubjectRef:       "portstack-terminal-gateway",
			ObservationType:  "known_exploited_vulnerability",
			PlaceID:          "plc:us-la-new-orleans",
			ObservedAt:       time.Date(2026, 3, 10, 10, 0, 0, 0, time.UTC),
			PublishedAt:      &publishedOne,
			MeasurementUnit:  "cvss",
			MeasurementValue: 9.8,
			Confidence:       0.89,
			SourceURL:        "https://www.cisa.gov/known-exploited-vulnerabilities-catalog",
			Attrs: map[string]any{
				"cve":                 "CVE-2025-1001",
				"ransomware_use":      true,
				"sector_mapping":      map[string]any{"sector": "port_operations", "confidence": 0.88, "status": "resolved"},
				"location_resolution": map[string]any{"status": "resolved", "confidence": 0.9},
			},
		},
		{
			SourceID:         SourceKEV,
			NativeID:         "kev:CVE-2025-1777",
			SubjectType:      "entity",
			SubjectRef:       "harborcontrol-edge",
			ObservationType:  "known_exploited_vulnerability",
			PlaceID:          "plc:us",
			ObservedAt:       time.Date(2026, 3, 10, 10, 5, 0, 0, time.UTC),
			PublishedAt:      &publishedTwo,
			MeasurementUnit:  "cvss",
			MeasurementValue: 8.1,
			Confidence:       0.52,
			SourceURL:        "https://www.cisa.gov/known-exploited-vulnerabilities-catalog",
			Attrs: map[string]any{
				"cve": "CVE-2025-1777",
				"sector_mapping": map[string]any{
					"sector":     "maritime_ics",
					"confidence": 0.34,
					"status":     "review",
				},
				"location_resolution": map[string]any{
					"status":            "coarse",
					"confidence":        0.41,
					"claimed_place_id":  "plc:us-la-new-orleans",
					"resolved_place_id": "plc:us",
				},
			},
		},
	}
	return entities, observations, nil
}

func normalizeRecords(rawEntities []rawEntity, rawObservations []rawObservation, now time.Time) ([]EntityRecord, []ObservationRecord, []EntityPlaceLink) {
	entityIDs := map[string]string{}
	for _, raw := range rawEntities {
		entityIDs[raw.Ref] = entityID(raw.EntityType, raw.Name)
	}

	entityByID := map[string]EntityRecord{}
	entityPlaceByID := map[string]EntityPlaceLink{}
	for _, raw := range rawEntities {
		entityID := entityIDs[raw.Ref]
		evidence := []canonical.Evidence{canonical.NewRawDocumentEvidence(raw.SourceID, raw.NativeID, raw.SourceURL)}
		relations := make([]map[string]any, 0, len(raw.Relations))
		for _, relation := range raw.Relations {
			targetID := entityIDs[relation.TargetRef]
			relations = append(relations, map[string]any{
				"entity_id": targetID,
				"relation":  relation.Type,
				"value":     relation.Value,
			})
			evidence = append(evidence, canonical.Evidence{Kind: "entity_graph_link", SourceID: raw.SourceID, Ref: targetID, URL: relation.URL, Value: relation.Type})
		}
		attrs := cloneMap(raw.Attrs)
		attrs["graph_relations"] = relations
		attrs["source_id"] = raw.SourceID
		attrs["source_entity_id"] = raw.NativeID
		entityByID[entityID] = EntityRecord{
			EntityID:        entityID,
			EntityType:      raw.EntityType,
			CanonicalName:   raw.Name,
			Status:          raw.Status,
			RiskBand:        riskBand(raw.RiskScore),
			PrimaryPlaceID:  raw.PrimaryPlaceID,
			SourceEntityKey: raw.NativeID,
			SourceSystem:    raw.SourceID,
			ValidFrom:       raw.OccurredAt,
			UpdatedAt:       now,
			RecordVersion:   uint64(now.UnixMilli()),
			Attrs:           attrs,
			Evidence:        evidence,
		}
		bridgeID := fmt.Sprintf("bep:%s:%s", entityID, raw.PrimaryPlaceID)
		entityPlaceByID[bridgeID] = EntityPlaceLink{
			BridgeID:      bridgeID,
			EntityID:      entityID,
			PlaceID:       raw.PrimaryPlaceID,
			RelationType:  "primary_presence",
			LinkedAt:      raw.OccurredAt,
			SchemaVersion: defaultSchemaVersion,
			Attrs:         map[string]any{"source_id": raw.SourceID},
			Evidence:      append([]canonical.Evidence(nil), evidence...),
		}
	}

	observations := make([]ObservationRecord, 0, len(rawObservations))
	for _, raw := range rawObservations {
		placeID := raw.PlaceID
		meta := lookupPlace(placeID)
		subjectID := raw.SubjectRef
		if raw.SubjectType == "entity" {
			subjectID = entityIDs[raw.SubjectRef]
		}
		attrs := cloneMap(raw.Attrs)
		attrs["source_id"] = raw.SourceID
		attrs["source_observation_id"] = raw.NativeID
		attrs["admin0_place_id"] = meta.admin0
		evidence := []canonical.Evidence{
			canonical.NewRawDocumentEvidence(raw.SourceID, raw.NativeID, raw.SourceURL),
		}
		if raw.SubjectType == "entity" {
			evidence = append(evidence, canonical.Evidence{Kind: "subject_entity", SourceID: raw.SourceID, Ref: subjectID})
		}
		observations = append(observations, ObservationRecord{
			ObservationID:    observationID(raw.SourceID, raw.NativeID),
			SourceID:         raw.SourceID,
			SubjectType:      raw.SubjectType,
			SubjectID:        subjectID,
			ObservationType:  raw.ObservationType,
			PlaceID:          placeID,
			ParentChain:      meta.chain,
			ObservedAt:       raw.ObservedAt,
			PublishedAt:      raw.PublishedAt,
			ConfidenceBand:   confidenceBand(raw.Confidence),
			MeasurementUnit:  raw.MeasurementUnit,
			MeasurementValue: metricValue(raw.MeasurementValue),
			Attrs:            attrs,
			Evidence:         evidence,
		})
	}

	entities := sortEntities(entityByID)
	entityPlaces := sortEntityPlaces(entityPlaceByID)
	sort.Slice(observations, func(i, j int) bool { return observations[i].ObservationID < observations[j].ObservationID })
	return entities, observations, entityPlaces
}

func buildMetricRegistry(now time.Time) []metrics.RegistryRecord {
	types := []struct {
		metricID    string
		description string
		formula     string
	}{
		{metricID: "fire_hotspot", description: "Daily hotspot pressure derived from NASA FIRMS detections near critical places.", formula: "sum(hotspot_count * confidence * 5) capped at 100"},
		{metricID: "sanctions_exposure", description: "Daily sanctions exposure score derived from OpenSanctions entity matches and ownership-chain evidence.", formula: "sum(exposure_score * confidence) capped at 100"},
	}
	records := make([]metrics.RegistryRecord, 0, len(types))
	for idx, item := range types {
		records = append(records, metrics.RegistryRecord{
			MetricID:           item.metricID,
			MetricFamily:       "safety_security",
			SubjectGrain:       "place",
			Unit:               "score",
			ValueType:          "gauge",
			RollupEngine:       "MergeTree",
			RollupRule:         "latest_daily_pack_score",
			Enabled:            true,
			UpdatedAt:          now,
			SchemaVersion:      metrics.SchemaVersion,
			RecordVersion:      uint64(idx + 2500),
			APIContractVersion: metrics.APIContractVersion,
			Attrs: map[string]any{
				"description":     item.description,
				"formula":         item.formula,
				"refresh_cadence": "run_once",
				"pack":            "safety",
				"window_grains":   []string{"day"},
			},
			Evidence: []canonical.Evidence{{Kind: "metric_spec", Ref: item.metricID, Value: item.formula, Attrs: map[string]any{"pack": "safety"}}},
		})
	}
	sort.Slice(records, func(i, j int) bool { return records[i].MetricID < records[j].MetricID })
	return records
}

func buildMetricContributions(observations []ObservationRecord) []metrics.Contribution {
	accumulators := map[string]*metricAccumulator{}
	for _, observation := range observations {
		windowStart := time.Date(observation.ObservedAt.Year(), observation.ObservedAt.Month(), observation.ObservedAt.Day(), 0, 0, 0, 0, time.UTC)
		windowEnd := windowStart.Add(24 * time.Hour)
		switch observation.ObservationType {
		case "sanctions_match":
			programCount := countStringSlice(observation.Attrs["programs"])
			confidence := confidenceWeight(observation.ConfidenceBand)
			accumulate(accumulators, "sanctions_exposure", observation.PlaceID, observation.SourceID, observation.ObservationID, windowStart, windowEnd, clamp(observation.MeasurementValue*confidence), observation.Evidence, map[string]float64{"sanctions_matches": 1, "program_count": float64(programCount)})
		case "fire_hotspot":
			frp, _ := asFloat64(observation.Attrs["frp_mw"])
			confidence := confidenceWeight(observation.ConfidenceBand)
			accumulate(accumulators, "fire_hotspot", observation.PlaceID, observation.SourceID, observation.ObservationID, windowStart, windowEnd, clamp(observation.MeasurementValue*confidence*5), observation.Evidence, map[string]float64{"hotspot_count": observation.MeasurementValue, "frp_mw": frp})
		}
	}

	rows := make([]metrics.Contribution, 0, len(accumulators))
	for _, acc := range accumulators {
		sources := make([]string, 0, len(acc.sourceIDs))
		for sourceID := range acc.sourceIDs {
			sources = append(sources, sourceID)
		}
		sort.Strings(sources)
		sort.Strings(acc.observationIDs)
		featureContributions := make([]map[string]any, 0, len(acc.featureValues))
		for key, value := range acc.featureValues {
			featureContributions = append(featureContributions, map[string]any{"feature": key, "value": metricValue(value), "weight": 1.0})
		}
		sort.Slice(featureContributions, func(i, j int) bool {
			return featureContributions[i]["feature"].(string) < featureContributions[j]["feature"].(string)
		})
		rows = append(rows, metrics.Contribution{
			ContributionID:     fmt.Sprintf("mc:%s:%s:%d", acc.metricID, acc.placeID, acc.windowStart.Unix()),
			MetricID:           acc.metricID,
			SubjectGrain:       "place",
			SubjectID:          acc.placeID,
			SourceRecordType:   "observation",
			SourceRecordID:     strings.Join(acc.observationIDs, ","),
			PlaceID:            acc.placeID,
			WindowGrain:        "day",
			WindowStart:        acc.windowStart,
			WindowEnd:          acc.windowEnd,
			ContributionType:   "pack_score",
			ContributionValue:  clamp(acc.value),
			ContributionWeight: 1,
			SchemaVersion:      metrics.SchemaVersion,
			Attrs: map[string]any{
				"source_id":  firstOrEmpty(sources),
				"source_ids": sources,
				"explainability": map[string]any{
					"observation_ids":       acc.observationIDs,
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

func accumulate(target map[string]*metricAccumulator, metricID, placeID, sourceID, observationID string, start, end time.Time, value float64, evidence []canonical.Evidence, features map[string]float64) {
	key := metricID + "|" + placeID + "|" + start.Format(time.RFC3339)
	acc, ok := target[key]
	if !ok {
		acc = &metricAccumulator{metricID: metricID, placeID: placeID, windowStart: start, windowEnd: end, sourceIDs: map[string]struct{}{}, featureValues: map[string]float64{}}
		target[key] = acc
	}
	acc.value = clamp(acc.value + value)
	acc.sourceIDs[sourceID] = struct{}{}
	acc.observationIDs = append(acc.observationIDs, observationID)
	acc.evidence = mergeEvidence(acc.evidence, evidence)
	for key, featureValue := range features {
		acc.featureValues[key] += featureValue
	}
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
		"plc:ae":                {admin0: "plc:ae", chain: []string{"plc:world", "plc:continent:as"}},
		"plc:ae-du":             {admin0: "plc:ae", chain: []string{"plc:world", "plc:continent:as", "plc:ae"}},
		"plc:ae-du-dubai":       {admin0: "plc:ae", chain: []string{"plc:world", "plc:continent:as", "plc:ae", "plc:ae-du"}},
		"plc:continent:as":      {chain: []string{"plc:world"}},
		"plc:continent:eu":      {chain: []string{"plc:world"}},
		"plc:continent:na":      {chain: []string{"plc:world"}},
		"plc:cy":                {admin0: "plc:cy", chain: []string{"plc:world", "plc:continent:eu"}},
		"plc:cy-02":             {admin0: "plc:cy", chain: []string{"plc:world", "plc:continent:eu", "plc:cy"}},
		"plc:cy-02-limassol":    {admin0: "plc:cy", chain: []string{"plc:world", "plc:continent:eu", "plc:cy", "plc:cy-02"}},
		"plc:jp":                {admin0: "plc:jp", chain: []string{"plc:world", "plc:continent:as"}},
		"plc:jp-13":             {admin0: "plc:jp", chain: []string{"plc:world", "plc:continent:as", "plc:jp"}},
		"plc:jp-13-tokyo":       {admin0: "plc:jp", chain: []string{"plc:world", "plc:continent:as", "plc:jp", "plc:jp-13"}},
		"plc:us":                {admin0: "plc:us", chain: []string{"plc:world", "plc:continent:na"}},
		"plc:us-la":             {admin0: "plc:us", chain: []string{"plc:world", "plc:continent:na", "plc:us"}},
		"plc:us-la-new-orleans": {admin0: "plc:us", chain: []string{"plc:world", "plc:continent:na", "plc:us", "plc:us-la"}},
		"plc:world":             {chain: []string{}},
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

func buildObservationInsertSQL(rows []ObservationRecord) (string, error) {
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
		publishedAt := "NULL"
		if row.PublishedAt != nil {
			publishedAt = sqlTime(*row.PublishedAt)
		}
		values = append(values, fmt.Sprintf("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%d,%s,%s)",
			sqlString(row.ObservationID),
			sqlString(row.SourceID),
			sqlString(row.SubjectType),
			sqlString(row.SubjectID),
			sqlString(row.ObservationType),
			sqlString(row.PlaceID),
			stringArray(row.ParentChain),
			sqlTime(row.ObservedAt),
			publishedAt,
			sqlString(row.ConfidenceBand),
			sqlString(row.MeasurementUnit),
			formatFloat64(row.MeasurementValue),
			defaultSchemaVersion,
			sqlString(attrs),
			sqlString(evidence),
		))
	}
	return "INSERT INTO silver.fact_observation (observation_id, source_id, subject_type, subject_id, observation_type, place_id, parent_place_chain, observed_at, published_at, confidence_band, measurement_unit, measurement_value, schema_version, attrs, evidence) VALUES " + strings.Join(values, ","), nil
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

func sortEntityPlaces(items map[string]EntityPlaceLink) []EntityPlaceLink {
	out := make([]EntityPlaceLink, 0, len(items))
	for _, item := range items {
		out = append(out, item)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].BridgeID < out[j].BridgeID })
	return out
}

func observationID(sourceID, nativeID string) string {
	return "obs:safety:" + slug(sourceID) + ":" + slug(nativeID)
}

func entityID(entityType, name string) string {
	return "ent:safety:" + slug(entityType) + ":" + slug(name)
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
	case score >= 0.85:
		return "high"
	case score >= 0.6:
		return "medium"
	default:
		return "low"
	}
}

func confidenceWeight(band string) float64 {
	switch band {
	case "high":
		return 1
	case "medium":
		return 0.75
	default:
		return 0.5
	}
}

func riskBand(score float64) string {
	switch {
	case score >= 0.8:
		return "severe"
	case score >= 0.6:
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
	return metricValue(v)
}

func metricValue(v float64) float64 {
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

func countStringSlice(v any) int {
	switch value := v.(type) {
	case []string:
		return len(value)
	case []any:
		return len(value)
	default:
		return 0
	}
}

func firstOrEmpty(items []string) string {
	if len(items) == 0 {
		return ""
	}
	return items[0]
}

func cloneMap(input map[string]any) map[string]any {
	out := make(map[string]any, len(input)+4)
	for key, value := range input {
		out[key] = value
	}
	return out
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
