package maritime

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"global-osint-backend/internal/canonical"
)

type Vessel struct {
	SourceID       string
	NativeID       string
	RecordVersion  uint64
	Name           string
	Aliases        []string
	IMO            string
	MMSI           string
	CallSign       string
	VesselType     string
	FlagState      string
	Status         string
	RiskBand       string
	PrimaryPlaceID string
	OwnerName      string
	OperatorName   string
	BuildYear      int
	DeadweightTons int
	GrossTonnage   int
	ValidFrom      time.Time
	ValidTo        time.Time
	Evidence       []canonical.Evidence
}

type VesselTrackPoint struct {
	SourceID       string
	NativeID       string
	RecordVersion  uint64
	TrackID        string
	EntityID       string
	PlaceID        string
	ObservedAt     time.Time
	Latitude       float64
	Longitude      float64
	SpeedKPH       *float32
	CourseDeg      *float32
	Status         string
	ParentPlaceIDs []string
	Evidence       []canonical.Evidence
}

type VesselTrackSegment struct {
	SourceID       string
	NativeID       string
	RecordVersion  uint64
	TrackID        string
	EntityID       string
	PlaceID        string
	FromPlaceID    string
	ToPlaceID      string
	StartedAt      time.Time
	EndedAt        time.Time
	DistanceKM     float64
	PointCount     uint32
	AvgSpeedKPH    *float32
	ParentPlaceIDs []string
	Evidence       []canonical.Evidence
}

type PortCall struct {
	SourceID         string
	NativeID         string
	RecordVersion    uint64
	EntityID         string
	PlaceID          string
	PortName         string
	Terminal         string
	Berth            string
	CallType         string
	Status           string
	StartedAt        time.Time
	EndedAt          time.Time
	NextPlaceID      string
	ParentPlaceChain []string
	Evidence         []canonical.Evidence
}

type AISGap struct {
	SourceID         string
	NativeID         string
	RecordVersion    uint64
	TrackID          string
	EntityID         string
	PlaceID          string
	StartsAt         time.Time
	EndsAt           time.Time
	Reason           string
	LastKnownPortID  string
	NextKnownPortID  string
	ParentPlaceChain []string
	Evidence         []canonical.Evidence
}

func (v Vessel) EntityEnvelope() canonical.EntityEnvelope {
	nativeID := firstNonEmpty(v.NativeID, prefixedID("imo", v.IMO), prefixedID("mmsi", v.MMSI), prefixedID("callsign", v.CallSign), strings.TrimSpace(v.Name))
	aliases := append([]string(nil), v.Aliases...)
	aliases = append(aliases, v.CallSign)
	aliases = uniqueStrings(aliases...)
	data := map[string]any{
		"entity_type":   "vessel",
		"imo":           strings.TrimSpace(v.IMO),
		"mmsi":          strings.TrimSpace(v.MMSI),
		"call_sign":     strings.TrimSpace(v.CallSign),
		"vessel_type":   strings.TrimSpace(v.VesselType),
		"flag_state":    strings.TrimSpace(v.FlagState),
		"registry_name": strings.TrimSpace(v.Name),
	}
	attrs := map[string]any{
		"domain_family":      domainFamily,
		"pack":               domainFamily,
		"home_port_place_id": strings.TrimSpace(v.PrimaryPlaceID),
	}
	payload := map[string]any{
		"identifiers": map[string]any{
			"imo":       strings.TrimSpace(v.IMO),
			"mmsi":      strings.TrimSpace(v.MMSI),
			"call_sign": strings.TrimSpace(v.CallSign),
		},
		"flag_state": strings.TrimSpace(v.FlagState),
		"ownership": map[string]any{
			"owner_name":    strings.TrimSpace(v.OwnerName),
			"operator_name": strings.TrimSpace(v.OperatorName),
		},
		"capacity": map[string]any{
			"deadweight_tons": v.DeadweightTons,
			"gross_tonnage":   v.GrossTonnage,
		},
		"build_year": v.BuildYear,
	}
	evidence := mergeEvidence(v.Evidence, []canonical.Evidence{{
		Kind:  "registry_identifier",
		Ref:   firstNonEmpty(v.IMO, v.MMSI, v.CallSign, nativeID),
		Value: strings.TrimSpace(v.Name),
		Attrs: map[string]any{"entity_type": "vessel"},
	}})

	entity := canonical.EntityEnvelope{
		RecordEnvelope: canonical.NewRecordEnvelope("entity", data, canonical.EnvelopeOptions{
			SourceID:      firstNonEmpty(v.SourceID, "maritime:registry:vessel"),
			NativeID:      nativeID,
			RecordVersion: v.RecordVersion,
			Attrs:         attrs,
			Evidence:      evidence,
		}),
		EntityType:     "vessel",
		CanonicalName:  firstNonEmpty(v.Name, v.IMO, v.MMSI, nativeID),
		Status:         firstNonEmpty(v.Status, "active"),
		RiskBand:       firstNonEmpty(v.RiskBand, "watch"),
		PrimaryPlaceID: strings.TrimSpace(v.PrimaryPlaceID),
		Aliases:        aliases,
		Payload:        payload,
	}
	entity.ValidFrom = timePtr(v.ValidFrom)
	entity.ValidTo = timePtr(v.ValidTo)
	return entity
}

func (p VesselTrackPoint) Envelope() canonical.TrackEnvelope {
	nativeID := firstNonEmpty(p.NativeID, fmt.Sprintf("%s:%s", firstNonEmpty(p.TrackID, "track"), p.ObservedAt.UTC().Format("20060102150405")))
	data := map[string]any{
		"sensor":            "ais",
		"track_grain":       "point",
		"navigation_status": strings.TrimSpace(p.Status),
	}
	evidence := mergeEvidence(p.Evidence, []canonical.Evidence{{
		Kind:  "track_source",
		Ref:   firstNonEmpty(p.TrackID, nativeID),
		Value: "ais",
		Attrs: map[string]any{"track_grain": "point"},
	}})
	track := canonical.TrackEnvelope{
		RecordEnvelope: canonical.NewRecordEnvelope("track", data, canonical.EnvelopeOptions{
			SourceID:      firstNonEmpty(p.SourceID, "maritime:ais:community"),
			NativeID:      nativeID,
			RecordVersion: p.RecordVersion,
			Attrs: map[string]any{
				"domain_family":    domainFamily,
				"pack":             domainFamily,
				"parent_place_ids": copyStringSlice(p.ParentPlaceIDs),
			},
			Evidence: evidence,
		}),
		TrackID:    firstNonEmpty(p.TrackID, nativeID),
		TrackType:  "maritime_position",
		EntityID:   strings.TrimSpace(p.EntityID),
		PlaceID:    strings.TrimSpace(p.PlaceID),
		ObservedAt: timePtr(p.ObservedAt),
		Latitude:   float64Ptr(p.Latitude),
		Longitude:  float64Ptr(p.Longitude),
		SpeedKPH:   cloneFloat32Ptr(p.SpeedKPH),
		CourseDeg:  cloneFloat32Ptr(p.CourseDeg),
		Payload: map[string]any{
			"navigation_status": strings.TrimSpace(p.Status),
			"position_source":   "ais",
		},
	}
	return track
}

func (s VesselTrackSegment) Envelope() canonical.TrackEnvelope {
	nativeID := firstNonEmpty(s.NativeID, fmt.Sprintf("%s:%s", firstNonEmpty(s.TrackID, "track"), s.StartedAt.UTC().Format("20060102150405")))
	evidence := mergeEvidence(s.Evidence, []canonical.Evidence{{
		Kind:  "track_source",
		Ref:   firstNonEmpty(s.TrackID, nativeID),
		Value: "ais_segment",
		Attrs: map[string]any{"track_grain": "segment"},
	}})
	track := canonical.TrackEnvelope{
		RecordEnvelope: canonical.NewRecordEnvelope("track", map[string]any{
			"sensor":      "ais",
			"track_grain": "segment",
		}, canonical.EnvelopeOptions{
			SourceID:      firstNonEmpty(s.SourceID, "maritime:ais:community"),
			NativeID:      nativeID,
			RecordVersion: s.RecordVersion,
			Attrs: map[string]any{
				"domain_family":    domainFamily,
				"pack":             domainFamily,
				"parent_place_ids": copyStringSlice(s.ParentPlaceIDs),
			},
			Evidence: evidence,
		}),
		TrackID:     firstNonEmpty(s.TrackID, nativeID),
		TrackType:   "maritime_segment",
		EntityID:    strings.TrimSpace(s.EntityID),
		PlaceID:     firstNonEmpty(s.PlaceID, s.ToPlaceID, s.FromPlaceID),
		FromPlaceID: strings.TrimSpace(s.FromPlaceID),
		ToPlaceID:   strings.TrimSpace(s.ToPlaceID),
		StartedAt:   timePtr(s.StartedAt),
		EndedAt:     timePtr(s.EndedAt),
		DistanceKM:  float64Ptr(s.DistanceKM),
		PointCount:  uint32Ptr(s.PointCount),
		SpeedKPH:    cloneFloat32Ptr(s.AvgSpeedKPH),
		Payload: map[string]any{
			"position_source": "ais",
			"segment_hours":   roundMetric(durationHours(s.StartedAt, s.EndedAt)),
		},
	}
	return track
}

func (c PortCall) EventEnvelope() canonical.EventEnvelope {
	nativeID := firstNonEmpty(c.NativeID, fmt.Sprintf("port-call:%s:%s", firstNonEmpty(c.EntityID, "entity"), c.StartedAt.UTC().Format("20060102150405")))
	evidence := mergeEvidence(c.Evidence, []canonical.Evidence{{
		Kind:  "port_reference",
		Ref:   firstNonEmpty(c.PlaceID, c.PortName),
		Value: firstNonEmpty(c.PortName, c.PlaceID),
		Attrs: map[string]any{"call_type": firstNonEmpty(c.CallType, "turnaround")},
	}})
	event := canonical.EventEnvelope{
		RecordEnvelope: canonical.NewRecordEnvelope("event", map[string]any{
			"event_type": "port_call",
			"entity_id":  strings.TrimSpace(c.EntityID),
			"port_name":  strings.TrimSpace(c.PortName),
		}, canonical.EnvelopeOptions{
			SourceID:      firstNonEmpty(c.SourceID, "maritime:port:unlocode"),
			NativeID:      nativeID,
			RecordVersion: c.RecordVersion,
			Attrs: map[string]any{
				"domain_family": domainFamily,
				"pack":          domainFamily,
			},
			Evidence: evidence,
		}),
		EventType:        "port_call",
		EventSubtype:     firstNonEmpty(c.CallType, "turnaround"),
		PlaceID:          strings.TrimSpace(c.PlaceID),
		ParentPlaceChain: copyStringSlice(c.ParentPlaceChain),
		StartsAt:         c.StartedAt.UTC().Truncate(time.Millisecond),
		EndsAt:           timePtr(c.EndedAt),
		Status:           firstNonEmpty(c.Status, defaultEventStatus(c.EndedAt)),
		ConfidenceBand:   confidenceBand(len(evidence)),
		Payload: map[string]any{
			"entity_id":     strings.TrimSpace(c.EntityID),
			"terminal":      strings.TrimSpace(c.Terminal),
			"berth":         strings.TrimSpace(c.Berth),
			"next_place_id": strings.TrimSpace(c.NextPlaceID),
			"dwell_hours":   roundMetric(durationHours(c.StartedAt, c.EndedAt)),
		},
	}
	return event
}

func (g AISGap) EventEnvelope() canonical.EventEnvelope {
	nativeID := firstNonEmpty(g.NativeID, fmt.Sprintf("ais-gap:%s:%s", firstNonEmpty(g.TrackID, g.EntityID, "track"), g.StartsAt.UTC().Format("20060102150405")))
	duration := g.DurationHours()
	evidence := mergeEvidence(g.Evidence, []canonical.Evidence{{
		Kind:  "ais_gap",
		Ref:   nativeID,
		Value: fmt.Sprintf("%.4f", duration),
		Attrs: map[string]any{"reason": firstNonEmpty(g.Reason, "signal_loss")},
	}})
	event := canonical.EventEnvelope{
		RecordEnvelope: canonical.NewRecordEnvelope("event", map[string]any{
			"event_type": "ais_gap",
			"track_id":   strings.TrimSpace(g.TrackID),
			"entity_id":  strings.TrimSpace(g.EntityID),
		}, canonical.EnvelopeOptions{
			SourceID:      firstNonEmpty(g.SourceID, "maritime:ais:community"),
			NativeID:      nativeID,
			RecordVersion: g.RecordVersion,
			Attrs: map[string]any{
				"domain_family": domainFamily,
				"pack":          domainFamily,
			},
			Evidence: evidence,
		}),
		EventType:        "ais_gap",
		EventSubtype:     firstNonEmpty(g.Reason, "signal_loss"),
		PlaceID:          strings.TrimSpace(g.PlaceID),
		ParentPlaceChain: copyStringSlice(g.ParentPlaceChain),
		StartsAt:         g.StartsAt.UTC().Truncate(time.Millisecond),
		EndsAt:           timePtr(g.EndsAt),
		Status:           defaultEventStatus(g.EndsAt),
		ConfidenceBand:   confidenceBand(len(evidence)),
		Payload: map[string]any{
			"track_id":           strings.TrimSpace(g.TrackID),
			"entity_id":          strings.TrimSpace(g.EntityID),
			"duration_hours":     duration,
			"last_known_port_id": strings.TrimSpace(g.LastKnownPortID),
			"next_known_port_id": strings.TrimSpace(g.NextKnownPortID),
		},
	}
	return event
}

func (g AISGap) DurationHours() float64 {
	return roundMetric(durationHours(g.StartsAt, g.EndsAt))
}

func prefixedID(prefix, value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	return prefix + ":" + value
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			return value
		}
	}
	return ""
}

func uniqueStrings(values ...string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func mergeEvidence(slices ...[]canonical.Evidence) []canonical.Evidence {
	seen := map[string]struct{}{}
	var merged []canonical.Evidence
	for _, slice := range slices {
		for _, item := range slice {
			key := item.Kind + "|" + item.Ref + "|" + item.RawID + "|" + item.Selector
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			merged = append(merged, item)
		}
	}
	return merged
}

func evidenceRefs(evidence []canonical.Evidence) []string {
	seen := map[string]struct{}{}
	refs := make([]string, 0, len(evidence))
	for _, item := range evidence {
		ref := firstNonEmpty(item.Ref, item.RawID, item.URL)
		if ref == "" {
			continue
		}
		if _, ok := seen[ref]; ok {
			continue
		}
		seen[ref] = struct{}{}
		refs = append(refs, ref)
	}
	return refs
}

func defaultEventStatus(endedAt time.Time) string {
	if endedAt.IsZero() {
		return "open"
	}
	return "completed"
}

func confidenceBand(evidenceCount int) string {
	if evidenceCount >= 3 {
		return "high"
	}
	if evidenceCount >= 2 {
		return "medium"
	}
	return "low"
}

func durationHours(start, end time.Time) float64 {
	if start.IsZero() || end.IsZero() {
		return 0
	}
	if !end.After(start) {
		return 0
	}
	return end.Sub(start).Hours()
}

func timePtr(ts time.Time) *time.Time {
	if ts.IsZero() {
		return nil
	}
	ts = ts.UTC().Truncate(time.Millisecond)
	return &ts
}

func float64Ptr(v float64) *float64 {
	return &v
}

func uint32Ptr(v uint32) *uint32 {
	return &v
}

func cloneFloat32Ptr(v *float32) *float32 {
	if v == nil {
		return nil
	}
	copyValue := *v
	return &copyValue
}

func copyStringSlice(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	return append([]string(nil), values...)
}

func cloneAnyMap(input map[string]any) map[string]any {
	if len(input) == 0 {
		return nil
	}
	out := make(map[string]any, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}
