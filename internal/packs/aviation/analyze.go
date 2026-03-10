package aviation

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"global-osint-backend/internal/canonical"
)

const (
	defaultGapThreshold        = 20 * time.Minute
	defaultGroundStopThreshold = 45 * time.Minute
	defaultAirportRadiusKM     = 25.0
	airportLowAltitudeM        = 1500.0
	knotsToKPH                 = 3.6
	degreesToRadians           = math.Pi / 180
	radiansToDegrees           = 180 / math.Pi
)

type segmentWork struct {
	points []TrackPoint
	gaps   int
}

func Analyze(input InputBundle, options Options) (Bundle, error) {
	if strings.TrimSpace(input.SourceID) == "" {
		return Bundle{}, fmt.Errorf("source_id is required")
	}
	now := options.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC().Truncate(time.Millisecond) }
	}
	gapThreshold := options.GapThreshold
	if gapThreshold <= 0 {
		gapThreshold = defaultGapThreshold
	}
	groundStopThreshold := options.GroundStopThreshold
	if groundStopThreshold <= 0 {
		groundStopThreshold = defaultGroundStopThreshold
	}
	airportRadiusKM := options.AirportRadiusKM
	if airportRadiusKM <= 0 {
		airportRadiusKM = defaultAirportRadiusKM
	}

	registry := RegistryIndex(input.Registry)
	airports := airportIndex(input.Airports)
	grouped := map[string][]StateVector{}
	for _, state := range input.StateVectors {
		if normalizeICAO24(state.ICAO24) == "" || state.ObservedAt().IsZero() {
			continue
		}
		grouped[normalizeICAO24(state.ICAO24)] = append(grouped[normalizeICAO24(state.ICAO24)], state)
	}

	bundle := Bundle{}
	for _, icao24 := range sortedKeys(grouped) {
		states := grouped[icao24]
		sort.Slice(states, func(i, j int) bool {
			return states[i].ObservedAt().Before(states[j].ObservedAt())
		})
		entityID := canonical.NewIdentity(canonical.IDOptions{
			Namespace: "entity",
			SourceID:  input.SourceID,
			NativeID:  "aircraft-" + icao24,
		}).ID
		trackID := canonical.NewIdentity(canonical.IDOptions{
			Namespace: "track",
			SourceID:  input.SourceID,
			NativeID:  TrackType + "-" + icao24,
		}).ID

		points := buildTrackPoints(input.SourceID, entityID, trackID, states, input.Airports, airportRadiusKM)
		gaps := buildGapEvents(input.SourceID, entityID, trackID, states, input.Airports, airportRadiusKM, gapThreshold)
		segments := buildSegments(input.SourceID, entityID, trackID, points, gapThreshold, groundStopThreshold, airports, airportRadiusKM)
		interactions := buildAirportInteractions(input.SourceID, entityID, trackID, segments, airports)

		callsign := latestCallsign(states)
		registryRecord := registry[icao24]
		routeScore, routeEvidence, routeAttrs := routeIrregularityMetric(entityID, segments, gaps)
		militaryScore, militaryStatus, militaryEvidence, militaryAttrs := militaryLikelihoodMetric(callsign, registryRecord, states, segments, airports)
		primaryPlaceID := choosePrimaryPlaceID(segments)
		entityEvidence := mergeEvidence(mergeEvidence(copyEvidence(registryRecord.Evidence), militaryEvidence), routeEvidence)
		entity := AircraftEntity{
			EntityID:           entityID,
			ICAO24:             icao24,
			Callsign:           callsign,
			Registration:       registryRecord.Registration,
			Manufacturer:       registryRecord.Manufacturer,
			Model:              registryRecord.Model,
			RegistrantName:     registryRecord.RegistrantName,
			RegistrantType:     registryRecord.RegistrantType,
			CountryCode:        nonEmpty(registryRecord.CountryCode, latestCountry(states)),
			SourceSystem:       "opensky_public",
			PrimaryPlaceID:     primaryPlaceID,
			ObservedFrom:       states[0].ObservedAt(),
			ObservedUntil:      states[len(states)-1].ObservedAt(),
			MilitaryLikelihood: militaryScore,
			MilitaryStatus:     militaryStatus,
			RouteIrregularity:  routeScore,
			RiskBand:           riskBand(maxFloat(militaryScore, routeScore)),
			Attrs: map[string]any{
				"registration":              registryRecord.Registration,
				"military_likelihood_score": militaryScore,
				"military_status":           militaryStatus,
				"route_irregularity_score":  routeScore,
				"metric_explainability": map[string]any{
					MetricMilitaryLikelihood: militaryAttrs,
					MetricRouteIrregularity:  routeAttrs,
				},
			},
			Evidence: entityEvidence,
		}

		windowStart, windowEnd := dayWindow(entity.ObservedUntil)
		metrics := []MetricSnapshot{
			newMetricSnapshot(MetricMilitaryLikelihood, entity, militaryScore, now(), militaryAttrs, militaryEvidence, windowStart, windowEnd),
			newMetricSnapshot(MetricRouteIrregularity, entity, routeScore, now(), routeAttrs, routeEvidence, windowStart, windowEnd),
		}

		bundle.Aircraft = append(bundle.Aircraft, entity)
		bundle.TrackPoints = append(bundle.TrackPoints, points...)
		bundle.GapEvents = append(bundle.GapEvents, gaps...)
		bundle.Segments = append(bundle.Segments, segments...)
		bundle.AirportInteractions = append(bundle.AirportInteractions, interactions...)
		bundle.Metrics = append(bundle.Metrics, metrics...)
	}

	sortAircraft(bundle.Aircraft)
	sortTrackPoints(bundle.TrackPoints)
	sortSegments(bundle.Segments)
	sortGapEvents(bundle.GapEvents)
	sortInteractions(bundle.AirportInteractions)
	sortMetrics(bundle.Metrics)
	assignMetricRanks(bundle.Metrics)
	bundle.Stats = Stats{
		AircraftEntities:    len(bundle.Aircraft),
		TrackPoints:         len(bundle.TrackPoints),
		FlightSegments:      len(bundle.Segments),
		TransponderGaps:     len(bundle.GapEvents),
		AirportInteractions: len(bundle.AirportInteractions),
		Metrics:             len(bundle.Metrics),
	}
	return bundle, nil
}

func buildTrackPoints(sourceID, entityID, trackID string, states []StateVector, airports []Airport, airportRadiusKM float64) []TrackPoint {
	out := make([]TrackPoint, 0, len(states))
	for idx, state := range states {
		if !state.HasPosition {
			continue
		}
		placeID := ""
		if airport, _, ok := nearestAirport(state.Latitude, state.Longitude, state, airports, airportRadiusKM); ok {
			placeID = airport.PlaceID
		}
		trackPointID := canonical.NewIdentity(canonical.IDOptions{
			Namespace: "track_point",
			SourceID:  sourceID,
			NativeID:  fmt.Sprintf("%s-%d", trackID, idx+1),
		}).ID
		attrs := map[string]any{
			"icao24":          state.ICAO24,
			"position_source": state.PositionSource,
			"category":        state.Category,
			"origin_country":  state.OriginCountry,
			"squawk":          state.Squawk,
			"adapter":         "opensky_public",
		}
		out = append(out, TrackPoint{
			TrackPointID: trackPointID,
			TrackID:      trackID,
			EntityID:     entityID,
			SourceID:     sourceID,
			PlaceID:      placeID,
			ObservedAt:   state.ObservedAt(),
			Latitude:     roundFloat(state.Latitude),
			Longitude:    roundFloat(state.Longitude),
			AltitudeM:    preferredAltitude(state),
			SpeedKPH:     speedKPH(state),
			CourseDeg:    state.TrueTrackDeg,
			OnGround:     state.OnGround,
			Callsign:     state.Callsign,
			Attrs:        attrs,
			Evidence:     copyEvidence(state.Evidence),
		})
	}
	return out
}

func buildGapEvents(sourceID, entityID, trackID string, states []StateVector, airports []Airport, airportRadiusKM float64, gapThreshold time.Duration) []GapEvent {
	out := []GapEvent{}
	for i := 1; i < len(states); i++ {
		previous := states[i-1]
		current := states[i]
		gap := current.ObservedAt().Sub(previous.ObservedAt())
		if gap <= gapThreshold {
			continue
		}
		placeID := ""
		if airport, _, ok := nearestAirport(current.Latitude, current.Longitude, current, airports, airportRadiusKM); ok {
			placeID = airport.PlaceID
		} else if airport, _, ok := nearestAirport(previous.Latitude, previous.Longitude, previous, airports, airportRadiusKM); ok {
			placeID = airport.PlaceID
		}
		out = append(out, GapEvent{
			EventID:   canonical.NewIdentity(canonical.IDOptions{Namespace: "event", SourceID: sourceID, NativeID: fmt.Sprintf("gap-%s-%d", trackID, i)}).ID,
			TrackID:   trackID,
			EntityID:  entityID,
			SourceID:  sourceID,
			PlaceID:   placeID,
			StartedAt: previous.ObservedAt(),
			EndedAt:   current.ObservedAt(),
			GapHours:  roundFloat(gap.Hours()),
			InFlight:  inFlight(previous) || inFlight(current),
			Attrs: map[string]any{
				"icao24":           previous.ICAO24,
				"callsign":         nonEmpty(current.Callsign, previous.Callsign),
				"gap_minutes":      roundFloat(gap.Minutes()),
				"from_observed_at": previous.ObservedAt().Format(time.RFC3339),
				"to_observed_at":   current.ObservedAt().Format(time.RFC3339),
			},
			Evidence: mergeEvidence(copyEvidence(previous.Evidence), current.Evidence),
		})
	}
	return out
}

func buildSegments(sourceID, entityID, trackID string, points []TrackPoint, gapThreshold, groundStopThreshold time.Duration, airports map[string]Airport, airportRadiusKM float64) []FlightSegment {
	if len(points) == 0 {
		return nil
	}
	works := []segmentWork{{points: []TrackPoint{points[0]}}}
	for i := 1; i < len(points); i++ {
		current := points[i]
		previous := points[i-1]
		gap := current.ObservedAt.Sub(previous.ObservedAt)
		split := gap > gapThreshold || (previous.OnGround && current.OnGround && gap > groundStopThreshold)
		if split {
			works = append(works, segmentWork{points: []TrackPoint{current}})
			if gap > gapThreshold && len(works) > 1 {
				works[len(works)-2].gaps++
			}
			continue
		}
		works[len(works)-1].points = append(works[len(works)-1].points, current)
	}
	out := []FlightSegment{}
	for idx, work := range works {
		if len(work.points) < 2 {
			continue
		}
		first := work.points[0]
		last := work.points[len(work.points)-1]
		distanceKM := trackDistance(work.points)
		hours := last.ObservedAt.Sub(first.ObservedAt).Hours()
		avgSpeed := 0.0
		if hours > 0 {
			avgSpeed = distanceKM / hours
		}
		fromAirport, fromDistance, _ := nearestAirportForPoint(first, airports, airportRadiusKM)
		toAirport, toDistance, _ := nearestAirportForPoint(last, airports, airportRadiusKM)
		score, attrs := segmentIrregularity(work.points, work.gaps)
		segment := FlightSegment{
			SegmentID:         canonical.NewIdentity(canonical.IDOptions{Namespace: "track_segment", SourceID: sourceID, NativeID: fmt.Sprintf("%s-%d", trackID, idx+1)}).ID,
			TrackID:           trackID,
			EntityID:          entityID,
			SourceID:          sourceID,
			StartedAt:         first.ObservedAt,
			EndedAt:           last.ObservedAt,
			PointCount:        len(work.points),
			DistanceKM:        roundFloat(distanceKM),
			AvgSpeedKPH:       roundFloat(avgSpeed),
			RouteIrregularity: score,
			GapCount:          work.gaps,
			Attrs:             attrs,
			Evidence:          mergeEvidence(copyEvidence(first.Evidence), last.Evidence),
		}
		if fromAirport.AirportID != "" {
			segment.FromAirportID = fromAirport.AirportID
			segment.FromPlaceID = fromAirport.PlaceID
			segment.Attrs["from_airport_distance_km"] = roundFloat(fromDistance)
		}
		if toAirport.AirportID != "" {
			segment.ToAirportID = toAirport.AirportID
			segment.ToPlaceID = toAirport.PlaceID
			segment.Attrs["to_airport_distance_km"] = roundFloat(toDistance)
		}
		out = append(out, segment)
	}
	return out
}

func buildAirportInteractions(sourceID, entityID, trackID string, segments []FlightSegment, airports map[string]Airport) []AirportInteractionEvent {
	out := []AirportInteractionEvent{}
	for idx, segment := range segments {
		if airport, ok := airports[segment.FromAirportID]; ok {
			out = append(out, AirportInteractionEvent{
				EventID:         canonical.NewIdentity(canonical.IDOptions{Namespace: "event", SourceID: sourceID, NativeID: fmt.Sprintf("airport-departure-%s-%d", trackID, idx+1)}).ID,
				TrackID:         trackID,
				EntityID:        entityID,
				SourceID:        sourceID,
				AirportID:       airport.AirportID,
				PlaceID:         airport.PlaceID,
				InteractionType: "departure",
				ObservedAt:      segment.StartedAt,
				Attrs: map[string]any{
					"airport_use": airport.Use,
				},
				Evidence: append(copyEvidence(segment.Evidence), canonical.Evidence{Kind: "airport_match", Ref: airport.AirportID, Value: airport.Use}),
			})
		}
		if airport, ok := airports[segment.ToAirportID]; ok {
			out = append(out, AirportInteractionEvent{
				EventID:         canonical.NewIdentity(canonical.IDOptions{Namespace: "event", SourceID: sourceID, NativeID: fmt.Sprintf("airport-arrival-%s-%d", trackID, idx+1)}).ID,
				TrackID:         trackID,
				EntityID:        entityID,
				SourceID:        sourceID,
				AirportID:       airport.AirportID,
				PlaceID:         airport.PlaceID,
				InteractionType: "arrival",
				ObservedAt:      segment.EndedAt,
				Attrs: map[string]any{
					"airport_use": airport.Use,
				},
				Evidence: append(copyEvidence(segment.Evidence), canonical.Evidence{Kind: "airport_match", Ref: airport.AirportID, Value: airport.Use}),
			})
		}
	}
	return out
}

func routeIrregularityMetric(entityID string, segments []FlightSegment, gaps []GapEvent) (float64, []canonical.Evidence, map[string]any) {
	if len(segments) == 0 {
		return 0, nil, map[string]any{"segments": 0, "gap_hours": 0.0}
	}
	totalWeight := 0.0
	weighted := 0.0
	segmentEvidence := []canonical.Evidence{}
	detourRatios := make([]float64, 0, len(segments))
	for _, segment := range segments {
		weight := math.Max(float64(segment.PointCount-1), 1)
		totalWeight += weight
		weighted += segment.RouteIrregularity * weight
		segmentEvidence = mergeEvidence(segmentEvidence, segment.Evidence)
		if value, ok := floatFromAny(segment.Attrs["detour_ratio"]); ok {
			detourRatios = append(detourRatios, roundFloat(value))
		}
	}
	gapHours := 0.0
	for _, gap := range gaps {
		gapHours += gap.GapHours
		segmentEvidence = mergeEvidence(segmentEvidence, gap.Evidence)
	}
	base := 0.0
	if totalWeight > 0 {
		base = weighted / totalWeight
	}
	gapPenalty := clamp((gapHours/2.5)+(float64(len(gaps))*0.1), 0, 0.35)
	score := clamp(base+gapPenalty, 0, 1)
	return roundFloat(score), segmentEvidence, map[string]any{
		"entity_id":             entityID,
		"segments":              len(segments),
		"gap_events":            len(gaps),
		"segment_detour_ratios": detourRatios,
		"gap_hours":             roundFloat(gapHours),
		"gap_penalty":           roundFloat(gapPenalty),
		"base_score":            roundFloat(base),
	}
}

func militaryLikelihoodMetric(callsign string, record RegistryRecord, states []StateVector, segments []FlightSegment, airports map[string]Airport) (float64, string, []canonical.Evidence, map[string]any) {
	score := 0.0
	strongEvidence := 0
	weakSignals := []string{}
	evidence := copyEvidence(record.Evidence)
	militaryKeywords := []string{"AIR FORCE", "NAVY", "ARMY", "DEFENSE", "DEFENCE", "MILITARY", "AIR NATIONAL GUARD"}
	civilKeywords := []string{"AIRLINES", "AIRWAYS", "CHARTER", "LEASE", "LOGISTICS", "LLC", "INC", "CORPORATION", "BANK"}
	upperOwner := strings.ToUpper(record.RegistrantName)
	upperType := strings.ToUpper(record.RegistrantType)
	if containsAny(upperOwner, militaryKeywords) || containsAny(upperType, []string{"GOVERNMENT", "PUBLIC"}) {
		score += 0.65
		strongEvidence++
		evidence = append(evidence, canonical.Evidence{Kind: "metric_factor", Ref: "registry_owner", Value: record.RegistrantName})
	}
	if prefix := militaryCallsignPrefix(callsign); prefix != "" {
		score += 0.20
		weakSignals = append(weakSignals, "callsign_prefix:"+prefix)
		evidence = append(evidence, canonical.Evidence{Kind: "metric_factor", Ref: "callsign_prefix", Value: prefix})
	}
	militaryAirportVisits := 0
	for _, segment := range segments {
		for _, airportID := range []string{segment.FromAirportID, segment.ToAirportID} {
			airport, ok := airports[airportID]
			if !ok {
				continue
			}
			if airport.Use == "military" || airport.Use == "joint_use" {
				militaryAirportVisits++
			}
		}
	}
	if militaryAirportVisits > 0 {
		airportScore := math.Min(0.10*float64(militaryAirportVisits), 0.20)
		score += airportScore
		weakSignals = append(weakSignals, fmt.Sprintf("military_airport_visits:%d", militaryAirportVisits))
		evidence = append(evidence, canonical.Evidence{Kind: "metric_factor", Ref: "military_airports", Value: fmt.Sprintf("%d", militaryAirportVisits)})
	}
	highPerformance := false
	for _, state := range states {
		if state.Category == 7 || state.Category == 8 {
			highPerformance = true
			break
		}
	}
	if highPerformance {
		score += 0.05
		weakSignals = append(weakSignals, "high_performance_category")
	}
	civilConflict := containsAny(upperOwner, civilKeywords) || containsAny(upperType, []string{"CORPORATION", "INDIVIDUAL", "PARTNERSHIP"})
	if civilConflict {
		score -= 0.25
		evidence = append(evidence, canonical.Evidence{Kind: "metric_guardrail", Ref: "civil_registry_conflict", Value: record.RegistrantName})
		if strongEvidence == 0 && score > 0.45 {
			score = 0.45
		}
	}
	score = clamp(score, 0, 1)
	status := "unknown"
	if strongEvidence > 0 && score >= 0.85 {
		status = "likely_military"
	} else if strongEvidence > 0 && score >= 0.55 {
		status = "possible_military"
	}
	return roundFloat(score), status, evidence, map[string]any{
		"strong_evidence_count":   strongEvidence,
		"weak_signals":            weakSignals,
		"civil_registry_conflict": civilConflict,
		"registrant_name":         record.RegistrantName,
		"registrant_type":         record.RegistrantType,
		"callsign":                callsign,
		"status":                  status,
	}
}

func newMetricSnapshot(metricID string, entity AircraftEntity, value float64, snapshotAt time.Time, attrs map[string]any, evidence []canonical.Evidence, windowStart, windowEnd time.Time) MetricSnapshot {
	return MetricSnapshot{
		SnapshotID:   canonical.NewIdentity(canonical.IDOptions{Namespace: "metric_snapshot", SourceID: entity.EntityID, NativeID: metricID + "-" + windowStart.Format("20060102")}).ID,
		MetricID:     metricID,
		SubjectGrain: "entity",
		SubjectID:    entity.EntityID,
		PlaceID:      entity.PrimaryPlaceID,
		WindowGrain:  "day",
		WindowStart:  windowStart,
		WindowEnd:    windowEnd,
		SnapshotAt:   snapshotAt.UTC().Truncate(time.Millisecond),
		MetricValue:  roundFloat(value),
		MetricDelta:  0,
		Attrs: map[string]any{
			"entity_type":    "aircraft",
			"registration":   entity.Registration,
			"icao24":         entity.ICAO24,
			"callsign":       entity.Callsign,
			"explainability": attrs,
		},
		Evidence: copyEvidence(evidence),
	}
}

func segmentIrregularity(points []TrackPoint, gapCount int) (float64, map[string]any) {
	actualDistance := trackDistance(points)
	directDistance := haversineKM(points[0].Latitude, points[0].Longitude, points[len(points)-1].Latitude, points[len(points)-1].Longitude)
	detourRatio := 1.0
	if directDistance >= 1 {
		detourRatio = actualDistance / directDistance
	}
	turnCount := 0
	for idx := 2; idx < len(points); idx++ {
		first := bearing(points[idx-2], points[idx-1])
		second := bearing(points[idx-1], points[idx])
		if headingDelta(first, second) > 45 {
			turnCount++
		}
	}
	turnRate := 0.0
	if len(points) > 2 {
		turnRate = float64(turnCount) / float64(len(points)-2)
	}
	detourComponent := clamp((detourRatio-1)/0.8, 0, 1)
	gapComponent := clamp(float64(gapCount)*0.15, 0, 0.3)
	score := clamp((0.7*detourComponent)+(0.3*turnRate)+gapComponent, 0, 1)
	return roundFloat(score), map[string]any{
		"actual_distance_km": roundFloat(actualDistance),
		"direct_distance_km": roundFloat(directDistance),
		"detour_ratio":       roundFloat(detourRatio),
		"turn_count":         turnCount,
		"turn_rate":          roundFloat(turnRate),
		"gap_count":          gapCount,
	}
}

func nearestAirportForPoint(point TrackPoint, airports map[string]Airport, airportRadiusKM float64) (Airport, float64, bool) {
	list := make([]Airport, 0, len(airports))
	for _, airport := range airports {
		list = append(list, airport)
	}
	state := StateVector{Latitude: point.Latitude, Longitude: point.Longitude, HasPosition: true, OnGround: point.OnGround, BaroAltitudeM: point.AltitudeM}
	return nearestAirport(point.Latitude, point.Longitude, state, list, airportRadiusKM)
}

func nearestAirport(lat, lon float64, state StateVector, airports []Airport, airportRadiusKM float64) (Airport, float64, bool) {
	if !state.HasPosition {
		return Airport{}, 0, false
	}
	altitude := preferredAltitude(state)
	if !state.OnGround && (altitude == nil || *altitude > airportLowAltitudeM) {
		return Airport{}, 0, false
	}
	bestDistance := math.MaxFloat64
	best := Airport{}
	for _, airport := range airports {
		distance := haversineKM(lat, lon, airport.Latitude, airport.Longitude)
		if distance < bestDistance {
			best = airport
			bestDistance = distance
		}
	}
	if best.AirportID == "" || bestDistance > airportRadiusKM {
		return Airport{}, 0, false
	}
	return best, bestDistance, true
}

func airportIndex(airports []Airport) map[string]Airport {
	index := make(map[string]Airport, len(airports))
	for _, airport := range airports {
		if strings.TrimSpace(airport.AirportID) == "" {
			continue
		}
		index[airport.AirportID] = airport
	}
	return index
}

func sortedKeys(values map[string][]StateVector) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func latestCallsign(states []StateVector) string {
	for idx := len(states) - 1; idx >= 0; idx-- {
		if strings.TrimSpace(states[idx].Callsign) != "" {
			return strings.TrimSpace(states[idx].Callsign)
		}
	}
	return ""
}

func latestCountry(states []StateVector) string {
	for idx := len(states) - 1; idx >= 0; idx-- {
		if strings.TrimSpace(states[idx].OriginCountry) != "" {
			return strings.TrimSpace(states[idx].OriginCountry)
		}
	}
	return ""
}

func choosePrimaryPlaceID(segments []FlightSegment) string {
	for idx := len(segments) - 1; idx >= 0; idx-- {
		if segments[idx].ToPlaceID != "" {
			return segments[idx].ToPlaceID
		}
	}
	for idx := len(segments) - 1; idx >= 0; idx-- {
		if segments[idx].FromPlaceID != "" {
			return segments[idx].FromPlaceID
		}
	}
	return ""
}

func preferredAltitude(state StateVector) *float64 {
	if state.GeoAltitudeM != nil {
		value := roundFloat(*state.GeoAltitudeM)
		return &value
	}
	if state.BaroAltitudeM != nil {
		value := roundFloat(*state.BaroAltitudeM)
		return &value
	}
	return nil
}

func speedKPH(state StateVector) *float64 {
	if state.VelocityMPS == nil {
		return nil
	}
	value := roundFloat(*state.VelocityMPS * knotsToKPH)
	return &value
}

func inFlight(state StateVector) bool {
	if state.OnGround {
		return false
	}
	altitude := preferredAltitude(state)
	return altitude != nil && *altitude > 300
}

func trackDistance(points []TrackPoint) float64 {
	total := 0.0
	for idx := 1; idx < len(points); idx++ {
		total += haversineKM(points[idx-1].Latitude, points[idx-1].Longitude, points[idx].Latitude, points[idx].Longitude)
	}
	return total
}

func haversineKM(lat1, lon1, lat2, lon2 float64) float64 {
	dLat := (lat2 - lat1) * degreesToRadians
	dLon := (lon2 - lon1) * degreesToRadians
	a := math.Sin(dLat/2)*math.Sin(dLat/2) + math.Cos(lat1*degreesToRadians)*math.Cos(lat2*degreesToRadians)*math.Sin(dLon/2)*math.Sin(dLon/2)
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
	return 6371 * c
}

func bearing(from, to TrackPoint) float64 {
	lat1 := from.Latitude * degreesToRadians
	lat2 := to.Latitude * degreesToRadians
	dLon := (to.Longitude - from.Longitude) * degreesToRadians
	y := math.Sin(dLon) * math.Cos(lat2)
	x := math.Cos(lat1)*math.Sin(lat2) - math.Sin(lat1)*math.Cos(lat2)*math.Cos(dLon)
	value := math.Atan2(y, x) * radiansToDegrees
	if value < 0 {
		value += 360
	}
	return value
}

func headingDelta(a, b float64) float64 {
	delta := math.Abs(a - b)
	if delta > 180 {
		delta = 360 - delta
	}
	return delta
}

func dayWindow(ts time.Time) (time.Time, time.Time) {
	start := time.Date(ts.UTC().Year(), ts.UTC().Month(), ts.UTC().Day(), 0, 0, 0, 0, time.UTC)
	return start, start.Add(24 * time.Hour)
}

func mergeEvidence(base []canonical.Evidence, incoming []canonical.Evidence) []canonical.Evidence {
	if len(incoming) == 0 {
		return base
	}
	seen := map[string]struct{}{}
	out := append([]canonical.Evidence(nil), base...)
	for _, item := range out {
		seen[item.Kind+"|"+item.Ref+"|"+item.RawID+"|"+item.Value] = struct{}{}
	}
	for _, item := range incoming {
		key := item.Kind + "|" + item.Ref + "|" + item.RawID + "|" + item.Value
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, item)
	}
	return out
}

func copyEvidence(input []canonical.Evidence) []canonical.Evidence {
	if len(input) == 0 {
		return nil
	}
	return append([]canonical.Evidence(nil), input...)
}

func containsAny(value string, fragments []string) bool {
	for _, fragment := range fragments {
		if strings.Contains(value, fragment) {
			return true
		}
	}
	return false
}

func militaryCallsignPrefix(callsign string) string {
	callsign = strings.ToUpper(strings.TrimSpace(callsign))
	for _, prefix := range []string{"RCH", "MCF", "FORTE", "DUKE", "LAGR", "PAT"} {
		if strings.HasPrefix(callsign, prefix) {
			return prefix
		}
	}
	return ""
}

func nonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func clamp(v, minValue, maxValue float64) float64 {
	if v < minValue {
		return minValue
	}
	if v > maxValue {
		return maxValue
	}
	return v
}

func roundFloat(v float64) float64 {
	return math.Round(v*10000) / 10000
}

func maxFloat(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

func riskBand(score float64) string {
	switch {
	case score >= 0.8:
		return "high"
	case score >= 0.45:
		return "medium"
	default:
		return "low"
	}
}

func floatFromAny(v any) (float64, bool) {
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

func sortAircraft(rows []AircraftEntity) {
	sort.Slice(rows, func(i, j int) bool { return rows[i].EntityID < rows[j].EntityID })
}

func sortTrackPoints(rows []TrackPoint) {
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].TrackID != rows[j].TrackID {
			return rows[i].TrackID < rows[j].TrackID
		}
		return rows[i].ObservedAt.Before(rows[j].ObservedAt)
	})
}

func sortSegments(rows []FlightSegment) {
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].TrackID != rows[j].TrackID {
			return rows[i].TrackID < rows[j].TrackID
		}
		return rows[i].StartedAt.Before(rows[j].StartedAt)
	})
}

func sortGapEvents(rows []GapEvent) {
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].TrackID != rows[j].TrackID {
			return rows[i].TrackID < rows[j].TrackID
		}
		return rows[i].StartedAt.Before(rows[j].StartedAt)
	})
}

func sortInteractions(rows []AirportInteractionEvent) {
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].TrackID != rows[j].TrackID {
			return rows[i].TrackID < rows[j].TrackID
		}
		return rows[i].ObservedAt.Before(rows[j].ObservedAt)
	})
}

func sortMetrics(rows []MetricSnapshot) {
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].MetricID != rows[j].MetricID {
			return rows[i].MetricID < rows[j].MetricID
		}
		if rows[i].MetricValue != rows[j].MetricValue {
			return rows[i].MetricValue > rows[j].MetricValue
		}
		return rows[i].SubjectID < rows[j].SubjectID
	})
}

func assignMetricRanks(rows []MetricSnapshot) {
	rankByMetric := map[string]uint32{}
	for idx := range rows {
		rankByMetric[rows[idx].MetricID]++
		rows[idx].Rank = rankByMetric[rows[idx].MetricID]
	}
}
