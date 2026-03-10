package location

import (
	"fmt"
	"time"
)

const (
	GeoMethodExplicitCoordinate        = "explicit_coordinate"
	GeoMethodExplicitGeometry          = "explicit_geometry"
	GeoMethodParsedPlaceName           = "parsed_place_name"
	GeoMethodReverseGeocode            = "reverse_geocode"
	GeoMethodTrackDerivedContext       = "track_derived_context"
	GeoMethodEntityHomeFallback        = "entity_home_fallback"
	GeoMethodSourceJurisdiction        = "source_jurisdiction_fallback"
	GeoAnchorTypePoint                 = "point"
	GeoAnchorTypePolygon               = "polygon"
	GeoAnchorTypeBBox                  = "bbox"
	GeoAnchorTypePlaceName             = "place_name"
	GeoAnchorTypeTrackContext          = "track_context"
	GeoAnchorTypeEntityHome            = "entity_home"
	GeoAnchorTypeSourceJurisdiction    = "source_jurisdiction"
	UnresolvedReasonNoMatch            = "no_match"
	UnresolvedReasonLowConfidence      = "low_confidence"
	UnresolvedResolverStageAttribution = "location_attribution"
)

type ParsedPlaceHint struct {
	Name    string
	Context PlaceContext
}

type AttributionInput struct {
	SubjectKind               string
	SubjectID                 string
	SourceID                  string
	RawID                     string
	ExplicitCoordinate        *Coordinate
	ExplicitPolygon           []Coordinate
	ExplicitBBox              *BBox
	ParsedPlaceHints          []ParsedPlaceHint
	ReverseGeocodeCoordinate  *Coordinate
	TrackDerivedPlaceID       string
	EntityHomePlaceID         string
	SourceJurisdictionPlaceID string
	Metadata                  map[string]any
	Evidence                  map[string]any
}

type AttributionResult struct {
	PlaceID           string
	ContinentID       string
	Admin0ID          string
	Admin1ID          string
	Admin2ID          string
	Admin3ID          string
	Admin4ID          string
	GeoAnchorType     string
	GeoMethod         string
	GeoConfidence     float64
	DeepestAdminLevel int
	Resolved          bool
	Queued            bool
}

type Attributor struct {
	Resolver      *Resolver
	Unresolved    *UnresolvedQueue
	MinConfidence float64
	Now           func() time.Time
}

func NewAttributor(resolver *Resolver, unresolved *UnresolvedQueue) *Attributor {
	return &Attributor{
		Resolver:      resolver,
		Unresolved:    unresolved,
		MinConfidence: 0.65,
		Now:           time.Now,
	}
}

func (a *Attributor) Attribute(input AttributionInput) AttributionResult {
	return a.attribute(input, true)
}

func (a *Attributor) attribute(input AttributionInput, queueOnFailure bool) AttributionResult {
	if a == nil || a.Resolver == nil {
		return AttributionResult{}
	}

	attempt := func(resolved ResolvedPlace, method, anchor string, confidence float64) AttributionResult {
		result := AttributionResult{
			PlaceID:           resolved.PlaceID,
			ContinentID:       resolved.ContinentID,
			Admin0ID:          resolved.Admin0ID,
			Admin1ID:          resolved.Admin1ID,
			Admin2ID:          resolved.Admin2ID,
			Admin3ID:          resolved.Admin3ID,
			Admin4ID:          resolved.Admin4ID,
			GeoAnchorType:     anchor,
			GeoMethod:         method,
			GeoConfidence:     confidence,
			DeepestAdminLevel: resolved.DeepestAdminLevel,
			Resolved:          confidence >= a.MinConfidence,
		}
		if result.Resolved {
			return result
		}
		if queueOnFailure {
			a.queue(input, result, UnresolvedReasonLowConfidence)
			result.Queued = true
		}
		return result
	}

	if input.ExplicitCoordinate != nil {
		if resolved, ok := a.Resolver.ResolveByCoordinate(*input.ExplicitCoordinate); ok {
			return attempt(resolved, GeoMethodExplicitCoordinate, GeoAnchorTypePoint, 0.99)
		}
	}

	if len(input.ExplicitPolygon) > 0 {
		if resolved, ok := a.Resolver.ResolveByPolygon(input.ExplicitPolygon); ok {
			return attempt(resolved, GeoMethodExplicitGeometry, GeoAnchorTypePolygon, 0.95)
		}
	}

	if input.ExplicitBBox != nil {
		if resolved, ok := a.Resolver.ResolveByBBox(*input.ExplicitBBox); ok {
			return attempt(resolved, GeoMethodExplicitGeometry, GeoAnchorTypeBBox, 0.93)
		}
	}

	for _, hint := range input.ParsedPlaceHints {
		if resolved, confidence, ok := a.Resolver.ResolveByName(hint.Name, hint.Context); ok {
			return attempt(resolved, GeoMethodParsedPlaceName, GeoAnchorTypePlaceName, confidence)
		}
	}

	if input.ReverseGeocodeCoordinate != nil {
		if resolved, ok := a.Resolver.ResolveByCoordinate(*input.ReverseGeocodeCoordinate); ok {
			return attempt(resolved, GeoMethodReverseGeocode, GeoAnchorTypePoint, 0.78)
		}
	}

	if input.TrackDerivedPlaceID != "" {
		if resolved, ok := a.Resolver.ResolveByPlaceID(input.TrackDerivedPlaceID); ok {
			return attempt(resolved, GeoMethodTrackDerivedContext, GeoAnchorTypeTrackContext, 0.74)
		}
	}

	if input.EntityHomePlaceID != "" {
		if resolved, ok := a.Resolver.ResolveByPlaceID(input.EntityHomePlaceID); ok {
			return attempt(resolved, GeoMethodEntityHomeFallback, GeoAnchorTypeEntityHome, 0.70)
		}
	}

	if input.SourceJurisdictionPlaceID != "" {
		if resolved, ok := a.Resolver.ResolveByPlaceID(input.SourceJurisdictionPlaceID); ok {
			return attempt(resolved, GeoMethodSourceJurisdiction, GeoAnchorTypeSourceJurisdiction, 0.66)
		}
	}

	result := AttributionResult{}
	if queueOnFailure {
		a.queue(input, result, UnresolvedReasonNoMatch)
		result.Queued = true
	}
	return result
}

func (a *Attributor) queue(input AttributionInput, result AttributionResult, reason string) {
	if a.Unresolved == nil {
		return
	}
	now := time.Now().UTC()
	if a.Now != nil {
		now = a.Now().UTC()
	}
	a.Unresolved.Enqueue(UnresolvedRecord{
		SubjectKind:   input.SubjectKind,
		SubjectID:     input.SubjectID,
		SourceID:      input.SourceID,
		RawID:         input.RawID,
		ResolverStage: UnresolvedResolverStageAttribution,
		FailureReason: reason,
		State:         UnresolvedStatePending,
		Priority:      100,
		RetryCount:    0,
		FirstFailedAt: now,
		LastFailedAt:  now,
		NextRetryAt:   now,
		LocationHint:  buildLocationHint(input),
		Attrs: map[string]any{
			"geo_method":     result.GeoMethod,
			"geo_confidence": result.GeoConfidence,
			"place_id":       result.PlaceID,
		},
		Evidence: input.Evidence,
		Input:    input,
	})
}

func buildLocationHint(input AttributionInput) string {
	if len(input.ParsedPlaceHints) > 0 {
		return input.ParsedPlaceHints[0].Name
	}
	if input.TrackDerivedPlaceID != "" {
		return "track:" + input.TrackDerivedPlaceID
	}
	if input.SourceJurisdictionPlaceID != "" {
		return "source:" + input.SourceJurisdictionPlaceID
	}
	if input.ExplicitCoordinate != nil {
		return fmt.Sprintf("%0.6f,%0.6f", input.ExplicitCoordinate.Lat, input.ExplicitCoordinate.Lon)
	}
	return ""
}
