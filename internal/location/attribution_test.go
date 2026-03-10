package location

import (
	"testing"
	"time"
)

func TestLocationAttribution(t *testing.T) {
	resolver, err := NewResolver(testPlaces())
	if err != nil {
		t.Fatalf("new resolver: %v", err)
	}
	queue := NewUnresolvedQueue()
	attributor := NewAttributor(resolver, queue)
	attributor.MinConfidence = 0.65
	attributor.Now = func() time.Time { return time.Date(2026, time.March, 10, 10, 0, 0, 0, time.UTC) }

	t.Run("explicit coordinates take precedence", func(t *testing.T) {
		result := attributor.Attribute(AttributionInput{
			SubjectKind:        "event",
			SubjectID:          "evt-1",
			SourceID:           "fixture:events",
			ExplicitCoordinate: &Coordinate{Lat: 48.8566, Lon: 2.3522},
			ParsedPlaceHints:   []ParsedPlaceHint{{Name: "Paris", Context: PlaceContext{CountryCode: "US"}}},
		})
		if !result.Resolved {
			t.Fatal("expected record to resolve")
		}
		if result.GeoMethod != GeoMethodExplicitCoordinate {
			t.Fatalf("expected method %q, got %q", GeoMethodExplicitCoordinate, result.GeoMethod)
		}
		if result.PlaceID != "plc:fr-idf-paris" {
			t.Fatalf("expected French Paris, got %q", result.PlaceID)
		}
		assertHierarchyIDs(t, result, "plc:continent:eu", "plc:fr", "plc:fr-idf", "plc:fr-idf-paris", "", "")
	})

	t.Run("explicit polygon then bbox", func(t *testing.T) {
		polygonResult := attributor.Attribute(AttributionInput{
			SubjectKind:     "event",
			SubjectID:       "evt-2",
			SourceID:        "fixture:events",
			ExplicitPolygon: rectangle(33.55, -95.70, 33.82, -95.35),
		})
		if polygonResult.GeoAnchorType != GeoAnchorTypePolygon {
			t.Fatalf("expected polygon anchor, got %q", polygonResult.GeoAnchorType)
		}
		if polygonResult.PlaceID != "plc:us-tx-paris" {
			t.Fatalf("expected Texas Paris from polygon, got %q", polygonResult.PlaceID)
		}

		bboxResult := attributor.Attribute(AttributionInput{
			SubjectKind: "event",
			SubjectID:   "evt-3",
			SourceID:    "fixture:events",
			ExplicitBBox: &BBox{
				MinLat: 33.55,
				MinLon: -95.70,
				MaxLat: 33.82,
				MaxLon: -95.35,
			},
		})
		if bboxResult.GeoAnchorType != GeoAnchorTypeBBox {
			t.Fatalf("expected bbox anchor, got %q", bboxResult.GeoAnchorType)
		}
		if bboxResult.PlaceID != "plc:us-tx-paris" {
			t.Fatalf("expected Texas Paris from bbox, got %q", bboxResult.PlaceID)
		}
	})

	t.Run("parsed place name with context", func(t *testing.T) {
		result := attributor.Attribute(AttributionInput{
			SubjectKind: "event",
			SubjectID:   "evt-4",
			SourceID:    "fixture:events",
			ParsedPlaceHints: []ParsedPlaceHint{{
				Name: "Paris",
				Context: PlaceContext{
					CountryCode: "US",
					Admin0ID:    "plc:us",
				},
			}},
		})
		if result.GeoMethod != GeoMethodParsedPlaceName {
			t.Fatalf("expected parsed place name method, got %q", result.GeoMethod)
		}
		if result.PlaceID != "plc:us-tx-paris" {
			t.Fatalf("expected US Paris disambiguation, got %q", result.PlaceID)
		}
		if result.GeoConfidence < 0.8 {
			t.Fatalf("expected high confidence name match, got %.2f", result.GeoConfidence)
		}
	})

	t.Run("reverse geocoding and fallback chain", func(t *testing.T) {
		reverse := attributor.Attribute(AttributionInput{
			SubjectKind:              "observation",
			SubjectID:                "obs-1",
			SourceID:                 "fixture:events",
			ReverseGeocodeCoordinate: &Coordinate{Lat: -0.5477, Lon: 166.9211},
		})
		if reverse.GeoMethod != GeoMethodReverseGeocode {
			t.Fatalf("expected reverse geocode method, got %q", reverse.GeoMethod)
		}
		if reverse.PlaceID != "plc:nr-yaren" {
			t.Fatalf("expected Nauru Yaren, got %q", reverse.PlaceID)
		}

		track := attributor.Attribute(AttributionInput{
			SubjectKind:         "track",
			SubjectID:           "trk-1",
			SourceID:            "fixture:events",
			TrackDerivedPlaceID: "plc:us-tx",
		})
		if track.GeoMethod != GeoMethodTrackDerivedContext {
			t.Fatalf("expected track fallback, got %q", track.GeoMethod)
		}

		home := attributor.Attribute(AttributionInput{
			SubjectKind:       "entity",
			SubjectID:         "ent-1",
			SourceID:          "fixture:events",
			EntityHomePlaceID: "plc:fr",
		})
		if home.GeoMethod != GeoMethodEntityHomeFallback {
			t.Fatalf("expected entity-home fallback, got %q", home.GeoMethod)
		}

		source := attributor.Attribute(AttributionInput{
			SubjectKind:               "event",
			SubjectID:                 "evt-5",
			SourceID:                  "fixture:events",
			SourceJurisdictionPlaceID: "plc:us",
		})
		if source.GeoMethod != GeoMethodSourceJurisdiction {
			t.Fatalf("expected source jurisdiction fallback, got %q", source.GeoMethod)
		}
	})
}

func TestUnresolvedLocationQueue(t *testing.T) {
	resolver, err := NewResolver(testPlaces())
	if err != nil {
		t.Fatalf("new resolver: %v", err)
	}
	queue := NewUnresolvedQueue()
	now := time.Date(2026, time.March, 10, 12, 0, 0, 0, time.UTC)
	attributor := NewAttributor(resolver, queue)
	attributor.MinConfidence = 0.80
	attributor.Now = func() time.Time { return now }

	result := attributor.Attribute(AttributionInput{
		SubjectKind: "event",
		SubjectID:   "evt-unresolved-1",
		SourceID:    "fixture:events",
		ParsedPlaceHints: []ParsedPlaceHint{{
			Name: "Paris",
		}},
	})
	if result.Resolved {
		t.Fatal("expected ambiguous low-confidence record to remain unresolved")
	}
	if !result.Queued {
		t.Fatal("expected unresolved record to be queued")
	}
	if got := queue.CountPending(); got != 1 {
		t.Fatalf("expected one pending queue record, got %d", got)
	}
	pending := queue.Pending()[0]
	if pending.FailureReason != UnresolvedReasonLowConfidence {
		t.Fatalf("expected failure reason %q, got %q", UnresolvedReasonLowConfidence, pending.FailureReason)
	}
	if pending.ResolverStage != UnresolvedResolverStageAttribution {
		t.Fatalf("expected resolver stage %q, got %q", UnresolvedResolverStageAttribution, pending.ResolverStage)
	}

	now = now.Add(10 * time.Minute)
	reprocess := queue.Reprocess(attributor, func(input *AttributionInput) {
		input.ParsedPlaceHints = []ParsedPlaceHint{{
			Name: "Paris",
			Context: PlaceContext{
				CountryCode: "FR",
				Admin0ID:    "plc:fr",
			},
		}}
	})
	if len(reprocess) != 1 {
		t.Fatalf("expected one reprocess result, got %d", len(reprocess))
	}
	if reprocess[0].State != UnresolvedStateResolved {
		t.Fatalf("expected queue state %q after reprocess, got %q", UnresolvedStateResolved, reprocess[0].State)
	}
	if !reprocess[0].Result.Resolved {
		t.Fatal("expected reprocessed record to resolve")
	}
	if got := queue.CountPending(); got != 0 {
		t.Fatalf("expected no pending records after successful reprocess, got %d", got)
	}
}

func testPlaces() []Place {
	return []Place{
		{PlaceID: "plc:world", Name: "World", PlaceType: "world"},
		{PlaceID: "plc:continent:eu", ParentPlaceID: "plc:world", Name: "Europe", PlaceType: "continent", ContinentID: "plc:continent:eu"},
		{PlaceID: "plc:continent:na", ParentPlaceID: "plc:world", Name: "North America", PlaceType: "continent", ContinentID: "plc:continent:na"},
		{PlaceID: "plc:continent:oc", ParentPlaceID: "plc:world", Name: "Oceania", PlaceType: "continent", ContinentID: "plc:continent:oc"},

		{PlaceID: "plc:fr", ParentPlaceID: "plc:continent:eu", Name: "France", PlaceType: "country", AdminLevel: 0, CountryCode: "FR", GeometryBBox: bbox(42.0, -5.0, 51.0, 8.5)},
		{PlaceID: "plc:fr-idf", ParentPlaceID: "plc:fr", Name: "Ile de France", PlaceType: "admin1", AdminLevel: 1, CountryCode: "FR", GeometryBBox: bbox(48.0, 1.5, 49.4, 3.7)},
		{PlaceID: "plc:fr-idf-paris", ParentPlaceID: "plc:fr-idf", Name: "Paris", AltNames: []string{"Paris, France"}, PlaceType: "admin2", AdminLevel: 2, CountryCode: "FR", GeometryBBox: bbox(48.80, 2.20, 48.95, 2.45)},

		{PlaceID: "plc:us", ParentPlaceID: "plc:continent:na", Name: "United States", AltNames: []string{"USA"}, PlaceType: "country", AdminLevel: 0, CountryCode: "US", GeometryBBox: bbox(24.0, -125.0, 49.5, -66.0)},
		{PlaceID: "plc:us-tx", ParentPlaceID: "plc:us", Name: "Texas", PlaceType: "admin1", AdminLevel: 1, CountryCode: "US", GeometryBBox: bbox(25.0, -106.8, 36.6, -93.3)},
		{PlaceID: "plc:us-tx-paris", ParentPlaceID: "plc:us-tx", Name: "Paris", AltNames: []string{"Paris, Texas"}, PlaceType: "admin2", AdminLevel: 2, CountryCode: "US", GeometryBBox: bbox(33.55, -95.70, 33.82, -95.35)},

		{PlaceID: "plc:nr", ParentPlaceID: "plc:continent:oc", Name: "Nauru", PlaceType: "country", AdminLevel: 0, CountryCode: "NR", GeometryBBox: bbox(-0.65, 166.85, -0.40, 167.00)},
		{PlaceID: "plc:nr-yaren", ParentPlaceID: "plc:nr", Name: "Yaren", PlaceType: "admin1", AdminLevel: 1, CountryCode: "NR", GeometryBBox: bbox(-0.58, 166.89, -0.50, 166.96)},
	}
}

func rectangle(minLat, minLon, maxLat, maxLon float64) []Coordinate {
	return []Coordinate{
		{Lat: minLat, Lon: minLon},
		{Lat: minLat, Lon: maxLon},
		{Lat: maxLat, Lon: maxLon},
		{Lat: maxLat, Lon: minLon},
		{Lat: minLat, Lon: minLon},
	}
}

func bbox(minLat, minLon, maxLat, maxLon float64) *BBox {
	return &BBox{MinLat: minLat, MinLon: minLon, MaxLat: maxLat, MaxLon: maxLon}
}

func assertHierarchyIDs(t *testing.T, got AttributionResult, continentID, admin0, admin1, admin2, admin3, admin4 string) {
	t.Helper()
	if got.ContinentID != continentID {
		t.Fatalf("expected continent %q, got %q", continentID, got.ContinentID)
	}
	if got.Admin0ID != admin0 {
		t.Fatalf("expected admin0 %q, got %q", admin0, got.Admin0ID)
	}
	if got.Admin1ID != admin1 {
		t.Fatalf("expected admin1 %q, got %q", admin1, got.Admin1ID)
	}
	if got.Admin2ID != admin2 {
		t.Fatalf("expected admin2 %q, got %q", admin2, got.Admin2ID)
	}
	if got.Admin3ID != admin3 {
		t.Fatalf("expected admin3 %q, got %q", admin3, got.Admin3ID)
	}
	if got.Admin4ID != admin4 {
		t.Fatalf("expected admin4 %q, got %q", admin4, got.Admin4ID)
	}
}
