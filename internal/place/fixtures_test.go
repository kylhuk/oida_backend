package place

import (
	"math"
	"sort"
	"strings"
	"testing"
)

type fixturePoint struct {
	Lat float64
	Lon float64
}

type fixturePlace struct {
	ID          string
	CountryCode string
	AdminLevel  int
	ParentID    string
	Names       []string
	Polygon     []fixturePoint
}

type fixtureMatch struct {
	PlaceID           string
	CountryCode       string
	DeepestAdminLevel int
	AdministrativeIDs map[int]string
}

type fixtureResolver struct {
	places               []fixturePlace
	byID                 map[string]fixturePlace
	nameIndex            map[string][]fixturePlace
	invalidPolygonIDs    []string
	invalidByCountryCode map[string]int
}

func TestReverseGeocodeFixtures(t *testing.T) {
	resolver := mustFixtureResolver(t)

	t.Run("coordinate resolution reaches deepest available level", func(t *testing.T) {
		cases := []struct {
			name      string
			lat       float64
			lon       float64
			wantID    string
			wantDepth int
		}{
			{name: "Paris France", lat: 48.8566, lon: 2.3522, wantID: "plc:fr-idf-paris", wantDepth: 2},
			{name: "Paris Texas", lat: 33.6609, lon: -95.5555, wantID: "plc:us-tx-paris", wantDepth: 2},
			{name: "Yaren Nauru", lat: -0.5477, lon: 166.9211, wantID: "plc:nr-yaren", wantDepth: 1},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				match, ok := resolver.ResolveCoordinate(tc.lat, tc.lon)
				if !ok {
					t.Fatalf("expected coordinate %f,%f to resolve", tc.lat, tc.lon)
				}
				if match.PlaceID != tc.wantID {
					t.Fatalf("expected place %q, got %q", tc.wantID, match.PlaceID)
				}
				if match.DeepestAdminLevel != tc.wantDepth {
					t.Fatalf("expected depth %d, got %d", tc.wantDepth, match.DeepestAdminLevel)
				}
			})
		}
	})

	t.Run("ambiguous names remain deterministic", func(t *testing.T) {
		matches := resolver.ResolveName("Paris")
		if len(matches) != 2 {
			t.Fatalf("expected 2 Paris matches, got %d", len(matches))
		}
		got := []string{matches[0].ID, matches[1].ID}
		want := []string{"plc:fr-idf-paris", "plc:us-tx-paris"}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("expected ordered ambiguous matches %v, got %v", want, got)
			}
		}
	})

	t.Run("missing admin depth leaves lower levels empty", func(t *testing.T) {
		match, ok := resolver.ResolveCoordinate(-0.5477, 166.9211)
		if !ok {
			t.Fatal("expected Nauru fixture to resolve")
		}
		if match.DeepestAdminLevel != 1 {
			t.Fatalf("expected deepest depth 1, got %d", match.DeepestAdminLevel)
		}
		if match.AdministrativeIDs[0] != "plc:nr" {
			t.Fatalf("expected admin0 chain to include plc:nr, got %q", match.AdministrativeIDs[0])
		}
		if match.AdministrativeIDs[1] != "plc:nr-yaren" {
			t.Fatalf("expected admin1 chain to include plc:nr-yaren, got %q", match.AdministrativeIDs[1])
		}
		if _, ok := match.AdministrativeIDs[2]; ok {
			t.Fatalf("expected no fabricated admin2 id, got %q", match.AdministrativeIDs[2])
		}
		if _, ok := match.AdministrativeIDs[3]; ok {
			t.Fatalf("expected no fabricated admin3 id, got %q", match.AdministrativeIDs[3])
		}
		if _, ok := match.AdministrativeIDs[4]; ok {
			t.Fatalf("expected no fabricated admin4 id, got %q", match.AdministrativeIDs[4])
		}
	})

	t.Run("overlap picks deepest then smallest polygon deterministically", func(t *testing.T) {
		match, ok := resolver.ResolveCoordinate(11.0, 11.0)
		if !ok {
			t.Fatal("expected overlap fixture to resolve")
		}
		if match.PlaceID != "plc:ovl-west" {
			t.Fatalf("expected overlap tie-break to pick plc:ovl-west, got %q", match.PlaceID)
		}
		if match.DeepestAdminLevel != 2 {
			t.Fatalf("expected overlap fixture depth 2, got %d", match.DeepestAdminLevel)
		}
	})

	t.Run("invalid polygons are excluded deterministically", func(t *testing.T) {
		if len(resolver.invalidPolygonIDs) != 1 || resolver.invalidPolygonIDs[0] != "plc:ivl-bowtie" {
			t.Fatalf("expected invalid polygon list [plc:ivl-bowtie], got %v", resolver.invalidPolygonIDs)
		}
		match, ok := resolver.ResolveCoordinate(20.55, 20.55)
		if !ok {
			t.Fatal("expected invalid fixture country shell to resolve")
		}
		if match.PlaceID != "plc:ivl" {
			t.Fatalf("expected invalid child polygon to be skipped, got %q", match.PlaceID)
		}
	})
}

func BenchmarkReverseGeocode(b *testing.B) {
	resolver := mustFixtureResolver(b)
	cases := []struct {
		name string
		lat  float64
		lon  float64
	}{
		{name: "ParisFrance", lat: 48.8566, lon: 2.3522},
		{name: "ParisTexas", lat: 33.6609, lon: -95.5555},
		{name: "YarenNauru", lat: -0.5477, lon: 166.9211},
		{name: "OverlapTieBreak", lat: 11.0, lon: 11.0},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				if _, ok := resolver.ResolveCoordinate(tc.lat, tc.lon); !ok {
					b.Fatalf("expected coordinate %f,%f to resolve", tc.lat, tc.lon)
				}
			}
		})
	}
}

func mustFixtureResolver(tb testing.TB) *fixtureResolver {
	tb.Helper()
	resolver, err := newFixtureResolver(fixturePlaces())
	if err != nil {
		tb.Fatalf("build fixture resolver: %v", err)
	}
	return resolver
}

func newFixtureResolver(places []fixturePlace) (*fixtureResolver, error) {
	resolver := &fixtureResolver{
		byID:                 make(map[string]fixturePlace, len(places)),
		nameIndex:            make(map[string][]fixturePlace),
		invalidByCountryCode: map[string]int{},
	}

	for _, place := range places {
		if !validPolygon(place.Polygon) {
			resolver.invalidPolygonIDs = append(resolver.invalidPolygonIDs, place.ID)
			resolver.invalidByCountryCode[place.CountryCode]++
			continue
		}
		resolver.places = append(resolver.places, place)
		resolver.byID[place.ID] = place
		for _, name := range place.Names {
			key := normalizeName(name)
			resolver.nameIndex[key] = append(resolver.nameIndex[key], place)
		}
	}

	for key := range resolver.nameIndex {
		matches := resolver.nameIndex[key]
		sort.Slice(matches, func(i, j int) bool {
			if matches[i].AdminLevel != matches[j].AdminLevel {
				return matches[i].AdminLevel > matches[j].AdminLevel
			}
			return matches[i].ID < matches[j].ID
		})
		resolver.nameIndex[key] = matches
	}

	sort.Strings(resolver.invalidPolygonIDs)
	return resolver, nil
}

func (r *fixtureResolver) ResolveCoordinate(lat, lon float64) (fixtureMatch, bool) {
	point := fixturePoint{Lat: lat, Lon: lon}
	candidates := make([]fixturePlace, 0, 4)
	for _, place := range r.places {
		if pointInPolygon(point, place.Polygon) {
			candidates = append(candidates, place)
		}
	}
	if len(candidates) == 0 {
		return fixtureMatch{}, false
	}

	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].AdminLevel != candidates[j].AdminLevel {
			return candidates[i].AdminLevel > candidates[j].AdminLevel
		}
		areaI := polygonArea(candidates[i].Polygon)
		areaJ := polygonArea(candidates[j].Polygon)
		if math.Abs(areaI-areaJ) > 1e-9 {
			return areaI < areaJ
		}
		return candidates[i].ID < candidates[j].ID
	})

	selected := candidates[0]
	match := fixtureMatch{
		PlaceID:           selected.ID,
		CountryCode:       selected.CountryCode,
		DeepestAdminLevel: selected.AdminLevel,
		AdministrativeIDs: map[int]string{selected.AdminLevel: selected.ID},
	}

	current := selected
	for current.ParentID != "" {
		parent, ok := r.byID[current.ParentID]
		if !ok {
			break
		}
		match.AdministrativeIDs[parent.AdminLevel] = parent.ID
		current = parent
	}

	return match, true
}

func (r *fixtureResolver) ResolveName(name string) []fixturePlace {
	return append([]fixturePlace(nil), r.nameIndex[normalizeName(name)]...)
}

func normalizeName(name string) string {
	return strings.ToLower(strings.Join(strings.Fields(name), " "))
}

func validPolygon(polygon []fixturePoint) bool {
	if len(polygon) < 4 {
		return false
	}
	if polygon[0] != polygon[len(polygon)-1] {
		return false
	}
	if math.Abs(polygonArea(polygon)) < 1e-9 {
		return false
	}
	edges := len(polygon) - 1
	for i := 0; i < edges; i++ {
		a1 := polygon[i]
		a2 := polygon[i+1]
		for j := i + 1; j < edges; j++ {
			if j == i || j == i+1 || (i == 0 && j == edges-1) {
				continue
			}
			b1 := polygon[j]
			b2 := polygon[j+1]
			if segmentsIntersect(a1, a2, b1, b2) {
				return false
			}
		}
	}
	return true
}

func polygonArea(polygon []fixturePoint) float64 {
	area := 0.0
	for i := 0; i < len(polygon)-1; i++ {
		area += polygon[i].Lon*polygon[i+1].Lat - polygon[i+1].Lon*polygon[i].Lat
	}
	return math.Abs(area) / 2
}

func pointInPolygon(point fixturePoint, polygon []fixturePoint) bool {
	if len(polygon) < 4 {
		return false
	}
	n := len(polygon) - 1
	inside := false
	for i, j := 0, n-1; i < n; j, i = i, i+1 {
		if pointOnSegment(point, polygon[j], polygon[i]) {
			return true
		}
		intersects := ((polygon[i].Lat > point.Lat) != (polygon[j].Lat > point.Lat)) &&
			(point.Lon < (polygon[j].Lon-polygon[i].Lon)*(point.Lat-polygon[i].Lat)/(polygon[j].Lat-polygon[i].Lat)+polygon[i].Lon)
		if intersects {
			inside = !inside
		}
	}
	return inside
}

func pointOnSegment(point, a, b fixturePoint) bool {
	cross := (point.Lat-a.Lat)*(b.Lon-a.Lon) - (point.Lon-a.Lon)*(b.Lat-a.Lat)
	if math.Abs(cross) > 1e-9 {
		return false
	}
	dot := (point.Lon-a.Lon)*(b.Lon-a.Lon) + (point.Lat-a.Lat)*(b.Lat-a.Lat)
	if dot < 0 {
		return false
	}
	lengthSquared := (b.Lon-a.Lon)*(b.Lon-a.Lon) + (b.Lat-a.Lat)*(b.Lat-a.Lat)
	return dot <= lengthSquared+1e-9
}

func segmentsIntersect(a1, a2, b1, b2 fixturePoint) bool {
	o1 := orientation(a1, a2, b1)
	o2 := orientation(a1, a2, b2)
	o3 := orientation(b1, b2, a1)
	o4 := orientation(b1, b2, a2)

	if o1 == 0 && pointOnSegment(b1, a1, a2) {
		return true
	}
	if o2 == 0 && pointOnSegment(b2, a1, a2) {
		return true
	}
	if o3 == 0 && pointOnSegment(a1, b1, b2) {
		return true
	}
	if o4 == 0 && pointOnSegment(a2, b1, b2) {
		return true
	}
	return o1 != o2 && o3 != o4
}

func orientation(a, b, c fixturePoint) int {
	value := (b.Lon-a.Lon)*(c.Lat-a.Lat) - (b.Lat-a.Lat)*(c.Lon-a.Lon)
	switch {
	case math.Abs(value) < 1e-9:
		return 0
	case value > 0:
		return 1
	default:
		return 2
	}
}

func rectangle(minLat, minLon, maxLat, maxLon float64) []fixturePoint {
	return []fixturePoint{
		{Lat: minLat, Lon: minLon},
		{Lat: minLat, Lon: maxLon},
		{Lat: maxLat, Lon: maxLon},
		{Lat: maxLat, Lon: minLon},
		{Lat: minLat, Lon: minLon},
	}
}

func fixturePlaces() []fixturePlace {
	return []fixturePlace{
		{ID: "plc:fr", CountryCode: "FR", AdminLevel: 0, Names: []string{"France"}, Polygon: rectangle(42.0, -5.0, 51.0, 8.5)},
		{ID: "plc:fr-idf", CountryCode: "FR", AdminLevel: 1, ParentID: "plc:fr", Names: []string{"Ile de France"}, Polygon: rectangle(48.0, 1.5, 49.4, 3.7)},
		{ID: "plc:fr-idf-paris", CountryCode: "FR", AdminLevel: 2, ParentID: "plc:fr-idf", Names: []string{"Paris"}, Polygon: rectangle(48.80, 2.20, 48.95, 2.45)},
		{ID: "plc:us", CountryCode: "US", AdminLevel: 0, Names: []string{"United States", "USA"}, Polygon: rectangle(24.0, -125.0, 49.5, -66.0)},
		{ID: "plc:us-tx", CountryCode: "US", AdminLevel: 1, ParentID: "plc:us", Names: []string{"Texas"}, Polygon: rectangle(25.0, -106.8, 36.6, -93.3)},
		{ID: "plc:us-tx-paris", CountryCode: "US", AdminLevel: 2, ParentID: "plc:us-tx", Names: []string{"Paris"}, Polygon: rectangle(33.55, -95.70, 33.82, -95.35)},
		{ID: "plc:nr", CountryCode: "NR", AdminLevel: 0, Names: []string{"Nauru"}, Polygon: rectangle(-0.65, 166.85, -0.40, 167.00)},
		{ID: "plc:nr-yaren", CountryCode: "NR", AdminLevel: 1, ParentID: "plc:nr", Names: []string{"Yaren"}, Polygon: rectangle(-0.58, 166.89, -0.50, 166.96)},
		{ID: "plc:ovl", CountryCode: "OV", AdminLevel: 0, Names: []string{"Overlapia"}, Polygon: rectangle(10.0, 10.0, 12.0, 12.0)},
		{ID: "plc:ovl-central", CountryCode: "OV", AdminLevel: 1, ParentID: "plc:ovl", Names: []string{"Central Overlapia"}, Polygon: rectangle(10.2, 10.2, 11.8, 11.8)},
		{ID: "plc:ovl-east", CountryCode: "OV", AdminLevel: 2, ParentID: "plc:ovl-central", Names: []string{"East Overlap"}, Polygon: rectangle(10.75, 10.75, 11.35, 11.35)},
		{ID: "plc:ovl-west", CountryCode: "OV", AdminLevel: 2, ParentID: "plc:ovl-central", Names: []string{"West Overlap"}, Polygon: rectangle(10.85, 10.85, 11.15, 11.15)},
		{ID: "plc:ivl", CountryCode: "IV", AdminLevel: 0, Names: []string{"Invalidia"}, Polygon: rectangle(20.0, 20.0, 21.0, 21.0)},
		{ID: "plc:ivl-bowtie", CountryCode: "IV", AdminLevel: 1, ParentID: "plc:ivl", Names: []string{"Broken District"}, Polygon: []fixturePoint{{Lat: 20.2, Lon: 20.2}, {Lat: 20.8, Lon: 20.8}, {Lat: 20.2, Lon: 20.8}, {Lat: 20.8, Lon: 20.2}, {Lat: 20.2, Lon: 20.2}}},
	}
}
