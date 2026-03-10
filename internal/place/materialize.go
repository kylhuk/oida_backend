package place

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

const (
	SchemaVersion                uint32 = 1
	APIContractVersion           uint32 = 1
	ReverseGeocodeDictionaryName        = "silver.place_reverse_geocode"
	PolygonDictionarySourceTable        = "silver.place_polygon_dictionary_source"
)

type Point struct {
	Lon float64
	Lat float64
}

type Ring []Point

type Polygon []Ring

type MultiPolygon []Polygon

type EvidenceEntry struct {
	SourceSystem string `json:"source_system"`
	SourceKey    string `json:"source_key"`
	Kind         string `json:"kind"`
}

type PlaceRow struct {
	PlaceID            string
	ParentPlaceID      string
	CanonicalName      string
	PlaceType          string
	AdminLevel         uint8
	CountryCode        string
	ContinentCode      string
	SourcePlaceKey     string
	SourceSystem       string
	Status             string
	CentroidLat        float64
	CentroidLon        float64
	BBoxMinLat         float64
	BBoxMinLon         float64
	BBoxMaxLat         float64
	BBoxMaxLon         float64
	ValidFrom          time.Time
	SchemaVersion      uint32
	RecordVersion      uint64
	APIContractVersion uint32
	UpdatedAt          time.Time
	Attrs              map[string]any
	Evidence           []EvidenceEntry
}

type HierarchyRow struct {
	EdgeID            string
	AncestorPlaceID   string
	DescendantPlaceID string
	RelationshipType  string
	Depth             uint8
	IsDirect          uint8
	PathSource        string
	ValidFrom         time.Time
	SchemaVersion     uint32
	RecordVersion     uint64
	UpdatedAt         time.Time
	Attrs             map[string]any
	Evidence          []EvidenceEntry
}

type PolygonRow struct {
	PolygonID      string
	PlaceID        string
	PolygonRole    string
	GeometryFormat string
	SourceSystem   string
	RingCount      uint32
	PointCount     uint32
	BBoxMinLat     float64
	BBoxMinLon     float64
	BBoxMaxLat     float64
	BBoxMaxLon     float64
	ValidFrom      time.Time
	SchemaVersion  uint32
	RecordVersion  uint64
	UpdatedAt      time.Time
	Geometry       MultiPolygon
	Attrs          map[string]any
	Evidence       []EvidenceEntry
	CanonicalName  string
	CountryCode    string
	PlaceType      string
	AdminLevel     uint8
	SourcePlaceKey string
}

type ReverseFixture struct {
	Name            string
	Lat             float64
	Lon             float64
	ExpectedPlaceID string
	ExpectedDepth   uint8
}

type Bundle struct {
	Places          []PlaceRow
	Hierarchies     []HierarchyRow
	Polygons        []PolygonRow
	ReverseFixtures []ReverseFixture
}

type fixtureDef struct {
	PlaceID        string
	ParentPlaceID  string
	CanonicalName  string
	PlaceType      string
	AdminLevel     uint8
	CountryCode    string
	ContinentCode  string
	SourcePlaceKey string
	SourceSystem   string
	AlternateNames []string
	Boundary       MultiPolygon
	Pseudo         bool
}

type bounds struct {
	MinLat float64
	MinLon float64
	MaxLat float64
	MaxLon float64
	has    bool
}

func BuildBundle(now time.Time) (Bundle, error) {
	now = now.UTC().Truncate(time.Millisecond)
	recordVersion := uint64(now.UnixMilli())
	fixtures := baseFixtures()
	filtered := make([]fixtureDef, 0, len(fixtures))
	for _, fixture := range fixtures {
		if len(fixture.Boundary) > 0 && !fixture.Boundary.Valid() {
			continue
		}
		filtered = append(filtered, fixture)
	}

	fixtureByID := make(map[string]fixtureDef, len(filtered))
	children := map[string][]string{}
	for _, fixture := range filtered {
		fixtureByID[fixture.PlaceID] = fixture
	}
	for _, fixture := range filtered {
		if fixture.ParentPlaceID == "" {
			continue
		}
		if _, ok := fixtureByID[fixture.ParentPlaceID]; !ok {
			return Bundle{}, fmt.Errorf("missing parent %q for %q", fixture.ParentPlaceID, fixture.PlaceID)
		}
		children[fixture.ParentPlaceID] = append(children[fixture.ParentPlaceID], fixture.PlaceID)
	}

	places := make([]PlaceRow, 0, len(filtered))
	baseBounds := make(map[string]bounds, len(filtered))
	for _, fixture := range filtered {
		row := PlaceRow{
			PlaceID:            fixture.PlaceID,
			ParentPlaceID:      fixture.ParentPlaceID,
			CanonicalName:      fixture.CanonicalName,
			PlaceType:          fixture.PlaceType,
			AdminLevel:         fixture.AdminLevel,
			CountryCode:        fixture.CountryCode,
			ContinentCode:      fixture.ContinentCode,
			SourcePlaceKey:     fixture.SourcePlaceKey,
			SourceSystem:       fixture.SourceSystem,
			Status:             "active",
			ValidFrom:          now,
			SchemaVersion:      SchemaVersion,
			RecordVersion:      recordVersion,
			APIContractVersion: APIContractVersion,
			UpdatedAt:          now,
			Attrs: map[string]any{
				"alternate_names":          append([]string(nil), fixture.AlternateNames...),
				"deepest_admin_level":      float64(fixture.AdminLevel),
				"pseudo_place":             fixture.Pseudo,
				"parent_chain_place_ids":   []string{},
				"parent_chain_place_types": []string{},
				"h3_coverage_res7":         []string{},
			},
			Evidence: []EvidenceEntry{{
				SourceSystem: fixture.SourceSystem,
				SourceKey:    fixture.SourcePlaceKey,
				Kind:         "materialized_place",
			}},
		}
		if len(fixture.Boundary) > 0 {
			bbox := fixture.Boundary.Bounds()
			row.CentroidLat = (bbox.MinLat + bbox.MaxLat) / 2
			row.CentroidLon = (bbox.MinLon + bbox.MaxLon) / 2
			row.BBoxMinLat = bbox.MinLat
			row.BBoxMinLon = bbox.MinLon
			row.BBoxMaxLat = bbox.MaxLat
			row.BBoxMaxLon = bbox.MaxLon
			baseBounds[fixture.PlaceID] = bbox
		}
		places = append(places, row)
	}

	var resolveBounds func(string) bounds
	resolveBounds = func(placeID string) bounds {
		if bbox, ok := baseBounds[placeID]; ok && bbox.has {
			return bbox
		}
		bbox := bounds{}
		for _, childID := range children[placeID] {
			bbox = mergeBounds(bbox, resolveBounds(childID))
		}
		if bbox.has {
			baseBounds[placeID] = bbox
		}
		return bbox
	}

	deepestCache := map[string]uint8{}
	var deepestDescendant func(string) uint8
	deepestDescendant = func(placeID string) uint8 {
		if depth, ok := deepestCache[placeID]; ok {
			return depth
		}
		depth := fixtureByID[placeID].AdminLevel
		for _, childID := range children[placeID] {
			childDepth := deepestDescendant(childID)
			if childDepth > depth {
				depth = childDepth
			}
		}
		deepestCache[placeID] = depth
		return depth
	}

	for idx := range places {
		bbox := resolveBounds(places[idx].PlaceID)
		if bbox.has {
			places[idx].BBoxMinLat = bbox.MinLat
			places[idx].BBoxMinLon = bbox.MinLon
			places[idx].BBoxMaxLat = bbox.MaxLat
			places[idx].BBoxMaxLon = bbox.MaxLon
			if places[idx].CentroidLat == 0 && places[idx].CentroidLon == 0 {
				places[idx].CentroidLat = (bbox.MinLat + bbox.MaxLat) / 2
				places[idx].CentroidLon = (bbox.MinLon + bbox.MaxLon) / 2
			}
		}
		lineage := map[string]any{
			"admin0_place_id": nil,
			"admin1_place_id": nil,
			"admin2_place_id": nil,
			"admin3_place_id": nil,
			"admin4_place_id": nil,
		}
		chainIDs, chainTypes := lineageForPlace(fixtureByID, places[idx].PlaceID)
		for _, id := range append([]string{places[idx].PlaceID}, chainIDs...) {
			fixture := fixtureByID[id]
			if fixture.PlaceType == "country" || strings.HasPrefix(fixture.PlaceType, "admin") {
				lineage[fmt.Sprintf("admin%d_place_id", fixture.AdminLevel)] = fixture.PlaceID
			}
		}
		places[idx].Attrs["parent_chain_place_ids"] = chainIDs
		places[idx].Attrs["parent_chain_place_types"] = chainTypes
		for key, value := range lineage {
			places[idx].Attrs[key] = value
		}
		places[idx].Attrs["deepest_admin_level"] = float64(deepestDescendant(places[idx].PlaceID))
	}

	polygons := make([]PolygonRow, 0, len(filtered))
	for _, fixture := range filtered {
		if len(fixture.Boundary) == 0 {
			continue
		}
		bbox := fixture.Boundary.Bounds()
		polygons = append(polygons, PolygonRow{
			PolygonID:      polygonIDForPlace(fixture.PlaceID),
			PlaceID:        fixture.PlaceID,
			PolygonRole:    "primary_boundary",
			GeometryFormat: "multipolygon-array",
			SourceSystem:   "fixture:geoboundaries",
			RingCount:      fixture.Boundary.RingCount(),
			PointCount:     fixture.Boundary.PointCount(),
			BBoxMinLat:     bbox.MinLat,
			BBoxMinLon:     bbox.MinLon,
			BBoxMaxLat:     bbox.MaxLat,
			BBoxMaxLon:     bbox.MaxLon,
			ValidFrom:      now,
			SchemaVersion:  SchemaVersion,
			RecordVersion:  recordVersion,
			UpdatedAt:      now,
			Geometry:       fixture.Boundary,
			Attrs: map[string]any{
				"source_place_key": fixture.SourcePlaceKey,
				"country_code":     fixture.CountryCode,
			},
			Evidence: []EvidenceEntry{{
				SourceSystem: "fixture:geoboundaries",
				SourceKey:    fixture.SourcePlaceKey,
				Kind:         "boundary",
			}},
			CanonicalName:  fixture.CanonicalName,
			CountryCode:    fixture.CountryCode,
			PlaceType:      fixture.PlaceType,
			AdminLevel:     fixture.AdminLevel,
			SourcePlaceKey: fixture.SourcePlaceKey,
		})
	}

	hierarchies := make([]HierarchyRow, 0, len(filtered)*2)
	for _, fixture := range filtered {
		depth := uint8(0)
		currentID := fixture.ParentPlaceID
		for currentID != "" {
			depth++
			ancestor := fixtureByID[currentID]
			hierarchies = append(hierarchies, HierarchyRow{
				EdgeID:            fmt.Sprintf("edge:%s>%s:%d", ancestor.PlaceID, fixture.PlaceID, depth),
				AncestorPlaceID:   ancestor.PlaceID,
				DescendantPlaceID: fixture.PlaceID,
				RelationshipType:  "contains",
				Depth:             depth,
				IsDirect:          boolToUInt8(depth == 1),
				PathSource:        "materialized_graph",
				ValidFrom:         now,
				SchemaVersion:     SchemaVersion,
				RecordVersion:     recordVersion,
				UpdatedAt:         now,
				Attrs: map[string]any{
					"ancestor_place_type":   ancestor.PlaceType,
					"descendant_place_type": fixture.PlaceType,
				},
				Evidence: []EvidenceEntry{{
					SourceSystem: fixture.SourceSystem,
					SourceKey:    fixture.SourcePlaceKey,
					Kind:         "hierarchy",
				}},
			})
			currentID = ancestor.ParentPlaceID
		}
	}

	sort.Slice(places, func(i, j int) bool { return places[i].PlaceID < places[j].PlaceID })
	sort.Slice(polygons, func(i, j int) bool { return polygons[i].PolygonID < polygons[j].PolygonID })
	sort.Slice(hierarchies, func(i, j int) bool { return hierarchies[i].EdgeID < hierarchies[j].EdgeID })

	return Bundle{
		Places:      places,
		Hierarchies: hierarchies,
		Polygons:    polygons,
		ReverseFixtures: []ReverseFixture{
			{Name: "Paris France", Lat: 48.8566, Lon: 2.3522, ExpectedPlaceID: "plc:fr-idf-paris", ExpectedDepth: 2},
			{Name: "Paris Texas", Lat: 33.6609, Lon: -95.5555, ExpectedPlaceID: "plc:us-tx-paris", ExpectedDepth: 2},
			{Name: "Yaren Nauru", Lat: -0.5477, Lon: 166.9211, ExpectedPlaceID: "plc:nr-yaren", ExpectedDepth: 1},
			{Name: "Overlap Tie Break", Lat: 11.0, Lon: 11.0, ExpectedPlaceID: "plc:ovl-west", ExpectedDepth: 2},
		},
	}, nil
}

func (b *Bundle) ApplyH3Coverage(coverage map[string][]string) {
	for idx := range b.Places {
		cells := append([]string(nil), coverage[b.Places[idx].PlaceID]...)
		if cells == nil {
			cells = []string{}
		}
		b.Places[idx].Attrs["h3_coverage_res7"] = cells
	}
}

func (m MultiPolygon) Bounds() bounds {
	bbox := bounds{}
	for _, polygon := range m {
		for _, ring := range polygon {
			for _, point := range ring {
				if !bbox.has {
					bbox = bounds{MinLat: point.Lat, MinLon: point.Lon, MaxLat: point.Lat, MaxLon: point.Lon, has: true}
					continue
				}
				if point.Lat < bbox.MinLat {
					bbox.MinLat = point.Lat
				}
				if point.Lat > bbox.MaxLat {
					bbox.MaxLat = point.Lat
				}
				if point.Lon < bbox.MinLon {
					bbox.MinLon = point.Lon
				}
				if point.Lon > bbox.MaxLon {
					bbox.MaxLon = point.Lon
				}
			}
		}
	}
	return bbox
}

func (m MultiPolygon) RingCount() uint32 {
	var total uint32
	for _, polygon := range m {
		total += uint32(len(polygon))
	}
	return total
}

func (m MultiPolygon) PointCount() uint32 {
	var total uint32
	for _, polygon := range m {
		for _, ring := range polygon {
			total += uint32(len(ring))
		}
	}
	return total
}

func (m MultiPolygon) JSONString() (string, error) {
	if m == nil {
		return "[]", nil
	}
	return marshalString(m.toNestedSlice())
}

func (m MultiPolygon) SQLLiteral() string {
	var b strings.Builder
	b.WriteByte('[')
	for i, polygon := range m {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteByte('[')
		for j, ring := range polygon {
			if j > 0 {
				b.WriteByte(',')
			}
			b.WriteByte('[')
			for k, point := range ring {
				if k > 0 {
					b.WriteByte(',')
				}
				fmt.Fprintf(&b, "(%s,%s)", formatFloat(point.Lon), formatFloat(point.Lat))
			}
			b.WriteByte(']')
		}
		b.WriteByte(']')
	}
	b.WriteByte(']')
	return b.String()
}

func (m MultiPolygon) Valid() bool {
	if len(m) == 0 {
		return true
	}
	for _, polygon := range m {
		if len(polygon) == 0 {
			return false
		}
		for _, ring := range polygon {
			if !ringValid(ring) {
				return false
			}
		}
	}
	return true
}

func (m MultiPolygon) toNestedSlice() [][][][]float64 {
	nested := make([][][][]float64, 0, len(m))
	for _, polygon := range m {
		rings := make([][][]float64, 0, len(polygon))
		for _, ring := range polygon {
			points := make([][]float64, 0, len(ring))
			for _, point := range ring {
				points = append(points, []float64{point.Lon, point.Lat})
			}
			rings = append(rings, points)
		}
		nested = append(nested, rings)
	}
	return nested
}

func baseFixtures() []fixtureDef {
	return []fixtureDef{
		{PlaceID: "plc:world", CanonicalName: "World", PlaceType: "world", SourcePlaceKey: "pseudo:world", SourceSystem: "internal:pseudo", Pseudo: true},
		{PlaceID: "plc:continent:eu", ParentPlaceID: "plc:world", CanonicalName: "Europe", PlaceType: "continent", ContinentCode: "EU", SourcePlaceKey: "pseudo:continent:eu", SourceSystem: "internal:pseudo", Pseudo: true},
		{PlaceID: "plc:continent:na", ParentPlaceID: "plc:world", CanonicalName: "North America", PlaceType: "continent", ContinentCode: "NA", SourcePlaceKey: "pseudo:continent:na", SourceSystem: "internal:pseudo", Pseudo: true},
		{PlaceID: "plc:continent:oc", ParentPlaceID: "plc:world", CanonicalName: "Oceania", PlaceType: "continent", ContinentCode: "OC", SourcePlaceKey: "pseudo:continent:oc", SourceSystem: "internal:pseudo", Pseudo: true},
		{PlaceID: "plc:continent:xt", ParentPlaceID: "plc:world", CanonicalName: "Test Continent", PlaceType: "continent", ContinentCode: "XT", SourcePlaceKey: "pseudo:continent:xt", SourceSystem: "internal:pseudo", Pseudo: true},

		{PlaceID: "plc:fr", ParentPlaceID: "plc:continent:eu", CanonicalName: "France", PlaceType: "country", AdminLevel: 0, CountryCode: "FR", ContinentCode: "EU", SourcePlaceKey: "fixture:fr", SourceSystem: "fixture:geonames", Boundary: materializeRectangle(42.0, -5.0, 51.0, 8.5)},
		{PlaceID: "plc:fr-idf", ParentPlaceID: "plc:fr", CanonicalName: "Ile de France", PlaceType: "admin1", AdminLevel: 1, CountryCode: "FR", ContinentCode: "EU", SourcePlaceKey: "fixture:fr-idf", SourceSystem: "fixture:geonames", AlternateNames: []string{"Ile-de-France"}, Boundary: materializeRectangle(48.0, 1.5, 49.4, 3.7)},
		{PlaceID: "plc:fr-idf-paris", ParentPlaceID: "plc:fr-idf", CanonicalName: "Paris", PlaceType: "admin2", AdminLevel: 2, CountryCode: "FR", ContinentCode: "EU", SourcePlaceKey: "fixture:fr-idf-paris", SourceSystem: "fixture:geonames", AlternateNames: []string{"Paris, France"}, Boundary: materializeRectangle(48.80, 2.20, 48.95, 2.45)},

		{PlaceID: "plc:us", ParentPlaceID: "plc:continent:na", CanonicalName: "United States", PlaceType: "country", AdminLevel: 0, CountryCode: "US", ContinentCode: "NA", SourcePlaceKey: "fixture:us", SourceSystem: "fixture:geonames", AlternateNames: []string{"USA"}, Boundary: materializeRectangle(24.0, -125.0, 49.5, -66.0)},
		{PlaceID: "plc:us-tx", ParentPlaceID: "plc:us", CanonicalName: "Texas", PlaceType: "admin1", AdminLevel: 1, CountryCode: "US", ContinentCode: "NA", SourcePlaceKey: "fixture:us-tx", SourceSystem: "fixture:geonames", Boundary: materializeRectangle(25.0, -106.8, 36.6, -93.3)},
		{PlaceID: "plc:us-tx-paris", ParentPlaceID: "plc:us-tx", CanonicalName: "Paris", PlaceType: "admin2", AdminLevel: 2, CountryCode: "US", ContinentCode: "NA", SourcePlaceKey: "fixture:us-tx-paris", SourceSystem: "fixture:geonames", AlternateNames: []string{"Paris, Texas"}, Boundary: materializeRectangle(33.55, -95.70, 33.82, -95.35)},

		{PlaceID: "plc:nr", ParentPlaceID: "plc:continent:oc", CanonicalName: "Nauru", PlaceType: "country", AdminLevel: 0, CountryCode: "NR", ContinentCode: "OC", SourcePlaceKey: "fixture:nr", SourceSystem: "fixture:geonames", Boundary: materializeRectangle(-0.65, 166.85, -0.40, 167.00)},
		{PlaceID: "plc:nr-yaren", ParentPlaceID: "plc:nr", CanonicalName: "Yaren", PlaceType: "admin1", AdminLevel: 1, CountryCode: "NR", ContinentCode: "OC", SourcePlaceKey: "fixture:nr-yaren", SourceSystem: "fixture:geonames", Boundary: materializeRectangle(-0.58, 166.89, -0.50, 166.96)},

		{PlaceID: "plc:ovl", ParentPlaceID: "plc:continent:xt", CanonicalName: "Overlapia", PlaceType: "country", AdminLevel: 0, CountryCode: "OV", ContinentCode: "XT", SourcePlaceKey: "fixture:ovl", SourceSystem: "fixture:geonames", Boundary: materializeRectangle(10.0, 10.0, 12.0, 12.0)},
		{PlaceID: "plc:ovl-central", ParentPlaceID: "plc:ovl", CanonicalName: "Central Overlapia", PlaceType: "admin1", AdminLevel: 1, CountryCode: "OV", ContinentCode: "XT", SourcePlaceKey: "fixture:ovl-central", SourceSystem: "fixture:geonames", Boundary: materializeRectangle(10.2, 10.2, 11.8, 11.8)},
		{PlaceID: "plc:ovl-east", ParentPlaceID: "plc:ovl-central", CanonicalName: "East Overlap", PlaceType: "admin2", AdminLevel: 2, CountryCode: "OV", ContinentCode: "XT", SourcePlaceKey: "fixture:ovl-east", SourceSystem: "fixture:geonames", Boundary: materializeRectangle(10.75, 10.75, 11.35, 11.35)},
		{PlaceID: "plc:ovl-west", ParentPlaceID: "plc:ovl-central", CanonicalName: "West Overlap", PlaceType: "admin2", AdminLevel: 2, CountryCode: "OV", ContinentCode: "XT", SourcePlaceKey: "fixture:ovl-west", SourceSystem: "fixture:geonames", Boundary: materializeRectangle(10.85, 10.85, 11.15, 11.15)},

		{PlaceID: "plc:ivl", ParentPlaceID: "plc:continent:xt", CanonicalName: "Invalidia", PlaceType: "country", AdminLevel: 0, CountryCode: "IV", ContinentCode: "XT", SourcePlaceKey: "fixture:ivl", SourceSystem: "fixture:geonames", Boundary: materializeRectangle(20.0, 20.0, 21.0, 21.0)},
		{PlaceID: "plc:ivl-bowtie", ParentPlaceID: "plc:ivl", CanonicalName: "Broken District", PlaceType: "admin1", AdminLevel: 1, CountryCode: "IV", ContinentCode: "XT", SourcePlaceKey: "fixture:ivl-bowtie", SourceSystem: "fixture:geonames", Boundary: MultiPolygon{{Ring{{Lon: 20.2, Lat: 20.2}, {Lon: 20.8, Lat: 20.8}, {Lon: 20.8, Lat: 20.2}, {Lon: 20.2, Lat: 20.8}, {Lon: 20.2, Lat: 20.2}}}}},
	}
}

func lineageForPlace(fixtures map[string]fixtureDef, placeID string) ([]string, []string) {
	chainIDs := []string{}
	chainTypes := []string{}
	currentID := fixtures[placeID].ParentPlaceID
	for currentID != "" {
		fixture := fixtures[currentID]
		chainIDs = append(chainIDs, fixture.PlaceID)
		chainTypes = append(chainTypes, fixture.PlaceType)
		currentID = fixture.ParentPlaceID
	}
	for i, j := 0, len(chainIDs)-1; i < j; i, j = i+1, j-1 {
		chainIDs[i], chainIDs[j] = chainIDs[j], chainIDs[i]
		chainTypes[i], chainTypes[j] = chainTypes[j], chainTypes[i]
	}
	return chainIDs, chainTypes
}

func polygonIDForPlace(placeID string) string {
	return strings.Replace(placeID, "plc:", "poly:", 1)
}

func materializeRectangle(minLat, minLon, maxLat, maxLon float64) MultiPolygon {
	return MultiPolygon{
		{
			{
				{Lon: minLon, Lat: minLat},
				{Lon: maxLon, Lat: minLat},
				{Lon: maxLon, Lat: maxLat},
				{Lon: minLon, Lat: maxLat},
				{Lon: minLon, Lat: minLat},
			},
		},
	}
}

func ringValid(ring Ring) bool {
	if len(ring) < 4 {
		return false
	}
	if ring[0] != ring[len(ring)-1] {
		return false
	}
	if math.Abs(ringArea(ring)) < 1e-9 {
		return false
	}
	edges := len(ring) - 1
	for i := 0; i < edges; i++ {
		a1 := ring[i]
		a2 := ring[i+1]
		for j := i + 1; j < edges; j++ {
			if j == i || j == i+1 || (i == 0 && j == edges-1) {
				continue
			}
			b1 := ring[j]
			b2 := ring[j+1]
			if ringSegmentsIntersect(a1, a2, b1, b2) {
				return false
			}
		}
	}
	return true
}

func ringArea(ring Ring) float64 {
	area := 0.0
	for i := 0; i < len(ring)-1; i++ {
		area += ring[i].Lon*ring[i+1].Lat - ring[i+1].Lon*ring[i].Lat
	}
	return math.Abs(area) / 2
}

func ringSegmentsIntersect(a1, a2, b1, b2 Point) bool {
	o1 := ringOrientation(a1, a2, b1)
	o2 := ringOrientation(a1, a2, b2)
	o3 := ringOrientation(b1, b2, a1)
	o4 := ringOrientation(b1, b2, a2)

	if o1 == 0 && ringPointOnSegment(b1, a1, a2) {
		return true
	}
	if o2 == 0 && ringPointOnSegment(b2, a1, a2) {
		return true
	}
	if o3 == 0 && ringPointOnSegment(a1, b1, b2) {
		return true
	}
	if o4 == 0 && ringPointOnSegment(a2, b1, b2) {
		return true
	}
	return o1 != o2 && o3 != o4
}

func ringOrientation(a, b, c Point) int {
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

func ringPointOnSegment(point, a, b Point) bool {
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

func mergeBounds(left, right bounds) bounds {
	if !left.has {
		return right
	}
	if !right.has {
		return left
	}
	return bounds{
		MinLat: math.Min(left.MinLat, right.MinLat),
		MinLon: math.Min(left.MinLon, right.MinLon),
		MaxLat: math.Max(left.MaxLat, right.MaxLat),
		MaxLon: math.Max(left.MaxLon, right.MaxLon),
		has:    true,
	}
}

func boolToUInt8(v bool) uint8 {
	if v {
		return 1
	}
	return 0
}

func marshalString(v any) (string, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func formatFloat(v float64) string {
	formatted := strings.TrimRight(strings.TrimRight(fmt.Sprintf("%.8f", v), "0"), ".")
	if formatted == "" || formatted == "-" {
		return "0"
	}
	return formatted
}
