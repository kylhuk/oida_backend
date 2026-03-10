package location

import (
	"fmt"
	"math"
	"sort"
	"strings"
)

type Coordinate struct {
	Lat float64
	Lon float64
}

type BBox struct {
	MinLat float64
	MinLon float64
	MaxLat float64
	MaxLon float64
}

func (b BBox) Contains(point Coordinate) bool {
	return point.Lat >= b.MinLat && point.Lat <= b.MaxLat && point.Lon >= b.MinLon && point.Lon <= b.MaxLon
}

func (b BBox) Centroid() Coordinate {
	return Coordinate{
		Lat: (b.MinLat + b.MaxLat) / 2,
		Lon: (b.MinLon + b.MaxLon) / 2,
	}
}

func (b BBox) Area() float64 {
	return math.Abs((b.MaxLat - b.MinLat) * (b.MaxLon - b.MinLon))
}

type Place struct {
	PlaceID       string
	ParentPlaceID string
	Name          string
	AltNames      []string
	PlaceType     string
	AdminLevel    int
	CountryCode   string
	ContinentID   string
	GeometryBBox  *BBox
	Polygon       []Coordinate
}

type PlaceContext struct {
	ContinentID string
	CountryCode string
	Admin0ID    string
	Admin1ID    string
	Admin2ID    string
	Admin3ID    string
	Admin4ID    string
}

type ResolvedPlace struct {
	PlaceID            string
	ContinentID        string
	Admin0ID           string
	Admin1ID           string
	Admin2ID           string
	Admin3ID           string
	Admin4ID           string
	DeepestAdminLevel  int
	CountryCode        string
	PlaceType          string
	AdministrativePath []string
}

type Resolver struct {
	places      []Place
	placeByID   map[string]Place
	nameIndex   map[string][]Place
	lineageByID map[string]ResolvedPlace
}

func NewResolver(places []Place) (*Resolver, error) {
	placeByID := make(map[string]Place, len(places))
	for _, place := range places {
		if strings.TrimSpace(place.PlaceID) == "" {
			return nil, fmt.Errorf("place with empty id")
		}
		placeByID[place.PlaceID] = place
	}

	r := &Resolver{
		places:      append([]Place(nil), places...),
		placeByID:   placeByID,
		nameIndex:   map[string][]Place{},
		lineageByID: map[string]ResolvedPlace{},
	}

	for _, place := range places {
		names := append([]string{place.Name}, place.AltNames...)
		for _, name := range names {
			key := normalizeName(name)
			if key == "" {
				continue
			}
			r.nameIndex[key] = append(r.nameIndex[key], place)
		}
		if _, err := r.buildLineage(place.PlaceID); err != nil {
			return nil, err
		}
	}

	for name := range r.nameIndex {
		matches := r.nameIndex[name]
		sort.Slice(matches, func(i, j int) bool {
			if matches[i].AdminLevel != matches[j].AdminLevel {
				return matches[i].AdminLevel > matches[j].AdminLevel
			}
			return matches[i].PlaceID < matches[j].PlaceID
		})
		r.nameIndex[name] = matches
	}

	return r, nil
}

func (r *Resolver) ResolveByPlaceID(placeID string) (ResolvedPlace, bool) {
	resolved, ok := r.lineageByID[placeID]
	return resolved, ok
}

func (r *Resolver) ResolveByCoordinate(point Coordinate) (ResolvedPlace, bool) {
	type candidate struct {
		place    Place
		lineage  ResolvedPlace
		area     float64
		hasArea  bool
		contains bool
	}

	candidates := make([]candidate, 0, 8)
	for _, place := range r.places {
		contains, area, hasArea := placeContainsPoint(place, point)
		if !contains {
			continue
		}
		lineage := r.lineageByID[place.PlaceID]
		candidates = append(candidates, candidate{place: place, lineage: lineage, area: area, hasArea: hasArea, contains: true})
	}

	if len(candidates) == 0 {
		return ResolvedPlace{}, false
	}

	sort.Slice(candidates, func(i, j int) bool {
		left := candidates[i]
		right := candidates[j]
		if left.place.AdminLevel != right.place.AdminLevel {
			return left.place.AdminLevel > right.place.AdminLevel
		}
		if left.hasArea && right.hasArea {
			if math.Abs(left.area-right.area) > 1e-9 {
				return left.area < right.area
			}
		}
		return left.place.PlaceID < right.place.PlaceID
	})

	return candidates[0].lineage, true
}

func (r *Resolver) ResolveByBBox(bbox BBox) (ResolvedPlace, bool) {
	return r.ResolveByCoordinate(bbox.Centroid())
}

func (r *Resolver) ResolveByPolygon(polygon []Coordinate) (ResolvedPlace, bool) {
	if len(polygon) == 0 {
		return ResolvedPlace{}, false
	}
	centroid := polygonCentroid(polygon)
	return r.ResolveByCoordinate(centroid)
}

func (r *Resolver) ResolveByName(name string, ctx PlaceContext) (ResolvedPlace, float64, bool) {
	matches := r.nameIndex[normalizeName(name)]
	if len(matches) == 0 {
		return ResolvedPlace{}, 0, false
	}

	type scored struct {
		place      Place
		lineage    ResolvedPlace
		confidence float64
	}

	scoredMatches := make([]scored, 0, len(matches))
	for _, place := range matches {
		lineage := r.lineageByID[place.PlaceID]
		score := 0.45
		if ctx.CountryCode != "" && strings.EqualFold(ctx.CountryCode, place.CountryCode) {
			score += 0.25
		}
		if ctx.ContinentID != "" && ctx.ContinentID == lineage.ContinentID {
			score += 0.05
		}
		if ctx.Admin0ID != "" && ctx.Admin0ID == lineage.Admin0ID {
			score += 0.15
		}
		if ctx.Admin1ID != "" && ctx.Admin1ID == lineage.Admin1ID {
			score += 0.10
		}
		if ctx.Admin2ID != "" && ctx.Admin2ID == lineage.Admin2ID {
			score += 0.07
		}
		if ctx.Admin3ID != "" && ctx.Admin3ID == lineage.Admin3ID {
			score += 0.04
		}
		if ctx.Admin4ID != "" && ctx.Admin4ID == lineage.Admin4ID {
			score += 0.04
		}
		if score > 0.99 {
			score = 0.99
		}
		scoredMatches = append(scoredMatches, scored{place: place, lineage: lineage, confidence: score})
	}

	sort.Slice(scoredMatches, func(i, j int) bool {
		if math.Abs(scoredMatches[i].confidence-scoredMatches[j].confidence) > 1e-9 {
			return scoredMatches[i].confidence > scoredMatches[j].confidence
		}
		if scoredMatches[i].place.AdminLevel != scoredMatches[j].place.AdminLevel {
			return scoredMatches[i].place.AdminLevel > scoredMatches[j].place.AdminLevel
		}
		return scoredMatches[i].place.PlaceID < scoredMatches[j].place.PlaceID
	})

	return scoredMatches[0].lineage, scoredMatches[0].confidence, true
}

func (r *Resolver) buildLineage(placeID string) (ResolvedPlace, error) {
	if resolved, ok := r.lineageByID[placeID]; ok {
		return resolved, nil
	}

	place, ok := r.placeByID[placeID]
	if !ok {
		return ResolvedPlace{}, fmt.Errorf("unknown place id %q", placeID)
	}

	resolved := ResolvedPlace{
		PlaceID:            place.PlaceID,
		CountryCode:        place.CountryCode,
		PlaceType:          place.PlaceType,
		DeepestAdminLevel:  -1,
		AdministrativePath: []string{place.PlaceID},
	}

	for current := place; current.PlaceID != ""; {
		if current.PlaceType == "continent" {
			resolved.ContinentID = current.PlaceID
		}
		if current.PlaceType == "country" || current.PlaceType == "admin0" {
			resolved.Admin0ID = current.PlaceID
			if resolved.DeepestAdminLevel < 0 {
				resolved.DeepestAdminLevel = 0
			}
		}
		switch current.AdminLevel {
		case 1:
			resolved.Admin1ID = current.PlaceID
			if resolved.DeepestAdminLevel < 1 {
				resolved.DeepestAdminLevel = 1
			}
		case 2:
			resolved.Admin2ID = current.PlaceID
			if resolved.DeepestAdminLevel < 2 {
				resolved.DeepestAdminLevel = 2
			}
		case 3:
			resolved.Admin3ID = current.PlaceID
			if resolved.DeepestAdminLevel < 3 {
				resolved.DeepestAdminLevel = 3
			}
		case 4:
			resolved.Admin4ID = current.PlaceID
			if resolved.DeepestAdminLevel < 4 {
				resolved.DeepestAdminLevel = 4
			}
		}
		if current.ParentPlaceID == "" {
			break
		}
		parent, parentOK := r.placeByID[current.ParentPlaceID]
		if !parentOK {
			return ResolvedPlace{}, fmt.Errorf("missing parent %q for place %q", current.ParentPlaceID, current.PlaceID)
		}
		resolved.AdministrativePath = append(resolved.AdministrativePath, parent.PlaceID)
		current = parent
	}

	r.lineageByID[placeID] = resolved
	return resolved, nil
}

func placeContainsPoint(place Place, point Coordinate) (bool, float64, bool) {
	if len(place.Polygon) > 0 {
		if !pointInPolygon(point, place.Polygon) {
			return false, 0, false
		}
		return true, polygonArea(place.Polygon), true
	}
	if place.GeometryBBox != nil {
		if !place.GeometryBBox.Contains(point) {
			return false, 0, false
		}
		return true, place.GeometryBBox.Area(), true
	}
	return false, 0, false
}

func polygonCentroid(polygon []Coordinate) Coordinate {
	if len(polygon) == 0 {
		return Coordinate{}
	}
	signedArea := 0.0
	cx := 0.0
	cy := 0.0
	for i := 0; i < len(polygon)-1; i++ {
		x0 := polygon[i].Lon
		y0 := polygon[i].Lat
		x1 := polygon[i+1].Lon
		y1 := polygon[i+1].Lat
		a := x0*y1 - x1*y0
		signedArea += a
		cx += (x0 + x1) * a
		cy += (y0 + y1) * a
	}
	if math.Abs(signedArea) < 1e-9 {
		return polygon[0]
	}
	signedArea *= 0.5
	cx /= 6 * signedArea
	cy /= 6 * signedArea
	return Coordinate{Lat: cy, Lon: cx}
}

func normalizeName(name string) string {
	return strings.ToLower(strings.Join(strings.Fields(name), " "))
}

func pointInPolygon(point Coordinate, polygon []Coordinate) bool {
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

func pointOnSegment(point, a, b Coordinate) bool {
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

func polygonArea(polygon []Coordinate) float64 {
	area := 0.0
	for i := 0; i < len(polygon)-1; i++ {
		area += polygon[i].Lon*polygon[i+1].Lat - polygon[i+1].Lon*polygon[i].Lat
	}
	return math.Abs(area) / 2
}
