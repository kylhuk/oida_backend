package place

import (
	"testing"
	"time"
)

func TestBuildBundleMaterializesPseudoPlacesAndDepths(t *testing.T) {
	bundle, err := BuildBundle(time.Date(2026, time.March, 10, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("build bundle: %v", err)
	}

	world := mustPlace(t, bundle, "plc:world")
	if got := world.PlaceType; got != "world" {
		t.Fatalf("expected world place_type, got %q", got)
	}
	if got := world.Attrs["pseudo_place"]; got != true {
		t.Fatalf("expected pseudo_place true, got %#v", got)
	}

	yaren := mustPlace(t, bundle, "plc:nr-yaren")
	if got := yaren.Attrs["admin2_place_id"]; got != nil {
		t.Fatalf("expected no fabricated admin2 id, got %#v", got)
	}
	if got := yaren.Attrs["deepest_admin_level"]; got != float64(1) {
		t.Fatalf("expected Yaren deepest depth 1, got %#v", got)
	}

	invalidia := mustPlace(t, bundle, "plc:ivl")
	if got := invalidia.Attrs["deepest_admin_level"]; got != float64(0) {
		t.Fatalf("expected Invalidia deepest depth 0 after invalid child removal, got %#v", got)
	}
	if _, ok := maybePlace(bundle, "plc:ivl-bowtie"); ok {
		t.Fatal("expected invalid bowtie place to be excluded")
	}
}

func TestBuildBundleGeneratesHierarchyAndPolygonRows(t *testing.T) {
	bundle, err := BuildBundle(time.Date(2026, time.March, 10, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("build bundle: %v", err)
	}

	if _, ok := maybeHierarchy(bundle, "plc:world", "plc:fr-idf-paris", 4); !ok {
		t.Fatal("expected world -> Paris France hierarchy edge at depth 4")
	}
	if _, ok := maybeHierarchy(bundle, "plc:ovl-central", "plc:ovl-west", 1); !ok {
		t.Fatal("expected direct overlap hierarchy edge")
	}

	polygon := mustPolygon(t, bundle, "poly:ovl-west")
	if polygon.PointCount == 0 || polygon.RingCount == 0 {
		t.Fatalf("expected overlap polygon geometry stats, got points=%d rings=%d", polygon.PointCount, polygon.RingCount)
	}
	if polygon.PlaceID != "plc:ovl-west" {
		t.Fatalf("expected overlap polygon for plc:ovl-west, got %q", polygon.PlaceID)
	}
	if got := len(bundle.ReverseFixtures); got < 4 {
		t.Fatalf("expected reverse fixtures, got %d", got)
	}
}

func mustPlace(t *testing.T, bundle Bundle, id string) PlaceRow {
	t.Helper()
	place, ok := maybePlace(bundle, id)
	if !ok {
		t.Fatalf("missing place %s", id)
	}
	return place
}

func maybePlace(bundle Bundle, id string) (PlaceRow, bool) {
	for _, place := range bundle.Places {
		if place.PlaceID == id {
			return place, true
		}
	}
	return PlaceRow{}, false
}

func maybeHierarchy(bundle Bundle, ancestorID, descendantID string, depth uint8) (HierarchyRow, bool) {
	for _, edge := range bundle.Hierarchies {
		if edge.AncestorPlaceID == ancestorID && edge.DescendantPlaceID == descendantID && edge.Depth == depth {
			return edge, true
		}
	}
	return HierarchyRow{}, false
}

func mustPolygon(t *testing.T, bundle Bundle, id string) PolygonRow {
	t.Helper()
	for _, polygon := range bundle.Polygons {
		if polygon.PolygonID == id {
			return polygon
		}
	}
	t.Fatalf("missing polygon %s", id)
	return PolygonRow{}
}
