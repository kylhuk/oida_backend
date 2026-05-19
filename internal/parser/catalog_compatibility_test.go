package parser

import (
	"reflect"
	"testing"
)

func TestArchetypeParserCompatibility(t *testing.T) {
	registry := DefaultRegistry()
	registered := make(map[string]struct{})
	for _, record := range registry.Records() {
		registered[record.ParserID] = struct{}{}
	}

	cases := map[string][]string{
		"http_json":            {"parser:json"},
		"http_csv":             {"parser:csv"},
		"http_xml":             {"parser:xml"},
		"rss_atom":             {"parser:atom", "parser:rss"},
		"html_profile":         {"parser:html-profile"},
		"browser_rendered":     {"parser:vesselfinder-html", "parser:vesselfinder-route-json"},
		"bulk_file":            {"parser:csv", "parser:json", "parser:xml"},
		"stac_api":             {"parser:json"},
		"catalog_ckan":         {"parser:json"},
		"catalog_socrata":      {"parser:json"},
		"catalog_opendatasoft": {"parser:json"},
		"arcgis_rest":          {"parser:json"},
		"ogc_features":         {"parser:json"},
		"ogc_records":          {"parser:xml"},
		"discovery_web":        {"parser:html-profile"},
		"deferred_transport":   nil,
	}

	t.Run("websocket_stream", func(t *testing.T) {
		archetype := "websocket_stream"
		want := []string{"parser:aisstream-json"}
		if !SupportedCatalogArchetype(archetype) {
			t.Fatalf("expected %q to be a supported catalog archetype", archetype)
		}
		got := CompatibleParserIDsForCatalogArchetype(archetype)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("expected archetype %q compatibility %v, got %v", archetype, want, got)
		}
		for _, parserID := range got {
			if _, ok := registered[parserID]; !ok {
				t.Fatalf("expected parser %q for archetype %q to be registered", parserID, archetype)
			}
			if !ArchetypeParserCompatible(archetype, parserID) {
				t.Fatalf("expected parser %q to be compatible with archetype %q", parserID, archetype)
			}
		}
	})

	for archetype, want := range cases {
		if !SupportedCatalogArchetype(archetype) {
			t.Fatalf("expected %q to remain a supported catalog archetype", archetype)
		}
		got := CompatibleParserIDsForCatalogArchetype(archetype)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("expected archetype %q compatibility %v, got %v", archetype, want, got)
		}
		for _, parserID := range got {
			if _, ok := registered[parserID]; !ok {
				t.Fatalf("expected parser %q for archetype %q to stay registered", parserID, archetype)
			}
			if !ArchetypeParserCompatible(archetype, parserID) {
				t.Fatalf("expected parser %q to remain compatible with archetype %q", parserID, archetype)
			}
		}
	}

	if ArchetypeParserCompatible("http_json", "parser:csv") {
		t.Fatal("expected csv parser to stay incompatible with http_json")
	}
	if ArchetypeParserCompatible("deferred_transport", "parser:json") {
		t.Fatal("expected deferred_transport to remain parser-free")
	}
	if SupportedCatalogArchetype("made_up") {
		t.Fatal("expected unsupported archetype to be rejected")
	}
}
