package parser

import (
	"slices"
	"sort"
	"strings"
)

var catalogArchetypeParserCompatibility = map[string][]string{
	"http_json":            {"parser:json"},
	"http_csv":             {"parser:csv"},
	"http_xml":             {"parser:xml"},
	"rss_atom":             {"parser:rss", "parser:atom"},
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

func SupportedCatalogArchetype(archetype string) bool {
	_, ok := catalogArchetypeParserCompatibility[strings.TrimSpace(archetype)]
	return ok
}

func CompatibleParserIDsForCatalogArchetype(archetype string) []string {
	allowed := catalogArchetypeParserCompatibility[strings.TrimSpace(archetype)]
	if len(allowed) == 0 {
		return nil
	}
	out := append([]string(nil), allowed...)
	sort.Strings(out)
	return out
}

func ArchetypeParserCompatible(archetype, parserID string) bool {
	parserID = strings.TrimSpace(parserID)
	if parserID == "" {
		return false
	}
	return slices.Contains(catalogArchetypeParserCompatibility[strings.TrimSpace(archetype)], parserID)
}
