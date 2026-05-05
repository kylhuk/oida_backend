package discovery

import (
	"testing"
	"time"
)

func TestFingerprintProbeGeneration(t *testing.T) {
	now := time.Date(2026, time.March, 11, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name        string
		probe       FingerprintProbe
		observed    []string
		expectedURL string
		wantSignal  string
	}{
		{
			name:        "ckan",
			probe:       FingerprintProbe{CatalogID: "catalog:fingerprint:discovery:ckan-action-api", ProbeName: "CKAN Action API", IntegrationArchetype: "catalog_ckan", ProbePatterns: []string{"/api/3/action/package_search", "dataset pages with /dataset/"}},
			observed:    []string{"https://data.example.org/portal", "https://data.example.org/portal?utm_source=test"},
			expectedURL: "https://data.example.org/api/3/action/package_search?rows=1",
			wantSignal:  "/api/3/action/package_search",
		},
		{
			name:        "socrata",
			probe:       FingerprintProbe{CatalogID: "catalog:fingerprint:discovery:socrata-soda", ProbeName: "Socrata / SODA", IntegrationArchetype: "catalog_socrata", ProbePatterns: []string{"/api/views", "/resource/<id>.json", "CSV/JSON download links"}},
			observed:    []string{"https://city.example.gov/open-data"},
			expectedURL: "https://city.example.gov/api/views",
			wantSignal:  "/api/views",
		},
		{
			name:        "arcgis-hub",
			probe:       FingerprintProbe{CatalogID: "catalog:fingerprint:discovery:arcgis-hub", ProbeName: "ArcGIS Hub", IntegrationArchetype: "arcgis_rest", ProbePatterns: []string{"/api/search/v1", "hub site catalogs and datasets"}},
			observed:    []string{"https://maps.example.gov/"},
			expectedURL: "https://maps.example.gov/api/search/v1",
			wantSignal:  "/api/search/v1",
		},
		{
			name:        "arcgis-rest",
			probe:       FingerprintProbe{CatalogID: "catalog:fingerprint:discovery:arcgis-rest-services", ProbeName: "ArcGIS REST Services", IntegrationArchetype: "arcgis_rest", ProbePatterns: []string{"/arcgis/rest/services", "/FeatureServer", "/MapServer", "/ImageServer"}},
			observed:    []string{"https://services.example.gov/portal"},
			expectedURL: "https://services.example.gov/FeatureServer",
			wantSignal:  "/featureserver",
		},
		{
			name:        "opendatasoft",
			probe:       FingerprintProbe{CatalogID: "catalog:fingerprint:discovery:opendatasoft-explore-api", ProbeName: "Opendatasoft Explore API", IntegrationArchetype: "catalog_opendatasoft", ProbePatterns: []string{"/api/explore/v2.1/catalog/datasets", "dataset pages with records/explore"}},
			observed:    []string{"https://data.example.fr/"},
			expectedURL: "https://data.example.fr/api/explore/v2.1/catalog/datasets",
			wantSignal:  "/api/explore/v2.1/catalog/datasets",
		},
		{
			name:        "geonetwork",
			probe:       FingerprintProbe{CatalogID: "catalog:fingerprint:discovery:geonetwork", ProbeName: "GeoNetwork", IntegrationArchetype: "discovery_web", ProbePatterns: []string{"/geonetwork/srv/api/records", "CSW"}},
			observed:    []string{"https://geo.example.org/portal"},
			expectedURL: "https://geo.example.org/csw",
			wantSignal:  "csw",
		},
		{
			name:        "geonode",
			probe:       FingerprintProbe{CatalogID: "catalog:fingerprint:discovery:geonode", ProbeName: "GeoNode", IntegrationArchetype: "discovery_web", ProbePatterns: []string{"/api/v2/", "/catalogue", "/layers", "/documents"}},
			observed:    []string{"https://geonode.example.org/"},
			expectedURL: "https://geonode.example.org/api/v2/datasets",
			wantSignal:  "/api/v2/",
		},
		{
			name:        "ogc-features",
			probe:       FingerprintProbe{CatalogID: "catalog:fingerprint:discovery:ogc-api-features", ProbeName: "OGC API - Features", IntegrationArchetype: "ogc_features", ProbePatterns: []string{"/collections", "/collections/{id}/items"}},
			observed:    []string{"https://ogc.example.org/service"},
			expectedURL: "https://ogc.example.org/collections",
			wantSignal:  "/collections",
		},
		{
			name:        "ogc-records",
			probe:       FingerprintProbe{CatalogID: "catalog:fingerprint:discovery:ogc-api-records", ProbeName: "OGC API - Records", IntegrationArchetype: "ogc_records", ProbePatterns: []string{"/collections or /records depending on implementation"}},
			observed:    []string{"https://records.example.org/catalog"},
			expectedURL: "https://records.example.org/records",
			wantSignal:  "/collections or /records depending on implementation",
		},
		{
			name:        "ogc-wfs",
			probe:       FingerprintProbe{CatalogID: "catalog:fingerprint:discovery:ogc-wfs", ProbeName: "OGC WFS", IntegrationArchetype: "ogc_records", ProbePatterns: []string{"service=WFS or service=WMS query parameters"}},
			observed:    []string{"https://geo.example.org/wfs"},
			expectedURL: "https://geo.example.org/?request=GetCapabilities&service=WFS",
			wantSignal:  "service=wfs or service=wms query parameters",
		},
		{
			name:        "ogc-wms",
			probe:       FingerprintProbe{CatalogID: "catalog:fingerprint:discovery:ogc-wms", ProbeName: "OGC WMS", IntegrationArchetype: "ogc_records", ProbePatterns: []string{"service=WFS or service=WMS query parameters"}},
			observed:    []string{"https://geo.example.org/wms"},
			expectedURL: "https://geo.example.org/?request=GetCapabilities&service=WMS",
			wantSignal:  "service=wfs or service=wms query parameters",
		},
		{
			name:        "stac",
			probe:       FingerprintProbe{CatalogID: "catalog:fingerprint:discovery:stac", ProbeName: "STAC", IntegrationArchetype: "stac_api", ProbePatterns: []string{"/collections", "/search", "/stac", "/api/stac"}},
			observed:    []string{"https://imagery.example.org/earth"},
			expectedURL: "https://imagery.example.org/api/stac",
			wantSignal:  "/api/stac",
		},
		{
			name:        "robots",
			probe:       FingerprintProbe{CatalogID: "catalog:fingerprint:discovery:robots", ProbeName: "Robots Exclusion Protocol", IntegrationArchetype: "discovery_web", ProbePatterns: []string{"/robots.txt"}},
			observed:    []string{"https://www.example.com/newsroom"},
			expectedURL: "https://www.example.com/robots.txt",
			wantSignal:  "/robots.txt",
		},
		{
			name:        "sitemap",
			probe:       FingerprintProbe{CatalogID: "catalog:fingerprint:discovery:sitemaps", ProbeName: "Sitemaps / Sitemap Index", IntegrationArchetype: "discovery_web", ProbePatterns: []string{"/sitemap.xml", "/sitemap_index.xml"}},
			observed:    []string{"https://www.example.com/newsroom"},
			expectedURL: "https://www.example.com/sitemap.xml",
			wantSignal:  "/sitemap.xml",
		},
		{
			name:        "rss-atom",
			probe:       FingerprintProbe{CatalogID: "catalog:fingerprint:discovery:rss-atom", ProbeName: "RSS / Atom / GeoRSS", IntegrationArchetype: "discovery_web", ProbePatterns: []string{"<link rel='alternate' type='application/rss+xml'> or feed.xml/feed.atom"}},
			observed:    []string{"https://alerts.example.com/blog"},
			expectedURL: "https://alerts.example.com/atom.xml",
			wantSignal:  "<link rel='alternate' type='application/rss+xml'> or feed.xml/feed.atom",
		},
		{
			name:        "wayback",
			probe:       FingerprintProbe{CatalogID: "catalog:fingerprint:discovery:wayback-machine-cdx", ProbeName: "Wayback Machine / CDX", IntegrationArchetype: "discovery_web", ProbePatterns: []string{"archived URL enumeration for dead or changed sites"}},
			observed:    []string{"https://legacy.example.net/archive"},
			expectedURL: "https://web.archive.org/cdx/search/cdx?fl=timestamp%2Coriginal%2Cstatuscode%2Cmimetype&from=2000&limit=1000&output=json&url=legacy.example.net%2F%2A",
			wantSignal:  "archived url enumeration for dead or changed sites",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GenerateFingerprintCandidates(tt.probe, tt.observed, now)
			if len(got) == 0 {
				t.Fatalf("expected fingerprint candidates for %s", tt.name)
			}
			var matched *FingerprintCandidate
			for i := range got {
				if got[i].CandidateURL == tt.expectedURL {
					matched = &got[i]
					break
				}
			}
			if matched == nil {
				t.Fatalf("expected candidate %q, got %#v", tt.expectedURL, got)
			}
			if matched.DetectedPlatform != tt.probe.ProbeName {
				t.Fatalf("expected detected platform %q, got %q", tt.probe.ProbeName, matched.DetectedPlatform)
			}
			if matched.IntegrationArchetype != tt.probe.IntegrationArchetype {
				t.Fatalf("expected integration archetype %q, got %q", tt.probe.IntegrationArchetype, matched.IntegrationArchetype)
			}
			if matched.ReviewStatus != "review_required" {
				t.Fatalf("expected review_required candidates, got %q", matched.ReviewStatus)
			}
			if matched.ClassifierKind != "fingerprint_probe" {
				t.Fatalf("expected fingerprint classifier kind, got %q", matched.ClassifierKind)
			}
			if len(matched.ObservedFrom) == 0 || matched.ObservedFrom[0] == "" {
				t.Fatalf("expected observed-from metadata, got %#v", matched.ObservedFrom)
			}
			foundSignal := false
			for _, signal := range matched.ClassifierSignals {
				if signal == tt.wantSignal {
					foundSignal = true
					break
				}
			}
			if !foundSignal {
				t.Fatalf("expected classifier signals %#v to contain %q", matched.ClassifierSignals, tt.wantSignal)
			}
		})
	}
}
