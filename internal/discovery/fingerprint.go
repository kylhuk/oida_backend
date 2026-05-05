package discovery

import (
	"crypto/sha256"
	"encoding/hex"
	"net/url"
	"sort"
	"strings"
	"time"
)

type FingerprintProbe struct {
	CatalogID            string
	ProbeName            string
	IntegrationArchetype string
	ProbePatterns        []string
}

type FingerprintCandidate struct {
	CandidateID          string
	CatalogID            string
	CandidateName        string
	CandidateURL         string
	IntegrationArchetype string
	DetectedPlatform     string
	ClassifierKind       string
	ClassifierSignals    []string
	ObservedFrom         []string
	ReviewStatus         string
	MaterializedSourceID string
	DiscoveredAt         time.Time
}

func GenerateFingerprintCandidates(probe FingerprintProbe, observedURLs []string, now time.Time) []FingerprintCandidate {
	type aggregatedCandidate struct {
		candidate FingerprintCandidate
		signals   map[string]struct{}
		observed  map[string]struct{}
	}

	seen := map[string]*aggregatedCandidate{}
	patterns := normalizeProbePatterns(probe.ProbePatterns)
	for _, rawURL := range observedURLs {
		canonical, err := NormalizeURL(rawURL)
		if err != nil || canonical == "" {
			continue
		}
		parsed, err := url.Parse(canonical)
		if err != nil {
			continue
		}
		for _, match := range fingerprintMatches(canonical, parsed, patterns) {
			seed := probe.CatalogID + "|" + match.CandidateURL
			digest := sha256.Sum256([]byte(seed))
			candidateID := "candidate:" + hex.EncodeToString(digest[:])[:24]
			agg, ok := seen[candidateID]
			if !ok {
				name := strings.TrimSpace(match.CandidateURL)
				if candidateParsed, err := url.Parse(match.CandidateURL); err == nil && strings.TrimSpace(candidateParsed.Hostname()) != "" {
					name = strings.TrimSpace(candidateParsed.Hostname())
				}
				agg = &aggregatedCandidate{
					candidate: FingerprintCandidate{
						CandidateID:          candidateID,
						CatalogID:            strings.TrimSpace(probe.CatalogID),
						CandidateName:        name,
						CandidateURL:         match.CandidateURL,
						IntegrationArchetype: strings.TrimSpace(probe.IntegrationArchetype),
						DetectedPlatform:     strings.TrimSpace(probe.ProbeName),
						ClassifierKind:       "fingerprint_probe",
						ReviewStatus:         "review_required",
						MaterializedSourceID: "",
						DiscoveredAt:         now.UTC(),
					},
					signals:  map[string]struct{}{},
					observed: map[string]struct{}{},
				}
				seen[candidateID] = agg
			}
			for _, signal := range match.Signals {
				trimmed := strings.TrimSpace(signal)
				if trimmed == "" {
					continue
				}
				agg.signals[trimmed] = struct{}{}
			}
			agg.observed[canonical] = struct{}{}
		}
	}

	results := make([]FingerprintCandidate, 0, len(seen))
	for _, agg := range seen {
		agg.candidate.ClassifierSignals = sortedKeys(agg.signals)
		agg.candidate.ObservedFrom = sortedKeys(agg.observed)
		results = append(results, agg.candidate)
	}
	sort.Slice(results, func(i, j int) bool {
		if results[i].CandidateURL != results[j].CandidateURL {
			return results[i].CandidateURL < results[j].CandidateURL
		}
		return results[i].CandidateID < results[j].CandidateID
	})
	return results
}

type fingerprintMatch struct {
	CandidateURL string
	Signals      []string
}

func fingerprintMatches(canonicalURL string, parsed *url.URL, patterns []string) []fingerprintMatch {
	results := map[string]map[string]struct{}{}
	add := func(candidateURL string, signals ...string) {
		candidateURL = strings.TrimSpace(candidateURL)
		if candidateURL == "" {
			return
		}
		canonicalCandidate, err := NormalizeURL(candidateURL)
		if err != nil || canonicalCandidate == "" {
			return
		}
		if _, ok := results[canonicalCandidate]; !ok {
			results[canonicalCandidate] = map[string]struct{}{}
		}
		for _, signal := range signals {
			trimmed := strings.TrimSpace(signal)
			if trimmed == "" {
				continue
			}
			results[canonicalCandidate][trimmed] = struct{}{}
		}
	}

	root := parsed.Scheme + "://" + parsed.Host
	if matchesFingerprintPattern(canonicalURL, patterns) {
		add(canonicalURL, "observed_url_match")
	}
	for _, pattern := range patterns {
		literal, derived := fingerprintPatternCandidates(root, pattern)
		for _, candidateURL := range derived {
			add(candidateURL, literal)
		}
	}

	matches := make([]fingerprintMatch, 0, len(results))
	for candidateURL, signalSet := range results {
		matches = append(matches, fingerprintMatch{CandidateURL: candidateURL, Signals: sortedKeys(signalSet)})
	}
	sort.Slice(matches, func(i, j int) bool {
		if matches[i].CandidateURL != matches[j].CandidateURL {
			return matches[i].CandidateURL < matches[j].CandidateURL
		}
		return strings.Join(matches[i].Signals, "\n") < strings.Join(matches[j].Signals, "\n")
	})
	return matches
}

func fingerprintPatternCandidates(root string, pattern string) (string, []string) {
	pattern = strings.ToLower(strings.TrimSpace(pattern))
	if pattern == "" {
		return "", nil
	}
	literal := pattern
	switch {
	case strings.HasPrefix(pattern, "/api/3/action/package_search"):
		return literal, []string{root + "/api/3/action/package_search?rows=1"}
	case strings.HasPrefix(pattern, "/api/3/action/package_show"):
		return literal, []string{root + "/api/3/action/package_show?id=sample"}
	case strings.HasPrefix(pattern, "/api/views"):
		return literal, []string{root + "/api/views"}
	case strings.HasPrefix(pattern, "/resource/<id>.json"):
		return literal, []string{root + "/resource/sample.json"}
	case strings.HasPrefix(pattern, "/api/search/v1"):
		return literal, []string{root + "/api/search/v1"}
	case strings.HasPrefix(pattern, "/arcgis/rest/services"):
		return literal, []string{root + "/arcgis/rest/services"}
	case strings.HasPrefix(pattern, "/featureserver"):
		return literal, []string{root + "/FeatureServer"}
	case strings.HasPrefix(pattern, "/mapserver"):
		return literal, []string{root + "/MapServer"}
	case strings.HasPrefix(pattern, "/imageserver"):
		return literal, []string{root + "/ImageServer"}
	case strings.HasPrefix(pattern, "/api/explore/v2.1/catalog/datasets"):
		return literal, []string{root + "/api/explore/v2.1/catalog/datasets"}
	case strings.HasPrefix(pattern, "/geonetwork/srv/api/records"):
		return literal, []string{root + "/geonetwork/srv/api/records"}
	case strings.HasPrefix(pattern, "/api/v2/"):
		return literal, []string{root + "/api/v2/datasets"}
	case strings.HasPrefix(pattern, "/catalogue"):
		return literal, []string{root + "/catalogue"}
	case strings.HasPrefix(pattern, "/layers"):
		return literal, []string{root + "/layers"}
	case strings.HasPrefix(pattern, "/documents"):
		return literal, []string{root + "/documents"}
	case strings.HasPrefix(pattern, "/collections/{id}/items"):
		return literal, []string{root + "/collections"}
	case strings.Contains(pattern, "collections or /records depending on implementation"):
		return literal, []string{root + "/collections", root + "/records"}
	case strings.HasPrefix(pattern, "/collections"):
		return literal, []string{root + "/collections"}
	case strings.HasPrefix(pattern, "/search"):
		return literal, []string{root + "/search"}
	case strings.HasPrefix(pattern, "/api/stac"):
		return literal, []string{root + "/api/stac"}
	case strings.HasPrefix(pattern, "/stac"):
		return literal, []string{root + "/stac"}
	case strings.HasPrefix(pattern, "/robots.txt"):
		return literal, []string{root + "/robots.txt"}
	case strings.HasPrefix(pattern, "/sitemap.xml"):
		return literal, []string{root + "/sitemap.xml"}
	case strings.HasPrefix(pattern, "/sitemap_index.xml"):
		return literal, []string{root + "/sitemap_index.xml"}
	case strings.Contains(pattern, "dataset pages with /dataset/"):
		return literal, []string{root + "/dataset/"}
	case strings.Contains(pattern, "csv/json download links"):
		return literal, []string{root + "/api/views"}
	case strings.Contains(pattern, "hub site catalogs and datasets"):
		return literal, []string{root + "/api/search/v1"}
	case strings.Contains(pattern, "dataset pages with records/explore"):
		return literal, []string{root + "/api/explore/v2.1/catalog/datasets"}
	case pattern == "csw":
		return literal, []string{root + "/csw"}
	case strings.Contains(pattern, "service=wfs or service=wms query parameters"):
		return literal, []string{root + "/?request=GetCapabilities&service=WFS", root + "/?request=GetCapabilities&service=WMS"}
	case strings.Contains(pattern, "service=wfs"):
		return literal, []string{root + "/?request=GetCapabilities&service=WFS"}
	case strings.Contains(pattern, "service=wms"):
		return literal, []string{root + "/?request=GetCapabilities&service=WMS"}
	case strings.Contains(pattern, "<link rel='alternate' type='application/rss+xml'> or feed.xml/feed.atom"):
		return literal, []string{root + "/feed.xml", root + "/feed.atom", root + "/rss.xml", root + "/atom.xml"}
	case strings.Contains(pattern, "archived url enumeration for dead or changed sites"):
		host := strings.TrimPrefix(root, "https://")
		host = strings.TrimPrefix(host, "http://")
		if host == "" {
			return literal, nil
		}
		archiveURL := "https://web.archive.org/cdx/search/cdx?fl=timestamp,original,statuscode,mimetype&from=2000&limit=1000&output=json&url=" + url.QueryEscape(host+"/*")
		return literal, []string{archiveURL}
	}
	return literal, nil
}

func matchesFingerprintPattern(canonicalURL string, patterns []string) bool {
	if len(patterns) == 0 {
		return false
	}
	lowerURL := strings.ToLower(strings.TrimSpace(canonicalURL))
	for _, pattern := range patterns {
		if pattern == "" {
			continue
		}
		if strings.Contains(lowerURL, pattern) {
			return true
		}
	}
	return false
}

func normalizeProbePatterns(patterns []string) []string {
	result := make([]string, 0, len(patterns))
	for _, pattern := range patterns {
		trimmed := strings.ToLower(strings.TrimSpace(pattern))
		if trimmed == "" {
			continue
		}
		result = append(result, trimmed)
	}
	return result
}

func sortedKeys(values map[string]struct{}) []string {
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}
