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
	ReviewStatus         string
	MaterializedSourceID string
	DiscoveredAt         time.Time
}

func GenerateFingerprintCandidates(probe FingerprintProbe, observedURLs []string, now time.Time) []FingerprintCandidate {
	seen := map[string]FingerprintCandidate{}
	patterns := normalizeProbePatterns(probe.ProbePatterns)
	for _, rawURL := range observedURLs {
		canonical, err := NormalizeURL(rawURL)
		if err != nil || canonical == "" {
			continue
		}
		if !matchesFingerprintPattern(canonical, patterns) {
			continue
		}
		parsed, err := url.Parse(canonical)
		if err != nil {
			continue
		}
		name := strings.TrimSpace(parsed.Hostname())
		if name == "" {
			name = canonical
		}
		seed := probe.CatalogID + "|" + canonical
		digest := sha256.Sum256([]byte(seed))
		candidateID := "candidate:" + hex.EncodeToString(digest[:])[:24]
		seen[candidateID] = FingerprintCandidate{
			CandidateID:          candidateID,
			CatalogID:            strings.TrimSpace(probe.CatalogID),
			CandidateName:        name,
			CandidateURL:         canonical,
			IntegrationArchetype: strings.TrimSpace(probe.IntegrationArchetype),
			DetectedPlatform:     strings.TrimSpace(probe.ProbeName),
			ReviewStatus:         "review_required",
			MaterializedSourceID: "",
			DiscoveredAt:         now.UTC(),
		}
	}
	results := make([]FingerprintCandidate, 0, len(seen))
	for _, candidate := range seen {
		results = append(results, candidate)
	}
	sort.Slice(results, func(i, j int) bool {
		if results[i].CandidateURL != results[j].CandidateURL {
			return results[i].CandidateURL < results[j].CandidateURL
		}
		return results[i].CandidateID < results[j].CandidateID
	})
	return results
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
