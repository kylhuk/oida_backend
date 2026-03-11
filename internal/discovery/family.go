package discovery

import (
	"crypto/sha256"
	"encoding/hex"
	"net/url"
	"sort"
	"strings"
	"time"
)

type FamilyTemplate struct {
	CatalogID            string
	Name                 string
	Scope                string
	Outputs              string
	IntegrationArchetype string
	Tags                 []string
}

type FamilyScope struct {
	Geography  string
	AdminLevel string
	BaseURL    string
}

type FamilyMember struct {
	CatalogID    string
	Name         string
	Scope        string
	Tags         []string
	CandidateURL string
}

type FamilyCandidate struct {
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

func GenerateFamilyCandidates(template FamilyTemplate, scopes []FamilyScope, now time.Time) []FamilyCandidate {
	results := make([]FamilyCandidate, 0, len(scopes))
	seen := map[string]struct{}{}
	for _, scope := range scopes {
		geo := strings.TrimSpace(scope.Geography)
		adminLevel := strings.TrimSpace(scope.AdminLevel)
		baseURL := strings.TrimSpace(scope.BaseURL)
		if geo == "" || adminLevel == "" || baseURL == "" {
			continue
		}
		canonicalURL, err := NormalizeURL(baseURL)
		if err != nil {
			continue
		}
		seed := strings.Join([]string{template.CatalogID, geo, adminLevel, canonicalURL}, "|")
		digest := sha256.Sum256([]byte(seed))
		candidateID := "candidate:" + hex.EncodeToString(digest[:])[:24]
		if _, ok := seen[candidateID]; ok {
			continue
		}
		seen[candidateID] = struct{}{}
		results = append(results, FamilyCandidate{
			CandidateID:          candidateID,
			CatalogID:            strings.TrimSpace(template.CatalogID),
			CandidateName:        strings.TrimSpace(template.Name) + " - " + geo + " (" + adminLevel + ")",
			CandidateURL:         canonicalURL,
			IntegrationArchetype: strings.TrimSpace(template.IntegrationArchetype),
			DetectedPlatform:     strings.TrimSpace(template.Name),
			ReviewStatus:         "review_required",
			MaterializedSourceID: "",
			DiscoveredAt:         now.UTC(),
		})
	}
	sort.Slice(results, func(i, j int) bool {
		if results[i].CandidateURL != results[j].CandidateURL {
			return results[i].CandidateURL < results[j].CandidateURL
		}
		return results[i].CandidateID < results[j].CandidateID
	})
	return results
}

func GenerateFamilyCandidatesFromMembers(template FamilyTemplate, members []FamilyMember, now time.Time) []FamilyCandidate {
	templateTags := toTagSet(template.Tags)
	results := make([]FamilyCandidate, 0, len(members))
	seen := map[string]struct{}{}
	for _, member := range members {
		canonicalURL, err := NormalizeURL(member.CandidateURL)
		if err != nil {
			continue
		}
		if !matchesFamilyTemplate(template, member, templateTags) {
			continue
		}
		seed := strings.Join([]string{template.CatalogID, member.CatalogID, canonicalURL}, "|")
		digest := sha256.Sum256([]byte(seed))
		candidateID := "candidate:" + hex.EncodeToString(digest[:])[:24]
		if _, ok := seen[candidateID]; ok {
			continue
		}
		seen[candidateID] = struct{}{}
		results = append(results, FamilyCandidate{
			CandidateID:          candidateID,
			CatalogID:            strings.TrimSpace(template.CatalogID),
			CandidateName:        strings.TrimSpace(member.Name) + " - " + strings.TrimSpace(member.Scope),
			CandidateURL:         canonicalURL,
			IntegrationArchetype: strings.TrimSpace(template.IntegrationArchetype),
			DetectedPlatform:     strings.TrimSpace(template.Name),
			ReviewStatus:         "review_required",
			MaterializedSourceID: "",
			DiscoveredAt:         now.UTC(),
		})
	}
	sort.Slice(results, func(i, j int) bool {
		if results[i].CandidateURL != results[j].CandidateURL {
			return results[i].CandidateURL < results[j].CandidateURL
		}
		return results[i].CandidateID < results[j].CandidateID
	})
	return results
}

func matchesFamilyTemplate(template FamilyTemplate, member FamilyMember, templateTags map[string]struct{}) bool {
	if strings.TrimSpace(member.CandidateURL) == "" {
		return false
	}
	if _, err := url.Parse(strings.TrimSpace(member.CandidateURL)); err != nil {
		return false
	}
	memberTags := toTagSet(member.Tags)
	shared := 0
	for tag := range templateTags {
		if _, ok := memberTags[tag]; ok {
			shared++
		}
	}
	if shared == 0 {
		return false
	}
	templateScope := strings.ToLower(strings.TrimSpace(template.Scope))
	memberScope := strings.ToLower(strings.TrimSpace(member.Scope))
	if templateScope == "" || memberScope == "" {
		return shared > 0
	}
	for _, token := range []string{"national", "subnational", "global"} {
		if strings.Contains(templateScope, token) && strings.Contains(memberScope, token) {
			return true
		}
	}
	return shared >= 2
}

func toTagSet(tags []string) map[string]struct{} {
	result := make(map[string]struct{}, len(tags))
	for _, tag := range tags {
		trimmed := strings.ToLower(strings.TrimSpace(tag))
		if trimmed == "" {
			continue
		}
		result[trimmed] = struct{}{}
	}
	return result
}
