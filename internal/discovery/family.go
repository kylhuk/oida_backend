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
	TransportType        string
	ScopeLevels          []string
	Tags                 []string
	ChildSource          FamilyChildSourceTemplate
}

type FamilyChildSourceTemplate struct {
	TransportType        string
	IntegrationArchetype string
	FormatHint           string
	ParserID             string
	SourceClass          string
	RefreshStrategy      string
	CrawlStrategy        string
	ExpectedPlaceTypes   []string
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
	Geography    string
	AdminLevel   string
}

type GeneratedChildSource struct {
	SourceID             string
	Domain               string
	Entrypoints          []string
	TransportType        string
	IntegrationArchetype string
	FormatHint           string
	ParserID             string
	SourceClass          string
	RefreshStrategy      string
	CrawlStrategy        string
	Geography            string
	AdminLevel           string
	ExpectedPlaceTypes   []string
}

type FamilyCandidate struct {
	CandidateID          string
	CatalogID            string
	CandidateName        string
	CandidateURL         string
	IntegrationArchetype string
	DetectedPlatform     string
	Geography            string
	AdminLevel           string
	ChildSource          GeneratedChildSource
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
		childSource := buildGeneratedChildSource(template, geo, adminLevel, canonicalURL)
		results = append(results, FamilyCandidate{
			CandidateID:          candidateID,
			CatalogID:            strings.TrimSpace(template.CatalogID),
			CandidateName:        strings.TrimSpace(template.Name) + " - " + geo + " (" + adminLevel + ")",
			CandidateURL:         canonicalURL,
			IntegrationArchetype: strings.TrimSpace(template.IntegrationArchetype),
			DetectedPlatform:     strings.TrimSpace(template.Name),
			Geography:            geo,
			AdminLevel:           adminLevel,
			ChildSource:          childSource,
			ReviewStatus:         "review_required",
			MaterializedSourceID: childSource.SourceID,
			DiscoveredAt:         now.UTC(),
		})
	}
	return sortFamilyCandidates(results)
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
		geo, adminLevel := familyMemberScope(member, canonicalURL)
		if geo == "" || adminLevel == "" {
			continue
		}
		seed := strings.Join([]string{template.CatalogID, member.CatalogID, geo, adminLevel, canonicalURL}, "|")
		digest := sha256.Sum256([]byte(seed))
		candidateID := "candidate:" + hex.EncodeToString(digest[:])[:24]
		if _, ok := seen[candidateID]; ok {
			continue
		}
		seen[candidateID] = struct{}{}
		childSource := buildGeneratedChildSource(template, geo, adminLevel, canonicalURL)
		results = append(results, FamilyCandidate{
			CandidateID:          candidateID,
			CatalogID:            strings.TrimSpace(template.CatalogID),
			CandidateName:        strings.TrimSpace(member.Name) + " - " + geo + " (" + adminLevel + ")",
			CandidateURL:         canonicalURL,
			IntegrationArchetype: strings.TrimSpace(template.IntegrationArchetype),
			DetectedPlatform:     strings.TrimSpace(template.Name),
			Geography:            geo,
			AdminLevel:           adminLevel,
			ChildSource:          childSource,
			ReviewStatus:         "review_required",
			MaterializedSourceID: childSource.SourceID,
			DiscoveredAt:         now.UTC(),
		})
	}
	return sortFamilyCandidates(results)
}

func sortFamilyCandidates(results []FamilyCandidate) []FamilyCandidate {
	sort.Slice(results, func(i, j int) bool {
		if results[i].CandidateURL != results[j].CandidateURL {
			return results[i].CandidateURL < results[j].CandidateURL
		}
		return results[i].CandidateID < results[j].CandidateID
	})
	return results
}

func buildGeneratedChildSource(template FamilyTemplate, geography, adminLevel, canonicalURL string) GeneratedChildSource {
	host := ""
	if parsed, err := url.Parse(strings.TrimSpace(canonicalURL)); err == nil {
		host = strings.ToLower(strings.TrimSpace(parsed.Hostname()))
	}
	seed := strings.Join([]string{strings.TrimSpace(template.CatalogID), strings.TrimSpace(geography), strings.TrimSpace(adminLevel), strings.TrimSpace(canonicalURL)}, "|")
	digest := sha256.Sum256([]byte(seed))
	return GeneratedChildSource{
		SourceID:             "generated:family:" + hex.EncodeToString(digest[:])[:24],
		Domain:               host,
		Entrypoints:          []string{strings.TrimSpace(canonicalURL)},
		TransportType:        firstNonEmpty(strings.TrimSpace(template.ChildSource.TransportType), strings.TrimSpace(template.TransportType), "http"),
		IntegrationArchetype: firstNonEmpty(strings.TrimSpace(template.ChildSource.IntegrationArchetype), strings.TrimSpace(template.IntegrationArchetype), "discovery_web"),
		FormatHint:           strings.TrimSpace(template.ChildSource.FormatHint),
		ParserID:             strings.TrimSpace(template.ChildSource.ParserID),
		SourceClass:          firstNonEmpty(strings.TrimSpace(template.ChildSource.SourceClass), "family_generated"),
		RefreshStrategy:      firstNonEmpty(strings.TrimSpace(template.ChildSource.RefreshStrategy), "scheduled"),
		CrawlStrategy:        firstNonEmpty(strings.TrimSpace(template.ChildSource.CrawlStrategy), "delta"),
		Geography:            strings.TrimSpace(geography),
		AdminLevel:           strings.TrimSpace(adminLevel),
		ExpectedPlaceTypes:   familyExpectedPlaceTypes(template, adminLevel),
	}
}

func familyExpectedPlaceTypes(template FamilyTemplate, adminLevel string) []string {
	if len(template.ChildSource.ExpectedPlaceTypes) > 0 {
		return append([]string(nil), template.ChildSource.ExpectedPlaceTypes...)
	}
	if trimmed := strings.TrimSpace(adminLevel); trimmed != "" {
		return []string{trimmed}
	}
	return append([]string(nil), template.ScopeLevels...)
}

func familyMemberScope(member FamilyMember, canonicalURL string) (string, string) {
	geography := normalizeFamilyToken(firstNonEmpty(member.Geography, InferGeographyFromScope(member.Scope), inferGeographyFromURL(canonicalURL), member.Name))
	adminLevel := strings.TrimSpace(member.AdminLevel)
	if adminLevel == "" {
		adminLevel = InferAdminLevelFromScope(member.Scope)
	}
	return geography, adminLevel
}

func InferAdminLevelFromScope(scope string) string {
	lower := strings.ToLower(strings.TrimSpace(scope))
	switch {
	case strings.Contains(lower, "global"):
		return "global"
	case strings.Contains(lower, "subnational") || strings.Contains(lower, "state") || strings.Contains(lower, "province") || strings.Contains(lower, "municipal") || strings.Contains(lower, "city") || strings.Contains(lower, "local"):
		return "admin1"
	case strings.Contains(lower, "national") || strings.Contains(lower, "country"):
		return "admin0"
	default:
		return "admin0"
	}
}

func InferGeographyFromScope(scope string) string {
	trimmed := strings.TrimSpace(scope)
	if trimmed == "" {
		return ""
	}
	replacer := strings.NewReplacer(
		"(", " ",
		")", " ",
		"/", " ",
		",", " ",
	)
	cleaned := strings.ToLower(replacer.Replace(trimmed))
	for _, token := range []string{"national", "subnational", "global", "relevance", "state", "province", "municipal", "local", "country"} {
		cleaned = strings.ReplaceAll(cleaned, token, " ")
	}
	fields := strings.Fields(cleaned)
	if len(fields) == 0 {
		return ""
	}
	return strings.Join(fields, "-")
}

func inferGeographyFromURL(rawURL string) string {
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return ""
	}
	host := strings.TrimSpace(parsed.Hostname())
	if host == "" {
		return ""
	}
	parts := strings.Split(host, ".")
	if len(parts) >= 2 {
		candidate := parts[len(parts)-1]
		if len(candidate) == 2 {
			return strings.ToLower(candidate)
		}
		return normalizeFamilyToken(parts[len(parts)-2])
	}
	return normalizeFamilyToken(host)
}

func normalizeFamilyToken(value string) string {
	trimmed := strings.ToLower(strings.TrimSpace(value))
	if trimmed == "" {
		return ""
	}
	var b strings.Builder
	lastDash := false
	for _, r := range trimmed {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			lastDash = false
			continue
		}
		if !lastDash {
			b.WriteRune('-')
			lastDash = true
		}
	}
	return strings.Trim(b.String(), "-")
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
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
