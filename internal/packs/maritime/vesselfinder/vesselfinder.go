package vesselfinder

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"html"
	"math/rand"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	ErrorBotPage       = "vesselfinder_bot_page"
	ErrorMissingVessel = "vesselfinder_missing_vessel"
	ErrorInvalidDetail = "vesselfinder_invalid_detail"
)

type ParseError struct {
	Code    string
	Message string
}

func (e *ParseError) Error() string {
	if e == nil {
		return ""
	}
	return e.Code + ": " + e.Message
}

type Dimension struct {
	Code  string
	Label string
}

type Dimensions struct {
	Countries []Dimension
	Types     []Dimension
}

type VesselMetadata struct {
	DetailID   string
	DetailURL  string
	Name       string
	IMO        string
	MMSI       string
	CallSign   string
	Flag       string
	VesselType string
	Status     string
	Latitude   *float64
	Longitude  *float64
	SpeedKPH   *float64
	CourseDeg  *float64
	ObservedAt time.Time
	FetchedAt  time.Time
}

type FieldChange struct {
	Field    string
	OldValue string
	NewValue string
}

type PageJob struct {
	CountryCode string
	TypeCode    string
	Page        int
}

type Terminal404 struct {
	CountryCode string
	TypeCode    string
	Page        int
}

type ScanQueueItem struct {
	DetailURL     string
	CountryCode   string
	CountryLabel  string
	TypeCode      string
	TypeLabel     string
	PlaceID       string
	Status        string
	NextScanAt    time.Time
	LastScannedAt time.Time
	AttemptCount  int
	LeaseID       string
	LastErrorCode string
	StatusCode    int
}

type ScanResult struct {
	StatusCode int
	Success    bool
	ErrorCode  string
}

var (
	selectRE     = regexp.MustCompile(`(?is)<select\b([^>]*)>(.*?)</select>`)
	optionRE     = regexp.MustCompile(`(?is)<option\b([^>]*)>(.*?)</option>`)
	attrRE       = regexp.MustCompile(`(?is)\b([a-zA-Z0-9_:-]+)\s*=\s*["']([^"']*)["']`)
	hrefRE       = regexp.MustCompile(`(?is)\bhref\s*=\s*["']([^"']*/vessels/details/[0-9A-Za-z_-]+[^"']*)["']`)
	dtddRE       = regexp.MustCompile(`(?is)<dt\b[^>]*>(.*?)</dt>\s*<dd\b[^>]*>(.*?)</dd>`)
	h1RE         = regexp.MustCompile(`(?is)<h1\b[^>]*>(.*?)</h1>`)
	h2RE         = regexp.MustCompile(`(?is)<h2\b[^>]*>(.*?)</h2>`)
	titleRE      = regexp.MustCompile(`(?is)<title\b[^>]*>(.*?)</title>`)
	dataJSONRE   = regexp.MustCompile(`(?is)\bdata-json\s*=\s*(?:"([^"]*)"|'([^']*)')`)
	scriptVarRE  = regexp.MustCompile(`(?is)\b([A-Za-z_][A-Za-z0-9_]*)\s*=\s*([0-9]+)`)
	titleMMSIRE  = regexp.MustCompile(`(?i)\bMMSI\s*([0-9]{6,9})\b`)
	titleIMORE   = regexp.MustCompile(`(?i)\bIMO\s*([0-9]{7})\b`)
	dataLatLonRE = regexp.MustCompile(`(?is)data-lat\s*=\s*["']([-+]?\d+(?:\.\d+)?)["'][^>]*data-lon\s*=\s*["']([-+]?\d+(?:\.\d+)?)["']`)
	latLonTextRE = regexp.MustCompile(`(?is)(?:lat(?:itude)?)[^\d+-]*([-+]?\d+(?:\.\d+)?)[\s,;]+(?:lon(?:gitude)?)[^\d+-]*([-+]?\d+(?:\.\d+)?)`)
	detailIDRE   = regexp.MustCompile(`/vessels/details/([0-9A-Za-z_-]+)`)
	numberRE     = regexp.MustCompile(`[-+]?\d+(?:\.\d+)?`)
	tagRE        = regexp.MustCompile(`(?is)<[^>]+>`)
	spaceRE      = regexp.MustCompile(`\s+`)
)

func ExtractDimensions(body string) Dimensions {
	var dims Dimensions
	for _, selectMatch := range selectRE.FindAllStringSubmatch(body, -1) {
		attrs := parseAttrs(selectMatch[1])
		key := strings.ToLower(firstNonEmpty(attrs["id"], attrs["name"]))
		var target *[]Dimension
		switch {
		case key == "flag" || strings.Contains(key, "country") || strings.Contains(key, "ship-flag"):
			target = &dims.Countries
		case key == "type" || strings.Contains(key, "shiptype") || strings.Contains(key, "ship-type") || strings.Contains(key, "vesseltype") || strings.Contains(key, "vessel-type"):
			target = &dims.Types
		default:
			continue
		}
		for _, optionMatch := range optionRE.FindAllStringSubmatch(selectMatch[2], -1) {
			optionAttrs := parseAttrs(optionMatch[1])
			code := strings.TrimSpace(optionAttrs["value"])
			label := cleanText(optionMatch[2])
			lowerLabel := strings.ToLower(label)
			if code == "" || code == "0" || code == "-" || strings.EqualFold(label, "all") || strings.HasPrefix(lowerLabel, "all ") || strings.HasPrefix(lowerLabel, "any ") {
				continue
			}
			*target = append(*target, Dimension{Code: code, Label: label})
		}
	}
	sortDimensions(dims.Countries)
	sortDimensions(dims.Types)
	return dims
}

func ExtractDetailLinks(body, baseURL string) []string {
	seen := map[string]struct{}{}
	var links []string
	for _, match := range hrefRE.FindAllStringSubmatch(body, -1) {
		normalized := normalizeURL(match[1], baseURL)
		if normalized == "" {
			continue
		}
		if _, ok := seen[normalized]; ok {
			continue
		}
		seen[normalized] = struct{}{}
		links = append(links, normalized)
	}
	sort.Strings(links)
	return links
}

func ParseDetail(body, detailURL string, fetchedAt time.Time) (VesselMetadata, error) {
	if IsBotPage(body) {
		return VesselMetadata{}, &ParseError{Code: ErrorBotPage, Message: "vesselfinder returned a bot or captcha page"}
	}
	lower := strings.ToLower(body)
	if strings.Contains(lower, "vessel not found") || strings.Contains(lower, "ship not found") {
		return VesselMetadata{}, &ParseError{Code: ErrorMissingVessel, Message: "vesselfinder detail page reports missing vessel"}
	}

	fields := extractDefinitionFields(body)
	hints := extractCurrentPageHints(body)
	name := firstNonEmpty(cleanText(firstMatch(h1RE, body)), hints.Name, strings.TrimSuffix(cleanText(firstMatch(titleRE, body)), " Current Position - VesselFinder"))
	meta := VesselMetadata{
		DetailID:   detailID(detailURL, body),
		DetailURL:  strings.TrimSpace(detailURL),
		Name:       name,
		IMO:        firstNonEmpty(digitsOnly(firstField(fields, "imo")), hints.IMO),
		MMSI:       firstNonEmpty(digitsOnly(firstField(fields, "mmsi")), hints.MMSI),
		CallSign:   firstField(fields, "callsign", "call sign"),
		Flag:       firstField(fields, "flag"),
		VesselType: firstNonEmpty(firstField(fields, "vessel type", "type"), hints.VesselType),
		Status:     firstField(fields, "status", "navigation status"),
		FetchedAt:  fetchedAt.UTC(),
	}
	if meta.MMSI == "" {
		meta.MMSI = hints.JSONMMSI
	}
	if meta.DetailID == "" {
		meta.DetailID = firstNonEmpty(meta.IMO, meta.MMSI)
	}
	lat, lon, ok := extractCoordinates(body)
	if ok {
		meta.Latitude = &lat
		meta.Longitude = &lon
	}
	if speed, ok := parseSpeedKPH(firstField(fields, "speed")); ok {
		meta.SpeedKPH = &speed
	}
	if course, ok := parseNumber(firstField(fields, "course", "course / speed")); ok {
		meta.CourseDeg = &course
	}
	if hints.JSONPosition.Valid {
		meta.Latitude = &hints.JSONPosition.Latitude
		meta.Longitude = &hints.JSONPosition.Longitude
		if hints.JSONPosition.SpeedKPH != nil {
			meta.SpeedKPH = hints.JSONPosition.SpeedKPH
		}
		if hints.JSONPosition.CourseDeg != nil {
			meta.CourseDeg = hints.JSONPosition.CourseDeg
		}
	}
	if observed, ok := parseObservedAt(firstField(fields, "position received", "last report", "last received")); ok {
		meta.ObservedAt = observed
	} else {
		meta.ObservedAt = fetchedAt.UTC()
	}
	if meta.Name == "" && meta.IMO == "" && meta.MMSI == "" && meta.DetailID == "" {
		return VesselMetadata{}, &ParseError{Code: ErrorInvalidDetail, Message: "vesselfinder detail page did not contain vessel metadata"}
	}
	return meta, nil
}

func MetadataFingerprint(meta VesselMetadata) string {
	payload := map[string]any{
		"detail_id":   strings.TrimSpace(meta.DetailID),
		"name":        strings.TrimSpace(meta.Name),
		"imo":         strings.TrimSpace(meta.IMO),
		"mmsi":        strings.TrimSpace(meta.MMSI),
		"call_sign":   strings.TrimSpace(meta.CallSign),
		"flag":        strings.TrimSpace(meta.Flag),
		"vessel_type": strings.TrimSpace(meta.VesselType),
		"status":      strings.TrimSpace(meta.Status),
		"latitude":    meta.Latitude,
		"longitude":   meta.Longitude,
		"speed_kph":   meta.SpeedKPH,
		"course_deg":  meta.CourseDeg,
		"observed_at": meta.ObservedAt.UTC().Format(time.RFC3339),
	}
	b, _ := json.Marshal(payload)
	sum := md5.Sum(b)
	return hex.EncodeToString(sum[:])
}

func DiffFields(oldMeta, newMeta VesselMetadata) []FieldChange {
	fields := []struct {
		name string
		old  string
		new  string
	}{
		{"name", oldMeta.Name, newMeta.Name},
		{"imo", oldMeta.IMO, newMeta.IMO},
		{"mmsi", oldMeta.MMSI, newMeta.MMSI},
		{"call_sign", oldMeta.CallSign, newMeta.CallSign},
		{"flag", oldMeta.Flag, newMeta.Flag},
		{"vessel_type", oldMeta.VesselType, newMeta.VesselType},
		{"status", oldMeta.Status, newMeta.Status},
	}
	var changes []FieldChange
	for _, field := range fields {
		oldValue := strings.TrimSpace(field.old)
		newValue := strings.TrimSpace(field.new)
		if oldValue != newValue {
			changes = append(changes, FieldChange{Field: field.name, OldValue: oldValue, NewValue: newValue})
		}
	}
	return changes
}

func IsBotPageError(err error) bool {
	var parseErr *ParseError
	return errors.As(err, &parseErr) && parseErr.Code == ErrorBotPage
}

func IsMissingVesselError(err error) bool {
	var parseErr *ParseError
	return errors.As(err, &parseErr) && parseErr.Code == ErrorMissingVessel
}

func BuildPageJobs(countries, types []Dimension, maxPage int, seed int64) []PageJob {
	if maxPage <= 0 {
		return nil
	}
	var dimensions []PageJob
	for _, country := range countries {
		for _, vesselType := range types {
			dimensions = append(dimensions, PageJob{CountryCode: country.Code, TypeCode: vesselType.Code})
		}
	}
	rng := rand.New(rand.NewSource(seed))
	rng.Shuffle(len(dimensions), func(i, j int) { dimensions[i], dimensions[j] = dimensions[j], dimensions[i] })
	jobs := make([]PageJob, 0, len(dimensions)*maxPage)
	for _, dimension := range dimensions {
		for page := 1; page <= maxPage; page++ {
			jobs = append(jobs, PageJob{CountryCode: dimension.CountryCode, TypeCode: dimension.TypeCode, Page: page})
		}
	}
	return jobs
}

func ListPageOutcome(statusCode int, links []string) (string, bool) {
	if statusCode == 404 {
		return "terminal_404", true
	}
	if len(links) == 0 {
		return "empty", true
	}
	return "success", false
}

func ShouldSkipPage(job PageJob, terminals []Terminal404) bool {
	for _, terminal := range terminals {
		if job.CountryCode == terminal.CountryCode && job.TypeCode == terminal.TypeCode && job.Page > terminal.Page {
			return true
		}
	}
	return false
}

func ClaimOldest(queue []ScanQueueItem, now time.Time, leaseID string, limit int) ([]ScanQueueItem, []ScanQueueItem) {
	updated := append([]ScanQueueItem(nil), queue...)
	if limit <= 0 {
		return nil, updated
	}
	var indexes []int
	for idx, item := range updated {
		status := strings.TrimSpace(item.Status)
		if (status == "" || status == "pending" || status == "failed") && !item.NextScanAt.After(now) {
			indexes = append(indexes, idx)
		}
	}
	sort.SliceStable(indexes, func(i, j int) bool {
		return updated[indexes[i]].NextScanAt.Before(updated[indexes[j]].NextScanAt)
	})
	if len(indexes) > limit {
		indexes = indexes[:limit]
	}
	claimed := make([]ScanQueueItem, 0, len(indexes))
	for _, idx := range indexes {
		updated[idx].Status = "leased"
		updated[idx].LeaseID = leaseID
		claimed = append(claimed, updated[idx])
	}
	return claimed, updated
}

func ApplyScanResult(item ScanQueueItem, result ScanResult, now time.Time, successInterval time.Duration) ScanQueueItem {
	item.StatusCode = result.StatusCode
	item.LastScannedAt = now.UTC()
	item.LeaseID = ""
	if result.Success {
		item.Status = "pending"
		item.AttemptCount = 0
		item.LastErrorCode = ""
		item.NextScanAt = now.UTC().Add(successInterval)
		return item
	}
	item.Status = "failed"
	item.AttemptCount++
	item.LastErrorCode = strings.TrimSpace(result.ErrorCode)
	if item.LastErrorCode == "" {
		item.LastErrorCode = "scan_failed"
	}
	backoff := time.Duration(item.AttemptCount*item.AttemptCount) * time.Minute
	if backoff < time.Minute {
		backoff = time.Minute
	}
	if backoff > time.Hour {
		backoff = time.Hour
	}
	item.NextScanAt = now.UTC().Add(backoff)
	return item
}

func extractDefinitionFields(body string) map[string]string {
	fields := map[string]string{}
	for _, match := range dtddRE.FindAllStringSubmatch(body, -1) {
		key := strings.ToLower(cleanText(match[1]))
		value := cleanText(match[2])
		if key != "" && value != "" {
			fields[key] = value
		}
	}
	return fields
}

type currentPageHints struct {
	Name         string
	IMO          string
	MMSI         string
	JSONMMSI     string
	VesselType   string
	JSONPosition djsonPosition
}

type djsonPosition struct {
	Valid     bool
	Latitude  float64
	Longitude float64
	SpeedKPH  *float64
	CourseDeg *float64
}

func extractCurrentPageHints(body string) currentPageHints {
	title := cleanText(firstMatch(titleRE, body))
	subtitle := cleanText(firstMatch(h2RE, body))
	hints := currentPageHints{
		Name:       extractNameFromTitle(title),
		IMO:        firstNonEmpty(firstRegexGroup(titleIMORE, title), firstRegexGroup(titleIMORE, subtitle)),
		MMSI:       firstNonEmpty(firstRegexGroup(titleMMSIRE, title), firstRegexGroup(titleMMSIRE, subtitle)),
		VesselType: firstNonEmpty(extractVesselTypeFromTitle(title), extractVesselTypeFromSubtitle(subtitle)),
	}
	scriptVars := extractScriptVars(body)
	if hints.IMO == "" && scriptVars["vu_imo"] != "" && scriptVars["vu_imo"] != "0" {
		hints.IMO = scriptVars["vu_imo"]
	}
	if hints.MMSI == "" && scriptVars["mmsi"] != "" && scriptVars["mmsi"] != "0" {
		hints.MMSI = scriptVars["mmsi"]
	}
	djson := extractDJSONPosition(body)
	hints.JSONPosition = djson
	hints.JSONMMSI = djsonMMSI(body)
	return hints
}

func extractNameFromTitle(title string) string {
	title = strings.TrimSpace(strings.TrimSuffix(title, " - VesselFinder"))
	if idx := strings.Index(strings.ToLower(title), " - details"); idx >= 0 {
		title = title[:idx]
	}
	if idx := strings.Index(title, ","); idx >= 0 {
		title = title[:idx]
	}
	title = strings.TrimSuffix(title, " Current Position")
	return strings.TrimSpace(title)
}

func extractVesselTypeFromTitle(title string) string {
	idx := strings.Index(title, ",")
	if idx < 0 {
		return ""
	}
	value := strings.TrimSpace(title[idx+1:])
	if end := strings.Index(strings.ToLower(value), " - details"); end >= 0 {
		value = value[:end]
	}
	if strings.Contains(strings.ToLower(value), "mmsi") || strings.Contains(strings.ToLower(value), "imo") {
		return ""
	}
	return strings.TrimSpace(value)
}

func extractVesselTypeFromSubtitle(subtitle string) string {
	if subtitle == "" {
		return ""
	}
	value := subtitle
	if idx := strings.Index(value, ","); idx >= 0 {
		value = value[:idx]
	}
	if strings.Contains(strings.ToLower(value), "mmsi") || strings.Contains(strings.ToLower(value), "imo") {
		return ""
	}
	return strings.TrimSpace(value)
}

func firstRegexGroup(re *regexp.Regexp, value string) string {
	match := re.FindStringSubmatch(value)
	if len(match) < 2 {
		return ""
	}
	return digitsOnly(match[1])
}

func extractScriptVars(body string) map[string]string {
	vars := map[string]string{}
	for _, match := range scriptVarRE.FindAllStringSubmatch(body, -1) {
		if len(match) == 3 {
			vars[strings.ToLower(match[1])] = match[2]
		}
	}
	return vars
}

func djsonMMSI(body string) string {
	payload := extractDJSON(body)
	if payload == nil {
		return ""
	}
	if value, ok := numberFromMap(payload, "mmsi"); ok && value > 0 {
		return strconv.FormatInt(int64(value), 10)
	}
	return ""
}

func extractDJSONPosition(body string) djsonPosition {
	payload := extractDJSON(body)
	if payload == nil {
		return djsonPosition{}
	}
	lat, latOK := numberFromMap(payload, "ship_lat")
	lon, lonOK := numberFromMap(payload, "ship_lon")
	if !latOK || !lonOK || !validLatitude(lat) || !validLongitude(lon) {
		return djsonPosition{}
	}
	position := djsonPosition{Valid: true, Latitude: lat, Longitude: lon}
	if speedKnots, ok := numberFromMap(payload, "ship_sog"); ok && speedKnots >= 0 {
		speedKPH := speedKnots * 1.852
		position.SpeedKPH = &speedKPH
	}
	if course, ok := numberFromMap(payload, "ship_cog"); ok && course >= 0 && course < 360 {
		position.CourseDeg = &course
	}
	return position
}

func extractDJSON(body string) map[string]any {
	for _, match := range dataJSONRE.FindAllStringSubmatch(body, -1) {
		raw := firstNonEmpty(match[1], match[2])
		if raw == "" {
			continue
		}
		var payload map[string]any
		if err := json.Unmarshal([]byte(html.UnescapeString(raw)), &payload); err == nil {
			return payload
		}
	}
	return nil
}

func numberFromMap(payload map[string]any, key string) (float64, bool) {
	switch value := payload[key].(type) {
	case float64:
		return value, true
	case string:
		return parseNumber(value)
	default:
		return 0, false
	}
}

func firstField(fields map[string]string, keys ...string) string {
	for _, key := range keys {
		if value := strings.TrimSpace(fields[strings.ToLower(key)]); value != "" {
			return value
		}
	}
	return ""
}

func extractCoordinates(body string) (float64, float64, bool) {
	for _, re := range []*regexp.Regexp{dataLatLonRE, latLonTextRE} {
		match := re.FindStringSubmatch(body)
		if len(match) != 3 {
			continue
		}
		lat, latErr := strconv.ParseFloat(match[1], 64)
		lon, lonErr := strconv.ParseFloat(match[2], 64)
		if latErr == nil && lonErr == nil && validLatitude(lat) && validLongitude(lon) {
			return lat, lon, true
		}
	}
	return 0, 0, false
}

func validLatitude(value float64) bool {
	return value >= -90 && value <= 90
}

func validLongitude(value float64) bool {
	return value >= -180 && value <= 180
}

func parseSpeedKPH(value string) (float64, bool) {
	number, ok := parseNumber(value)
	if !ok {
		return 0, false
	}
	lower := strings.ToLower(value)
	if strings.Contains(lower, "kn") || strings.Contains(lower, "knot") {
		return number * 1.852, true
	}
	return number, true
}

func parseNumber(value string) (float64, bool) {
	match := numberRE.FindString(value)
	if match == "" {
		return 0, false
	}
	number, err := strconv.ParseFloat(match, 64)
	if err != nil {
		return 0, false
	}
	return number, true
}

func parseObservedAt(value string) (time.Time, bool) {
	cleaned := strings.TrimSpace(strings.TrimSuffix(value, " UTC"))
	for _, layout := range []string{"2006-01-02 15:04", "2006-01-02 15:04:05", time.RFC3339} {
		parsed, err := time.ParseInLocation(layout, cleaned, time.UTC)
		if err == nil {
			return parsed.UTC(), true
		}
	}
	return time.Time{}, false
}

func detailID(detailURL, body string) string {
	for _, value := range []string{detailURL, body} {
		if match := detailIDRE.FindStringSubmatch(value); len(match) == 2 {
			return match[1]
		}
	}
	return ""
}

func normalizeURL(rawValue, baseURL string) string {
	parsed, err := url.Parse(strings.TrimSpace(html.UnescapeString(rawValue)))
	if err != nil {
		return ""
	}
	if !parsed.IsAbs() {
		base, err := url.Parse(baseURL)
		if err != nil {
			base = &url.URL{Scheme: "https", Host: "www.vesselfinder.com"}
		}
		parsed = base.ResolveReference(parsed)
	}
	parsed.Fragment = ""
	parsed.RawQuery = ""
	return parsed.String()
}

func parseAttrs(raw string) map[string]string {
	attrs := map[string]string{}
	for _, match := range attrRE.FindAllStringSubmatch(raw, -1) {
		attrs[strings.ToLower(match[1])] = html.UnescapeString(match[2])
	}
	return attrs
}

func cleanText(raw string) string {
	text := tagRE.ReplaceAllString(raw, " ")
	text = html.UnescapeString(text)
	text = spaceRE.ReplaceAllString(text, " ")
	return strings.TrimSpace(text)
}

func firstMatch(re *regexp.Regexp, body string) string {
	match := re.FindStringSubmatch(body)
	if len(match) < 2 {
		return ""
	}
	return match[1]
}

func sortDimensions(dims []Dimension) {
	sort.Slice(dims, func(i, j int) bool {
		if dims[i].Code != dims[j].Code {
			return dims[i].Code < dims[j].Code
		}
		return dims[i].Label < dims[j].Label
	})
}

func digitsOnly(value string) string {
	var b strings.Builder
	for _, r := range value {
		if r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
