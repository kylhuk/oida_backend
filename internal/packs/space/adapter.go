package space

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"global-osint-backend/internal/canonical"
)

func ParseTLEFeed(feed []byte) ([]ElementSet, error) {
	lines := normalizeFeedLines(string(feed))
	sets := make([]ElementSet, 0, len(lines)/2)
	for i := 0; i < len(lines); {
		name := ""
		if !strings.HasPrefix(lines[i], "1 ") {
			name = strings.TrimSpace(strings.TrimPrefix(lines[i], "0 "))
			i++
			if i >= len(lines) {
				return nil, fmt.Errorf("truncated TLE feed after name line %q", name)
			}
		}
		if i+1 >= len(lines) {
			return nil, fmt.Errorf("truncated TLE pair at line %d", i+1)
		}
		line1 := lines[i]
		line2 := lines[i+1]
		if !strings.HasPrefix(line1, "1 ") || !strings.HasPrefix(line2, "2 ") {
			return nil, fmt.Errorf("invalid TLE pair starting at line %d", i+1)
		}
		set, err := parseTLEPair(name, line1, line2)
		if err != nil {
			return nil, err
		}
		sets = append(sets, set)
		i += 2
	}
	return sets, nil
}

func ParseOMMFeed(feed []byte) ([]ElementSet, error) {
	var payload any
	if err := json.Unmarshal(feed, &payload); err != nil {
		return nil, fmt.Errorf("decode OMM JSON: %w", err)
	}
	records, err := extractOMMRecords(payload)
	if err != nil {
		return nil, err
	}
	sets := make([]ElementSet, 0, len(records))
	for idx, record := range records {
		set, err := parseOMMRecord(record)
		if err != nil {
			return nil, fmt.Errorf("record %d: %w", idx+1, err)
		}
		sets = append(sets, set)
	}
	return sets, nil
}

func normalizeFeedLines(feed string) []string {
	rawLines := strings.Split(strings.ReplaceAll(feed, "\r\n", "\n"), "\n")
	lines := make([]string, 0, len(rawLines))
	for _, line := range rawLines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		lines = append(lines, line)
	}
	return lines
}

func parseTLEPair(name, line1, line2 string) (ElementSet, error) {
	line1 = padRight(line1, 69)
	line2 = padRight(line2, 69)
	noradID := strings.TrimSpace(field(line1, 2, 7))
	epoch, err := parseTLEEpoch(field(line1, 18, 20), field(line1, 20, 32))
	if err != nil {
		return ElementSet{}, fmt.Errorf("parse TLE epoch for %s: %w", noradID, err)
	}
	inclination, err := strconv.ParseFloat(strings.TrimSpace(field(line2, 8, 16)), 64)
	if err != nil {
		return ElementSet{}, fmt.Errorf("parse inclination for %s: %w", noradID, err)
	}
	raan, err := strconv.ParseFloat(strings.TrimSpace(field(line2, 17, 25)), 64)
	if err != nil {
		return ElementSet{}, fmt.Errorf("parse RAAN for %s: %w", noradID, err)
	}
	eccentricity, err := strconv.ParseFloat("0."+strings.TrimSpace(field(line2, 26, 33)), 64)
	if err != nil {
		return ElementSet{}, fmt.Errorf("parse eccentricity for %s: %w", noradID, err)
	}
	argPerigee, err := strconv.ParseFloat(strings.TrimSpace(field(line2, 34, 42)), 64)
	if err != nil {
		return ElementSet{}, fmt.Errorf("parse arg_perigee for %s: %w", noradID, err)
	}
	meanAnomaly, err := strconv.ParseFloat(strings.TrimSpace(field(line2, 43, 51)), 64)
	if err != nil {
		return ElementSet{}, fmt.Errorf("parse mean_anomaly for %s: %w", noradID, err)
	}
	meanMotion, err := strconv.ParseFloat(strings.TrimSpace(field(line2, 52, 63)), 64)
	if err != nil {
		return ElementSet{}, fmt.Errorf("parse mean_motion for %s: %w", noradID, err)
	}
	bStar, err := parseTLEExponent(field(line1, 53, 61))
	if err != nil {
		return ElementSet{}, fmt.Errorf("parse BSTAR for %s: %w", noradID, err)
	}
	revNumber, _ := strconv.ParseUint(strings.TrimSpace(field(line2, 63, 68)), 10, 64)
	set := ElementSet{
		SourceFormat:            "tle",
		ObjectName:              strings.TrimSpace(name),
		NORADID:                 noradID,
		InternationalDesignator: strings.TrimSpace(field(line1, 9, 17)),
		Classification:          strings.TrimSpace(field(line1, 7, 8)),
		Epoch:                   epoch,
		InclinationDeg:          inclination,
		RAANDeg:                 raan,
		Eccentricity:            eccentricity,
		ArgPerigeeDeg:           argPerigee,
		MeanAnomalyDeg:          meanAnomaly,
		MeanMotionRevPerDay:     meanMotion,
		BStar:                   bStar,
		RevNumber:               revNumber,
		Evidence: []canonical.Evidence{{
			Kind:  "tle",
			Ref:   "tle:" + noradID,
			Value: strings.Join([]string{strings.TrimSpace(name), strings.TrimSpace(line1), strings.TrimSpace(line2)}, " | "),
			Attrs: map[string]any{
				"line1": strings.TrimSpace(line1),
				"line2": strings.TrimSpace(line2),
			},
		}},
	}
	if err := set.Validate(); err != nil {
		return ElementSet{}, err
	}
	return set, nil
}

func parseOMMRecord(record map[string]any) (ElementSet, error) {
	noradID := stringValue(record, "NORAD_CAT_ID")
	epoch, err := parseOMMTime(stringValue(record, "EPOCH"))
	if err != nil {
		return ElementSet{}, fmt.Errorf("parse epoch for %s: %w", noradID, err)
	}
	set := ElementSet{
		SourceFormat:            "omm",
		ObjectName:              stringValue(record, "OBJECT_NAME"),
		NORADID:                 noradID,
		InternationalDesignator: stringValue(record, "OBJECT_ID"),
		Classification:          stringValue(record, "CLASSIFICATION_TYPE"),
		Epoch:                   epoch,
		InclinationDeg:          floatValue(record, "INCLINATION"),
		RAANDeg:                 floatValue(record, "RA_OF_ASC_NODE"),
		Eccentricity:            floatValue(record, "ECCENTRICITY"),
		ArgPerigeeDeg:           floatValue(record, "ARG_OF_PERICENTER"),
		MeanAnomalyDeg:          floatValue(record, "MEAN_ANOMALY"),
		MeanMotionRevPerDay:     floatValue(record, "MEAN_MOTION"),
		BStar:                   floatValue(record, "BSTAR"),
		Evidence: []canonical.Evidence{{
			Kind:  "omm",
			Ref:   "omm:" + noradID,
			Value: epoch.Format(time.RFC3339),
			Attrs: map[string]any{"object_name": stringValue(record, "OBJECT_NAME")},
		}},
	}
	if err := set.Validate(); err != nil {
		return ElementSet{}, err
	}
	return set, nil
}

func extractOMMRecords(payload any) ([]map[string]any, error) {
	switch value := payload.(type) {
	case []any:
		return anySliceToMaps(value)
	case map[string]any:
		for _, key := range []string{"omm", "OMM", "data", "objects", "items"} {
			if nested, ok := value[key]; ok {
				if list, ok := nested.([]any); ok {
					return anySliceToMaps(list)
				}
			}
		}
		if _, ok := value["OBJECT_NAME"]; ok {
			return []map[string]any{value}, nil
		}
	}
	return nil, fmt.Errorf("unsupported OMM JSON shape")
}

func anySliceToMaps(list []any) ([]map[string]any, error) {
	rows := make([]map[string]any, 0, len(list))
	for _, item := range list {
		row, ok := item.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("OMM record is not an object")
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func parseTLEEpoch(yearField, dayField string) (time.Time, error) {
	yearValue, err := strconv.Atoi(strings.TrimSpace(yearField))
	if err != nil {
		return time.Time{}, err
	}
	dayValue, err := strconv.ParseFloat(strings.TrimSpace(dayField), 64)
	if err != nil {
		return time.Time{}, err
	}
	year := 1900 + yearValue
	if yearValue < 57 {
		year = 2000 + yearValue
	}
	day := int(math.Floor(dayValue))
	frac := dayValue - float64(day)
	start := time.Date(year, time.January, 1, 0, 0, 0, 0, time.UTC)
	seconds := frac * 86400
	return start.AddDate(0, 0, day-1).Add(time.Duration(seconds * float64(time.Second))), nil
}

func parseOMMTime(value string) (time.Time, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, fmt.Errorf("empty epoch")
	}
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02 15:04:05", "2006-01-02T15:04:05"} {
		if ts, err := time.Parse(layout, value); err == nil {
			return ts.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("unsupported epoch format %q", value)
}

func parseTLEExponent(value string) (float64, error) {
	value = strings.TrimSpace(value)
	if value == "" || value == "0" {
		return 0, nil
	}
	sign := ""
	if strings.HasPrefix(value, "-") || strings.HasPrefix(value, "+") {
		sign = value[:1]
		value = value[1:]
	}
	if len(value) < 2 {
		return 0, fmt.Errorf("invalid TLE exponent %q", value)
	}
	mantissa := strings.TrimSpace(value[:len(value)-2])
	exponent := strings.TrimSpace(value[len(value)-2:])
	parsed, err := strconv.ParseFloat(sign+"0."+mantissa+"e"+exponent, 64)
	if err != nil {
		return 0, err
	}
	return parsed, nil
}

func field(line string, start, end int) string {
	if start >= len(line) {
		return ""
	}
	if end > len(line) {
		end = len(line)
	}
	return line[start:end]
}

func padRight(value string, width int) string {
	if len(value) >= width {
		return value
	}
	return value + strings.Repeat(" ", width-len(value))
}

func stringValue(record map[string]any, key string) string {
	value, ok := record[key]
	if !ok || value == nil {
		return ""
	}
	switch v := value.(type) {
	case string:
		return strings.TrimSpace(v)
	case float64:
		if math.Trunc(v) == v {
			return strconv.FormatInt(int64(v), 10)
		}
		return strconv.FormatFloat(v, 'f', -1, 64)
	default:
		return strings.TrimSpace(fmt.Sprint(v))
	}
}

func floatValue(record map[string]any, key string) float64 {
	value, ok := record[key]
	if !ok || value == nil {
		return 0
	}
	switch v := value.(type) {
	case float64:
		return v
	case string:
		parsed, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
		if err == nil {
			return parsed
		}
	}
	return 0
}
