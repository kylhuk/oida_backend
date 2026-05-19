package unlocode

import (
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// Port holds the essential fields from a UN/LOCODE port entry.
type Port struct {
	Code        string  // 5-char UN/LOCODE, e.g. "SGSIN"
	CountryCode string  // ISO 3166-1 alpha-2, e.g. "SG"
	Location    string  // 3-char location code, e.g. "SIN"
	Name        string  // English name without diacritics
	Lat         float64
	Lon         float64
}

// Load reads the filtered UN/LOCODE CSV (columns: Country,Location,Name,Coordinates)
// and returns all ports with valid coordinates.
func Load(r io.Reader) ([]Port, error) {
	cr := csv.NewReader(r)
	header, err := cr.Read()
	if err != nil {
		return nil, fmt.Errorf("unlocode: read header: %w", err)
	}
	idx := indexColumns(header)
	if idx["Country"] < 0 || idx["Location"] < 0 || idx["Name"] < 0 || idx["Coordinates"] < 0 {
		return nil, fmt.Errorf("unlocode: missing required columns in %v", header)
	}

	var ports []Port
	for {
		row, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("unlocode: read row: %w", err)
		}
		country := strings.TrimSpace(row[idx["Country"]])
		location := strings.TrimSpace(row[idx["Location"]])
		name := strings.TrimSpace(row[idx["Name"]])
		coords := strings.TrimSpace(row[idx["Coordinates"]])
		if country == "" || location == "" || coords == "" {
			continue
		}
		lat, lon, ok := ParseCoordinates(coords)
		if !ok {
			continue
		}
		ports = append(ports, Port{
			Code:        country + location,
			CountryCode: country,
			Location:    location,
			Name:        name,
			Lat:         lat,
			Lon:         lon,
		})
	}
	return ports, nil
}

// ParseCoordinates parses the UN/LOCODE coordinate format "DDMMN DDDMME" into
// decimal degrees. Returns ok=false for empty or malformed strings.
//
// Examples:
//
//	"0117N 10351E" → (1.2833, 103.8500)
//	"5130N 00007W" → (51.5000, -0.1167)
//	"3347S 07040W" → (-33.7833, -70.6667)
func ParseCoordinates(raw string) (lat, lon float64, ok bool) {
	raw = strings.TrimSpace(raw)
	if len(raw) < 11 {
		return 0, 0, false
	}
	// Format: "DDMMH DDDMMH" where H ∈ {N,S,E,W}
	// E.g.   "0117N 10351E"
	//         0123456789...
	parts := strings.Fields(raw)
	if len(parts) != 2 {
		return 0, 0, false
	}
	lat, ok = parseDM(parts[0], true)
	if !ok {
		return 0, 0, false
	}
	lon, ok = parseDM(parts[1], false)
	if !ok {
		return 0, 0, false
	}
	return lat, lon, true
}

// parseDM parses a degree-minute hemisphere string:
// latitude "DDMMH" (4 digits + hemi) or longitude "DDDMMH" (5 digits + hemi).
func parseDM(s string, _ bool) (float64, bool) {
	if len(s) < 5 {
		return 0, false
	}
	hemi := s[len(s)-1]
	digits := s[:len(s)-1]
	if len(digits) < 4 {
		return 0, false
	}
	// Latitude: 4 digits = DDMM; Longitude: 5 digits = DDDMM
	minStart := len(digits) - 2
	degStr := digits[:minStart]
	minStr := digits[minStart:]
	deg, err := strconv.ParseFloat(degStr, 64)
	if err != nil {
		return 0, false
	}
	min, err := strconv.ParseFloat(minStr, 64)
	if err != nil {
		return 0, false
	}
	val := deg + min/60.0
	switch hemi {
	case 'N', 'n':
		// positive lat
	case 'S', 's':
		val = -val
	case 'E', 'e':
		// positive lon
	case 'W', 'w':
		val = -val
	default:
		return 0, false
	}
	return val, true
}

func indexColumns(header []string) map[string]int {
	m := make(map[string]int, len(header))
	for i, col := range header {
		m[strings.TrimSpace(col)] = i
	}
	for _, required := range []string{"Country", "Location", "Name", "Coordinates"} {
		if _, exists := m[required]; !exists {
			m[required] = -1
		}
	}
	return m
}
