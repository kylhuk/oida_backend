package aviation

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"global-osint-backend/internal/canonical"
)

const defaultOpenSkyBaseURL = "https://opensky-network.org/api"

type OpenSkyAdapter struct {
	BaseURL     string
	Client      *http.Client
	BearerToken string
}

func NewOpenSkyAdapter(baseURL string, client *http.Client) *OpenSkyAdapter {
	if strings.TrimSpace(baseURL) == "" {
		baseURL = defaultOpenSkyBaseURL
	}
	if client == nil {
		client = http.DefaultClient
	}
	return &OpenSkyAdapter{BaseURL: strings.TrimRight(baseURL, "/"), Client: client}
}

func (a *OpenSkyAdapter) SourceMode() string {
	return SourceModePublic
}

func (a *OpenSkyAdapter) FetchStateVectors(ctx context.Context, query StateVectorQuery) ([]StateVector, error) {
	endpoint, err := a.requestURL(query)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(a.BearerToken) != "" {
		req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(a.BearerToken))
	}
	resp, err := a.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("opensky states/all returned %s", resp.Status)
	}
	return DecodeStateVectors(resp.Body)
}

func (a *OpenSkyAdapter) requestURL(query StateVectorQuery) (string, error) {
	base, err := url.Parse(a.BaseURL + "/states/all")
	if err != nil {
		return "", err
	}
	params := url.Values{}
	if query.Time != nil {
		params.Set("time", strconv.FormatInt(query.Time.UTC().Unix(), 10))
	}
	for _, icao24 := range query.ICAO24 {
		icao24 = normalizeICAO24(icao24)
		if icao24 == "" {
			continue
		}
		params.Add("icao24", icao24)
	}
	if query.Bounds != nil {
		params.Set("lamin", formatQueryFloat(query.Bounds.LatMin))
		params.Set("lomin", formatQueryFloat(query.Bounds.LonMin))
		params.Set("lamax", formatQueryFloat(query.Bounds.LatMax))
		params.Set("lomax", formatQueryFloat(query.Bounds.LonMax))
	}
	if query.Extended {
		params.Set("extended", "1")
	}
	base.RawQuery = params.Encode()
	return base.String(), nil
}

func DecodeStateVectors(r io.Reader) ([]StateVector, error) {
	var payload struct {
		Time   int64 `json:"time"`
		States []any `json:"states"`
	}
	decoder := json.NewDecoder(r)
	decoder.UseNumber()
	if err := decoder.Decode(&payload); err != nil {
		return nil, err
	}
	out := make([]StateVector, 0, len(payload.States))
	for idx, raw := range payload.States {
		row, ok := raw.([]any)
		if !ok {
			return nil, fmt.Errorf("state row %d has unexpected type %T", idx+1, raw)
		}
		vector, err := decodeStateVectorRow(row)
		if err != nil {
			return nil, fmt.Errorf("state row %d: %w", idx+1, err)
		}
		vector.Evidence = []canonical.Evidence{{
			Kind:     "source_record",
			SourceID: "opensky:states-all",
			Ref:      fmt.Sprintf("%s:%d", vector.ICAO24, vector.ObservedAt().Unix()),
			Attrs: map[string]any{
				"adapter": "opensky_public",
			},
		}}
		out = append(out, vector)
	}
	return out, nil
}

func decodeStateVectorRow(row []any) (StateVector, error) {
	if len(row) < 17 {
		return StateVector{}, fmt.Errorf("expected at least 17 columns, got %d", len(row))
	}
	vector := StateVector{
		ICAO24:         normalizeICAO24(stringValue(row[0])),
		Callsign:       strings.TrimSpace(stringValue(row[1])),
		OriginCountry:  strings.TrimSpace(stringValue(row[2])),
		OnGround:       boolValue(row[8]),
		Squawk:         strings.TrimSpace(stringValue(row[14])),
		SPI:            boolValue(row[15]),
		PositionSource: intValue(row[16]),
	}
	if len(row) > 17 {
		vector.Category = intValue(row[17])
	}
	if ts := int64Value(row[3]); ts > 0 {
		vector.TimePosition = time.Unix(ts, 0).UTC()
	}
	if ts := int64Value(row[4]); ts > 0 {
		vector.LastContact = time.Unix(ts, 0).UTC()
	}
	if lon, ok := float64Value(row[5]); ok {
		vector.Longitude = lon
		vector.HasPosition = true
	}
	if lat, ok := float64Value(row[6]); ok {
		vector.Latitude = lat
		vector.HasPosition = vector.HasPosition && true
	} else {
		vector.HasPosition = false
	}
	vector.BaroAltitudeM = nullableFloat64(row[7])
	vector.VelocityMPS = nullableFloat64(row[9])
	vector.TrueTrackDeg = nullableFloat64(row[10])
	vector.VerticalRateMPS = nullableFloat64(row[11])
	if len(row) > 13 {
		vector.GeoAltitudeM = nullableFloat64(row[13])
	}
	if vector.ICAO24 == "" {
		return StateVector{}, fmt.Errorf("icao24 is required")
	}
	if vector.ObservedAt().IsZero() {
		return StateVector{}, fmt.Errorf("last_contact or time_position is required")
	}
	return vector, nil
}

func stringValue(v any) string {
	switch value := v.(type) {
	case nil:
		return ""
	case string:
		return value
	case json.Number:
		return value.String()
	default:
		return fmt.Sprint(value)
	}
}

func boolValue(v any) bool {
	b, _ := v.(bool)
	return b
}

func intValue(v any) int {
	if n, ok := float64Value(v); ok {
		return int(n)
	}
	return 0
}

func int64Value(v any) int64 {
	switch value := v.(type) {
	case nil:
		return 0
	case json.Number:
		i, _ := value.Int64()
		return i
	case float64:
		return int64(value)
	case int64:
		return value
	case int:
		return int64(value)
	default:
		return 0
	}
}

func nullableFloat64(v any) *float64 {
	if f, ok := float64Value(v); ok {
		return &f
	}
	return nil
}

func float64Value(v any) (float64, bool) {
	switch value := v.(type) {
	case nil:
		return 0, false
	case json.Number:
		f, err := value.Float64()
		return f, err == nil
	case float64:
		return value, true
	case int:
		return float64(value), true
	case int64:
		return float64(value), true
	default:
		return 0, false
	}
}

func normalizeICAO24(v string) string {
	v = strings.ToLower(strings.TrimSpace(v))
	if v == "" {
		return ""
	}
	return v
}

func formatQueryFloat(v float64) string {
	return strconv.FormatFloat(v, 'f', -1, 64)
}
