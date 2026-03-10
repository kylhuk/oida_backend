package aviation

import (
	"time"

	"global-osint-backend/internal/canonical"
)

const (
	SchemaVersion            uint32 = 1
	TrackType                       = "aviation"
	SourceModePublic                = "public"
	MetricMilitaryLikelihood        = "military_likelihood_score"
	MetricRouteIrregularity         = "route_irregularity_score"
	DefaultFixtureSourceID          = "fixture:aviation"
)

type BoundingBox struct {
	LatMin float64
	LonMin float64
	LatMax float64
	LonMax float64
}

type StateVectorQuery struct {
	Time     *time.Time
	ICAO24   []string
	Bounds   *BoundingBox
	Extended bool
}

type StateVector struct {
	ICAO24          string
	Callsign        string
	OriginCountry   string
	TimePosition    time.Time
	LastContact     time.Time
	Longitude       float64
	Latitude        float64
	HasPosition     bool
	BaroAltitudeM   *float64
	OnGround        bool
	VelocityMPS     *float64
	TrueTrackDeg    *float64
	VerticalRateMPS *float64
	GeoAltitudeM    *float64
	Squawk          string
	SPI             bool
	PositionSource  int
	Category        int

	Evidence []canonical.Evidence
}

func (v StateVector) ObservedAt() time.Time {
	if !v.TimePosition.IsZero() {
		return v.TimePosition.UTC().Truncate(time.Millisecond)
	}
	return v.LastContact.UTC().Truncate(time.Millisecond)
}

type RegistryRecord struct {
	Registration   string
	ModeSCodeHex   string
	SerialNumber   string
	RegistrantName string
	RegistrantType string
	Manufacturer   string
	Model          string
	Year           int
	AircraftType   string
	EngineType     string
	CountryCode    string

	Evidence []canonical.Evidence
}

type Airport struct {
	AirportID   string  `json:"airport_id"`
	PlaceID     string  `json:"place_id"`
	Name        string  `json:"name"`
	CountryCode string  `json:"country_code"`
	Use         string  `json:"use"`
	Latitude    float64 `json:"latitude"`
	Longitude   float64 `json:"longitude"`
}

type InputBundle struct {
	SourceID     string
	StateVectors []StateVector
	Registry     []RegistryRecord
	Airports     []Airport
}

type Options struct {
	Now                 func() time.Time
	GapThreshold        time.Duration
	GroundStopThreshold time.Duration
	AirportRadiusKM     float64
}

type AircraftEntity struct {
	EntityID           string
	ICAO24             string
	Callsign           string
	Registration       string
	Manufacturer       string
	Model              string
	RegistrantName     string
	RegistrantType     string
	CountryCode        string
	SourceSystem       string
	PrimaryPlaceID     string
	ObservedFrom       time.Time
	ObservedUntil      time.Time
	MilitaryLikelihood float64
	MilitaryStatus     string
	RouteIrregularity  float64
	RiskBand           string
	Attrs              map[string]any
	Evidence           []canonical.Evidence
}

type TrackPoint struct {
	TrackPointID string
	TrackID      string
	EntityID     string
	SourceID     string
	PlaceID      string
	ObservedAt   time.Time
	Latitude     float64
	Longitude    float64
	AltitudeM    *float64
	SpeedKPH     *float64
	CourseDeg    *float64
	OnGround     bool
	Callsign     string
	Attrs        map[string]any
	Evidence     []canonical.Evidence
}

type FlightSegment struct {
	SegmentID         string
	TrackID           string
	EntityID          string
	SourceID          string
	StartedAt         time.Time
	EndedAt           time.Time
	PointCount        int
	DistanceKM        float64
	AvgSpeedKPH       float64
	RouteIrregularity float64
	FromAirportID     string
	FromPlaceID       string
	ToAirportID       string
	ToPlaceID         string
	GapCount          int
	Attrs             map[string]any
	Evidence          []canonical.Evidence
}

type GapEvent struct {
	EventID   string
	TrackID   string
	EntityID  string
	SourceID  string
	PlaceID   string
	StartedAt time.Time
	EndedAt   time.Time
	GapHours  float64
	InFlight  bool
	Attrs     map[string]any
	Evidence  []canonical.Evidence
}

type AirportInteractionEvent struct {
	EventID         string
	TrackID         string
	EntityID        string
	SourceID        string
	AirportID       string
	PlaceID         string
	InteractionType string
	ObservedAt      time.Time
	Attrs           map[string]any
	Evidence        []canonical.Evidence
}

type MetricDefinition struct {
	MetricID     string
	MetricFamily string
	SubjectGrain string
	Unit         string
	ValueType    string
	RollupEngine string
	RollupRule   string
	Description  string
	Formula      string
}

type MetricSnapshot struct {
	SnapshotID   string
	MetricID     string
	SubjectGrain string
	SubjectID    string
	PlaceID      string
	WindowGrain  string
	WindowStart  time.Time
	WindowEnd    time.Time
	SnapshotAt   time.Time
	MetricValue  float64
	MetricDelta  float64
	Rank         uint32
	Attrs        map[string]any
	Evidence     []canonical.Evidence
}

type Stats struct {
	AircraftEntities    int
	TrackPoints         int
	FlightSegments      int
	TransponderGaps     int
	AirportInteractions int
	Metrics             int
}

type Bundle struct {
	Aircraft            []AircraftEntity
	TrackPoints         []TrackPoint
	Segments            []FlightSegment
	GapEvents           []GapEvent
	AirportInteractions []AirportInteractionEvent
	Metrics             []MetricSnapshot
	Stats               Stats
}

func MetricDefinitions() []MetricDefinition {
	return []MetricDefinition{
		{
			MetricID:     MetricMilitaryLikelihood,
			MetricFamily: "aviation",
			SubjectGrain: "entity",
			Unit:         "score",
			ValueType:    "gauge",
			RollupEngine: "snapshot",
			RollupRule:   "latest",
			Description:  "Evidence-weighted score for likely military affiliation without forcing classification when support is weak.",
			Formula:      "registry_owner_keywords + callsign_prefixes + military_airport_usage - civil_registry_conflicts",
		},
		{
			MetricID:     MetricRouteIrregularity,
			MetricFamily: "aviation",
			SubjectGrain: "entity",
			Unit:         "score",
			ValueType:    "gauge",
			RollupEngine: "snapshot",
			RollupRule:   "latest",
			Description:  "Segment detours, heading volatility, and transponder gaps rolled into a normalized irregularity score.",
			Formula:      "weighted_segment_detour + heading_turn_rate + transponder_gap_penalty",
		},
	}
}
