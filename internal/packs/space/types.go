package space

import (
	"fmt"
	"strings"
	"time"
	"unicode"

	"global-osint-backend/internal/canonical"
	"global-osint-backend/internal/location"
)

const SchemaVersion uint32 = 1

type ElementSet struct {
	SourceFormat            string
	ObjectName              string
	NORADID                 string
	InternationalDesignator string
	Classification          string
	Epoch                   time.Time
	InclinationDeg          float64
	RAANDeg                 float64
	Eccentricity            float64
	ArgPerigeeDeg           float64
	MeanAnomalyDeg          float64
	MeanMotionRevPerDay     float64
	BStar                   float64
	RevNumber               uint64
	Transmitters            []Transmitter
	Evidence                []canonical.Evidence
	Attrs                   map[string]any
}

type Transmitter struct {
	Callsign    string
	Mode        string
	DownlinkMHz float64
	Status      string
}

type TrackPoint struct {
	PointID       string               `json:"point_id"`
	SatelliteID   string               `json:"satellite_id"`
	ObservedAt    time.Time            `json:"observed_at"`
	Latitude      float64              `json:"latitude"`
	Longitude     float64              `json:"longitude"`
	AltitudeKM    float64              `json:"altitude_km"`
	AlongTrackKM  float64              `json:"along_track_km"`
	SchemaVersion uint32               `json:"schema_version"`
	Attrs         map[string]any       `json:"attrs,omitempty"`
	Evidence      []canonical.Evidence `json:"evidence,omitempty"`
}

type Place struct {
	PlaceID  string
	Name     string
	Center   location.Coordinate
	BBox     *location.BBox
	RadiusKM float64
	Tags     []string
	Attrs    map[string]any
}

type OverpassWindow struct {
	WindowID          string               `json:"window_id"`
	SatelliteID       string               `json:"satellite_id"`
	PlaceID           string               `json:"place_id"`
	StartedAt         time.Time            `json:"started_at"`
	EndedAt           time.Time            `json:"ended_at"`
	Duration          time.Duration        `json:"duration"`
	SampleCount       int                  `json:"sample_count"`
	ClosestApproachKM float64              `json:"closest_approach_km"`
	PeakAltitudeKM    float64              `json:"peak_altitude_km"`
	SchemaVersion     uint32               `json:"schema_version"`
	Attrs             map[string]any       `json:"attrs,omitempty"`
	Evidence          []canonical.Evidence `json:"evidence,omitempty"`
}

type PlaceIntersection struct {
	IntersectionID string               `json:"intersection_id"`
	SatelliteID    string               `json:"satellite_id"`
	PlaceID        string               `json:"place_id"`
	ObservedAt     time.Time            `json:"observed_at"`
	Latitude       float64              `json:"latitude"`
	Longitude      float64              `json:"longitude"`
	DistanceKM     float64              `json:"distance_km"`
	Inside         bool                 `json:"inside"`
	SchemaVersion  uint32               `json:"schema_version"`
	Attrs          map[string]any       `json:"attrs,omitempty"`
	Evidence       []canonical.Evidence `json:"evidence,omitempty"`
}

type ConjunctionAdvisory struct {
	AdvisoryID        string
	SatelliteID       string
	SecondaryNORADID  string
	ClosestApproachAt time.Time
	MissDistanceKM    float64
	Probability       float64
	SourceID          string
	Evidence          []canonical.Evidence
	Attrs             map[string]any
}

type Metric struct {
	MetricID      string               `json:"metric_id"`
	SubjectType   string               `json:"subject_type"`
	SubjectID     string               `json:"subject_id"`
	WindowStart   time.Time            `json:"window_start"`
	WindowEnd     time.Time            `json:"window_end"`
	Value         float64              `json:"value"`
	SchemaVersion uint32               `json:"schema_version"`
	Attrs         map[string]any       `json:"attrs,omitempty"`
	Evidence      []canonical.Evidence `json:"evidence,omitempty"`
}

type Input struct {
	Catalog      []ElementSet
	Places       []Place
	Conjunctions []ConjunctionAdvisory
	Start        time.Time
	End          time.Time
	Step         time.Duration
}

type SatelliteReport struct {
	SatelliteID   string
	Element       ElementSet
	Track         []TrackPoint
	Windows       []OverpassWindow
	Intersections []PlaceIntersection
	Conjunctions  []ConjunctionAdvisory
}

type Result struct {
	Start      time.Time
	End        time.Time
	Satellites []SatelliteReport
	Metrics    []Metric
}

func (e ElementSet) SatelliteID() string {
	if strings.TrimSpace(e.NORADID) != "" {
		return "sat:" + strings.TrimSpace(e.NORADID)
	}
	var b strings.Builder
	for _, r := range strings.ToLower(strings.TrimSpace(e.ObjectName)) {
		switch {
		case unicode.IsLetter(r), unicode.IsDigit(r):
			b.WriteRune(r)
		case b.Len() > 0:
			if tail := b.String(); !strings.HasSuffix(tail, "-") {
				b.WriteByte('-')
			}
		}
	}
	id := strings.Trim(b.String(), "-")
	if id == "" {
		id = "unknown"
	}
	return "sat:" + id
}

func (e ElementSet) Validate() error {
	if e.Epoch.IsZero() {
		return fmt.Errorf("epoch is required")
	}
	if e.MeanMotionRevPerDay <= 0 {
		return fmt.Errorf("mean_motion_rev_per_day must be positive")
	}
	if e.Eccentricity < 0 || e.Eccentricity >= 1 {
		return fmt.Errorf("eccentricity must be in [0,1)")
	}
	if e.ObjectName == "" && e.NORADID == "" {
		return fmt.Errorf("object identity is required")
	}
	return nil
}
