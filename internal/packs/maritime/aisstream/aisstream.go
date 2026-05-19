package aisstream

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

// MetaDataTimeLayout is the timestamp format used by the AISstream MetaData field.
// Milliseconds are optional in the source but the layout with .999 handles both.
const MetaDataTimeLayout = "2006-01-02 15:04:05.999 +0000 UTC"

// Envelope is the outer JSON wrapper for every AISstream WebSocket message.
type Envelope struct {
	MessageType string          `json:"MessageType"`
	MetaData    MetaData        `json:"MetaData"`
	Message     json.RawMessage `json:"Message"`
}

// MetaData contains vessel context delivered with every AISstream message.
// MetaData contains vessel context delivered with every AISstream message.
// Note: MMSI_String is sent as an integer by the AISstream API despite the name.
// mmsiInt handles both JSON number and string representations defensively.
type MetaData struct {
	MMSI       int      `json:"MMSI"`
	ShipName   string   `json:"ShipName"`
	Latitude   float64  `json:"latitude"`
	Longitude  float64  `json:"longitude"`
	TimeUTC    time.Time
	MMSIString mmsiInt  `json:"MMSI_String"`
}

// mmsiInt accepts a JSON number OR a quoted decimal string for MMSI_String.
type mmsiInt int

func (m *mmsiInt) UnmarshalJSON(data []byte) error {
	if len(data) > 0 && data[0] == '"' {
		var s string
		if err := json.Unmarshal(data, &s); err != nil {
			return err
		}
		if s == "" {
			*m = 0
			return nil
		}
		v, err := strconv.Atoi(s)
		if err != nil {
			return fmt.Errorf("mmsiInt: cannot parse %q as int: %w", s, err)
		}
		*m = mmsiInt(v)
		return nil
	}
	var v int
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	*m = mmsiInt(v)
	return nil
}

// UnmarshalJSON implements custom unmarshalling for MetaData so that TimeUTC
// can be parsed from the AISstream string format.
func (m *MetaData) UnmarshalJSON(data []byte) error {
	type alias MetaData
	aux := struct {
		TimeUTC string `json:"time_utc"`
		*alias
	}{
		alias: (*alias)(m),
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	if aux.TimeUTC != "" {
		ts, err := parseMetaDataTime(aux.TimeUTC)
		if err != nil {
			return fmt.Errorf("aisstream: MetaData.time_utc: %w", err)
		}
		m.TimeUTC = ts
	}
	return nil
}

// parseMetaDataTime parses the AISstream timestamp format.
func parseMetaDataTime(s string) (time.Time, error) {
	t, err := time.Parse(MetaDataTimeLayout, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("unsupported time_utc format: %q", s)
	}
	return t.UTC(), nil
}

// PositionReport holds Class A GNSS/radio position data.
type PositionReport struct {
	Mmsi               int     `json:"Mmsi"`
	Sog                float64 `json:"Sog"`
	Cog                float64 `json:"Cog"`
	TrueHeading        int     `json:"TrueHeading"`
	NavigationalStatus int     `json:"NavigationalStatus"`
	Latitude           float64 `json:"Latitude"`
	Longitude          float64 `json:"Longitude"`
	RateOfTurn         int     `json:"RateOfTurn"`
	PositionAccuracy   bool    `json:"PositionAccuracy"`
	Timestamp          int     `json:"Timestamp"`
}

// ShipStaticData holds static/voyage data (AIS message type 5 / 24).
type ShipStaticData struct {
	Mmsi         int    `json:"Mmsi"`
	IMO          int    `json:"ImoNumber"`
	Name         string `json:"Name"`
	CallSign     string `json:"CallSign"`
	ShipType     int    `json:"Type"`
	DimToBow     int
	DimToStern   int
	DimToPort    int
	DimToStarboard int
}

// shipStaticDataRaw mirrors ShipStaticData but uses intermediate structs for
// nested JSON fields that AISstream places inside a "Dimension" object.
type shipStaticDataRaw struct {
	Mmsi     int    `json:"Mmsi"`
	IMO      int    `json:"ImoNumber"`
	Name     string `json:"Name"`
	CallSign string `json:"CallSign"`
	ShipType int    `json:"Type"`
	Dimension struct {
		A int `json:"A"`
		B int `json:"B"`
		C int `json:"C"`
		D int `json:"D"`
	} `json:"Dimension"`
}

// UnmarshalJSON implements custom JSON decoding for ShipStaticData to flatten
// the nested Dimension object into discrete Dim* fields.
func (s *ShipStaticData) UnmarshalJSON(data []byte) error {
	var raw shipStaticDataRaw
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	s.Mmsi = raw.Mmsi
	s.IMO = raw.IMO
	s.Name = raw.Name
	s.CallSign = raw.CallSign
	s.ShipType = raw.ShipType
	s.DimToBow = raw.Dimension.A
	s.DimToStern = raw.Dimension.B
	s.DimToPort = raw.Dimension.C
	s.DimToStarboard = raw.Dimension.D
	return nil
}

// StandardClassBPositionReport holds Class B position data.
type StandardClassBPositionReport struct {
	Mmsi               int     `json:"Mmsi"`
	Sog                float64 `json:"Sog"`
	Cog                float64 `json:"Cog"`
	TrueHeading        int     `json:"TrueHeading"`
	NavigationalStatus int     `json:"NavigationalStatus"`
	Latitude           float64 `json:"Latitude"`
	Longitude          float64 `json:"Longitude"`
}

// ExtendedClassBEquipmentPositionReport holds extended Class B position data.
type ExtendedClassBEquipmentPositionReport struct {
	Mmsi               int     `json:"Mmsi"`
	Sog                float64 `json:"Sog"`
	Cog                float64 `json:"Cog"`
	TrueHeading        int     `json:"TrueHeading"`
	NavigationalStatus int     `json:"NavigationalStatus"`
	Latitude           float64 `json:"Latitude"`
	Longitude          float64 `json:"Longitude"`
	Name               string  `json:"Name"`
}

// AidToNavigationReport holds AtoN beacon data.
type AidToNavigationReport struct {
	Mmsi      int     `json:"Mmsi"`
	Name      string  `json:"Name"`
	Latitude  float64 `json:"Latitude"`
	Longitude float64 `json:"Longitude"`
}

// BaseStationReport holds base station position data.
type BaseStationReport struct {
	Mmsi      int     `json:"Mmsi"`
	Latitude  float64 `json:"Latitude"`
	Longitude float64 `json:"Longitude"`
}

// ParseBatch decodes a JSON array of Envelope messages. The body must contain
// a JSON array; an object is not accepted. Unknown MessageType values are
// preserved as raw JSON in Envelope.Message without error.
func ParseBatch(body []byte) ([]Envelope, error) {
	if len(body) == 0 {
		return nil, fmt.Errorf("aisstream: parsebatch: empty body")
	}
	var envelopes []Envelope
	if err := json.Unmarshal(body, &envelopes); err != nil {
		return nil, fmt.Errorf("aisstream: parsebatch: %w", err)
	}
	return envelopes, nil
}

// PositionData returns the best available position from MetaData.
// ok is false when both latitude and longitude are zero (no fix).
func (e Envelope) PositionData() (lat, lon float64, ts time.Time, ok bool) {
	if e.MetaData.Latitude == 0 && e.MetaData.Longitude == 0 {
		return 0, 0, time.Time{}, false
	}
	return e.MetaData.Latitude, e.MetaData.Longitude, e.MetaData.TimeUTC, true
}

// EntityID returns a stable entity identifier for the vessel.
// If the message is a ShipStaticData with a non-zero IMO number the ID is
// "ent:vessel:<IMO>"; otherwise it falls back to "ent:vessel:mmsi:<MMSI>".
func (e Envelope) EntityID() string {
	if e.MessageType == "ShipStaticData" {
		var msg struct {
			ShipStaticData struct {
				ImoNumber int `json:"ImoNumber"`
			} `json:"ShipStaticData"`
		}
		if err := json.Unmarshal(e.Message, &msg); err == nil {
			if imo := msg.ShipStaticData.ImoNumber; imo != 0 {
				return "ent:vessel:" + strconv.Itoa(imo)
			}
		}
	}
	return "ent:vessel:mmsi:" + strconv.Itoa(int(e.MetaData.MMSIString))
}
