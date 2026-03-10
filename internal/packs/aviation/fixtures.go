package aviation

import (
	"embed"
	"encoding/json"
	"fmt"
	"strings"
)

//go:embed testdata/*.json
var fixtureFS embed.FS

type embeddedFixture struct {
	SourceID       string          `json:"source_id"`
	OpenSkyPayload json.RawMessage `json:"opensky_payload"`
	RegistryCSV    string          `json:"registry_csv"`
	Airports       []Airport       `json:"airports"`
}

func LoadFixtureBundle(sourceID string) (InputBundle, error) {
	if strings.TrimSpace(sourceID) == "" {
		sourceID = DefaultFixtureSourceID
	}
	name, ok := map[string]string{
		"fixture:aviation":              "testdata/fixture_aviation.json",
		"fixture:aviation-low-evidence": "testdata/fixture_aviation_low_evidence.json",
	}[strings.TrimSpace(sourceID)]
	if !ok {
		return InputBundle{}, fmt.Errorf("unknown aviation fixture source %q", sourceID)
	}
	payload, err := fixtureFS.ReadFile(name)
	if err != nil {
		return InputBundle{}, err
	}
	var fixture embeddedFixture
	if err := json.Unmarshal(payload, &fixture); err != nil {
		return InputBundle{}, err
	}
	states, err := DecodeStateVectors(strings.NewReader(string(fixture.OpenSkyPayload)))
	if err != nil {
		return InputBundle{}, err
	}
	registry, err := DecodeRegistryCSV(strings.NewReader(fixture.RegistryCSV))
	if err != nil {
		return InputBundle{}, err
	}
	return InputBundle{
		SourceID:     fixture.SourceID,
		StateVectors: states,
		Registry:     registry,
		Airports:     fixture.Airports,
	}, nil
}
