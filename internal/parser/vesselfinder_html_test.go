package parser

import (
	"context"
	"testing"
	"time"
)

const vesselFinderDetailFixture = `
<html><body>
  <h1>ALPHA TRADER</h1>
  <dl>
    <dt>IMO</dt><dd>9303801</dd>
    <dt>MMSI</dt><dd>538009877</dd>
    <dt>Callsign</dt><dd>V7NL8</dd>
    <dt>Flag</dt><dd>Marshall Islands</dd>
    <dt>Vessel Type</dt><dd>Oil/Chemical Tanker</dd>
    <dt>Status</dt><dd>Underway</dd>
    <dt>Speed</dt><dd>12.3 kn</dd>
    <dt>Course</dt><dd>134.2 deg</dd>
    <dt>Position received</dt><dd>2026-05-06 10:12 UTC</dd>
  </dl>
  <span data-lat="25.2472" data-lon="56.3575"></span>
</body></html>`

func TestDefaultRegistryIncludesVesselFinderHTMLParser(t *testing.T) {
	parser, ok := DefaultRegistry().Lookup("parser:vesselfinder-html")
	if !ok {
		t.Fatal("expected parser:vesselfinder-html to be registered")
	}
	desc := parser.Descriptor()
	if desc.Family != "vesselfinder" || desc.SourceClass != "browser_rendered_vessel_detail" {
		t.Fatalf("unexpected descriptor: %#v", desc)
	}
	if desc.Version != "1.1.0" {
		t.Fatalf("unexpected parser version %q", desc.Version)
	}
}

func TestVesselFinderParserEmitsEntityAndTrackPoint(t *testing.T) {
	result, parseErr := DefaultRegistry().Parse(context.Background(), Input{
		ParserID:    "parser:vesselfinder-html",
		SourceID:    "catalog:auto:maritime-ocean-and-coastal-sources-vesselfinder",
		RawID:       "raw:vf:1",
		URL:         "https://www.vesselfinder.com/vessels/details/9303801",
		ContentType: "text/html",
		Body:        []byte(vesselFinderDetailFixture),
		FetchedAt:   time.Date(2026, 5, 6, 10, 15, 0, 0, time.UTC),
		Attrs: map[string]any{
			"vesselfinder": map[string]any{
				"country_code":  "MH",
				"country_label": "Marshall Islands",
				"type_code":     "8",
				"type_label":    "Tankers",
				"place_id":      "plc:flag:mh",
			},
		},
	})
	if parseErr != nil {
		t.Fatalf("Parse returned error: %v", parseErr)
	}
	if len(result.Candidates) != 2 {
		t.Fatalf("expected entity and track point candidates, got %d", len(result.Candidates))
	}
	entity := result.Candidates[0]
	if entity.Kind != "entity" || entity.NativeID != "imo:9303801" {
		t.Fatalf("unexpected entity candidate: %#v", entity)
	}
	if entity.Data["entity_id"] != "ent:vessel:9303801" || entity.Data["canonical_name"] != "ALPHA TRADER" || entity.Data["entity_type"] != "vessel" {
		t.Fatalf("unexpected entity data: %#v", entity.Data)
	}
	if entity.Data["place_id"] != "plc:flag:mh" || entity.Data["flag_state_code"] != "MH" {
		t.Fatalf("expected discovery place context on entity, got %#v", entity.Data)
	}
	point := result.Candidates[1]
	if point.Kind != "track_point" || point.NativeID != "9303801:20260506101200" {
		t.Fatalf("unexpected track point candidate: %#v", point)
	}
	if point.Data["entity_id"] != "ent:vessel:9303801" || point.Data["track_type"] != "vessel" || point.Data["record_kind"] != "track_point" {
		t.Fatalf("unexpected track point data: %#v", point.Data)
	}
	if point.Data["lat"] != 25.2472 || point.Data["lon"] != 56.3575 {
		t.Fatalf("unexpected coordinates: %#v", point.Data)
	}
	if point.Data["place_id"] != "plc:flag:mh" {
		t.Fatalf("expected discovery place context on track point, got %#v", point.Data)
	}
	if got := point.Data["speed_kph"].(float64); got < 22.77 || got > 22.78 {
		t.Fatalf("unexpected speed kph: %v", got)
	}
}

func TestVesselFinderParserReturnsZeroCandidatesForListPage(t *testing.T) {
	result, parseErr := DefaultRegistry().Parse(context.Background(), Input{
		ParserID: "parser:vesselfinder-html",
		URL:      "https://www.vesselfinder.com/vessels?type=2&page=1",
		Body:     []byte(`<html><a href="/vessels/details/1234567">Alpha</a></html>`),
	})
	if parseErr != nil {
		t.Fatalf("list page should not be a parse error: %v", parseErr)
	}
	if len(result.Candidates) != 0 {
		t.Fatalf("expected zero list page candidates, got %d", len(result.Candidates))
	}
}

func TestVesselFinderParserDerivesFlagPlaceFromMMSIMID(t *testing.T) {
	body := `
	<html>
	<head><title>MALAYSIA TEST, Cargo ship - Details and current position - MMSI 533123456 - VesselFinder</title></head>
	<body>
	  <h1>MALAYSIA TEST</h1>
	  <div id="djson" data-json="{ &quot;mmsi&quot;:533123456,&quot;ship_lat&quot;:1.2,&quot;ship_lon&quot;:104.1,&quot;ship_cog&quot;:90,&quot;ship_sog&quot;:4.5}"></div>
	</body>
	</html>`
	result, parseErr := DefaultRegistry().Parse(context.Background(), Input{
		ParserID:  "parser:vesselfinder-html",
		SourceID:  "catalog:auto:maritime-ocean-and-coastal-sources-vesselfinder",
		RawID:     "raw:vf:mid",
		URL:       "https://www.vesselfinder.com/vessels/details/533123456",
		Body:      []byte(body),
		FetchedAt: time.Date(2026, 5, 7, 10, 0, 0, 0, time.UTC),
	})
	if parseErr != nil {
		t.Fatalf("Parse returned error: %v", parseErr)
	}
	if len(result.Candidates) != 2 {
		t.Fatalf("expected entity and track point candidates, got %d", len(result.Candidates))
	}
	entity := result.Candidates[0]
	if entity.Data["place_id"] != "plc:flag:my" || entity.Data["flag_state_code"] != "MY" || entity.Data["flag_state"] != "Malaysia" {
		t.Fatalf("expected MMSI MID-derived flag context, got %#v", entity.Data)
	}
	point := result.Candidates[1]
	if point.Data["place_id"] != "plc:flag:my" {
		t.Fatalf("expected MMSI MID-derived point place, got %#v", point.Data)
	}
}

func TestVesselFinderParserEmitsPortCallCandidates(t *testing.T) {
	body := `
<html><body>
  <h1>TEST VESSEL</h1>
  <dl>
    <dt>IMO</dt><dd>9303801</dd>
    <dt>MMSI</dt><dd>538009877</dd>
  </dl>
  <div id="djson" data-json="{ &quot;mmsi&quot;:538009877,&quot;ship_lat&quot;:1.2,&quot;ship_lon&quot;:104.1}"></div>
  <div id="port-calls"><div><div>
    <a class="flx _rLk" href="/ports/SGSIN001">
      <img alt="Singapore" title="Singapore">Singapore, Singapore</a>
    <div class="flx _1hgmG">
      <div class="_211eJ"><div class="_2nufK">Arrival (UTC)</div><div class="_1GQkK">Apr 26, 11:07</div></div>
      <div class="_211eJ"><div class="_2nufK">Departure (UTC)</div><div class="_1GQkK">-</div></div>
    </div>
  </div></div></div>
  <div class="flx habh"><a id="habtn" href="/historical-ais-data">Historical AIS Data</a></div>
</body></html>`
	result, parseErr := DefaultRegistry().Parse(context.Background(), Input{
		ParserID:  "parser:vesselfinder-html",
		SourceID:  "catalog:auto:maritime-ocean-and-coastal-sources-vesselfinder",
		RawID:     "raw:vf:pc",
		URL:       "https://www.vesselfinder.com/vessels/details/9303801",
		Body:      []byte(body),
		FetchedAt: time.Date(2026, 5, 19, 10, 0, 0, 0, time.UTC),
	})
	if parseErr != nil {
		t.Fatalf("Parse returned error: %v", parseErr)
	}
	// entity + AIS track_point + 1 port_call track_point = 3
	if len(result.Candidates) != 3 {
		t.Fatalf("expected 3 candidates (entity, ais_point, port_call), got %d", len(result.Candidates))
	}
	pc := result.Candidates[2]
	if pc.Kind != "track_point" {
		t.Errorf("port call candidate kind=%q, want track_point", pc.Kind)
	}
	if pc.Data["place_id"] != "plc:port:sgsin" {
		t.Errorf("place_id=%q, want plc:port:sgsin", pc.Data["place_id"])
	}
	if pc.Data["origin"] != "port_call" {
		t.Errorf("origin=%q, want port_call", pc.Data["origin"])
	}
	if pc.Data["entity_id"] != "ent:vessel:9303801" {
		t.Errorf("entity_id=%q, want ent:vessel:9303801", pc.Data["entity_id"])
	}
	if _, hasLat := pc.Data["lat"]; hasLat {
		t.Error("port call candidate should not have lat (resolved via dim_place join)")
	}
}

func TestVesselFinderParserMapsBotPageToTypedParseError(t *testing.T) {
	_, parseErr := DefaultRegistry().Parse(context.Background(), Input{
		ParserID: "parser:vesselfinder-html",
		URL:      "https://www.vesselfinder.com/vessels/details/1",
		Body:     []byte(`<html>Checking if the site connection is secure</html>`),
	})
	if parseErr == nil || parseErr.Code != "bot_or_captcha_page" || !parseErr.Retryable {
		t.Fatalf("expected retryable bot page parse error, got %#v", parseErr)
	}
}
