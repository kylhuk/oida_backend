package vesselfinder

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

const dimensionsHTML = `
<html><body>
  <select id="country">
    <option value="">All countries</option>
    <option value="US">United States</option>
    <option value="PA">Panama</option>
  </select>
  <select name="type">
    <option value="0">All types</option>
    <option value="1">Cargo vessels</option>
    <option value="2">Tankers</option>
  </select>
</body></html>`

const listHTML = `
<html><body>
  <a href="/vessels/details/1234567">Alpha</a>
  <a href="https://www.vesselfinder.com/vessels/details/7654321">Bravo</a>
  <a href="/news">Noise</a>
  <a href="/vessels/details/1234567">Duplicate</a>
</body></html>`

const detailHTML = `
<html>
<head>
  <title>ALPHA TRADER Current Position - VesselFinder</title>
  <meta property="og:url" content="https://www.vesselfinder.com/vessels/details/9303801">
</head>
<body>
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
</body>
</html>`

func TestExtractDimensionsFromCountryAndTypeForms(t *testing.T) {
	dims := ExtractDimensions(dimensionsHTML)
	if got, want := dims.Countries, []Dimension{{Code: "PA", Label: "Panama"}, {Code: "US", Label: "United States"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("countries mismatch\n got: %#v\nwant: %#v", got, want)
	}
	if got, want := dims.Types, []Dimension{{Code: "1", Label: "Cargo vessels"}, {Code: "2", Label: "Tankers"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("types mismatch\n got: %#v\nwant: %#v", got, want)
	}
}

func TestExtractDimensionsFromLiveFlagAndTypeSelectNames(t *testing.T) {
	html := `
		<select id="advsearch-ship-flag" name="flag" class="inpsel">
			<option value="-">Any flag</option>
			<option value="pa">Panama</option>
			<option value="mh">Marshall Islands</option>
		</select>
		<select id="advsearch-ship-type" name="type" class="inpsel">
			<option value="-">Any type</option>
			<option value="7">Cargo vessels</option>
			<option value="8">Tankers</option>
		</select>`
	dims := ExtractDimensions(html)
	if got, want := dims.Countries, []Dimension{{Code: "mh", Label: "Marshall Islands"}, {Code: "pa", Label: "Panama"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("countries mismatch\n got: %#v\nwant: %#v", got, want)
	}
	if got, want := dims.Types, []Dimension{{Code: "7", Label: "Cargo vessels"}, {Code: "8", Label: "Tankers"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("types mismatch\n got: %#v\nwant: %#v", got, want)
	}
}

func TestExtractDetailLinksDeduplicatesAndNormalizes(t *testing.T) {
	links := ExtractDetailLinks(listHTML, "https://www.vesselfinder.com/vessels?type=2")
	want := []string{
		"https://www.vesselfinder.com/vessels/details/1234567",
		"https://www.vesselfinder.com/vessels/details/7654321",
	}
	if !reflect.DeepEqual(links, want) {
		t.Fatalf("links mismatch\n got: %#v\nwant: %#v", links, want)
	}
}

func TestParseDetailMetadataNormalizesPositionAndIdentifiers(t *testing.T) {
	vessel, err := ParseDetail(detailHTML, "https://www.vesselfinder.com/vessels/details/9303801", time.Date(2026, 5, 6, 10, 15, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("ParseDetail returned error: %v", err)
	}
	if vessel.DetailID != "9303801" || vessel.Name != "ALPHA TRADER" || vessel.IMO != "9303801" || vessel.MMSI != "538009877" {
		t.Fatalf("unexpected identifiers: %#v", vessel)
	}
	if vessel.Flag != "Marshall Islands" || vessel.VesselType != "Oil/Chemical Tanker" || vessel.Status != "Underway" {
		t.Fatalf("unexpected metadata: %#v", vessel)
	}
	if vessel.Latitude == nil || vessel.Longitude == nil || *vessel.Latitude != 25.2472 || *vessel.Longitude != 56.3575 {
		t.Fatalf("unexpected coordinates: lat=%v lon=%v", vessel.Latitude, vessel.Longitude)
	}
	if vessel.SpeedKPH == nil || *vessel.SpeedKPH < 22.77 || *vessel.SpeedKPH > 22.78 {
		t.Fatalf("expected 12.3kn converted to kph, got %v", vessel.SpeedKPH)
	}
	if vessel.CourseDeg == nil || *vessel.CourseDeg != 134.2 {
		t.Fatalf("unexpected course: %v", vessel.CourseDeg)
	}
	if got, want := vessel.ObservedAt.UTC().Format(time.RFC3339), "2026-05-06T10:12:00Z"; got != want {
		t.Fatalf("observed_at got %s want %s", got, want)
	}
}

func TestParseDetailExtractsCurrentTitleScriptAndDJSONShape(t *testing.T) {
	body := `
	<html>
	<head>
	  <title>KNS12839-03-92, Unknown - Details and current position - MMSI 451283903 - VesselFinder</title>
	  <script>var vu_flags=0;var vu_imo=0;var mhpc=0;var MMSI=451283903</script>
	</head>
	<body>
	  <h1 class="title">KNS12839-03-92</h1>
	  <h2 class="vst">Unknown, MMSI 451283903</h2>
	  <div id="djson" data-json="{ &quot;v&quot;:false,&quot;lrpd&quot;:&quot;-&quot;,&quot;mmsi&quot;:451283903,&quot;ship_lat&quot;:91,&quot;ship_lon&quot;:181,&quot;ship_cog&quot;:360.0,&quot;ship_sog&quot;:102.3,&quot;ship_type&quot;:0}"></div>
	</body>
	</html>`
	vessel, err := ParseDetail(body, "https://www.vesselfinder.com/vessels/details/451283903", time.Date(2026, 5, 6, 10, 15, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("ParseDetail returned error: %v", err)
	}
	if vessel.Name != "KNS12839-03-92" || vessel.MMSI != "451283903" || vessel.VesselType != "Unknown" {
		t.Fatalf("unexpected metadata: %#v", vessel)
	}
	if vessel.Latitude != nil || vessel.Longitude != nil || vessel.SpeedKPH != nil || vessel.CourseDeg != nil {
		t.Fatalf("sentinel djson position should be ignored, got %#v", vessel)
	}
}

func TestParseDetailExtractsValidDJSONPosition(t *testing.T) {
	body := `
	<html>
	<head><title>BRAVO STAR, Cargo ship - Details and current position - MMSI 538009877 - VesselFinder</title></head>
	<body>
	  <h1>BRAVO STAR</h1>
	  <div id="djson" data-json="{ &quot;mmsi&quot;:538009877,&quot;ship_lat&quot;:25.2472,&quot;ship_lon&quot;:56.3575,&quot;ship_cog&quot;:134.2,&quot;ship_sog&quot;:12.3,&quot;ship_type&quot;:70}"></div>
	</body>
	</html>`
	vessel, err := ParseDetail(body, "https://www.vesselfinder.com/vessels/details/538009877", time.Date(2026, 5, 6, 10, 15, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("ParseDetail returned error: %v", err)
	}
	if vessel.MMSI != "538009877" || vessel.VesselType != "Cargo ship" {
		t.Fatalf("unexpected metadata: %#v", vessel)
	}
	if vessel.Latitude == nil || vessel.Longitude == nil || *vessel.Latitude != 25.2472 || *vessel.Longitude != 56.3575 {
		t.Fatalf("unexpected djson coordinates: lat=%v lon=%v", vessel.Latitude, vessel.Longitude)
	}
	if vessel.SpeedKPH == nil || *vessel.SpeedKPH < 22.77 || *vessel.SpeedKPH > 22.78 {
		t.Fatalf("expected djson ship_sog converted to kph, got %v", vessel.SpeedKPH)
	}
	if vessel.CourseDeg == nil || *vessel.CourseDeg != 134.2 {
		t.Fatalf("unexpected djson course: %v", vessel.CourseDeg)
	}
}

func TestBotAndMissingPagesReturnTypedErrors(t *testing.T) {
	if _, err := ParseDetail("<html>Checking if the site connection is secure</html>", "https://www.vesselfinder.com/vessels/details/1", time.Now()); !IsBotPageError(err) {
		t.Fatalf("expected bot page error, got %v", err)
	}
	if _, err := ParseDetail("<html><h1>Vessel Not Found</h1></html>", "https://www.vesselfinder.com/vessels/details/1", time.Now()); !IsMissingVesselError(err) {
		t.Fatalf("expected missing vessel error, got %v", err)
	}
}

func TestParseDetailDoesNotTreatCloudflareScriptReferenceAsBotPage(t *testing.T) {
	html := strings.Replace(detailHTML, "</body>", `<script src="https://static.example/cloudflare-insights.min.js"></script></body>`, 1)
	if _, err := ParseDetail(html, "https://www.vesselfinder.com/vessels/details/9303801", time.Now()); err != nil {
		t.Fatalf("expected valid detail page with cloudflare script reference, got %v", err)
	}
}

func TestIsBotPage(t *testing.T) {
	cases := []struct {
		html string
		want bool
	}{
		{"<html><body>checking if the site connection is secure</body></html>", true},
		{"<html><body>verify you are human</body></html>", true},
		{"<html><body>cf-challenge</body></html>", true},
		{"<html><body>g-recaptcha</body></html>", true},
		{"<html><body>h-captcha</body></html>", true},
		{"<html><body><h1>MMSI: 123456789</h1></body></html>", false},
		{"", false},
	}
	for _, c := range cases {
		got := IsBotPage(c.html)
		if got != c.want {
			t.Errorf("IsBotPage(%q) = %v, want %v", c.html, got, c.want)
		}
	}
}

func TestMetadataFingerprintAndDiffAreStable(t *testing.T) {
	now := time.Date(2026, 5, 6, 10, 0, 0, 0, time.UTC)
	old := VesselMetadata{DetailID: "1", Name: "ALPHA", IMO: "9303801", MMSI: "538009877", Flag: "Panama", VesselType: "Cargo", ObservedAt: now}
	newer := old
	newer.Flag = "Marshall Islands"
	newer.Status = "Underway"

	fp1 := MetadataFingerprint(old)
	fp2 := MetadataFingerprint(old)
	if fp1 != fp2 {
		t.Fatal("fingerprint is not stable for identical metadata")
	}
	if MetadataFingerprint(old) == MetadataFingerprint(newer) {
		t.Fatal("fingerprint did not change after metadata changed")
	}
	changes := DiffFields(old, newer)
	want := []FieldChange{
		{Field: "flag", OldValue: "Panama", NewValue: "Marshall Islands"},
		{Field: "status", OldValue: "", NewValue: "Underway"},
	}
	if !reflect.DeepEqual(changes, want) {
		t.Fatalf("changes mismatch\n got: %#v\nwant: %#v", changes, want)
	}
}
