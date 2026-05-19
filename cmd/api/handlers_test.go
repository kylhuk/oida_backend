package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

type stubQuerier struct {
	queryFn func(ctx context.Context, query string) (string, error)
}

func (s stubQuerier) Query(ctx context.Context, query string) (string, error) {
	return s.queryFn(ctx, query)
}

// wrapFormatJSON converts a newline-delimited JSON body (JSONEachRow) into a
// ClickHouse FORMAT JSON envelope with rows_before_limit_at_least set to the
// number of rows. It is used in test stubs to simulate the FORMAT JSON response
// that queryResourceListEnvelope expects.
func wrapFormatJSON(ndjson string) string {
	lines := strings.Split(strings.TrimSpace(ndjson), "\n")
	rowCount := uint64(0)
	rowsJSON := make([]string, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		rowsJSON = append(rowsJSON, line)
		rowCount++
	}
	return `{"data":[` + strings.Join(rowsJSON, ",") + `],"rows":` + strconv.FormatUint(rowCount, 10) + `,"rows_before_limit_at_least":` + strconv.FormatUint(rowCount, 10) + `}`
}

func TestAPICoreContracts(t *testing.T) {
	TestAPIExpandedContracts(t)
}

func TestAPIExpandedContracts(t *testing.T) {
	// isFmtJSON returns true when the list query uses FORMAT JSON (not JSONEachRow).
	// Detail queries use JSONEachRow; list queries use FORMAT JSON after the refactor.
	isFmtJSON := func(q string) bool {
		return strings.HasSuffix(strings.TrimSpace(q), "FORMAT JSON")
	}
	maybeWrap := func(ndjson string, q string) string {
		if isFmtJSON(q) {
			return wrapFormatJSON(ndjson)
		}
		return ndjson
	}

	mux := newAPIMuxWithServer("v1", "", serverWithTestAuth(&apiServer{
		version: "v1",
		clickhouse: stubQuerier{queryFn: func(ctx context.Context, query string) (string, error) {
			switch {
			case strings.Contains(query, "FROM gold.api_v1_jobs"):
				job1 := `{"job_id":"job:1","job_type":"place-build","status":"succeeded","started_at":"2026-03-10T09:00:00Z","finished_at":"2026-03-10T09:01:00Z","message":"done","stats":"{\"places\":4}"}`
				job2 := `{"job_id":"job:2","job_type":"ingest-geopolitical","status":"running","started_at":"2026-03-10T10:00:00Z","finished_at":null,"message":"running","stats":"{\"events\":1}"}`
				switch {
				case strings.Contains(query, "WHERE job_id = 'job:1'"):
					return job1 + "\n", nil
				case strings.Contains(query, "job_id > 'job:1'"):
					return maybeWrap(job2+"\n", query), nil
				default:
					return maybeWrap(job1+"\n"+job2+"\n", query), nil
				}
			case strings.Contains(query, "FROM gold.api_v1_source_coverage"):
				row := `{"coverage_id":"src:001:coverage","source_id":"src:001","scope_type":"source","scope_id":"src:001","geo_scope":"global","place_count":2,"event_count":1,"coverage_state":"silver_landed","reason":"promoted rows observed","updated_at":"2026-03-10T08:00:00Z"}` + "\n"
				return maybeWrap(row, query), nil
			case strings.Contains(query, "FROM gold.api_v1_sources"):
				src1 := `{"source_id":"src:001","domain":"example.com","domain_family":"web","source_class":"news","entrypoints":["https://example.com/feed"],"auth_mode":"none","auth_config_json":"{}","format_hint":"rss","robots_policy":"honor","refresh_strategy":"poll","requests_per_minute":60,"burst_size":10,"retention_class":"warm","license":"CC-BY","terms_url":"https://example.com/terms","attribution_required":1,"geo_scope":"global","priority":10,"parser_id":"parser-rss","entity_types":["org"],"expected_place_types":["country"],"supports_historical":1,"supports_delta":1,"backfill_priority":5,"confidence_baseline":0.9,"enabled":1,"disabled_reason":null,"disabled_at":null,"disabled_by":null,"review_status":"approved","review_notes":"","schema_version":1,"record_version":1,"api_contract_version":1,"updated_at":"2026-03-10T08:00:00Z","attrs":"{}","evidence":"[]"}`
				src2 := `{"source_id":"src:002","domain":"example.org","domain_family":"web","source_class":"bulletin","entrypoints":[],"auth_mode":"none","auth_config_json":"{}","format_hint":"json","robots_policy":"honor","refresh_strategy":"poll","requests_per_minute":30,"burst_size":5,"retention_class":"warm","license":"public","terms_url":"","attribution_required":0,"geo_scope":"regional","priority":20,"parser_id":"parser-json","entity_types":[],"expected_place_types":[],"supports_historical":0,"supports_delta":1,"backfill_priority":10,"confidence_baseline":0.7,"enabled":1,"disabled_reason":null,"disabled_at":null,"disabled_by":null,"review_status":"approved","review_notes":"","schema_version":1,"record_version":2,"api_contract_version":1,"updated_at":"2026-03-10T08:05:00Z","attrs":"{}","evidence":"[]"}`
				if strings.Contains(query, "WHERE source_id = 'src:001'") {
					return src1 + "\n", nil
				}
				return maybeWrap(src1+"\n"+src2+"\n", query), nil
			case strings.Contains(query, "GROUP BY place_type"):
				return `{"kind":"place","data_class":"admin0","count":10}` + "\n" + `{"kind":"place","data_class":"admin1","count":5}` + "\n", nil
			case strings.Contains(query, "FROM gold.api_v1_places"):
				root := `{"place_id":"plc:001","parent_place_id":null,"canonical_name":"Ukraine","place_type":"admin0","admin_level":0,"country_code":"UA","continent_code":"EU","source_place_key":"ua","source_system":"fixture","status":"active","centroid_lat":48.3,"centroid_lon":31.1,"bbox_min_lat":44.0,"bbox_min_lon":22.0,"bbox_max_lat":52.0,"bbox_max_lon":40.0,"valid_from":"2026-03-01T00:00:00Z","valid_to":null,"schema_version":1,"record_version":1,"api_contract_version":1,"updated_at":"2026-03-10T08:00:00Z","attrs":"{}","evidence":"[]"}`
				child := `{"place_id":"plc:002","parent_place_id":"plc:001","canonical_name":"Kyiv","place_type":"admin1","admin_level":1,"country_code":"UA","continent_code":"EU","source_place_key":"ua-30","source_system":"fixture","status":"active","centroid_lat":50.45,"centroid_lon":30.52,"bbox_min_lat":50.0,"bbox_min_lon":30.0,"bbox_max_lat":51.0,"bbox_max_lon":31.0,"valid_from":"2026-03-01T00:00:00Z","valid_to":null,"schema_version":1,"record_version":1,"api_contract_version":1,"updated_at":"2026-03-10T08:00:00Z","attrs":"{}","evidence":"[]"}`
				switch {
				case strings.Contains(query, "WHERE place_id = 'plc:001'"):
					return root + "\n", nil
				case strings.Contains(query, "parent_place_id = 'plc:001'"):
					return maybeWrap(child+"\n", query), nil
				case strings.Contains(query, "does-not-exist"):
					return maybeWrap("", query), nil
				case strings.Contains(query, "Kyiv"):
					return maybeWrap(child+"\n", query), nil
				default:
					return maybeWrap(root+"\n"+child+"\n", query), nil
				}
			case strings.Contains(query, "GROUP BY entity_type"):
				return `{"kind":"entity","data_class":"organization","count":4}` + "\n" + `{"kind":"entity","data_class":"vessel","count":2}` + "\n", nil
			case strings.Contains(query, "FROM gold.api_v1_entities"):
				entity1 := `{"entity_id":"ent:001","entity_type":"organization","canonical_name":"Relief Cluster","status":"active","risk_band":"medium","primary_place_id":"plc:002","source_system":"fixture","valid_from":"2026-03-01T00:00:00Z","valid_to":null,"schema_version":1,"record_version":1,"api_contract_version":1,"updated_at":"2026-03-10T08:00:00Z","attrs":"{}","evidence":"[]"}`
				entity2 := `{"entity_id":"ent:002","entity_type":"vessel","canonical_name":"MV Aurora","status":"active","risk_band":"high","primary_place_id":"plc:001","source_system":"fixture","valid_from":"2026-03-01T00:00:00Z","valid_to":null,"schema_version":1,"record_version":1,"api_contract_version":1,"updated_at":"2026-03-10T08:10:00Z","attrs":"{}","evidence":"[]"}`
				switch {
				case strings.Contains(query, "WHERE entity_id = 'ent:001'"):
					return entity1 + "\n", nil
				case strings.Contains(query, "does-not-exist"):
					return maybeWrap("", query), nil
				case strings.Contains(query, "Aurora"):
					return maybeWrap(entity2+"\n", query), nil
				default:
					return maybeWrap(entity1+"\n"+entity2+"\n", query), nil
				}
			case strings.Contains(query, "FROM gold.api_v1_tracks"):
				row := `{"track_record_id":"trk:001","track_id":"track:aurora","track_type":"maritime","entity_id":"ent:002","place_id":"plc:001","from_place_id":"plc:001","to_place_id":"plc:002","started_at":"2026-03-09T09:00:00Z","ended_at":"2026-03-09T11:00:00Z","distance_km":120.5,"point_count":16,"avg_speed_kph":60.2}` + "\n"
				return maybeWrap(row, query), nil
			case strings.Contains(query, "FROM gold.api_v1_entity_events"):
				row := `{"entity_id":"ent:001","event_id":"evt:001","event_type":"humanitarian_access","event_subtype":"checkpoint_delay","place_id":"plc:002","starts_at":"2026-03-10T07:30:00Z","status":"open","confidence_band":"high","impact_score":0.78}` + "\n"
				return maybeWrap(row, query), nil
			case strings.Contains(query, "FROM gold.api_v1_entity_places"):
				row := `{"entity_id":"ent:001","place_id":"plc:002","canonical_name":"Kyiv","place_type":"admin1","relation_type":"operates_in","linked_at":"2026-03-10T08:00:00Z"}` + "\n"
				return maybeWrap(row, query), nil
			case strings.Contains(query, "FROM gold.api_v1_events"):
				event := `{"event_id":"evt:001","source_id":"src:001","event_type":"humanitarian_access","event_subtype":"checkpoint_delay","place_id":"plc:002","parent_place_chain":["plc:001"],"starts_at":"2026-03-10T07:30:00Z","ends_at":null,"status":"open","confidence_band":"high","impact_score":0.78,"schema_version":1,"attrs":"{}","evidence":"[]"}`
				if strings.Contains(query, "WHERE event_id = 'evt:missing'") {
					return maybeWrap("", query), nil
				}
				return maybeWrap(event+"\n", query), nil
			case strings.Contains(query, "FROM gold.api_v1_observations"):
				row := `{"observation_id":"obs:001","source_id":"src:001","subject_type":"place","subject_id":"plc:001","observation_type":"media_attention","place_id":"plc:001","parent_place_chain":["plc:world"],"observed_at":"2026-03-10T06:00:00Z","published_at":"2026-03-10T06:05:00Z","confidence_band":"medium","measurement_unit":"score","measurement_value":42.5,"schema_version":1,"attrs":"{}","evidence":"[]"}` + "\n"
				return maybeWrap(row, query), nil
			case strings.Contains(query, "FROM gold.api_v1_metrics"):
				metric := `{"metric_id":"media_attention_score","metric_family":"geopolitical","subject_grain":"place","unit":"score","value_type":"gauge","rollup_engine":"snapshot","rollup_rule":"latest","enabled":1,"updated_at":"2026-03-10T08:30:00Z","attrs":"{}","evidence":"[]"}`
				if strings.Contains(query, "WHERE metric_id = 'metric:missing'") {
					return "", nil
				}
				return maybeWrap(metric+"\n", query), nil
			case strings.Contains(query, "FROM gold.api_v1_metric_rollups"):
				row := `{"snapshot_id":"snap:001","metric_id":"media_attention_score","subject_grain":"place","subject_id":"plc:001","place_id":"plc:001","window_grain":"day","window_start":"2026-03-10T00:00:00Z","window_end":"2026-03-10T23:59:59Z","snapshot_at":"2026-03-10T08:30:00Z","metric_value":42.5,"metric_delta":5.2,"rank":1,"attrs":"{}","evidence":"[]"}` + "\n"
				return maybeWrap(row, query), nil
			case strings.Contains(query, "FROM gold.api_v1_time_series"):
				row := `{"point_id":"point:001","metric_id":"media_attention_score","subject_grain":"place","subject_id":"plc:001","place_id":"plc:001","window_grain":"day","window_start":"2026-03-10T00:00:00Z","window_end":"2026-03-10T23:59:59Z","snapshot_at":"2026-03-10T08:30:00Z","metric_value":42.5,"metric_delta":5.2,"rank":1}` + "\n"
				return maybeWrap(row, query), nil
			case strings.Contains(query, "FROM gold.api_v1_hotspots"):
				row := `{"hotspot_id":"hot:001","metric_id":"media_attention_score","scope_type":"admin0","scope_id":"plc:001","place_id":"plc:002","snapshot_at":"2026-03-10T08:35:00Z","window_grain":"day","window_start":"2026-03-10T00:00:00Z","window_end":"2026-03-10T23:59:59Z","rank":1,"hotspot_score":88.4,"attrs":"{}","evidence":"[]"}` + "\n"
				return maybeWrap(row, query), nil
			case strings.Contains(query, "FROM gold.api_v1_cross_domain"):
				row := `{"cross_domain_id":"cross:001","subject_grain":"place","subject_id":"plc:001","place_id":"plc:001","domains":["geopolitical","humanitarian"],"composite_score":64.8,"snapshot_at":"2026-03-10T08:40:00Z","metric_ids":["media_attention_score"],"attrs":"{}","evidence":"[]"}` + "\n"
				return maybeWrap(row, query), nil
			default:
				t.Fatalf("unexpected query: %s", query)
				return "", nil
			}
		}},
		queryTimeout: time.Second,
	}))

	ts := httptest.NewServer(mux)
	defer ts.Close()

	t.Run("pagination and fields filtering", func(t *testing.T) {
		resp := mustAPIRequest(t, ts.URL+"/v1/jobs?limit=1&fields=job_id,status")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200 got %d", resp.StatusCode)
		}
		payload := decodePayload(t, resp)
		data := payload["data"].(map[string]any)
		items := data["items"].([]any)
		if len(items) != 1 {
			t.Fatalf("expected 1 item got %d", len(items))
		}
		item := items[0].(map[string]any)
		if len(item) != 2 || item["job_id"] != "job:1" {
			t.Fatalf("unexpected projected job: %#v", item)
		}
		nextCursor, ok := data["next_cursor"].(string)
		if !ok || nextCursor == "" {
			t.Fatal("expected next_cursor")
		}

		resp = mustAPIRequest(t, ts.URL+"/v1/jobs?limit=1&cursor="+nextCursor)
		payload = decodePayload(t, resp)
		items = payload["data"].(map[string]any)["items"].([]any)
		if len(items) != 1 || items[0].(map[string]any)["job_id"] != "job:2" {
			t.Fatalf("unexpected second page: %#v", items)
		}
	})

	t.Run("expanded filters and search pagination", func(t *testing.T) {
		resp := mustAPIRequest(t, ts.URL+"/v1/sources?limit=1")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200 got %d", resp.StatusCode)
		}
		payload := decodePayload(t, resp)
		source := payload["data"].(map[string]any)["items"].([]any)[0].(map[string]any)
		if _, ok := source["enabled"].(bool); !ok {
			t.Fatalf("expected enabled boolean, got %T", source["enabled"])
		}
		if _, ok := source["supports_delta"].(bool); !ok {
			t.Fatalf("expected supports_delta boolean, got %T", source["supports_delta"])
		}

		resp = mustAPIRequest(t, ts.URL+"/v1/metrics?limit=1")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200 got %d", resp.StatusCode)
		}
		payload = decodePayload(t, resp)
		metric := payload["data"].(map[string]any)["items"].([]any)[0].(map[string]any)
		if _, ok := metric["enabled"].(bool); !ok {
			t.Fatalf("expected metric enabled boolean, got %T", metric["enabled"])
		}

		resp = mustAPIRequest(t, ts.URL+"/v1/entities?entity_type=vessel&q=Aurora&fields=entity_id,entity_type&limit=1")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200 got %d", resp.StatusCode)
		}
		payload = decodePayload(t, resp)
		data := payload["data"].(map[string]any)
		items := data["items"].([]any)
		if len(items) != 1 {
			t.Fatalf("expected 1 entity got %d", len(items))
		}
		entity := items[0].(map[string]any)
		if entity["entity_id"] != "ent:002" || entity["entity_type"] != "vessel" {
			t.Fatalf("unexpected entity payload %#v", entity)
		}

		resp = mustAPIRequest(t, ts.URL+"/v1/analytics/rollups?metric_id=media_attention_score&fields=snapshot_id,metric_id,rank,metric_value")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200 got %d", resp.StatusCode)
		}
		payload = decodePayload(t, resp)
		rollup := payload["data"].(map[string]any)["items"].([]any)[0].(map[string]any)
		if rollup["snapshot_id"] != "snap:001" || rollup["metric_id"] != "media_attention_score" {
			t.Fatalf("unexpected rollup payload %#v", rollup)
		}
		if _, ok := rollup["rank"].(float64); !ok {
			t.Fatalf("expected rank numeric, got %T", rollup["rank"])
		}
		if _, ok := rollup["metric_value"].(float64); !ok {
			t.Fatalf("expected metric_value numeric, got %T", rollup["metric_value"])
		}

		resp = mustAPIRequest(t, ts.URL+"/v1/search/places?place_type=admin1&q=Kyiv&fields=place_id,canonical_name")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200 got %d", resp.StatusCode)
		}
		payload = decodePayload(t, resp)
		place := payload["data"].(map[string]any)["items"].([]any)[0].(map[string]any)
		if place["place_id"] != "plc:002" || place["canonical_name"] != "Kyiv" {
			t.Fatalf("unexpected place payload %#v", place)
		}

		resp = mustAPIRequest(t, ts.URL+"/v1/search?q=ua&limit=1&fields=kind,canonical_name")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200 got %d", resp.StatusCode)
		}
		payload = decodePayload(t, resp)
		data = payload["data"].(map[string]any)
		items = data["items"].([]any)
		if len(items) != 1 {
			t.Fatalf("expected 1 combined search result got %d", len(items))
		}
		firstItem := items[0].(map[string]any)
		nextCursor, ok := data["next_cursor"].(string)
		if !ok || nextCursor == "" {
			t.Fatal("expected next_cursor for combined search")
		}

		resp = mustAPIRequest(t, ts.URL+"/v1/search?q=ua&limit=1&fields=kind,canonical_name&cursor="+nextCursor)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200 got %d", resp.StatusCode)
		}
		payload = decodePayload(t, resp)
		items = payload["data"].(map[string]any)["items"].([]any)
		if len(items) != 1 {
			t.Fatalf("expected 1 paged combined result got %d", len(items))
		}
		if items[0].(map[string]any)["canonical_name"] == firstItem["canonical_name"] {
			t.Fatalf("expected second page to advance, got %#v then %#v", firstItem, items[0])
		}
	})

	t.Run("nested entity routes stay scoped and analytics payloads keep frontend shape", func(t *testing.T) {
		resp := mustAPIRequest(t, ts.URL+"/v1/entities/ent:001/events")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200 got %d", resp.StatusCode)
		}
		payload := decodePayload(t, resp)
		eventItems := payload["data"].(map[string]any)["items"].([]any)
		if len(eventItems) == 0 {
			t.Fatal("expected entity events items")
		}
		eventItem := eventItems[0].(map[string]any)
		if eventItem["entity_id"] != "ent:001" || eventItem["event_id"] == nil {
			t.Fatalf("unexpected entity event payload %#v", eventItem)
		}

		resp = mustAPIRequest(t, ts.URL+"/v1/entities/ent:001/places")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200 got %d", resp.StatusCode)
		}
		payload = decodePayload(t, resp)
		placeItems := payload["data"].(map[string]any)["items"].([]any)
		if len(placeItems) == 0 {
			t.Fatal("expected entity places items")
		}
		placeItem := placeItems[0].(map[string]any)
		if placeItem["entity_id"] != "ent:001" || placeItem["place_id"] != "plc:002" {
			t.Fatalf("unexpected entity place payload %#v", placeItem)
		}

		resp = mustAPIRequest(t, ts.URL+"/v1/entities/ent:002/tracks")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200 got %d", resp.StatusCode)
		}
		payload = decodePayload(t, resp)
		trackItems := payload["data"].(map[string]any)["items"].([]any)
		if len(trackItems) == 0 {
			t.Fatal("expected entity tracks items")
		}
		trackItem := trackItems[0].(map[string]any)
		if trackItem["entity_id"] != "ent:002" || trackItem["track_record_id"] == nil {
			t.Fatalf("unexpected entity track payload %#v", trackItem)
		}

		resp = mustAPIRequest(t, ts.URL+"/v1/analytics/rollups?metric_id=media_attention_score")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200 got %d", resp.StatusCode)
		}
		payload = decodePayload(t, resp)
		rollupItems := payload["data"].(map[string]any)["items"].([]any)
		if len(rollupItems) == 0 {
			t.Fatal("expected metric rollup items")
		}
		rollupItem := rollupItems[0].(map[string]any)
		if _, ok := rollupItem["metric_value"].(float64); !ok {
			t.Fatalf("expected metric_value numeric, got %T", rollupItem["metric_value"])
		}
		if _, ok := rollupItem["rank"].(float64); !ok {
			t.Fatalf("expected rank numeric, got %T", rollupItem["rank"])
		}
		if _, ok := rollupItem["attrs"].(map[string]any); !ok {
			t.Fatalf("expected attrs object, got %T", rollupItem["attrs"])
		}
		if _, ok := rollupItem["evidence"].([]any); !ok {
			t.Fatalf("expected evidence array, got %T", rollupItem["evidence"])
		}

		resp = mustAPIRequest(t, ts.URL+"/v1/analytics/cross-domain")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200 got %d", resp.StatusCode)
		}
		payload = decodePayload(t, resp)
		crossDomainItems := payload["data"].(map[string]any)["items"].([]any)
		if len(crossDomainItems) == 0 {
			t.Fatal("expected cross-domain items")
		}
		crossDomainItem := crossDomainItems[0].(map[string]any)
		if _, ok := crossDomainItem["domains"].([]any); !ok {
			t.Fatalf("expected domains array, got %T", crossDomainItem["domains"])
		}
		if _, ok := crossDomainItem["metric_ids"].([]any); !ok {
			t.Fatalf("expected metric_ids array, got %T", crossDomainItem["metric_ids"])
		}
	})

	for _, tc := range []struct {
		name string
		path string
		kind string
		key  string
	}{
		{name: "job detail", path: "/v1/jobs/job:1", kind: "job", key: "job_id"},
		{name: "source coverage", path: "/v1/sources/src:001/coverage", kind: "source_coverage", key: "coverage_id"},
		{name: "place children", path: "/v1/places/plc:001/children", kind: "place_children", key: "place_id"},
		{name: "place metrics", path: "/v1/places/plc:001/metrics", kind: "place_metrics", key: "snapshot_id"},
		{name: "place events", path: "/v1/places/plc:002/events", kind: "place_events", key: "event_id"},
		{name: "place observations", path: "/v1/places/plc:001/observations", kind: "place_observations", key: "observation_id"},
		{name: "entity list", path: "/v1/entities", kind: "entities", key: "entity_id"},
		{name: "entity detail", path: "/v1/entities/ent:001", kind: "entity", key: "entity_id"},
		{name: "entity tracks", path: "/v1/entities/ent:002/tracks", kind: "entity_tracks", key: "track_record_id"},
		{name: "entity events", path: "/v1/entities/ent:001/events", kind: "entity_events", key: "event_id"},
		{name: "entity places", path: "/v1/entities/ent:001/places", kind: "entity_places", key: "place_id"},
		{name: "metric list", path: "/v1/metrics", kind: "metrics", key: "metric_id"},
		{name: "metric detail", path: "/v1/metrics/media_attention_score", kind: "metric", key: "metric_id"},
		{name: "rollups", path: "/v1/analytics/rollups", kind: "metric_rollups", key: "snapshot_id"},
		{name: "time series", path: "/v1/analytics/time-series", kind: "metric_time_series", key: "point_id"},
		{name: "hotspots", path: "/v1/analytics/hotspots", kind: "metric_hotspots", key: "hotspot_id"},
		{name: "cross domain", path: "/v1/analytics/cross-domain", kind: "metric_cross_domain", key: "cross_domain_id"},
		{name: "search classes", path: "/v1/search/classes", kind: "classes", key: "data_class"},
		{name: "search places", path: "/v1/search/places?q=Kyiv", kind: "search_places", key: "place_id"},
		{name: "search entities", path: "/v1/search/entities?q=Aurora", kind: "search_entities", key: "entity_id"},
		{name: "search combined", path: "/v1/search?q=ua", kind: "search", key: "kind"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp := mustAPIRequest(t, ts.URL+tc.path)
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("expected 200 got %d", resp.StatusCode)
			}
			payload := decodePayload(t, resp)
			data := payload["data"].(map[string]any)
			if data["kind"] != tc.kind {
				t.Fatalf("expected kind %q got %#v", tc.kind, data["kind"])
			}
			if item, ok := data["item"].(map[string]any); ok {
				if item[tc.key] == nil {
					t.Fatalf("expected detail key %q in %#v", tc.key, item)
				}
				return
			}
			items := data["items"].([]any)
			if len(items) == 0 {
				t.Fatal("expected items")
			}
			first := items[0].(map[string]any)
			if first[tc.key] == nil {
				t.Fatalf("expected key %q in %#v", tc.key, first)
			}
			// All list routes must include total_count.
			totalCount := data["total_count"]
			if totalCount == nil {
				t.Fatalf("expected total_count in list response, got nil")
			}
			if _, ok := totalCount.(float64); !ok {
				t.Fatalf("expected total_count to be numeric, got %T: %v", totalCount, totalCount)
			}
		})
	}

	t.Run("search entities requires q", func(t *testing.T) {
		resp := mustAPIRequest(t, ts.URL+"/v1/search/entities")
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("expected 400 got %d", resp.StatusCode)
		}
		payload := decodePayload(t, resp)
		data, ok := payload["data"].(map[string]any)
		if !ok {
			t.Fatalf("expected data object, got %#v", payload)
		}
		errObj, ok := data["error"].(map[string]any)
		if !ok {
			t.Fatalf("expected error object, got %#v", data)
		}
		if errObj["code"] != "invalid_request" {
			t.Fatalf("expected invalid_request code, got %#v", errObj["code"])
		}
		if msg, _ := errObj["message"].(string); !strings.Contains(msg, "q is required") {
			t.Fatalf("expected message to contain 'q is required', got %q", msg)
		}
	})

	t.Run("search places requires q", func(t *testing.T) {
		resp := mustAPIRequest(t, ts.URL+"/v1/search/places")
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("expected 400 got %d", resp.StatusCode)
		}
		payload := decodePayload(t, resp)
		data, ok := payload["data"].(map[string]any)
		if !ok {
			t.Fatalf("expected data object, got %#v", payload)
		}
		errObj, ok := data["error"].(map[string]any)
		if !ok {
			t.Fatalf("expected error object, got %#v", data)
		}
		if errObj["code"] != "invalid_request" {
			t.Fatalf("expected invalid_request code, got %#v", errObj["code"])
		}
		if msg, _ := errObj["message"].(string); !strings.Contains(msg, "q is required") {
			t.Fatalf("expected message to contain 'q is required', got %q", msg)
		}
	})

	t.Run("search classes merges seed metadata", func(t *testing.T) {
		seededServer := serverWithTestAuth(&apiServer{
			version: "v1",
			clickhouse: stubQuerier{queryFn: func(ctx context.Context, query string) (string, error) {
				switch {
				case strings.Contains(query, "GROUP BY entity_type"):
					return `{"kind":"entity","data_class":"vessel","count":2}` + "\n", nil
				case strings.Contains(query, "GROUP BY place_type"):
					return `{"kind":"place","data_class":"admin0","count":10}` + "\n", nil
				default:
					t.Fatalf("unexpected query in search classes seed test: %s", query)
					return "", nil
				}
			}},
			queryTimeout: time.Second,
			dataClasses: map[string]dataClassEntry{
				"entity:vessel": {Kind: "entity", DataClass: "vessel", Category: "Assets", Description: "Maritime vessels"},
			},
		})
		seededMux := newAPIMuxWithServer("v1", "", seededServer)
		seededTS := httptest.NewServer(seededMux)
		defer seededTS.Close()

		resp := mustAPIRequest(t, seededTS.URL+"/v1/search/classes")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200 got %d", resp.StatusCode)
		}
		payload := decodePayload(t, resp)
		data := payload["data"].(map[string]any)
		if data["kind"] != "classes" {
			t.Fatalf("expected kind 'classes' got %#v", data["kind"])
		}
		items := data["items"].([]any)
		if len(items) != 2 {
			t.Fatalf("expected 2 items got %d", len(items))
		}
		totalCount, ok := data["total_count"].(float64)
		if !ok || totalCount != float64(len(items)) {
			t.Fatalf("expected total_count %d got %#v", len(items), data["total_count"])
		}

		var vesselItem, admin0Item map[string]any
		for _, raw := range items {
			item := raw.(map[string]any)
			switch item["data_class"] {
			case "vessel":
				vesselItem = item
			case "admin0":
				admin0Item = item
			}
		}
		if vesselItem == nil || admin0Item == nil {
			t.Fatalf("expected vessel and admin0 items, got %#v", items)
		}
		if vesselItem["category"] != "Assets" {
			t.Fatalf("expected vessel category 'Assets' got %#v", vesselItem["category"])
		}
		if vesselItem["description"] != "Maritime vessels" {
			t.Fatalf("expected vessel description 'Maritime vessels' got %#v", vesselItem["description"])
		}
		if admin0Item["category"] != nil {
			t.Fatalf("expected admin0 to have no category, got %#v", admin0Item["category"])
		}
		if admin0Item["description"] != nil {
			t.Fatalf("expected admin0 to have no description, got %#v", admin0Item["description"])
		}
	})

	t.Run("schema contract exposes auth params fields metadata", func(t *testing.T) {
		resp := mustAPIRequest(t, ts.URL+"/v1/schema")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200 got %d", resp.StatusCode)
		}
		payload := decodePayload(t, resp)
		endpoints, ok := payload["data"].(map[string]any)["endpoints"].([]any)
		if !ok {
			t.Fatalf("schema endpoints missing or wrong type: %#v", payload)
		}
		if len(endpoints) != 36 {
			t.Fatalf("expected 36 endpoints, got %d", len(endpoints))
		}

		var metricsEndpoint map[string]any
		var versionEndpoint map[string]any
		var searchEndpoint map[string]any
		for _, endpoint := range endpoints {
			typed, _ := endpoint.(map[string]any)
			if typed["path"] == "/v1/metrics" {
				metricsEndpoint = typed
			}
			if typed["path"] == "/v1/version" {
				versionEndpoint = typed
			}
			if typed["path"] == "/v1/search" {
				searchEndpoint = typed
			}
		}
		if metricsEndpoint == nil || versionEndpoint == nil || searchEndpoint == nil {
			t.Fatalf("expected key schema endpoints to exist")
		}
		metricsAuth := metricsEndpoint["auth"].(map[string]any)
		if metricsAuth["required"] != true || metricsAuth["header"] != apiKeyHeader {
			t.Fatalf("unexpected metrics auth metadata: %#v", metricsAuth)
		}
		limitMeta := metricsEndpoint["query"].(map[string]any)["limit"].(map[string]any)
		if limitMeta["default"] != float64(defaultPageLimit) || limitMeta["max"] != float64(maxPageLimit) {
			t.Fatalf("unexpected metrics limit metadata: %#v", limitMeta)
		}
		metricFields := metricsEndpoint["fields"].(map[string]any)["selectable"].([]any)
		if len(metricFields) == 0 || metricFields[0] == nil {
			t.Fatalf("expected selectable fields for metrics endpoint")
		}

		versionAuth := versionEndpoint["auth"].(map[string]any)
		if versionAuth["required"] != false {
			t.Fatalf("version endpoint must be public, got %#v", versionAuth)
		}

		searchResponse := searchEndpoint["response"].(map[string]any)
		if searchResponse["container"] != "items" {
			t.Fatalf("search endpoint container mismatch: %#v", searchResponse)
		}
	})
}

func TestAPIExpandedEdgeCases(t *testing.T) {
	mux := newAPIMuxWithServer("v1", "", serverWithTestAuth(&apiServer{
		version: "v1",
		clickhouse: stubQuerier{queryFn: func(ctx context.Context, query string) (string, error) {
			switch {
			case strings.Contains(query, "FROM gold.api_v1_metrics") && strings.Contains(query, "metric:missing"):
				return "", nil
			case strings.Contains(query, "FROM gold.api_v1_entities") && strings.Contains(query, "does-not-exist"):
				return "", nil
			default:
				return "", nil
			}
		}},
		queryTimeout: time.Second,
	}))

	ts := httptest.NewServer(mux)
	defer ts.Close()

	t.Run("bad cursor rejected", func(t *testing.T) {
		resp := mustAPIRequest(t, ts.URL+"/v1/search?cursor=bad-cursor")
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("expected 400 got %d", resp.StatusCode)
		}
	})

	t.Run("unsupported filter rejected", func(t *testing.T) {
		resp := mustAPIRequest(t, ts.URL+"/v1/analytics/hotspots?unknown=value")
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("expected 400 got %d", resp.StatusCode)
		}
	})

	t.Run("detail not found contract", func(t *testing.T) {
		resp := mustAPIRequest(t, ts.URL+"/v1/metrics/metric:missing")
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("expected 404 got %d", resp.StatusCode)
		}
		payload := decodePayload(t, resp)
		errorPayload := payload["data"].(map[string]any)["error"].(map[string]any)
		if errorPayload["code"] != "not_found" {
			t.Fatalf("unexpected error payload %#v", errorPayload)
		}
	})
}

func TestOffsetPagination(t *testing.T) {
	job1 := `{"job_id":"job:1","job_type":"place-build","status":"succeeded","started_at":"2026-03-10T09:00:00Z","finished_at":"2026-03-10T09:01:00Z","message":"done","stats":"{}"}`
	job2 := `{"job_id":"job:2","job_type":"ingest-geopolitical","status":"running","started_at":"2026-03-10T10:00:00Z","finished_at":null,"message":"running","stats":"{}"}`
	isFmtJSON := func(q string) bool { return strings.HasSuffix(strings.TrimSpace(q), "FORMAT JSON") }
	maybeWrap := func(ndjson string, q string) string {
		if isFmtJSON(q) {
			return wrapFormatJSON(ndjson)
		}
		return ndjson
	}
	mux := newAPIMuxWithServer("v1", "", serverWithTestAuth(&apiServer{
		version: "v1",
		clickhouse: stubQuerier{queryFn: func(ctx context.Context, query string) (string, error) {
			switch {
			case strings.Contains(query, "FROM gold.api_v1_jobs"):
				// Simulate OFFSET: if query contains OFFSET 1, return only job2
				if strings.Contains(query, "OFFSET 1") {
					return maybeWrap(job2+"\n", query), nil
				}
				return maybeWrap(job1+"\n"+job2+"\n", query), nil
			case strings.Contains(query, "FROM gold.api_v1_tracks"):
				row := `{"track_record_id":"trk:001","track_id":"track:aurora","track_type":"maritime","entity_id":"ent:002","place_id":"plc:001","from_place_id":"plc:001","to_place_id":"plc:002","started_at":"2026-03-09T09:00:00Z","ended_at":"2026-03-09T11:00:00Z","distance_km":120.5,"point_count":16,"avg_speed_kph":60.2}` + "\n"
				return maybeWrap(row, query), nil
			case strings.Contains(query, "FROM gold.api_v1_entities"):
				entity1 := `{"entity_id":"ent:001","entity_type":"organization","canonical_name":"Relief Cluster","status":"active","risk_band":"medium","primary_place_id":"plc:002","source_system":"fixture","valid_from":"2026-03-01T00:00:00Z","valid_to":null,"schema_version":1,"record_version":1,"api_contract_version":1,"updated_at":"2026-03-10T08:00:00Z","attrs":"{}","evidence":"[]"}`
				entity2 := `{"entity_id":"ent:002","entity_type":"vessel","canonical_name":"MV Aurora","status":"active","risk_band":"high","primary_place_id":"plc:001","source_system":"fixture","valid_from":"2026-03-01T00:00:00Z","valid_to":null,"schema_version":1,"record_version":1,"api_contract_version":1,"updated_at":"2026-03-10T08:10:00Z","attrs":"{}","evidence":"[]"}`
				return maybeWrap(entity1+"\n"+entity2+"\n", query), nil
			case strings.Contains(query, "FROM gold.api_v1_places"):
				row := `{"place_id":"plc:001","parent_place_id":null,"canonical_name":"Ukraine","place_type":"admin0","admin_level":0,"country_code":"UA","continent_code":"EU","source_place_key":"ua","source_system":"fixture","status":"active","centroid_lat":48.3,"centroid_lon":31.1,"bbox_min_lat":44.0,"bbox_min_lon":22.0,"bbox_max_lat":52.0,"bbox_max_lon":40.0,"valid_from":"2026-03-01T00:00:00Z","valid_to":null,"schema_version":1,"record_version":1,"api_contract_version":1,"updated_at":"2026-03-10T08:00:00Z","attrs":"{}","evidence":"[]"}` + "\n"
				return maybeWrap(row, query), nil
			default:
				return "", nil
			}
		}},
		queryTimeout: time.Second,
	}))

	ts := httptest.NewServer(mux)
	defer ts.Close()

	t.Run("offset skips first item", func(t *testing.T) {
		resp := mustAPIRequest(t, ts.URL+"/v1/jobs?limit=10")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200 got %d", resp.StatusCode)
		}
		payload := decodePayload(t, resp)
		items := payload["data"].(map[string]any)["items"].([]any)
		if len(items) != 2 {
			t.Fatalf("expected 2 items without offset, got %d", len(items))
		}
		firstID := items[0].(map[string]any)["job_id"]

		resp2 := mustAPIRequest(t, ts.URL+"/v1/jobs?limit=10&offset=1")
		if resp2.StatusCode != http.StatusOK {
			t.Fatalf("expected 200 got %d", resp2.StatusCode)
		}
		payload2 := decodePayload(t, resp2)
		items2 := payload2["data"].(map[string]any)["items"].([]any)
		if len(items2) != 1 {
			t.Fatalf("expected 1 item with offset=1, got %d", len(items2))
		}
		if items2[0].(map[string]any)["job_id"] == firstID {
			t.Fatalf("expected offset=1 to skip first item %v, but got same item", firstID)
		}
	})

	t.Run("cursor and offset are mutually exclusive", func(t *testing.T) {
		validCursor := encodeCursor("job:1")
		resp := mustAPIRequest(t, ts.URL+"/v1/jobs?cursor="+validCursor+"&offset=1")
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("expected 400 got %d", resp.StatusCode)
		}
		payload := decodePayload(t, resp)
		data, ok := payload["data"].(map[string]any)
		if !ok {
			t.Fatalf("expected data object, got %#v", payload)
		}
		errObj, ok := data["error"].(map[string]any)
		if !ok {
			t.Fatalf("expected error object, got %#v", data)
		}
		if msg, _ := errObj["message"].(string); !strings.Contains(msg, "cursor and offset are mutually exclusive") {
			t.Fatalf("expected 'cursor and offset are mutually exclusive' message, got %q", msg)
		}
	})

	t.Run("cursor and offset=0 are mutually exclusive", func(t *testing.T) {
		validCursor := encodeCursor("job:1")
		resp := mustAPIRequest(t, ts.URL+"/v1/jobs?cursor="+validCursor+"&offset=0")
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("expected 400 got %d", resp.StatusCode)
		}
		payload := decodePayload(t, resp)
		data, ok := payload["data"].(map[string]any)
		if !ok {
			t.Fatalf("expected data object, got %#v", payload)
		}
		errObj, ok := data["error"].(map[string]any)
		if !ok {
			t.Fatalf("expected error object, got %#v", data)
		}
		if errObj["code"] != "invalid_request" {
			t.Fatalf("expected invalid_request code, got %#v", errObj["code"])
		}
		if msg, _ := errObj["message"].(string); !strings.Contains(msg, "cursor and offset are mutually exclusive") {
			t.Fatalf("expected 'cursor and offset are mutually exclusive' message, got %q", msg)
		}
	})

	t.Run("negative offset rejected", func(t *testing.T) {
		resp := mustAPIRequest(t, ts.URL+"/v1/jobs?offset=-1")
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("expected 400 got %d", resp.StatusCode)
		}
	})

	t.Run("entity tracks rejects offset parameter", func(t *testing.T) {
		resp := mustAPIRequest(t, ts.URL+"/v1/entities/ent:002/tracks?offset=1")
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("expected 400 got %d", resp.StatusCode)
		}
		payload := decodePayload(t, resp)
		data, ok := payload["data"].(map[string]any)
		if !ok {
			t.Fatalf("expected data object, got %#v", payload)
		}
		errObj, ok := data["error"].(map[string]any)
		if !ok {
			t.Fatalf("expected error object, got %#v", data)
		}
		if errObj["code"] != "invalid_request" {
			t.Fatalf("expected invalid_request code, got %#v", errObj["code"])
		}
	})

	t.Run("combined search cursor and offset are mutually exclusive", func(t *testing.T) {
		validCursor := encodeCursor("place:plc:001")
		resp := mustAPIRequest(t, ts.URL+"/v1/search?q=ua&cursor="+validCursor+"&offset=1")
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("expected 400 got %d", resp.StatusCode)
		}
		payload := decodePayload(t, resp)
		data, ok := payload["data"].(map[string]any)
		if !ok {
			t.Fatalf("expected data object, got %#v", payload)
		}
		errObj, ok := data["error"].(map[string]any)
		if !ok {
			t.Fatalf("expected error object, got %#v", data)
		}
		if msg, _ := errObj["message"].(string); !strings.Contains(msg, "cursor and offset are mutually exclusive") {
			t.Fatalf("expected 'cursor and offset are mutually exclusive' message, got %q", msg)
		}
	})

	t.Run("combined search cursor and offset=0 are mutually exclusive", func(t *testing.T) {
		validCursor := encodeCursor("place:plc:001")
		resp := mustAPIRequest(t, ts.URL+"/v1/search?q=ua&cursor="+validCursor+"&offset=0")
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("expected 400 got %d", resp.StatusCode)
		}
		payload := decodePayload(t, resp)
		data, ok := payload["data"].(map[string]any)
		if !ok {
			t.Fatalf("expected data object, got %#v", payload)
		}
		errObj, ok := data["error"].(map[string]any)
		if !ok {
			t.Fatalf("expected error object, got %#v", data)
		}
		if errObj["code"] != "invalid_request" {
			t.Fatalf("expected invalid_request code, got %#v", errObj["code"])
		}
		if msg, _ := errObj["message"].(string); !strings.Contains(msg, "cursor and offset are mutually exclusive") {
			t.Fatalf("expected 'cursor and offset are mutually exclusive' message, got %q", msg)
		}
	})
}

func mustAPIRequest(t *testing.T, requestURL string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, requestURL, nil)
	if err != nil {
		t.Fatalf("new request %s: %v", requestURL, err)
	}
	req.Header.Set(apiKeyHeader, testAPIKey)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get %s: %v", requestURL, err)
	}
	return resp
}

func decodePayload(t *testing.T, resp *http.Response) map[string]any {
	t.Helper()
	defer resp.Body.Close()
	var payload map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	return payload
}
