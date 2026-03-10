package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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

func TestAPICoreContracts(t *testing.T) {
	TestAPIExpandedContracts(t)
}

func TestAPIExpandedContracts(t *testing.T) {
	mux := newAPIMuxWithServer("v1", "", &apiServer{
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
					return job2 + "\n", nil
				default:
					return job1 + "\n" + job2 + "\n", nil
				}
			case strings.Contains(query, "FROM gold.api_v1_source_coverage"):
				return `{"coverage_id":"src:001:coverage","source_id":"src:001","scope_type":"source","scope_id":"src:001","geo_scope":"global","place_count":2,"event_count":1,"updated_at":"2026-03-10T08:00:00Z"}` + "\n", nil
			case strings.Contains(query, "FROM gold.api_v1_sources"):
				src1 := `{"source_id":"src:001","domain":"example.com","domain_family":"web","source_class":"news","entrypoints":["https://example.com/feed"],"auth_mode":"none","auth_config_json":"{}","format_hint":"rss","robots_policy":"honor","refresh_strategy":"poll","requests_per_minute":60,"burst_size":10,"retention_class":"warm","license":"CC-BY","terms_url":"https://example.com/terms","attribution_required":1,"geo_scope":"global","priority":10,"parser_id":"parser-rss","entity_types":["org"],"expected_place_types":["country"],"supports_historical":1,"supports_delta":1,"backfill_priority":5,"confidence_baseline":0.9,"enabled":1,"disabled_reason":null,"disabled_at":null,"disabled_by":null,"review_status":"approved","review_notes":"","schema_version":1,"record_version":1,"api_contract_version":1,"updated_at":"2026-03-10T08:00:00Z","attrs":"{}","evidence":"[]"}`
				src2 := `{"source_id":"src:002","domain":"example.org","domain_family":"web","source_class":"bulletin","entrypoints":[],"auth_mode":"none","auth_config_json":"{}","format_hint":"json","robots_policy":"honor","refresh_strategy":"poll","requests_per_minute":30,"burst_size":5,"retention_class":"warm","license":"public","terms_url":"","attribution_required":0,"geo_scope":"regional","priority":20,"parser_id":"parser-json","entity_types":[],"expected_place_types":[],"supports_historical":0,"supports_delta":1,"backfill_priority":10,"confidence_baseline":0.7,"enabled":1,"disabled_reason":null,"disabled_at":null,"disabled_by":null,"review_status":"approved","review_notes":"","schema_version":1,"record_version":2,"api_contract_version":1,"updated_at":"2026-03-10T08:05:00Z","attrs":"{}","evidence":"[]"}`
				if strings.Contains(query, "WHERE source_id = 'src:001'") {
					return src1 + "\n", nil
				}
				return src1 + "\n" + src2 + "\n", nil
			case strings.Contains(query, "FROM gold.api_v1_places"):
				root := `{"place_id":"plc:001","parent_place_id":null,"canonical_name":"Ukraine","place_type":"admin0","admin_level":0,"country_code":"UA","continent_code":"EU","source_place_key":"ua","source_system":"fixture","status":"active","centroid_lat":48.3,"centroid_lon":31.1,"bbox_min_lat":44.0,"bbox_min_lon":22.0,"bbox_max_lat":52.0,"bbox_max_lon":40.0,"valid_from":"2026-03-01T00:00:00Z","valid_to":null,"schema_version":1,"record_version":1,"api_contract_version":1,"updated_at":"2026-03-10T08:00:00Z","attrs":"{}","evidence":"[]"}`
				child := `{"place_id":"plc:002","parent_place_id":"plc:001","canonical_name":"Kyiv","place_type":"admin1","admin_level":1,"country_code":"UA","continent_code":"EU","source_place_key":"ua-30","source_system":"fixture","status":"active","centroid_lat":50.45,"centroid_lon":30.52,"bbox_min_lat":50.0,"bbox_min_lon":30.0,"bbox_max_lat":51.0,"bbox_max_lon":31.0,"valid_from":"2026-03-01T00:00:00Z","valid_to":null,"schema_version":1,"record_version":1,"api_contract_version":1,"updated_at":"2026-03-10T08:00:00Z","attrs":"{}","evidence":"[]"}`
				switch {
				case strings.Contains(query, "WHERE place_id = 'plc:001'"):
					return root + "\n", nil
				case strings.Contains(query, "parent_place_id = 'plc:001'"):
					return child + "\n", nil
				case strings.Contains(query, "does-not-exist"):
					return "", nil
				case strings.Contains(query, "Kyiv"):
					return child + "\n", nil
				default:
					return root + "\n" + child + "\n", nil
				}
			case strings.Contains(query, "FROM gold.api_v1_entities"):
				entity1 := `{"entity_id":"ent:001","entity_type":"organization","canonical_name":"Relief Cluster","status":"active","risk_band":"medium","primary_place_id":"plc:002","source_system":"fixture","valid_from":"2026-03-01T00:00:00Z","valid_to":null,"schema_version":1,"record_version":1,"api_contract_version":1,"updated_at":"2026-03-10T08:00:00Z","attrs":"{}","evidence":"[]"}`
				entity2 := `{"entity_id":"ent:002","entity_type":"vessel","canonical_name":"MV Aurora","status":"active","risk_band":"high","primary_place_id":"plc:001","source_system":"fixture","valid_from":"2026-03-01T00:00:00Z","valid_to":null,"schema_version":1,"record_version":1,"api_contract_version":1,"updated_at":"2026-03-10T08:10:00Z","attrs":"{}","evidence":"[]"}`
				switch {
				case strings.Contains(query, "WHERE entity_id = 'ent:001'"):
					return entity1 + "\n", nil
				case strings.Contains(query, "does-not-exist"):
					return "", nil
				case strings.Contains(query, "Aurora"):
					return entity2 + "\n", nil
				default:
					return entity1 + "\n" + entity2 + "\n", nil
				}
			case strings.Contains(query, "FROM gold.api_v1_tracks"):
				return `{"track_record_id":"trk:001","track_id":"track:aurora","track_type":"maritime","entity_id":"ent:002","place_id":"plc:001","from_place_id":"plc:001","to_place_id":"plc:002","started_at":"2026-03-09T09:00:00Z","ended_at":"2026-03-09T11:00:00Z","distance_km":120.5,"point_count":16,"avg_speed_kph":60.2}` + "\n", nil
			case strings.Contains(query, "FROM gold.api_v1_entity_events"):
				return `{"entity_id":"ent:001","event_id":"evt:001","event_type":"humanitarian_access","event_subtype":"checkpoint_delay","place_id":"plc:002","starts_at":"2026-03-10T07:30:00Z","status":"open","confidence_band":"high","impact_score":0.78}` + "\n", nil
			case strings.Contains(query, "FROM gold.api_v1_entity_places"):
				return `{"entity_id":"ent:001","place_id":"plc:002","canonical_name":"Kyiv","place_type":"admin1","relation_type":"operates_in","linked_at":"2026-03-10T08:00:00Z"}` + "\n", nil
			case strings.Contains(query, "FROM gold.api_v1_events"):
				event := `{"event_id":"evt:001","source_id":"src:001","event_type":"humanitarian_access","event_subtype":"checkpoint_delay","place_id":"plc:002","parent_place_chain":["plc:001"],"starts_at":"2026-03-10T07:30:00Z","ends_at":null,"status":"open","confidence_band":"high","impact_score":0.78,"schema_version":1,"attrs":"{}","evidence":"[]"}`
				if strings.Contains(query, "WHERE event_id = 'evt:missing'") {
					return "", nil
				}
				return event + "\n", nil
			case strings.Contains(query, "FROM gold.api_v1_observations"):
				return `{"observation_id":"obs:001","source_id":"src:001","subject_type":"place","subject_id":"plc:001","observation_type":"media_attention","place_id":"plc:001","parent_place_chain":["plc:world"],"observed_at":"2026-03-10T06:00:00Z","published_at":"2026-03-10T06:05:00Z","confidence_band":"medium","measurement_unit":"score","measurement_value":42.5,"schema_version":1,"attrs":"{}","evidence":"[]"}` + "\n", nil
			case strings.Contains(query, "FROM gold.api_v1_metrics"):
				metric := `{"metric_id":"media_attention_score","metric_family":"geopolitical","subject_grain":"place","unit":"score","value_type":"gauge","rollup_engine":"snapshot","rollup_rule":"latest","enabled":1,"updated_at":"2026-03-10T08:30:00Z","attrs":"{}","evidence":"[]"}`
				if strings.Contains(query, "WHERE metric_id = 'metric:missing'") {
					return "", nil
				}
				return metric + "\n", nil
			case strings.Contains(query, "FROM gold.api_v1_metric_rollups"):
				return `{"snapshot_id":"snap:001","metric_id":"media_attention_score","subject_grain":"place","subject_id":"plc:001","place_id":"plc:001","window_grain":"day","window_start":"2026-03-10T00:00:00Z","window_end":"2026-03-10T23:59:59Z","snapshot_at":"2026-03-10T08:30:00Z","metric_value":42.5,"metric_delta":5.2,"rank":1,"attrs":"{}","evidence":"[]"}` + "\n", nil
			case strings.Contains(query, "FROM gold.api_v1_time_series"):
				return `{"point_id":"point:001","metric_id":"media_attention_score","subject_grain":"place","subject_id":"plc:001","place_id":"plc:001","window_grain":"day","window_start":"2026-03-10T00:00:00Z","window_end":"2026-03-10T23:59:59Z","snapshot_at":"2026-03-10T08:30:00Z","metric_value":42.5,"metric_delta":5.2,"rank":1}` + "\n", nil
			case strings.Contains(query, "FROM gold.api_v1_hotspots"):
				return `{"hotspot_id":"hot:001","metric_id":"media_attention_score","scope_type":"admin0","scope_id":"plc:001","place_id":"plc:002","snapshot_at":"2026-03-10T08:35:00Z","window_grain":"day","window_start":"2026-03-10T00:00:00Z","window_end":"2026-03-10T23:59:59Z","rank":1,"hotspot_score":88.4,"attrs":"{}","evidence":"[]"}` + "\n", nil
			case strings.Contains(query, "FROM gold.api_v1_cross_domain"):
				return `{"cross_domain_id":"cross:001","subject_grain":"place","subject_id":"plc:001","place_id":"plc:001","domains":["geopolitical","humanitarian"],"composite_score":64.8,"snapshot_at":"2026-03-10T08:40:00Z","metric_ids":["media_attention_score"],"attrs":"{}","evidence":"[]"}` + "\n", nil
			default:
				t.Fatalf("unexpected query: %s", query)
				return "", nil
			}
		}},
		queryTimeout: time.Second,
	})

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
		})
	}
}

func TestAPIExpandedEdgeCases(t *testing.T) {
	mux := newAPIMuxWithServer("v1", "", &apiServer{
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
	})

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

func mustAPIRequest(t *testing.T, requestURL string) *http.Response {
	t.Helper()
	resp, err := http.Get(requestURL)
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
