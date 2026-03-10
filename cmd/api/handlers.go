package main

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	defaultClickHouseHTTPURL = "http://clickhouse:8123"
	defaultAPIQueryTimeout   = 5 * time.Second
	defaultPageLimit         = 50
	maxPageLimit             = 200
	cursorPrefix             = "cursor:"
)

type clickhouseQuerier interface {
	Query(ctx context.Context, query string) (string, error)
}

type apiServer struct {
	version      string
	clickhouse   clickhouseQuerier
	queryTimeout time.Duration
}

type resourceSpec struct {
	kind          string
	itemKind      string
	view          string
	idColumn      string
	pathParam     string
	selectFields  []string
	allowedFields map[string]struct{}
	queryFilters  map[string]string
	searchColumns []string
	fixedFilters  func(*http.Request) map[string]string
}

type listOptions struct {
	limit   int
	cursor  string
	fields  []string
	filters map[string]string
	search  string
}

type clickhouseClient struct {
	baseURL  string
	username string
	password string
	client   *http.Client
}

var (
	jobResource = newResourceSpec(resourceSpec{
		kind:      "jobs",
		itemKind:  "job",
		view:      "gold.api_v1_jobs",
		idColumn:  "job_id",
		pathParam: "jobId",
		selectFields: []string{
			"job_id", "job_type", "status", "started_at", "finished_at", "message", "stats",
		},
		queryFilters: map[string]string{
			"job_type": "job_type",
			"status":   "status",
		},
		searchColumns: []string{"job_id", "job_type", "message"},
	})
	sourceResource = newResourceSpec(resourceSpec{
		kind:      "sources",
		itemKind:  "source",
		view:      "gold.api_v1_sources",
		idColumn:  "source_id",
		pathParam: "sourceId",
		selectFields: []string{
			"source_id", "domain", "domain_family", "source_class", "entrypoints", "auth_mode", "auth_config_json",
			"format_hint", "robots_policy", "refresh_strategy", "requests_per_minute", "burst_size", "retention_class",
			"license", "terms_url", "attribution_required", "geo_scope", "priority", "parser_id", "entity_types",
			"expected_place_types", "supports_historical", "supports_delta", "backfill_priority", "confidence_baseline",
			"enabled", "disabled_reason", "disabled_at", "disabled_by", "review_status", "review_notes",
			"schema_version", "record_version", "api_contract_version", "updated_at", "attrs", "evidence",
		},
		queryFilters: map[string]string{
			"domain_family": "domain_family",
			"source_class":  "source_class",
			"geo_scope":     "geo_scope",
			"enabled":       "enabled",
		},
		searchColumns: []string{"source_id", "domain", "domain_family", "source_class"},
	})
	sourceCoverageResource = newResourceSpec(resourceSpec{
		kind:      "source_coverage",
		itemKind:  "source_coverage",
		view:      "gold.api_v1_source_coverage",
		idColumn:  "coverage_id",
		pathParam: "sourceId",
		selectFields: []string{
			"coverage_id", "source_id", "scope_type", "scope_id", "geo_scope", "place_count", "event_count", "updated_at",
		},
		queryFilters: map[string]string{
			"source_id":  "source_id",
			"scope_type": "scope_type",
			"scope_id":   "scope_id",
			"geo_scope":  "geo_scope",
		},
		searchColumns: []string{"source_id", "scope_id", "geo_scope"},
		fixedFilters: func(r *http.Request) map[string]string {
			return map[string]string{"source_id": strings.TrimSpace(r.PathValue("sourceId"))}
		},
	})
	placeResource = newResourceSpec(resourceSpec{
		kind:      "places",
		itemKind:  "place",
		view:      "gold.api_v1_places",
		idColumn:  "place_id",
		pathParam: "placeId",
		selectFields: []string{
			"place_id", "parent_place_id", "canonical_name", "place_type", "admin_level", "country_code", "continent_code",
			"source_place_key", "source_system", "status", "centroid_lat", "centroid_lon", "bbox_min_lat", "bbox_min_lon",
			"bbox_max_lat", "bbox_max_lon", "valid_from", "valid_to", "schema_version", "record_version",
			"api_contract_version", "updated_at", "attrs", "evidence",
		},
		queryFilters: map[string]string{
			"parent_place_id": "parent_place_id",
			"place_type":      "place_type",
			"country_code":    "country_code",
			"continent_code":  "continent_code",
			"status":          "status",
		},
		searchColumns: []string{"place_id", "canonical_name", "country_code", "continent_code"},
	})
	placeChildResource = newResourceSpec(resourceSpec{
		kind:         "place_children",
		itemKind:     "place",
		view:         "gold.api_v1_places",
		idColumn:     "place_id",
		pathParam:    "placeId",
		selectFields: append([]string(nil), placeResource.selectFields...),
		queryFilters: map[string]string{
			"parent_place_id": "parent_place_id",
			"place_type":      "place_type",
			"country_code":    "country_code",
			"continent_code":  "continent_code",
			"status":          "status",
		},
		searchColumns: []string{"place_id", "canonical_name", "country_code", "continent_code"},
		fixedFilters: func(r *http.Request) map[string]string {
			return map[string]string{"parent_place_id": strings.TrimSpace(r.PathValue("placeId"))}
		},
	})
	entityResource = newResourceSpec(resourceSpec{
		kind:      "entities",
		itemKind:  "entity",
		view:      "gold.api_v1_entities",
		idColumn:  "entity_id",
		pathParam: "entityId",
		selectFields: []string{
			"entity_id", "entity_type", "canonical_name", "status", "risk_band", "primary_place_id", "source_system",
			"valid_from", "valid_to", "schema_version", "record_version", "api_contract_version", "updated_at", "attrs", "evidence",
		},
		queryFilters: map[string]string{
			"entity_type":      "entity_type",
			"status":           "status",
			"risk_band":        "risk_band",
			"primary_place_id": "primary_place_id",
		},
		searchColumns: []string{"entity_id", "canonical_name", "entity_type", "primary_place_id"},
	})
	entityTrackResource = newResourceSpec(resourceSpec{
		kind:      "entity_tracks",
		itemKind:  "track",
		view:      "gold.api_v1_tracks",
		idColumn:  "track_record_id",
		pathParam: "entityId",
		selectFields: []string{
			"track_record_id", "track_id", "track_type", "entity_id", "place_id", "from_place_id", "to_place_id",
			"started_at", "ended_at", "distance_km", "point_count", "avg_speed_kph",
		},
		queryFilters: map[string]string{
			"entity_id":     "entity_id",
			"track_type":    "track_type",
			"place_id":      "place_id",
			"from_place_id": "from_place_id",
			"to_place_id":   "to_place_id",
		},
		searchColumns: []string{"track_id", "entity_id", "track_type", "place_id"},
		fixedFilters: func(r *http.Request) map[string]string {
			return map[string]string{"entity_id": strings.TrimSpace(r.PathValue("entityId"))}
		},
	})
	entityEventResource = newResourceSpec(resourceSpec{
		kind:      "entity_events",
		itemKind:  "event",
		view:      "gold.api_v1_entity_events",
		idColumn:  "event_id",
		pathParam: "entityId",
		selectFields: []string{
			"entity_id", "event_id", "event_type", "event_subtype", "place_id", "starts_at", "status", "confidence_band", "impact_score",
		},
		queryFilters: map[string]string{
			"entity_id":     "entity_id",
			"event_type":    "event_type",
			"event_subtype": "event_subtype",
			"place_id":      "place_id",
			"status":        "status",
		},
		searchColumns: []string{"event_id", "event_type", "event_subtype", "place_id"},
		fixedFilters: func(r *http.Request) map[string]string {
			return map[string]string{"entity_id": strings.TrimSpace(r.PathValue("entityId"))}
		},
	})
	entityPlaceResource = newResourceSpec(resourceSpec{
		kind:      "entity_places",
		itemKind:  "place_link",
		view:      "gold.api_v1_entity_places",
		idColumn:  "place_id",
		pathParam: "entityId",
		selectFields: []string{
			"entity_id", "place_id", "canonical_name", "place_type", "relation_type", "linked_at",
		},
		queryFilters: map[string]string{
			"entity_id":     "entity_id",
			"place_type":    "place_type",
			"relation_type": "relation_type",
		},
		searchColumns: []string{"place_id", "canonical_name", "relation_type"},
		fixedFilters: func(r *http.Request) map[string]string {
			return map[string]string{"entity_id": strings.TrimSpace(r.PathValue("entityId"))}
		},
	})
	eventResource = newResourceSpec(resourceSpec{
		kind:      "events",
		itemKind:  "event",
		view:      "gold.api_v1_events",
		idColumn:  "event_id",
		pathParam: "eventId",
		selectFields: []string{
			"event_id", "source_id", "event_type", "event_subtype", "place_id", "parent_place_chain", "starts_at",
			"ends_at", "status", "confidence_band", "impact_score", "schema_version", "attrs", "evidence",
		},
		queryFilters: map[string]string{
			"source_id":     "source_id",
			"event_type":    "event_type",
			"event_subtype": "event_subtype",
			"place_id":      "place_id",
			"status":        "status",
		},
		searchColumns: []string{"event_id", "event_type", "event_subtype", "place_id", "source_id"},
	})
	placeEventResource = newResourceSpec(resourceSpec{
		kind:         "place_events",
		itemKind:     "event",
		view:         "gold.api_v1_events",
		idColumn:     "event_id",
		pathParam:    "placeId",
		selectFields: append([]string(nil), eventResource.selectFields...),
		queryFilters: map[string]string{
			"place_id":      "place_id",
			"source_id":     "source_id",
			"event_type":    "event_type",
			"event_subtype": "event_subtype",
			"status":        "status",
		},
		searchColumns: []string{"event_id", "event_type", "event_subtype", "source_id"},
		fixedFilters: func(r *http.Request) map[string]string {
			return map[string]string{"place_id": strings.TrimSpace(r.PathValue("placeId"))}
		},
	})
	observationResource = newResourceSpec(resourceSpec{
		kind:      "observations",
		itemKind:  "observation",
		view:      "gold.api_v1_observations",
		idColumn:  "observation_id",
		pathParam: "recordId",
		selectFields: []string{
			"observation_id", "source_id", "subject_type", "subject_id", "observation_type", "place_id", "parent_place_chain",
			"observed_at", "published_at", "confidence_band", "measurement_unit", "measurement_value", "schema_version",
			"attrs", "evidence",
		},
		queryFilters: map[string]string{
			"source_id":        "source_id",
			"subject_type":     "subject_type",
			"subject_id":       "subject_id",
			"observation_type": "observation_type",
			"place_id":         "place_id",
		},
		searchColumns: []string{"observation_id", "observation_type", "subject_id", "place_id", "source_id"},
	})
	placeObservationResource = newResourceSpec(resourceSpec{
		kind:         "place_observations",
		itemKind:     "observation",
		view:         "gold.api_v1_observations",
		idColumn:     "observation_id",
		pathParam:    "placeId",
		selectFields: append([]string(nil), observationResource.selectFields...),
		queryFilters: map[string]string{
			"place_id":         "place_id",
			"source_id":        "source_id",
			"subject_type":     "subject_type",
			"subject_id":       "subject_id",
			"observation_type": "observation_type",
		},
		searchColumns: []string{"observation_id", "observation_type", "subject_id", "source_id"},
		fixedFilters: func(r *http.Request) map[string]string {
			return map[string]string{"place_id": strings.TrimSpace(r.PathValue("placeId"))}
		},
	})
	metricResource = newResourceSpec(resourceSpec{
		kind:      "metrics",
		itemKind:  "metric",
		view:      "gold.api_v1_metrics",
		idColumn:  "metric_id",
		pathParam: "metricId",
		selectFields: []string{
			"metric_id", "metric_family", "subject_grain", "unit", "value_type", "rollup_engine", "rollup_rule",
			"enabled", "updated_at", "attrs", "evidence",
		},
		queryFilters: map[string]string{
			"metric_family": "metric_family",
			"subject_grain": "subject_grain",
			"enabled":       "enabled",
		},
		searchColumns: []string{"metric_id", "metric_family", "subject_grain"},
	})
	rollupResource = newResourceSpec(resourceSpec{
		kind:     "metric_rollups",
		itemKind: "metric_rollup",
		view:     "gold.api_v1_metric_rollups",
		idColumn: "snapshot_id",
		selectFields: []string{
			"snapshot_id", "metric_id", "subject_grain", "subject_id", "place_id", "window_grain", "window_start", "window_end",
			"snapshot_at", "metric_value", "metric_delta", "rank", "attrs", "evidence",
		},
		queryFilters: map[string]string{
			"metric_id":     "metric_id",
			"subject_grain": "subject_grain",
			"subject_id":    "subject_id",
			"place_id":      "place_id",
			"window_grain":  "window_grain",
		},
		searchColumns: []string{"metric_id", "subject_id", "place_id"},
	})
	placeMetricResource = newResourceSpec(resourceSpec{
		kind:         "place_metrics",
		itemKind:     "metric_rollup",
		view:         "gold.api_v1_metric_rollups",
		idColumn:     "snapshot_id",
		pathParam:    "placeId",
		selectFields: append([]string(nil), rollupResource.selectFields...),
		queryFilters: map[string]string{
			"place_id":      "place_id",
			"metric_id":     "metric_id",
			"subject_grain": "subject_grain",
			"subject_id":    "subject_id",
			"window_grain":  "window_grain",
		},
		searchColumns: []string{"metric_id", "subject_id", "place_id"},
		fixedFilters: func(r *http.Request) map[string]string {
			return map[string]string{"place_id": strings.TrimSpace(r.PathValue("placeId"))}
		},
	})
	timeSeriesResource = newResourceSpec(resourceSpec{
		kind:     "metric_time_series",
		itemKind: "metric_point",
		view:     "gold.api_v1_time_series",
		idColumn: "point_id",
		selectFields: []string{
			"point_id", "metric_id", "subject_grain", "subject_id", "place_id", "window_grain", "window_start", "window_end",
			"snapshot_at", "metric_value", "metric_delta", "rank",
		},
		queryFilters: map[string]string{
			"metric_id":     "metric_id",
			"subject_grain": "subject_grain",
			"subject_id":    "subject_id",
			"place_id":      "place_id",
			"window_grain":  "window_grain",
		},
		searchColumns: []string{"metric_id", "subject_id", "place_id"},
	})
	hotspotResource = newResourceSpec(resourceSpec{
		kind:     "metric_hotspots",
		itemKind: "hotspot",
		view:     "gold.api_v1_hotspots",
		idColumn: "hotspot_id",
		selectFields: []string{
			"hotspot_id", "metric_id", "scope_type", "scope_id", "place_id", "snapshot_at", "window_grain", "window_start",
			"window_end", "rank", "hotspot_score", "attrs", "evidence",
		},
		queryFilters: map[string]string{
			"metric_id":    "metric_id",
			"scope_type":   "scope_type",
			"scope_id":     "scope_id",
			"place_id":     "place_id",
			"window_grain": "window_grain",
		},
		searchColumns: []string{"metric_id", "scope_type", "scope_id", "place_id"},
	})
	crossDomainResource = newResourceSpec(resourceSpec{
		kind:     "metric_cross_domain",
		itemKind: "cross_domain",
		view:     "gold.api_v1_cross_domain",
		idColumn: "cross_domain_id",
		selectFields: []string{
			"cross_domain_id", "subject_grain", "subject_id", "place_id", "domains", "composite_score", "snapshot_at", "metric_ids", "attrs", "evidence",
		},
		queryFilters: map[string]string{
			"subject_grain": "subject_grain",
			"subject_id":    "subject_id",
			"place_id":      "place_id",
		},
		searchColumns: []string{"subject_id", "place_id"},
	})
	searchPlaceResource = newResourceSpec(resourceSpec{
		kind:     "search_places",
		itemKind: "place",
		view:     "gold.api_v1_places",
		idColumn: "place_id",
		selectFields: []string{
			"place_id", "canonical_name", "place_type", "country_code", "continent_code",
		},
		queryFilters: map[string]string{
			"place_type":     "place_type",
			"country_code":   "country_code",
			"continent_code": "continent_code",
		},
		searchColumns: []string{"place_id", "canonical_name", "country_code", "continent_code"},
	})
	searchEntityResource = newResourceSpec(resourceSpec{
		kind:     "search_entities",
		itemKind: "entity",
		view:     "gold.api_v1_entities",
		idColumn: "entity_id",
		selectFields: []string{
			"entity_id", "canonical_name", "entity_type", "risk_band", "primary_place_id",
		},
		queryFilters: map[string]string{
			"entity_type":      "entity_type",
			"risk_band":        "risk_band",
			"primary_place_id": "primary_place_id",
		},
		searchColumns: []string{"entity_id", "canonical_name", "entity_type", "primary_place_id"},
	})
)

func newResourceSpec(spec resourceSpec) resourceSpec {
	allowed := make(map[string]struct{}, len(spec.selectFields))
	for _, field := range spec.selectFields {
		allowed[field] = struct{}{}
	}
	spec.allowedFields = allowed
	spec.selectFields = append([]string(nil), spec.selectFields...)
	if spec.queryFilters == nil {
		spec.queryFilters = map[string]string{}
	}
	return spec
}

func newAPIServer(version string) *apiServer {
	timeout := parseDurationEnv("API_QUERY_TIMEOUT", defaultAPIQueryTimeout)
	return &apiServer{
		version: version,
		clickhouse: &clickhouseClient{
			baseURL:  strings.TrimRight(getenv("CLICKHOUSE_HTTP_URL", defaultClickHouseHTTPURL), "/"),
			username: getenv("CLICKHOUSE_API_USER", "svc_api"),
			password: getenv("CLICKHOUSE_API_PASSWORD", "api_change_me"),
			client:   &http.Client{Timeout: timeout},
		},
		queryTimeout: timeout,
	}
}

func parseDurationEnv(key string, fallback time.Duration) time.Duration {
	if raw := strings.TrimSpace(getenv(key, "")); raw != "" {
		if parsed, err := time.ParseDuration(raw); err == nil && parsed > 0 {
			return parsed
		}
	}
	return fallback
}

func (c *clickhouseClient) Query(ctx context.Context, query string) (string, error) {
	requestURL := c.baseURL + "/?query=" + url.QueryEscape(query)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, requestURL, nil)
	if err != nil {
		return "", err
	}
	if c.username != "" {
		req.SetBasicAuth(c.username, c.password)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= http.StatusMultipleChoices {
		return "", fmt.Errorf("clickhouse http %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return string(body), nil
}

func (s *apiServer) listHandler(spec resourceSpec) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		options, err := parseListOptions(r, spec)
		if err != nil {
			respondError(w, s.version, http.StatusBadRequest, "invalid_request", err.Error(), r.URL.Path)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), s.queryTimeout)
		defer cancel()

		rows, err := s.queryResourceRows(ctx, spec, options, options.limit+1)
		if err != nil {
			respondError(w, s.version, http.StatusBadGateway, "query_failed", err.Error(), r.URL.Path)
			return
		}

		hasNext := len(rows) > options.limit
		if hasNext {
			rows = rows[:options.limit]
		}

		items := make([]map[string]any, 0, len(rows))
		for _, row := range rows {
			items = append(items, filterRow(row, options.fields))
		}

		data := envelope{"kind": spec.kind, "items": items, "limit": options.limit, "path": r.URL.Path, "applied_filters": options.filters, "sort": spec.idColumn + ":asc"}
		if len(options.fields) > 0 {
			data["fields"] = options.fields
		}
		if hasNext && len(rows) > 0 {
			if nextCursor, ok := cursorFromRow(rows[len(rows)-1], spec.idColumn); ok {
				data["next_cursor"] = nextCursor
			}
		}
		respond(w, s.version, data)
	}
}

func (s *apiServer) detailHandler(spec resourceSpec) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		resourceID := strings.TrimSpace(r.PathValue(spec.pathParam))
		if resourceID == "" {
			respondError(w, s.version, http.StatusBadRequest, "invalid_request", "missing resource id", r.URL.Path)
			return
		}
		if err := rejectUnsupportedQueryParams(r, []string{"fields"}); err != nil {
			respondError(w, s.version, http.StatusBadRequest, "invalid_request", err.Error(), r.URL.Path)
			return
		}
		fields, err := parseFields(spec, r.URL.Query().Get("fields"))
		if err != nil {
			respondError(w, s.version, http.StatusBadRequest, "invalid_request", err.Error(), r.URL.Path)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), s.queryTimeout)
		defer cancel()
		output, err := s.clickhouse.Query(ctx, buildDetailQuery(spec, resourceID))
		if err != nil {
			respondError(w, s.version, http.StatusBadGateway, "query_failed", err.Error(), r.URL.Path)
			return
		}
		rows, err := decodeJSONEachRow(output)
		if err != nil {
			respondError(w, s.version, http.StatusInternalServerError, "decode_failed", err.Error(), r.URL.Path)
			return
		}
		if len(rows) == 0 {
			respondError(w, s.version, http.StatusNotFound, "not_found", fmt.Sprintf("%s %q not found", spec.itemKind, resourceID), r.URL.Path)
			return
		}
		data := envelope{"kind": spec.itemKind, "item": filterRow(rows[0], fields), "path": r.URL.Path}
		if len(fields) > 0 {
			data["fields"] = fields
		}
		respond(w, s.version, data)
	}
}

func (s *apiServer) combinedSearchHandler() http.HandlerFunc {
	allowedFields := map[string]struct{}{"kind": {}, "place_id": {}, "entity_id": {}, "canonical_name": {}, "place_type": {}, "entity_type": {}, "country_code": {}, "continent_code": {}, "risk_band": {}, "primary_place_id": {}}
	return func(w http.ResponseWriter, r *http.Request) {
		if err := rejectUnsupportedQueryParams(r, []string{"q", "limit", "cursor", "fields"}); err != nil {
			respondError(w, s.version, http.StatusBadRequest, "invalid_request", err.Error(), r.URL.Path)
			return
		}
		limit, cursor, err := parseLimitAndCursor(r)
		if err != nil {
			respondError(w, s.version, http.StatusBadRequest, "invalid_request", err.Error(), r.URL.Path)
			return
		}
		fields, err := parseCombinedFields(r.URL.Query().Get("fields"), allowedFields)
		if err != nil {
			respondError(w, s.version, http.StatusBadRequest, "invalid_request", err.Error(), r.URL.Path)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), s.queryTimeout)
		defer cancel()
		search := strings.TrimSpace(r.URL.Query().Get("q"))
		baseOptions := listOptions{limit: maxPageLimit, search: search, filters: map[string]string{}}
		placeRows, err := s.queryResourceRows(ctx, searchPlaceResource, baseOptions, maxPageLimit)
		if err != nil {
			respondError(w, s.version, http.StatusBadGateway, "query_failed", err.Error(), r.URL.Path)
			return
		}
		entityRows, err := s.queryResourceRows(ctx, searchEntityResource, baseOptions, maxPageLimit)
		if err != nil {
			respondError(w, s.version, http.StatusBadGateway, "query_failed", err.Error(), r.URL.Path)
			return
		}
		items := make([]map[string]any, 0, len(placeRows)+len(entityRows))
		for _, row := range placeRows {
			row["kind"] = "place"
			row["cursor_key"] = "place:" + asString(row["place_id"])
			items = append(items, row)
		}
		for _, row := range entityRows {
			row["kind"] = "entity"
			row["cursor_key"] = "entity:" + asString(row["entity_id"])
			items = append(items, row)
		}
		sort.Slice(items, func(i, j int) bool { return asString(items[i]["cursor_key"]) < asString(items[j]["cursor_key"]) })
		if cursor != "" {
			filtered := items[:0]
			for _, item := range items {
				if asString(item["cursor_key"]) > cursor {
					filtered = append(filtered, item)
				}
			}
			items = filtered
		}
		hasNext := len(items) > limit
		if hasNext {
			items = items[:limit]
		}
		projected := make([]map[string]any, 0, len(items))
		for _, item := range items {
			projected = append(projected, filterCombinedRow(item, fields))
		}
		data := envelope{"kind": "search", "items": projected, "limit": limit, "path": r.URL.Path, "applied_filters": envelope{"q": search}, "sort": "cursor_key:asc"}
		if len(fields) > 0 {
			data["fields"] = fields
		}
		if hasNext && len(items) > 0 {
			data["next_cursor"] = encodeCursor(asString(items[len(items)-1]["cursor_key"]))
		}
		respond(w, s.version, data)
	}
}

func (s *apiServer) queryResourceRows(ctx context.Context, spec resourceSpec, options listOptions, limit int) ([]map[string]any, error) {
	output, err := s.clickhouse.Query(ctx, buildListQuery(spec, options, limit))
	if err != nil {
		return nil, err
	}
	return decodeJSONEachRow(output)
}

func parseListOptions(r *http.Request, spec resourceSpec) (listOptions, error) {
	limit, cursor, err := parseLimitAndCursor(r)
	if err != nil {
		return listOptions{}, err
	}
	allowedQueryParams := []string{"limit", "cursor", "fields"}
	if len(spec.searchColumns) > 0 {
		allowedQueryParams = append(allowedQueryParams, "q")
	}
	for param := range spec.queryFilters {
		allowedQueryParams = append(allowedQueryParams, param)
	}
	if err := rejectUnsupportedQueryParams(r, allowedQueryParams); err != nil {
		return listOptions{}, err
	}
	fields, err := parseFields(spec, r.URL.Query().Get("fields"))
	if err != nil {
		return listOptions{}, err
	}
	filters := make(map[string]string)
	for param := range spec.queryFilters {
		if value := strings.TrimSpace(r.URL.Query().Get(param)); value != "" {
			filters[param] = value
		}
	}
	if spec.fixedFilters != nil {
		for key, value := range spec.fixedFilters(r) {
			if value != "" {
				filters[key] = value
			}
		}
	}
	return listOptions{limit: limit, cursor: cursor, fields: fields, filters: filters, search: strings.TrimSpace(r.URL.Query().Get("q"))}, nil
}

func parseLimitAndCursor(r *http.Request) (int, string, error) {
	limit := defaultPageLimit
	if raw := strings.TrimSpace(r.URL.Query().Get("limit")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 {
			return 0, "", fmt.Errorf("limit must be a positive integer")
		}
		if parsed > maxPageLimit {
			parsed = maxPageLimit
		}
		limit = parsed
	}
	cursor, err := decodeCursor(r.URL.Query().Get("cursor"))
	if err != nil {
		return 0, "", fmt.Errorf("cursor must be valid base64url")
	}
	return limit, cursor, nil
}

func rejectUnsupportedQueryParams(r *http.Request, allowed []string) error {
	allowedSet := make(map[string]struct{}, len(allowed))
	for _, key := range allowed {
		allowedSet[key] = struct{}{}
	}
	for key := range r.URL.Query() {
		if _, ok := allowedSet[key]; !ok {
			return fmt.Errorf("unsupported filter %q", key)
		}
	}
	return nil
}

func parseFields(spec resourceSpec, raw string) ([]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	parts := strings.Split(raw, ",")
	fields := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		field := strings.TrimSpace(part)
		if field == "" {
			continue
		}
		if _, ok := spec.allowedFields[field]; !ok {
			return nil, fmt.Errorf("unsupported field %q", field)
		}
		if _, ok := seen[field]; ok {
			continue
		}
		seen[field] = struct{}{}
		fields = append(fields, field)
	}
	if len(fields) == 0 {
		return nil, fmt.Errorf("fields must include at least one valid field")
	}
	return fields, nil
}

func parseCombinedFields(raw string, allowed map[string]struct{}) ([]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	parts := strings.Split(raw, ",")
	fields := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		field := strings.TrimSpace(part)
		if field == "" {
			continue
		}
		if _, ok := allowed[field]; !ok {
			return nil, fmt.Errorf("unsupported field %q", field)
		}
		if _, ok := seen[field]; ok {
			continue
		}
		seen[field] = struct{}{}
		fields = append(fields, field)
	}
	if len(fields) == 0 {
		return nil, fmt.Errorf("fields must include at least one valid field")
	}
	return fields, nil
}

func buildListQuery(spec resourceSpec, options listOptions, limit int) string {
	clauses := buildWhereClauses(spec, options)
	where := ""
	if len(clauses) > 0 {
		where = " WHERE " + strings.Join(clauses, " AND ")
	}
	return fmt.Sprintf("SELECT %s FROM %s%s ORDER BY %s ASC LIMIT %d FORMAT JSONEachRow", strings.Join(spec.selectFields, ", "), spec.view, where, spec.idColumn, limit)
}

func buildWhereClauses(spec resourceSpec, options listOptions) []string {
	clauses := make([]string, 0, len(options.filters)+2)
	if options.cursor != "" {
		clauses = append(clauses, fmt.Sprintf("%s > %s", spec.idColumn, sqlLiteral(options.cursor)))
	}
	keys := make([]string, 0, len(options.filters))
	for key := range options.filters {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		column := spec.queryFilters[key]
		if column == "" {
			continue
		}
		clauses = append(clauses, fmt.Sprintf("%s = %s", column, sqlLiteral(options.filters[key])))
	}
	if options.search != "" && len(spec.searchColumns) > 0 {
		searchClauses := make([]string, 0, len(spec.searchColumns))
		for _, column := range spec.searchColumns {
			searchClauses = append(searchClauses, fmt.Sprintf("positionCaseInsensitiveUTF8(toString(%s), %s) > 0", column, sqlLiteral(options.search)))
		}
		clauses = append(clauses, "("+strings.Join(searchClauses, " OR ")+")")
	}
	return clauses
}

func buildDetailQuery(spec resourceSpec, resourceID string) string {
	return fmt.Sprintf("SELECT %s FROM %s WHERE %s = '%s' ORDER BY %s ASC LIMIT 1 FORMAT JSONEachRow", strings.Join(spec.selectFields, ", "), spec.view, spec.idColumn, escapeClickHouseString(resourceID), spec.idColumn)
}

func decodeJSONEachRow(input string) ([]map[string]any, error) {
	trimmed := strings.TrimSpace(input)
	if trimmed == "" {
		return nil, nil
	}
	scanner := bufio.NewScanner(strings.NewReader(input))
	scanner.Buffer(make([]byte, 0, 64*1024), 10*1024*1024)
	rows := make([]map[string]any, 0, 8)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var row map[string]any
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			return nil, err
		}
		rows = append(rows, normalizeRow(row))
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return rows, nil
}

func normalizeRow(row map[string]any) map[string]any {
	for _, field := range []string{"attrs", "evidence", "auth_config_json", "domains", "metric_ids", "entrypoints", "entity_types", "expected_place_types", "parent_place_chain"} {
		value, ok := row[field]
		if !ok {
			continue
		}
		text, ok := value.(string)
		if !ok || strings.TrimSpace(text) == "" {
			continue
		}
		var decoded any
		if err := json.Unmarshal([]byte(text), &decoded); err == nil {
			row[field] = decoded
		}
	}
	return row
}

func filterRow(row map[string]any, fields []string) map[string]any {
	if len(fields) == 0 {
		return row
	}
	filtered := make(map[string]any, len(fields))
	for _, field := range fields {
		if value, ok := row[field]; ok {
			filtered[field] = value
		}
	}
	return filtered
}

func filterCombinedRow(row map[string]any, fields []string) map[string]any {
	if len(fields) == 0 {
		delete(row, "cursor_key")
		return row
	}
	filtered := make(map[string]any, len(fields))
	for _, field := range fields {
		if value, ok := row[field]; ok {
			filtered[field] = value
		}
	}
	return filtered
}

func cursorFromRow(row map[string]any, idColumn string) (string, bool) {
	value, ok := row[idColumn]
	if !ok {
		return "", false
	}
	text := asString(value)
	if text == "" {
		return "", false
	}
	return encodeCursor(text), true
}

func encodeCursor(value string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(cursorPrefix + value))
}

func decodeCursor(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", nil
	}
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return "", err
	}
	text := string(decoded)
	if !strings.HasPrefix(text, cursorPrefix) {
		return "", fmt.Errorf("invalid cursor prefix")
	}
	return strings.TrimPrefix(text, cursorPrefix), nil
}

func sqlLiteral(value string) string {
	if _, err := strconv.ParseInt(value, 10, 64); err == nil {
		return value
	}
	if _, err := strconv.ParseFloat(value, 64); err == nil {
		return value
	}
	return "'" + escapeClickHouseString(value) + "'"
}

func escapeClickHouseString(value string) string {
	return strings.ReplaceAll(value, "'", "''")
}

func asString(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return typed
	default:
		return fmt.Sprint(typed)
	}
}

func respondError(w http.ResponseWriter, apiVersion string, status int, code, message, path string) {
	respondStatus(w, status, apiVersion, envelope{"error": envelope{"code": code, "message": message}, "path": path})
}
