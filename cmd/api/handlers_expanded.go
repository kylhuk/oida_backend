package main

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strings"
)

var (
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
