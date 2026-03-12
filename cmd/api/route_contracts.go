package main

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
)

const apiKeyHeader = "X-API-Key"

type apiAuthContract struct {
	Required bool   `json:"required"`
	Header   string `json:"header,omitempty"`
}

type apiPathParamContract struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	Required    bool   `json:"required"`
	Description string `json:"description"`
}

type apiQueryLimitContract struct {
	Default int `json:"default"`
	Max     int `json:"max"`
}

type apiQueryParamContract struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	Required    bool   `json:"required"`
	Description string `json:"description"`
}

type apiQueryContract struct {
	Limit  *apiQueryLimitContract `json:"limit,omitempty"`
	Cursor bool                   `json:"cursor"`
	Q      bool                   `json:"q"`
	Params []apiQueryParamContract `json:"params"`
}

type apiFieldsContract struct {
	Selectable []string `json:"selectable"`
}

type apiResponseContract struct {
	Container string `json:"container"`
	Kind      string `json:"kind"`
	Sort      string `json:"sort,omitempty"`
}

type apiRouteContract struct {
	Method     string                 `json:"method"`
	Path       string                 `json:"path"`
	Summary    string                 `json:"summary"`
	Kind       string                 `json:"kind"`
	ItemKind   *string                `json:"item_kind"`
	Auth       apiAuthContract        `json:"auth"`
	PathParams []apiPathParamContract `json:"path_params"`
	Query      apiQueryContract       `json:"query"`
	Fields     apiFieldsContract      `json:"fields"`
	Response   apiResponseContract    `json:"response"`
	Notes      []string               `json:"notes"`

	handler         http.HandlerFunc `json:"-"`
	protected       bool             `json:"-"`
	kindForRouting  string           `json:"-"`
	resourcePathRef string           `json:"-"`
}

func buildRouteContracts(version, readyMarker string, server *apiServer) []apiRouteContract {
	contracts := []apiRouteContract{
		publicRouteContract(http.MethodGet, "/v1/health", "Liveness probe for API process", "operational", nil, nil, nil, apiResponseContract{Container: "status", Kind: "health"}, []string{"Public operational probe."}, func(w http.ResponseWriter, r *http.Request) {
			respond(w, version, envelope{"status": "ok"})
		}),
		publicRouteContract(http.MethodGet, "/v1/ready", "Readiness probe for bootstrap completion", "operational", nil, nil, nil, apiResponseContract{Container: "status", Kind: "readiness"}, []string{"Public operational probe.", "Reports bootstrap marker readiness."}, readyHandler(version, readyMarker)),
		publicRouteContract(http.MethodGet, "/v1/version", "Service and API version metadata", "operational", nil, nil, nil, apiResponseContract{Container: "item", Kind: "version"}, []string{"Public operational probe."}, func(w http.ResponseWriter, r *http.Request) {
			respond(w, version, envelope{"service": "api", "api_version": version})
		}),
		publicRouteContract(http.MethodGet, "/v1/schema", "Machine-readable API contract for frontend integration", "contract", nil, nil, nil, apiResponseContract{Container: "items", Kind: "endpoint"}, []string{"Public route used for route/auth/query/field discovery."}, nil),

		protectedListContract("/v1/jobs", "List control-plane jobs", jobResource, server.listHandler(jobResource), nil),
		protectedDetailContract("/v1/jobs/{jobId}", "Get a single control-plane job", jobResource, server.detailHandler(jobResource), nil),
		protectedListContract("/v1/sources", "List source registry entries", sourceResource, server.listHandler(sourceResource), []string{"Boolean fields are normalized from ClickHouse scalar values."}),
		protectedDetailContract("/v1/sources/{sourceId}", "Get a single source registry entry", sourceResource, server.detailHandler(sourceResource), []string{"Boolean and JSON-like fields are normalized."}),
		protectedListContract("/v1/sources/{sourceId}/coverage", "List coverage records for a source", sourceCoverageResource, server.listHandler(sourceCoverageResource), []string{"Nested list uses fixed source_id filter from path parameter."}),
		protectedListContract("/v1/places", "List places", placeResource, server.listHandler(placeResource), nil),
		protectedDetailContract("/v1/places/{placeId}", "Get a single place", placeResource, server.detailHandler(placeResource), nil),
		protectedListContract("/v1/places/{placeId}/children", "List child places for a parent place", placeChildResource, server.listHandler(placeChildResource), []string{"Nested list uses fixed parent_place_id filter from path parameter."}),
		protectedListContract("/v1/places/{placeId}/metrics", "List metric rollups for a place", placeMetricResource, server.listHandler(placeMetricResource), []string{"Nested list uses fixed place_id filter from path parameter."}),
		protectedListContract("/v1/places/{placeId}/events", "List events for a place", placeEventResource, server.listHandler(placeEventResource), []string{"Nested list uses fixed place_id filter from path parameter."}),
		protectedListContract("/v1/places/{placeId}/observations", "List observations for a place", placeObservationResource, server.listHandler(placeObservationResource), []string{"Nested list uses fixed place_id filter from path parameter."}),
		protectedListContract("/v1/entities", "List entities", entityResource, server.listHandler(entityResource), nil),
		protectedDetailContract("/v1/entities/{entityId}", "Get a single entity", entityResource, server.detailHandler(entityResource), nil),
		protectedListContract("/v1/entities/{entityId}/tracks", "List tracks for an entity", entityTrackResource, server.listHandler(entityTrackResource), []string{"Nested list uses fixed entity_id filter from path parameter."}),
		protectedListContract("/v1/entities/{entityId}/events", "List events linked to an entity", entityEventResource, server.listHandler(entityEventResource), []string{"Nested list uses fixed entity_id filter from path parameter."}),
		protectedListContract("/v1/entities/{entityId}/places", "List place links for an entity", entityPlaceResource, server.listHandler(entityPlaceResource), []string{"Nested list uses fixed entity_id filter from path parameter."}),
		protectedListContract("/v1/events", "List events", eventResource, server.listHandler(eventResource), []string{"parent_place_chain is normalized from JSON text when present."}),
		protectedDetailContract("/v1/events/{eventId}", "Get a single event", eventResource, server.detailHandler(eventResource), []string{"parent_place_chain is normalized from JSON text when present."}),
		protectedListContract("/v1/observations", "List observations", observationResource, server.listHandler(observationResource), []string{"parent_place_chain is normalized from JSON text when present."}),
		protectedDetailContract("/v1/observations/{recordId}", "Get a single observation", observationResource, server.detailHandler(observationResource), []string{"parent_place_chain is normalized from JSON text when present."}),
		protectedListContract("/v1/metrics", "List metric definitions", metricResource, server.listHandler(metricResource), []string{"enabled is normalized from ClickHouse scalar values."}),
		protectedDetailContract("/v1/metrics/{metricId}", "Get a single metric definition", metricResource, server.detailHandler(metricResource), []string{"enabled is normalized from ClickHouse scalar values."}),
		protectedListContract("/v1/analytics/rollups", "List metric rollups", rollupResource, server.listHandler(rollupResource), nil),
		protectedListContract("/v1/analytics/time-series", "List metric time-series points", timeSeriesResource, server.listHandler(timeSeriesResource), nil),
		protectedListContract("/v1/analytics/hotspots", "List metric hotspots", hotspotResource, server.listHandler(hotspotResource), nil),
		protectedListContract("/v1/analytics/cross-domain", "List cross-domain metric composites", crossDomainResource, server.listHandler(crossDomainResource), []string{"domains and metric_ids are normalized from JSON text when present."}),
		protectedCombinedSearchContract(server.combinedSearchHandler()),
		protectedListContract("/v1/search/places", "List place search results", searchPlaceResource, server.listHandler(searchPlaceResource), nil),
		protectedListContract("/v1/search/entities", "List entity search results", searchEntityResource, server.listHandler(searchEntityResource), nil),
		protectedOperationalContract(http.MethodGet, "/v1/internal/stats", "Service-side dashboard statistics", "internal_stats", "internal_stat", apiResponseContract{Container: "item", Kind: "internal_stats"}, []string{"Protected operational endpoint for internal dashboards."}, server.internalStatsHandler()),
	}

	schemaEndpoints := make([]apiRouteContract, 0, len(contracts))
	for _, contract := range contracts {
		schemaEndpoints = append(schemaEndpoints, contractForSchema(contract))
	}
	for i := range contracts {
		if contracts[i].Path == "/v1/schema" && contracts[i].Method == http.MethodGet {
			endpoints := schemaEndpoints
			contracts[i].handler = func(w http.ResponseWriter, r *http.Request) {
				respond(w, version, envelope{"endpoints": endpoints})
			}
			break
		}
	}

	return contracts
}

func publicRouteContract(method, path, summary, kind string, itemKind *string, pathParams []apiPathParamContract, queryParams []apiQueryParamContract, response apiResponseContract, notes []string, handler http.HandlerFunc) apiRouteContract {
	return apiRouteContract{
		Method:     method,
		Path:       path,
		Summary:    summary,
		Kind:       kind,
		ItemKind:   itemKind,
		Auth:       apiAuthContract{Required: false},
		PathParams: copyPathParams(pathParams),
		Query: apiQueryContract{
			Limit:  nil,
			Cursor: false,
			Q:      false,
			Params: copyQueryParams(queryParams),
		},
		Fields: apiFieldsContract{
			Selectable: nil,
		},
		Response:         response,
		Notes:            append([]string(nil), notes...),
		handler:          handler,
		protected:        false,
		kindForRouting:   kind,
		resourcePathRef:  path,
	}
}

func protectedOperationalContract(method, path, summary, kind, itemKind string, response apiResponseContract, notes []string, handler http.HandlerFunc) apiRouteContract {
	item := itemKind
	contract := publicRouteContract(method, path, summary, kind, &item, nil, nil, response, notes, handler)
	contract.Auth = apiAuthContract{Required: true, Header: apiKeyHeader}
	contract.protected = true
	return contract
}

func protectedListContract(path, summary string, spec resourceSpec, handler http.HandlerFunc, notes []string) apiRouteContract {
	pathParams := pathParamsFromRoute(path)
	params := listQueryParams(spec)
	item := spec.itemKind
	return apiRouteContract{
		Method:     http.MethodGet,
		Path:       path,
		Summary:    summary,
		Kind:       spec.kind,
		ItemKind:   &item,
		Auth:       apiAuthContract{Required: true, Header: apiKeyHeader},
		PathParams: pathParams,
		Query: apiQueryContract{
			Limit:  &apiQueryLimitContract{Default: defaultPageLimit, Max: maxPageLimit},
			Cursor: true,
			Q:      len(spec.searchColumns) > 0,
			Params: params,
		},
		Fields: apiFieldsContract{
			Selectable: append([]string(nil), spec.selectFields...),
		},
		Response: apiResponseContract{
			Container: "items",
			Kind:      spec.kind,
			Sort:      spec.idColumn + ":asc",
		},
		Notes:           append([]string(nil), notes...),
		handler:         handler,
		protected:       true,
		kindForRouting:  "list",
		resourcePathRef: path,
	}
}

func protectedDetailContract(path, summary string, spec resourceSpec, handler http.HandlerFunc, notes []string) apiRouteContract {
	params := []apiQueryParamContract{{
		Name:        "fields",
		Type:        "csv",
		Required:    false,
		Description: "Optional projected field list; all fields returned when omitted.",
	}}
	item := spec.itemKind
	return apiRouteContract{
		Method:     http.MethodGet,
		Path:       path,
		Summary:    summary,
		Kind:       spec.kind,
		ItemKind:   &item,
		Auth:       apiAuthContract{Required: true, Header: apiKeyHeader},
		PathParams: pathParamsFromRoute(path),
		Query: apiQueryContract{
			Limit:  nil,
			Cursor: false,
			Q:      false,
			Params: params,
		},
		Fields: apiFieldsContract{
			Selectable: append([]string(nil), spec.selectFields...),
		},
		Response: apiResponseContract{
			Container: "item",
			Kind:      spec.itemKind,
			Sort:      spec.idColumn + ":asc",
		},
		Notes:           append([]string(nil), notes...),
		handler:         handler,
		protected:       true,
		kindForRouting:  "detail",
		resourcePathRef: path,
	}
}

func protectedCombinedSearchContract(handler http.HandlerFunc) apiRouteContract {
	item := "search_result"
	return apiRouteContract{
		Method:   http.MethodGet,
		Path:     "/v1/search",
		Summary:  "Combined place/entity search with cursor pagination",
		Kind:     "search",
		ItemKind: &item,
		Auth:     apiAuthContract{Required: true, Header: apiKeyHeader},
		Query: apiQueryContract{
			Limit:  &apiQueryLimitContract{Default: defaultPageLimit, Max: maxPageLimit},
			Cursor: true,
			Q:      true,
			Params: []apiQueryParamContract{
				{Name: "q", Type: "string", Required: false, Description: "Case-insensitive search text applied to both place and entity dimensions."},
				{Name: "limit", Type: "int", Required: false, Description: fmt.Sprintf("Page size, default %d, max %d.", defaultPageLimit, maxPageLimit)},
				{Name: "cursor", Type: "string", Required: false, Description: "Opaque base64url cursor from prior response next_cursor."},
				{Name: "fields", Type: "csv", Required: false, Description: "Optional projected field list for combined search rows."},
			},
		},
		Fields: apiFieldsContract{
			Selectable: []string{"kind", "place_id", "entity_id", "canonical_name", "place_type", "entity_type", "country_code", "continent_code", "risk_band", "primary_place_id"},
		},
		Response: apiResponseContract{
			Container: "items",
			Kind:      "search",
			Sort:      "cursor_key:asc",
		},
		Notes: []string{
			"Search merges place and entity rows then sorts by synthetic cursor_key.",
			"next_cursor is present when additional merged rows are available.",
		},
		handler:         handler,
		protected:       true,
		kindForRouting:  "search",
		resourcePathRef: "/v1/search",
	}
}

func contractForSchema(contract apiRouteContract) apiRouteContract {
	out := contract
	out.handler = nil
	out.protected = false
	out.kindForRouting = ""
	out.resourcePathRef = ""
	out.PathParams = copyPathParams(out.PathParams)
	out.Query.Params = copyQueryParams(out.Query.Params)
	out.Fields.Selectable = append([]string(nil), out.Fields.Selectable...)
	out.Notes = append([]string(nil), out.Notes...)
	if out.ItemKind != nil {
		item := *out.ItemKind
		out.ItemKind = &item
	}
	return out
}

func listQueryParams(spec resourceSpec) []apiQueryParamContract {
	params := []apiQueryParamContract{
		{Name: "limit", Type: "int", Required: false, Description: fmt.Sprintf("Page size, default %d, max %d.", defaultPageLimit, maxPageLimit)},
		{Name: "cursor", Type: "string", Required: false, Description: "Opaque base64url cursor from prior response next_cursor."},
		{Name: "fields", Type: "csv", Required: false, Description: "Optional projected field list; all fields returned when omitted."},
	}
	if len(spec.searchColumns) > 0 {
		params = append(params, apiQueryParamContract{Name: "q", Type: "string", Required: false, Description: "Case-insensitive search text matched across route-specific searchable columns."})
	}
	keys := make([]string, 0, len(spec.queryFilters))
	for key := range spec.queryFilters {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		params = append(params, apiQueryParamContract{Name: key, Type: "string", Required: false, Description: "Allowlisted exact-match filter parameter."})
	}
	return params
}

func pathParamsFromRoute(path string) []apiPathParamContract {
	parts := strings.Split(path, "/")
	params := make([]apiPathParamContract, 0, 2)
	for _, part := range parts {
		if len(part) < 3 || part[0] != '{' || part[len(part)-1] != '}' {
			continue
		}
		name := strings.TrimSpace(part[1 : len(part)-1])
		if name == "" {
			continue
		}
		params = append(params, apiPathParamContract{
			Name:        name,
			Type:        "string",
			Required:    true,
			Description: "Path identifier segment.",
		})
	}
	return params
}

func copyPathParams(in []apiPathParamContract) []apiPathParamContract {
	if len(in) == 0 {
		return nil
	}
	out := make([]apiPathParamContract, len(in))
	copy(out, in)
	return out
}

func copyQueryParams(in []apiQueryParamContract) []apiQueryParamContract {
	if len(in) == 0 {
		return nil
	}
	out := make([]apiQueryParamContract, len(in))
	copy(out, in)
	return out
}

func renderAPIReferenceMarkdown(contracts []apiRouteContract) string {
	var b strings.Builder
	b.WriteString("# API Reference\n\n")
	b.WriteString("The REST API is read-only. Frontend traffic should flow through a server-side BFF; the BFF attaches `X-API-Key` for protected routes and keeps the shared key out of browser clients.\n\n")
	b.WriteString("Public routes: `/v1/health`, `/v1/ready`, `/v1/version`, `/v1/schema`. All other `/v1/*` routes require `X-API-Key`.\n\n")
	b.WriteString("## Endpoint Contracts\n\n")

	for _, contract := range contracts {
		b.WriteString("## ")
		b.WriteString(contract.Method)
		b.WriteString(" ")
		b.WriteString(contract.Path)
		b.WriteString("\n\n")

		b.WriteString("- Summary: ")
		b.WriteString(contract.Summary)
		b.WriteString("\n")
		if contract.Auth.Required {
			b.WriteString("- Auth: Required (`")
			b.WriteString(contract.Auth.Header)
			b.WriteString("`)\n")
		} else {
			b.WriteString("- Auth: Not required\n")
		}
		b.WriteString("- Kind: `")
		b.WriteString(contract.Kind)
		b.WriteString("`\n")
		if contract.ItemKind != nil {
			b.WriteString("- Item kind: `")
			b.WriteString(*contract.ItemKind)
			b.WriteString("`\n")
		}
		b.WriteString("- Response container: `")
		b.WriteString(contract.Response.Container)
		b.WriteString("`\n")
		if contract.Response.Sort != "" {
			b.WriteString("- Response sort: `")
			b.WriteString(contract.Response.Sort)
			b.WriteString("`\n")
		}

		b.WriteString("\n")
		b.WriteString("Path parameters\n")
		if len(contract.PathParams) == 0 {
			b.WriteString("- none\n")
		} else {
			for _, param := range contract.PathParams {
				b.WriteString("- `")
				b.WriteString(param.Name)
				b.WriteString("` (")
				b.WriteString(param.Type)
				b.WriteString(", required)")
				if param.Description != "" {
					b.WriteString(": ")
					b.WriteString(param.Description)
				}
				b.WriteString("\n")
			}
		}

		b.WriteString("\n")
		b.WriteString("Query parameters\n")
		if contract.Query.Limit != nil {
			b.WriteString("- `limit` (int): default ")
			b.WriteString(strconv.Itoa(contract.Query.Limit.Default))
			b.WriteString(", max ")
			b.WriteString(strconv.Itoa(contract.Query.Limit.Max))
			b.WriteString("\n")
		}
		if contract.Query.Cursor {
			b.WriteString("- `cursor` (string): opaque cursor from `next_cursor`\n")
		}
		if contract.Query.Q {
			b.WriteString("- `q` (string): case-insensitive search text\n")
		}
		if len(contract.Query.Params) == 0 {
			b.WriteString("- none\n")
		} else {
			for _, param := range contract.Query.Params {
				b.WriteString("- `")
				b.WriteString(param.Name)
				b.WriteString("` (")
				b.WriteString(param.Type)
				if param.Required {
					b.WriteString(", required")
				} else {
					b.WriteString(", optional")
				}
				b.WriteString(")")
				if param.Description != "" {
					b.WriteString(": ")
					b.WriteString(param.Description)
				}
				b.WriteString("\n")
			}
		}

		b.WriteString("\n")
		b.WriteString("Selectable fields\n")
		if len(contract.Fields.Selectable) == 0 {
			b.WriteString("- none\n")
		} else {
			b.WriteString("- ")
			b.WriteString(strings.Join(contract.Fields.Selectable, ", "))
			b.WriteString("\n")
		}

		if len(contract.Notes) > 0 {
			b.WriteString("\n")
			b.WriteString("Notes\n")
			for _, note := range contract.Notes {
				b.WriteString("- ")
				b.WriteString(note)
				b.WriteString("\n")
			}
		}

		b.WriteString("\n")
	}

	return b.String()
}
