package main

import (
	"fmt"
	"net/http"
	"strings"
)

const apiKeyHeader = "X-API-Key"

var (
	apiReadScopes     = []string{"read:*"}
	apiInternalScopes = []string{"read:internal"}
)

type apiHandlerKind string

const (
	apiHandlerKindHealth         apiHandlerKind = "health"
	apiHandlerKindReady          apiHandlerKind = "ready"
	apiHandlerKindVersion        apiHandlerKind = "version"
	apiHandlerKindSchema         apiHandlerKind = "schema"
	apiHandlerKindList           apiHandlerKind = "list"
	apiHandlerKindDetail         apiHandlerKind = "detail"
	apiHandlerKindCombinedSearch apiHandlerKind = "combined_search"
	apiHandlerKindSearchClasses  apiHandlerKind = "search_classes"
	apiHandlerKindInternalStats        apiHandlerKind = "internal_stats"
	apiHandlerKindWorkerTail           apiHandlerKind = "worker_tail"
	apiHandlerKindRawQuery             apiHandlerKind = "raw_query"
	apiHandlerKindVectorSearch         apiHandlerKind = "vector_search"
	apiHandlerKindEmbeddingsResolve    apiHandlerKind = "embeddings_resolve"
	apiHandlerKindVectorSpaceDescribe  apiHandlerKind = "vector_space_describe"
	apiHandlerKindRegistryLookup       apiHandlerKind = "registry_lookup"
	apiHandlerKindArtifactRead         apiHandlerKind = "artifact_read"
)

type apiAuthContract struct {
	Required bool     `json:"required"`
	Header   string   `json:"header,omitempty"`
	Scopes   []string `json:"scopes,omitempty"`
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
	Limit        *apiQueryLimitContract  `json:"limit,omitempty"`
	Cursor       bool                    `json:"cursor"`
	Q            bool                    `json:"q"`
	Params       []apiQueryParamContract `json:"params"`
	CursorParam  string                  `json:"cursor_param,omitempty"`
	SearchParam  string                  `json:"search_param,omitempty"`
	FieldsParam  string                  `json:"fields_param,omitempty"`
	FilterParams []string                `json:"filter_params,omitempty"`
}

type apiFieldsContract struct {
	Param      string   `json:"param,omitempty"`
	Default    string   `json:"default,omitempty"`
	Selectable []string `json:"selectable"`
}

type apiCombinedSearchResultKindContract struct {
	Kind    string `json:"kind"`
	IDField string `json:"id_field"`
}

type apiCombinedSearchContract struct {
	KindField   string                                `json:"kind_field"`
	SortField   string                                `json:"sort_field"`
	ResultKinds []apiCombinedSearchResultKindContract `json:"result_kinds"`
}

type apiResponseContract struct {
	Container       string                     `json:"container"`
	Kind            string                     `json:"kind"`
	Sort            string                     `json:"sort,omitempty"`
	NextCursorField string                     `json:"next_cursor_field,omitempty"`
	CombinedSearch  *apiCombinedSearchContract `json:"combined_search,omitempty"`
}

type apiSchemaAuthSummary struct {
	PublicRoutes         []string `json:"public_routes"`
	ProtectedPathPattern string   `json:"protected_path_pattern"`
	ProtectedHeader      string   `json:"protected_header"`
}

type apiSchemaDefaultsContract struct {
	Pagination      apiQueryLimitContract `json:"pagination"`
	CursorParam     string                `json:"cursor_param"`
	SearchParam     string                `json:"search_param"`
	FieldsParam     string                `json:"fields_param"`
	NextCursorField string                `json:"next_cursor_field"`
}

type apiSchemaContract struct {
	Auth      apiSchemaAuthSummary      `json:"auth"`
	Defaults  apiSchemaDefaultsContract `json:"defaults"`
	Endpoints []apiRouteContract        `json:"endpoints"`
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
	handlerKind     apiHandlerKind   `json:"-"`
	resourcePathRef string           `json:"-"`
}

type apiRouteSpec struct {
	Method     string
	Path       string
	Summary    string
	Kind       string
	ItemKind   *string
	Auth       apiAuthContract
	PathParams []apiPathParamContract
	Query      apiQueryContract
	Fields     apiFieldsContract
	Response   apiResponseContract
	Notes      []string

	handlerKind apiHandlerKind
	resource    *resourceSpec
}

func buildRouteContracts(version, readyMarker string, server *apiServer) []apiRouteContract {
	specs := buildRouteSpecs()
	contracts := make([]apiRouteContract, 0, len(specs))
	for _, spec := range specs {
		contracts = append(contracts, routeContractFromSpec(spec))
	}

	schemaContract := schemaContractFromRoutes(contracts)
	for i, spec := range specs {
		contracts[i].handler = routeHandlerForSpec(spec, version, readyMarker, server, schemaContract)
	}

	return contracts
}

func buildRouteSpecs() []apiRouteSpec {
	return []apiRouteSpec{
		publicRouteSpec(http.MethodGet, "/v1/health", "Liveness probe for API process", "operational", nil, nil, nil, apiResponseContract{Container: "status", Kind: "health"}, []string{"Public operational probe."}, apiHandlerKindHealth),
		publicRouteSpec(http.MethodGet, "/v1/ready", "Readiness probe for bootstrap completion", "operational", nil, nil, nil, apiResponseContract{Container: "status", Kind: "readiness"}, []string{"Public operational probe.", "Reports bootstrap marker readiness."}, apiHandlerKindReady),
		publicRouteSpec(http.MethodGet, "/v1/version", "Service and API version metadata", "operational", nil, nil, nil, apiResponseContract{Container: "item", Kind: "version"}, []string{"Public operational probe."}, apiHandlerKindVersion),
		publicRouteSpec(http.MethodGet, "/v1/schema", "Machine-readable API contract for frontend integration", "contract", nil, nil, nil, apiResponseContract{Container: "items", Kind: "endpoint"}, []string{"Public route used for route/auth/query/field discovery."}, apiHandlerKindSchema),
		protectedListRouteSpec("/v1/jobs", "List control-plane jobs", &jobResource, nil),
		protectedDetailRouteSpec("/v1/jobs/{jobId}", "Get a single control-plane job", &jobResource, nil),
		protectedListRouteSpec("/v1/sources", "List source registry entries", &sourceResource, []string{"Boolean fields are normalized from ClickHouse scalar values."}),
		protectedDetailRouteSpec("/v1/sources/{sourceId}", "Get a single source registry entry", &sourceResource, []string{"Boolean and JSON-like fields are normalized."}),
		protectedListRouteSpec("/v1/sources/{sourceId}/coverage", "List coverage records for a source", &sourceCoverageResource, []string{"Nested list uses fixed source_id filter from path parameter."}),
		protectedListRouteSpec("/v1/places", "List places", &placeResource, nil),
		protectedDetailRouteSpec("/v1/places/{placeId}", "Get a single place", &placeResource, nil),
		protectedListRouteSpec("/v1/places/{placeId}/children", "List child places for a parent place", &placeChildResource, []string{"Nested list uses fixed parent_place_id filter from path parameter."}),
		protectedListRouteSpec("/v1/places/{placeId}/metrics", "List metric rollups for a place", &placeMetricResource, []string{"Nested list uses fixed place_id filter from path parameter."}),
		protectedListRouteSpec("/v1/places/{placeId}/events", "List events for a place", &placeEventResource, []string{"Nested list uses fixed place_id filter from path parameter."}),
		protectedListRouteSpec("/v1/places/{placeId}/observations", "List observations for a place", &placeObservationResource, []string{"Nested list uses fixed place_id filter from path parameter."}),
		protectedListRouteSpec("/v1/entities", "List entities", &entityResource, nil),
		protectedDetailRouteSpec("/v1/entities/{entityId}", "Get a single entity", &entityResource, nil),
		protectedListRouteSpec("/v1/entities/{entityId}/tracks", "List tracks for an entity", &entityTrackResource, []string{"Nested list uses fixed entity_id filter from path parameter."}),
		protectedListRouteSpec("/v1/entities/{entityId}/events", "List events linked to an entity", &entityEventResource, []string{"Nested list uses fixed entity_id filter from path parameter."}),
		protectedListRouteSpec("/v1/entities/{entityId}/places", "List place links for an entity", &entityPlaceResource, []string{"Nested list uses fixed entity_id filter from path parameter."}),
		protectedListRouteSpec("/v1/events", "List events", &eventResource, []string{"parent_place_chain is normalized from JSON text when present."}),
		protectedDetailRouteSpec("/v1/events/{eventId}", "Get a single event", &eventResource, []string{"parent_place_chain is normalized from JSON text when present."}),
		protectedListRouteSpec("/v1/observations", "List observations", &observationResource, []string{"parent_place_chain is normalized from JSON text when present."}),
		protectedDetailRouteSpec("/v1/observations/{recordId}", "Get a single observation", &observationResource, []string{"parent_place_chain is normalized from JSON text when present."}),
		protectedListRouteSpec("/v1/metrics", "List metric definitions", &metricResource, []string{"enabled is normalized from ClickHouse scalar values."}),
		protectedDetailRouteSpec("/v1/metrics/{metricId}", "Get a single metric definition", &metricResource, []string{"enabled is normalized from ClickHouse scalar values."}),
		protectedListRouteSpec("/v1/analytics/rollups", "List metric rollups", &rollupResource, nil),
		protectedListRouteSpec("/v1/analytics/time-series", "List metric time-series points", &timeSeriesResource, nil),
		protectedListRouteSpec("/v1/analytics/hotspots", "List metric hotspots", &hotspotResource, nil),
		protectedListRouteSpec("/v1/analytics/cross-domain", "List cross-domain metric composites", &crossDomainResource, []string{"domains and metric_ids are normalized from JSON text when present."}),
		protectedCombinedSearchRouteSpec(),
		protectedSearchClassesRouteSpec(),
		protectedListRouteSpec("/v1/search/places", "List place search results", &searchPlaceResource, nil),
		protectedListRouteSpec("/v1/search/entities", "List entity search results", &searchEntityResource, nil),
		protectedListRouteSpec("/v1/query-dialects", "List registered OIDA-QL query dialects", &queryDialectResource, nil),
		{
			Method:      http.MethodGet,
			Path:        "/v1/registry/{name}",
			Summary:     "Fetch a saved query by name",
			Kind:        "saved_query",
			Auth:        protectedAuth(apiReadScopes),
			PathParams:  pathParamsFromRoute("/v1/registry/{name}"),
			Query:       apiQueryContract{Params: []apiQueryParamContract{{Name: "version", Type: "string", Required: false, Description: "Specific version to retrieve; defaults to latest."}}},
			Fields:      apiFieldsContract{Selectable: nil},
			Response:    apiResponseContract{Container: "item", Kind: "saved_query"},
			Notes:       []string{"Returns the latest version when ?version= is omitted."},
			handlerKind: apiHandlerKindRegistryLookup,
		},
		protectedOperationalRouteSpec(http.MethodGet, "/v1/internal/stats", "Service-side dashboard statistics", "internal_stats", "internal_stat", apiResponseContract{Container: "item", Kind: "internal_stats"}, []string{"Protected operational endpoint for internal dashboards."}, apiHandlerKindInternalStats),
		protectedOperationalQueryRouteSpec(http.MethodGet, "/v1/internal/worker-tail", "Recent worker and control-plane activity tail", "worker_tail", "worker_tail_entry", apiResponseContract{Container: "items", Kind: "worker_tail_entry", NextCursorField: "next_cursor"}, []apiQueryParamContract{{Name: "limit", Type: "integer", Required: false, Description: "Maximum number of tail entries to return."}, {Name: "cursor", Type: "string", Required: false, Description: "Opaque cursor for older tail entries."}, {Name: "source_id", Type: "string", Required: false, Description: "Optional source filter across fetch and parse activity."}, {Name: "correlation_id", Type: "string", Required: false, Description: "Optional correlation filter across API, workers, and control-plane jobs."}}, []string{"Protected operational endpoint backed by persisted worker/control-plane ledgers."}, apiHandlerKindWorkerTail),
	}
}

func routeContractFromSpec(spec apiRouteSpec) apiRouteContract {
	contract := apiRouteContract{
		Method:     spec.Method,
		Path:       spec.Path,
		Summary:    spec.Summary,
		Kind:       spec.Kind,
		ItemKind:   copyItemKind(spec.ItemKind),
		Auth:       spec.Auth,
		PathParams: copyPathParams(spec.PathParams),
		Query: apiQueryContract{
			Limit:        copyQueryLimit(spec.Query.Limit),
			Cursor:       spec.Query.Cursor,
			Q:            spec.Query.Q,
			Params:       copyQueryParams(spec.Query.Params),
			CursorParam:  spec.Query.CursorParam,
			SearchParam:  spec.Query.SearchParam,
			FieldsParam:  spec.Query.FieldsParam,
			FilterParams: append([]string(nil), spec.Query.FilterParams...),
		},
		Fields: apiFieldsContract{
			Param:      spec.Fields.Param,
			Default:    spec.Fields.Default,
			Selectable: append([]string(nil), spec.Fields.Selectable...),
		},
		Response:        spec.Response,
		Notes:           append([]string(nil), spec.Notes...),
		protected:       spec.Auth.Required,
		handlerKind:     spec.handlerKind,
		resourcePathRef: spec.Path,
	}
	return contract
}

func publicRouteSpec(method, path, summary, kind string, itemKind *string, pathParams []apiPathParamContract, queryParams []apiQueryParamContract, response apiResponseContract, notes []string, handlerKind apiHandlerKind) apiRouteSpec {
	return apiRouteSpec{
		Method:     method,
		Path:       path,
		Summary:    summary,
		Kind:       kind,
		ItemKind:   copyItemKind(itemKind),
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
		Response:    response,
		Notes:       append([]string(nil), notes...),
		handlerKind: handlerKind,
	}
}

func protectedAuth(scopes []string) apiAuthContract {
	return apiAuthContract{Required: true, Header: apiKeyHeader, Scopes: append([]string(nil), scopes...)}
}

func protectedOperationalRouteSpec(method, path, summary, kind, itemKind string, response apiResponseContract, notes []string, handlerKind apiHandlerKind) apiRouteSpec {
	item := itemKind
	spec := publicRouteSpec(method, path, summary, kind, &item, nil, nil, response, notes, handlerKind)
	spec.Auth = protectedAuth(apiInternalScopes)
	return spec
}

func protectedOperationalQueryRouteSpec(method, path, summary, kind, itemKind string, response apiResponseContract, queryParams []apiQueryParamContract, notes []string, handlerKind apiHandlerKind) apiRouteSpec {
	item := itemKind
	return apiRouteSpec{
		Method:     method,
		Path:       path,
		Summary:    summary,
		Kind:       kind,
		ItemKind:   &item,
		Auth:       protectedAuth(apiInternalScopes),
		PathParams: nil,
		Query: apiQueryContract{
			Limit:  &apiQueryLimitContract{Default: defaultWorkerTailLimit, Max: maxWorkerTailLimit},
			Cursor: true,
			Params: copyQueryParams(queryParams),
		},
		Fields:      apiFieldsContract{Selectable: nil},
		Response:    response,
		Notes:       append([]string(nil), notes...),
		handlerKind: handlerKind,
	}
}

func protectedListRouteSpec(path, summary string, spec *resourceSpec, notes []string) apiRouteSpec {
	item := spec.itemKind
	return apiRouteSpec{
		Method:     http.MethodGet,
		Path:       path,
		Summary:    summary,
		Kind:       spec.kind,
		ItemKind:   &item,
		Auth:       protectedAuth(apiReadScopes),
		PathParams: pathParamsFromRoute(path),
		Query:      spec.listQueryContract(),
		Fields:     spec.selectableFieldsContract(),
		Response: apiResponseContract{
			Container: "items",
			Kind:      spec.kind,
			Sort:      spec.idColumn + ":asc",
		},
		Notes:       append([]string(nil), notes...),
		handlerKind: apiHandlerKindList,
		resource:    spec,
	}
}

func protectedDetailRouteSpec(path, summary string, spec *resourceSpec, notes []string) apiRouteSpec {
	item := spec.itemKind
	return apiRouteSpec{
		Method:     http.MethodGet,
		Path:       path,
		Summary:    summary,
		Kind:       spec.kind,
		ItemKind:   &item,
		Auth:       protectedAuth(apiReadScopes),
		PathParams: pathParamsFromRoute(path),
		Query:      spec.detailQueryContract(),
		Fields:     spec.selectableFieldsContract(),
		Response: apiResponseContract{
			Container: "item",
			Kind:      spec.itemKind,
			Sort:      spec.idColumn + ":asc",
		},
		Notes:       append([]string(nil), notes...),
		handlerKind: apiHandlerKindDetail,
		resource:    spec,
	}
}

func protectedCombinedSearchRouteSpec() apiRouteSpec {
	item := "search_result"
	return apiRouteSpec{
		Method:   http.MethodGet,
		Path:     "/v1/search",
		Summary:  "Combined place/entity search with cursor pagination",
		Kind:     "search",
		ItemKind: &item,
		Auth:     protectedAuth(apiReadScopes),
		Query: apiQueryContract{
			Limit:  &apiQueryLimitContract{Default: defaultPageLimit, Max: maxPageLimit},
			Cursor: true,
			Q:      true,
			Params: []apiQueryParamContract{
				{Name: "q", Type: "string", Required: false, Description: "Case-insensitive search text applied to both place and entity dimensions."},
				{Name: "limit", Type: "int", Required: false, Description: fmt.Sprintf("Page size, default %d, max %d.", defaultPageLimit, maxPageLimit)},
				{Name: "cursor", Type: "string", Required: false, Description: "Opaque base64url cursor from prior response next_cursor."},
				{Name: "offset", Type: "int", Required: false, Description: "Skip this many rows before returning results. Non-negative integer; mutually exclusive with cursor."},
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
		handlerKind: apiHandlerKindCombinedSearch,
	}
}

func protectedSearchClassesRouteSpec() apiRouteSpec {
	item := "schema_class"
	return apiRouteSpec{
		Method:   http.MethodGet,
		Path:     "/v1/search/classes",
		Summary:  "List distinct entity and place data classes with counts",
		Kind:     "classes",
		ItemKind: &item,
		Auth:     protectedAuth(apiReadScopes),
		Query: apiQueryContract{
			Cursor: false,
			Q:      false,
			Params: nil,
		},
		Fields: apiFieldsContract{
			Selectable: nil,
		},
		Response: apiResponseContract{
			Container: "items",
			Kind:      "classes",
		},
		Notes: []string{
			"Returns all distinct entity_type and place_type values with row counts.",
			"category and description are merged from operator-curated seed metadata when available.",
		},
		handlerKind: apiHandlerKindSearchClasses,
	}
}

func routeHandlerForSpec(spec apiRouteSpec, version, readyMarker string, server *apiServer, schemaContract apiSchemaContract) http.HandlerFunc {
	switch spec.handlerKind {
	case apiHandlerKindHealth:
		return func(w http.ResponseWriter, r *http.Request) {
			respond(w, version, envelope{"status": "ok"})
		}
	case apiHandlerKindReady:
		return readyHandler(version, readyMarker)
	case apiHandlerKindVersion:
		return func(w http.ResponseWriter, r *http.Request) {
			respond(w, version, envelope{"service": "api", "api_version": version})
		}
	case apiHandlerKindSchema:
		payload := schemaContract
		return func(w http.ResponseWriter, r *http.Request) {
			respond(w, version, envelope{"auth": payload.Auth, "defaults": payload.Defaults, "endpoints": payload.Endpoints})
		}
	case apiHandlerKindList:
		return server.listHandler(*spec.resource)
	case apiHandlerKindDetail:
		return server.detailHandler(*spec.resource)
	case apiHandlerKindCombinedSearch:
		return server.combinedSearchHandler()
	case apiHandlerKindSearchClasses:
		return server.searchClassesHandler()
	case apiHandlerKindInternalStats:
		return server.internalStatsHandler()
	case apiHandlerKindWorkerTail:
		return server.workerTailHandler()
	case apiHandlerKindRegistryLookup:
		return server.registryLookupHandler()
	default:
		return nil
	}
}

func contractForSchema(contract apiRouteContract) apiRouteContract {
	out := contract
	out.handler = nil
	out.protected = false
	out.handlerKind = ""
	out.resourcePathRef = ""
	out.Auth.Scopes = append([]string(nil), out.Auth.Scopes...)
	out.PathParams = copyPathParams(out.PathParams)
	out.Query.FilterParams = append([]string(nil), out.Query.FilterParams...)
	out.Query.Params = copyQueryParams(out.Query.Params)
	out.Fields.Selectable = append([]string(nil), out.Fields.Selectable...)
	out.Notes = append([]string(nil), out.Notes...)
	if out.Response.CombinedSearch != nil {
		combined := *out.Response.CombinedSearch
		combined.ResultKinds = append([]apiCombinedSearchResultKindContract(nil), combined.ResultKinds...)
		out.Response.CombinedSearch = &combined
	}
	if out.ItemKind != nil {
		item := *out.ItemKind
		out.ItemKind = &item
	}
	return out
}

func schemaContractFromRoutes(contracts []apiRouteContract) apiSchemaContract {
	publicRoutes := make([]string, 0, len(contracts))
	endpoints := make([]apiRouteContract, 0, len(contracts))
	protectedHeader := apiKeyHeader
	for _, contract := range contracts {
		if contract.Method == http.MethodGet && !contract.Auth.Required {
			publicRoutes = append(publicRoutes, contract.Path)
		}
		if contract.Auth.Required && contract.Auth.Header != "" {
			protectedHeader = contract.Auth.Header
		}
		endpoints = append(endpoints, contractForSchema(enrichSchemaRouteContract(contract)))
	}
	return apiSchemaContract{
		Auth: apiSchemaAuthSummary{
			PublicRoutes:         publicRoutes,
			ProtectedPathPattern: "/v1/*",
			ProtectedHeader:      protectedHeader,
		},
		Defaults: apiSchemaDefaultsContract{
			Pagination:      apiQueryLimitContract{Default: defaultPageLimit, Max: maxPageLimit},
			CursorParam:     "cursor",
			SearchParam:     "q",
			FieldsParam:     "fields",
			NextCursorField: "next_cursor",
		},
		Endpoints: endpoints,
	}
}

func enrichSchemaRouteContract(contract apiRouteContract) apiRouteContract {
	out := contract
	if out.Query.Cursor {
		out.Query.CursorParam = "cursor"
		out.Response.NextCursorField = "next_cursor"
	}
	if out.Query.Q {
		out.Query.SearchParam = "q"
	}
	if hasQueryParam(out.Query.Params, "fields") {
		out.Query.FieldsParam = "fields"
		out.Fields.Param = "fields"
		out.Fields.Default = "all"
	}
	for _, param := range out.Query.Params {
		switch param.Name {
		case "limit", "cursor", "offset", "fields", "q":
			continue
		default:
			out.Query.FilterParams = append(out.Query.FilterParams, param.Name)
		}
	}
	if out.handlerKind == apiHandlerKindCombinedSearch {
		out.Response.CombinedSearch = &apiCombinedSearchContract{
			KindField: "kind",
			SortField: "cursor_key",
			ResultKinds: []apiCombinedSearchResultKindContract{
				{Kind: "place", IDField: "place_id"},
				{Kind: "entity", IDField: "entity_id"},
			},
		}
	}
	return out
}

func hasQueryParam(params []apiQueryParamContract, name string) bool {
	for _, param := range params {
		if param.Name == name {
			return true
		}
	}
	return false
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

func copyQueryLimit(in *apiQueryLimitContract) *apiQueryLimitContract {
	if in == nil {
		return nil
	}
	out := *in
	return &out
}

func copyItemKind(in *string) *string {
	if in == nil {
		return nil
	}
	out := *in
	return &out
}

func renderAPIReferenceMarkdown(contracts []apiRouteContract) string {
	var b strings.Builder
	b.WriteString("# API Reference\n\n")
	b.WriteString("The REST API is read-only. Frontend traffic should flow through a server-side BFF; the BFF attaches a scoped API key in `X-API-Key` for protected routes and keeps raw keys out of browser clients.\n\n")
	b.WriteString(renderAPIReferenceAccessSummary(contracts))
	b.WriteString("\n\n")
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

func renderREADMEAPIContractSection(contracts []apiRouteContract) string {
	var b strings.Builder
	b.WriteString("## Frontend Go REST API contract\n\n")
	b.WriteString("The read-only Go REST API keeps one authoritative route inventory shared by router registration, `/v1/schema`, contract fixtures, and generated docs.\n\n")
	b.WriteString(renderAPIReferenceAccessSummary(contracts))
	b.WriteString("\n\n")
	b.WriteString("Current route inventory:\n")
	for _, contract := range contracts {
		b.WriteString("- `")
		b.WriteString(contract.Method)
		b.WriteString(" ")
		b.WriteString(contract.Path)
		b.WriteString("`")
		if contract.Auth.Required {
			b.WriteString(" — protected")
		} else {
			b.WriteString(" — public")
		}
		b.WriteString("\n")
	}
	return strings.TrimSuffix(b.String(), "\n")
}

func renderAPIReferenceAccessSummary(contracts []apiRouteContract) string {
	publicRoutes := make([]string, 0, len(contracts))
	protectedHeader := apiKeyHeader
	for _, contract := range contracts {
		if contract.Method != http.MethodGet {
			continue
		}
		if contract.Auth.Required {
			if contract.Auth.Header != "" {
				protectedHeader = contract.Auth.Header
			}
			continue
		}
		publicRoutes = append(publicRoutes, fmt.Sprintf("`%s`", contract.Path))
	}
	if len(publicRoutes) == 0 {
		return fmt.Sprintf("All `/v1/*` routes require `%s`.", protectedHeader)
	}
	return fmt.Sprintf("Public routes: %s. All other `/v1/*` routes require `%s` with the route's documented scopes.", strings.Join(publicRoutes, ", "), protectedHeader)
}
