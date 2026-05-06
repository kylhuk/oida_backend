package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
)

func TestHealthContract(t *testing.T) {
	ts := httptest.NewServer(newAPIMux("v1", ""))
	defer ts.Close()

	assertContract(t, ts.URL+"/v1/health", "api_v1_health.json")
}

func TestSchemaContract(t *testing.T) {
	ts := httptest.NewServer(newAPIMux("v1", ""))
	defer ts.Close()

	assertContract(t, ts.URL+"/v1/schema", "api_v1_schema.json")
}

func TestSchemaContractFrontendMetadata(t *testing.T) {
	contracts := buildRouteContracts("v1", "", &apiServer{version: "v1"})
	schema := schemaContractFromRoutes(contracts)

	if !reflect.DeepEqual(schema.Auth.PublicRoutes, []string{"/v1/health", "/v1/ready", "/v1/version", "/v1/schema"}) {
		t.Fatalf("unexpected public routes: %#v", schema.Auth.PublicRoutes)
	}
	if schema.Auth.ProtectedPathPattern != "/v1/*" {
		t.Fatalf("unexpected protected path pattern %q", schema.Auth.ProtectedPathPattern)
	}
	if schema.Auth.ProtectedHeader != apiKeyHeader {
		t.Fatalf("unexpected protected header %q", schema.Auth.ProtectedHeader)
	}
	if schema.Defaults.Pagination.Default != defaultPageLimit || schema.Defaults.Pagination.Max != maxPageLimit {
		t.Fatalf("unexpected pagination defaults: %#v", schema.Defaults.Pagination)
	}
	if schema.Defaults.CursorParam != "cursor" || schema.Defaults.SearchParam != "q" || schema.Defaults.FieldsParam != "fields" || schema.Defaults.NextCursorField != "next_cursor" {
		t.Fatalf("unexpected shared defaults: %#v", schema.Defaults)
	}

	var jobsEndpoint *apiRouteContract
	var schemaEndpoint *apiRouteContract
	var searchEndpoint *apiRouteContract
	var internalStatsEndpoint *apiRouteContract
	for i := range schema.Endpoints {
		switch schema.Endpoints[i].Path {
		case "/v1/jobs":
			jobsEndpoint = &schema.Endpoints[i]
		case "/v1/schema":
			schemaEndpoint = &schema.Endpoints[i]
		case "/v1/search":
			searchEndpoint = &schema.Endpoints[i]
		case "/v1/internal/stats":
			internalStatsEndpoint = &schema.Endpoints[i]
		}
	}
	if jobsEndpoint == nil || schemaEndpoint == nil || searchEndpoint == nil || internalStatsEndpoint == nil {
		t.Fatalf("missing expected schema endpoints: jobs=%v schema=%v search=%v internalStats=%v", jobsEndpoint != nil, schemaEndpoint != nil, searchEndpoint != nil, internalStatsEndpoint != nil)
	}
	if !reflect.DeepEqual(jobsEndpoint.Query.FilterParams, []string{"job_type", "status"}) {
		t.Fatalf("unexpected jobs filter params: %#v", jobsEndpoint.Query.FilterParams)
	}
	if jobsEndpoint.Query.CursorParam != "cursor" || jobsEndpoint.Query.SearchParam != "q" || jobsEndpoint.Query.FieldsParam != "fields" {
		t.Fatalf("jobs endpoint missing query metadata: %#v", jobsEndpoint.Query)
	}
	if jobsEndpoint.Fields.Param != "fields" || jobsEndpoint.Fields.Default != "all" {
		t.Fatalf("jobs endpoint missing field selection metadata: %#v", jobsEndpoint.Fields)
	}
	if jobsEndpoint.Response.NextCursorField != "next_cursor" {
		t.Fatalf("jobs endpoint missing next cursor metadata: %#v", jobsEndpoint.Response)
	}
	if schemaEndpoint.Auth.Required {
		t.Fatalf("schema endpoint must remain public: %#v", schemaEndpoint.Auth)
	}
	if schemaEndpoint.Query.FieldsParam != "" || schemaEndpoint.Fields.Param != "" {
		t.Fatalf("schema endpoint should not advertise field selection: query=%#v fields=%#v", schemaEndpoint.Query, schemaEndpoint.Fields)
	}
	if searchEndpoint.Response.CombinedSearch == nil {
		t.Fatalf("combined search metadata missing from response: %#v", searchEndpoint.Response)
	}
	if searchEndpoint.Response.CombinedSearch.KindField != "kind" || searchEndpoint.Response.CombinedSearch.SortField != "cursor_key" {
		t.Fatalf("unexpected combined search shape: %#v", searchEndpoint.Response.CombinedSearch)
	}
	if !reflect.DeepEqual(searchEndpoint.Response.CombinedSearch.ResultKinds, []apiCombinedSearchResultKindContract{{Kind: "place", IDField: "place_id"}, {Kind: "entity", IDField: "entity_id"}}) {
		t.Fatalf("unexpected combined search result kinds: %#v", searchEndpoint.Response.CombinedSearch.ResultKinds)
	}
	if !internalStatsEndpoint.Auth.Required || internalStatsEndpoint.Auth.Header != apiKeyHeader {
		t.Fatalf("internal stats endpoint must remain protected in schema: %#v", internalStatsEndpoint.Auth)
	}
}

func TestAPIReferenceParity(t *testing.T) {
	contracts := buildRouteContracts("v1", "", &apiServer{version: "v1"})
	doc := loadDocFixture(t, "api-reference.md")
	expected := renderAPIReferenceMarkdown(contracts)

	if doc != expected {
		t.Fatal("docs/api-reference.md drifted from generated route contract output")
	}
}

func TestFrontendDocsEntryPointsExist(t *testing.T) {
	if path, err := resolveDocPath("api-reference.md"); err != nil {
		t.Fatalf("resolve docs/api-reference.md: %v", err)
	} else if info, statErr := os.Stat(path); statErr != nil {
		t.Fatalf("stat docs/api-reference.md: %v", statErr)
	} else if info.IsDir() {
		t.Fatalf("expected docs/api-reference.md to be a file, got directory %q", path)
	}

	if path, err := resolveREADMEPath(); err != nil {
		t.Fatalf("resolve README.md: %v", err)
	} else if info, statErr := os.Stat(path); statErr != nil {
		t.Fatalf("stat README.md: %v", statErr)
	} else if info.IsDir() {
		t.Fatalf("expected README.md to be a file, got directory %q", path)
	}
}

func TestREADMEAPIInventoryParity(t *testing.T) {
	contracts := buildRouteContracts("v1", "", &apiServer{version: "v1"})
	path, err := resolveREADMEPath()
	if err != nil {
		t.Fatalf("resolve README path: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read README: %v", err)
	}

	const beginMarker = "<!-- BEGIN GENERATED: api-route-inventory -->"
	const endMarker = "<!-- END GENERATED: api-route-inventory -->"
	expected := renderREADMEAPIContractSection(contracts)
	actual, ok := extractMarkedSection(string(data), beginMarker, endMarker)
	if !ok {
		t.Fatalf("README missing generated route inventory markers %q/%q", beginMarker, endMarker)
	}
	if actual != expected {
		t.Fatal("README route inventory drifted from generated route contract output")
	}
}

func TestRegenerateAPIReference(t *testing.T) {
	if os.Getenv("WRITE_API_REFERENCE") != "1" {
		t.Skip("set WRITE_API_REFERENCE=1 to regenerate docs/api-reference.md")
	}
	contracts := buildRouteContracts("v1", "", &apiServer{version: "v1"})
	path, err := resolveDocPath("api-reference.md")
	if err != nil {
		t.Fatalf("resolve api-reference path: %v", err)
	}
	if err := os.WriteFile(path, []byte(renderAPIReferenceMarkdown(contracts)), 0o644); err != nil {
		t.Fatalf("write api-reference.md: %v", err)
	}
}

func TestRegenerateREADMEAPIInventory(t *testing.T) {
	if os.Getenv("WRITE_README_API_INVENTORY") != "1" {
		t.Skip("set WRITE_README_API_INVENTORY=1 to regenerate README route inventory section")
	}
	contracts := buildRouteContracts("v1", "", &apiServer{version: "v1"})
	path, err := resolveREADMEPath()
	if err != nil {
		t.Fatalf("resolve README path: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read README: %v", err)
	}

	const beginMarker = "<!-- BEGIN GENERATED: api-route-inventory -->"
	const endMarker = "<!-- END GENERATED: api-route-inventory -->"
	updated, ok := replaceMarkedSection(string(data), beginMarker, endMarker, renderREADMEAPIContractSection(contracts))
	if !ok {
		t.Fatalf("README missing generated route inventory markers %q/%q", beginMarker, endMarker)
	}
	if err := os.WriteFile(path, []byte(updated), 0o644); err != nil {
		t.Fatalf("write README: %v", err)
	}
}

func TestRouteMetadataIsAuthoritativeForInventory(t *testing.T) {
	specs := buildRouteSpecs()
	contracts := buildRouteContracts("v1", "", &apiServer{version: "v1"})

	if len(contracts) != len(specs) {
		t.Fatalf("route contract count %d does not match route spec count %d", len(contracts), len(specs))
	}

	for i, spec := range specs {
		contract := contracts[i]
		if contract.Method != spec.Method || contract.Path != spec.Path {
			t.Fatalf("route order drift at index %d: got %s %s want %s %s", i, contract.Method, contract.Path, spec.Method, spec.Path)
		}
		if contract.handler == nil {
			t.Fatalf("route %s %s missing handler", contract.Method, contract.Path)
		}
		if contract.handlerKind != spec.handlerKind {
			t.Fatalf("route %s %s handler kind %q does not match spec %q", contract.Method, contract.Path, contract.handlerKind, spec.handlerKind)
		}
		if !reflect.DeepEqual(contract.Auth, spec.Auth) {
			t.Fatalf("route %s %s auth mismatch: got %#v want %#v", contract.Method, contract.Path, contract.Auth, spec.Auth)
		}
		if !reflect.DeepEqual(copyPathParams(contract.PathParams), copyPathParams(spec.PathParams)) {
			t.Fatalf("route %s %s path params mismatch: got %#v want %#v", contract.Method, contract.Path, contract.PathParams, spec.PathParams)
		}
		if !reflect.DeepEqual(contract.Query, spec.Query) {
			t.Fatalf("route %s %s query mismatch: got %#v want %#v", contract.Method, contract.Path, contract.Query, spec.Query)
		}
		if !reflect.DeepEqual(contract.Fields, spec.Fields) {
			t.Fatalf("route %s %s fields mismatch: got %#v want %#v", contract.Method, contract.Path, contract.Fields, spec.Fields)
		}
		if contract.resourcePathRef != spec.Path {
			t.Fatalf("route %s %s resource path ref mismatch: got %q want %q", contract.Method, contract.Path, contract.resourcePathRef, spec.Path)
		}

		if spec.resource != nil {
			var expectedQuery apiQueryContract
			switch spec.handlerKind {
			case apiHandlerKindList:
				expectedQuery = spec.resource.listQueryContract()
			case apiHandlerKindDetail:
				expectedQuery = spec.resource.detailQueryContract()
			default:
				t.Fatalf("unexpected resource-backed handler kind %q", spec.handlerKind)
			}
			if !reflect.DeepEqual(contract.Query, expectedQuery) {
				t.Fatalf("route %s %s query drifted from resource spec", contract.Method, contract.Path)
			}
			if !reflect.DeepEqual(contract.Fields, spec.resource.selectableFieldsContract()) {
				t.Fatalf("route %s %s fields drifted from resource spec", contract.Method, contract.Path)
			}
		}
	}

	publicGetPaths := make([]string, 0, len(contracts))
	for _, contract := range contracts {
		if contract.Method == http.MethodGet && !contract.Auth.Required {
			publicGetPaths = append(publicGetPaths, contract.Path)
		}
	}
	if !slices.Equal(publicGetPaths, []string{"/v1/health", "/v1/ready", "/v1/version", "/v1/schema"}) {
		t.Fatalf("unexpected public route inventory: %#v", publicGetPaths)
	}
}

func assertContract(t *testing.T, url, fixture string) {
	t.Helper()

	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("%s request: %v", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("%s status %d", url, resp.StatusCode)
	}

	var actual map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&actual); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	delete(actual, "generated_at")

	if os.Getenv("WRITE_CONTRACT_FIXTURES") == "1" {
		writeContractFixture(t, fixture, actual)
		return
	}

	expected := loadContractFixture(t, fixture)

	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("contract mismatch for %s\nactual:   %#v\nexpected: %#v", url, actual, expected)
	}
}

func writeContractFixture(t *testing.T, name string, payload map[string]any) {
	t.Helper()
	path := filepath.Join("..", "..", "testdata", "fixtures", "contracts", name)
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		t.Fatalf("marshal fixture %s: %v", name, err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write fixture %s: %v", name, err)
	}
}

func loadContractFixture(t *testing.T, name string) map[string]any {
	t.Helper()

	relativePaths := []string{
		filepath.Join("testdata", "fixtures", "contracts", name),
		filepath.Join("..", "..", "testdata", "fixtures", "contracts", name),
	}

	var lastErr error
	for _, path := range relativePaths {
		data, err := os.ReadFile(path)
		if err != nil {
			lastErr = err
			continue
		}

		var payload map[string]any
		if err := json.Unmarshal(data, &payload); err != nil {
			t.Fatalf("decode fixture %s: %v", name, err)
		}

		return payload
	}

	if lastErr == nil {
		lastErr = os.ErrNotExist
	}
	t.Fatalf("read fixture %s: %v", name, lastErr)
	return nil
}

func loadDocFixture(t *testing.T, name string) string {
	t.Helper()
	path, err := resolveDocPath(name)
	if err != nil {
		t.Fatalf("resolve doc fixture %s: %v", name, err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read doc fixture %s: %v", name, err)
	}
	return string(data)
}

func resolveDocPath(name string) (string, error) {
	relativePaths := []string{
		filepath.Join("docs", name),
		filepath.Join("..", "..", "docs", name),
	}

	var lastErr error
	for _, path := range relativePaths {
		_, err := os.Stat(path)
		if err != nil {
			lastErr = err
			continue
		}
		return path, nil
	}

	if lastErr == nil {
		lastErr = os.ErrNotExist
	}
	return "", lastErr
}

func resolveREADMEPath() (string, error) {
	relativePaths := []string{
		"README.md",
		filepath.Join("..", "..", "README.md"),
	}

	var lastErr error
	for _, path := range relativePaths {
		_, err := os.Stat(path)
		if err != nil {
			lastErr = err
			continue
		}
		return path, nil
	}

	if lastErr == nil {
		lastErr = os.ErrNotExist
	}
	return "", lastErr
}

func extractMarkedSection(doc, beginMarker, endMarker string) (string, bool) {
	start := strings.Index(doc, beginMarker)
	if start == -1 {
		return "", false
	}
	start += len(beginMarker)
	if start < len(doc) && doc[start] == '\n' {
		start++
	}
	end := strings.Index(doc[start:], endMarker)
	if end == -1 {
		return "", false
	}
	section := doc[start : start+end]
	section = strings.TrimSuffix(section, "\n")
	return section, true
}

func replaceMarkedSection(doc, beginMarker, endMarker, replacement string) (string, bool) {
	start := strings.Index(doc, beginMarker)
	if start == -1 {
		return "", false
	}
	end := strings.Index(doc[start+len(beginMarker):], endMarker)
	if end == -1 {
		return "", false
	}
	end += start + len(beginMarker)

	var b strings.Builder
	b.WriteString(doc[:start])
	b.WriteString(beginMarker)
	b.WriteString("\n")
	b.WriteString(strings.TrimSuffix(replacement, "\n"))
	b.WriteString("\n")
	b.WriteString(doc[end:])
	return b.String(), true
}
