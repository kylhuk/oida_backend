package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
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

func TestAPIReferenceParity(t *testing.T) {
	contracts := buildRouteContracts("v1", "", &apiServer{version: "v1"})
	doc := loadDocFixture(t, "api-reference.md")
	expected := renderAPIReferenceMarkdown(contracts)

	if doc != expected {
		t.Fatal("docs/api-reference.md drifted from route contract renderer output")
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

	expected := loadContractFixture(t, fixture)

	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("contract mismatch for %s\nactual:   %#v\nexpected: %#v", url, actual, expected)
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
