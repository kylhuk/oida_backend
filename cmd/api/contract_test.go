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
