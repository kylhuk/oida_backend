package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestRespond(t *testing.T) {
	rr := httptest.NewRecorder()
	respond(rr, "v1", envelope{"status": "ok"})
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 got %d", rr.Code)
	}
	if rr.Body.Len() == 0 {
		t.Fatal("expected body")
	}
}

func TestListStub(t *testing.T) {
	h := listStub("v1", "sources")
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/sources", nil)
	h(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 got %d", rr.Code)
	}
}

func TestReady(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "bootstrap.ready")
	h := readyHandler("v1", marker)

	t.Run("false before marker exists", func(t *testing.T) {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v1/ready", nil)
		h(rr, req)

		if rr.Code != http.StatusOK {
			t.Fatalf("expected 200 got %d", rr.Code)
		}
		if ready := decodeReady(t, rr.Body.Bytes()); ready {
			t.Fatal("expected readiness false before bootstrap marker exists")
		}
	})

	t.Run("true after marker exists", func(t *testing.T) {
		if err := os.WriteFile(marker, []byte("ready\n"), 0o644); err != nil {
			t.Fatalf("write marker: %v", err)
		}

		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v1/ready", nil)
		h(rr, req)

		if rr.Code != http.StatusOK {
			t.Fatalf("expected 200 got %d", rr.Code)
		}
		if ready := decodeReady(t, rr.Body.Bytes()); !ready {
			t.Fatal("expected readiness true after bootstrap marker exists")
		}
	})
}

func decodeReady(t *testing.T, body []byte) bool {
	t.Helper()

	var payload struct {
		Data struct {
			Ready bool `json:"ready"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return payload.Data.Ready
}
