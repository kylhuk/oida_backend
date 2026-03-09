package main

import (
	"net/http"
	"net/http/httptest"
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
