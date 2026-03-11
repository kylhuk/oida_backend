package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"global-osint-backend/internal/discovery"
	"global-osint-backend/internal/fetch"
)

func TestClaimFrontierLease(t *testing.T) {
	now := time.Date(2026, time.March, 10, 12, 0, 0, 0, time.UTC)
	entry := discovery.FrontierEntry{
		SourceID:    "seed:gdelt",
		State:       discovery.FrontierStatePending,
		NextFetchAt: now,
	}

	leased := entry.ClaimLease("worker-fetch-1", 2*time.Minute, now)
	if leased.State != discovery.FrontierStateLeased {
		t.Fatalf("expected state %q, got %q", discovery.FrontierStateLeased, leased.State)
	}
	if leased.AttemptCount != 1 {
		t.Fatalf("expected attempt_count=1, got %d", leased.AttemptCount)
	}
	if leased.LeaseOwner == nil || *leased.LeaseOwner != "worker-fetch-1" {
		t.Fatalf("expected lease owner to be tracked, got %#v", leased.LeaseOwner)
	}
	if leased.LeaseExpiresAt == nil || !leased.LeaseExpiresAt.Equal(now.Add(2*time.Minute)) {
		t.Fatalf("expected lease expiry to equal now+2m, got %#v", leased.LeaseExpiresAt)
	}
}

func TestFailedFetchPersistsLogOnly(t *testing.T) {
	now := time.Date(2026, time.March, 10, 12, 0, 0, 0, time.UTC)
	persisted, err := fetch.RetainResponse(context.Background(), fetch.PersistOptions{
		FetchID:  "fetch:failed",
		RawID:    "raw:failed",
		SourceID: "seed:gdelt",
		Bucket:   "raw",
		Policy:   fetch.ResolveRetentionPolicy("warm"),
		Now:      now,
	}, fetch.Request{
		Method: "GET",
		URL:    "https://example.com/feed",
		Source: fetch.SourcePolicy{SourceID: "seed:gdelt", RetentionClass: "warm", SupportsLiveGET: true, ForceObjectStore: true},
	}, fetch.Response{
		FetchURL:     "https://example.com/feed",
		FinalURL:     "https://example.com/feed",
		SourceID:     "seed:gdelt",
		Method:       "GET",
		StatusCode:   500,
		Success:      false,
		FetchedAt:    now,
		Latency:      20 * time.Millisecond,
		Attempts:     3,
		BodyBytes:    0,
		ErrorMessage: "upstream error",
	}, nil)
	if err != nil {
		t.Fatalf("retain response: %v", err)
	}
	if persisted.RawDocument != nil {
		t.Fatalf("expected failed non-304 fetch to skip raw document write, got %#v", persisted.RawDocument)
	}
	if persisted.FetchLog.AttemptCount != 3 || persisted.FetchLog.RetryCount != 2 {
		t.Fatalf("expected fetch log attempt/retry counts 3/2, got %d/%d", persisted.FetchLog.AttemptCount, persisted.FetchLog.RetryCount)
	}
}

func TestMissingCredentialBlocksFetch(t *testing.T) {
	policy := sourcePolicyRecord{
		SourceID:       "fixture:acled",
		AuthMode:       "user_supplied_key",
		AuthConfigJSON: `{"env_var":"SOURCE_ACLED_API_KEY","placement":"header","name":"Authorization","prefix":"Bearer "}`,
	}
	headers, preparedURL, err := resolveAuthRequest(policy, "https://api.example.test/events")
	if !errors.Is(err, errMissingCredential) {
		t.Fatalf("expected missing credential error, got %v", err)
	}
	if headers != nil || preparedURL != "" {
		t.Fatalf("expected missing credential auth resolution to stop request preparation, got headers=%v url=%q", headers, preparedURL)
	}
}
