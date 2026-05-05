package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"global-osint-backend/internal/discovery"
	"global-osint-backend/internal/fetch"
)

func TestClaimFrontierLease(t *testing.T) {
	now := time.Date(2026, time.March, 10, 12, 0, 0, 0, time.UTC)
	t.Run("records active lease details", func(t *testing.T) {
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
	})

	t.Run("reclaims expired leased rows in both claim queries", func(t *testing.T) {
		selectQuery := buildClaimFrontierLeaseSelectQuery("seed:gdelt", now)
		if !strings.Contains(selectQuery, "state = 'leased' AND lease_expires_at IS NOT NULL AND lease_expires_at <= toDateTime64('") {
			t.Fatalf("expected claim select query to include expired leased rows, got %s", selectQuery)
		}

		updateQuery := buildClaimFrontierLeaseUpdateQuery("seed:gdelt", "worker-fetch-1", "https://example.com/feed", 2, now.Add(2*time.Minute), now, now)
		if !strings.Contains(updateQuery, "state = 'leased' AND lease_expires_at IS NOT NULL AND lease_expires_at <= toDateTime64('") {
			t.Fatalf("expected claim update query to allow reclaiming expired leased rows, got %s", updateQuery)
		}
	})

	t.Run("guards frontier result updates by lease owner", func(t *testing.T) {
		updated := discovery.FrontierEntry{
			SourceID:       "seed:gdelt",
			CanonicalURL:   "https://example.com/feed",
			State:          discovery.FrontierStateFetched,
			AttemptCount:   1,
			LastAttemptAt:  ptr(now),
			LastFetchID:    ptr("fetch-123"),
			LastStatusCode: ptr(uint16(http.StatusOK)),
			NextFetchAt:    now,
		}

		query := buildUpdateFrontierEntryQuery("worker-fetch-1", updated)
		if !strings.Contains(query, "AND state = 'leased' AND lease_owner = 'worker-fetch-1'") {
			t.Fatalf("expected frontier update query to guard the active lease owner, got %s", query)
		}
	})
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

func TestResolveAuthRequestOAuth2ClientCredentials(t *testing.T) {
	resetOAuthTokenCache()
	defer resetOAuthTokenCache()

	requestCount := 0
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		if r.Method != http.MethodPost {
			t.Fatalf("expected POST token request, got %s", r.Method)
		}
		if err := r.ParseForm(); err != nil {
			t.Fatalf("parse token form: %v", err)
		}
		if got := r.Form.Get("grant_type"); got != "client_credentials" {
			t.Fatalf("expected grant_type client_credentials, got %q", got)
		}
		if got := r.Form.Get("client_id"); got != "cid" {
			t.Fatalf("expected client_id cid, got %q", got)
		}
		if got := r.Form.Get("client_secret"); got != "csecret" {
			t.Fatalf("expected client_secret csecret, got %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"abc123","expires_in":1800}`))
	}))
	defer tokenServer.Close()

	t.Setenv("SOURCE_OPENSKY_NETWORK_CLIENT_ID", "cid")
	t.Setenv("SOURCE_OPENSKY_NETWORK_CLIENT_SECRET", "csecret")

	policy := sourcePolicyRecord{
		SourceID:       "catalog:auto:aviation-airports-drones-and-mobility-opensky-network",
		AuthMode:       "oauth2_client_credentials",
		AuthConfigJSON: `{"client_id_env_var":"SOURCE_OPENSKY_NETWORK_CLIENT_ID","client_secret_env_var":"SOURCE_OPENSKY_NETWORK_CLIENT_SECRET","token_url":"` + tokenServer.URL + `","grant_type":"client_credentials","placement":"header","name":"Authorization","prefix":"Bearer "}`,
	}

	headers, preparedURL, err := resolveAuthRequest(policy, "https://opensky-network.org/api/states/all?extended=1")
	if err != nil {
		t.Fatalf("resolve auth request: %v", err)
	}
	if preparedURL != "https://opensky-network.org/api/states/all?extended=1" {
		t.Fatalf("expected URL unchanged, got %q", preparedURL)
	}
	if got := headers.Get("Authorization"); got != "Bearer abc123" {
		t.Fatalf("expected bearer token header, got %q", got)
	}

	headers2, _, err := resolveAuthRequest(policy, "https://opensky-network.org/api/states/all?extended=1")
	if err != nil {
		t.Fatalf("resolve auth request second call: %v", err)
	}
	if got := headers2.Get("Authorization"); got != "Bearer abc123" {
		t.Fatalf("expected bearer token header from cache, got %q", got)
	}
	if requestCount != 1 {
		t.Fatalf("expected exactly one token request due to cache, got %d", requestCount)
	}
}

func TestResolveAuthRequestOAuth2MissingCredential(t *testing.T) {
	resetOAuthTokenCache()
	defer resetOAuthTokenCache()
	os.Unsetenv("SOURCE_OPENSKY_NETWORK_CLIENT_ID")
	os.Unsetenv("SOURCE_OPENSKY_NETWORK_CLIENT_SECRET")

	policy := sourcePolicyRecord{
		SourceID:       "catalog:auto:aviation-airports-drones-and-mobility-opensky-network",
		AuthMode:       "oauth2_client_credentials",
		AuthConfigJSON: `{"client_id_env_var":"SOURCE_OPENSKY_NETWORK_CLIENT_ID","client_secret_env_var":"SOURCE_OPENSKY_NETWORK_CLIENT_SECRET","token_url":"https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"}`,
	}
	_, _, err := resolveAuthRequest(policy, "https://opensky-network.org/api/states/all?extended=1")
	if !errors.Is(err, errMissingCredential) {
		t.Fatalf("expected missing credential error, got %v", err)
	}
	if !strings.Contains(err.Error(), "SOURCE_OPENSKY_NETWORK_CLIENT_ID") {
		t.Fatalf("expected missing client id env var in error, got %v", err)
	}
}

func TestResolveAuthRequestUserSuppliedKeyQueryPlacement(t *testing.T) {
	t.Setenv("SOURCE_AISHUB_USERNAME", "mariner")
	policy := sourcePolicyRecord{
		SourceID:       "catalog:auto:maritime-ocean-and-coastal-sources-aishub",
		AuthMode:       "user_supplied_key",
		AuthConfigJSON: `{"env_var":"SOURCE_AISHUB_USERNAME","placement":"query","name":"username","prefix":""}`,
	}
	headers, preparedURL, err := resolveAuthRequest(policy, "https://data.aishub.net/ws.php?format=1&output=json")
	if err != nil {
		t.Fatalf("resolve auth request: %v", err)
	}
	if len(headers) != 0 {
		t.Fatalf("expected no auth headers for query placement, got %v", headers)
	}
	if !strings.Contains(preparedURL, "username=mariner") {
		t.Fatalf("expected query credential in URL, got %q", preparedURL)
	}
}

func TestResolveAuthRequestNoAuthMode(t *testing.T) {
	policy := sourcePolicyRecord{
		SourceID:       "catalog:auto:aviation-airports-drones-and-mobility-airplanes-live",
		AuthMode:       "none",
		AuthConfigJSON: "",
	}
	headers, preparedURL, err := resolveAuthRequest(policy, "https://api.airplanes.live/v2/mil")
	if err != nil {
		t.Fatalf("resolve auth request: %v", err)
	}
	if len(headers) != 0 {
		t.Fatalf("expected no auth headers for no-auth source, got %v", headers)
	}
	if preparedURL != "https://api.airplanes.live/v2/mil" {
		t.Fatalf("expected URL unchanged, got %q", preparedURL)
	}
}

func TestFrontierFetchOutcomeKeepsResultState(t *testing.T) {
	now := time.Date(2026, time.March, 10, 12, 0, 0, 0, time.UTC)
	lease := discovery.FrontierEntry{
		SourceID:     "seed:gdelt",
		CanonicalURL: "https://example.com/feed",
		State:        discovery.FrontierStatePending,
		NextFetchAt:  now,
	}.ClaimLease("worker-fetch-1", time.Minute, now)

	tests := []struct {
		name       string
		statusCode int
		want       string
	}{
		{name: "200 fetched", statusCode: http.StatusOK, want: discovery.FrontierStateFetched},
		{name: "204 fetched", statusCode: http.StatusNoContent, want: discovery.FrontierStateFetched},
		{name: "304 not modified", statusCode: http.StatusNotModified, want: discovery.FrontierStateNotModified},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			updated := lease.ApplyFetchOutcome(frontierOutcomeFromFetch(
				"fetch-123",
				now.Add(time.Minute),
				fetch.Response{StatusCode: tt.statusCode},
				nil,
				fetch.RetryPolicy{MaxAttempts: 3, InitialBackoff: time.Second, MaxBackoff: 5 * time.Second},
			))
			if updated.State != tt.want {
				t.Fatalf("expected state %q, got %q", tt.want, updated.State)
			}
			if updated.State == discovery.FrontierStatePending {
				t.Fatal("expected successful fetch outcomes to remain terminal and not return to pending")
			}
		})
	}
}

func TestFrontierFetchOutcomeBlockedSourceStaysBlocked(t *testing.T) {
	now := time.Date(2026, time.March, 10, 12, 0, 0, 0, time.UTC)
	lease := discovery.FrontierEntry{
		SourceID:     "seed:gdelt",
		CanonicalURL: "https://example.com/feed",
		State:        discovery.FrontierStatePending,
		NextFetchAt:  now,
	}.ClaimLease("worker-fetch-1", time.Minute, now)

	updated := lease.ApplyFetchOutcome(frontierOutcomeFromFetch(
		"fetch-blocked",
		now.Add(time.Minute),
		fetch.Response{},
		fmt.Errorf("%w: live HTTP fetch disabled by source policy", fetch.ErrSourceBlocked),
		fetch.RetryPolicy{MaxAttempts: 3, InitialBackoff: time.Second, MaxBackoff: 5 * time.Second},
	))

	if updated.State != discovery.FrontierStateBlocked {
		t.Fatalf("expected state %q, got %q", discovery.FrontierStateBlocked, updated.State)
	}
	if updated.LeaseOwner != nil || updated.LeaseExpiresAt != nil {
		t.Fatalf("expected blocked outcome to clear lease, got owner=%#v expiry=%#v", updated.LeaseOwner, updated.LeaseExpiresAt)
	}
}

func TestSanitizeFetchPersistenceRedactsQueryPlacementURL(t *testing.T) {
	now := time.Date(2026, time.March, 10, 12, 0, 0, 0, time.UTC)
	safeURL := "https://data.aishub.net/ws.php?format=1&output=json"
	preparedURL := safeURL + "&username=mariner"
	policy := sourcePolicyRecord{
		SourceID:       "catalog:auto:maritime-ocean-and-coastal-sources-aishub",
		AuthMode:       "user_supplied_key",
		AuthConfigJSON: `{"env_var":"SOURCE_AISHUB_USERNAME","placement":"query","name":"username","prefix":""}`,
	}
	body := []byte(`{"ok":true}`)
	request := fetch.Request{
		Method: "GET",
		URL:    preparedURL,
		Source: fetch.SourcePolicy{SourceID: policy.SourceID, RetentionClass: "warm", SupportsLiveGET: true},
	}
	response := fetch.Response{
		FetchURL:    preparedURL,
		FinalURL:    preparedURL,
		SourceID:    policy.SourceID,
		Method:      "GET",
		StatusCode:  200,
		Success:     true,
		FetchedAt:   now,
		Body:        body,
		BodyBytes:   int64(len(body)),
		ContentHash: "hash-123",
		ContentType: "application/json",
	}

	persistRequest, persistResponse := sanitizeFetchPersistence(policy, safeURL, request, response)
	if request.URL != preparedURL || response.FetchURL != preparedURL || response.FinalURL != preparedURL {
		t.Fatalf("expected live request values to remain unchanged, got request=%q fetch=%q final=%q", request.URL, response.FetchURL, response.FinalURL)
	}
	if persistRequest.URL != safeURL {
		t.Fatalf("expected persisted request URL to use safe URL, got %q", persistRequest.URL)
	}
	if persistResponse.FetchURL != safeURL || persistResponse.FinalURL != safeURL {
		t.Fatalf("expected persisted response URLs to be safe, got fetch=%q final=%q", persistResponse.FetchURL, persistResponse.FinalURL)
	}
	persisted, err := fetch.RetainResponse(context.Background(), fetch.PersistOptions{
		FetchID:  "fetch:redacted-url",
		RawID:    "raw:redacted-url",
		SourceID: policy.SourceID,
		Bucket:   "raw",
		Policy:   fetch.ResolveRetentionPolicy("warm"),
		Now:      now,
	}, persistRequest, persistResponse, nil)
	if err != nil {
		t.Fatalf("retain response: %v", err)
	}
	if persisted.Metadata.FetchURL != safeURL || persisted.Metadata.FinalURL != safeURL {
		t.Fatalf("expected persisted metadata URLs to be redacted back to safe URL, got fetch=%q final=%q", persisted.Metadata.FetchURL, persisted.Metadata.FinalURL)
	}
	if persisted.RawDocument == nil {
		t.Fatal("expected raw document metadata to be written")
	}
	if persisted.RawDocument.URL != safeURL || persisted.RawDocument.FinalURL != safeURL {
		t.Fatalf("expected raw document URLs to be redacted back to safe URL, got url=%q final=%q", persisted.RawDocument.URL, persisted.RawDocument.FinalURL)
	}
	if strings.Contains(persisted.RawDocument.FetchMetadata, "mariner") {
		t.Fatalf("expected persisted metadata to omit query credential, got %s", persisted.RawDocument.FetchMetadata)
	}
}

func ptr[T any](value T) *T {
	return &value
}

func TestBuildSourceRequestURLOpenSkyForcesExtended(t *testing.T) {
	requestURL, err := buildSourceRequestURL(
		"catalog:auto:aviation-airports-drones-and-mobility-opensky-network",
		"https://opensky-network.org/api/states/all",
	)
	if err != nil {
		t.Fatalf("build source request url: %v", err)
	}
	if !strings.Contains(requestURL, "extended=1") {
		t.Fatalf("expected opensky request to include extended=1, got %q", requestURL)
	}
}

func TestBuildSourceRequestURLAISHubDefaultsWSQuery(t *testing.T) {
	requestURL, err := buildSourceRequestURL(
		"catalog:auto:maritime-ocean-and-coastal-sources-aishub",
		"https://data.aishub.net/ws.php",
	)
	if err != nil {
		t.Fatalf("build source request url: %v", err)
	}
	for _, fragment := range []string{
		"format=1",
		"output=json",
		"compress=2",
		"latmin=-90",
		"latmax=90",
		"lonmin=-180",
		"lonmax=180",
		"interval=5",
	} {
		if !strings.Contains(requestURL, fragment) {
			t.Fatalf("expected aishub request to include %q, got %q", fragment, requestURL)
		}
	}
}

func TestBuildSourceRequestURLNoAuthProvidersRemainStable(t *testing.T) {
	tests := []struct {
		name     string
		sourceID string
		inputURL string
	}{
		{
			name:     "airplanes_live",
			sourceID: "catalog:auto:aviation-airports-drones-and-mobility-airplanes-live",
			inputURL: "https://api.airplanes.live/v2/point/40.7128/-74.0060/250",
		},
		{
			name:     "adsb_lol",
			sourceID: "catalog:auto:security-addendum-air-adsblol-api",
			inputURL: "https://api.adsb.lol/v2/mil",
		},
		{
			name:     "openaip",
			sourceID: "catalog:auto:aviation-airports-drones-and-mobility-openaip-core-api",
			inputURL: "https://api.core.openaip.net/api/airports",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			requestURL, err := buildSourceRequestURL(tc.sourceID, tc.inputURL)
			if err != nil {
				t.Fatalf("build source request url: %v", err)
			}
			if requestURL != tc.inputURL {
				t.Fatalf("expected provider URL unchanged, got %q want %q", requestURL, tc.inputURL)
			}
		})
	}
}
