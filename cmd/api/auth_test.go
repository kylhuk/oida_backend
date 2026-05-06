package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const testAPIKey = "oida_test_secret"

type testAPIKeyAuthenticator struct {
	identities map[string]apiKeyIdentity
}

func (a testAPIKeyAuthenticator) AuthenticateAPIKey(_ context.Context, rawKey string, requiredScopes []string) (apiKeyIdentity, error) {
	identity, ok := a.identities[rawKey]
	if !ok || !identity.Enabled {
		return apiKeyIdentity{}, errAPIKeyUnauthorized
	}
	if !identity.HasScopes(requiredScopes) {
		return apiKeyIdentity{}, errAPIKeyForbidden
	}
	return identity, nil
}

func serverWithTestAuth(server *apiServer) *apiServer {
	server.authenticator = testAPIKeyAuthenticator{identities: map[string]apiKeyIdentity{
		testAPIKey: {
			KeyID:   "test",
			Name:    "Test API client",
			Scopes:  []string{"read:*", "read:internal", "admin:*"},
			Enabled: true,
		},
	}}
	return server
}

func TestAPIKeyAuthUsesScopedAuthenticator(t *testing.T) {
	server := serverWithTestAuth(&apiServer{
		version: "v1",
		clickhouse: stubQuerier{queryFn: func(_ context.Context, _ string) (string, error) {
			return `{"metric_id":"obs_count","metric_family":"activity","subject_grain":"place","unit":"count","value_type":"count","rollup_engine":"snapshot","rollup_rule":"sum","enabled":1,"updated_at":"2026-03-10T08:30:00Z","attrs":"{}","evidence":"[]"}` + "\n", nil
		}},
		queryTimeout: time.Second,
	})
	mux := newAPIMuxWithServer("v1", "", server)

	t.Run("protected route rejects missing key", func(t *testing.T) {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v1/metrics", nil)
		mux.ServeHTTP(rr, req)
		if rr.Code != http.StatusUnauthorized {
			t.Fatalf("expected 401 got %d body=%s", rr.Code, rr.Body.String())
		}
	})

	t.Run("protected route rejects invalid key", func(t *testing.T) {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v1/metrics", nil)
		req.Header.Set(apiKeyHeader, "oida_unknown_secret")
		mux.ServeHTTP(rr, req)
		if rr.Code != http.StatusUnauthorized {
			t.Fatalf("expected 401 got %d body=%s", rr.Code, rr.Body.String())
		}
	})

	t.Run("protected route accepts key with read scope", func(t *testing.T) {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v1/metrics", nil)
		req.Header.Set(apiKeyHeader, testAPIKey)
		mux.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("expected 200 got %d body=%s", rr.Code, rr.Body.String())
		}
	})
}

func TestAPIKeyAuthRejectsMissingInternalScope(t *testing.T) {
	server := &apiServer{
		version:      "v1",
		queryTimeout: time.Second,
		clickhouse:   statsStubQuerier{queryFn: stubStatsQueries},
		authenticator: testAPIKeyAuthenticator{identities: map[string]apiKeyIdentity{
			testAPIKey: {
				KeyID:   "test",
				Name:    "Test read-only API client",
				Scopes:  []string{"read:*"},
				Enabled: true,
			},
		}},
	}
	mux := newAPIMuxWithServer("v1", "", server)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/internal/stats", nil)
	req.Header.Set(apiKeyHeader, testAPIKey)
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("expected 403 got %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestAPIKeyHashAuthenticatorQueriesClickHouse(t *testing.T) {
	rawKey := "oida_client_secret"
	hash := sha256Hex(rawKey)
	authenticator := clickhouseAPIKeyAuthenticator{
		clickhouse: stubQuerier{queryFn: func(_ context.Context, query string) (string, error) {
			if !strings.Contains(query, hash) || !strings.Contains(query, "FROM meta.api_clients FINAL") {
				t.Fatalf("query did not filter by hashed key: %s", query)
			}
			return `{"key_id":"client","name":"Client","scopes":["read:*"],"enabled":1,"expires_at":null}` + "\n", nil
		}},
		timeout: time.Second,
	}
	identity, err := authenticator.AuthenticateAPIKey(context.Background(), rawKey, []string{"read:*"})
	if err != nil {
		t.Fatalf("authenticate: %v", err)
	}
	if identity.KeyID != "client" || !identity.HasScopes([]string{"read:*"}) {
		t.Fatalf("unexpected identity %#v", identity)
	}
}

func TestAPIKeyHashAuthenticatorParsesClickHouseDateTime64Expiry(t *testing.T) {
	authenticator := clickhouseAPIKeyAuthenticator{
		clickhouse: stubQuerier{queryFn: func(_ context.Context, _ string) (string, error) {
			return `{"key_id":"client","name":"Client","scopes":["read:*"],"enabled":1,"expires_at":"2099-01-02 03:04:05.678"}` + "\n", nil
		}},
		timeout: time.Second,
	}
	identity, err := authenticator.AuthenticateAPIKey(context.Background(), "oida_client_secret", []string{"read:*"})
	if err != nil {
		t.Fatalf("authenticate with ClickHouse DateTime64 expires_at: %v", err)
	}
	if identity.ExpiresAt == nil || identity.ExpiresAt.Year() != 2099 {
		t.Fatalf("unexpected expires_at %#v", identity.ExpiresAt)
	}
}

func TestAPIKeyHashAuthenticatorMapsQueryFailure(t *testing.T) {
	authenticator := clickhouseAPIKeyAuthenticator{
		clickhouse: stubQuerier{queryFn: func(_ context.Context, _ string) (string, error) {
			return "", errors.New("clickhouse unavailable")
		}},
		timeout: time.Second,
	}
	if _, err := authenticator.AuthenticateAPIKey(context.Background(), "oida_client_secret", []string{"read:*"}); !errors.Is(err, errAPIKeyUnauthorized) {
		t.Fatalf("expected unauthorized on query failure, got %v", err)
	}
}
