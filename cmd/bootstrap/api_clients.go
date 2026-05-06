package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"
)

const apiClientSchemaVersion = 1

var apiClientHashPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)

type apiClientSeed struct {
	KeyID     string         `json:"key_id"`
	Name      string         `json:"name"`
	KeySHA256 string         `json:"key_sha256"`
	Scopes    []string       `json:"scopes"`
	Enabled   bool           `json:"enabled"`
	ExpiresAt *string        `json:"expires_at"`
	Attrs     map[string]any `json:"attrs"`
}

func loadAPIClients(ctx context.Context, store sourceRegistryStore, path string) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return fmt.Errorf("api client seed path is required")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read api client seed: %w", err)
	}
	var raw []map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("decode api client seed: %w", err)
	}
	for _, record := range raw {
		for _, forbidden := range []string{"raw_key", "api_key", "secret", "token"} {
			if _, ok := record[forbidden]; ok {
				return fmt.Errorf("api client %v contains forbidden raw secret field %q", record["key_id"], forbidden)
			}
		}
	}
	var seeds []apiClientSeed
	if err := json.Unmarshal(data, &seeds); err != nil {
		return fmt.Errorf("decode api client seed records: %w", err)
	}
	if len(seeds) == 0 {
		return fmt.Errorf("api client seed must contain at least one client")
	}
	values := make([]string, 0, len(seeds))
	for _, seed := range seeds {
		normalized, err := normalizeAPIClientSeed(seed)
		if err != nil {
			return err
		}
		values = append(values, apiClientInsertTuple(normalized))
	}
	sql := `INSERT INTO meta.api_clients
(key_id, name, key_sha256, scopes, enabled, expires_at, schema_version, record_version, api_contract_version, updated_at, attrs, evidence)
VALUES ` + strings.Join(values, ",")
	return store.ApplySQL(ctx, sql)
}

func normalizeAPIClientSeed(seed apiClientSeed) (apiClientSeed, error) {
	seed.KeyID = strings.TrimSpace(seed.KeyID)
	seed.Name = strings.TrimSpace(seed.Name)
	seed.KeySHA256 = strings.ToLower(strings.TrimSpace(seed.KeySHA256))
	if seed.KeyID == "" {
		return apiClientSeed{}, fmt.Errorf("api client key_id is required")
	}
	if seed.Name == "" {
		return apiClientSeed{}, fmt.Errorf("api client %s name is required", seed.KeyID)
	}
	if !apiClientHashPattern.MatchString(seed.KeySHA256) {
		return apiClientSeed{}, fmt.Errorf("api client %s key_sha256 must be 64 lowercase hex characters", seed.KeyID)
	}
	seenScopes := map[string]struct{}{}
	scopes := make([]string, 0, len(seed.Scopes))
	for _, scope := range seed.Scopes {
		scope = strings.TrimSpace(scope)
		if scope == "" {
			continue
		}
		if _, ok := seenScopes[scope]; ok {
			continue
		}
		seenScopes[scope] = struct{}{}
		scopes = append(scopes, scope)
	}
	if len(scopes) == 0 {
		return apiClientSeed{}, fmt.Errorf("api client %s must include at least one scope", seed.KeyID)
	}
	if seed.ExpiresAt != nil && strings.TrimSpace(*seed.ExpiresAt) != "" {
		if _, err := time.Parse(time.RFC3339, strings.TrimSpace(*seed.ExpiresAt)); err != nil {
			return apiClientSeed{}, fmt.Errorf("api client %s expires_at must be RFC3339: %w", seed.KeyID, err)
		}
	}
	seed.Scopes = scopes
	if seed.Attrs == nil {
		seed.Attrs = map[string]any{}
	}
	return seed, nil
}

func apiClientInsertTuple(seed apiClientSeed) string {
	attrs, _ := json.Marshal(seed.Attrs)
	expiresAt := "NULL"
	if seed.ExpiresAt != nil && strings.TrimSpace(*seed.ExpiresAt) != "" {
		expiresAt = fmt.Sprintf("toDateTime64('%s', 3, 'UTC')", esc(*seed.ExpiresAt))
	}
	return fmt.Sprintf("('%s','%s','%s',%s,%d,%s,%d,1,1,now64(3),'%s','[]')",
		esc(seed.KeyID),
		esc(seed.Name),
		esc(seed.KeySHA256),
		arr(seed.Scopes),
		btoi(seed.Enabled),
		expiresAt,
		apiClientSchemaVersion,
		esc(string(attrs)),
	)
}
