package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

var (
	errAPIKeyUnauthorized = errors.New("api key unauthorized")
	errAPIKeyForbidden    = errors.New("api key forbidden")
)

type apiKeyIdentity struct {
	KeyID     string
	Name      string
	Scopes    []string
	Enabled   bool
	ExpiresAt *time.Time
}

func (i apiKeyIdentity) HasScopes(required []string) bool {
	if len(required) == 0 {
		return true
	}
	owned := make(map[string]struct{}, len(i.Scopes))
	for _, scope := range i.Scopes {
		owned[strings.TrimSpace(scope)] = struct{}{}
	}
	if _, ok := owned["admin:*"]; ok {
		return true
	}
	for _, requiredScope := range required {
		requiredScope = strings.TrimSpace(requiredScope)
		if requiredScope == "" {
			continue
		}
		if _, ok := owned[requiredScope]; ok {
			continue
		}
		return false
	}
	return true
}

type apiKeyAuthenticator interface {
	AuthenticateAPIKey(ctx context.Context, rawKey string, requiredScopes []string) (apiKeyIdentity, error)
}

type denyAPIKeyAuthenticator struct{}

func (denyAPIKeyAuthenticator) AuthenticateAPIKey(context.Context, string, []string) (apiKeyIdentity, error) {
	return apiKeyIdentity{}, errAPIKeyUnauthorized
}

type clickhouseAPIKeyAuthenticator struct {
	clickhouse clickhouseQuerier
	timeout    time.Duration
}

func (a clickhouseAPIKeyAuthenticator) AuthenticateAPIKey(ctx context.Context, rawKey string, requiredScopes []string) (apiKeyIdentity, error) {
	rawKey = strings.TrimSpace(rawKey)
	if rawKey == "" || !strings.HasPrefix(rawKey, "oida_") {
		return apiKeyIdentity{}, errAPIKeyUnauthorized
	}
	if a.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, a.timeout)
		defer cancel()
	}
	query := fmt.Sprintf(`SELECT key_id, name, scopes, enabled, expires_at
FROM meta.api_clients FINAL
WHERE key_sha256 = '%s' AND enabled = 1
ORDER BY updated_at DESC
LIMIT 1
FORMAT JSONEachRow`, escSQL(sha256Hex(rawKey)))
	out, err := a.clickhouse.Query(ctx, query)
	if err != nil {
		return apiKeyIdentity{}, errAPIKeyUnauthorized
	}
	line := strings.TrimSpace(out)
	if line == "" {
		return apiKeyIdentity{}, errAPIKeyUnauthorized
	}
	identity, err := decodeAPIKeyIdentity(line)
	if err != nil || !identity.Enabled {
		return apiKeyIdentity{}, errAPIKeyUnauthorized
	}
	if identity.ExpiresAt != nil && time.Now().UTC().After(*identity.ExpiresAt) {
		return apiKeyIdentity{}, errAPIKeyUnauthorized
	}
	if !identity.HasScopes(requiredScopes) {
		return apiKeyIdentity{}, errAPIKeyForbidden
	}
	return identity, nil
}

func sha256Hex(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func decodeAPIKeyIdentity(line string) (apiKeyIdentity, error) {
	var raw struct {
		KeyID     string          `json:"key_id"`
		Name      string          `json:"name"`
		Scopes    json.RawMessage `json:"scopes"`
		Enabled   any             `json:"enabled"`
		ExpiresAt *string         `json:"expires_at"`
	}
	if err := json.Unmarshal([]byte(line), &raw); err != nil {
		return apiKeyIdentity{}, err
	}
	scopes, err := decodeStringArray(raw.Scopes)
	if err != nil {
		return apiKeyIdentity{}, err
	}
	identity := apiKeyIdentity{
		KeyID:   raw.KeyID,
		Name:    raw.Name,
		Scopes:  scopes,
		Enabled: decodeBoolish(raw.Enabled),
	}
	if raw.ExpiresAt != nil && strings.TrimSpace(*raw.ExpiresAt) != "" {
		parsed, err := parseClickHouseTime(strings.TrimSpace(*raw.ExpiresAt))
		if err != nil {
			return apiKeyIdentity{}, err
		}
		parsed = parsed.UTC()
		identity.ExpiresAt = &parsed
	}
	return identity, nil
}

func parseClickHouseTime(value string) (time.Time, error) {
	for _, layout := range []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05",
	} {
		if parsed, err := time.Parse(layout, value); err == nil {
			return parsed.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("unsupported timestamp %q", value)
}

func decodeStringArray(raw json.RawMessage) ([]string, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	var values []string
	if err := json.Unmarshal(raw, &values); err == nil {
		return values, nil
	}
	var encoded string
	if err := json.Unmarshal(raw, &encoded); err != nil {
		return nil, err
	}
	if strings.TrimSpace(encoded) == "" {
		return nil, nil
	}
	if err := json.Unmarshal([]byte(encoded), &values); err != nil {
		return nil, err
	}
	return values, nil
}

func decodeBoolish(value any) bool {
	switch v := value.(type) {
	case bool:
		return v
	case float64:
		return v != 0
	case string:
		return v == "1" || strings.EqualFold(v, "true")
	default:
		return false
	}
}

func escSQL(value string) string {
	return strings.ReplaceAll(value, "'", "''")
}
