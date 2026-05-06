package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadAPIClientsSeedsHashedKeysOnly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "api_clients.json")
	payload := `[
  {
    "key_id": "frontend",
    "name": "Frontend BFF",
    "key_sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "scopes": ["read:*", "read:internal"],
    "enabled": true,
    "expires_at": null,
    "attrs": {"owner":"platform"}
  }
]`
	if err := os.WriteFile(path, []byte(payload), 0o644); err != nil {
		t.Fatalf("write seed: %v", err)
	}
	runner := &stubSourceRegistryStore{}
	if err := loadAPIClients(context.Background(), runner, path); err != nil {
		t.Fatalf("load api clients: %v", err)
	}
	if len(runner.appliedSQL) != 1 {
		t.Fatalf("expected one insert, got %d", len(runner.appliedSQL))
	}
	insert := runner.appliedSQL[0]
	for _, want := range []string{"INSERT INTO meta.api_clients", "frontend", "Frontend BFF", "read:*", "read:internal"} {
		if !strings.Contains(insert, want) {
			t.Fatalf("expected insert to contain %q, got %s", want, insert)
		}
	}
	if strings.Contains(insert, "oida_") || strings.Contains(insert, "secret") {
		t.Fatalf("api client seed insert must not include raw keys: %s", insert)
	}
}

func TestLoadAPIClientsRejectsInvalidHash(t *testing.T) {
	path := filepath.Join(t.TempDir(), "api_clients.json")
	payload := `[{"key_id":"bad","name":"Bad","key_sha256":"not-a-hash","scopes":["read:*"],"enabled":true}]`
	if err := os.WriteFile(path, []byte(payload), 0o644); err != nil {
		t.Fatalf("write seed: %v", err)
	}
	if err := loadAPIClients(context.Background(), &stubSourceRegistryStore{}, path); err == nil || !strings.Contains(err.Error(), "key_sha256") {
		t.Fatalf("expected key_sha256 validation error, got %v", err)
	}
}
