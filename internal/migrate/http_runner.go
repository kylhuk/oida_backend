package migrate

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

type HTTPRunner struct {
	baseURL string
	client  *http.Client
}

func NewHTTPRunner(baseURL string) *HTTPRunner {
	return &HTTPRunner{baseURL: strings.TrimRight(baseURL, "/"), client: &http.Client{}}
}

func (r *HTTPRunner) EnsureMigrationsTable(ctx context.Context) error {
	return r.ApplySQL(ctx, `
CREATE DATABASE IF NOT EXISTS meta;
CREATE TABLE IF NOT EXISTS meta.schema_migrations
(
  version String,
  applied_at DateTime DEFAULT now(),
  checksum String,
  success UInt8,
  notes String
)
ENGINE = MergeTree
ORDER BY (version, applied_at)
`)
}

func (r *HTTPRunner) IsApplied(ctx context.Context, version, checksum string) (bool, error) {
	q := fmt.Sprintf("SELECT count() FROM meta.schema_migrations WHERE version = '%s' AND checksum = '%s' AND success = 1 FORMAT TabSeparated", esc(version), esc(checksum))
	out, err := r.Query(ctx, q)
	if err != nil {
		return false, err
	}
	return strings.TrimSpace(out) != "0", nil
}

func (r *HTTPRunner) Record(ctx context.Context, version, checksum string, success bool, notes string) error {
	s := 0
	if success {
		s = 1
	}
	q := fmt.Sprintf("INSERT INTO meta.schema_migrations (version, checksum, success, notes) VALUES ('%s','%s',%d,'%s')", esc(version), esc(checksum), s, esc(notes))
	return r.ApplySQL(ctx, q)
}

func (r *HTTPRunner) ApplySQL(ctx context.Context, sql string) error {
	for _, stmt := range SplitStatements(sql) {
		if strings.TrimSpace(stmt) == "" {
			continue
		}
		if _, err := r.Query(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

func (r *HTTPRunner) Query(ctx context.Context, q string) (string, error) {
	u := r.baseURL + "/?query=" + url.QueryEscape(q)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, nil)
	if err != nil {
		return "", err
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return "", fmt.Errorf("clickhouse http %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	return string(b), nil
}

func esc(s string) string { return strings.ReplaceAll(s, "'", "''") }
