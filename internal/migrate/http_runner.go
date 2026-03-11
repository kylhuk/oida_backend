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

var schemaMigrationColumns = []string{"version", "applied_at", "checksum", "success", "notes"}

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

func (r *HTTPRunner) VerifyMigrationsTableContract(ctx context.Context) error {
	columns, err := r.Query(ctx, "SELECT name FROM system.columns WHERE database = 'meta' AND table = 'schema_migrations' ORDER BY position FORMAT TabSeparated")
	if err != nil {
		return err
	}
	if err := validateSchemaMigrationColumns(parseTabSeparatedRows(columns)); err != nil {
		return fmt.Errorf("meta.schema_migrations is bootstrap-owned: %w", err)
	}
	return nil
}

func (r *HTTPRunner) CheckAppliedMigration(ctx context.Context, version, checksum string) (bool, error) {
	q := fmt.Sprintf("SELECT DISTINCT checksum FROM meta.schema_migrations WHERE version = '%s' AND success = 1 ORDER BY checksum FORMAT TabSeparated", esc(version))
	rows, err := r.Query(ctx, q)
	if err != nil {
		return false, err
	}
	return validateAppliedMigrationChecksum(version, checksum, parseTabSeparatedRows(rows))
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

func parseTabSeparatedRows(out string) []string {
	trimmed := strings.TrimSpace(out)
	if trimmed == "" {
		return nil
	}
	rows := strings.Split(trimmed, "\n")
	clean := make([]string, 0, len(rows))
	for _, row := range rows {
		row = strings.TrimSpace(row)
		if row != "" {
			clean = append(clean, row)
		}
	}
	return clean
}

func validateSchemaMigrationColumns(actual []string) error {
	if len(actual) != len(schemaMigrationColumns) {
		return fmt.Errorf("expected columns %s, got %s", joinColumns(schemaMigrationColumns), joinColumns(actual))
	}
	for i, column := range schemaMigrationColumns {
		if actual[i] != column {
			return fmt.Errorf("expected columns %s, got %s", joinColumns(schemaMigrationColumns), joinColumns(actual))
		}
	}
	return nil
}

func validateAppliedMigrationChecksum(version, checksum string, appliedChecksums []string) (bool, error) {
	if len(appliedChecksums) == 0 {
		return false, nil
	}
	if len(appliedChecksums) > 1 {
		return false, fmt.Errorf("migration %s has conflicting recorded checksums: %s", version, strings.Join(appliedChecksums, ", "))
	}
	if appliedChecksums[0] != checksum {
		return false, fmt.Errorf("migration %s is immutable: recorded checksum %s does not match file checksum %s", version, appliedChecksums[0], checksum)
	}
	return true, nil
}

func joinColumns(columns []string) string {
	if len(columns) == 0 {
		return "(none)"
	}
	return strings.Join(columns, ", ")
}

func esc(s string) string { return strings.ReplaceAll(s, "'", "''") }
