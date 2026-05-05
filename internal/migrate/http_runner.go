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
	user    string
	pass    string
	client  *http.Client
}

var schemaMigrationColumns = []string{"version", "applied_at", "checksum", "success", "notes"}

var schemaChangeRegistryColumns = []string{
	"migration_version",
	"migration_checksum",
	"schema_scope",
	"target_kind",
	"target_name",
	"diff_status",
	"diff_summary",
	"compatibility_status",
	"compatibility_notes",
	"approval_status",
	"approval_notes",
	"approval_ref",
	"approved_by",
	"approved_at",
	"summary",
	"schema_version",
	"record_version",
	"api_contract_version",
	"updated_at",
	"attrs",
	"evidence",
}

func NewHTTPRunner(baseURL string) *HTTPRunner {
	trimmed := strings.TrimRight(baseURL, "/")
	parsed, err := url.Parse(trimmed)
	if err != nil || parsed == nil {
		return &HTTPRunner{baseURL: trimmed, client: &http.Client{}}
	}
	runner := &HTTPRunner{baseURL: parsed.Scheme + "://" + parsed.Host + parsed.Path, client: &http.Client{}}
	if parsed.User != nil {
		runner.user = parsed.User.Username()
		runner.pass, _ = parsed.User.Password()
	}
	if runner.baseURL == "" {
		runner.baseURL = trimmed
	}
	return runner
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
	columns, err := r.TableColumns(ctx, "meta", "schema_migrations")
	if err != nil {
		return err
	}
	if err := validateSchemaMigrationColumns(columns); err != nil {
		return fmt.Errorf("meta.schema_migrations is bootstrap-owned: %w", err)
	}
	return nil
}

func (r *HTTPRunner) VerifySchemaChangeRegistryContract(ctx context.Context) error {
	columns, err := r.TableColumns(ctx, "meta", "schema_change_registry")
	if err != nil {
		return err
	}
	if err := validateSchemaChangeRegistryColumns(columns); err != nil {
		return fmt.Errorf("meta.schema_change_registry is bootstrap-owned: %w", err)
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

func (r *HTTPRunner) HasTable(ctx context.Context, database, table string) (bool, error) {
	q := fmt.Sprintf("SELECT count() FROM system.tables WHERE database = '%s' AND name = '%s' FORMAT TabSeparated", esc(database), esc(table))
	out, err := r.Query(ctx, q)
	if err != nil {
		return false, err
	}
	return strings.TrimSpace(out) != "0", nil
}

type SchemaChangeRecord struct {
	MigrationVersion    string
	MigrationChecksum   string
	SchemaScope         string
	TargetKind          string
	TargetName          string
	DiffStatus          string
	DiffSummary         string
	CompatibilityStatus string
	CompatibilityNotes  string
	ApprovalStatus      string
	ApprovalNotes       string
	ApprovalRef         string
	ApprovedBy          string
	Summary             string
	Attrs               string
	Evidence            string
}

func (r *HTTPRunner) RecordSchemaChange(ctx context.Context, record SchemaChangeRecord) error {
	columns, err := r.TableColumns(ctx, "meta", "schema_change_registry")
	if err != nil {
		return err
	}
	hasColumn := make(map[string]struct{}, len(columns))
	for _, column := range columns {
		hasColumn[column] = struct{}{}
	}
	has := func(column string) bool {
		_, ok := hasColumn[column]
		return ok
	}

	approvedBy := nullableSQLString(record.ApprovedBy)
	approvedAt := "NULL"
	if approvedBy != "NULL" {
		approvedAt = "now64(3)"
	}
	columnNames := []string{
		"migration_version",
		"migration_checksum",
		"schema_scope",
		"target_kind",
		"target_name",
		"diff_status",
	}
	values := []string{
		fmt.Sprintf("'%s'", esc(record.MigrationVersion)),
		fmt.Sprintf("'%s'", esc(record.MigrationChecksum)),
		fmt.Sprintf("'%s'", esc(record.SchemaScope)),
		fmt.Sprintf("'%s'", esc(record.TargetKind)),
		fmt.Sprintf("'%s'", esc(record.TargetName)),
		fmt.Sprintf("'%s'", esc(record.DiffStatus)),
	}
	if has("diff_summary") {
		columnNames = append(columnNames, "diff_summary")
		values = append(values, fmt.Sprintf("'%s'", esc(record.DiffSummary)))
	}
	columnNames = append(columnNames, "compatibility_status")
	values = append(values, fmt.Sprintf("'%s'", esc(record.CompatibilityStatus)))
	if has("compatibility_notes") {
		columnNames = append(columnNames, "compatibility_notes")
		values = append(values, fmt.Sprintf("'%s'", esc(record.CompatibilityNotes)))
	}
	columnNames = append(columnNames, "approval_status", "approval_notes")
	values = append(values, fmt.Sprintf("'%s'", esc(record.ApprovalStatus)), fmt.Sprintf("'%s'", esc(record.ApprovalNotes)))
	if has("approval_ref") {
		columnNames = append(columnNames, "approval_ref")
		values = append(values, nullableSQLString(record.ApprovalRef))
	}
	columnNames = append(columnNames,
		"approved_by",
		"approved_at",
		"summary",
		"schema_version",
		"record_version",
		"api_contract_version",
		"updated_at",
		"attrs",
		"evidence",
	)
	values = append(values,
		approvedBy,
		approvedAt,
		fmt.Sprintf("'%s'", esc(record.Summary)),
		"1",
		"toUInt64(toUnixTimestamp64Milli(now64(3)))",
		"1",
		"now64(3)",
		fmt.Sprintf("'%s'", esc(record.Attrs)),
		fmt.Sprintf("'%s'", esc(record.Evidence)),
	)
	q := fmt.Sprintf("INSERT INTO meta.schema_change_registry (%s) VALUES (%s)", strings.Join(columnNames, ", "), strings.Join(values, ", "))
	return r.ApplySQL(ctx, q)
}

func (r *HTTPRunner) TableColumns(ctx context.Context, database, table string) ([]string, error) {
	q := fmt.Sprintf("SELECT name FROM system.columns WHERE database = '%s' AND table = '%s' ORDER BY position FORMAT TabSeparated", esc(database), esc(table))
	out, err := r.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	return parseTabSeparatedRows(out), nil
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
	if r.user != "" {
		req.SetBasicAuth(r.user, r.pass)
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

func validateSchemaChangeRegistryColumns(actual []string) error {
	if len(actual) != len(schemaChangeRegistryColumns) {
		return fmt.Errorf("expected columns %s, got %s", joinColumns(schemaChangeRegistryColumns), joinColumns(actual))
	}
	for i, column := range schemaChangeRegistryColumns {
		if actual[i] != column {
			return fmt.Errorf("expected columns %s, got %s", joinColumns(schemaChangeRegistryColumns), joinColumns(actual))
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

func nullableSQLString(s string) string {
	if strings.TrimSpace(s) == "" {
		return "NULL"
	}
	return fmt.Sprintf("'%s'", esc(s))
}
