package migrate

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestValidateSchemaMigrationColumnsAcceptsBootstrapContract(t *testing.T) {
	if err := validateSchemaMigrationColumns([]string{"version", "applied_at", "checksum", "success", "notes"}); err != nil {
		t.Fatalf("expected bootstrap-owned ledger contract to pass: %v", err)
	}
}

func TestValidateSchemaMigrationColumnsRejectsStaleIDContract(t *testing.T) {
	err := validateSchemaMigrationColumns([]string{"id", "version", "applied_at", "checksum", "success", "notes"})
	if err == nil {
		t.Fatal("expected stale id-based ledger contract to fail")
	}
}

func TestValidateSchemaChangeRegistryColumnsAcceptsPlanningContract(t *testing.T) {
	if err := validateSchemaChangeRegistryColumns([]string{
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
	}); err != nil {
		t.Fatalf("expected schema change registry contract to pass: %v", err)
	}
}

func TestValidateSchemaChangeRegistryColumnsRejectsMissingPlanningColumns(t *testing.T) {
	err := validateSchemaChangeRegistryColumns([]string{
		"migration_version",
		"migration_checksum",
		"schema_scope",
		"target_kind",
		"target_name",
		"diff_status",
		"compatibility_status",
		"approval_status",
		"approval_notes",
		"approved_by",
		"approved_at",
		"summary",
		"schema_version",
		"record_version",
		"api_contract_version",
		"updated_at",
		"attrs",
		"evidence",
	})
	if err == nil {
		t.Fatal("expected missing planning columns to fail")
	}
}

func TestValidateAppliedMigrationChecksumAllowsUnappliedMigration(t *testing.T) {
	applied, err := validateAppliedMigrationChecksum("0001_init.sql", "abc123", nil)
	if err != nil {
		t.Fatalf("expected unapplied migration to pass: %v", err)
	}
	if applied {
		t.Fatal("expected unapplied migration to report not applied")
	}
}

func TestValidateAppliedMigrationChecksumAllowsMatchingChecksum(t *testing.T) {
	applied, err := validateAppliedMigrationChecksum("0001_init.sql", "abc123", []string{"abc123"})
	if err != nil {
		t.Fatalf("expected matching checksum to pass: %v", err)
	}
	if !applied {
		t.Fatal("expected matching checksum to report already applied")
	}
}

func TestValidateAppliedMigrationChecksumRejectsEditedMigration(t *testing.T) {
	_, err := validateAppliedMigrationChecksum("0001_init.sql", "new456", []string{"abc123"})
	if err == nil {
		t.Fatal("expected edited applied migration to fail")
	}
	if got := err.Error(); got != "migration 0001_init.sql is immutable: recorded checksum abc123 does not match file checksum new456" {
		t.Fatalf("unexpected immutability error %q", got)
	}
}

func TestValidateAppliedMigrationChecksumRejectsLedgerConflicts(t *testing.T) {
	_, err := validateAppliedMigrationChecksum("0001_init.sql", "abc123", []string{"abc123", "def789"})
	if err == nil {
		t.Fatal("expected conflicting ledger checksums to fail")
	}
	if got := err.Error(); got != "migration 0001_init.sql has conflicting recorded checksums: abc123, def789" {
		t.Fatalf("unexpected conflict error %q", got)
	}
}

func TestRecordSchemaChangeOmitsPlanningColumnsUntilMigrationAddsThem(t *testing.T) {
	query := captureSchemaChangeInsert(t, []string{
		"migration_version",
		"migration_checksum",
		"schema_scope",
		"target_kind",
		"target_name",
		"diff_status",
		"compatibility_status",
		"approval_status",
		"approval_notes",
		"approved_by",
		"approved_at",
		"summary",
		"schema_version",
		"record_version",
		"api_contract_version",
		"updated_at",
		"attrs",
		"evidence",
	})
	if strings.Contains(query, "diff_summary") {
		t.Fatalf("expected legacy registry insert to omit diff_summary, got %q", query)
	}
	if strings.Contains(query, "compatibility_notes") {
		t.Fatalf("expected legacy registry insert to omit compatibility_notes, got %q", query)
	}
	if strings.Contains(query, "approval_ref") {
		t.Fatalf("expected legacy registry insert to omit approval_ref, got %q", query)
	}
}

func TestRecordSchemaChangeIncludesPlanningColumnsAfterRegistryUpgrade(t *testing.T) {
	query := captureSchemaChangeInsert(t, schemaChangeRegistryColumns)
	for _, fragment := range []string{"diff_summary", "compatibility_notes", "approval_ref", "jira://schema-review/TASK-1"} {
		if !strings.Contains(query, fragment) {
			t.Fatalf("expected upgraded registry insert to include %q, got %q", fragment, query)
		}
	}
}

func captureSchemaChangeInsert(t *testing.T, columns []string) string {
	t.Helper()
	var insertQuery string
	var handlerErr error
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q, err := url.QueryUnescape(r.URL.Query().Get("query"))
		if err != nil {
			handlerErr = err
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		switch {
		case strings.Contains(q, "FROM system.columns"):
			_, _ = io.WriteString(w, strings.Join(columns, "\n"))
		case strings.Contains(q, "INSERT INTO meta.schema_change_registry"):
			insertQuery = q
			_, _ = io.WriteString(w, "OK")
		default:
			handlerErr = errors.New(q)
			http.Error(w, "unexpected query", http.StatusBadRequest)
		}
	}))
	defer server.Close()

	runner := NewHTTPRunner(server.URL)
	err := runner.RecordSchemaChange(context.Background(), SchemaChangeRecord{
		MigrationVersion:    "0029_schema_change_planning.sql",
		MigrationChecksum:   "abc123",
		SchemaScope:         "metadata",
		TargetKind:          "table",
		TargetName:          "meta.schema_change_registry",
		DiffStatus:          "additive",
		DiffSummary:         "Add planning detail columns",
		CompatibilityStatus: "backward_compatible",
		CompatibilityNotes:  "Existing rows keep their rollout status.",
		ApprovalStatus:      "approved",
		ApprovalNotes:       "Captured in migration metadata",
		ApprovalRef:         "jira://schema-review/TASK-1",
		ApprovedBy:          "bootstrap",
		Summary:             "Persist queryable schema planning details",
		Attrs:               `{}`,
		Evidence:            `[]`,
	})
	if err != nil {
		t.Fatalf("record schema change: %v", err)
	}
	if handlerErr != nil {
		t.Fatalf("unexpected query during schema change insert capture: %v", handlerErr)
	}
	if insertQuery == "" {
		t.Fatal("expected insert query to be captured")
	}
	return insertQuery
}
