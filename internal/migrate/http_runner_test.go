package migrate

import "testing"

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
