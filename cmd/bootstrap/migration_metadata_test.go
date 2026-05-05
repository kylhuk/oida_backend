package main

import "testing"

func TestParseMigrationMetadataReadsStructuredHeader(t *testing.T) {
	contents := []byte(`-- migrate:scope metadata
-- migrate:target_kind table
-- migrate:target_name meta.schema_change_registry
-- migrate:diff additive
-- migrate:diff_summary Add queryable planning detail columns to the schema registry.
-- migrate:compatibility backward_compatible
-- migrate:compatibility_notes Existing registry rows keep their status while new detail columns backfill safely.
-- migrate:approval auto_approved
-- migrate:approval_notes Reviewed during bootstrap rollout
-- migrate:approval_ref roadmap/task-1
-- migrate:approved_by bootstrap
-- migrate:summary Establish the metadata schema change registry

CREATE TABLE IF NOT EXISTS meta.schema_change_registry (id String) ENGINE = MergeTree ORDER BY (id);
`)

	metadata := parseMigrationMetadata(contents)
	if metadata.SchemaScope != "metadata" {
		t.Fatalf("expected scope metadata, got %q", metadata.SchemaScope)
	}
	if metadata.TargetKind != "table" {
		t.Fatalf("expected target_kind table, got %q", metadata.TargetKind)
	}
	if metadata.TargetName != "meta.schema_change_registry" {
		t.Fatalf("expected target_name meta.schema_change_registry, got %q", metadata.TargetName)
	}
	if metadata.DiffStatus != "additive" {
		t.Fatalf("expected diff additive, got %q", metadata.DiffStatus)
	}
	if metadata.DiffSummary != "Add queryable planning detail columns to the schema registry." {
		t.Fatalf("expected diff_summary to be preserved, got %q", metadata.DiffSummary)
	}
	if metadata.CompatibilityStatus != "backward_compatible" {
		t.Fatalf("expected compatibility backward_compatible, got %q", metadata.CompatibilityStatus)
	}
	if metadata.CompatibilityNotes != "Existing registry rows keep their status while new detail columns backfill safely." {
		t.Fatalf("expected compatibility_notes to be preserved, got %q", metadata.CompatibilityNotes)
	}
	if metadata.ApprovalStatus != "auto_approved" {
		t.Fatalf("expected approval auto_approved, got %q", metadata.ApprovalStatus)
	}
	if metadata.ApprovalNotes != "Reviewed during bootstrap rollout" {
		t.Fatalf("expected approval_notes to be preserved, got %q", metadata.ApprovalNotes)
	}
	if metadata.ApprovalRef != "roadmap/task-1" {
		t.Fatalf("expected approval_ref roadmap/task-1, got %q", metadata.ApprovalRef)
	}
	if metadata.ApprovedBy != "bootstrap" {
		t.Fatalf("expected approved_by bootstrap, got %q", metadata.ApprovedBy)
	}
	if metadata.Summary != "Establish the metadata schema change registry" {
		t.Fatalf("expected summary to be preserved, got %q", metadata.Summary)
	}
}

func TestParseMigrationMetadataFallsBackToPendingDefaults(t *testing.T) {
	metadata := parseMigrationMetadata([]byte("CREATE TABLE foo (id String) ENGINE = MergeTree ORDER BY (id);"))
	if metadata.SchemaScope != "general" {
		t.Fatalf("expected default scope general, got %q", metadata.SchemaScope)
	}
	if metadata.TargetKind != "migration" {
		t.Fatalf("expected default target_kind migration, got %q", metadata.TargetKind)
	}
	if metadata.DiffStatus != "unspecified" {
		t.Fatalf("expected default diff unspecified, got %q", metadata.DiffStatus)
	}
	if metadata.CompatibilityStatus != "unknown" {
		t.Fatalf("expected default compatibility unknown, got %q", metadata.CompatibilityStatus)
	}
	if metadata.ApprovalStatus != "pending" {
		t.Fatalf("expected default approval pending, got %q", metadata.ApprovalStatus)
	}
}
