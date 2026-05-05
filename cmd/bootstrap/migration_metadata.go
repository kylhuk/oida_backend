package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"global-osint-backend/internal/migrate"
)

const schemaChangeRegistryTable = "schema_change_registry"

type migrationMetadata struct {
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
}

func defaultMigrationMetadata() migrationMetadata {
	return migrationMetadata{
		SchemaScope:         "general",
		TargetKind:          "migration",
		TargetName:          "",
		DiffStatus:          "unspecified",
		DiffSummary:         "",
		CompatibilityStatus: "unknown",
		CompatibilityNotes:  "",
		ApprovalStatus:      "pending",
		ApprovalNotes:       "",
		ApprovalRef:         "",
		ApprovedBy:          "",
		Summary:             "",
	}
}

func parseMigrationMetadata(contents []byte) migrationMetadata {
	metadata := defaultMigrationMetadata()
	for _, rawLine := range strings.Split(string(contents), "\n") {
		line := strings.TrimSpace(rawLine)
		if line == "" {
			continue
		}
		if !strings.HasPrefix(line, "--") {
			break
		}
		body := strings.TrimSpace(strings.TrimPrefix(line, "--"))
		if !strings.HasPrefix(body, "migrate:") {
			continue
		}
		directive := strings.TrimSpace(strings.TrimPrefix(body, "migrate:"))
		parts := strings.Fields(directive)
		if len(parts) < 2 {
			continue
		}
		key := strings.ToLower(parts[0])
		value := strings.TrimSpace(directive[len(parts[0]):])
		if value == "" {
			continue
		}
		switch key {
		case "scope":
			metadata.SchemaScope = value
		case "target_kind":
			metadata.TargetKind = value
		case "target_name":
			metadata.TargetName = value
		case "diff":
			metadata.DiffStatus = value
		case "diff_summary":
			metadata.DiffSummary = value
		case "compatibility":
			metadata.CompatibilityStatus = value
		case "compatibility_notes":
			metadata.CompatibilityNotes = value
		case "approval":
			metadata.ApprovalStatus = value
		case "approval_notes":
			metadata.ApprovalNotes = value
		case "approval_ref":
			metadata.ApprovalRef = value
		case "approved_by":
			metadata.ApprovedBy = value
		case "summary":
			metadata.Summary = value
		}
	}
	return metadata
}

func recordSchemaChangeMetadata(ctx context.Context, runner *migrate.HTTPRunner, version, checksum string, metadata migrationMetadata) error {
	hasTable, err := runner.HasTable(ctx, "meta", schemaChangeRegistryTable)
	if err != nil {
		return fmt.Errorf("check schema change registry: %w", err)
	}
	if !hasTable {
		return nil
	}

	attrs, err := json.Marshal(map[string]any{
		"migration_version":   version,
		"migration_checksum":  checksum,
		"diff_summary":        metadata.DiffSummary,
		"compatibility_notes": metadata.CompatibilityNotes,
		"summary":             metadata.Summary,
		"approval_notes":      metadata.ApprovalNotes,
		"approval_ref":        metadata.ApprovalRef,
	})
	if err != nil {
		return fmt.Errorf("marshal schema change attrs: %w", err)
	}
	evidence, err := json.Marshal([]string{
		"migration:" + version,
		"checksum:" + checksum,
	})
	if err != nil {
		return fmt.Errorf("marshal schema change evidence: %w", err)
	}
	if metadata.ApprovalRef != "" {
		evidence, err = json.Marshal([]string{
			"migration:" + version,
			"checksum:" + checksum,
			"approval_ref:" + metadata.ApprovalRef,
		})
		if err != nil {
			return fmt.Errorf("marshal schema change evidence: %w", err)
		}
	}

	return runner.RecordSchemaChange(ctx, migrate.SchemaChangeRecord{
		MigrationVersion:    version,
		MigrationChecksum:   checksum,
		SchemaScope:         metadata.SchemaScope,
		TargetKind:          metadata.TargetKind,
		TargetName:          metadata.TargetName,
		DiffStatus:          metadata.DiffStatus,
		DiffSummary:         metadata.DiffSummary,
		CompatibilityStatus: metadata.CompatibilityStatus,
		CompatibilityNotes:  metadata.CompatibilityNotes,
		ApprovalStatus:      metadata.ApprovalStatus,
		ApprovalNotes:       metadata.ApprovalNotes,
		ApprovalRef:         metadata.ApprovalRef,
		ApprovedBy:          metadata.ApprovedBy,
		Summary:             metadata.Summary,
		Attrs:               string(attrs),
		Evidence:            string(evidence),
	})
}
