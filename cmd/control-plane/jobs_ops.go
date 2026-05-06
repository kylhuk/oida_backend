package main

import (
	"context"
	"fmt"
	"net/url"
	"path"
	"strings"
	"time"
	"unicode"

	"global-osint-backend/internal/migrate"
)

const (
	backupClickHouseJobName     = "backup-clickhouse"
	restoreClickHouseJobName    = "restore-clickhouse"
	retentionMaterializeJobName = "retention-materialize"
	defaultClickHouseBackupPath = "clickhouse"
)

var clickHouseAppDatabases = []string{"meta", "ops", "bronze", "silver", "gold"}

func init() {
	jobRegistry[backupClickHouseJobName] = jobRunner{
		description: "Create a native ClickHouse S3 backup for all application databases.",
		run:         runBackupClickHouse,
	}
	jobRegistry[restoreClickHouseJobName] = jobRunner{
		description: "Restore application databases from an explicit native ClickHouse S3 backup URL.",
		run:         runRestoreClickHouse,
	}
	jobRegistry[retentionMaterializeJobName] = jobRunner{
		description: "Materialize TTL policies for application MergeTree tables that define retention.",
		run:         runRetentionMaterialize,
	}
}

func runBackupClickHouse(ctx context.Context) error {
	startedAt := time.Now().UTC().Truncate(time.Millisecond)
	runner := migrate.NewHTTPRunner(controlPlaneClickHouseURL())
	backupName := clickHouseBackupName(startedAt)
	backupURL := clickHouseBackupURL(backupName)
	jobID := fmt.Sprintf("job:%s:%d", backupClickHouseJobName, startedAt.UnixMilli())
	statement := fmt.Sprintf("%s TO S3(%s, %s, %s) SETTINGS id = %s",
		clickHouseBackupObjectList("BACKUP"),
		sqlString(backupURL),
		sqlString(ctlGetenv("MINIO_ACCESS_KEY", "minio")),
		sqlString(ctlGetenv("MINIO_SECRET_KEY", "minio_change_me")),
		sqlString("oida_backup_"+backupName),
	)
	if err := runner.ApplySQL(ctx, statement); err != nil {
		return recordOpsJobFailure(ctx, runner, jobID, backupClickHouseJobName, startedAt, "native ClickHouse backup failed", err, map[string]any{"backup_url": backupURL, "backup_name": backupName})
	}
	return recordJobRun(ctx, runner, jobID, backupClickHouseJobName, "success", startedAt, time.Now().UTC().Truncate(time.Millisecond), "native ClickHouse backup completed", map[string]any{"backup_url": backupURL, "backup_name": backupName, "databases": clickHouseAppDatabases})
}

func runRestoreClickHouse(ctx context.Context) error {
	startedAt := time.Now().UTC().Truncate(time.Millisecond)
	runner := migrate.NewHTTPRunner(controlPlaneClickHouseURL())
	restoreURL := strings.TrimSpace(ctlGetenv("CLICKHOUSE_RESTORE_URL", ""))
	if restoreURL == "" {
		return fmt.Errorf("restore-clickhouse requires CLICKHOUSE_RESTORE_URL")
	}
	restoreName := backupNameFromURL(restoreURL)
	jobID := fmt.Sprintf("job:%s:%d", restoreClickHouseJobName, startedAt.UnixMilli())
	statement := fmt.Sprintf("%s FROM S3(%s, %s, %s) SETTINGS allow_non_empty_tables = 0, id = %s",
		clickHouseBackupObjectList("RESTORE"),
		sqlString(restoreURL),
		sqlString(ctlGetenv("MINIO_ACCESS_KEY", "minio")),
		sqlString(ctlGetenv("MINIO_SECRET_KEY", "minio_change_me")),
		sqlString("oida_restore_"+restoreName),
	)
	if err := runner.ApplySQL(ctx, statement); err != nil {
		return recordOpsJobFailure(ctx, runner, jobID, restoreClickHouseJobName, startedAt, "native ClickHouse restore failed", err, map[string]any{"restore_url": restoreURL, "restore_name": restoreName})
	}
	return recordJobRun(ctx, runner, jobID, restoreClickHouseJobName, "success", startedAt, time.Now().UTC().Truncate(time.Millisecond), "native ClickHouse restore completed", map[string]any{"restore_url": restoreURL, "restore_name": restoreName, "databases": clickHouseAppDatabases})
}

func runRetentionMaterialize(ctx context.Context) error {
	startedAt := time.Now().UTC().Truncate(time.Millisecond)
	runner := migrate.NewHTTPRunner(controlPlaneClickHouseURL())
	jobID := fmt.Sprintf("job:%s:%d", retentionMaterializeJobName, startedAt.UnixMilli())
	query := `SELECT concat(database, '.', name)
FROM system.tables
WHERE database IN ('meta', 'ops', 'bronze', 'silver', 'gold')
  AND engine LIKE '%MergeTree%'
  AND position(create_table_query, ' TTL ') > 0
ORDER BY database, name
FORMAT TabSeparated`
	out, err := runner.Query(ctx, query)
	if err != nil {
		return recordOpsJobFailure(ctx, runner, jobID, retentionMaterializeJobName, startedAt, "discover TTL tables failed", err, nil)
	}
	tables := parseTabSeparatedList(out)
	materialized := []string{}
	for _, table := range tables {
		table = strings.TrimSpace(table)
		if !validQualifiedTable(table) {
			return recordOpsJobFailure(ctx, runner, jobID, retentionMaterializeJobName, startedAt, "invalid TTL table name", fmt.Errorf("invalid qualified table %q", table), map[string]any{"table": table})
		}
		if err := runner.ApplySQL(ctx, "ALTER TABLE "+table+" MATERIALIZE TTL"); err != nil {
			return recordOpsJobFailure(ctx, runner, jobID, retentionMaterializeJobName, startedAt, "materialize TTL failed", err, map[string]any{"table": table})
		}
		materialized = append(materialized, table)
	}
	return recordJobRun(ctx, runner, jobID, retentionMaterializeJobName, "success", startedAt, time.Now().UTC().Truncate(time.Millisecond), "materialized ClickHouse TTL policies", map[string]any{"table_count": len(materialized), "tables": materialized})
}

func clickHouseBackupObjectList(verb string) string {
	parts := make([]string, 0, len(clickHouseAppDatabases))
	for _, database := range clickHouseAppDatabases {
		parts = append(parts, "DATABASE "+database)
	}
	return strings.TrimSpace(strings.ToUpper(verb)) + " " + strings.Join(parts, ", ")
}

func clickHouseBackupName(startedAt time.Time) string {
	if name := strings.TrimSpace(ctlGetenv("CLICKHOUSE_BACKUP_NAME", "")); name != "" {
		return sanitizeBackupName(name)
	}
	return "oida-" + startedAt.UTC().Format("20060102T150405Z")
}

func clickHouseBackupURL(backupName string) string {
	endpoint := strings.TrimRight(ctlGetenv("MINIO_ENDPOINT", defaultStageEndpoint), "/")
	bucket := strings.Trim(ctlGetenv("BACKUP_BUCKET", "backup"), "/")
	prefix := strings.Trim(ctlGetenv("CLICKHOUSE_BACKUP_PREFIX", defaultClickHouseBackupPath), "/")
	return endpoint + "/" + path.Join(bucket, prefix, backupName)
}

func backupNameFromURL(rawURL string) string {
	parsed, err := url.Parse(rawURL)
	if err == nil && strings.Trim(parsed.Path, "/") != "" {
		return sanitizeBackupName(path.Base(parsed.Path))
	}
	trimmed := strings.Trim(rawURL, "/")
	if trimmed == "" {
		return "restore"
	}
	return sanitizeBackupName(path.Base(trimmed))
}

func sanitizeBackupName(name string) string {
	name = strings.TrimSpace(name)
	var b strings.Builder
	for _, r := range name {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '-' || r == '_' || r == '.' {
			b.WriteRune(r)
			continue
		}
		b.WriteByte('_')
	}
	sanitized := strings.Trim(b.String(), "._-")
	if sanitized == "" {
		return "backup"
	}
	return sanitized
}

func parseTabSeparatedList(out string) []string {
	rows := strings.Split(strings.TrimSpace(out), "\n")
	values := make([]string, 0, len(rows))
	for _, row := range rows {
		value := strings.TrimSpace(row)
		if value != "" {
			values = append(values, value)
		}
	}
	return values
}

func validQualifiedTable(value string) bool {
	database, table, ok := strings.Cut(value, ".")
	return ok && validClickHouseIdentifier(database) && validClickHouseIdentifier(table)
}

func validClickHouseIdentifier(value string) bool {
	if value == "" {
		return false
	}
	for idx, r := range value {
		if idx == 0 {
			if r != '_' && !unicode.IsLetter(r) {
				return false
			}
			continue
		}
		if r != '_' && !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			return false
		}
	}
	return true
}

func recordOpsJobFailure(ctx context.Context, runner *migrate.HTTPRunner, jobID, jobName string, startedAt time.Time, message string, err error, stats map[string]any) error {
	if stats == nil {
		stats = map[string]any{}
	}
	stats["error"] = err.Error()
	if recordErr := recordJobRun(ctx, runner, jobID, jobName, "failed", startedAt, time.Now().UTC().Truncate(time.Millisecond), message, stats); recordErr != nil {
		return fmt.Errorf("%w (job log failed: %v)", err, recordErr)
	}
	return err
}
