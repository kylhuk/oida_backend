package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestOpsJobsRunNativeClickHouseBackupRestoreAndRetention(t *testing.T) {
	t.Run("backup", func(t *testing.T) {
		queries, server := captureClickHouseQueries(t, nil)
		defer server.Close()
		t.Setenv("CLICKHOUSE_HTTP_URL", server.URL)
		t.Setenv("MINIO_ENDPOINT", "http://minio:9000")
		t.Setenv("MINIO_ACCESS_KEY", "minio")
		t.Setenv("MINIO_SECRET_KEY", "minio_change_me")
		t.Setenv("BACKUP_BUCKET", "backup")
		t.Setenv("CLICKHOUSE_BACKUP_NAME", "test-backup")

		if code := run([]string{"run-once", "--job", "backup-clickhouse"}, &strings.Builder{}, &strings.Builder{}); code != 0 {
			t.Fatalf("expected backup job to succeed, got code %d", code)
		}
		joined := strings.Join(*queries, "\n")
		for _, want := range []string{
			"BACKUP DATABASE meta, DATABASE ops, DATABASE bronze, DATABASE silver, DATABASE gold TO S3('http://minio:9000/backup/clickhouse/test-backup'",
			"SETTINGS id = 'oida_backup_test-backup'",
			"INSERT INTO ops.job_run",
			"backup-clickhouse",
		} {
			if !strings.Contains(joined, want) {
				t.Fatalf("backup query missing %q: %s", want, joined)
			}
		}
		if strings.Contains(joined, "minio_change_me\"") {
			t.Fatalf("backup job stats must not JSON-log raw object-store secrets: %s", joined)
		}
	})

	t.Run("restore", func(t *testing.T) {
		queries, server := captureClickHouseQueries(t, nil)
		defer server.Close()
		t.Setenv("CLICKHOUSE_HTTP_URL", server.URL)
		t.Setenv("MINIO_ACCESS_KEY", "minio")
		t.Setenv("MINIO_SECRET_KEY", "minio_change_me")
		t.Setenv("CLICKHOUSE_RESTORE_URL", "http://minio:9000/backup/clickhouse/test-backup")

		if code := run([]string{"run-once", "--job", "restore-clickhouse"}, &strings.Builder{}, &strings.Builder{}); code != 0 {
			t.Fatalf("expected restore job to succeed, got code %d", code)
		}
		joined := strings.Join(*queries, "\n")
		for _, want := range []string{
			"RESTORE DATABASE meta, DATABASE ops, DATABASE bronze, DATABASE silver, DATABASE gold FROM S3('http://minio:9000/backup/clickhouse/test-backup'",
			"SETTINGS allow_non_empty_tables = 0, id = 'oida_restore_test-backup'",
			"INSERT INTO ops.job_run",
			"restore-clickhouse",
		} {
			if !strings.Contains(joined, want) {
				t.Fatalf("restore query missing %q: %s", want, joined)
			}
		}
	})

	t.Run("retention", func(t *testing.T) {
		queries, server := captureClickHouseQueries(t, func(w http.ResponseWriter, query string) bool {
			if strings.Contains(query, "FROM system.tables") {
				_, _ = w.Write([]byte("ops.job_run\nbronze.raw_fetch\n"))
				return true
			}
			return false
		})
		defer server.Close()
		t.Setenv("CLICKHOUSE_HTTP_URL", server.URL)

		if code := run([]string{"run-once", "--job", "retention-materialize"}, &strings.Builder{}, &strings.Builder{}); code != 0 {
			t.Fatalf("expected retention job to succeed, got code %d", code)
		}
		joined := strings.Join(*queries, "\n")
		for _, want := range []string{
			"ALTER TABLE ops.job_run MATERIALIZE TTL",
			"ALTER TABLE bronze.raw_fetch MATERIALIZE TTL",
			"retention-materialize",
		} {
			if !strings.Contains(joined, want) {
				t.Fatalf("retention query missing %q: %s", want, joined)
			}
		}
	})
}

func TestRestoreJobRequiresExplicitBackupURL(t *testing.T) {
	stderr := &strings.Builder{}
	if code := run([]string{"run-once", "--job", "restore-clickhouse"}, &strings.Builder{}, stderr); code == 0 {
		t.Fatalf("expected restore without CLICKHOUSE_RESTORE_URL to fail")
	}
	if !strings.Contains(stderr.String(), "CLICKHOUSE_RESTORE_URL") {
		t.Fatalf("expected restore error to mention CLICKHOUSE_RESTORE_URL, got %s", stderr.String())
	}
}

func captureClickHouseQueries(t *testing.T, special func(http.ResponseWriter, string) bool) (*[]string, *httptest.Server) {
	t.Helper()
	queries := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query().Get("query")
		queries = append(queries, query)
		if special != nil && special(w, query) {
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	return &queries, server
}
