package migrate

import (
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
)

func TestMetaRegistryMigrationDefinesRequiredTables(t *testing.T) {
	migration := readRepoFile(t, "migrations", "clickhouse", "0004_meta_registries.sql")

	requiredSnippets := []string{
		"ALTER TABLE meta.source_registry\n    RENAME COLUMN IF EXISTS version TO record_version;",
		"CREATE TABLE IF NOT EXISTS meta.parser_registry",
		"CREATE TABLE IF NOT EXISTS meta.metric_registry",
		"CREATE TABLE IF NOT EXISTS meta.api_schema_registry",
	}

	for _, snippet := range requiredSnippets {
		if !strings.Contains(migration, snippet) {
			t.Fatalf("migration missing snippet %q", snippet)
		}
	}
}

func TestMetaRegistryTablesFollowFrozenStandards(t *testing.T) {
	migration := readRepoFile(t, "migrations", "clickhouse", "0004_meta_registries.sql")

	tables := []string{"parser_registry", "metric_registry", "api_schema_registry"}
	for _, table := range tables {
		section := mustMatchTableSection(t, migration, table)
		for _, fragment := range []string{
			"attrs String",
			"evidence String",
			"schema_version UInt32",
			"record_version UInt64",
			"api_contract_version UInt32",
			"updated_at DateTime64(3, 'UTC')",
			"ENGINE = ReplacingMergeTree(record_version)",
			"ORDER BY (",
			"LowCardinality(String)",
		} {
			if !strings.Contains(section, fragment) {
				t.Fatalf("table %s missing fragment %q", table, fragment)
			}
		}
	}
}

func TestSourceRegistryMigrationBackfillsStandardColumns(t *testing.T) {
	migration := readRepoFile(t, "migrations", "clickhouse", "0004_meta_registries.sql")

	for _, fragment := range []string{
		"ADD COLUMN IF NOT EXISTS schema_version UInt32 DEFAULT 1",
		"ADD COLUMN IF NOT EXISTS api_contract_version UInt32 DEFAULT 1",
		"ADD COLUMN IF NOT EXISTS attrs String DEFAULT '{}'",
		"ADD COLUMN IF NOT EXISTS evidence String DEFAULT '[]'",
	} {
		if !strings.Contains(migration, fragment) {
			t.Fatalf("source_registry migration missing fragment %q", fragment)
		}
	}
}

func TestSchemaStandardsDocumentFreezesConventions(t *testing.T) {
	doc := readRepoFile(t, "docs", "schema-standards.md")

	for _, fragment := range []string{
		"lowercase snake_case",
		"DateTime64(3, 'UTC')",
		"schema_version",
		"record_version",
		"api_contract_version",
		"attrs",
		"evidence",
		"toYYYYMM",
		"LowCardinality(String)",
		"MergeTree",
		"ReplacingMergeTree(record_version)",
		"AggregatingMergeTree",
		"inventing table conventions ad hoc",
	} {
		if !strings.Contains(doc, fragment) {
			t.Fatalf("schema standards doc missing fragment %q", fragment)
		}
	}
}

func mustMatchTableSection(t *testing.T, migration, table string) string {
	t.Helper()

	pattern := regexp.MustCompile(`(?s)CREATE TABLE IF NOT EXISTS meta\.` + regexp.QuoteMeta(table) + `\n\(.*?\)\nENGINE = ReplacingMergeTree\(record_version\)\nORDER BY \(.*?\);`)
	match := pattern.FindString(migration)
	if match == "" {
		t.Fatalf("unable to find table section for %s", table)
	}
	return match
}

func readRepoFile(t *testing.T, parts ...string) string {
	t.Helper()

	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve current file")
	}
	root := filepath.Join(filepath.Dir(currentFile), "..", "..")
	path := filepath.Join(append([]string{root}, parts...)...)
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(b)
}
