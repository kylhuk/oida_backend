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
		"ADD COLUMN IF NOT EXISTS record_version UInt64 DEFAULT version AFTER version;",
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
		"ADD COLUMN IF NOT EXISTS record_version UInt64 DEFAULT version",
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
		"requests_per_minute",
		"seed_checksum",
		"FINAL",
	} {
		if !strings.Contains(doc, fragment) {
			t.Fatalf("schema standards doc missing fragment %q", fragment)
		}
	}
}

func TestSourceGovernanceMigrationDefinesFrozenColumnsAndCompatibilityView(t *testing.T) {
	migration := readRepoFile(t, "migrations", "clickhouse", "0006_source_governance.sql")

	for _, fragment := range []string{
		"ADD COLUMN IF NOT EXISTS auth_config_json String DEFAULT '{}'",
		"ADD COLUMN IF NOT EXISTS requests_per_minute UInt32 DEFAULT 60",
		"ADD COLUMN IF NOT EXISTS burst_size UInt16 DEFAULT 10",
		"ADD COLUMN IF NOT EXISTS retention_class LowCardinality(String) DEFAULT 'warm'",
		"ADD COLUMN IF NOT EXISTS disabled_reason Nullable(String)",
		"ADD COLUMN IF NOT EXISTS disabled_at Nullable(DateTime64(3, 'UTC'))",
		"ADD COLUMN IF NOT EXISTS disabled_by Nullable(String)",
		"ADD COLUMN IF NOT EXISTS review_status LowCardinality(String) DEFAULT 'approved'",
		"ADD COLUMN IF NOT EXISTS review_notes String DEFAULT ''",
		"ADD COLUMN IF NOT EXISTS backfill_priority UInt16 DEFAULT 100",
		"ADD COLUMN IF NOT EXISTS attribution_required UInt8 DEFAULT 0",
		"DROP VIEW IF EXISTS gold.api_v1_sources;",
		"FROM meta.source_registry FINAL;",
	} {
		if !strings.Contains(migration, fragment) {
			t.Fatalf("governance migration missing fragment %q", fragment)
		}
	}
}

func TestBaselineStorageMigrationDefinesRequiredTables(t *testing.T) {
	migration := readRepoFile(t, "migrations", "clickhouse", "0005_baseline_tables.sql")

	requiredTables := []string{
		"ops.parse_log",
		"ops.unresolved_location_queue",
		"ops.quality_incident",
		"bronze.raw_structured_row",
		"silver.dim_place",
		"silver.place_polygon",
		"silver.place_hierarchy",
		"silver.dim_entity",
		"silver.entity_alias",
		"silver.fact_observation",
		"silver.fact_event",
		"silver.fact_track_point",
		"silver.fact_track_segment",
		"silver.bridge_event_entity",
		"silver.bridge_event_place",
		"silver.bridge_entity_place",
		"silver.metric_contribution",
		"gold.metric_state",
		"gold.metric_snapshot",
		"gold.hotspot_snapshot",
	}

	for _, table := range requiredTables {
		snippet := "CREATE TABLE IF NOT EXISTS " + table
		if !strings.Contains(migration, snippet) {
			t.Fatalf("migration missing table %q", table)
		}
	}
}

func TestBaselineStorageTablesFollowEngineAndPartitionConventions(t *testing.T) {
	migration := readRepoFile(t, "migrations", "clickhouse", "0005_baseline_tables.sql")

	for _, spec := range []struct {
		database  string
		table     string
		fragments []string
	}{
		{
			database: "silver",
			table:    "dim_place",
			fragments: []string{
				"record_version UInt64",
				"LowCardinality(String)",
				"ENGINE = ReplacingMergeTree(record_version)",
				"ORDER BY (country_code, place_type, place_id)",
			},
		},
		{
			database: "silver",
			table:    "fact_observation",
			fragments: []string{
				"LowCardinality(String)",
				"ENGINE = MergeTree",
				"PARTITION BY toYYYYMM(observed_at)",
				"ORDER BY (place_id, observation_type, observed_at, observation_id)",
				"TTL toDateTime(observed_at) + INTERVAL 1095 DAY DELETE",
			},
		},
		{
			database: "gold",
			table:    "metric_state",
			fragments: []string{
				"AggregateFunction(count)",
				"AggregateFunction(sum, Float64)",
				"ENGINE = AggregatingMergeTree",
				"PARTITION BY toYYYYMM(window_start)",
				"ORDER BY (metric_id, subject_grain, subject_id, window_grain, window_start)",
				"TTL toDateTime(window_start) + INTERVAL 730 DAY DELETE",
			},
		},
		{
			database: "ops",
			table:    "parse_log",
			fragments: []string{
				"LowCardinality(String)",
				"ENGINE = MergeTree",
				"PARTITION BY toYYYYMM(started_at)",
				"TTL toDateTime(started_at) + INTERVAL 180 DAY DELETE",
			},
		},
		{
			database: "bronze",
			table:    "raw_structured_row",
			fragments: []string{
				"CODEC(ZSTD(5))",
				"ENGINE = MergeTree",
				"PARTITION BY toYYYYMM(extracted_at)",
				"ORDER BY (source_id, extracted_at, raw_id, row_number, row_id)",
				"TTL toDateTime(extracted_at) + INTERVAL 180 DAY DELETE",
			},
		},
	} {
		section := mustMatchCreateSection(t, migration, spec.database, spec.table)
		for _, fragment := range spec.fragments {
			if !strings.Contains(section, fragment) {
				t.Fatalf("table %s.%s missing fragment %q", spec.database, spec.table, fragment)
			}
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

func mustMatchCreateSection(t *testing.T, migration, database, table string) string {
	t.Helper()

	pattern := regexp.MustCompile(`(?s)CREATE TABLE IF NOT EXISTS ` + regexp.QuoteMeta(database) + `\.` + regexp.QuoteMeta(table) + `\n\(.*?\)\nENGINE = .*?;`)
	match := pattern.FindString(migration)
	if match == "" {
		t.Fatalf("unable to find table section for %s.%s", database, table)
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
