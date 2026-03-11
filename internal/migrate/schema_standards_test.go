package migrate

import (
	"encoding/json"
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

func TestCrawlFrontierLeaseMigrationDefinesQueueColumns(t *testing.T) {
	migration := readRepoFile(t, "migrations", "clickhouse", "0013_crawl_frontier_leases.sql")

	for _, fragment := range []string{
		"ALTER TABLE ops.crawl_frontier",
		"ADD COLUMN IF NOT EXISTS lease_owner Nullable(String)",
		"ADD COLUMN IF NOT EXISTS lease_expires_at Nullable(DateTime64(3, 'UTC'))",
		"ADD COLUMN IF NOT EXISTS attempt_count UInt16 DEFAULT 0",
		"ADD COLUMN IF NOT EXISTS last_attempt_at Nullable(DateTime64(3, 'UTC'))",
		"ADD COLUMN IF NOT EXISTS last_fetch_id Nullable(String)",
		"ADD COLUMN IF NOT EXISTS last_status_code Nullable(UInt16)",
		"ADD COLUMN IF NOT EXISTS last_error_code Nullable(String)",
		"ADD COLUMN IF NOT EXISTS last_error_message Nullable(String)",
		"ADD COLUMN IF NOT EXISTS etag Nullable(String)",
		"ADD COLUMN IF NOT EXISTS last_modified Nullable(String)",
		"ADD COLUMN IF NOT EXISTS discovery_kind LowCardinality(String) DEFAULT 'unknown'",
	} {
		if !strings.Contains(migration, fragment) {
			t.Fatalf("crawl frontier lease migration missing fragment %q", fragment)
		}
	}
}

func TestSourceCatalogContractMigrationDefinesCatalogKindAndLifecycleState(t *testing.T) {
	migration := readRepoFile(t, "migrations", "clickhouse", "0016_source_catalog_contract.sql")

	for _, fragment := range []string{
		"ALTER TABLE meta.source_registry",
		"ADD COLUMN IF NOT EXISTS catalog_kind LowCardinality(String) DEFAULT 'concrete'",
		"ADD COLUMN IF NOT EXISTS lifecycle_state LowCardinality(String) DEFAULT 'approved_enabled'",
	} {
		if !strings.Contains(migration, fragment) {
			t.Fatalf("source catalog contract migration missing fragment %q", fragment)
		}
	}
}

func TestSourceGenerationGovernanceMigrationDefinesReviewGatedMetaTables(t *testing.T) {
	migration := readRepoFile(t, "migrations", "clickhouse", "0017_source_generation_governance.sql")

	for _, table := range []string{
		"meta.source_catalog",
		"meta.source_family_template",
		"meta.discovery_probe",
		"meta.discovery_candidate",
		"meta.source_generation_log",
	} {
		snippet := "CREATE TABLE IF NOT EXISTS " + table
		if !strings.Contains(migration, snippet) {
			t.Fatalf("source generation governance migration missing table %q", table)
		}
	}

	for _, fragment := range []string{
		"review_status LowCardinality(String)",
		"materialized_source_id Nullable(String)",
		"schema_version UInt32",
		"record_version UInt64",
		"api_contract_version UInt32",
		"updated_at DateTime64(3, 'UTC')",
		"attrs String",
		"evidence String",
		"ENGINE = ReplacingMergeTree(record_version)",
	} {
		if !strings.Contains(migration, fragment) {
			t.Fatalf("source generation governance migration missing fragment %q", fragment)
		}
	}
}

func TestFetchLedgerMigrationDefinesImmutableReplayColumns(t *testing.T) {
	migration := readRepoFile(t, "migrations", "clickhouse", "0014_fetch_ledger_contract.sql")

	for _, fragment := range []string{
		"ALTER TABLE ops.fetch_log",
		"ADD COLUMN IF NOT EXISTS attempt_count UInt16 DEFAULT 1",
		"ADD COLUMN IF NOT EXISTS retry_count UInt16 DEFAULT 0",
		"ALTER TABLE bronze.raw_document",
		"ADD COLUMN IF NOT EXISTS fetch_id String DEFAULT ''",
		"ADD COLUMN IF NOT EXISTS final_url String DEFAULT url",
		"ADD COLUMN IF NOT EXISTS etag Nullable(String)",
		"ADD COLUMN IF NOT EXISTS last_modified Nullable(String)",
		"ADD COLUMN IF NOT EXISTS not_modified UInt8 DEFAULT 0",
		"ADD COLUMN IF NOT EXISTS storage_class LowCardinality(String) DEFAULT 'metadata-only'",
	} {
		if !strings.Contains(migration, fragment) {
			t.Fatalf("fetch ledger migration missing fragment %q", fragment)
		}
	}
}

func TestSourceBronzeTablesMigrationDefinesAllStaticTables(t *testing.T) {
	migration := readRepoFile(t, "migrations", "clickhouse", "0015_source_bronze_tables.sql")

	for _, table := range []string{
		"bronze.src_seed_gdelt_v1",
		"bronze.src_fixture_reliefweb_v1",
		"bronze.src_fixture_acled_v1",
		"bronze.src_fixture_opensanctions_v1",
		"bronze.src_fixture_nasa_firms_v1",
		"bronze.src_fixture_noaa_hazards_v1",
		"bronze.src_fixture_kev_v1",
	} {
		snippet := "CREATE TABLE IF NOT EXISTS " + table
		if !strings.Contains(migration, snippet) {
			t.Fatalf("source bronze migration missing table %q", table)
		}
	}

	for _, fragment := range []string{
		"raw_id String",
		"fetch_id String",
		"source_id LowCardinality(String)",
		"parser_id LowCardinality(String)",
		"parser_version String",
		"source_record_key String",
		"source_record_index UInt32",
		"record_kind LowCardinality(String)",
		"native_id Nullable(String)",
		"source_url String",
		"canonical_url Nullable(String)",
		"fetched_at DateTime64(3, 'UTC')",
		"parsed_at DateTime64(3, 'UTC')",
		"occurred_at Nullable(DateTime64(3, 'UTC'))",
		"published_at Nullable(DateTime64(3, 'UTC'))",
		"title Nullable(String)",
		"summary Nullable(String)",
		"status Nullable(String)",
		"place_hint Nullable(String)",
		"lat Nullable(Float64)",
		"lon Nullable(Float64)",
		"severity Nullable(String)",
		"content_hash String",
		"schema_version UInt32",
		"record_version UInt64",
		"attrs String",
		"evidence String",
		"payload_json String",
		"ENGINE = ReplacingMergeTree(record_version)",
		"PARTITION BY toYYYYMM(parsed_at)",
		"ORDER BY (source_record_key, parsed_at, raw_id, source_record_index)",
	} {
		if !strings.Contains(migration, fragment) {
			t.Fatalf("source bronze migration missing fragment %q", fragment)
		}
	}
}

func TestSourceBronzeTablesMigrationMatchesCompiledCatalogManifest(t *testing.T) {
	migration := readRepoFile(t, "migrations", "clickhouse", "0015_source_bronze_tables.sql")
	type bronzeManifestRow struct {
		BronzeTable string `json:"bronze_table"`
	}
	type compiledCatalog struct {
		BronzeDDLManifest []bronzeManifestRow `json:"bronze_ddl_manifest"`
	}
	var compiled compiledCatalog
	b := readRepoFile(t, "seed", "source_catalog_compiled.json")
	if err := json.Unmarshal([]byte(b), &compiled); err != nil {
		t.Fatalf("decode compiled source catalog manifest: %v", err)
	}
	if len(compiled.BronzeDDLManifest) != 7 {
		t.Fatalf("expected 7 bronze manifest rows, got %d", len(compiled.BronzeDDLManifest))
	}
	for _, row := range compiled.BronzeDDLManifest {
		snippet := "CREATE TABLE IF NOT EXISTS " + row.BronzeTable
		if !strings.Contains(migration, snippet) {
			t.Fatalf("compiled bronze manifest table %q missing from migration", row.BronzeTable)
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
				"ORDER BY (metric_id, subject_grain, subject_id, window_grain, window_start, materialization_key)",
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

func TestParseCheckpointLedgerMigrationDefinesProcessedDocumentContract(t *testing.T) {
	migration := readRepoFile(t, "migrations", "clickhouse", "0018_parse_checkpoint_ledger.sql")
	for _, fragment := range []string{
		"CREATE TABLE IF NOT EXISTS ops.parse_checkpoint",
		"checkpoint_id String",
		"raw_id String",
		"parser_version String",
		"content_hash String",
		"bronze_table String",
		"status LowCardinality(String)",
		"ENGINE = ReplacingMergeTree(record_version)",
	} {
		if !strings.Contains(migration, fragment) {
			t.Fatalf("parse checkpoint migration missing fragment %q", fragment)
		}
	}
}

func TestSourceFamilyTemplateContractMigrationDefinesTemplateMetadata(t *testing.T) {
	migration := readRepoFile(t, "migrations", "clickhouse", "0019_source_family_template_contract.sql")
	for _, fragment := range []string{
		"ALTER TABLE meta.source_family_template",
		"ADD COLUMN IF NOT EXISTS scope String",
		"ADD COLUMN IF NOT EXISTS integration_archetype LowCardinality(String)",
		"ADD COLUMN IF NOT EXISTS review_status_default LowCardinality(String)",
		"ADD COLUMN IF NOT EXISTS generator_relationships Array(String)",
	} {
		if !strings.Contains(migration, fragment) {
			t.Fatalf("source family template contract migration missing fragment %q", fragment)
		}
	}
}

func TestAPIExpansionViewsMigrationDefinesExpandedSurface(t *testing.T) {
	migration := readRepoFile(t, "migrations", "clickhouse", "0007_api_expansion_views.sql")

	for _, fragment := range []string{
		"CREATE VIEW IF NOT EXISTS gold.api_v1_jobs AS",
		"CREATE VIEW IF NOT EXISTS gold.api_v1_places AS",
		"CREATE VIEW IF NOT EXISTS gold.api_v1_entities AS",
		"CREATE VIEW IF NOT EXISTS gold.api_v1_events AS",
		"CREATE VIEW IF NOT EXISTS gold.api_v1_observations AS",
		"CREATE VIEW IF NOT EXISTS gold.api_v1_metrics AS",
		"CREATE VIEW IF NOT EXISTS gold.api_v1_source_coverage AS",
		"CREATE VIEW IF NOT EXISTS gold.api_v1_metric_rollups AS",
		"CREATE VIEW IF NOT EXISTS gold.api_v1_time_series AS",
		"CREATE VIEW IF NOT EXISTS gold.api_v1_hotspots AS",
		"CREATE VIEW IF NOT EXISTS gold.api_v1_tracks AS",
		"CREATE VIEW IF NOT EXISTS gold.api_v1_entity_events AS",
		"CREATE VIEW IF NOT EXISTS gold.api_v1_entity_places AS",
		"CREATE VIEW IF NOT EXISTS gold.api_v1_cross_domain AS",
	} {
		if !strings.Contains(migration, fragment) {
			t.Fatalf("API expansion migration missing fragment %q", fragment)
		}
	}
}

func TestMetricMaterializationMigrationAddsIdempotentColumns(t *testing.T) {
	migration := readRepoFile(t, "migrations", "clickhouse", "0009_metric_materialization_idempotency.sql")
	for _, fragment := range []string{
		"ALTER TABLE silver.metric_contribution",
		"ADD COLUMN IF NOT EXISTS source_id String",
		"ADD COLUMN IF NOT EXISTS materialization_key String",
		"ALTER TABLE gold.metric_state",
		"ADD COLUMN IF NOT EXISTS distinct_source_count_state AggregateFunction(uniqExact, String)",
		"ADD COLUMN IF NOT EXISTS latest_value_state AggregateFunction(argMax, Float64, DateTime64(3, 'UTC'))",
		"ALTER TABLE gold.metric_snapshot",
	} {
		if !strings.Contains(migration, fragment) {
			t.Fatalf("metric materialization migration missing fragment %q", fragment)
		}
	}
}

func TestRuntimeAnalyticsOutputsMigrationAddsCrossDomainStorage(t *testing.T) {
	migration := readRepoFile(t, "migrations", "clickhouse", "0010_runtime_analytics_outputs.sql")
	for _, fragment := range []string{
		"CREATE TABLE IF NOT EXISTS gold.cross_domain_snapshot",
		"domains Array(String)",
		"metric_ids Array(String)",
		"ENGINE = MergeTree",
		"DROP VIEW IF EXISTS gold.api_v1_cross_domain",
		"CREATE VIEW IF NOT EXISTS gold.api_v1_cross_domain AS",
		"FROM gold.cross_domain_snapshot",
	} {
		if !strings.Contains(migration, fragment) {
			t.Fatalf("runtime analytics migration missing fragment %q", fragment)
		}
	}
}

func TestBootstrapOwnsSchemaMigrationLedger(t *testing.T) {
	runner := readRepoFile(t, "internal", "migrate", "http_runner.go")
	for _, fragment := range []string{
		"CREATE TABLE IF NOT EXISTS meta.schema_migrations",
		"version String",
		"applied_at DateTime DEFAULT now()",
		"checksum String",
		"success UInt8",
		"notes String",
		"schemaMigrationColumns = []string{\"version\", \"applied_at\", \"checksum\", \"success\", \"notes\"}",
	} {
		if !strings.Contains(runner, fragment) {
			t.Fatalf("http runner missing ledger ownership fragment %q", fragment)
		}
	}

	files, err := filepath.Glob(filepath.Join(repoRoot(t), "migrations", "clickhouse", "*.sql"))
	if err != nil {
		t.Fatalf("glob migrations: %v", err)
	}
	for _, path := range files {
		rel, err := filepath.Rel(repoRoot(t), path)
		if err != nil {
			t.Fatalf("rel path for %s: %v", path, err)
		}
		contents := readRepoFile(t, strings.Split(filepath.ToSlash(rel), "/")...)
		if strings.Contains(contents, "schema_migrations") {
			t.Fatalf("sql migration %s must not manage bootstrap-owned schema ledger", filepath.Base(path))
		}
	}
}

func TestRunbooksUseSchemaMigrationVersionAndChecksumColumns(t *testing.T) {
	upgrade := readRepoFile(t, "docs", "runbooks", "upgrade-migration.md")
	for _, fragment := range []string{"bootstrap-owned", "version", "applied_at", "checksum", "success", "notes"} {
		if !strings.Contains(upgrade, fragment) {
			t.Fatalf("upgrade runbook missing fragment %q", fragment)
		}
	}
	for _, fragment := range []string{"SELECT%20id", "id ordering", "right `id` ordering"} {
		if strings.Contains(upgrade, fragment) {
			t.Fatalf("upgrade runbook still contains stale id fragment %q", fragment)
		}
	}

	backupRestore := readRepoFile(t, "docs", "runbooks", "backup-restore.md")
	for _, fragment := range []string{"highest migration ID", "migration ID"} {
		if strings.Contains(backupRestore, fragment) {
			t.Fatalf("backup/restore runbook still contains stale id fragment %q", fragment)
		}
	}
	for _, fragment := range []string{"highest applied migration version", "checksum"} {
		if !strings.Contains(backupRestore, fragment) {
			t.Fatalf("backup/restore runbook missing fragment %q", fragment)
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

	path := filepath.Join(append([]string{repoRoot(t)}, parts...)...)
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(b)
}

func repoRoot(t *testing.T) string {
	t.Helper()

	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve current file")
	}
	return filepath.Join(filepath.Dir(currentFile), "..", "..")
}
