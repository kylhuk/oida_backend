package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"global-osint-backend/internal/migrate"
)

const (
	defaultMigrationDir  = "/app/migrations/clickhouse"
	defaultCatalogPath   = "/app/seed/source_catalog.json"
	defaultRegistryPath  = "/app/seed/source_registry.json"
	defaultSeedPath      = "/app/seed/source_catalog_compiled.json"
	defaultReadyMarker   = "/tmp/bootstrap.ready"
	defaultClickHouseURL = "http://clickhouse:8123"
	defaultMinIOEndpoint = "http://minio:9000"
	defaultMinIORegion   = "us-east-1"
	defaultBuckets       = "raw,stage,backup"
	defaultBackupDir     = "/app/infra/backup"
	defaultBackupBucket  = "backup"
	defaultBackupPrefix  = "bootstrap"
	defaultStageAssets   = "/app/seed/staged"
	defaultStageBucket   = "stage"
	defaultAPIClients    = "/app/seed/api_clients.json"
)

var (
	logicalDatabases = []string{"meta", "ops", "bronze", "silver", "gold"}
	roleSpecs        = []clickhouseRole{
		{
			Name: "osint_reader",
			Grants: []string{
				"GRANT SELECT ON meta.* TO osint_reader",
				"GRANT SELECT ON ops.* TO osint_reader",
				"GRANT SELECT ON bronze.* TO osint_reader",
				"GRANT SELECT ON silver.* TO osint_reader",
				"GRANT SELECT ON gold.* TO osint_reader",
				"GRANT SELECT ON system.parts TO osint_reader",
				"GRANT SELECT ON system.tables TO osint_reader",
			},
		},
		{
			Name: "osint_ingest",
			Grants: []string{
				"GRANT SELECT ON meta.* TO osint_ingest",
				"GRANT SELECT ON ops.* TO osint_ingest",
				"GRANT INSERT ON ops.* TO osint_ingest",
				"GRANT ALTER UPDATE(state, lease_owner, lease_expires_at, attempt_count, last_attempt_at, last_fetch_id, last_status_code, last_error_code, last_error_message, etag, last_modified) ON ops.crawl_frontier TO osint_ingest",
				"GRANT SELECT ON bronze.* TO osint_ingest",
				"GRANT INSERT ON bronze.* TO osint_ingest",
			},
			Revokes: []string{
				"REVOKE SELECT ON silver.* FROM osint_ingest",
				"REVOKE INSERT ON silver.* FROM osint_ingest",
				"REVOKE SELECT ON gold.* FROM osint_ingest",
				"REVOKE INSERT ON gold.* FROM osint_ingest",
			},
		},
		{
			Name: "osint_promote",
			Grants: []string{
				"GRANT SELECT ON meta.* TO osint_promote",
				"GRANT INSERT ON meta.discovery_candidate TO osint_promote",
				"GRANT OPTIMIZE ON meta.discovery_candidate TO osint_promote",
				"GRANT SELECT ON ops.* TO osint_promote",
				"GRANT INSERT ON ops.job_run TO osint_promote",
				"GRANT INSERT ON ops.crawl_frontier TO osint_promote",
				"GRANT ALTER UPDATE(url, state, lease_owner, lease_expires_at, discovery_kind, last_attempt_at) ON ops.crawl_frontier TO osint_promote",
				"GRANT SELECT ON bronze.* TO osint_promote",
				"GRANT SELECT ON silver.* TO osint_promote",
				"GRANT INSERT ON silver.* TO osint_promote",
				"GRANT TRUNCATE ON silver.* TO osint_promote",
				"GRANT DROP TABLE ON silver.* TO osint_promote",
				"GRANT CREATE TABLE ON silver.* TO osint_promote",
				"GRANT CREATE DICTIONARY ON silver.* TO osint_promote",
				"GRANT DROP DICTIONARY ON silver.* TO osint_promote",
				"GRANT dictGet ON silver.* TO osint_promote",
				"GRANT SELECT ON gold.* TO osint_promote",
				"GRANT INSERT ON gold.* TO osint_promote",
			},
			Revokes: []string{
				"REVOKE INSERT ON bronze.* FROM osint_promote",
			},
		},
		{
			Name: "osint_admin",
			Grants: []string{
				"GRANT ALL ON *.* TO osint_admin",
			},
		},
	}
)

type clickhouseRole struct {
	Name    string
	Grants  []string
	Revokes []string
}

type clickhouseUser struct {
	Name     string
	Password string
	Roles    []string
}

type config struct {
	MigrationDir   string
	ClickHouseHTTP string
	SeedPath       string
	ReadyMarker    string
	MinIOEndpoint  string
	MinIOAccessKey string
	MinIOSecretKey string
	MinIORegion    string
	Buckets        []string
	BackupAssets   string
	BackupBucket   string
	BackupPrefix   string
	StageAssets    string
	StageBucket    string
	APIClientsPath string
	Users          []clickhouseUser
}

type sourceSeed struct {
	SourceID            string         `json:"source_id"`
	CatalogKind         string         `json:"catalog_kind"`
	LifecycleState      string         `json:"lifecycle_state"`
	Domain              string         `json:"domain"`
	DomainFamily        string         `json:"domain_family"`
	SourceClass         string         `json:"source_class"`
	Entrypoints         []string       `json:"entrypoints"`
	AuthMode            string         `json:"auth_mode"`
	AuthConfig          map[string]any `json:"auth_config_json"`
	TransportType       string         `json:"transport_type"`
	CrawlEnabled        bool           `json:"crawl_enabled"`
	AllowedHosts        []string       `json:"allowed_hosts"`
	FormatHint          string         `json:"format_hint"`
	RobotsPolicy        string         `json:"robots_policy"`
	RefreshStrategy     string         `json:"refresh_strategy"`
	CrawlStrategy       string         `json:"crawl_strategy"`
	CrawlConfig         map[string]any `json:"crawl_config_json"`
	RequestsPerMinute   int            `json:"requests_per_minute"`
	BurstSize           int            `json:"burst_size"`
	RetentionClass      string         `json:"retention_class"`
	License             string         `json:"license"`
	TermsURL            string         `json:"terms_url"`
	AttributionRequired bool           `json:"attribution_required"`
	GeoScope            string         `json:"geo_scope"`
	Priority            int            `json:"priority"`
	ParserID            string         `json:"parser_id"`
	ParseConfig         map[string]any `json:"parse_config_json"`
	BronzeTable         string         `json:"bronze_table"`
	BronzeSchemaVersion int            `json:"bronze_schema_version"`
	PromoteProfile      string         `json:"promote_profile"`
	EntityTypes         []string       `json:"entity_types"`
	ExpectedPlaceTypes  []string       `json:"expected_place_types"`
	SupportsHistorical  bool           `json:"supports_historical"`
	SupportsDelta       bool           `json:"supports_delta"`
	BackfillPriority    int            `json:"backfill_priority"`
	ReviewStatus        string         `json:"review_status"`
	ReviewNotes         string         `json:"review_notes"`
	ConfidenceBaseline  float64        `json:"confidence_baseline"`
}

type s3Client struct {
	endpoint  *url.URL
	accessKey string
	secretKey string
	region    string
	client    *http.Client
}

func main() {
	ctx := context.Background()

	mode := "install"
	if len(os.Args) > 1 {
		mode = strings.TrimSpace(os.Args[1])
	}

	if mode == "compile-catalog" {
		if err := compileCatalogArtifact(); err != nil {
			log.Fatal(err)
		}
		return
	}

	cfg, err := loadConfig()
	if err != nil {
		log.Fatal(err)
	}

	switch mode {
	case "", "install":
		if err := install(ctx, cfg); err != nil {
			log.Fatal(err)
		}
	case "verify":
		if err := verify(ctx, cfg); err != nil {
			log.Fatal(err)
		}
	case "help", "-h", "--help":
		fmt.Fprintln(os.Stdout, "Usage: bootstrap [verify|compile-catalog]")
	default:
		log.Fatalf("unknown bootstrap mode %q", mode)
	}
}

func compileCatalogArtifact() error {
	catalogPath := getenv("SOURCE_CATALOG_PATH", defaultCatalogPath)
	registryPath := getenv("SOURCE_REGISTRY_PATH", defaultRegistryPath)
	compiledPath := getenv("SOURCE_CATALOG_COMPILED_PATH", defaultSeedPath)
	bronzeMigrationPath := getenv("SOURCE_BRONZE_MIGRATION_PATH", defaultBronzeMigrationPath)

	compiled, err := compileSourceCatalog(catalogPath, registryPath)
	if err != nil {
		return fmt.Errorf("compile source catalog: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(compiledPath), 0o755); err != nil {
		return fmt.Errorf("create compiled catalog dir: %w", err)
	}
	f, err := os.Create(compiledPath)
	if err != nil {
		return fmt.Errorf("create compiled source catalog: %w", err)
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)
	if err := enc.Encode(compiled); err != nil {
		return fmt.Errorf("write compiled source catalog: %w", err)
	}
	migrationSQL, err := renderSourceBronzeMigration(compiled)
	if err != nil {
		return fmt.Errorf("render source bronze migration: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(bronzeMigrationPath), 0o755); err != nil {
		return fmt.Errorf("create source bronze migration dir: %w", err)
	}
	if err := os.WriteFile(bronzeMigrationPath, []byte(migrationSQL), 0o644); err != nil {
		return fmt.Errorf("write source bronze migration: %w", err)
	}
	log.Printf("compiled source catalog: entries=%d runnable=%d bronze_manifest=%d -> %s", len(compiled.Catalog.Entries), len(compiled.RunnableSeeds), len(compiled.BronzeDDLManifest), compiledPath)
	log.Printf("compiled source bronze migration: tables=%d -> %s", len(compiled.BronzeDDLManifest)-1, bronzeMigrationPath)
	return nil
}

func loadConfig() (config, error) {
	buckets := splitCSV(getenv("MINIO_BUCKETS", defaultBuckets))
	backupBucket := getenv("BACKUP_BUCKET", defaultBackupBucket)
	if !contains(buckets, backupBucket) {
		buckets = append(buckets, backupBucket)
	}
	stageBucket := strings.TrimSpace(getenv("STAGE_BUCKET", defaultStageBucket))
	if stageBucket != "" && !contains(buckets, stageBucket) {
		buckets = append(buckets, stageBucket)
	}

	endpoint, err := url.Parse(getenv("MINIO_ENDPOINT", defaultMinIOEndpoint))
	if err != nil {
		return config{}, fmt.Errorf("parse minio endpoint: %w", err)
	}
	if endpoint.Scheme == "" || endpoint.Host == "" {
		return config{}, fmt.Errorf("invalid MinIO endpoint %q", endpoint.String())
	}

	stageAssets := getenv("STAGE_ASSETS_DIR", defaultStageAssets)

	return config{
		MigrationDir:   getenv("MIGRATIONS_DIR", defaultMigrationDir),
		ClickHouseHTTP: getenv("CLICKHOUSE_HTTP_URL", defaultClickHouseURL),
		SeedPath:       getenv("SOURCE_REGISTRY_SEED", defaultSeedPath),
		ReadyMarker:    getenv("BOOTSTRAP_READY_MARKER", defaultReadyMarker),
		MinIOEndpoint:  endpoint.String(),
		MinIOAccessKey: getenv("MINIO_ACCESS_KEY", getenv("MINIO_ROOT_USER", "minio")),
		MinIOSecretKey: getenv("MINIO_SECRET_KEY", getenv("MINIO_ROOT_PASSWORD", "minio_change_me")),
		MinIORegion:    getenv("MINIO_REGION", defaultMinIORegion),
		Buckets:        buckets,
		BackupAssets:   getenv("BACKUP_ASSETS_DIR", defaultBackupDir),
		BackupBucket:   backupBucket,
		BackupPrefix:   strings.Trim(getenv("BACKUP_PREFIX", defaultBackupPrefix), "/"),
		StageAssets:    stageAssets,
		StageBucket:    stageBucket,
		APIClientsPath: getenv("API_BOOTSTRAP_KEYS_FILE", defaultAPIClients),
		Users: []clickhouseUser{
			{Name: getenv("CLICKHOUSE_BOOTSTRAP_USER", "svc_bootstrap"), Password: getenv("CLICKHOUSE_BOOTSTRAP_PASSWORD", "bootstrap_change_me"), Roles: []string{"osint_admin"}},
			{Name: getenv("CLICKHOUSE_API_USER", "svc_api"), Password: getenv("CLICKHOUSE_API_PASSWORD", "api_change_me"), Roles: []string{"osint_reader"}},
			{Name: getenv("CLICKHOUSE_CONTROL_PLANE_USER", "svc_control_plane"), Password: getenv("CLICKHOUSE_CONTROL_PLANE_PASSWORD", "control_plane_change_me"), Roles: []string{"osint_promote"}},
			{Name: getenv("CLICKHOUSE_WORKER_FETCH_USER", "svc_worker_fetch"), Password: getenv("CLICKHOUSE_WORKER_FETCH_PASSWORD", "worker_fetch_change_me"), Roles: []string{"osint_ingest"}},
			{Name: getenv("CLICKHOUSE_WORKER_PARSE_USER", "svc_worker_parse"), Password: getenv("CLICKHOUSE_WORKER_PARSE_PASSWORD", "worker_parse_change_me"), Roles: []string{"osint_ingest"}},
		},
	}, nil
}

func install(ctx context.Context, cfg config) error {
	runner := migrate.NewHTTPRunner(cfg.ClickHouseHTTP)
	minio, err := newS3Client(cfg)
	if err != nil {
		return err
	}

	if err := waitForDependencies(ctx, runner, minio); err != nil {
		return err
	}
	if err := ensureBuckets(ctx, minio, cfg.Buckets); err != nil {
		return err
	}
	if err := ensureDatabases(ctx, runner, logicalDatabases); err != nil {
		return err
	}
	if err := ensureRBAC(ctx, runner, cfg.Users); err != nil {
		return err
	}
	if err := runner.EnsureMigrationsTable(ctx); err != nil {
		return fmt.Errorf("ensure migration table: %w", err)
	}
	if err := runner.VerifyMigrationsTableContract(ctx); err != nil {
		return fmt.Errorf("verify migration ledger contract: %w", err)
	}
	if err := applyMigrations(ctx, runner, cfg.MigrationDir); err != nil {
		return err
	}
	if err := runner.VerifySchemaChangeRegistryContract(ctx); err != nil {
		return fmt.Errorf("verify schema change registry contract: %w", err)
	}
	if err := loadSourceCatalogGovernance(ctx, runner, cfg.SeedPath); err != nil {
		return fmt.Errorf("load source catalog governance: %w", err)
	}
	if err := loadSourceSeed(ctx, runner, cfg.SeedPath); err != nil {
		return fmt.Errorf("load source seed: %w", err)
	}
	if err := loadAPIClients(ctx, runner, cfg.APIClientsPath); err != nil {
		return fmt.Errorf("load api clients: %w", err)
	}
	if err := seedLiveSnapshot(ctx, runner); err != nil {
		return fmt.Errorf("seed live snapshot: %w", err)
	}
	if err := registerStageAssets(ctx, minio, cfg); err != nil {
		return err
	}
	if err := registerBackupAssets(ctx, minio, cfg); err != nil {
		return err
	}
	if err := writeReadyMarker(cfg.ReadyMarker); err != nil {
		return err
	}
	log.Println("bootstrap complete")
	return nil
}

func verify(ctx context.Context, cfg config) error {
	runner := migrate.NewHTTPRunner(cfg.ClickHouseHTTP)
	minio, err := newS3Client(cfg)
	if err != nil {
		return err
	}

	if err := waitForDependencies(ctx, runner, minio); err != nil {
		return err
	}
	if err := verifyBuckets(ctx, minio, cfg.Buckets); err != nil {
		return err
	}
	if err := verifyDatabases(ctx, runner, logicalDatabases); err != nil {
		return err
	}
	if err := verifyRBAC(ctx, runner, cfg.Users); err != nil {
		return err
	}
	if err := runner.VerifyMigrationsTableContract(ctx); err != nil {
		return fmt.Errorf("verify migration ledger contract: %w", err)
	}
	if err := runner.VerifySchemaChangeRegistryContract(ctx); err != nil {
		return fmt.Errorf("verify schema change registry contract: %w", err)
	}
	if err := verifyMinimumCount(ctx, runner, "SELECT count() FROM meta.schema_migrations FORMAT TabSeparated", 1, "meta.schema_migrations rows"); err != nil {
		return err
	}
	if err := verifyMinimumCount(ctx, runner, "SELECT count() FROM meta.source_registry FORMAT TabSeparated", 1, "meta.source_registry rows"); err != nil {
		return err
	}
	if err := verifyMinimumCount(ctx, runner, "SELECT count() FROM meta.source_catalog FORMAT TabSeparated", 1, "meta.source_catalog rows"); err != nil {
		return err
	}
	if err := verifyMinimumCount(ctx, runner, "SELECT count() FROM meta.api_clients WHERE enabled = 1 FORMAT TabSeparated", 1, "enabled meta.api_clients rows"); err != nil {
		return err
	}
	if err := verifyMinimumCount(ctx, runner, "SELECT count() FROM meta.source_family_template FORMAT TabSeparated", 1, "meta.source_family_template rows"); err != nil {
		return err
	}
	if err := verifyMinimumCount(ctx, runner, "SELECT count() FROM meta.discovery_probe FORMAT TabSeparated", 1, "meta.discovery_probe rows"); err != nil {
		return err
	}
	if err := verifyTableEngine(ctx, runner, "meta", "source_registry", "ReplacingMergeTree"); err != nil {
		return err
	}
	for _, table := range []string{"source_catalog", "source_family_template", "discovery_probe", "discovery_candidate", "source_generation_log"} {
		if err := verifyTableEngine(ctx, runner, "meta", table, "ReplacingMergeTree"); err != nil {
			return err
		}
	}
	if err := verifyTableEngine(ctx, runner, "meta", "schema_change_registry", "ReplacingMergeTree"); err != nil {
		return err
	}
	if err := verifyTableEngine(ctx, runner, "meta", "parser_registry", "ReplacingMergeTree"); err != nil {
		return err
	}
	if err := verifyTableEngine(ctx, runner, "meta", "metric_registry", "ReplacingMergeTree"); err != nil {
		return err
	}
	if err := verifyTableEngine(ctx, runner, "meta", "api_schema_registry", "ReplacingMergeTree"); err != nil {
		return err
	}
	if err := verifyTableEngine(ctx, runner, "meta", "api_clients", "ReplacingMergeTree"); err != nil {
		return err
	}
	for _, spec := range []struct {
		database string
		table    string
		columns  []string
	}{
		{database: "meta", table: "source_registry", columns: []string{"schema_version", "record_version", "api_contract_version", "updated_at", "requests_per_minute", "burst_size", "retention_class", "disabled_reason", "disabled_at", "disabled_by", "review_status", "review_notes", "auth_config_json", "backfill_priority", "attribution_required", "transport_type", "crawl_enabled", "allowed_hosts", "crawl_strategy", "crawl_config_json", "parse_config_json", "bronze_table", "bronze_schema_version", "promote_profile", "catalog_kind", "lifecycle_state"}},
		{database: "meta", table: "schema_change_registry", columns: []string{"migration_version", "migration_checksum", "schema_scope", "target_kind", "target_name", "diff_status", "diff_summary", "compatibility_status", "compatibility_notes", "approval_status", "approval_notes", "approval_ref", "approved_by", "approved_at", "summary", "schema_version", "record_version", "api_contract_version", "updated_at", "attrs", "evidence"}},
		{database: "meta", table: "source_catalog", columns: []string{"catalog_kind", "integration_archetype", "generator_kind", "runtime_source_id", "generator_relationships", "source_markdown_line", "source_markdown_path", "source_markdown_checksum", "review_status", "materialized_source_id", "schema_version", "record_version", "api_contract_version", "updated_at", "attrs", "evidence"}},
		{database: "meta", table: "source_family_template", columns: []string{"scope", "integration_archetype", "review_status_default", "generator_relationships", "review_status", "schema_version", "record_version", "api_contract_version", "updated_at", "attrs", "evidence"}},
		{database: "meta", table: "discovery_probe", columns: []string{"integration_archetype", "probe_patterns", "review_status", "schema_version", "record_version", "api_contract_version", "updated_at", "attrs", "evidence"}},
		{database: "meta", table: "discovery_candidate", columns: []string{"integration_archetype", "detected_platform", "review_status", "materialized_source_id", "schema_version", "record_version", "api_contract_version", "updated_at", "attrs", "evidence"}},
		{database: "meta", table: "source_generation_log", columns: []string{"generator_kind", "emitted_candidate_id", "emitted_source_id", "review_status", "schema_version", "record_version", "api_contract_version", "updated_at", "attrs", "evidence"}},
		{database: "meta", table: "parser_registry", columns: []string{"schema_version", "record_version", "api_contract_version", "updated_at"}},
		{database: "meta", table: "metric_registry", columns: []string{"schema_version", "record_version", "api_contract_version", "updated_at"}},
		{database: "meta", table: "api_schema_registry", columns: []string{"schema_version", "record_version", "api_contract_version", "updated_at"}},
		{database: "meta", table: "api_clients", columns: []string{"key_id", "name", "key_sha256", "scopes", "enabled", "expires_at", "schema_version", "record_version", "api_contract_version", "updated_at", "attrs", "evidence"}},
	} {
		for _, column := range spec.columns {
			if err := verifyColumnExists(ctx, runner, spec.database, spec.table, column); err != nil {
				return err
			}
		}
	}
	if err := verifyMinimumCount(ctx, runner, "SELECT count() FROM meta.schema_change_registry FORMAT TabSeparated", 1, "meta.schema_change_registry rows"); err != nil {
		return err
	}
	if err := verifyBackupAssets(ctx, minio, cfg); err != nil {
		return err
	}
	if err := verifyStageAssets(ctx, minio, cfg); err != nil {
		return err
	}
	if err := verifyBronzeCatalogParity(ctx, runner, cfg.SeedPath); err != nil {
		return err
	}
	if err := verifySourceSilverCoverageContract(ctx, runner); err != nil {
		return err
	}
	log.Println("bootstrap verify complete")
	return nil
}

const sourceSilverCoverageInScopePredicate = "catalog_kind='concrete' AND transport_type='http' AND bronze_table IS NOT NULL AND bronze_table != ''"

func sourceSilverCoverageRequiredColumns() []string {
	return []string{
		"source_id",
		"coverage_state",
		"routing_mode",
		"promote_profile",
		"terminal_kind",
		"terminal_destination",
		"last_bronze_at",
		"last_parse_at",
		"last_promote_at",
		"last_silver_at",
		"reason",
		"attrs",
		"updated_at",
	}
}

func sourceSilverCoverageRequiredStates() []string {
	return []string{
		"silver_landed",
		"silver_view_only",
		"blocked_missing_credential",
		"parsed_no_promotable_rows",
		"unresolved_only",
		"unsupported_profile",
	}
}

func sourceSilverCoverageRegistryDenominatorQuery() string {
	return fmt.Sprintf("SELECT countDistinct(source_id) FROM meta.source_registry FINAL WHERE %s FORMAT TabSeparated", sourceSilverCoverageInScopePredicate)
}

func sourceSilverCoverageDistinctSourcesQuery() string {
	return "SELECT countDistinct(source_id) FROM meta.source_silver_coverage FORMAT TabSeparated"
}

func sourceSilverCoverageDuplicateSourcesQuery() string {
	return "SELECT count() FROM (SELECT source_id FROM meta.source_silver_coverage GROUP BY source_id HAVING count() > 1) FORMAT TabSeparated"
}

func sourceSilverCoverageUnexpectedStatesQuery() string {
	quotedStates := make([]string, 0, len(sourceSilverCoverageRequiredStates()))
	for _, state := range sourceSilverCoverageRequiredStates() {
		quotedStates = append(quotedStates, "'"+esc(state)+"'")
	}
	return "SELECT count() FROM meta.source_silver_coverage WHERE coverage_state NOT IN (" + strings.Join(quotedStates, ",") + ") FORMAT TabSeparated"
}

func sourceSilverCoverageMissingRoutingMetadataQuery() string {
	return "SELECT count() FROM meta.source_silver_coverage WHERE routing_mode = '' OR promote_profile = '' OR terminal_destination = '' FORMAT TabSeparated"
}

func verifySourceSilverCoverageContract(ctx context.Context, runner *migrate.HTTPRunner) error {
	if err := verifyTableEngine(ctx, runner, "meta", "source_silver_coverage", "View"); err != nil {
		return err
	}
	for _, column := range sourceSilverCoverageRequiredColumns() {
		if err := verifyColumnExists(ctx, runner, "meta", "source_silver_coverage", column); err != nil {
			return err
		}
	}

	registryCount, err := queryCount(ctx, runner, sourceSilverCoverageRegistryDenominatorQuery())
	if err != nil {
		return fmt.Errorf("count source_silver_coverage denominator: %w", err)
	}
	coverageCount, err := queryCount(ctx, runner, sourceSilverCoverageDistinctSourcesQuery())
	if err != nil {
		return fmt.Errorf("count source_silver_coverage rows: %w", err)
	}
	if coverageCount != registryCount {
		return fmt.Errorf("source_silver_coverage denominator mismatch: registry=%d coverage=%d", registryCount, coverageCount)
	}
	log.Printf("verified source_silver_coverage denominator parity: %d", coverageCount)

	duplicateCount, err := queryCount(ctx, runner, sourceSilverCoverageDuplicateSourcesQuery())
	if err != nil {
		return fmt.Errorf("count duplicate source_silver_coverage source ids: %w", err)
	}
	if duplicateCount != 0 {
		return fmt.Errorf("source_silver_coverage has %d duplicate source_id rows", duplicateCount)
	}
	log.Printf("verified source_silver_coverage source_id key uniqueness")

	unexpectedStateCount, err := queryCount(ctx, runner, sourceSilverCoverageUnexpectedStatesQuery())
	if err != nil {
		return fmt.Errorf("count unexpected source_silver_coverage states: %w", err)
	}
	if unexpectedStateCount != 0 {
		return fmt.Errorf("source_silver_coverage exposes %d unexpected coverage states", unexpectedStateCount)
	}
	log.Printf("verified source_silver_coverage states: %s", strings.Join(sourceSilverCoverageRequiredStates(), ", "))

	missingRoutingMetadataCount, err := queryCount(ctx, runner, sourceSilverCoverageMissingRoutingMetadataQuery())
	if err != nil {
		return fmt.Errorf("count source_silver_coverage rows missing routing metadata: %w", err)
	}
	if missingRoutingMetadataCount != 0 {
		return fmt.Errorf("source_silver_coverage has %d rows missing routing metadata", missingRoutingMetadataCount)
	}
	log.Printf("verified source_silver_coverage routing metadata completeness")

	return nil
}

func verifyBronzeCatalogParity(ctx context.Context, runner *migrate.HTTPRunner, seedPath string) error {
	compiledCandidate, err := looksLikeCompiledSourceCatalog(seedPath)
	if err != nil {
		return fmt.Errorf("inspect source catalog seed shape: %w", err)
	}
	if !compiledCandidate {
		return nil
	}
	compiled, err := loadCompiledSourceCatalog(seedPath)
	if err != nil {
		return fmt.Errorf("load compiled source catalog: %w", err)
	}
	expectedBronzeTables := manifestBronzeTableSet(compiled)
	expectedCount := len(compiled.BronzeDDLManifest)
	if expectedCount == 0 {
		return fmt.Errorf("compiled source catalog bronze manifest is empty")
	}
	expectedRegistryBronzeTables := runnableSeedBronzeTableSet(compiled)
	expectedRegistryCount := len(expectedRegistryBronzeTables)

	liveBronzeCount, err := queryCount(ctx, runner, "SELECT count() FROM system.tables WHERE database='bronze' AND name LIKE 'src_%' FORMAT TabSeparated")
	if err != nil {
		return fmt.Errorf("count live bronze tables: %w", err)
	}
	if liveBronzeCount != expectedCount {
		return fmt.Errorf("bronze manifest/live table mismatch: manifest=%d live=%d", expectedCount, liveBronzeCount)
	}
	liveBronzeRows, err := runner.Query(ctx, "SELECT concat(database, '.', name) FROM system.tables WHERE database='bronze' AND name LIKE 'src_%' FORMAT TabSeparated")
	if err != nil {
		return fmt.Errorf("fetch live bronze table names: %w", err)
	}
	liveBronzeTables := parseLineSet(liveBronzeRows)
	if err := verifySetEquality("manifest", expectedBronzeTables, "live", liveBronzeTables); err != nil {
		return fmt.Errorf("bronze manifest/live set mismatch: %w", err)
	}
	log.Printf("verified bronze manifest/live parity: %d", expectedCount)

	registryBronzeCount, err := queryCount(ctx, runner, "SELECT countDistinct(bronze_table) FROM meta.source_registry FINAL WHERE catalog_kind='concrete' AND transport_type='http' AND bronze_table IS NOT NULL AND bronze_table != '' FORMAT TabSeparated")
	if err != nil {
		return fmt.Errorf("count registry bronze tables: %w", err)
	}
	if registryBronzeCount != expectedRegistryCount {
		return fmt.Errorf("bronze runnable/registry mismatch: runnable=%d registry=%d", expectedRegistryCount, registryBronzeCount)
	}
	registryBronzeRows, err := runner.Query(ctx, "SELECT bronze_table FROM meta.source_registry FINAL WHERE catalog_kind='concrete' AND transport_type='http' AND bronze_table IS NOT NULL AND bronze_table != '' FORMAT TabSeparated")
	if err != nil {
		return fmt.Errorf("fetch registry bronze table names: %w", err)
	}
	registryBronzeTables := parseLineSet(registryBronzeRows)
	if err := verifySetEquality("runnable", expectedRegistryBronzeTables, "registry", registryBronzeTables); err != nil {
		return fmt.Errorf("bronze runnable/registry set mismatch: %w", err)
	}
	log.Printf("verified bronze runnable/registry parity: %d", expectedRegistryCount)

	missingRegistryRefs, err := queryCount(ctx, runner, `SELECT count()
FROM meta.source_registry FINAL
WHERE catalog_kind='concrete'
  AND transport_type='http'
  AND bronze_table IS NOT NULL
  AND bronze_table != ''
  AND bronze_table NOT IN (
    SELECT concat(database, '.', name)
    FROM system.tables
    WHERE database='bronze' AND name LIKE 'src_%'
  )
FORMAT TabSeparated`)
	if err != nil {
		return fmt.Errorf("count missing registry bronze references: %w", err)
	}
	if missingRegistryRefs != 0 {
		return fmt.Errorf("registry references %d missing bronze tables", missingRegistryRefs)
	}
	log.Printf("verified registry bronze references: none missing")

	return nil
}

func manifestBronzeTableSet(compiled compiledSourceCatalog) map[string]struct{} {
	tables := make(map[string]struct{}, len(compiled.BronzeDDLManifest))
	for _, row := range compiled.BronzeDDLManifest {
		table := strings.TrimSpace(row.BronzeTable)
		if table == "" {
			continue
		}
		tables[table] = struct{}{}
	}
	return tables
}

func runnableSeedBronzeTableSet(compiled compiledSourceCatalog) map[string]struct{} {
	tables := make(map[string]struct{}, len(compiled.RunnableSeeds))
	for _, seed := range compiled.RunnableSeeds {
		table := strings.TrimSpace(seed.BronzeTable)
		if table == "" {
			continue
		}
		tables[table] = struct{}{}
	}
	return tables
}

func queryCount(ctx context.Context, runner *migrate.HTTPRunner, query string) (int, error) {
	out, err := runner.Query(ctx, query)
	if err != nil {
		return 0, err
	}
	count, err := strconv.Atoi(strings.TrimSpace(out))
	if err != nil {
		return 0, fmt.Errorf("parse count %q: %w", strings.TrimSpace(out), err)
	}
	return count, nil
}

func parseLineSet(raw string) map[string]struct{} {
	set := map[string]struct{}{}
	for _, line := range strings.Split(strings.TrimSpace(raw), "\n") {
		value := strings.TrimSpace(line)
		if value == "" {
			continue
		}
		set[value] = struct{}{}
	}
	return set
}

func verifySetEquality(labelLeft string, left map[string]struct{}, labelRight string, right map[string]struct{}) error {
	leftOnly := make([]string, 0)
	for value := range left {
		if _, ok := right[value]; !ok {
			leftOnly = append(leftOnly, value)
		}
	}
	rightOnly := make([]string, 0)
	for value := range right {
		if _, ok := left[value]; !ok {
			rightOnly = append(rightOnly, value)
		}
	}
	sort.Strings(leftOnly)
	sort.Strings(rightOnly)
	if len(leftOnly) == 0 && len(rightOnly) == 0 {
		return nil
	}
	preview := 5
	if len(leftOnly) > preview {
		leftOnly = leftOnly[:preview]
	}
	if len(rightOnly) > preview {
		rightOnly = rightOnly[:preview]
	}
	return fmt.Errorf("%s_only=%v %s_only=%v", labelLeft, leftOnly, labelRight, rightOnly)
}

func waitForDependencies(ctx context.Context, runner *migrate.HTTPRunner, minio *s3Client) error {
	if err := retry(ctx, 20, 2*time.Second, func() error {
		_, err := runner.Query(ctx, "SELECT 1 FORMAT TabSeparated")
		return err
	}); err != nil {
		return fmt.Errorf("wait for ClickHouse: %w", err)
	}
	if err := retry(ctx, 20, 2*time.Second, func() error {
		return minio.Ping(ctx)
	}); err != nil {
		return fmt.Errorf("wait for MinIO: %w", err)
	}
	return nil
}

func ensureBuckets(ctx context.Context, client *s3Client, buckets []string) error {
	for _, bucket := range buckets {
		exists, err := client.BucketExists(ctx, bucket)
		if err != nil {
			return fmt.Errorf("check bucket %s: %w", bucket, err)
		}
		if exists {
			log.Printf("bucket already exists: %s", bucket)
			continue
		}
		if err := client.CreateBucket(ctx, bucket); err != nil {
			return fmt.Errorf("create bucket %s: %w", bucket, err)
		}
		log.Printf("created bucket: %s", bucket)
	}
	return nil
}

func verifyBuckets(ctx context.Context, client *s3Client, buckets []string) error {
	for _, bucket := range buckets {
		exists, err := client.BucketExists(ctx, bucket)
		if err != nil {
			return fmt.Errorf("verify bucket %s: %w", bucket, err)
		}
		if !exists {
			return fmt.Errorf("bucket missing: %s", bucket)
		}
		log.Printf("verified bucket: %s", bucket)
	}
	return nil
}

func ensureDatabases(ctx context.Context, runner *migrate.HTTPRunner, databases []string) error {
	var sql strings.Builder
	for _, database := range databases {
		fmt.Fprintf(&sql, "CREATE DATABASE IF NOT EXISTS %s;\n", database)
	}
	if err := runner.ApplySQL(ctx, sql.String()); err != nil {
		return fmt.Errorf("ensure databases: %w", err)
	}
	return nil
}

func verifyDatabases(ctx context.Context, runner *migrate.HTTPRunner, databases []string) error {
	for _, database := range databases {
		query := fmt.Sprintf("SELECT count() FROM system.databases WHERE name = '%s' FORMAT TabSeparated", esc(database))
		if err := verifyMinimumCount(ctx, runner, query, 1, "database "+database); err != nil {
			return err
		}
	}
	return nil
}

func ensureRBAC(ctx context.Context, runner *migrate.HTTPRunner, users []clickhouseUser) error {
	for _, role := range roleSpecs {
		existsQuery := fmt.Sprintf("SELECT count() FROM system.roles WHERE name = '%s' FORMAT TabSeparated", esc(role.Name))
		out, err := runner.Query(ctx, existsQuery)
		if err != nil {
			return fmt.Errorf("check role %s existence: %w", role.Name, err)
		}
		exists, err := strconv.Atoi(strings.TrimSpace(out))
		if err != nil {
			return fmt.Errorf("parse role %s existence %q: %w", role.Name, strings.TrimSpace(out), err)
		}
		if exists == 0 {
			if err := runner.ApplySQL(ctx, fmt.Sprintf("CREATE ROLE IF NOT EXISTS %s", role.Name)); err != nil {
				return fmt.Errorf("create role %s: %w", role.Name, err)
			}
		}
		grants, err := runner.Query(ctx, fmt.Sprintf("SHOW GRANTS FOR %s", role.Name))
		if err != nil {
			return fmt.Errorf("show grants for role %s: %w", role.Name, err)
		}
		for _, grant := range role.Grants {
			if hasGrant(grants, grant) {
				continue
			}
			if err := runner.ApplySQL(ctx, grant); err != nil {
				return fmt.Errorf("grant role privilege %s: %w", role.Name, err)
			}
			grants += "\n" + grant
		}
		for _, revoke := range role.Revokes {
			if err := runner.ApplySQL(ctx, revoke); err != nil && !strings.Contains(strings.ToUpper(err.Error()), "NOT GRANTED") {
				return fmt.Errorf("revoke role privilege %s: %w", role.Name, err)
			}
		}
	}

	for _, user := range users {
		existsQuery := fmt.Sprintf("SELECT count() FROM system.users WHERE name = '%s' FORMAT TabSeparated", esc(user.Name))
		out, err := runner.Query(ctx, existsQuery)
		if err != nil {
			return fmt.Errorf("check user %s existence: %w", user.Name, err)
		}
		exists, err := strconv.Atoi(strings.TrimSpace(out))
		if err != nil {
			return fmt.Errorf("parse user %s existence %q: %w", user.Name, strings.TrimSpace(out), err)
		}
		if exists > 0 {
			continue
		}
		createUser := fmt.Sprintf("CREATE USER IF NOT EXISTS %s IDENTIFIED BY '%s'", user.Name, esc(user.Password))
		if err := runner.ApplySQL(ctx, createUser); err != nil {
			return fmt.Errorf("create user %s: %w", user.Name, err)
		}
		grants, err := runner.Query(ctx, fmt.Sprintf("SHOW GRANTS FOR %s", user.Name))
		if err != nil {
			return fmt.Errorf("show grants for user %s: %w", user.Name, err)
		}
		for _, role := range user.Roles {
			grantRole := fmt.Sprintf("GRANT %s TO %s", role, user.Name)
			if hasGrant(grants, grantRole) {
				continue
			}
			if err := runner.ApplySQL(ctx, grantRole); err != nil {
				return fmt.Errorf("grant role %s to user %s: %w", role, user.Name, err)
			}
			grants += "\n" + grantRole
		}
		for _, role := range managedRoleNames() {
			if contains(user.Roles, role) {
				continue
			}
			revokeRole := fmt.Sprintf("REVOKE %s FROM %s", role, user.Name)
			if err := runner.ApplySQL(ctx, revokeRole); err != nil && !strings.Contains(strings.ToUpper(err.Error()), "NOT GRANTED") {
				return fmt.Errorf("revoke role %s from user %s: %w", role, user.Name, err)
			}
		}
	}
	return nil
}

func verifyRBAC(ctx context.Context, runner *migrate.HTTPRunner, users []clickhouseUser) error {
	for _, role := range roleSpecs {
		query := fmt.Sprintf("SELECT count() FROM system.roles WHERE name = '%s' FORMAT TabSeparated", esc(role.Name))
		if err := verifyMinimumCount(ctx, runner, query, 1, "role "+role.Name); err != nil {
			return err
		}
		if err := verifyRolePrivileges(ctx, runner, role); err != nil {
			return err
		}
	}
	for _, user := range users {
		query := fmt.Sprintf("SELECT count() FROM system.users WHERE name = '%s' FORMAT TabSeparated", esc(user.Name))
		if err := verifyMinimumCount(ctx, runner, query, 1, "user "+user.Name); err != nil {
			return err
		}
		for _, role := range user.Roles {
			roleGrantQuery := fmt.Sprintf("SELECT count() FROM system.role_grants WHERE user_name = '%s' AND granted_role_name = '%s' FORMAT TabSeparated", esc(user.Name), esc(role))
			if err := verifyMinimumCount(ctx, runner, roleGrantQuery, 1, fmt.Sprintf("role grant %s -> %s", role, user.Name)); err != nil {
				return err
			}
		}
	}
	return nil
}

func verifyRolePrivileges(ctx context.Context, runner *migrate.HTTPRunner, role clickhouseRole) error {
	for _, grant := range role.Grants {
		privileges, database, table := parseGrantExpectation(grant)
		if len(privileges) == 0 {
			continue
		}
		for _, privilege := range privileges {
			accessType := systemGrantAccessType(privilege)
			query := ""
			if database == "*" && table == "*" {
				query = fmt.Sprintf("SELECT count() FROM system.grants WHERE role_name = '%s' AND access_type = '%s' AND isNull(database) AND isNull(table) FORMAT TabSeparated", esc(role.Name), esc(accessType))
			} else if table == "*" {
				query = fmt.Sprintf("SELECT count() FROM system.grants WHERE role_name = '%s' AND access_type = '%s' AND database = '%s' AND isNull(table) FORMAT TabSeparated", esc(role.Name), esc(accessType), esc(database))
			} else {
				query = fmt.Sprintf("SELECT count() FROM system.grants WHERE role_name = '%s' AND access_type = '%s' AND database = '%s' AND table = '%s' FORMAT TabSeparated", esc(role.Name), esc(accessType), esc(database), esc(table))
			}
			if err := verifyMinimumCount(ctx, runner, query, 1, fmt.Sprintf("grant %s on %s.%s for %s", privilege, database, table, role.Name)); err != nil {
				return err
			}
		}
	}
	return nil
}

func verifyMinimumCount(ctx context.Context, runner *migrate.HTTPRunner, query string, min int, label string) error {
	out, err := runner.Query(ctx, query)
	if err != nil {
		return fmt.Errorf("query %s: %w", label, err)
	}
	count, err := strconv.Atoi(strings.TrimSpace(out))
	if err != nil {
		return fmt.Errorf("parse %s count %q: %w", label, strings.TrimSpace(out), err)
	}
	if count < min {
		return fmt.Errorf("expected %s >= %d, got %d", label, min, count)
	}
	log.Printf("verified %s: %d", label, count)
	return nil
}

func verifyTableEngine(ctx context.Context, runner *migrate.HTTPRunner, database, table, engine string) error {
	query := fmt.Sprintf("SELECT count() FROM system.tables WHERE database = '%s' AND name = '%s' AND engine = '%s' FORMAT TabSeparated", esc(database), esc(table), esc(engine))
	return verifyMinimumCount(ctx, runner, query, 1, fmt.Sprintf("table %s.%s engine %s", database, table, engine))
}

func verifyColumnExists(ctx context.Context, runner *migrate.HTTPRunner, database, table, column string) error {
	query := fmt.Sprintf("SELECT count() FROM system.columns WHERE database = '%s' AND table = '%s' AND name = '%s' FORMAT TabSeparated", esc(database), esc(table), esc(column))
	return verifyMinimumCount(ctx, runner, query, 1, fmt.Sprintf("column %s.%s.%s", database, table, column))
}

func registerBackupAssets(ctx context.Context, client *s3Client, cfg config) error {
	assets, err := backupAssetFiles(cfg.BackupAssets)
	if err != nil {
		return fmt.Errorf("collect backup assets: %w", err)
	}
	for _, asset := range assets {
		body, err := os.ReadFile(asset.Path)
		if err != nil {
			return fmt.Errorf("read backup asset %s: %w", asset.Path, err)
		}
		key := objectKey(cfg.BackupPrefix, asset.Key)
		if err := client.PutObject(ctx, cfg.BackupBucket, key, body, asset.ContentType); err != nil {
			return fmt.Errorf("upload backup asset %s: %w", key, err)
		}
		log.Printf("registered backup asset: s3://%s/%s", cfg.BackupBucket, key)
	}
	return nil
}

func registerStageAssets(ctx context.Context, client *s3Client, cfg config) error {
	if cfg.StageBucket == "" || cfg.StageAssets == "" {
		return nil
	}
	assets, err := stageAssetFiles(cfg.StageAssets)
	if err != nil {
		return fmt.Errorf("collect stage assets: %w", err)
	}
	for _, asset := range assets {
		body, err := os.ReadFile(asset.Path)
		if err != nil {
			return fmt.Errorf("read stage asset %s: %w", asset.Path, err)
		}
		if err := client.PutObject(ctx, cfg.StageBucket, asset.Key, body, asset.ContentType); err != nil {
			return fmt.Errorf("upload stage asset %s: %w", asset.Key, err)
		}
		log.Printf("registered stage asset: s3://%s/%s", cfg.StageBucket, asset.Key)
	}
	return nil
}

func verifyBackupAssets(ctx context.Context, client *s3Client, cfg config) error {
	assets, err := backupAssetFiles(cfg.BackupAssets)
	if err != nil {
		return fmt.Errorf("collect backup assets: %w", err)
	}
	for _, asset := range assets {
		key := objectKey(cfg.BackupPrefix, asset.Key)
		exists, err := client.ObjectExists(ctx, cfg.BackupBucket, key)
		if err != nil {
			return fmt.Errorf("verify backup asset %s: %w", key, err)
		}
		if !exists {
			return fmt.Errorf("backup asset missing: s3://%s/%s", cfg.BackupBucket, key)
		}
		log.Printf("verified backup asset: s3://%s/%s", cfg.BackupBucket, key)
	}
	return nil
}

func verifyStageAssets(ctx context.Context, client *s3Client, cfg config) error {
	if cfg.StageBucket == "" || cfg.StageAssets == "" {
		return nil
	}
	assets, err := stageAssetFiles(cfg.StageAssets)
	if err != nil {
		return fmt.Errorf("collect stage assets: %w", err)
	}
	for _, asset := range assets {
		exists, err := client.ObjectExists(ctx, cfg.StageBucket, asset.Key)
		if err != nil {
			return fmt.Errorf("verify stage asset %s: %w", asset.Key, err)
		}
		if !exists {
			return fmt.Errorf("stage asset missing: s3://%s/%s", cfg.StageBucket, asset.Key)
		}
		log.Printf("verified stage asset: s3://%s/%s", cfg.StageBucket, asset.Key)
	}
	return nil
}

func stageAssetFiles(root string) ([]backupAsset, error) {
	return backupAssetFiles(root)
}

func writeReadyMarker(path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create ready marker dir: %w", err)
	}
	data := []byte(fmt.Sprintf("ready %s\n", time.Now().UTC().Format(time.RFC3339)))
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("write ready marker: %w", err)
	}
	return nil
}

func newS3Client(cfg config) (*s3Client, error) {
	endpoint, err := url.Parse(cfg.MinIOEndpoint)
	if err != nil {
		return nil, fmt.Errorf("parse MinIO endpoint: %w", err)
	}
	return &s3Client{
		endpoint:  endpoint,
		accessKey: cfg.MinIOAccessKey,
		secretKey: cfg.MinIOSecretKey,
		region:    cfg.MinIORegion,
		client:    &http.Client{Timeout: 30 * time.Second},
	}, nil
}

func (c *s3Client) Ping(ctx context.Context) error {
	resp, body, err := c.do(ctx, http.MethodGet, "/", nil, "")
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

func (c *s3Client) BucketExists(ctx context.Context, bucket string) (bool, error) {
	resp, body, err := c.do(ctx, http.MethodHead, "/"+bucket, nil, "")
	if err != nil {
		return false, err
	}
	switch resp.StatusCode {
	case http.StatusOK:
		return true, nil
	case http.StatusNotFound:
		return false, nil
	default:
		return false, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
}

func (c *s3Client) CreateBucket(ctx context.Context, bucket string) error {
	resp, body, err := c.do(ctx, http.MethodPut, "/"+bucket, nil, "")
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

func (c *s3Client) PutObject(ctx context.Context, bucket, key string, body []byte, contentType string) error {
	resp, respBody, err := c.do(ctx, http.MethodPut, "/"+bucket+"/"+key, body, contentType)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	return nil
}

func (c *s3Client) ObjectExists(ctx context.Context, bucket, key string) (bool, error) {
	resp, body, err := c.do(ctx, http.MethodHead, "/"+bucket+"/"+key, nil, "")
	if err != nil {
		return false, err
	}
	switch resp.StatusCode {
	case http.StatusOK:
		return true, nil
	case http.StatusNotFound:
		return false, nil
	default:
		return false, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
}

func (c *s3Client) do(ctx context.Context, method, rawPath string, body []byte, contentType string) (*http.Response, []byte, error) {
	canonicalPath := escapePath(joinPath(c.endpoint.Path, rawPath))
	requestURL := *c.endpoint
	requestURL.Path = canonicalPath
	requestURL.RawPath = canonicalPath
	requestURL.RawQuery = ""

	payloadHash := sum(body)
	requestTime := time.Now().UTC()
	amzDate := requestTime.Format("20060102T150405Z")
	dateStamp := requestTime.Format("20060102")

	req, err := http.NewRequestWithContext(ctx, method, requestURL.String(), bytes.NewReader(body))
	if err != nil {
		return nil, nil, err
	}
	req.Host = c.endpoint.Host
	req.Header.Set("x-amz-content-sha256", payloadHash)
	req.Header.Set("x-amz-date", amzDate)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}

	canonicalHeaders := map[string]string{
		"host":                 req.Host,
		"x-amz-content-sha256": payloadHash,
		"x-amz-date":           amzDate,
	}
	if contentType != "" {
		canonicalHeaders["content-type"] = contentType
	}
	signedHeaders := sortedKeys(canonicalHeaders)
	var headerBuilder strings.Builder
	for _, name := range signedHeaders {
		headerBuilder.WriteString(name)
		headerBuilder.WriteByte(':')
		headerBuilder.WriteString(strings.TrimSpace(canonicalHeaders[name]))
		headerBuilder.WriteByte('\n')
	}
	canonicalRequest := strings.Join([]string{
		method,
		canonicalPath,
		"",
		headerBuilder.String(),
		strings.Join(signedHeaders, ";"),
		payloadHash,
	}, "\n")
	credentialScope := strings.Join([]string{dateStamp, c.region, "s3", "aws4_request"}, "/")
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		credentialScope,
		sum([]byte(canonicalRequest)),
	}, "\n")
	signature := hex.EncodeToString(signV4(c.secretKey, dateStamp, c.region, "s3", stringToSign))
	authorization := fmt.Sprintf(
		"AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		c.accessKey,
		credentialScope,
		strings.Join(signedHeaders, ";"),
		signature,
	)
	req.Header.Set("Authorization", authorization)

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		if method == http.MethodHead {
			io.Copy(io.Discard, resp.Body)
		}
	}()
	respBody, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		resp.Body.Close()
		return nil, nil, readErr
	}
	resp.Body.Close()
	return resp, respBody, nil
}

func signV4(secret, dateStamp, region, service, stringToSign string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secret), dateStamp)
	kRegion := hmacSHA256(kDate, region)
	kService := hmacSHA256(kRegion, service)
	kSigning := hmacSHA256(kService, "aws4_request")
	return hmacSHA256(kSigning, stringToSign)
}

func hmacSHA256(key []byte, value string) []byte {
	h := hmac.New(sha256.New, key)
	_, _ = h.Write([]byte(value))
	return h.Sum(nil)
}

type backupAsset struct {
	Path        string
	Key         string
	ContentType string
}

func backupAssetFiles(root string) ([]backupAsset, error) {
	assets := make([]backupAsset, 0, 8)
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		assets = append(assets, backupAsset{
			Path:        path,
			Key:         filepath.ToSlash(rel),
			ContentType: detectContentType(path),
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(assets, func(i, j int) bool {
		return assets[i].Key < assets[j].Key
	})
	return assets, nil
}

func detectContentType(path string) string {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".json":
		return "application/json"
	case ".sql":
		return "application/sql"
	default:
		return "application/octet-stream"
	}
}

func objectKey(parts ...string) string {
	clean := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.Trim(part, "/")
		if trimmed != "" {
			clean = append(clean, trimmed)
		}
	}
	return strings.Join(clean, "/")
}

func joinPath(basePath, rawPath string) string {
	basePath = strings.TrimRight(basePath, "/")
	rawPath = "/" + strings.TrimLeft(rawPath, "/")
	if basePath == "" {
		return rawPath
	}
	return basePath + rawPath
}

func escapePath(path string) string {
	if path == "" {
		return "/"
	}
	parts := strings.Split(path, "/")
	for i, part := range parts {
		parts[i] = url.PathEscape(part)
	}
	escaped := strings.Join(parts, "/")
	if !strings.HasPrefix(escaped, "/") {
		escaped = "/" + escaped
	}
	if strings.HasSuffix(path, "/") && !strings.HasSuffix(escaped, "/") {
		escaped += "/"
	}
	return escaped
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func retry(ctx context.Context, attempts int, delay time.Duration, fn func() error) error {
	var lastErr error
	for i := 0; i < attempts; i++ {
		if err := fn(); err == nil {
			return nil
		} else {
			lastErr = err
		}
		if i == attempts-1 {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
	return lastErr
}

func applyMigrations(ctx context.Context, runner *migrate.HTTPRunner, migrationDir string) error {
	files, err := filepath.Glob(filepath.Join(migrationDir, "*.sql"))
	if err != nil {
		return err
	}
	sort.Strings(files)

	for _, f := range files {
		name := filepath.Base(f)
		b, err := os.ReadFile(f)
		if err != nil {
			return fmt.Errorf("read migration %s: %w", name, err)
		}
		checksum := sum(b)
		metadata := parseMigrationMetadata(b)
		applied, err := runner.CheckAppliedMigration(ctx, name, checksum)
		if err != nil {
			return fmt.Errorf("check applied %s: %w", name, err)
		}
		if applied {
			log.Printf("migration already applied: %s", name)
			continue
		}
		if err := runner.ApplySQL(ctx, string(b)); err != nil {
			_ = runner.Record(ctx, name, checksum, false, err.Error())
			return fmt.Errorf("apply migration %s: %w", name, err)
		}
		if err := recordSchemaChangeMetadata(ctx, runner, name, checksum, metadata); err != nil {
			_ = runner.Record(ctx, name, checksum, false, err.Error())
			return fmt.Errorf("record schema change metadata %s: %w", name, err)
		}
		if err := runner.Record(ctx, name, checksum, true, "applied"); err != nil {
			return fmt.Errorf("record migration %s: %w", name, err)
		}
		log.Printf("applied migration: %s", name)
	}
	return nil
}

func hasGrant(existing, expected string) bool {
	return strings.Contains(normalizeGrantText(existing), normalizeGrantText(expected))
}

func normalizeGrantText(s string) string {
	return strings.Join(strings.Fields(strings.ToUpper(s)), " ")
}

func parseGrantExpectation(grant string) ([]string, string, string) {
	normalized := normalizeGrantText(grant)
	parts := strings.SplitN(normalized, " ON ", 2)
	if len(parts) != 2 {
		return nil, "", ""
	}
	privilegePart := strings.TrimPrefix(parts[0], "GRANT ")
	objectPart := parts[1]
	objectPart = strings.SplitN(objectPart, " TO ", 2)[0]
	privilegeItems := splitTopLevelCSV(privilegePart)
	privileges := make([]string, 0, len(privilegeItems))
	for _, privilege := range privilegeItems {
		trimmed := strings.TrimSpace(privilege)
		if trimmed != "" {
			privileges = append(privileges, trimmed)
		}
	}
	database := "*"
	table := "*"
	if objectPart != "*.*" {
		objectParts := strings.SplitN(objectPart, ".", 2)
		if len(objectParts) == 2 {
			database = strings.TrimSpace(strings.ToLower(objectParts[0]))
			table = strings.TrimSpace(strings.ToLower(objectParts[1]))
		}
	}
	for i, privilege := range privileges {
		privileges[i] = strings.TrimSpace(strings.ToUpper(privilege))
	}
	return privileges, database, table
}

func systemGrantAccessType(privilege string) string {
	normalized := strings.TrimSpace(strings.ToUpper(privilege))
	if strings.HasPrefix(normalized, "ALTER UPDATE(") {
		return "ALTER UPDATE"
	}
	if normalized == "DICTGET" {
		return "dictGet"
	}
	return privilege
}

func splitTopLevelCSV(s string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	parts := make([]string, 0, 4)
	depth := 0
	start := 0
	for i, r := range s {
		switch r {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		case ',':
			if depth == 0 {
				part := strings.TrimSpace(s[start:i])
				if part != "" {
					parts = append(parts, part)
				}
				start = i + 1
			}
		}
	}
	if tail := strings.TrimSpace(s[start:]); tail != "" {
		parts = append(parts, tail)
	}
	return parts
}

func btoi(b bool) int {
	if b {
		return 1
	}
	return 0
}

func arr(items []string) string {
	parts := make([]string, 0, len(items))
	for _, it := range items {
		parts = append(parts, fmt.Sprintf("'%s'", esc(it)))
	}
	return "[" + strings.Join(parts, ",") + "]"
}

func esc(s string) string { return strings.ReplaceAll(strings.TrimSpace(s), "'", "''") }

func sum(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	items := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" && !contains(items, trimmed) {
			items = append(items, trimmed)
		}
	}
	return items
}

func contains(items []string, target string) bool {
	for _, item := range items {
		if item == target {
			return true
		}
	}
	return false
}

func managedRoleNames() []string {
	names := make([]string, 0, len(roleSpecs))
	for _, role := range roleSpecs {
		names = append(names, role.Name)
	}
	return names
}

func getenv(k, d string) string {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		return v
	}
	return d
}
