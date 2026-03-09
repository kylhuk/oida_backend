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
	defaultSeedPath      = "/app/seed/source_registry.json"
	defaultReadyMarker   = "/tmp/bootstrap.ready"
	defaultClickHouseURL = "http://clickhouse:8123"
	defaultMinIOEndpoint = "http://minio:9000"
	defaultMinIORegion   = "us-east-1"
	defaultBuckets       = "raw,stage,backup"
	defaultBackupDir     = "/app/infra/backup"
	defaultBackupBucket  = "backup"
	defaultBackupPrefix  = "bootstrap"
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
			},
		},
		{
			Name: "osint_ingest",
			Grants: []string{
				"GRANT SELECT ON meta.* TO osint_ingest",
				"GRANT SELECT ON ops.* TO osint_ingest",
				"GRANT INSERT ON ops.* TO osint_ingest",
				"GRANT SELECT ON bronze.* TO osint_ingest",
				"GRANT INSERT ON bronze.* TO osint_ingest",
				"GRANT SELECT ON silver.* TO osint_ingest",
				"GRANT INSERT ON silver.* TO osint_ingest",
				"GRANT SELECT ON gold.* TO osint_ingest",
				"GRANT INSERT ON gold.* TO osint_ingest",
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
	Name   string
	Grants []string
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
	Users          []clickhouseUser
}

type sourceSeed struct {
	SourceID            string   `json:"source_id"`
	Domain              string   `json:"domain"`
	DomainFamily        string   `json:"domain_family"`
	SourceClass         string   `json:"source_class"`
	Entrypoints         []string `json:"entrypoints"`
	AuthMode            string   `json:"auth_mode"`
	FormatHint          string   `json:"format_hint"`
	RobotsPolicy        string   `json:"robots_policy"`
	RefreshStrategy     string   `json:"refresh_strategy"`
	License             string   `json:"license"`
	TermsURL            string   `json:"terms_url"`
	GeoScope            string   `json:"geo_scope"`
	Priority            int      `json:"priority"`
	ParserID            string   `json:"parser_id"`
	EntityTypes         []string `json:"entity_types"`
	ExpectedPlaceTypes  []string `json:"expected_place_types"`
	SupportsHistorical  bool     `json:"supports_historical"`
	SupportsDelta       bool     `json:"supports_delta"`
	ConfidenceBaseline  float64  `json:"confidence_baseline"`
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
	cfg, err := loadConfig()
	if err != nil {
		log.Fatal(err)
	}

	mode := "install"
	if len(os.Args) > 1 {
		mode = strings.TrimSpace(os.Args[1])
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
		fmt.Fprintln(os.Stdout, "Usage: bootstrap [verify]")
	default:
		log.Fatalf("unknown bootstrap mode %q", mode)
	}
}

func loadConfig() (config, error) {
	buckets := splitCSV(getenv("MINIO_BUCKETS", defaultBuckets))
	backupBucket := getenv("BACKUP_BUCKET", defaultBackupBucket)
	if !contains(buckets, backupBucket) {
		buckets = append(buckets, backupBucket)
	}

	endpoint, err := url.Parse(getenv("MINIO_ENDPOINT", defaultMinIOEndpoint))
	if err != nil {
		return config{}, fmt.Errorf("parse minio endpoint: %w", err)
	}
	if endpoint.Scheme == "" || endpoint.Host == "" {
		return config{}, fmt.Errorf("invalid MinIO endpoint %q", endpoint.String())
	}

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
		Users: []clickhouseUser{
			{Name: getenv("CLICKHOUSE_BOOTSTRAP_USER", "svc_bootstrap"), Password: getenv("CLICKHOUSE_BOOTSTRAP_PASSWORD", "bootstrap_change_me"), Roles: []string{"osint_admin"}},
			{Name: getenv("CLICKHOUSE_API_USER", "svc_api"), Password: getenv("CLICKHOUSE_API_PASSWORD", "api_change_me"), Roles: []string{"osint_reader"}},
			{Name: getenv("CLICKHOUSE_CONTROL_PLANE_USER", "svc_control_plane"), Password: getenv("CLICKHOUSE_CONTROL_PLANE_PASSWORD", "control_plane_change_me"), Roles: []string{"osint_admin"}},
			{Name: getenv("CLICKHOUSE_WORKER_FETCH_USER", "svc_worker_fetch"), Password: getenv("CLICKHOUSE_WORKER_FETCH_PASSWORD", "worker_fetch_change_me"), Roles: []string{"osint_ingest"}},
			{Name: getenv("CLICKHOUSE_WORKER_PARSE_USER", "svc_worker_parse"), Password: getenv("CLICKHOUSE_WORKER_PARSE_PASSWORD", "worker_parse_change_me"), Roles: []string{"osint_ingest"}},
			{Name: getenv("CLICKHOUSE_RENDERER_USER", "svc_renderer"), Password: getenv("CLICKHOUSE_RENDERER_PASSWORD", "renderer_change_me"), Roles: []string{"osint_reader"}},
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
	if err := applyMigrations(ctx, runner, cfg.MigrationDir); err != nil {
		return err
	}
	if err := loadSourceSeed(ctx, runner, cfg.SeedPath); err != nil {
		return fmt.Errorf("load source seed: %w", err)
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
	if err := verifyMinimumCount(ctx, runner, "SELECT count() FROM meta.schema_migrations FORMAT TabSeparated", 1, "meta.schema_migrations rows"); err != nil {
		return err
	}
	if err := verifyMinimumCount(ctx, runner, "SELECT count() FROM meta.source_registry FORMAT TabSeparated", 1, "meta.source_registry rows"); err != nil {
		return err
	}
	if err := verifyBackupAssets(ctx, minio, cfg); err != nil {
		return err
	}
	log.Println("bootstrap verify complete")
	return nil
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
		if err := runner.ApplySQL(ctx, fmt.Sprintf("CREATE ROLE IF NOT EXISTS %s", role.Name)); err != nil {
			return fmt.Errorf("create role %s: %w", role.Name, err)
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
	}

	for _, user := range users {
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
			query := fmt.Sprintf("SELECT count() FROM system.grants WHERE role_name = '%s' AND access_type = '%s' AND database = '%s' AND table = '%s' FORMAT TabSeparated", esc(role.Name), esc(privilege), esc(database), esc(table))
			if database == "*" && table == "*" {
				query = fmt.Sprintf("SELECT count() FROM system.grants WHERE role_name = '%s' AND access_type = '%s' FORMAT TabSeparated", esc(role.Name), esc(privilege))
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
		applied, err := runner.IsApplied(ctx, name, checksum)
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
		if err := runner.Record(ctx, name, checksum, true, "applied"); err != nil {
			return fmt.Errorf("record migration %s: %w", name, err)
		}
		log.Printf("applied migration: %s", name)
	}
	return nil
}

func loadSourceSeed(ctx context.Context, runner *migrate.HTTPRunner, path string) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var seeds []sourceSeed
	if err := json.Unmarshal(b, &seeds); err != nil {
		return err
	}
	for _, s := range seeds {
		check := fmt.Sprintf("SELECT count() FROM meta.source_registry WHERE source_id='%s' FORMAT TabSeparated", esc(s.SourceID))
		out, err := runner.Query(ctx, check)
		if err != nil {
			return err
		}
		if strings.TrimSpace(out) != "0" {
			continue
		}
		insert := fmt.Sprintf(`INSERT INTO meta.source_registry
(source_id, domain, domain_family, source_class, entrypoints, auth_mode, format_hint, robots_policy, refresh_strategy, license, terms_url, geo_scope, priority, parser_id, entity_types, expected_place_types, supports_historical, supports_delta, confidence_baseline, enabled, version, updated_at)
VALUES ('%s','%s','%s','%s',%s,'%s','%s','%s','%s','%s','%s','%s',%d,'%s',%s,%s,%d,%d,%f,1,1,now64(3))`,
			esc(s.SourceID), esc(s.Domain), esc(s.DomainFamily), esc(s.SourceClass), arr(s.Entrypoints), esc(s.AuthMode), esc(s.FormatHint), esc(s.RobotsPolicy), esc(s.RefreshStrategy), esc(s.License), esc(s.TermsURL), esc(s.GeoScope), s.Priority, esc(s.ParserID), arr(s.EntityTypes), arr(s.ExpectedPlaceTypes), btoi(s.SupportsHistorical), btoi(s.SupportsDelta), s.ConfidenceBaseline)
		if err := runner.ApplySQL(ctx, insert); err != nil {
			return err
		}
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
	privilegeItems := strings.Split(privilegePart, ",")
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

func btoi(b bool) int { if b { return 1 }; return 0 }

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

func getenv(k, d string) string {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		return v
	}
	return d
}
