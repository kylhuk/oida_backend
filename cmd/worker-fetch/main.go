package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"global-osint-backend/internal/fetch"
	"global-osint-backend/internal/migrate"
)

const (
	defaultClickHouseURL       = "http://clickhouse:8123"
	defaultMinIOEndpoint       = "http://minio:9000"
	defaultMinIORegion         = "us-east-1"
	defaultRawBucket           = "raw"
	defaultFetchTimeout        = 30 * time.Second
	defaultMaxBodyBytes  int64 = 16 << 20
	defaultUserAgent           = "global-osint-backend/worker-fetch"
)

type config struct {
	ClickHouseHTTP      string
	MinIOEndpoint       string
	MinIOAccessKey      string
	MinIOSecretKey      string
	MinIORegion         string
	RawBucket           string
	FetchTimeout        time.Duration
	MaxBodyBytes        int64
	InlineBodyMaxBytes  int
	RetryMaxAttempts    int
	RetryInitialBackoff time.Duration
	RetryMaxBackoff     time.Duration
	UserAgent           string
}

type sourcePolicyRecord struct {
	SourceID           string  `json:"source_id"`
	AuthMode           string  `json:"auth_mode"`
	RequestsPerMinute  uint32  `json:"requests_per_minute"`
	BurstSize          uint16  `json:"burst_size"`
	RetentionClass     string  `json:"retention_class"`
	Enabled            uint8   `json:"enabled"`
	DisabledReason     *string `json:"disabled_reason"`
	SupportsHistorical uint8   `json:"supports_historical"`
}

type rawDocumentResult struct {
	RawID         string  `json:"raw_id"`
	SourceID      string  `json:"source_id"`
	URL           string  `json:"url"`
	FetchedAt     string  `json:"fetched_at"`
	StatusCode    uint16  `json:"status_code"`
	ContentType   string  `json:"content_type"`
	ContentHash   string  `json:"content_hash"`
	BodyBytes     uint64  `json:"body_bytes"`
	ObjectKey     *string `json:"object_key"`
	FetchMetadata string  `json:"fetch_metadata"`
}

type clickhouseStore struct {
	runner *migrate.HTTPRunner
}

type s3Client struct {
	endpoint  *url.URL
	accessKey string
	secretKey string
	region    string
	client    *http.Client
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		serve()
		return 0
	}

	cfg, err := loadConfig()
	if err != nil {
		fmt.Fprintf(stderr, "load config: %v\n", err)
		return 1
	}

	switch args[0] {
	case "help", "-h", "--help":
		printUsage(stdout)
		return 0
	case "fetch-once":
		return runFetchOnce(cfg, args[1:], stdout, stderr)
	case "replay-once":
		return runReplayOnce(cfg, args[1:], stdout, stderr)
	default:
		fmt.Fprintf(stderr, "unknown command %q\n\n", args[0])
		printUsage(stderr)
		return 2
	}
}

func serve() {
	log.Println("worker-fetch started")
	for {
		time.Sleep(30 * time.Second)
		log.Println("worker-fetch idle")
	}
}

func runFetchOnce(cfg config, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("fetch-once", flag.ContinueOnError)
	fs.SetOutput(stderr)
	sourceID := fs.String("source-id", "", "Source registry id.")
	requestURL := fs.String("url", "", "Absolute URL to fetch.")
	method := fs.String("method", http.MethodGet, "HTTP method: GET or HEAD.")
	etag := fs.String("etag", "", "ETag validator for conditional fetch.")
	lastModified := fs.String("last-modified", "", "Last-Modified validator for conditional fetch.")
	retentionClass := fs.String("retention-class", "", "Retention class override when registry lookup is unavailable.")
	maxBodyBytes := fs.Int64("max-body-bytes", cfg.MaxBodyBytes, "Maximum decoded response body size in bytes.")
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), "Usage:\n  worker-fetch fetch-once --source-id <source-id> --url <url> [options]")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if *sourceID == "" || *requestURL == "" {
		fs.Usage()
		return 2
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.FetchTimeout)
	defer cancel()

	store := clickhouseStore{runner: migrate.NewHTTPRunner(cfg.ClickHouseHTTP)}
	policy, err := store.LookupSourcePolicy(ctx, *sourceID)
	if err != nil {
		fmt.Fprintf(stderr, "lookup source policy: %v\n", err)
		return 1
	}
	if strings.TrimSpace(*retentionClass) != "" {
		policy.RetentionClass = strings.TrimSpace(*retentionClass)
	}
	fetchPolicy := policy.toFetchPolicy(*maxBodyBytes)

	objectStore, err := newS3Client(cfg)
	if err != nil {
		fmt.Fprintf(stderr, "init object store: %v\n", err)
		return 1
	}

	client := fetch.NewClient(fetch.Config{
		HTTPClient: &http.Client{Timeout: cfg.FetchTimeout},
		RetryPolicy: fetch.RetryPolicy{
			MaxAttempts:    cfg.RetryMaxAttempts,
			InitialBackoff: cfg.RetryInitialBackoff,
			MaxBackoff:     cfg.RetryMaxBackoff,
		},
		MaxBodyBytes: *maxBodyBytes,
		UserAgent:    cfg.UserAgent,
	})

	request := fetch.Request{
		Method: *method,
		URL:    *requestURL,
		Source: fetchPolicy,
		Conditional: fetch.ConditionalRequest{
			ETag:         strings.TrimSpace(*etag),
			LastModified: strings.TrimSpace(*lastModified),
		},
	}
	response, err := client.Fetch(ctx, request)
	if err != nil && !errors.Is(err, fetch.ErrBodyTooLarge) && !errors.Is(err, fetch.ErrSourceBlocked) {
		log.Printf("fetch failed before persistence: %v", err)
	}

	now := time.Now().UTC()
	ids := buildIDs(*sourceID, *requestURL, response.ContentHash, now)
	retentionPolicy := fetch.ResolveRetentionPolicy(policy.RetentionClass)
	if cfg.InlineBodyMaxBytes > 0 {
		retentionPolicy.InlineBodyMaxBytes = cfg.InlineBodyMaxBytes
	}
	persisted, retainErr := fetch.RetainResponse(ctx, fetch.PersistOptions{
		FetchID:  ids.fetchID,
		RawID:    ids.rawID,
		SourceID: *sourceID,
		Bucket:   cfg.RawBucket,
		Policy:   retentionPolicy,
		Now:      now,
	}, request, response, objectStore)
	if retainErr != nil {
		fmt.Fprintf(stderr, "retain fetch response: %v\n", retainErr)
		return 1
	}
	if err := store.InsertFetchLog(ctx, persisted.FetchLog); err != nil {
		fmt.Fprintf(stderr, "insert fetch log: %v\n", err)
		return 1
	}
	if persisted.RawDocument != nil {
		if err := store.InsertRawDocument(ctx, *persisted.RawDocument); err != nil {
			fmt.Fprintf(stderr, "insert raw document: %v\n", err)
			return 1
		}
	}

	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(persisted); err != nil {
		fmt.Fprintf(stderr, "encode result: %v\n", err)
		return 1
	}
	if err != nil {
		return 1
	}
	return 0
}

func runReplayOnce(cfg config, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("replay-once", flag.ContinueOnError)
	fs.SetOutput(stderr)
	rawID := fs.String("raw-id", "", "Stored bronze.raw_document id to replay.")
	printBody := fs.Bool("print-body", false, "Also write the replayed body to stdout after the JSON summary.")
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), "Usage:\n  worker-fetch replay-once --raw-id <raw-id> [--print-body]")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if *rawID == "" {
		fs.Usage()
		return 2
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.FetchTimeout)
	defer cancel()

	store := clickhouseStore{runner: migrate.NewHTTPRunner(cfg.ClickHouseHTTP)}
	doc, err := store.LoadRawDocument(ctx, *rawID)
	if err != nil {
		fmt.Fprintf(stderr, "load raw document: %v\n", err)
		return 1
	}
	objectStore, err := newS3Client(cfg)
	if err != nil {
		fmt.Fprintf(stderr, "init object store: %v\n", err)
		return 1
	}
	replay, err := fetch.Replay(ctx, fetch.RawDocumentRow{
		RawID:         doc.RawID,
		SourceID:      doc.SourceID,
		URL:           doc.URL,
		FetchedAt:     doc.FetchedAt,
		StatusCode:    doc.StatusCode,
		ContentType:   doc.ContentType,
		ContentHash:   doc.ContentHash,
		BodyBytes:     doc.BodyBytes,
		ObjectKey:     doc.ObjectKey,
		FetchMetadata: doc.FetchMetadata,
	}, objectStore)
	if err != nil {
		fmt.Fprintf(stderr, "replay raw document: %v\n", err)
		return 1
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(replay); err != nil {
		fmt.Fprintf(stderr, "encode replay result: %v\n", err)
		return 1
	}
	if *printBody && len(replay.Body) > 0 {
		if _, err := stdout.Write(replay.Body); err != nil {
			fmt.Fprintf(stderr, "write replay body: %v\n", err)
			return 1
		}
	}
	return 0
}

func printUsage(w io.Writer) {
	fmt.Fprintln(w, "Usage:")
	fmt.Fprintln(w, "  worker-fetch [command]")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Commands:")
	fmt.Fprintln(w, "  fetch-once   Execute one GET/HEAD fetch with retention persistence")
	fmt.Fprintln(w, "  replay-once  Re-emit a stored raw document without a live fetch")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Without a command the worker stays alive and waits for orchestration.")
}

func loadConfig() (config, error) {
	endpoint, err := url.Parse(getenv("MINIO_ENDPOINT", defaultMinIOEndpoint))
	if err != nil {
		return config{}, fmt.Errorf("parse MinIO endpoint: %w", err)
	}
	if endpoint.Scheme == "" || endpoint.Host == "" {
		return config{}, fmt.Errorf("invalid MinIO endpoint %q", endpoint.String())
	}
	return config{
		ClickHouseHTTP:      getenv("CLICKHOUSE_HTTP_URL", defaultClickHouseURL),
		MinIOEndpoint:       endpoint.String(),
		MinIOAccessKey:      getenv("MINIO_ACCESS_KEY", getenv("MINIO_ROOT_USER", "minio")),
		MinIOSecretKey:      getenv("MINIO_SECRET_KEY", getenv("MINIO_ROOT_PASSWORD", "minio_change_me")),
		MinIORegion:         getenv("MINIO_REGION", defaultMinIORegion),
		RawBucket:           getenv("RAW_BUCKET", defaultRawBucket),
		FetchTimeout:        parseDurationEnv("FETCH_TIMEOUT", defaultFetchTimeout),
		MaxBodyBytes:        parseInt64Env("FETCH_MAX_BODY_BYTES", defaultMaxBodyBytes),
		InlineBodyMaxBytes:  parseIntEnv("FETCH_INLINE_BODY_BYTES", 64<<10),
		RetryMaxAttempts:    parseIntEnv("FETCH_RETRY_ATTEMPTS", 3),
		RetryInitialBackoff: parseDurationEnv("FETCH_RETRY_INITIAL_BACKOFF", 250*time.Millisecond),
		RetryMaxBackoff:     parseDurationEnv("FETCH_RETRY_MAX_BACKOFF", 3*time.Second),
		UserAgent:           getenv("FETCH_USER_AGENT", defaultUserAgent),
	}, nil
}

func (s clickhouseStore) LookupSourcePolicy(ctx context.Context, sourceID string) (sourcePolicyRecord, error) {
	query := fmt.Sprintf(`SELECT source_id, auth_mode, requests_per_minute, burst_size, retention_class, enabled, disabled_reason, supports_historical
FROM meta.source_registry FINAL
WHERE source_id = '%s'
LIMIT 1
FORMAT JSONEachRow`, esc(sourceID))
	output, err := s.runner.Query(ctx, query)
	if err != nil {
		return sourcePolicyRecord{}, err
	}
	line := strings.TrimSpace(output)
	if line == "" {
		return sourcePolicyRecord{}, fmt.Errorf("source %q not found in meta.source_registry", sourceID)
	}
	var record sourcePolicyRecord
	if err := json.Unmarshal([]byte(line), &record); err != nil {
		return sourcePolicyRecord{}, err
	}
	return record, nil
}

func (s clickhouseStore) InsertFetchLog(ctx context.Context, row fetch.FetchLogRow) error {
	query := fmt.Sprintf(`INSERT INTO ops.fetch_log
	(fetch_id, source_id, url_hash, status_code, success, fetched_at, latency_ms, body_bytes, error_message)
	VALUES ('%s','%s','%s',%d,%d,toDateTime64('%s', 3, 'UTC'),%d,%d,%s)`,
		esc(row.FetchID),
		esc(row.SourceID),
		esc(row.URLHash),
		row.StatusCode,
		row.Success,
		esc(row.FetchedAt),
		row.LatencyMS,
		row.BodyBytes,
		sqlNullableString(row.ErrorMessage),
	)
	return s.runner.ApplySQL(ctx, query)
}

func (s clickhouseStore) InsertRawDocument(ctx context.Context, row fetch.RawDocumentRow) error {
	query := fmt.Sprintf(`INSERT INTO bronze.raw_document
	(raw_id, source_id, url, fetched_at, status_code, content_type, content_hash, body_bytes, object_key, fetch_metadata)
	VALUES ('%s','%s','%s',toDateTime64('%s', 3, 'UTC'),%d,'%s','%s',%d,%s,'%s')`,
		esc(row.RawID),
		esc(row.SourceID),
		esc(row.URL),
		esc(row.FetchedAt),
		row.StatusCode,
		esc(row.ContentType),
		esc(row.ContentHash),
		row.BodyBytes,
		sqlNullableString(row.ObjectKey),
		esc(row.FetchMetadata),
	)
	return s.runner.ApplySQL(ctx, query)
}

func (s clickhouseStore) LoadRawDocument(ctx context.Context, rawID string) (rawDocumentResult, error) {
	query := fmt.Sprintf(`SELECT raw_id, source_id, url, fetched_at, status_code, content_type, content_hash, body_bytes, object_key, fetch_metadata
FROM bronze.raw_document
WHERE raw_id = '%s'
ORDER BY fetched_at DESC
LIMIT 1
FORMAT JSONEachRow`, esc(rawID))
	output, err := s.runner.Query(ctx, query)
	if err != nil {
		return rawDocumentResult{}, err
	}
	line := strings.TrimSpace(output)
	if line == "" {
		return rawDocumentResult{}, fmt.Errorf("raw document %q not found", rawID)
	}
	var doc rawDocumentResult
	if err := json.Unmarshal([]byte(line), &doc); err != nil {
		return rawDocumentResult{}, err
	}
	return doc, nil
}

func (record sourcePolicyRecord) toFetchPolicy(maxBodyBytes int64) fetch.SourcePolicy {
	disabledReason := ""
	if record.DisabledReason != nil {
		disabledReason = strings.TrimSpace(*record.DisabledReason)
	}
	retentionClass := strings.TrimSpace(record.RetentionClass)
	if retentionClass == "" {
		retentionClass = "warm"
	}
	return fetch.SourcePolicy{
		SourceID:        record.SourceID,
		RetentionClass:  retentionClass,
		Disabled:        record.Enabled == 0,
		DisabledReason:  disabledReason,
		AuthMode:        strings.TrimSpace(record.AuthMode),
		MaxBodyBytes:    maxBodyBytes,
		SupportsLiveGET: true,
	}
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
		client:    &http.Client{Timeout: cfg.FetchTimeout},
	}, nil
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

func (c *s3Client) GetObject(ctx context.Context, bucket, key string) ([]byte, string, error) {
	resp, respBody, err := c.do(ctx, http.MethodGet, "/"+bucket+"/"+key, nil, "")
	if err != nil {
		return nil, "", err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf("unexpected status %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	return respBody, strings.TrimSpace(resp.Header.Get("Content-Type")), nil
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
	defer resp.Body.Close()
	respBody, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return nil, nil, readErr
	}
	return resp, respBody, nil
}

type fetchIDs struct {
	fetchID string
	rawID   string
}

func buildIDs(sourceID, requestURL, contentHash string, now time.Time) fetchIDs {
	seed := strings.Join([]string{strings.TrimSpace(sourceID), strings.TrimSpace(requestURL), strings.TrimSpace(contentHash), now.UTC().Format(time.RFC3339Nano)}, "|")
	digest := sum([]byte(seed))
	return fetchIDs{
		fetchID: "fetch:" + digest[:16],
		rawID:   "raw:" + digest[16:32],
	}
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

func sortedKeys(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func sqlNullableString(value *string) string {
	if value == nil || strings.TrimSpace(*value) == "" {
		return "NULL"
	}
	return fmt.Sprintf("'%s'", esc(*value))
}

func esc(value string) string {
	return strings.ReplaceAll(strings.TrimSpace(value), "'", "''")
}

func sum(body []byte) string {
	hash := sha256.Sum256(body)
	return hex.EncodeToString(hash[:])
}

func getenv(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func parseDurationEnv(key string, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func parseIntEnv(key string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func parseInt64Env(key string, fallback int64) int64 {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return fallback
	}
	return parsed
}
