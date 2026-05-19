package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"global-osint-backend/internal/discovery"
	"global-osint-backend/internal/fetch"
	"global-osint-backend/internal/migrate"
	"global-osint-backend/internal/objectstore"
	"global-osint-backend/internal/observability"
	sharedretry "global-osint-backend/internal/retry"
)

const (
	defaultClickHouseURL       = "http://clickhouse:8123"
	defaultMinIOEndpoint       = "http://minio:9000"
	defaultMinIORegion         = "us-east-1"
	defaultRawBucket           = "raw"
	defaultFetchTimeout        = 30 * time.Second
	defaultMaxBodyBytes  int64 = 16 << 20
	defaultUserAgent           = "global-osint-backend/worker-fetch"
	defaultLeaseDuration       = 2 * time.Minute
	clickHouseTimeLayout       = "2006-01-02 15:04:05.000"
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
	SourceID           string   `json:"source_id"`
	AuthMode           string   `json:"auth_mode"`
	AuthConfigJSON     string   `json:"auth_config_json"`
	TransportType      string   `json:"transport_type"`
	AllowedHosts       []string `json:"allowed_hosts"`
	CrawlEnabled       uint8    `json:"crawl_enabled"`
	BronzeTable        *string  `json:"bronze_table"`
	RequestsPerMinute  uint32   `json:"requests_per_minute"`
	BurstSize          uint16   `json:"burst_size"`
	RetentionClass     string   `json:"retention_class"`
	Enabled            uint8    `json:"enabled"`
	DisabledReason     *string  `json:"disabled_reason"`
	SupportsHistorical uint8    `json:"supports_historical"`
}

type automaticSourceRecord struct {
	SourceID          string  `json:"source_id"`
	RequestsPerMinute uint32  `json:"requests_per_minute"`
	BurstSize         uint16  `json:"burst_size"`
	RefreshStrategy   string  `json:"refresh_strategy"`
	NotModifiedRatio  float64 `json:"not_modified_ratio"`
}

type rawDocumentResult struct {
	RawID         string  `json:"raw_id"`
	FetchID       string  `json:"fetch_id"`
	CorrelationID *string `json:"correlation_id"`
	SourceID      string  `json:"source_id"`
	URL           string  `json:"url"`
	FinalURL      string  `json:"final_url"`
	FetchedAt     string  `json:"fetched_at"`
	StatusCode    uint16  `json:"status_code"`
	ContentType   string  `json:"content_type"`
	ContentHash   string  `json:"content_hash"`
	BodyBytes     uint64  `json:"body_bytes"`
	ObjectKey     *string `json:"object_key"`
	ETag          *string `json:"etag"`
	LastModified  *string `json:"last_modified"`
	NotModified   uint8   `json:"not_modified"`
	StorageClass  string  `json:"storage_class"`
	FetchMetadata string  `json:"fetch_metadata"`
}

type frontierLeaseRow struct {
	SourceID       string  `json:"source_id"`
	Domain         string  `json:"domain"`
	URL            string  `json:"url"`
	CanonicalURL   string  `json:"canonical_url"`
	State          string  `json:"state"`
	AttemptCount   uint16  `json:"attempt_count"`
	ETag           *string `json:"etag"`
	LastModified   *string `json:"last_modified"`
	DiscoveryKind  string  `json:"discovery_kind"`
	LeaseOwner     *string `json:"lease_owner"`
	LeaseExpiresAt *string `json:"lease_expires_at"`
}

var errMissingCredential = errors.New("missing auth credential")

type authConfig struct {
	EnvVar             string `json:"env_var"`
	Placement          string `json:"placement"`
	Name               string `json:"name"`
	Prefix             string `json:"prefix"`
	ClientIDEnvVar     string `json:"client_id_env_var"`
	ClientSecretEnvVar string `json:"client_secret_env_var"`
	TokenURL           string `json:"token_url"`
	GrantType          string `json:"grant_type"`
	Scope              string `json:"scope"`
}

type oauthTokenCacheEntry struct {
	Token     string
	ExpiresAt time.Time
}

var oauthTokenCache = struct {
	mu      sync.Mutex
	entries map[string]oauthTokenCacheEntry
}{
	entries: map[string]oauthTokenCacheEntry{},
}

type clickhouseStore struct {
	runner *migrate.HTTPRunner
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
	case "fetch-source":
		return runFetchSource(cfg, args[1:], stdout, stderr)
	case "replay-once":
		return runReplayOnce(cfg, args[1:], stdout, stderr)
	default:
		fmt.Fprintf(stderr, "unknown command %q\n\n", args[0])
		printUsage(stderr)
		return 2
	}
}

func serve() {
	cfg, err := loadConfig()
	if err != nil {
		observability.LogEvent("worker-fetch", "config_error", "", map[string]any{"error": err.Error()})
		return
	}
	owner := "worker-fetch"
	if hostname, hostErr := os.Hostname(); hostErr == nil && strings.TrimSpace(hostname) != "" {
		owner = strings.TrimSpace(hostname)
	}
	observability.LogEvent("worker-fetch", "service_started", observability.NewCorrelationID("worker-fetch"), map[string]any{"owner": owner})
	store := clickhouseStore{runner: migrate.NewHTTPRunner(cfg.ClickHouseHTTP)}
	ctx, cancel := context.WithTimeout(context.Background(), cfg.FetchTimeout)
	sources, listErr := store.listAutomaticSources(ctx)
	cancel()
	if listErr != nil {
		observability.LogEvent("worker-fetch", "automatic_source_list_failed", "", map[string]any{"error": listErr.Error()})
		for {
			time.Sleep(5 * time.Second)
		}
	}
	if len(sources) == 0 {
		observability.LogEvent("worker-fetch", "automatic_sources_empty", "", nil)
		for {
			time.Sleep(30 * time.Second)
		}
	}
	workerTotal := 0
	for _, source := range sources {
		interval := suggestedFetchInterval(source)
		workers := suggestedWorkerCount(source)
		batch := suggestedBatchLimit(source)
		for idx := 0; idx < workers; idx++ {
			workerTotal++
			leaseOwner := fmt.Sprintf("%s:%s:%d", owner, source.SourceID, idx+1)
			go runSourceFetchLoop(cfg, source.SourceID, leaseOwner, interval, batch)
		}
		observability.LogEvent("worker-fetch", "source_worker_group_started", "", map[string]any{"source_id": source.SourceID, "workers": workers, "interval": interval.Round(100 * time.Millisecond).String(), "batch": batch, "not_modified_ratio": source.NotModifiedRatio})
	}
	observability.LogEvent("worker-fetch", "automatic_worker_pools_active", "", map[string]any{"sources": len(sources), "workers": workerTotal})
	select {}
}

func runSourceFetchLoop(cfg config, sourceID, leaseOwner string, interval time.Duration, batch int) {
	for {
		rc := runFetchSource(cfg, []string{"--source-id", sourceID, "--limit", strconv.Itoa(batch), "--lease-owner", leaseOwner}, io.Discard, io.Discard)
		if rc != 0 {
			observability.LogEvent("worker-fetch", "source_loop_error", "", map[string]any{"source_id": sourceID, "owner": leaseOwner, "code": rc})
		}
		time.Sleep(interval)
	}
}

func suggestedFetchInterval(source automaticSourceRecord) time.Duration {
	baseSeconds := 0.0
	if source.RequestsPerMinute > 0 {
		baseSeconds = 60.0 / float64(source.RequestsPerMinute)
	}
	if baseSeconds <= 0 {
		switch strings.TrimSpace(strings.ToLower(source.RefreshStrategy)) {
		case "frequent":
			baseSeconds = 1.0
		case "scheduled":
			baseSeconds = 30.0
		default:
			baseSeconds = 60.0
		}
	}
	ratio := source.NotModifiedRatio
	switch {
	case ratio >= 0.90:
		baseSeconds *= 5.0
	case ratio >= 0.70:
		baseSeconds *= 3.0
	case ratio <= 0.10:
		baseSeconds *= 0.5
	}
	if baseSeconds < 0.1 {
		baseSeconds = 0.1
	}
	if baseSeconds > 300.0 {
		baseSeconds = 300.0
	}
	return time.Duration(baseSeconds * float64(time.Second))
}

func suggestedWorkerCount(source automaticSourceRecord) int {
	workers := int(source.BurstSize)
	if workers <= 0 {
		workers = 1
	}
	if source.RequestsPerMinute >= 120 && workers < 2 {
		workers = 2
	}
	if workers > 16 {
		workers = 16
	}
	return workers
}

func suggestedBatchLimit(source automaticSourceRecord) int {
	batch := int(source.BurstSize) * 4
	if batch <= 0 {
		batch = 4
	}
	if batch > 256 {
		batch = 256
	}
	return batch
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
		observability.LogEvent("worker-fetch", "fetch_failed_before_persistence", observability.NewCorrelationID("fetch"), map[string]any{"source_id": *sourceID, "url": *requestURL, "error": err.Error()})
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

func runFetchSource(cfg config, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("fetch-source", flag.ContinueOnError)
	fs.SetOutput(stderr)
	sourceID := fs.String("source-id", "", "Source registry id.")
	limit := fs.Int("limit", 1, "Maximum number of frontier rows to fetch.")
	leaseOwner := fs.String("lease-owner", "", "Lease owner id (defaults to hostname).")
	leaseDuration := fs.Duration("lease-duration", defaultLeaseDuration, "Frontier lease duration.")
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), "Usage:\n  worker-fetch fetch-source --source-id <source-id> [--limit <n>]")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if *sourceID == "" || *limit <= 0 {
		fs.Usage()
		return 2
	}

	owner := strings.TrimSpace(*leaseOwner)
	if owner == "" {
		hostname, err := os.Hostname()
		if err == nil && strings.TrimSpace(hostname) != "" {
			owner = strings.TrimSpace(hostname)
		} else {
			owner = "worker-fetch"
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.FetchTimeout)
	defer cancel()

	store := clickhouseStore{runner: migrate.NewHTTPRunner(cfg.ClickHouseHTTP)}
	policy, err := store.LookupSourcePolicy(ctx, *sourceID)
	if err != nil {
		fmt.Fprintf(stderr, "lookup source policy: %v\n", err)
		return 1
	}
	fetchPolicy := policy.toFetchPolicy(cfg.MaxBodyBytes)

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
		MaxBodyBytes: cfg.MaxBodyBytes,
		UserAgent:    cfg.UserAgent,
	})

	processed := 0
	for processed < *limit {
		lease, ok, err := store.ClaimFrontierLease(ctx, *sourceID, owner, *leaseDuration)
		if err != nil {
			fmt.Fprintf(stderr, "claim frontier lease: %v\n", err)
			return 1
		}
		if !ok {
			break
		}

		attemptedAt := time.Now().UTC()
		requestURL := strings.TrimSpace(lease.CanonicalURL)
		if requestURL == "" {
			requestURL = strings.TrimSpace(lease.URL)
		}

		if !(discovery.SourcePolicy{Enabled: true, AllowedHosts: policy.AllowedHosts}.AllowsURL(requestURL)) {
			updated := lease.ApplyFetchOutcome(discovery.FetchOutcome{
				AttemptedAt:   attemptedAt,
				ErrorCode:     discovery.FrontierErrorDisabled,
				ErrorMessage:  "frontier url is outside allowed_hosts",
				LeaseOwner:    owner,
				LeaseDuration: *leaseDuration,
			})
			if err := store.UpdateFrontierEntry(ctx, owner, updated); err != nil {
				fmt.Fprintf(stderr, "update frontier blocked state: %v\n", err)
				return 1
			}
			processed++
			continue
		}

		headers, preparedURL, authErr := prepareSourceRequest(policy, requestURL)
		if authErr != nil {
			code := discovery.FrontierErrorUnsupportedAuth
			if errors.Is(authErr, errMissingCredential) {
				code = discovery.FrontierErrorMissingAuth
			}
			updated := lease.ApplyFetchOutcome(discovery.FetchOutcome{
				AttemptedAt:   attemptedAt,
				ErrorCode:     code,
				ErrorMessage:  authErr.Error(),
				LeaseOwner:    owner,
				LeaseDuration: *leaseDuration,
			})
			if err := store.UpdateFrontierEntry(ctx, owner, updated); err != nil {
				fmt.Fprintf(stderr, "update frontier auth-blocked state: %v\n", err)
				return 1
			}
			processed++
			continue
		}

		correlationID := observability.NewCorrelationID("fetch")
		request := fetch.Request{
			Method:        http.MethodGet,
			URL:           preparedURL,
			CorrelationID: correlationID,
			Headers:       headers,
			Source:        fetchPolicy,
			Conditional: fetch.ConditionalRequest{
				ETag:         strings.TrimSpace(nilString(lease.ETag)),
				LastModified: strings.TrimSpace(nilString(lease.LastModified)),
			},
		}
		response, fetchErr := client.Fetch(ctx, request)

		ids := buildIDs(*sourceID, preparedURL, response.ContentHash, attemptedAt)
		retentionPolicy := fetch.ResolveRetentionPolicy(policy.RetentionClass)
		if cfg.InlineBodyMaxBytes > 0 {
			retentionPolicy.InlineBodyMaxBytes = cfg.InlineBodyMaxBytes
		}
		persistRequest, persistResponse := sanitizeFetchPersistence(policy, requestURL, request, response)
		persisted, retainErr := fetch.RetainResponse(ctx, fetch.PersistOptions{
			FetchID:  ids.fetchID,
			RawID:    ids.rawID,
			SourceID: *sourceID,
			Bucket:   cfg.RawBucket,
			Policy:   retentionPolicy,
			Now:      attemptedAt,
		}, persistRequest, persistResponse, objectStore)
		if retainErr != nil {
			fmt.Fprintf(stderr, "retain fetch response: %v\n", retainErr)
			return 1
		}
		observability.LogEvent("worker-fetch", "fetch_attempt_persisted", correlationID, map[string]any{"source_id": *sourceID, "fetch_id": persisted.FetchLog.FetchID, "status_code": persisted.FetchLog.StatusCode, "success": persisted.FetchLog.Success == 1, "raw_written": persisted.RawDocument != nil})
		if err := store.InsertFetchLog(ctx, persisted.FetchLog); err != nil {
			fmt.Fprintf(stderr, "insert fetch log: %v\n", err)
			return 1
		}
		if persisted.RawDocument != nil {
			isDuplicate, err := store.IsDuplicateRawDocument(ctx, *persisted.RawDocument)
			if err != nil {
				fmt.Fprintf(stderr, "check raw dedupe fingerprint: %v\n", err)
				return 1
			}
			if !isDuplicate {
				if err := store.InsertRawDocument(ctx, *persisted.RawDocument); err != nil {
					fmt.Fprintf(stderr, "insert raw document: %v\n", err)
					return 1
				}
			}
		}

		outcome := frontierOutcomeFromFetch(ids.fetchID, attemptedAt, response, fetchErr, sharedretry.Policy{
			MaxAttempts:    cfg.RetryMaxAttempts,
			InitialBackoff: cfg.RetryInitialBackoff,
			MaxBackoff:     cfg.RetryMaxBackoff,
		})
		updated := lease.ApplyFetchOutcome(outcome)
		if err := store.UpdateFrontierEntry(ctx, owner, updated); err != nil {
			fmt.Fprintf(stderr, "update frontier outcome: %v\n", err)
			return 1
		}
		processed++
	}

	return writeJSONResult(stdout, map[string]any{"source_id": *sourceID, "processed": processed, "correlation_scope": "per-fetch-attempt"})
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
		FetchID:       doc.FetchID,
		SourceID:      doc.SourceID,
		URL:           doc.URL,
		FinalURL:      doc.FinalURL,
		FetchedAt:     doc.FetchedAt,
		StatusCode:    doc.StatusCode,
		ContentType:   doc.ContentType,
		ContentHash:   doc.ContentHash,
		BodyBytes:     doc.BodyBytes,
		ObjectKey:     doc.ObjectKey,
		ETag:          doc.ETag,
		LastModified:  doc.LastModified,
		NotModified:   doc.NotModified,
		StorageClass:  doc.StorageClass,
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
	fmt.Fprintln(w, "  fetch-source Lease and fetch frontier URLs for one source")
	fmt.Fprintln(w, "  replay-once  Re-emit a stored raw document without a live fetch")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Without a command the worker automatically polls eligible sources every 30s.")
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
	query := fmt.Sprintf(`SELECT source_id, auth_mode, auth_config_json, transport_type, allowed_hosts, crawl_enabled, bronze_table, requests_per_minute, burst_size, retention_class, enabled, disabled_reason, supports_historical
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

func (s clickhouseStore) listAutomaticSources(ctx context.Context) ([]automaticSourceRecord, error) {
	query := `SELECT s.source_id, s.requests_per_minute, s.burst_size, s.refresh_strategy,
if(count(f.status_code) = 0, 0.0, toFloat64(countIf(f.status_code = 304)) / toFloat64(count(f.status_code))) AS not_modified_ratio
FROM meta.source_registry s
LEFT JOIN ops.fetch_log f ON s.source_id = f.source_id AND f.fetched_at > now() - INTERVAL 30 MINUTE
WHERE s.enabled = 1
  AND s.crawl_enabled = 1
  AND s.transport_type = 'http'
  AND (s.disabled_reason IS NULL OR s.disabled_reason = '')
GROUP BY s.source_id, s.requests_per_minute, s.burst_size, s.refresh_strategy
ORDER BY s.source_id
FORMAT JSONEachRow`
	output, err := s.runner.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	rows := []automaticSourceRecord{}
	for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var row automaticSourceRecord
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func (s clickhouseStore) listAutomaticSourceIDs(ctx context.Context) ([]string, error) {
	query := `SELECT source_id
FROM meta.source_registry FINAL
WHERE enabled = 1
  AND crawl_enabled = 1
  AND transport_type = 'http'
  AND (disabled_reason IS NULL OR disabled_reason = '')
ORDER BY source_id
FORMAT TabSeparated`
	output, err := s.runner.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	rows := []string{}
	for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
		sourceID := strings.TrimSpace(line)
		if sourceID == "" {
			continue
		}
		rows = append(rows, sourceID)
	}
	return rows, nil
}

func (s clickhouseStore) InsertFetchLog(ctx context.Context, row fetch.FetchLogRow) error {
	fetchedAt := normalizeClickHouseTimeString(row.FetchedAt)
	query := fmt.Sprintf(`INSERT INTO ops.fetch_log
	(fetch_id, correlation_id, source_id, url_hash, status_code, success, fetched_at, latency_ms, attempt_count, retry_count, body_bytes, error_message)
	VALUES ('%s',%s,'%s','%s',%d,%d,toDateTime64('%s', 3, 'UTC'),%d,%d,%d,%d,%s)`,
		esc(row.FetchID),
		sqlNullableString(nilIfBlank(row.CorrelationID)),
		esc(row.SourceID),
		esc(row.URLHash),
		row.StatusCode,
		row.Success,
		esc(fetchedAt),
		row.LatencyMS,
		row.AttemptCount,
		row.RetryCount,
		row.BodyBytes,
		sqlNullableString(row.ErrorMessage),
	)
	return s.runner.ApplySQL(ctx, query)
}

func nilIfBlank(v string) *string {
	trimmed := strings.TrimSpace(v)
	if trimmed == "" {
		return nil
	}
	return &trimmed
}

func (s clickhouseStore) InsertRawDocument(ctx context.Context, row fetch.RawDocumentRow) error {
	fetchedAt := normalizeClickHouseTimeString(row.FetchedAt)
	query := fmt.Sprintf(`INSERT INTO bronze.raw_document
	(raw_id, fetch_id, source_id, url, final_url, fetched_at, status_code, content_type, content_hash, body_bytes, object_key, etag, last_modified, not_modified, storage_class, fetch_metadata)
	VALUES ('%s','%s','%s','%s','%s',toDateTime64('%s', 3, 'UTC'),%d,'%s','%s',%d,%s,%s,%s,%d,'%s','%s')`,
		esc(row.RawID),
		esc(row.FetchID),
		esc(row.SourceID),
		esc(row.URL),
		esc(row.FinalURL),
		esc(fetchedAt),
		row.StatusCode,
		esc(row.ContentType),
		esc(row.ContentHash),
		row.BodyBytes,
		sqlNullableString(row.ObjectKey),
		sqlNullableString(row.ETag),
		sqlNullableString(row.LastModified),
		row.NotModified,
		esc(row.StorageClass),
		esc(row.FetchMetadata),
	)
	return s.runner.ApplySQL(ctx, query)
}

func (s clickhouseStore) LoadRawDocument(ctx context.Context, rawID string) (rawDocumentResult, error) {
	query := fmt.Sprintf(`SELECT raw_id, fetch_id, source_id, url, final_url, fetched_at, status_code, content_type, content_hash, body_bytes, object_key, etag, last_modified, not_modified, storage_class, fetch_metadata
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

func (s clickhouseStore) IsDuplicateRawDocument(ctx context.Context, row fetch.RawDocumentRow) (bool, error) {
	latest, exists, err := s.latestRawFingerprint(ctx, row.SourceID, row.FinalURL, row.URL)
	if err != nil || !exists {
		return false, err
	}
	return latest == rawDocumentFingerprint(row), nil
}

func (s clickhouseStore) latestRawFingerprint(ctx context.Context, sourceID, finalURL, fallbackURL string) (string, bool, error) {
	canonicalURL := strings.TrimSpace(finalURL)
	if canonicalURL == "" {
		canonicalURL = strings.TrimSpace(fallbackURL)
	}
	query := fmt.Sprintf(`SELECT concat(
	toString(status_code), '|',
	content_hash, '|',
	ifNull(etag, ''), '|',
	ifNull(last_modified, ''), '|',
	if(final_url = '', url, final_url)
) AS fingerprint
FROM bronze.raw_document
WHERE source_id = '%s'
  AND if(final_url = '', url, final_url) = '%s'
ORDER BY fetched_at DESC
LIMIT 1
FORMAT TabSeparated`, esc(sourceID), esc(canonicalURL))
	output, err := s.runner.Query(ctx, query)
	if err != nil {
		if strings.Contains(err.Error(), "UNKNOWN_TABLE") {
			return "", false, nil
		}
		return "", false, err
	}
	fingerprint := strings.TrimSpace(output)
	if fingerprint == "" {
		return "", false, nil
	}
	return fingerprint, true, nil
}

func (s clickhouseStore) ClaimFrontierLease(ctx context.Context, sourceID, owner string, leaseDuration time.Duration) (discovery.FrontierEntry, bool, error) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	query := buildClaimFrontierLeaseSelectQuery(sourceID, now)
	output, err := s.runner.Query(ctx, query)
	if err != nil {
		return discovery.FrontierEntry{}, false, err
	}
	line := strings.TrimSpace(output)
	if line == "" {
		return discovery.FrontierEntry{}, false, nil
	}
	var row frontierLeaseRow
	if err := json.Unmarshal([]byte(line), &row); err != nil {
		return discovery.FrontierEntry{}, false, err
	}
	entry := discovery.FrontierEntry{
		SourceID:      row.SourceID,
		Domain:        row.Domain,
		URL:           row.URL,
		CanonicalURL:  row.CanonicalURL,
		State:         row.State,
		AttemptCount:  row.AttemptCount,
		ETag:          row.ETag,
		LastModified:  row.LastModified,
		DiscoveryKind: row.DiscoveryKind,
	}
	leased := entry.ClaimLease(owner, leaseDuration, now)
	leaseExpiresAt := now.Add(leaseDuration)
	if leased.LeaseExpiresAt != nil {
		leaseExpiresAt = leased.LeaseExpiresAt.UTC()
	}
	lastAttemptAt := now
	if leased.LastAttemptAt != nil {
		lastAttemptAt = leased.LastAttemptAt.UTC()
	}
	update := buildClaimFrontierLeaseUpdateQuery(sourceID, owner, leased.CanonicalURL, leased.AttemptCount, leaseExpiresAt, lastAttemptAt, now)
	if err := s.runner.ApplySQL(ctx, update); err != nil {
		return discovery.FrontierEntry{}, false, err
	}
	return leased, true, nil
}

func buildClaimFrontierLeaseSelectQuery(sourceID string, now time.Time) string {
	return fmt.Sprintf(`SELECT source_id, domain, url, canonical_url, state, attempt_count, etag, last_modified, discovery_kind
FROM ops.crawl_frontier
WHERE source_id = '%s'
  AND %s
ORDER BY priority DESC, next_fetch_at ASC, canonical_url ASC
LIMIT 1
FORMAT JSONEachRow`, esc(sourceID), buildClaimFrontierLeaseEligibilityPredicate(now))
}

func buildClaimFrontierLeaseUpdateQuery(sourceID, owner, canonicalURL string, attemptCount uint16, leaseExpiresAt, lastAttemptAt, now time.Time) string {
	return fmt.Sprintf(`ALTER TABLE ops.crawl_frontier
UPDATE state = '%s',
       lease_owner = '%s',
       lease_expires_at = toDateTime64('%s', 3, 'UTC'),
       attempt_count = %d,
       last_attempt_at = toDateTime64('%s', 3, 'UTC')
WHERE source_id = '%s'
  AND canonical_url = '%s'
  AND %s`,
		esc(discovery.FrontierStateLeased),
		esc(owner),
		esc(formatClickHouseTime(leaseExpiresAt)),
		attemptCount,
		esc(formatClickHouseTime(lastAttemptAt)),
		esc(sourceID),
		esc(canonicalURL),
		buildClaimFrontierLeaseEligibilityPredicate(now),
	)
}

func buildClaimFrontierLeaseEligibilityPredicate(now time.Time) string {
	timestamp := esc(formatClickHouseTime(now))
	return fmt.Sprintf(`(
    state = 'pending'
    OR (state = 'retry' AND next_fetch_at <= toDateTime64('%s', 3, 'UTC'))
    OR (state = 'leased' AND lease_expires_at IS NOT NULL AND lease_expires_at <= toDateTime64('%s', 3, 'UTC'))
  )`, timestamp, timestamp)
}

func (s clickhouseStore) UpdateFrontierEntry(ctx context.Context, expectedLeaseOwner string, entry discovery.FrontierEntry) error {
	query := buildUpdateFrontierEntryQuery(expectedLeaseOwner, entry)
	return s.runner.ApplySQL(ctx, query)
}

func buildUpdateFrontierEntryQuery(expectedLeaseOwner string, entry discovery.FrontierEntry) string {
	whereClause := fmt.Sprintf("source_id = '%s' AND canonical_url = '%s'", esc(entry.SourceID), esc(entry.CanonicalURL))
	if trimmedOwner := strings.TrimSpace(expectedLeaseOwner); trimmedOwner != "" {
		whereClause += fmt.Sprintf(" AND state = 'leased' AND lease_owner = '%s'", esc(trimmedOwner))
	}
	return fmt.Sprintf(`ALTER TABLE ops.crawl_frontier
UPDATE state = '%s',
       lease_owner = NULL,
       lease_expires_at = NULL,
       attempt_count = %d,
       last_attempt_at = %s,
       last_fetch_id = %s,
       last_status_code = %s,
       last_error_code = %s,
       last_error_message = %s,
       next_fetch_at = toDateTime64('%s', 3, 'UTC'),
       etag = %s,
       last_modified = %s
WHERE %s`,
		esc(entry.State),
		entry.AttemptCount,
		sqlNullableTime(entry.LastAttemptAt),
		sqlNullableString(entry.LastFetchID),
		sqlNullableUInt16(entry.LastStatusCode),
		sqlNullableString(entry.LastErrorCode),
		sqlNullableString(entry.LastErrorMessage),
		esc(formatClickHouseTime(entry.NextFetchAt)),
		sqlNullableString(entry.ETag),
		sqlNullableString(entry.LastModified),
		whereClause,
	)
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
		SourceID:         record.SourceID,
		RetentionClass:   retentionClass,
		Disabled:         record.Enabled == 0,
		DisabledReason:   disabledReason,
		AuthMode:         strings.TrimSpace(record.AuthMode),
		MaxBodyBytes:     maxBodyBytes,
		ForceObjectStore: strings.TrimSpace(record.TransportType) == "http" && record.BronzeTable != nil,
		SupportsLiveGET:  true,
	}
}

func newS3Client(cfg config) (*objectstore.Client, error) {
	return objectstore.New(cfg.MinIOEndpoint, cfg.MinIOAccessKey, cfg.MinIOSecretKey, cfg.MinIORegion)
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

func prepareSourceRequest(policy sourcePolicyRecord, requestURL string) (http.Header, string, error) {
	preparedURL, err := buildSourceRequestURL(policy.SourceID, requestURL)
	if err != nil {
		return nil, "", err
	}
	return resolveAuthRequest(policy, preparedURL)
}

func sanitizeFetchPersistence(policy sourcePolicyRecord, safeRequestURL string, request fetch.Request, response fetch.Response) (fetch.Request, fetch.Response) {
	sanitizedRequest := request
	sanitizedResponse := response
	contract, ok := authContractForPersistence(policy)
	if !ok {
		return sanitizedRequest, sanitizedResponse
	}
	placement := authPlacementForPersistence(policy.AuthMode, contract)
	switch placement {
	case "query":
		safeURL := strings.TrimSpace(safeRequestURL)
		if safeURL != "" {
			sanitizedRequest.URL = safeURL
			sanitizedResponse.FetchURL = stripQueryAuthValue(sanitizedResponse.FetchURL, contract)
			sanitizedResponse.FinalURL = stripQueryAuthValue(sanitizedResponse.FinalURL, contract)
		}
	case "header":
		sanitizedResponse.RequestHeaders = redactNamedHeaders(response.RequestHeaders, authHeaderNameForPersistence(policy.AuthMode, contract))
	case "cookie":
		sanitizedResponse.RequestHeaders = redactNamedHeaders(response.RequestHeaders, "Cookie")
	}
	return sanitizedRequest, sanitizedResponse
}

func authContractForPersistence(policy sourcePolicyRecord) (authConfig, bool) {
	authMode := strings.ToLower(strings.TrimSpace(policy.AuthMode))
	if authMode == "" || authMode == "none" {
		return authConfig{}, false
	}
	config := strings.TrimSpace(policy.AuthConfigJSON)
	if config == "" {
		return authConfig{}, false
	}
	var contract authConfig
	if err := json.Unmarshal([]byte(config), &contract); err != nil {
		return authConfig{}, false
	}
	return contract, true
}

func authPlacementForPersistence(authMode string, contract authConfig) string {
	placement := strings.ToLower(strings.TrimSpace(contract.Placement))
	if placement != "" {
		return placement
	}
	if strings.EqualFold(strings.TrimSpace(authMode), "oauth2_client_credentials") {
		return "header"
	}
	return placement
}

func authHeaderNameForPersistence(authMode string, contract authConfig) string {
	name := strings.TrimSpace(contract.Name)
	if name != "" {
		return name
	}
	if strings.EqualFold(strings.TrimSpace(authMode), "oauth2_client_credentials") {
		return "Authorization"
	}
	return name
}

func redactNamedHeaders(headers map[string][]string, names ...string) map[string][]string {
	if len(headers) == 0 {
		return nil
	}
	redacted := cloneHeaderValues(headers)
	lookup := map[string]struct{}{}
	for _, name := range names {
		trimmed := strings.ToLower(strings.TrimSpace(name))
		if trimmed == "" {
			continue
		}
		lookup[trimmed] = struct{}{}
	}
	for name := range redacted {
		if _, ok := lookup[strings.ToLower(strings.TrimSpace(name))]; ok {
			redacted[name] = []string{"[REDACTED]"}
		}
	}
	return redacted
}

func stripQueryAuthValue(rawURL string, contract authConfig) string {
	trimmed := strings.TrimSpace(rawURL)
	if trimmed == "" {
		return trimmed
	}
	name := strings.TrimSpace(contract.Name)
	if name == "" {
		return trimmed
	}
	placement := strings.ToLower(strings.TrimSpace(contract.Placement))
	if placement != "query" {
		return trimmed
	}
	parsed, err := url.Parse(trimmed)
	if err != nil {
		return trimmed
	}
	query := parsed.Query()
	if query.Get(name) == "" {
		return trimmed
	}
	query.Del(name)
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

func cloneHeaderValues(headers map[string][]string) map[string][]string {
	if len(headers) == 0 {
		return nil
	}
	cloned := make(map[string][]string, len(headers))
	for name, values := range headers {
		cloned[name] = append([]string(nil), values...)
	}
	return cloned
}

func buildSourceRequestURL(sourceID, requestURL string) (string, error) {
	trimmedURL := strings.TrimSpace(requestURL)
	if trimmedURL == "" {
		return "", fmt.Errorf("request url is empty")
	}
	parsed, err := url.Parse(trimmedURL)
	if err != nil {
		return "", fmt.Errorf("parse request url: %w", err)
	}

	switch strings.TrimSpace(sourceID) {
	case "catalog:auto:aviation-airports-drones-and-mobility-opensky-network":
		query := parsed.Query()
		if strings.TrimSpace(query.Get("extended")) == "" {
			query.Set("extended", "1")
		}
		parsed.RawQuery = query.Encode()
		return parsed.String(), nil
	case "catalog:auto:maritime-ocean-and-coastal-sources-aishub":
		query := parsed.Query()
		if strings.EqualFold(strings.TrimSpace(parsed.Path), "/ws.php") || strings.HasSuffix(strings.ToLower(strings.TrimSpace(parsed.Path)), "/ws.php") {
			if strings.TrimSpace(query.Get("format")) == "" {
				query.Set("format", "1")
			}
			if strings.TrimSpace(query.Get("output")) == "" {
				query.Set("output", "json")
			}
			if strings.TrimSpace(query.Get("compress")) == "" {
				query.Set("compress", "2")
			}
			if strings.TrimSpace(query.Get("latmin")) == "" {
				query.Set("latmin", "-90")
			}
			if strings.TrimSpace(query.Get("latmax")) == "" {
				query.Set("latmax", "90")
			}
			if strings.TrimSpace(query.Get("lonmin")) == "" {
				query.Set("lonmin", "-180")
			}
			if strings.TrimSpace(query.Get("lonmax")) == "" {
				query.Set("lonmax", "180")
			}
			if strings.TrimSpace(query.Get("interval")) == "" {
				query.Set("interval", "5")
			}
		}
		parsed.RawQuery = query.Encode()
		return parsed.String(), nil
	default:
		return parsed.String(), nil
	}
}

func resolveAuthRequest(policy sourcePolicyRecord, requestURL string) (http.Header, string, error) {
	headers := http.Header{}
	authMode := strings.ToLower(strings.TrimSpace(policy.AuthMode))
	if authMode == "" || authMode == "none" {
		return headers, requestURL, nil
	}

	config := strings.TrimSpace(policy.AuthConfigJSON)
	if config == "" {
		return nil, "", fmt.Errorf("%w: auth_mode=%s missing auth_config_json", errMissingCredential, authMode)
	}

	var contract authConfig
	if err := json.Unmarshal([]byte(config), &contract); err != nil {
		return nil, "", fmt.Errorf("unsupported auth_config_json: %w", err)
	}

	switch authMode {
	case "user_supplied_key":
		return resolveUserSuppliedKeyAuthRequest(contract, requestURL)
	case "oauth2_client_credentials":
		return resolveOAuth2ClientCredentialsAuthRequest(contract, requestURL)
	default:
		return nil, "", fmt.Errorf("unsupported auth_mode %q", policy.AuthMode)
	}
}

func resolveUserSuppliedKeyAuthRequest(contract authConfig, requestURL string) (http.Header, string, error) {
	headers := http.Header{}
	envVar := strings.TrimSpace(contract.EnvVar)
	if envVar == "" {
		return nil, "", fmt.Errorf("unsupported auth config: env_var is required")
	}
	secret := strings.TrimSpace(os.Getenv(envVar))
	if secret == "" {
		return nil, "", fmt.Errorf("%w: env var %s is not set", errMissingCredential, envVar)
	}
	name := strings.TrimSpace(contract.Name)
	if name == "" {
		return nil, "", fmt.Errorf("unsupported auth config: name is required")
	}
	value := strings.TrimSpace(contract.Prefix) + secret
	placement := strings.ToLower(strings.TrimSpace(contract.Placement))
	switch placement {
	case "header":
		headers.Set(name, value)
		return headers, requestURL, nil
	case "query":
		parsed, err := url.Parse(requestURL)
		if err != nil {
			return nil, "", fmt.Errorf("parse request url: %w", err)
		}
		query := parsed.Query()
		query.Set(name, value)
		parsed.RawQuery = query.Encode()
		return headers, parsed.String(), nil
	case "cookie":
		headers.Set("Cookie", fmt.Sprintf("%s=%s", name, value))
		return headers, requestURL, nil
	default:
		return nil, "", fmt.Errorf("unsupported auth placement %q", contract.Placement)
	}
}

func resolveOAuth2ClientCredentialsAuthRequest(contract authConfig, requestURL string) (http.Header, string, error) {
	headers := http.Header{}
	clientIDEnvVar := strings.TrimSpace(contract.ClientIDEnvVar)
	clientSecretEnvVar := strings.TrimSpace(contract.ClientSecretEnvVar)
	if clientIDEnvVar == "" || clientSecretEnvVar == "" {
		return nil, "", fmt.Errorf("unsupported auth config: client_id_env_var and client_secret_env_var are required")
	}
	clientID := strings.TrimSpace(os.Getenv(clientIDEnvVar))
	if clientID == "" {
		return nil, "", fmt.Errorf("%w: env var %s is not set", errMissingCredential, clientIDEnvVar)
	}
	clientSecret := strings.TrimSpace(os.Getenv(clientSecretEnvVar))
	if clientSecret == "" {
		return nil, "", fmt.Errorf("%w: env var %s is not set", errMissingCredential, clientSecretEnvVar)
	}
	tokenURL := strings.TrimSpace(contract.TokenURL)
	if tokenURL == "" {
		return nil, "", fmt.Errorf("unsupported auth config: token_url is required")
	}
	grantType := strings.TrimSpace(contract.GrantType)
	if grantType == "" {
		grantType = "client_credentials"
	}
	if grantType != "client_credentials" {
		return nil, "", fmt.Errorf("unsupported oauth2 grant_type %q", grantType)
	}
	scope := strings.TrimSpace(contract.Scope)
	token, err := oauthTokenForClientCredentials(tokenURL, clientID, clientSecret, scope)
	if err != nil {
		return nil, "", err
	}
	name := strings.TrimSpace(contract.Name)
	if name == "" {
		name = "Authorization"
	}
	prefix := strings.TrimSpace(contract.Prefix)
	if prefix == "" {
		prefix = "Bearer "
	} else if !strings.HasSuffix(prefix, " ") {
		prefix += " "
	}
	placement := strings.ToLower(strings.TrimSpace(contract.Placement))
	if placement == "" {
		placement = "header"
	}
	if placement != "header" {
		return nil, "", fmt.Errorf("unsupported oauth2 auth placement %q", contract.Placement)
	}
	headers.Set(name, prefix+token)
	return headers, requestURL, nil
}

func oauthTokenForClientCredentials(tokenURL, clientID, clientSecret, scope string) (string, error) {
	cacheKey := strings.Join([]string{strings.TrimSpace(tokenURL), strings.TrimSpace(clientID), strings.TrimSpace(scope)}, "|")
	now := time.Now().UTC()
	oauthTokenCache.mu.Lock()
	if cached, ok := oauthTokenCache.entries[cacheKey]; ok {
		if strings.TrimSpace(cached.Token) != "" && cached.ExpiresAt.After(now.Add(30*time.Second)) {
			token := cached.Token
			oauthTokenCache.mu.Unlock()
			return token, nil
		}
	}
	oauthTokenCache.mu.Unlock()

	issuedToken, expiresAt, err := issueClientCredentialsToken(tokenURL, clientID, clientSecret, scope)
	if err != nil {
		return "", err
	}
	oauthTokenCache.mu.Lock()
	oauthTokenCache.entries[cacheKey] = oauthTokenCacheEntry{Token: issuedToken, ExpiresAt: expiresAt}
	oauthTokenCache.mu.Unlock()
	return issuedToken, nil
}

func issueClientCredentialsToken(tokenURL, clientID, clientSecret, scope string) (string, time.Time, error) {
	form := url.Values{}
	form.Set("grant_type", "client_credentials")
	form.Set("client_id", clientID)
	form.Set("client_secret", clientSecret)
	if strings.TrimSpace(scope) != "" {
		form.Set("scope", strings.TrimSpace(scope))
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, tokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return "", time.Time{}, fmt.Errorf("build oauth2 token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := (&http.Client{Timeout: 15 * time.Second}).Do(req)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("oauth2 token request failed: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("read oauth2 token response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		message := strings.TrimSpace(string(body))
		if len(message) > 240 {
			message = message[:240]
		}
		return "", time.Time{}, fmt.Errorf("oauth2 token request returned %s: %s", resp.Status, message)
	}
	var payload struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int64  `json:"expires_in"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return "", time.Time{}, fmt.Errorf("decode oauth2 token response: %w", err)
	}
	token := strings.TrimSpace(payload.AccessToken)
	if token == "" {
		return "", time.Time{}, fmt.Errorf("oauth2 token response missing access_token")
	}
	expiresIn := payload.ExpiresIn
	if expiresIn <= 0 {
		expiresIn = 1800
	}
	return token, time.Now().UTC().Add(time.Duration(expiresIn) * time.Second), nil
}

func resetOAuthTokenCache() {
	oauthTokenCache.mu.Lock()
	oauthTokenCache.entries = map[string]oauthTokenCacheEntry{}
	oauthTokenCache.mu.Unlock()
}

func frontierOutcomeFromFetch(fetchID string, attemptedAt time.Time, response fetch.Response, fetchErr error, retryPolicy sharedretry.Policy) discovery.FetchOutcome {
	outcome := discovery.FetchOutcome{
		FetchID:      strings.TrimSpace(fetchID),
		StatusCode:   uint16(response.StatusCode),
		ETag:         strings.TrimSpace(response.ETag),
		LastModified: strings.TrimSpace(response.LastModified),
		AttemptedAt:  attemptedAt.UTC(),
		RetryPolicy:  retryPolicy.Normalize(),
	}
	if response.StatusCode == http.StatusNotFound {
		outcome.ErrorCode = discovery.FrontierErrorNotFound
	}
	if response.StatusCode == http.StatusGone {
		outcome.ErrorCode = discovery.FrontierErrorGone
	}
	if response.StatusCode == http.StatusTooManyRequests {
		outcome.ErrorCode = discovery.FrontierErrorRateLimited
	}
	if response.StatusCode >= http.StatusInternalServerError {
		outcome.ErrorCode = discovery.FrontierErrorUpstream
	}
	if fetchErr != nil {
		msg := strings.TrimSpace(fetchErr.Error())
		outcome.ErrorMessage = msg
		switch {
		case errors.Is(fetchErr, fetch.ErrSourceBlocked):
			outcome.ErrorCode = discovery.FrontierErrorDisabled
		case errors.Is(fetchErr, fetch.ErrBodyTooLarge):
			outcome.ErrorCode = discovery.FrontierErrorBodyTooLarge
		case errors.Is(fetchErr, context.DeadlineExceeded):
			outcome.ErrorCode = discovery.FrontierErrorTimeout
		case strings.Contains(strings.ToLower(msg), "retryable status"):
			if response.StatusCode == http.StatusTooManyRequests {
				outcome.ErrorCode = discovery.FrontierErrorRateLimited
			} else {
				outcome.ErrorCode = discovery.FrontierErrorUpstream
			}
		default:
			if outcome.ErrorCode == "" {
				outcome.ErrorCode = discovery.FrontierErrorNetwork
			}
		}
	}
	if outcome.ErrorMessage == "" {
		outcome.ErrorMessage = strings.TrimSpace(response.ErrorMessage)
	}
	return outcome
}

func nilString(value *string) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(*value)
}

func writeJSONResult(w io.Writer, value any) int {
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(value); err != nil {
		return 1
	}
	return 0
}

func sqlNullableTime(value *time.Time) string {
	if value == nil || value.IsZero() {
		return "NULL"
	}
	return fmt.Sprintf("toDateTime64('%s', 3, 'UTC')", esc(formatClickHouseTime(*value)))
}

func formatClickHouseTime(value time.Time) string {
	return value.UTC().Format(clickHouseTimeLayout)
}

func normalizeClickHouseTimeString(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return formatClickHouseTime(time.Now().UTC())
	}
	if parsed, err := time.Parse(time.RFC3339Nano, trimmed); err == nil {
		return formatClickHouseTime(parsed)
	}
	if parsed, err := time.Parse(clickHouseTimeLayout, trimmed); err == nil {
		return formatClickHouseTime(parsed)
	}
	return trimmed
}

func rawDocumentFingerprint(row fetch.RawDocumentRow) string {
	canonicalURL := strings.TrimSpace(row.FinalURL)
	if canonicalURL == "" {
		canonicalURL = strings.TrimSpace(row.URL)
	}
	etag := ""
	if row.ETag != nil {
		etag = strings.TrimSpace(*row.ETag)
	}
	lastModified := ""
	if row.LastModified != nil {
		lastModified = strings.TrimSpace(*row.LastModified)
	}
	return strings.Join([]string{
		strconv.FormatUint(uint64(row.StatusCode), 10),
		strings.TrimSpace(row.ContentHash),
		etag,
		lastModified,
		canonicalURL,
	}, "|")
}

func sqlNullableUInt16(value *uint16) string {
	if value == nil {
		return "NULL"
	}
	return strconv.FormatUint(uint64(*value), 10)
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
