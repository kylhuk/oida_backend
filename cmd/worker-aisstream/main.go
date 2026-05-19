package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"

	"global-osint-backend/internal/fetch"
	"global-osint-backend/internal/migrate"
)

const (
	defaultWSURL       = "wss://stream.aisstream.io/v0/stream"
	defaultBatchWindow = 5 * time.Second
	defaultRawBucket   = "raw"
	aistreamSourceID   = "catalog:auto:maritime-ocean-and-coastal-sources-aisstream"
	defaultCHURL       = "http://clickhouse:8123"
	defaultMinIOEndpoint = "http://minio:9000"
)

// config holds all configuration for the worker.
type config struct {
	WSURL          string
	APIKey         string
	BatchWindow    time.Duration
	ClickHouseURL  string
	MinioEndpoint  string
	MinioAccessKey string
	MinioSecretKey string
	RawBucket      string
	SourceID       string
}

// subscribeMessage is the JSON payload sent to AISstream on connect.
type subscribeMessage struct {
	APIKey             string       `json:"APIKey"`
	BoundingBoxes      [][][2]float64 `json:"BoundingBoxes"`
	FilterMessageTypes []string     `json:"FilterMessageTypes"`
}

// retainer is an interface over the batch-persist operation so tests can mock it.
type retainer interface {
	retain(ctx context.Context, cfg config, batch []json.RawMessage, fetchedAt time.Time) error
}

// realRetainer uses fetch.RetainResponse + ClickHouse.
type realRetainer struct {
	runner      *migrate.HTTPRunner
	objectStore fetch.ObjectStore
}

func (r *realRetainer) retain(ctx context.Context, cfg config, batch []json.RawMessage, fetchedAt time.Time) error {
	return flushBatch(ctx, cfg, batch, fetchedAt, r.runner, r.objectStore)
}

// loadConfig builds a config from environment variables.
func loadConfig() config {
	cfg := config{
		WSURL:          getenv("AISSTREAM_WS_URL", defaultWSURL),
		APIKey:         getenv("SOURCE_AISSTREAM_API_KEY", ""),
		BatchWindow:    getenvDuration("AISSTREAM_BATCH_WINDOW", defaultBatchWindow),
		ClickHouseURL:  getenv("CLICKHOUSE_HTTP_URL", defaultCHURL),
		MinioEndpoint:  getenv("MINIO_ENDPOINT", defaultMinIOEndpoint),
		MinioAccessKey: getenv("MINIO_ACCESS_KEY", getenv("MINIO_ROOT_USER", "minioadmin")),
		MinioSecretKey: getenv("MINIO_SECRET_KEY", getenv("MINIO_ROOT_PASSWORD", "minioadmin")),
		RawBucket:      getenv("RAW_BUCKET", defaultRawBucket),
		SourceID:       aistreamSourceID,
	}
	return cfg
}

// buildRetainer constructs the production retainer wired to ClickHouse + MinIO.
func buildRetainer(cfg config) retainer {
	runner := migrate.NewHTTPRunner(cfg.ClickHouseURL)
	store := newS3ObjectStore(cfg)
	return &realRetainer{runner: runner, objectStore: store}
}

// runStreamLoop drives reconnection with exponential back-off.
func runStreamLoop(ctx context.Context, cfg config, r retainer) {
	backoff := 1 * time.Second
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		err := runOnce(ctx, cfg, r)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("worker-aisstream: connection error: %v — retrying in %s", err, backoff)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			backoff = minDuration(backoff*2, 60*time.Second)
		} else {
			backoff = 1 * time.Second
		}
	}
}

// runOnce manages a single WebSocket connection lifecycle.
func runOnce(ctx context.Context, cfg config, r retainer) error {
	conn, _, _, err := ws.Dial(ctx, cfg.WSURL)
	if err != nil {
		return fmt.Errorf("dial %s: %w", cfg.WSURL, err)
	}
	defer conn.Close()

	sub := subscribeMessage{
		APIKey:             cfg.APIKey,
		BoundingBoxes:      [][][2]float64{{{-90, -180}, {90, 180}}},
		FilterMessageTypes: []string{},
	}
	subJSON, err := json.Marshal(sub)
	if err != nil {
		return fmt.Errorf("marshal subscribe: %w", err)
	}
	if err := wsutil.WriteClientText(conn, subJSON); err != nil {
		return fmt.Errorf("send subscribe: %w", err)
	}

	var batch []json.RawMessage
	batchStart := time.Now().UTC()
	firstMsg := true

	ticker := time.NewTicker(cfg.BatchWindow)
	defer ticker.Stop()

	// Use a channel to receive WS messages so we can also listen to the ticker
	// and context cancellation.
	type wsMsg struct {
		data []byte
		err  error
	}
	msgCh := make(chan wsMsg, 64)
	done := make(chan struct{})
	defer close(done)
	go func() {
		for {
			data, opCode, err := wsutil.ReadServerData(conn)
			if err != nil {
				select {
				case msgCh <- wsMsg{err: err}:
				case <-done:
				}
				return
			}
			if opCode != ws.OpText && opCode != ws.OpBinary {
				continue
			}
			select {
			case msgCh <- wsMsg{data: data}:
			case <-done:
				return
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			if len(batch) > 0 {
				flushCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				if err := r.retain(flushCtx, cfg, batch, batchStart); err != nil {
					log.Printf("worker-aisstream: shutdown flush: %v", err)
				}
				cancel()
			}
			return nil

		case <-ticker.C:
			if len(batch) > 0 {
				fetchedAt := batchStart
				b := batch
				batch = nil
				batchStart = time.Now().UTC()
				if err := r.retain(ctx, cfg, b, fetchedAt); err != nil {
					log.Printf("worker-aisstream: flush batch: %v", err)
				}
			}

		case m := <-msgCh:
			if m.err != nil {
				return fmt.Errorf("read ws: %w", m.err)
			}

			// Check first message for auth errors.
			if firstMsg {
				firstMsg = false
				var probe map[string]json.RawMessage
				if json.Unmarshal(m.data, &probe) == nil {
					if _, hasErr := probe["error"]; hasErr {
						return fmt.Errorf("subscription rejected by server: %s", string(m.data))
					}
				}
			}

			batch = append(batch, json.RawMessage(m.data))
		}
	}
}

// flushBatch persists a batch of AISstream messages as a single raw_document row.
func flushBatch(ctx context.Context, cfg config, batch []json.RawMessage, fetchedAt time.Time, runner *migrate.HTTPRunner, objectStore fetch.ObjectStore) error {
	body, err := json.Marshal(batch)
	if err != nil {
		return fmt.Errorf("marshal batch: %w", err)
	}

	contentHash := hashBytes(body)
	ts := fetchedAt.UTC().Format("20060102150405")
	fetchID := "fetch:aisstream:" + urlHashShort(cfg.WSURL) + ":" + ts
	rawID := "raw:aisstream:" + urlHashShort(cfg.WSURL) + ":" + ts + ":" + contentHash[:8]

	req := fetch.Request{
		Method: "STREAM",
		URL:    cfg.WSURL,
		Source: fetch.SourcePolicy{
			SourceID:         cfg.SourceID,
			RetentionClass:   "warm",
			SupportsLiveGET:  true,
			ForceObjectStore: false,
		},
	}
	fetchMetaAttrs, _ := json.Marshal(map[string]any{
		"aisstream": map[string]any{
			"batch_size":     len(batch),
			"window_seconds": int(cfg.BatchWindow.Seconds()),
		},
	})

	resp := fetch.Response{
		FetchURL:           cfg.WSURL,
		FinalURL:           cfg.WSURL,
		SourceID:           cfg.SourceID,
		Method:             "STREAM",
		StatusCode:         200,
		Success:            true,
		FetchedAt:          fetchedAt,
		Latency:            cfg.BatchWindow,
		Attempts:           1,
		Body:               body,
		BodyBytes:          int64(len(body)),
		ContentHash:        contentHash,
		ContentType:        "application/json",
		HeaderContentType:  "application/json",
		SniffedContentType: "application/json",
	}

	policy := fetch.ResolveRetentionPolicy("warm")
	policy.ObjectPrefix = "aisstream"

	stored, err := fetch.RetainResponse(ctx, fetch.PersistOptions{
		FetchID:  fetchID,
		RawID:    rawID,
		SourceID: cfg.SourceID,
		Bucket:   cfg.RawBucket,
		Policy:   policy,
		Now:      fetchedAt,
	}, req, resp, objectStore)
	if err != nil {
		return fmt.Errorf("retain response: %w", err)
	}
	if stored.RawDocument == nil {
		return fmt.Errorf("retention did not produce raw document")
	}

	// Enrich fetch_metadata with aisstream attrs.
	raw := stored.RawDocument
	raw.FetchMetadata = enrichAIStreamMetadata(raw.FetchMetadata, string(fetchMetaAttrs))

	fetchLog := stored.FetchLog
	fetchSQL := fmt.Sprintf(`INSERT INTO ops.fetch_log
(fetch_id, source_id, url_hash, status_code, success, fetched_at, latency_ms, body_bytes, error_message)
VALUES ('%s','%s','%s',%d,1,toDateTime64('%s', 3, 'UTC'),%d,%d,NULL)`,
		esc(fetchLog.FetchID), esc(fetchLog.SourceID), esc(fetchLog.URLHash),
		fetchLog.StatusCode, esc(fetchLog.FetchedAt),
		fetchLog.LatencyMS, fetchLog.BodyBytes)

	rawSQL := fmt.Sprintf(`INSERT INTO bronze.raw_document
(raw_id, fetch_id, source_id, url, final_url, fetched_at, status_code, content_type, content_hash, body_bytes, object_key, storage_class, fetch_metadata)
VALUES ('%s','%s','%s','%s','%s',toDateTime64('%s', 3, 'UTC'),%d,'application/json','%s',%d,%s,'%s','%s')`,
		esc(raw.RawID), esc(raw.FetchID), esc(raw.SourceID),
		esc(raw.URL), esc(raw.FinalURL), esc(raw.FetchedAt),
		raw.StatusCode, esc(raw.ContentHash), raw.BodyBytes,
		sqlNullableString(raw.ObjectKey), esc(raw.StorageClass), esc(raw.FetchMetadata))

	if err := runner.ApplySQL(ctx, fetchSQL); err != nil {
		return fmt.Errorf("insert fetch_log: %w", err)
	}
	return runner.ApplySQL(ctx, rawSQL)
}

// enrichAIStreamMetadata merges aisstream attrs into the fetch_metadata JSON blob.
func enrichAIStreamMetadata(fetchMeta, attrsJSON string) string {
	var meta map[string]any
	if err := json.Unmarshal([]byte(fetchMeta), &meta); err != nil {
		return fetchMeta
	}
	var attrs map[string]any
	if err := json.Unmarshal([]byte(attrsJSON), &attrs); err != nil {
		return fetchMeta
	}
	for k, v := range attrs {
		meta[k] = v
	}
	out, err := json.Marshal(meta)
	if err != nil {
		return fetchMeta
	}
	return string(out)
}

func main() {
	cfg := loadConfig()
	if cfg.APIKey == "" {
		log.Fatal("worker-aisstream: SOURCE_AISSTREAM_API_KEY is required")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	log.Printf("worker-aisstream: starting, source=%s ws=%s batch_window=%s",
		cfg.SourceID, cfg.WSURL, cfg.BatchWindow)

	r := buildRetainer(cfg)
	runStreamLoop(ctx, cfg, r)
	log.Println("worker-aisstream: shutdown complete")
}

// --------------------------------------------------------------------------
// S3 / MinIO object store (same pattern as worker-vesselfinder)
// --------------------------------------------------------------------------

type s3ObjectStore struct {
	endpoint  *url.URL
	accessKey string
	secretKey string
	httpClient *http.Client
}

func newS3ObjectStore(cfg config) *s3ObjectStore {
	endpoint, err := url.Parse(cfg.MinioEndpoint)
	if err != nil || endpoint.Scheme == "" || endpoint.Host == "" {
		log.Printf("worker-aisstream: invalid MinIO endpoint %q, object store disabled", cfg.MinioEndpoint)
		return nil
	}
	return &s3ObjectStore{
		endpoint:   endpoint,
		accessKey:  cfg.MinioAccessKey,
		secretKey:  cfg.MinioSecretKey,
		httpClient: &http.Client{Timeout: 30 * time.Second},
	}
}

func (s *s3ObjectStore) PutObject(ctx context.Context, bucket, key string, body []byte, contentType string) error {
	putURL := *s.endpoint
	putURL.Path = "/" + bucket + "/" + key
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, putURL.String(), bytes.NewReader(body))
	if err != nil {
		return err
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body) //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("minio PUT %s: status %d", key, resp.StatusCode)
	}
	return nil
}

func (s *s3ObjectStore) GetObject(ctx context.Context, bucket, key string) ([]byte, string, error) {
	return nil, "", fmt.Errorf("GetObject not implemented in worker-aisstream")
}

// --------------------------------------------------------------------------
// Helpers
// --------------------------------------------------------------------------

func getenv(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func getenvDuration(key string, fallback time.Duration) time.Duration {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	value, err := time.ParseDuration(raw)
	if err == nil && value > 0 {
		return value
	}
	seconds, err := strconv.Atoi(raw)
	if err != nil || seconds <= 0 {
		return fallback
	}
	return time.Duration(seconds) * time.Second
}

func hashBytes(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func urlHashShort(value string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(value)))
	return hex.EncodeToString(sum[:])[:16]
}

func esc(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	return strings.ReplaceAll(value, `'`, `\'`)
}

func sqlNullableString(value *string) string {
	if value == nil || strings.TrimSpace(*value) == "" {
		return "NULL"
	}
	return fmt.Sprintf("'%s'", esc(*value))
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
