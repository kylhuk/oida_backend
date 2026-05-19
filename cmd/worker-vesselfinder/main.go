package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/cdproto/page"
	"github.com/chromedp/chromedp"

	"global-osint-backend/internal/fetch"
	"global-osint-backend/internal/migrate"
	"global-osint-backend/internal/observability"
	vf "global-osint-backend/internal/packs/maritime/vesselfinder"
	"global-osint-backend/internal/proxypool"
	"global-osint-backend/internal/throttle"
)

const (
	sourceID              = "catalog:auto:maritime-ocean-and-coastal-sources-vesselfinder"
	routeSourceID         = "catalog:auto:maritime-ocean-and-coastal-sources-vesselfinder-routes"
	defaultClickHouseURL  = "http://clickhouse:8123"
	defaultWorkers        = 3
	defaultWorkerRate     = 18
	defaultDiscoveryRPS   = 1.0
	defaultMaxPage        = 200
	defaultRediscover     = time.Hour
	defaultListTimeout    = 30 * time.Second
	defaultDetailTimeout  = 30 * time.Second
	clickHouseTimeLayout  = "2006-01-02 15:04:05.000"
	vesselFinderBaseURL   = "https://www.vesselfinder.com"
	defaultDimensionSeed  = int64(8675309)
	defaultScanBatchLimit = 3
	defaultUserAgent      = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36"
	defaultMinIOEndpoint = "http://minio:9000"
	defaultMinIORegion   = "us-east-1"
	defaultRawBucket     = "raw"
)

const (
	defaultProxySourcesFile      = "/config/proxies/proxy_sources.txt"
	defaultProxyRefreshInterval  = time.Hour
	defaultProxyValidateInterval = 5 * time.Minute
	defaultBrowserRecycleAfter   = 50
	defaultProfileDir            = "/chrome-profile"
	defaultRateFloorPerMin       = 2.0
	defaultRateCeilPerMin        = 6.0
	defaultRateRampDuration      = 30 * time.Minute
	defaultDiscoveryFloorRPS     = 0.1
	defaultDiscoveryCeilRPS      = 0.3

	defaultRouteWorkers             = 4
	defaultRouteRefreshInterval     = time.Hour
	defaultRouteBatchLimit          = 8
	defaultRouteFetchTimeout        = 30 * time.Second
	defaultRouteBrowserRecycleAfter = 50
	defaultRouteRateFloorPerMin     = 4.0
	defaultRouteRateCeilPerMin      = 15.0
	defaultRouteRateRampDuration    = 30 * time.Minute
	defaultRouteQueueRefillInterval = 5 * time.Minute
)

type config struct {
	ClickHouseHTTP      string
	SourceID            string
	Workers             int
	WorkerRatePerMinute int
	DiscoveryRPS        float64
	MaxPage             int
	RediscoveryInterval time.Duration
	ListTimeout         time.Duration
	DetailTimeout       time.Duration
	ScanBatchLimit      int
	UserAgent           string
	MinIOEndpoint       string
	MinIOAccessKey      string
	MinIOSecretKey      string
	MinIORegion         string
	RawBucket           string
	ProxySourcesFile    string
	ProxyRefreshInterval time.Duration
	ProxyValidateInterval time.Duration
	BrowserRecycleAfter int
	ProfileDir          string
	RateFloorPerMin     float64
	RateCeilPerMin      float64
	RateRampDuration    time.Duration
	DiscoveryFloorRPS   float64
	DiscoveryCeilRPS    float64

	RouteWorkers             int
	RouteRefreshInterval     time.Duration
	RouteBatchLimit          int
	RouteFetchTimeout        time.Duration
	RouteBrowserRecycleAfter int
	RouteRateFloorPerMin     float64
	RouteRateCeilPerMin      float64
	RouteRateRampDuration    time.Duration
	RouteQueueRefillInterval time.Duration
}

type renderedPage struct {
	URL        string
	HTML       string
	StatusCode int
	FetchedAt  time.Time
	Latency    time.Duration
}

type browserSession struct {
	allocCtx      context.Context
	cancelAlloc   context.CancelFunc
	browserCtx    context.Context
	cancelBrowser context.CancelFunc
	reqCount      int
	proxyURL      string
}

type store struct {
	runner *migrate.HTTPRunner
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	cfg := loadConfig()
	if len(args) == 0 {
		serve(cfg)
		return 0
	}
	switch args[0] {
	case "help", "-h", "--help":
		printUsage(stdout)
		return 0
	case "discover-once":
		return discoverOnce(cfg, args[1:], stdout, stderr)
	case "scan-once":
		return scanOnce(cfg, args[1:], stdout, stderr)
	default:
		fmt.Fprintf(stderr, "unknown command %q\n\n", args[0])
		printUsage(stderr)
		return 2
	}
}

func serve(cfg config) {
	pool := proxypool.New()
	refresher := proxypool.NewRefresher(cfg.ProxySourcesFile, pool, cfg.ProxyRefreshInterval)
	validator := proxypool.NewValidator(pool, cfg.ProxyValidateInterval)

	detailFloor := time.Duration(float64(time.Minute) / cfg.RateFloorPerMin)
	detailCeil := time.Duration(float64(time.Minute) / cfg.RateCeilPerMin)
	discFloor := time.Duration(float64(time.Second) / cfg.DiscoveryFloorRPS)
	discCeil := time.Duration(float64(time.Second) / cfg.DiscoveryCeilRPS)

	scanThrottle := throttle.New(detailFloor, detailCeil, cfg.RateRampDuration)
	discThrottle := throttle.New(discFloor, discCeil, cfg.RateRampDuration)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go refresher.Run(ctx)
	go validator.Run(ctx)

	observability.LogEvent("worker-vesselfinder", "service_started", observability.NewCorrelationID("worker-vesselfinder"), map[string]any{
		"source_id":             cfg.SourceID,
		"proxy_sources_file":    cfg.ProxySourcesFile,
		"browser_recycle_after": cfg.BrowserRecycleAfter,
	})

	go runDiscoveryLoop(cfg, pool, discThrottle)
	go runRouteLoop(cfg, pool)
	runScanLoop(cfg, pool, scanThrottle)
}

func runDiscoveryLoop(cfg config, pool *proxypool.Pool, discThrottle *throttle.Adaptive) {
	var sess *browserSession
	backoff := 30 * time.Second
	consecutiveDead := 0
	useDirect := false

	defer func() { closeBrowserSession(sess) }()

	pickProxy := func() string {
		if useDirect {
			return ""
		}
		proxyURL, ok := pool.Pick()
		if !ok {
			return ""
		}
		return proxyURL
	}

	ensureSession := func() error {
		if sess != nil && sess.reqCount < cfg.BrowserRecycleAfter {
			return nil
		}
		closeBrowserSession(sess)
		sess = nil
		proxyURL := pickProxy()
		var err error
		sess, err = newBrowserSession(context.Background(), cfg, proxyURL)
		if err != nil {
			if proxyURL != "" {
				pool.Disable(proxyURL)
			}
			return err
		}
		return nil
	}

	for {
		if err := ensureSession(); err != nil {
			observability.LogEvent("worker-vesselfinder", "discovery_no_proxy", "", map[string]any{"error": err.Error()})
			time.Sleep(30 * time.Second)
			continue
		}

		if _, err := runDiscovery(context.Background(), cfg, store{runner: migrate.NewHTTPRunner(cfg.ClickHouseHTTP)}, defaultDimensionSeed, sess, discThrottle); err != nil {
			observability.LogEvent("worker-vesselfinder", "discovery_loop_error", "", map[string]any{"error": err.Error(), "source_id": cfg.SourceID})
			isDeadProxy := strings.Contains(err.Error(), "deadline exceeded") || strings.Contains(err.Error(), "connection refused") || strings.Contains(err.Error(), "connection reset")
			isBotBlock := err.Error() == "bot_block"
			if isBotBlock || isDeadProxy {
				if sess.proxyURL != "" {
					pool.Disable(sess.proxyURL)
				}
				closeBrowserSession(sess)
				sess = nil
				discThrottle.RecordBlock()
				if isBotBlock {
					useDirect = false
					consecutiveDead = 0
				} else {
					consecutiveDead++
					if consecutiveDead >= 5 {
						useDirect = true
						observability.LogEvent("worker-vesselfinder", "discovery_direct_fallback", "", map[string]any{
							"consecutive_dead": consecutiveDead,
						})
					}
				}
			} else {
				time.Sleep(backoff)
				backoff *= 2
				if backoff > 15*time.Minute {
					backoff = 15 * time.Minute
				}
			}
			continue
		}
		backoff = 30 * time.Second
		consecutiveDead = 0
		time.Sleep(cfg.RediscoveryInterval)
	}
}

func runScanLoop(cfg config, pool *proxypool.Pool, scanThrottle *throttle.Adaptive) {
	if cfg.ScanBatchLimit < cfg.Workers {
		cfg.ScanBatchLimit = cfg.Workers
	}

	var sess *browserSession
	consecutiveDead := 0
	useDirect := false

	pickProxy := func() string {
		if useDirect {
			return ""
		}
		proxyURL, ok := pool.Pick()
		if !ok {
			return ""
		}
		return proxyURL
	}

	ensureSession := func() error {
		if sess != nil && sess.reqCount < cfg.BrowserRecycleAfter {
			return nil
		}
		closeBrowserSession(sess)
		sess = nil
		proxyURL := pickProxy()
		var err error
		sess, err = newBrowserSession(context.Background(), cfg, proxyURL)
		if err != nil {
			if proxyURL != "" {
				pool.Disable(proxyURL)
			}
			return fmt.Errorf("browser session: %w", err)
		}
		observability.LogEvent("worker-vesselfinder", "browser_recycled", "", map[string]any{
			"proxy":  proxyURL,
			"direct": proxyURL == "",
		})
		return nil
	}

	rotateProxy := func(botBlock bool) {
		if sess != nil && sess.proxyURL != "" {
			pool.Disable(sess.proxyURL)
		}
		closeBrowserSession(sess)
		sess = nil
		scanThrottle.RecordBlock()
		if botBlock {
			// Bot block on direct → force proxy use.
			useDirect = false
			consecutiveDead = 0
		} else {
			consecutiveDead++
			if consecutiveDead >= 5 {
				useDirect = true
				observability.LogEvent("worker-vesselfinder", "scan_direct_fallback", "", map[string]any{
					"consecutive_dead": consecutiveDead,
				})
			}
		}
	}

	defer func() { closeBrowserSession(sess) }()

	for {
		if err := ensureSession(); err != nil {
			observability.LogEvent("worker-vesselfinder", "scan_loop_no_proxy", "", map[string]any{"error": err.Error()})
			time.Sleep(30 * time.Second)
			continue
		}

		stats, err := runScan(context.Background(), cfg, store{runner: migrate.NewHTTPRunner(cfg.ClickHouseHTTP)}, sess, scanThrottle)
		if err != nil {
			observability.LogEvent("worker-vesselfinder", "scan_loop_error", "", map[string]any{"error": err.Error()})
			time.Sleep(10 * time.Second)
			continue
		}

		claimed, _ := stats["claimed"].(int)
		scanned, _ := stats["scanned"].(int)

		if blocked, _ := stats["bot_block"].(bool); blocked {
			rotateProxy(true)
			continue
		}

		// Dead proxy: claimed work but every fetch timed out.
		if claimed > 0 && scanned == 0 {
			rotateProxy(false)
			continue
		}

		// Successful scan — reset dead-proxy counter and allow proxies again after cooldown.
		if scanned > 0 {
			consecutiveDead = 0
		}

		if claimed == 0 {
			time.Sleep(10 * time.Second)
			continue
		}
		observability.LogEvent("worker-vesselfinder", "scan_loop_batch", "", map[string]any{
			"source_id": cfg.SourceID,
			"claimed":   claimed,
			"scanned":   scanned,
			"failed":    stats["failed"],
		})
		time.Sleep(2 * time.Second)
	}
}

type routeWorkerSlot struct {
	sess            *browserSession
	consecutiveDead int
	useDirect       bool
}

func runRouteLoop(cfg config, pool *proxypool.Pool) {
	st := store{runner: migrate.NewHTTPRunner(cfg.ClickHouseHTTP)}
	slots := make([]*routeWorkerSlot, cfg.RouteWorkers)
	for i := range slots {
		slots[i] = &routeWorkerSlot{}
	}
	throttles := make([]*throttle.Adaptive, cfg.RouteWorkers)
	for i := range throttles {
		floor := time.Duration(float64(time.Minute) / cfg.RouteRateFloorPerMin)
		ceil := time.Duration(float64(time.Minute) / cfg.RouteRateCeilPerMin)
		throttles[i] = throttle.New(floor, ceil, cfg.RouteRateRampDuration)
	}

	for {
		ctx := context.Background()
		if err := st.refillRouteQueue(ctx, cfg.SourceID, routeSourceID); err != nil {
			observability.LogEvent("worker-vesselfinder", "route_refill_error", "", map[string]any{"error": err.Error()})
		}

		var wg sync.WaitGroup
		for i := 0; i < cfg.RouteWorkers; i++ {
			wg.Add(1)
			slotCfg := cfg
			slotCfg.ProfileDir = filepath.Join(cfg.ProfileDir, fmt.Sprintf("route-%d", i))
			go func(slot *routeWorkerSlot, th *throttle.Adaptive, c config) {
				defer wg.Done()
				runRouteWorker(ctx, c, st, pool, slot, th)
			}(slots[i], throttles[i], slotCfg)
		}
		wg.Wait()

		time.Sleep(cfg.RouteQueueRefillInterval)
	}
}

func runRouteWorker(ctx context.Context, cfg config, st store, pool *proxypool.Pool, slot *routeWorkerSlot, th *throttle.Adaptive) {
	if slot.sess == nil || slot.sess.reqCount >= cfg.RouteBrowserRecycleAfter {
		closeBrowserSession(slot.sess)
		slot.sess = nil
		proxyURL := ""
		if !slot.useDirect {
			if u, ok := pool.Pick(); ok {
				proxyURL = u
			}
		}
		var err error
		slot.sess, err = newBrowserSession(ctx, cfg, proxyURL)
		if err != nil {
			if proxyURL != "" {
				pool.Disable(proxyURL)
			}
			observability.LogEvent("worker-vesselfinder", "route_session_error", "", map[string]any{"error": err.Error()})
			return
		}
	}

	items, err := st.claimRouteQueue(ctx, routeSourceID, cfg.RouteBatchLimit)
	if err != nil {
		observability.LogEvent("worker-vesselfinder", "route_claim_error", "", map[string]any{"error": err.Error()})
		return
	}

	for idx, item := range items {
		if idx > 0 {
			time.Sleep(th.Delay())
		}

		dmURL := "https://www.vesselfinder.com/api/pub/dm3/" + item.MMSI + "?wp=1"
		pg, err := renderPage(slot.sess, dmURL, cfg.RouteFetchTimeout)
		if err != nil {
			errCode := classifyRenderError(err)
			_ = st.updateRouteFailure(ctx, routeSourceID, item, errCode, 0, cfg.RouteRefreshInterval)
			isDeadProxy := errCode == "connect_timeout" || errCode == "connect_error"
			if isDeadProxy {
				if slot.sess != nil && slot.sess.proxyURL != "" {
					pool.Disable(slot.sess.proxyURL)
				}
				closeBrowserSession(slot.sess)
				slot.sess = nil
				th.RecordBlock()
				slot.consecutiveDead++
				if slot.consecutiveDead >= 5 {
					slot.useDirect = true
					observability.LogEvent("worker-vesselfinder", "route_direct_fallback", "", map[string]any{"consecutive_dead": slot.consecutiveDead})
				}
				// sess is nil — remaining items cannot be rendered; their leases expire and are re-claimed next pass.
				break
			}
			continue
		}

		if pg.StatusCode == 403 || pg.StatusCode == 429 || vf.IsBotPage(pg.HTML) {
			_ = st.updateRouteFailure(ctx, routeSourceID, item, "bot_block", pg.StatusCode, cfg.RouteRefreshInterval)
			if slot.sess != nil && slot.sess.proxyURL != "" {
				pool.Disable(slot.sess.proxyURL)
			}
			closeBrowserSession(slot.sess)
			slot.sess = nil
			th.RecordBlock()
			slot.useDirect = false
			slot.consecutiveDead = 0
			return
		}

		if pg.StatusCode == 404 {
			_ = st.updateRouteFailure(ctx, routeSourceID, item, "http_404", pg.StatusCode, cfg.RouteRefreshInterval)
			continue
		}

		if pg.StatusCode != 200 {
			_ = st.updateRouteFailure(ctx, routeSourceID, item, "http_status", pg.StatusCode, cfg.RouteRefreshInterval)
			continue
		}

		jsonBody, err := extractJSONFromPre(pg.HTML)
		if err != nil {
			_ = st.updateRouteFailure(ctx, routeSourceID, item, "bot_block", pg.StatusCode, cfg.RouteRefreshInterval)
			if slot.sess != nil && slot.sess.proxyURL != "" {
				pool.Disable(slot.sess.proxyURL)
			}
			closeBrowserSession(slot.sess)
			slot.sess = nil
			th.RecordBlock()
			slot.useDirect = false
			slot.consecutiveDead = 0
			return
		}

		if err := st.insertRetainedJSON(ctx, cfg, pg, item, jsonBody); err != nil {
			observability.LogEvent("worker-vesselfinder", "route_retain_error", "", map[string]any{"mmsi": item.MMSI, "error": err.Error()})
			_ = st.updateRouteFailure(ctx, routeSourceID, item, "retain_error", pg.StatusCode, cfg.RouteRefreshInterval)
			continue
		}

		_ = st.updateRouteSuccess(ctx, routeSourceID, item, cfg.RouteRefreshInterval)
		th.RecordSuccess()
		slot.consecutiveDead = 0
		slot.useDirect = false
		observability.LogEvent("worker-vesselfinder", "route_fetch_success", "", map[string]any{
			"mmsi":       item.MMSI,
			"body_bytes": len(jsonBody),
		})
	}
}

func extractJSONFromPre(html string) ([]byte, error) {
	start := strings.Index(html, "<pre")
	if start < 0 {
		trimmed := strings.TrimSpace(html)
		if json.Valid([]byte(trimmed)) {
			return []byte(trimmed), nil
		}
		return nil, fmt.Errorf("no <pre> element in rendered HTML")
	}
	openEnd := strings.Index(html[start:], ">")
	if openEnd < 0 {
		return nil, fmt.Errorf("malformed <pre> element")
	}
	content := html[start+openEnd+1:]
	closeIdx := strings.Index(content, "</pre>")
	if closeIdx < 0 {
		return nil, fmt.Errorf("unclosed <pre> element")
	}
	raw := strings.TrimSpace(content[:closeIdx])
	if !json.Valid([]byte(raw)) {
		return nil, fmt.Errorf("pre content is not valid JSON")
	}
	return []byte(raw), nil
}

func (s store) refillRouteQueue(ctx context.Context, scanSourceID, routeSrcID string) error {
	query := fmt.Sprintf(`INSERT INTO ops.vesselfinder_route_queue
(source_id, mmsi, status, discovered_at, next_fetch_at, updated_at, schema_version, record_version, attrs, evidence)
SELECT
    '%s',
    JSONExtractString(payload_json, 'mmsi'),
    'pending',
    now64(3),
    now64(3),
    now64(3),
    1,
    toUInt64(toUnixTimestamp64Nano(now64(3))),
    '{}',
    '[]'
FROM ops.vesselfinder_vessel_state FINAL
WHERE source_id = '%s'
  AND JSONExtractString(payload_json, 'mmsi') != ''
  AND JSONExtractString(payload_json, 'mmsi') NOT IN (
    SELECT mmsi FROM ops.vesselfinder_route_queue FINAL
    WHERE source_id = '%s'
      AND status IN ('pending', 'leased', 'success')
  )`, esc(routeSrcID), esc(scanSourceID), esc(routeSrcID))
	return s.runner.ApplySQL(ctx, query)
}

func (s store) claimRouteQueue(ctx context.Context, sourceID string, limit int) ([]vf.RouteQueueItem, error) {
	query := claimRouteQueueQuery(sourceID, limit)
	out, err := s.runner.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	var rows []vf.RouteQueueItem
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		parts := strings.Split(line, "\t")
		if len(parts) < 5 {
			continue
		}
		attempts, _ := strconv.Atoi(parts[2])
		statusCode, _ := strconv.Atoi(parts[4])
		rows = append(rows, vf.RouteQueueItem{
			MMSI:          parts[0],
			DetailID:      parts[1],
			AttemptCount:  attempts,
			LastErrorCode: parts[3],
			StatusCode:    statusCode,
		})
	}
	now := time.Now().UTC()
	for _, row := range rows {
		if err := s.markRouteLeased(ctx, sourceID, row, now); err != nil {
			return nil, err
		}
	}
	return rows, nil
}

func claimRouteQueueQuery(sourceID string, limit int) string {
	return fmt.Sprintf(`SELECT
    mmsi,
    detail_id,
    attempt_count,
    last_error_code,
    status_code
FROM ops.vesselfinder_route_queue FINAL
WHERE source_id = '%s'
  AND ((status IN ('pending', 'failed') AND next_fetch_at <= now())
    OR (status = 'leased' AND lease_expires_at <= now()))
ORDER BY next_fetch_at ASC
LIMIT %d FORMAT TabSeparated`, esc(sourceID), limit)
}

func (s store) markRouteLeased(ctx context.Context, sourceID string, item vf.RouteQueueItem, now time.Time) error {
	query := fmt.Sprintf(`INSERT INTO ops.vesselfinder_route_queue
(source_id, mmsi, detail_id, status, discovered_at, next_fetch_at, attempt_count, lease_owner, lease_expires_at, updated_at, schema_version, record_version, attrs, evidence)
VALUES ('%s','%s','%s','leased',toDateTime64('%s', 3, 'UTC'),toDateTime64('%s', 3, 'UTC'),%d,'worker-vesselfinder',toDateTime64('%s', 3, 'UTC'),toDateTime64('%s', 3, 'UTC'),1,%d,'{}','[]')`,
		esc(sourceID), esc(item.MMSI), esc(item.DetailID),
		esc(formatClickHouseTime(now)),
		esc(formatClickHouseTime(now)),
		item.AttemptCount,
		esc(formatClickHouseTime(now.Add(2*time.Minute))),
		esc(formatClickHouseTime(now)),
		recordVersion(now))
	return s.runner.ApplySQL(ctx, query)
}

func (s store) insertRetainedJSON(ctx context.Context, cfg config, pg renderedPage, item vf.RouteQueueItem, body []byte) error {
	objectStore, err := newS3Client(cfg)
	if err != nil {
		return err
	}
	stored, err := retainRenderedJSON(ctx, cfg, pg, item, body, objectStore)
	if err != nil {
		return err
	}
	if stored.RawDocument == nil {
		return fmt.Errorf("retention did not produce raw document for %s", pg.URL)
	}
	fetchLog := stored.FetchLog
	raw := stored.RawDocument
	fetchSQL := fmt.Sprintf(`INSERT INTO ops.fetch_log
(fetch_id, source_id, url_hash, status_code, success, fetched_at, latency_ms, body_bytes, error_message)
VALUES ('%s','%s','%s',%d,1,toDateTime64('%s', 3, 'UTC'),%d,%d,NULL)`,
		esc(fetchLog.FetchID), esc(fetchLog.SourceID), esc(fetchLog.URLHash), fetchLog.StatusCode, esc(fetchLog.FetchedAt), fetchLog.LatencyMS, fetchLog.BodyBytes)
	rawSQL := fmt.Sprintf(`INSERT INTO bronze.raw_document
(raw_id, fetch_id, source_id, url, final_url, fetched_at, status_code, content_type, content_hash, body_bytes, object_key, storage_class, fetch_metadata)
VALUES ('%s','%s','%s','%s','%s',toDateTime64('%s', 3, 'UTC'),%d,'application/json','%s',%d,%s,'%s','%s')`,
		esc(raw.RawID), esc(raw.FetchID), esc(raw.SourceID), esc(raw.URL), esc(raw.FinalURL), esc(raw.FetchedAt), raw.StatusCode, esc(raw.ContentHash), raw.BodyBytes, sqlNullableString(raw.ObjectKey), esc(raw.StorageClass), esc(raw.FetchMetadata))
	if err := s.runner.ApplySQL(ctx, fetchSQL); err != nil {
		return err
	}
	return s.runner.ApplySQL(ctx, rawSQL)
}

func retainRenderedJSON(ctx context.Context, cfg config, pg renderedPage, item vf.RouteQueueItem, body []byte, objectStore fetch.ObjectStore) (fetch.StoredFetch, error) {
	contentHash := hashString(string(body))
	fetchID := "fetch:vesselfinder-routes:" + urlHash(pg.URL) + ":" + pg.FetchedAt.UTC().Format("20060102150405")
	rawID := "raw:vesselfinder-routes:" + urlHash(pg.URL) + ":" + pg.FetchedAt.UTC().Format("20060102150405")
	req := fetch.Request{
		Method: "GET",
		URL:    pg.URL,
		Source: fetch.SourcePolicy{
			SourceID:         routeSourceID,
			RetentionClass:   "warm",
			SupportsLiveGET:  true,
			ForceObjectStore: true,
		},
	}
	resp := fetch.Response{
		FetchURL:           pg.URL,
		FinalURL:           pg.URL,
		SourceID:           routeSourceID,
		Method:             "GET",
		StatusCode:         pg.StatusCode,
		Success:            true,
		FetchedAt:          pg.FetchedAt,
		Latency:            pg.Latency,
		Attempts:           1,
		Body:               body,
		BodyBytes:          int64(len(body)),
		ContentHash:        contentHash,
		ContentType:        "application/json",
		SniffedContentType: "application/json",
	}
	stored, err := fetch.RetainResponse(ctx, fetch.PersistOptions{
		FetchID:  fetchID,
		RawID:    rawID,
		SourceID: routeSourceID,
		Bucket:   cfg.RawBucket,
		Policy: fetch.RetentionPolicy{
			Name:             "warm",
			ForceObjectStore: true,
			ReplayClass:      fetch.ReplayClassCached,
			ObjectPrefix:     "vesselfinder-routes",
		},
		Now: pg.FetchedAt,
	}, req, resp, objectStore)
	if err != nil || stored.RawDocument == nil {
		return stored, err
	}
	stored.RawDocument.FetchMetadata = enrichVesselFinderRouteMetadata(stored.RawDocument.FetchMetadata, item.MMSI)
	return stored, nil
}

func enrichVesselFinderRouteMetadata(raw, mmsi string) string {
	payload := map[string]any{}
	if strings.TrimSpace(raw) != "" {
		_ = json.Unmarshal([]byte(raw), &payload)
	}
	payload["vesselfinder"] = map[string]any{"mmsi": mmsi}
	b, _ := json.Marshal(payload)
	return string(b)
}

func (s store) updateRouteSuccess(ctx context.Context, sourceID string, item vf.RouteQueueItem, refreshInterval time.Duration) error {
	now := time.Now().UTC()
	updated := vf.ApplyRouteResult(item, vf.RouteResult{StatusCode: 200, Success: true}, now, refreshInterval)
	return s.updateRoute(ctx, sourceID, updated, now)
}

func (s store) updateRouteFailure(ctx context.Context, sourceID string, item vf.RouteQueueItem, errorCode string, statusCode int, refreshInterval time.Duration) error {
	now := time.Now().UTC()
	updated := vf.ApplyRouteResult(item, vf.RouteResult{StatusCode: statusCode, Success: false, ErrorCode: errorCode}, now, refreshInterval)
	return s.updateRoute(ctx, sourceID, updated, now)
}

func (s store) updateRoute(ctx context.Context, sourceID string, item vf.RouteQueueItem, now time.Time) error {
	query := fmt.Sprintf(`INSERT INTO ops.vesselfinder_route_queue
(source_id, mmsi, detail_id, status, discovered_at, next_fetch_at, last_fetched_at, attempt_count, last_error_code, status_code, updated_at, schema_version, record_version, attrs, evidence)
VALUES ('%s','%s','%s','%s',toDateTime64('%s', 3, 'UTC'),toDateTime64('%s', 3, 'UTC'),toDateTime64('%s', 3, 'UTC'),%d,'%s',%d,toDateTime64('%s', 3, 'UTC'),1,%d,'{}','[]')`,
		esc(sourceID), esc(item.MMSI), esc(item.DetailID), esc(item.Status),
		esc(formatClickHouseTime(now)),
		esc(formatClickHouseTime(item.NextFetchAt)),
		esc(formatClickHouseTime(firstNonZeroTime(item.LastFetchedAt, now))),
		item.AttemptCount, esc(item.LastErrorCode), item.StatusCode,
		esc(formatClickHouseTime(now)),
		recordVersion(now))
	return s.runner.ApplySQL(ctx, query)
}

func discoverOnce(cfg config, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("discover-once", flag.ContinueOnError)
	fs.SetOutput(stderr)
	seed := fs.Int64("seed", defaultDimensionSeed, "Deterministic shuffle seed.")
	proxyURL := fs.String("proxy", "", "Proxy URL (e.g. socks5://host:port). Optional.")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	sess, err := newBrowserSession(context.Background(), cfg, *proxyURL)
	if err != nil {
		fmt.Fprintf(stderr, "browser: %v\n", err)
		return 1
	}
	defer closeBrowserSession(sess)
	th := throttle.New(
		time.Duration(float64(time.Second)/cfg.DiscoveryFloorRPS),
		time.Duration(float64(time.Second)/cfg.DiscoveryCeilRPS),
		cfg.RateRampDuration,
	)
	ctx := context.Background()
	stats, err := runDiscovery(ctx, cfg, store{runner: migrate.NewHTTPRunner(cfg.ClickHouseHTTP)}, *seed, sess, th)
	if err != nil {
		fmt.Fprintf(stderr, "discover: %v\n", err)
		return 1
	}
	return writeJSON(stdout, stats)
}

func scanOnce(cfg config, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("scan-once", flag.ContinueOnError)
	fs.SetOutput(stderr)
	limit := fs.Int("limit", cfg.ScanBatchLimit, "Maximum detail pages to scan.")
	proxyURL := fs.String("proxy", "", "Proxy URL (e.g. socks5://host:port). Optional.")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	cfg.ScanBatchLimit = *limit
	sess, err := newBrowserSession(context.Background(), cfg, *proxyURL)
	if err != nil {
		fmt.Fprintf(stderr, "browser: %v\n", err)
		return 1
	}
	defer closeBrowserSession(sess)
	th := throttle.New(
		time.Duration(float64(time.Minute)/cfg.RateFloorPerMin),
		time.Duration(float64(time.Minute)/cfg.RateCeilPerMin),
		cfg.RateRampDuration,
	)
	ctx := context.Background()
	stats, err := runScan(ctx, cfg, store{runner: migrate.NewHTTPRunner(cfg.ClickHouseHTTP)}, sess, th)
	if err != nil {
		fmt.Fprintf(stderr, "scan: %v\n", err)
		return 1
	}
	return writeJSON(stdout, stats)
}

func runDiscovery(ctx context.Context, cfg config, st store, seed int64, sess *browserSession, th *throttle.Adaptive) (map[string]any, error) {
	dimPage, err := renderPage(sess, vesselFinderBaseURL+"/vessels", cfg.ListTimeout)
	if err != nil {
		return nil, err
	}
	if dimPage.StatusCode == 403 || dimPage.StatusCode == 429 || vf.IsBotPage(dimPage.HTML) {
		return nil, fmt.Errorf("bot_block")
	}

	dims := vf.ExtractDimensions(dimPage.HTML)
	if err := st.upsertDimensions(ctx, cfg.SourceID, "country", dims.Countries); err != nil {
		return nil, err
	}
	if err := st.upsertDimensions(ctx, cfg.SourceID, "type", dims.Types); err != nil {
		return nil, err
	}
	countries := dimensionLabelMap(dims.Countries)
	types := dimensionLabelMap(dims.Types)
	jobs := vf.BuildPageJobs(dims.Countries, dims.Types, cfg.MaxPage, seed)
	insertedJobs, insertedLinks := 0, 0
	terminals, err := st.loadTerminal404(ctx, cfg.SourceID)
	if err != nil {
		return nil, err
	}
	for idx, job := range jobs {
		if vf.ShouldSkipPage(job, terminals) {
			continue
		}
		if idx > 0 {
			time.Sleep(th.Delay())
		}
		listPage, err := renderPage(sess, listURL(job), cfg.ListTimeout)
		if err != nil {
			if upsertErr := st.upsertPageJob(ctx, cfg.SourceID, job, 0, "failed", "render_error"); upsertErr != nil {
				return nil, upsertErr
			}
			continue
		}
		if listPage.StatusCode == 403 || listPage.StatusCode == 429 || vf.IsBotPage(listPage.HTML) {
			return nil, fmt.Errorf("bot_block")
		}
		th.RecordSuccess()
		links := vf.ExtractDetailLinks(listPage.HTML, listPage.URL)
		status, terminal := vf.ListPageOutcome(listPage.StatusCode, links)
		if err := st.upsertPageJob(ctx, cfg.SourceID, job, listPage.StatusCode, status, ""); err != nil {
			return nil, err
		}
		insertedJobs++
		for _, link := range links {
			item := vf.ScanQueueItem{
				DetailURL:    link,
				CountryCode:  job.CountryCode,
				CountryLabel: countries[job.CountryCode],
				TypeCode:     job.TypeCode,
				TypeLabel:    types[job.TypeCode],
				PlaceID:      flagPlaceID(job.CountryCode),
			}
			if err := st.upsertScanQueue(ctx, cfg.SourceID, item, time.Now().UTC()); err != nil {
				return nil, err
			}
			insertedLinks++
		}
		if terminal {
			terminals = append(terminals, vf.Terminal404{CountryCode: job.CountryCode, TypeCode: job.TypeCode, Page: job.Page})
		}
	}
	return map[string]any{"source_id": cfg.SourceID, "dimensions": len(dims.Countries) * len(dims.Types), "jobs": insertedJobs, "links": insertedLinks}, nil
}

func runScan(ctx context.Context, cfg config, st store, sess *browserSession, th *throttle.Adaptive) (map[string]any, error) {
	items, err := st.claimScanQueue(ctx, cfg.SourceID, cfg.ScanBatchLimit)
	if err != nil {
		return nil, err
	}
	scanned, failed := 0, 0
	for idx, item := range items {
		if idx > 0 {
			time.Sleep(th.Delay())
		}
		pg, err := renderPage(sess, item.DetailURL, cfg.DetailTimeout)
		if err != nil {
			_ = st.updateScanFailure(ctx, cfg.SourceID, item, classifyRenderError(err), 0)
			failed++
			continue
		}

		// Detect block at fetch time — rotate proxy immediately
		if pg.StatusCode == 403 || pg.StatusCode == 429 || vf.IsBotPage(pg.HTML) {
			_ = st.updateScanFailure(ctx, cfg.SourceID, item, "bot_block", pg.StatusCode)
			failed++
			return map[string]any{
				"source_id": cfg.SourceID, "claimed": len(items),
				"scanned": scanned, "failed": failed, "bot_block": true,
			}, nil
		}

		if pg.StatusCode == 200 {
			if err := st.insertRetainedHTML(ctx, cfg, pg, item); err != nil {
				return nil, err
			}
			_ = st.updateScanSuccess(ctx, cfg.SourceID, item)
			th.RecordSuccess()
			scanned++
			continue
		}
		_ = st.updateScanFailure(ctx, cfg.SourceID, item, "http_status", pg.StatusCode)
		failed++
	}
	return map[string]any{"source_id": cfg.SourceID, "claimed": len(items), "scanned": scanned, "failed": failed}, nil
}

func newBrowserSession(parent context.Context, cfg config, proxyURL string) (*browserSession, error) {
	// Remove stale singleton locks left by a crashed or restarted previous session.
	for _, name := range []string{"SingletonLock", "SingletonCookie", "SingletonSocket"} {
		_ = os.Remove(filepath.Join(cfg.ProfileDir, name))
	}

	opts := []chromedp.ExecAllocatorOption{
		chromedp.Flag("no-first-run", true),
		chromedp.Flag("no-default-browser-check", true),
		chromedp.Flag("headless", true),
		chromedp.Flag("disable-gpu", true),
		chromedp.Flag("no-sandbox", true),
		chromedp.Flag("disable-dev-shm-usage", true),
		chromedp.Flag("disable-crash-reporter", true),
		chromedp.Flag("disable-crashpad", true),
		chromedp.Flag("disable-extensions", true),
		chromedp.Flag("disable-popup-blocking", true),
		chromedp.Flag("disable-background-networking", true),
		chromedp.Flag("disable-renderer-backgrounding", true),
		chromedp.Flag("disable-blink-features", "AutomationControlled"),
		chromedp.Flag("exclude-switches", "enable-automation"),
		chromedp.Flag("disable-infobars", true),
		chromedp.Flag("lang", "en-US"),
		chromedp.UserDataDir(cfg.ProfileDir),
		chromedp.UserAgent(cfg.UserAgent),
	}
	if proxyURL != "" {
		opts = append(opts, chromedp.Flag("proxy-server", proxyURL))
	}
	allocCtx, cancelAlloc := chromedp.NewExecAllocator(parent, opts...)
	browserCtx, cancelBrowser := chromedp.NewContext(allocCtx)

	// Inject stealth JS that patches navigator.webdriver for every new page
	if err := chromedp.Run(browserCtx,
		chromedp.ActionFunc(func(ctx context.Context) error {
			_, err := page.AddScriptToEvaluateOnNewDocument(
				`Object.defineProperty(navigator,'webdriver',{get:()=>undefined});window.chrome={runtime:{}};`,
			).Do(ctx)
			return err
		}),
	); err != nil {
		cancelBrowser()
		cancelAlloc()
		return nil, fmt.Errorf("browser stealth init: %w", err)
	}

	return &browserSession{
		allocCtx:      allocCtx,
		cancelAlloc:   cancelAlloc,
		browserCtx:    browserCtx,
		cancelBrowser: cancelBrowser,
		proxyURL:      proxyURL,
	}, nil
}

func closeBrowserSession(sess *browserSession) {
	if sess == nil {
		return
	}
	sess.cancelBrowser()
	sess.cancelAlloc()
}

// renderPage renders target using the given persistent session.
// It captures the real HTTP status code via CDP network events.
// On success, sess.reqCount is incremented.
func renderPage(sess *browserSession, target string, timeout time.Duration) (renderedPage, error) {
	started := time.Now().UTC()

	statusCh := make(chan int, 10)
	listenerCtx, cancelListener := context.WithCancel(sess.browserCtx)
	chromedp.ListenTarget(listenerCtx, func(ev interface{}) {
		if e, ok := ev.(*network.EventResponseReceived); ok {
			if e.Type == network.ResourceTypeDocument {
				select {
				case statusCh <- int(e.Response.Status):
				default:
				}
			}
		}
	})

	timeoutCtx, cancelTimeout := context.WithTimeout(sess.browserCtx, timeout)
	defer cancelTimeout()

	var html string
	err := chromedp.Run(timeoutCtx,
		network.Enable(),
		chromedp.Navigate(target),
		chromedp.WaitReady("body", chromedp.ByQuery),
		chromedp.OuterHTML("html", &html, chromedp.ByQuery),
	)
	cancelListener()

	if err != nil {
		return renderedPage{}, err
	}

	statusCode := 200
	select {
	case code := <-statusCh:
		statusCode = code
	default:
	}

	sess.reqCount++
	return renderedPage{
		URL:        target,
		HTML:       html,
		StatusCode: statusCode,
		FetchedAt:  started,
		Latency:    time.Since(started),
	}, nil
}


func classifyRenderError(err error) string {
	if err == nil {
		return ""
	}
	message := strings.ToLower(err.Error())
	switch {
	case strings.Contains(message, "tcp preflight") && strings.Contains(message, "timeout"):
		return "connect_timeout"
	case strings.Contains(message, "tcp preflight"):
		return "connect_error"
	case strings.Contains(message, "context deadline exceeded"):
		return "render_timeout"
	default:
		return "render_error"
	}
}

func listURL(job vf.PageJob) string {
	return vesselFinderBaseURL + "/vessels?flag=" + url.QueryEscape(job.CountryCode) + "&type=" + url.QueryEscape(job.TypeCode) + "&page=" + strconv.Itoa(job.Page)
}

func dimensionLabelMap(dims []vf.Dimension) map[string]string {
	out := make(map[string]string, len(dims))
	for _, dim := range dims {
		out[dim.Code] = dim.Label
	}
	return out
}

func flagPlaceID(countryCode string) string {
	code := strings.ToLower(strings.TrimSpace(countryCode))
	if code == "" {
		return ""
	}
	return "plc:flag:" + code
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func loadConfig() config {
	return config{
		ClickHouseHTTP:      getenv("CLICKHOUSE_HTTP_URL", defaultClickHouseURL),
		SourceID:            sourceID,
		Workers:             getenvInt("WORKERS", defaultWorkers),
		WorkerRatePerMinute: getenvInt("WORKER_RATE_PER_MINUTE", defaultWorkerRate),
		DiscoveryRPS:        getenvFloat("DISCOVERY_RPS", defaultDiscoveryRPS),
		MaxPage:             getenvInt("MAX_PAGE", defaultMaxPage),
		RediscoveryInterval: getenvDuration("REDISCOVERY_INTERVAL", defaultRediscover),
		ListTimeout:         getenvDuration("LIST_TIMEOUT", defaultListTimeout),
		DetailTimeout:       getenvDuration("DETAIL_TIMEOUT", defaultDetailTimeout),
		ScanBatchLimit:      getenvInt("SCAN_BATCH_LIMIT", defaultScanBatchLimit),
		UserAgent:           getenv("USER_AGENT", defaultUserAgent),
		MinIOEndpoint:       getenv("MINIO_ENDPOINT", defaultMinIOEndpoint),
		MinIOAccessKey:      getenv("MINIO_ACCESS_KEY", getenv("MINIO_ROOT_USER", "minioadmin")),
		MinIOSecretKey:      getenv("MINIO_SECRET_KEY", getenv("MINIO_ROOT_PASSWORD", "minioadmin")),
		MinIORegion:         getenv("MINIO_REGION", defaultMinIORegion),
		RawBucket:           getenv("RAW_BUCKET", defaultRawBucket),
		ProxySourcesFile:      getenv("PROXY_SOURCES_FILE", defaultProxySourcesFile),
		ProxyRefreshInterval:  getenvDuration("PROXY_REFRESH_INTERVAL", defaultProxyRefreshInterval),
		ProxyValidateInterval: getenvDuration("PROXY_VALIDATE_INTERVAL", defaultProxyValidateInterval),
		BrowserRecycleAfter:   getenvInt("BROWSER_RECYCLE_AFTER", defaultBrowserRecycleAfter),
		ProfileDir:            getenv("CHROME_PROFILE_DIR", defaultProfileDir),
		RateFloorPerMin:       getenvFloat("RATE_FLOOR_PER_MIN", defaultRateFloorPerMin),
		RateCeilPerMin:        getenvFloat("RATE_CEIL_PER_MIN", defaultRateCeilPerMin),
		RateRampDuration:      getenvDuration("RATE_RAMP_DURATION", defaultRateRampDuration),
		DiscoveryFloorRPS:     getenvFloat("DISCOVERY_FLOOR_RPS", defaultDiscoveryFloorRPS),
		DiscoveryCeilRPS:      getenvFloat("DISCOVERY_CEIL_RPS", defaultDiscoveryCeilRPS),

		RouteWorkers:             getenvInt("ROUTE_WORKERS", defaultRouteWorkers),
		RouteRefreshInterval:     getenvDuration("ROUTE_REFRESH_INTERVAL", defaultRouteRefreshInterval),
		RouteBatchLimit:          getenvInt("ROUTE_BATCH_LIMIT", defaultRouteBatchLimit),
		RouteFetchTimeout:        getenvDuration("ROUTE_FETCH_TIMEOUT", defaultRouteFetchTimeout),
		RouteBrowserRecycleAfter: getenvInt("ROUTE_BROWSER_RECYCLE_AFTER", defaultRouteBrowserRecycleAfter),
		RouteRateFloorPerMin:     getenvFloat("ROUTE_RATE_FLOOR_PER_MIN", defaultRouteRateFloorPerMin),
		RouteRateCeilPerMin:      getenvFloat("ROUTE_RATE_CEIL_PER_MIN", defaultRouteRateCeilPerMin),
		RouteRateRampDuration:    getenvDuration("ROUTE_RATE_RAMP_DURATION", defaultRouteRateRampDuration),
		RouteQueueRefillInterval: getenvDuration("ROUTE_QUEUE_REFILL_INTERVAL", defaultRouteQueueRefillInterval),
	}
}

func (s store) loadTerminal404(ctx context.Context, sourceID string) ([]vf.Terminal404, error) {
	query := fmt.Sprintf(`SELECT country_code, type_code, min(page) FROM ops.vesselfinder_page_job FINAL WHERE source_id = '%s' AND status IN ('terminal_404','empty') GROUP BY country_code, type_code FORMAT TabSeparated`, esc(sourceID))
	out, err := s.runner.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	var rows []vf.Terminal404
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		parts := strings.Split(line, "\t")
		if len(parts) != 3 {
			continue
		}
		page, _ := strconv.Atoi(parts[2])
		rows = append(rows, vf.Terminal404{CountryCode: parts[0], TypeCode: parts[1], Page: page})
	}
	return rows, nil
}

func (s store) upsertPageJob(ctx context.Context, sourceID string, job vf.PageJob, statusCode int, status, errorCode string) error {
	nowTime := time.Now().UTC()
	now := formatClickHouseTime(nowTime)
	query := fmt.Sprintf(`INSERT INTO ops.vesselfinder_page_job
	(source_id, country_code, type_code, page, status, status_code, last_error_code, updated_at, schema_version, record_version)
	VALUES ('%s','%s','%s',%d,'%s',%d,'%s',toDateTime64('%s', 3, 'UTC'),1,%d)`,
		esc(sourceID), esc(job.CountryCode), esc(job.TypeCode), job.Page, esc(status), statusCode, esc(errorCode), esc(now), recordVersion(nowTime))
	return s.runner.ApplySQL(ctx, query)
}

func (s store) upsertDimensions(ctx context.Context, sourceID, kind string, dims []vf.Dimension) error {
	now := time.Now().UTC()
	for _, dim := range dims {
		query := fmt.Sprintf(`INSERT INTO ops.vesselfinder_dimension
	(source_id, dimension_kind, dimension_code, dimension_label, discovered_at, schema_version, record_version, updated_at, attrs, evidence)
	VALUES ('%s','%s','%s','%s',toDateTime64('%s', 3, 'UTC'),1,%d,toDateTime64('%s', 3, 'UTC'),'{}','[]')`,
			esc(sourceID), esc(kind), esc(dim.Code), esc(dim.Label), esc(formatClickHouseTime(now)), recordVersion(now), esc(formatClickHouseTime(now)))
		if err := s.runner.ApplySQL(ctx, query); err != nil {
			return err
		}
	}
	return nil
}

func (s store) upsertScanQueue(ctx context.Context, sourceID string, item vf.ScanQueueItem, now time.Time) error {
	detailID := urlHash(item.DetailURL)
	attrs := scanQueueAttrs(item)
	query := fmt.Sprintf(`INSERT INTO ops.vesselfinder_scan_queue
	(source_id, detail_id, detail_url, status, discovered_at, next_scan_at, attempt_count, updated_at, schema_version, record_version, attrs)
	VALUES ('%s','%s','%s','pending',toDateTime64('%s', 3, 'UTC'),toDateTime64('%s', 3, 'UTC'),0,toDateTime64('%s', 3, 'UTC'),1,%d,'%s')`,
		esc(sourceID), esc(detailID), esc(item.DetailURL), esc(formatClickHouseTime(now)), esc(formatClickHouseTime(now)), esc(formatClickHouseTime(now)), recordVersion(now), esc(attrs))
	return s.runner.ApplySQL(ctx, query)
}

func (s store) claimScanQueue(ctx context.Context, sourceID string, limit int) ([]vf.ScanQueueItem, error) {
	query := claimScanQueueQuery(sourceID, limit)
	out, err := s.runner.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	var rows []vf.ScanQueueItem
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		parts := strings.Split(line, "\t")
		if len(parts) < 9 {
			continue
		}
		attempts, _ := strconv.Atoi(parts[3])
		rows = append(rows, vf.ScanQueueItem{
			DetailURL:    parts[0],
			Status:       parts[1],
			AttemptCount: attempts,
			CountryCode:  parts[4],
			CountryLabel: parts[5],
			TypeCode:     parts[6],
			TypeLabel:    parts[7],
			PlaceID:      parts[8],
		})
	}
	for _, row := range rows {
		if err := s.markLeased(ctx, sourceID, row); err != nil {
			return nil, err
		}
	}
	return rows, nil
}

func claimScanQueueQuery(sourceID string, limit int) string {
	return fmt.Sprintf(`SELECT
	detail_url,
	status,
	next_scan_at,
	attempt_count,
	JSONExtractString(attrs, 'country_code'),
	JSONExtractString(attrs, 'country_label'),
	JSONExtractString(attrs, 'type_code'),
	JSONExtractString(attrs, 'type_label'),
	JSONExtractString(attrs, 'place_id')
FROM ops.vesselfinder_scan_queue FINAL
WHERE source_id = '%s' AND ((status IN ('pending','failed') AND next_scan_at <= now()) OR (status = 'leased' AND lease_expires_at <= now()))
ORDER BY if(JSONExtractString(attrs, 'place_id') != '' OR JSONExtractString(attrs, 'country_code') != '', 0, 1), next_scan_at ASC LIMIT %d FORMAT TabSeparated`, esc(sourceID), limit)
}

func (s store) markLeased(ctx context.Context, sourceID string, item vf.ScanQueueItem) error {
	now := time.Now().UTC()
	attrs := scanQueueAttrs(item)
	query := fmt.Sprintf(`INSERT INTO ops.vesselfinder_scan_queue
	(source_id, detail_id, detail_url, status, discovered_at, next_scan_at, attempt_count, lease_owner, lease_expires_at, updated_at, schema_version, record_version, attrs)
	VALUES ('%s','%s','%s','leased',toDateTime64('%s', 3, 'UTC'),toDateTime64('%s', 3, 'UTC'),%d,'worker-vesselfinder',toDateTime64('%s', 3, 'UTC'),toDateTime64('%s', 3, 'UTC'),1,%d,'%s')`,
		esc(sourceID), esc(urlHash(item.DetailURL)), esc(item.DetailURL), esc(formatClickHouseTime(now)), esc(formatClickHouseTime(firstNonZeroTime(item.NextScanAt, now))), item.AttemptCount, esc(formatClickHouseTime(now.Add(2*time.Minute))), esc(formatClickHouseTime(now)), recordVersion(now), esc(attrs))
	return s.runner.ApplySQL(ctx, query)
}

func (s store) insertRetainedHTML(ctx context.Context, cfg config, page renderedPage, item vf.ScanQueueItem) error {
	objectStore, err := newS3Client(cfg)
	if err != nil {
		return err
	}
	stored, err := retainRenderedHTML(ctx, cfg, page, item, objectStore)
	if err != nil {
		return err
	}
	if stored.RawDocument == nil {
		return fmt.Errorf("retention did not produce raw document for %s", page.URL)
	}
	fetchLog := stored.FetchLog
	raw := stored.RawDocument
	fetchSQL := fmt.Sprintf(`INSERT INTO ops.fetch_log
	(fetch_id, source_id, url_hash, status_code, success, fetched_at, latency_ms, body_bytes, error_message)
	VALUES ('%s','%s','%s',%d,1,toDateTime64('%s', 3, 'UTC'),%d,%d,NULL)`,
		esc(fetchLog.FetchID), esc(fetchLog.SourceID), esc(fetchLog.URLHash), fetchLog.StatusCode, esc(fetchLog.FetchedAt), fetchLog.LatencyMS, fetchLog.BodyBytes)
	rawSQL := fmt.Sprintf(`INSERT INTO bronze.raw_document
	(raw_id, fetch_id, source_id, url, final_url, fetched_at, status_code, content_type, content_hash, body_bytes, object_key, storage_class, fetch_metadata)
	VALUES ('%s','%s','%s','%s','%s',toDateTime64('%s', 3, 'UTC'),%d,'text/html','%s',%d,%s,'%s','%s')`,
		esc(raw.RawID), esc(raw.FetchID), esc(raw.SourceID), esc(raw.URL), esc(raw.FinalURL), esc(raw.FetchedAt), raw.StatusCode, esc(raw.ContentHash), raw.BodyBytes, sqlNullableString(raw.ObjectKey), esc(raw.StorageClass), esc(raw.FetchMetadata))
	if err := s.runner.ApplySQL(ctx, fetchSQL); err != nil {
		return err
	}
	return s.runner.ApplySQL(ctx, rawSQL)
}

func retainRenderedHTML(ctx context.Context, cfg config, page renderedPage, item vf.ScanQueueItem, objectStore fetch.ObjectStore) (fetch.StoredFetch, error) {
	contentHash := hashString(page.HTML)
	fetchID := "fetch:vesselfinder:" + urlHash(page.URL) + ":" + page.FetchedAt.UTC().Format("20060102150405")
	rawID := "raw:vesselfinder:" + urlHash(page.URL) + ":" + page.FetchedAt.UTC().Format("20060102150405")
	req := fetch.Request{
		Method: "GET",
		URL:    page.URL,
		Source: fetch.SourcePolicy{
			SourceID:         cfg.SourceID,
			RetentionClass:   "warm",
			SupportsLiveGET:  true,
			ForceObjectStore: true,
		},
	}
	resp := fetch.Response{
		FetchURL:           page.URL,
		FinalURL:           page.URL,
		SourceID:           cfg.SourceID,
		Method:             "GET",
		StatusCode:         page.StatusCode,
		Success:            page.StatusCode >= 200 && page.StatusCode < 300,
		FetchedAt:          page.FetchedAt,
		Latency:            page.Latency,
		Attempts:           1,
		Body:               []byte(page.HTML),
		BodyBytes:          int64(len(page.HTML)),
		ContentHash:        contentHash,
		ContentType:        "text/html",
		SniffedContentType: "text/html",
	}
	stored, err := fetch.RetainResponse(ctx, fetch.PersistOptions{
		FetchID:  fetchID,
		RawID:    rawID,
		SourceID: cfg.SourceID,
		Bucket:   cfg.RawBucket,
		Policy: fetch.RetentionPolicy{
			Name:             "warm",
			ForceObjectStore: true,
			ReplayClass:      fetch.ReplayClassCached,
			ObjectPrefix:     "vesselfinder",
		},
		Now: page.FetchedAt,
	}, req, resp, objectStore)
	if err != nil || stored.RawDocument == nil {
		return stored, err
	}
	stored.RawDocument.FetchMetadata = enrichVesselFinderMetadata(stored.RawDocument.FetchMetadata, item)
	return stored, nil
}

func scanQueueAttrs(item vf.ScanQueueItem) string {
	payload := vesselFinderContext(item)
	b, _ := json.Marshal(payload)
	return string(b)
}

func enrichVesselFinderMetadata(raw string, item vf.ScanQueueItem) string {
	payload := map[string]any{}
	if strings.TrimSpace(raw) != "" {
		_ = json.Unmarshal([]byte(raw), &payload)
	}
	payload["vesselfinder"] = vesselFinderContext(item)
	b, _ := json.Marshal(payload)
	return string(b)
}

func vesselFinderContext(item vf.ScanQueueItem) map[string]any {
	return map[string]any{
		"country_code":  strings.TrimSpace(item.CountryCode),
		"country_label": strings.TrimSpace(item.CountryLabel),
		"type_code":     strings.TrimSpace(item.TypeCode),
		"type_label":    strings.TrimSpace(item.TypeLabel),
		"place_id":      strings.TrimSpace(firstNonEmptyString(item.PlaceID, flagPlaceID(item.CountryCode))),
	}
}

func (s store) updateScanSuccess(ctx context.Context, sourceID string, item vf.ScanQueueItem) error {
	now := time.Now().UTC()
	updated := vf.ApplyScanResult(item, vf.ScanResult{StatusCode: 200, Success: true}, now, time.Hour)
	return s.updateScan(ctx, sourceID, updated)
}

func (s store) updateScanFailure(ctx context.Context, sourceID string, item vf.ScanQueueItem, errorCode string, statusCode int) error {
	now := time.Now().UTC()
	updated := vf.ApplyScanResult(item, vf.ScanResult{StatusCode: statusCode, Success: false, ErrorCode: errorCode}, now, time.Hour)
	return s.updateScan(ctx, sourceID, updated)
}

func (s store) updateScan(ctx context.Context, sourceID string, item vf.ScanQueueItem) error {
	now := time.Now().UTC()
	attrs := scanQueueAttrs(item)
	query := fmt.Sprintf(`INSERT INTO ops.vesselfinder_scan_queue
	(source_id, detail_id, detail_url, status, discovered_at, next_scan_at, last_scanned_at, attempt_count, last_error_code, status_code, updated_at, schema_version, record_version, attrs)
	VALUES ('%s','%s','%s','%s',toDateTime64('%s', 3, 'UTC'),toDateTime64('%s', 3, 'UTC'),toDateTime64('%s', 3, 'UTC'),%d,'%s',%d,toDateTime64('%s', 3, 'UTC'),1,%d,'%s')`,
		esc(sourceID), esc(urlHash(item.DetailURL)), esc(item.DetailURL), esc(item.Status), esc(formatClickHouseTime(now)), esc(formatClickHouseTime(item.NextScanAt)), esc(formatClickHouseTime(firstNonZeroTime(item.LastScannedAt, now))), item.AttemptCount, esc(item.LastErrorCode), item.StatusCode, esc(formatClickHouseTime(now)), recordVersion(now), esc(attrs))
	return s.runner.ApplySQL(ctx, query)
}

type s3Client struct {
	endpoint  *url.URL
	accessKey string
	secretKey string
	region    string
	client    *http.Client
}

func newS3Client(cfg config) (*s3Client, error) {
	endpoint, err := url.Parse(cfg.MinIOEndpoint)
	if err != nil {
		return nil, fmt.Errorf("parse MinIO endpoint: %w", err)
	}
	if endpoint.Scheme == "" || endpoint.Host == "" {
		return nil, fmt.Errorf("invalid MinIO endpoint %q", cfg.MinIOEndpoint)
	}
	return &s3Client{
		endpoint:  endpoint,
		accessKey: cfg.MinIOAccessKey,
		secretKey: cfg.MinIOSecretKey,
		region:    cfg.MinIORegion,
		client:    &http.Client{Timeout: cfg.DetailTimeout},
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

	payloadHash := hashString(string(body))
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
		hashString(canonicalRequest),
	}, "\n")
	signature := hex.EncodeToString(signV4(c.secretKey, dateStamp, c.region, "s3", stringToSign))
	req.Header.Set("Authorization", fmt.Sprintf(
		"AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		c.accessKey,
		credentialScope,
		strings.Join(signedHeaders, ";"),
		signature,
	))

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

func printUsage(w io.Writer) {
	fmt.Fprint(w, `Usage:
  worker-vesselfinder discover-once [--seed n]
  worker-vesselfinder scan-once [--limit n]
`)
}

func writeJSON(w io.Writer, value any) int {
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(value); err != nil {
		return 1
	}
	return 0
}

func getenv(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func getenvInt(key string, fallback int) int {
	value, err := strconv.Atoi(strings.TrimSpace(os.Getenv(key)))
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func getenvFloat(key string, fallback float64) float64 {
	value, err := strconv.ParseFloat(strings.TrimSpace(os.Getenv(key)), 64)
	if err != nil || value <= 0 {
		return fallback
	}
	return value
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

func hashString(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func urlHash(value string) string {
	return hashString(strings.TrimSpace(value))[:16]
}

func formatClickHouseTime(value time.Time) string {
	if value.IsZero() {
		value = time.Now().UTC()
	}
	return value.UTC().Truncate(time.Millisecond).Format(clickHouseTimeLayout)
}

func firstNonZeroTime(values ...time.Time) time.Time {
	for _, value := range values {
		if !value.IsZero() {
			return value
		}
	}
	return time.Now().UTC()
}

func recordVersion(value time.Time) uint64 {
	if value.IsZero() {
		value = time.Now().UTC()
	}
	return uint64(value.UTC().UnixNano())
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
