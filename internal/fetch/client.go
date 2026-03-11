package fetch

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"
	"time"

	brotli "github.com/andybalholm/brotli"
)

var (
	ErrBodyTooLarge  = errors.New("fetch body exceeds configured size limit")
	ErrSourceBlocked = errors.New("fetch source is not eligible for live fetch")
)

type RetryPolicy struct {
	MaxAttempts    int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
}

type Config struct {
	HTTPClient   *http.Client
	RetryPolicy  RetryPolicy
	MaxBodyBytes int64
	UserAgent    string
	Now          func() time.Time
	Sleep        func(context.Context, time.Duration) error
}

type Client struct {
	client       *http.Client
	retryPolicy  RetryPolicy
	maxBodyBytes int64
	userAgent    string
	now          func() time.Time
	sleep        func(context.Context, time.Duration) error
}

type SourcePolicy struct {
	SourceID         string
	RetentionClass   string
	Disabled         bool
	DisabledReason   string
	AuthMode         string
	SupportsLiveGET  bool
	MaxBodyBytes     int64
	ForceObjectStore bool
}

type ConditionalRequest struct {
	ETag         string
	LastModified string
}

type Request struct {
	Method      string
	URL         string
	Headers     http.Header
	Conditional ConditionalRequest
	Source      SourcePolicy
}

type Response struct {
	FetchURL           string
	FinalURL           string
	SourceID           string
	Method             string
	StatusCode         int
	Success            bool
	NotModified        bool
	FetchedAt          time.Time
	Latency            time.Duration
	Attempts           int
	RetryReasons       []string
	Body               []byte
	BodyBytes          int64
	ContentHash        string
	ContentType        string
	HeaderContentType  string
	SniffedContentType string
	ContentEncoding    string
	ETag               string
	LastModified       string
	RequestHeaders     map[string][]string
	ResponseHeaders    map[string][]string
	ErrorMessage       string
}

type BodyTooLargeError struct {
	Limit  int64
	Actual int64
}

func (e BodyTooLargeError) Error() string {
	return fmt.Sprintf("%s: limit=%d actual=%d", ErrBodyTooLarge.Error(), e.Limit, e.Actual)
}

func (e BodyTooLargeError) Unwrap() error {
	return ErrBodyTooLarge
}

func NewClient(cfg Config) *Client {
	httpClient := cfg.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	retryPolicy := cfg.RetryPolicy
	if retryPolicy.MaxAttempts <= 0 {
		retryPolicy.MaxAttempts = 3
	}
	if retryPolicy.InitialBackoff <= 0 {
		retryPolicy.InitialBackoff = 250 * time.Millisecond
	}
	if retryPolicy.MaxBackoff <= 0 {
		retryPolicy.MaxBackoff = 3 * time.Second
	}
	maxBodyBytes := cfg.MaxBodyBytes
	if maxBodyBytes <= 0 {
		maxBodyBytes = 16 << 20
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	sleep := cfg.Sleep
	if sleep == nil {
		sleep = defaultSleep
	}
	userAgent := strings.TrimSpace(cfg.UserAgent)
	if userAgent == "" {
		userAgent = "global-osint-backend/worker-fetch"
	}
	return &Client{
		client:       httpClient,
		retryPolicy:  retryPolicy,
		maxBodyBytes: maxBodyBytes,
		userAgent:    userAgent,
		now:          now,
		sleep:        sleep,
	}
}

func (c *Client) Fetch(ctx context.Context, req Request) (Response, error) {
	method := normalizeMethod(req.Method)
	if method != http.MethodGet && method != http.MethodHead {
		return Response{FetchURL: strings.TrimSpace(req.URL), Method: method, SourceID: req.Source.SourceID}, fmt.Errorf("unsupported fetch method %q", req.Method)
	}
	if err := req.Source.Validate(); err != nil {
		resp := Response{
			FetchURL:     strings.TrimSpace(req.URL),
			Method:       method,
			SourceID:     req.Source.SourceID,
			FetchedAt:    c.now().UTC(),
			ErrorMessage: err.Error(),
		}
		return resp, err
	}

	maxAttempts := c.retryPolicy.MaxAttempts
	retryReasons := make([]string, 0, maxAttempts)
	var lastResp Response
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		resp, retryable, err := c.fetchOnce(ctx, method, req)
		resp.Attempts = attempt
		resp.RetryReasons = append([]string(nil), retryReasons...)
		if err == nil {
			return resp, nil
		}
		resp.ErrorMessage = err.Error()
		lastResp = resp
		lastErr = err
		if !retryable || attempt == maxAttempts {
			return lastResp, lastErr
		}
		retryReasons = append(retryReasons, err.Error())
		if sleepErr := c.sleep(ctx, c.retryPolicy.backoff(attempt)); sleepErr != nil {
			lastResp.RetryReasons = append(lastResp.RetryReasons, sleepErr.Error())
			lastResp.ErrorMessage = sleepErr.Error()
			return lastResp, sleepErr
		}
	}
	return lastResp, lastErr
}

func (c *Client) fetchOnce(ctx context.Context, method string, req Request) (Response, bool, error) {
	preparedURL := strings.TrimSpace(req.URL)
	preparedHeaders := cloneHeader(req.Headers)
	if preparedHeaders.Get("Accept-Encoding") == "" {
		preparedHeaders.Set("Accept-Encoding", "gzip, br")
	}
	if preparedHeaders.Get("User-Agent") == "" {
		preparedHeaders.Set("User-Agent", c.userAgent)
	}
	if req.Conditional.ETag != "" {
		preparedHeaders.Set("If-None-Match", req.Conditional.ETag)
	}
	if req.Conditional.LastModified != "" {
		preparedHeaders.Set("If-Modified-Since", req.Conditional.LastModified)
	}

	httpReq, err := http.NewRequestWithContext(ctx, method, preparedURL, nil)
	if err != nil {
		return Response{FetchURL: preparedURL, Method: method, SourceID: req.Source.SourceID}, false, err
	}
	httpReq.Header = cloneHeader(preparedHeaders)

	startedAt := time.Now()
	resp, err := c.client.Do(httpReq)
	if err != nil {
		fetchResp := Response{
			FetchURL:       preparedURL,
			SourceID:       req.Source.SourceID,
			Method:         method,
			FetchedAt:      c.now().UTC(),
			Latency:        time.Since(startedAt),
			RequestHeaders: headerMap(preparedHeaders),
		}
		return fetchResp, isRetryableError(ctx, err), err
	}
	defer resp.Body.Close()

	finalURL := preparedURL
	if resp.Request != nil && resp.Request.URL != nil {
		finalURL = resp.Request.URL.String()
	}
	responseHeaders := headerMap(resp.Header)
	result := Response{
		FetchURL:          preparedURL,
		FinalURL:          finalURL,
		SourceID:          req.Source.SourceID,
		Method:            method,
		StatusCode:        resp.StatusCode,
		FetchedAt:         c.now().UTC(),
		Latency:           time.Since(startedAt),
		HeaderContentType: sanitizeMediaType(resp.Header.Get("Content-Type")),
		ContentEncoding:   strings.TrimSpace(resp.Header.Get("Content-Encoding")),
		ETag:              strings.TrimSpace(resp.Header.Get("ETag")),
		LastModified:      strings.TrimSpace(resp.Header.Get("Last-Modified")),
		RequestHeaders:    headerMap(preparedHeaders),
		ResponseHeaders:   responseHeaders,
	}

	if resp.StatusCode == http.StatusNotModified {
		result.Success = true
		result.NotModified = true
		result.ContentType = result.HeaderContentType
		return result, false, nil
	}
	if isRetryableStatus(resp.StatusCode) {
		return result, true, fmt.Errorf("retryable status %d", resp.StatusCode)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		message, _ := readErrorBody(resp.Body)
		if message == "" {
			message = fmt.Sprintf("unexpected status %d", resp.StatusCode)
		}
		return result, false, errors.New(message)
	}

	if method == http.MethodHead {
		result.Success = true
		result.ContentType = result.HeaderContentType
		result.ContentHash = sha256Hex(nil)
		return result, false, nil
	}

	decodedBody, err := readDecodedBody(resp.Header, resp.Body, effectiveMaxBodyBytes(c.maxBodyBytes, req.Source.MaxBodyBytes))
	if err != nil {
		result.BodyBytes = decodedBody.bytesRead
		result.ContentType = chooseContentType(result.HeaderContentType, nil)
		return result, false, err
	}

	result.Body = decodedBody.body
	result.BodyBytes = int64(len(decodedBody.body))
	result.SniffedContentType = http.DetectContentType(sampleForSniffing(decodedBody.body))
	result.ContentType = chooseContentType(result.HeaderContentType, decodedBody.body)
	result.ContentHash = sha256Hex(decodedBody.body)
	result.Success = true
	return result, false, nil
}

func (p RetryPolicy) backoff(attempt int) time.Duration {
	if attempt <= 0 {
		attempt = 1
	}
	backoff := p.InitialBackoff
	for i := 1; i < attempt; i++ {
		backoff *= 2
		if backoff >= p.MaxBackoff {
			return p.MaxBackoff
		}
	}
	if backoff > p.MaxBackoff {
		return p.MaxBackoff
	}
	return backoff
}

func (p SourcePolicy) Validate() error {
	if p.Disabled {
		reason := strings.TrimSpace(p.DisabledReason)
		if reason == "" {
			reason = "source disabled"
		}
		return fmt.Errorf("%w: %s", ErrSourceBlocked, reason)
	}
	authMode := strings.ToLower(strings.TrimSpace(p.AuthMode))
	if authMode != "" && authMode != "none" {
		return fmt.Errorf("%w: auth mode %q requires an explicit non-public adapter", ErrSourceBlocked, authMode)
	}
	if !p.SupportsLiveGET {
		return fmt.Errorf("%w: live HTTP fetch disabled by source policy", ErrSourceBlocked)
	}
	return nil
}

type decodedBody struct {
	body      []byte
	bytesRead int64
}

func readDecodedBody(headers http.Header, body io.Reader, maxBytes int64) (decodedBody, error) {
	reader, err := decodeReader(strings.TrimSpace(headers.Get("Content-Encoding")), body)
	if err != nil {
		return decodedBody{}, err
	}
	if closer, ok := reader.(io.Closer); ok {
		defer closer.Close()
	}
	limited := &io.LimitedReader{R: reader, N: maxBytes + 1}
	data, err := io.ReadAll(limited)
	if err != nil {
		return decodedBody{}, err
	}
	bytesRead := int64(len(data))
	if bytesRead > maxBytes {
		return decodedBody{bytesRead: bytesRead}, BodyTooLargeError{Limit: maxBytes, Actual: bytesRead}
	}
	return decodedBody{body: data, bytesRead: bytesRead}, nil
}

func decodeReader(contentEncoding string, body io.Reader) (io.Reader, error) {
	encodings := splitEncodings(contentEncoding)
	decoded := body
	for i := len(encodings) - 1; i >= 0; i-- {
		switch encodings[i] {
		case "", "identity":
		case "gzip":
			gzipReader, err := gzip.NewReader(decoded)
			if err != nil {
				return nil, err
			}
			decoded = gzipReader
		case "br":
			decoded = io.NopCloser(brotli.NewReader(decoded))
		default:
			return nil, fmt.Errorf("unsupported content encoding %q", encodings[i])
		}
	}
	return decoded, nil
}

func splitEncodings(value string) []string {
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.ToLower(strings.TrimSpace(part))
		if trimmed != "" {
			out = append(out, trimmed)
		}
	}
	if len(out) == 0 {
		return []string{"identity"}
	}
	return out
}

func chooseContentType(headerValue string, body []byte) string {
	headerType := sanitizeMediaType(headerValue)
	if headerType != "" && headerType != "application/octet-stream" {
		return headerType
	}
	if len(body) > 0 {
		return http.DetectContentType(sampleForSniffing(body))
	}
	if headerType != "" {
		return headerType
	}
	return "application/octet-stream"
}

func sanitizeMediaType(value string) string {
	if strings.TrimSpace(value) == "" {
		return ""
	}
	mediaType, _, err := mime.ParseMediaType(value)
	if err != nil {
		return strings.ToLower(strings.TrimSpace(value))
	}
	return strings.ToLower(strings.TrimSpace(mediaType))
}

func effectiveMaxBodyBytes(defaultLimit, sourceLimit int64) int64 {
	if sourceLimit > 0 && sourceLimit < defaultLimit {
		return sourceLimit
	}
	if defaultLimit > 0 {
		return defaultLimit
	}
	if sourceLimit > 0 {
		return sourceLimit
	}
	return 16 << 20
}

func isRetryableStatus(status int) bool {
	switch status {
	case http.StatusRequestTimeout, http.StatusTooEarly, http.StatusTooManyRequests, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	default:
		return status >= 500 && status < 600
	}
}

func isRetryableError(ctx context.Context, err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return ctx.Err() == nil
	}
	return true
}

func cloneHeader(header http.Header) http.Header {
	if header == nil {
		return make(http.Header)
	}
	cloned := make(http.Header, len(header))
	for key, values := range header {
		copied := make([]string, len(values))
		copy(copied, values)
		cloned[key] = copied
	}
	return cloned
}

func headerMap(header http.Header) map[string][]string {
	cloned := cloneHeader(header)
	if len(cloned) == 0 {
		return nil
	}
	out := make(map[string][]string, len(cloned))
	for key, values := range cloned {
		out[key] = values
	}
	return out
}

func defaultSleep(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func readErrorBody(body io.Reader) (string, error) {
	limited := &io.LimitedReader{R: body, N: 8192}
	data, err := io.ReadAll(limited)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(data)), nil
}

func normalizeMethod(method string) string {
	trimmed := strings.TrimSpace(method)
	if trimmed == "" {
		return http.MethodGet
	}
	return strings.ToUpper(trimmed)
}

func sampleForSniffing(body []byte) []byte {
	if len(body) > 512 {
		return body[:512]
	}
	return body
}

func sha256Hex(body []byte) string {
	hash := sha256.Sum256(body)
	return hex.EncodeToString(hash[:])
}
