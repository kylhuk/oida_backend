package fetch

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"path"
	"strings"
	"time"
)

type ReplayClass string

const (
	ReplayClassCached     ReplayClass = "cached"
	ReplayClassReplayOnly ReplayClass = "replay-only"
	ReplayClassLive       ReplayClass = "live"
)

type RetentionPolicy struct {
	Name               string
	InlineBodyMaxBytes int
	ForceObjectStore   bool
	ReplayClass        ReplayClass
	ObjectPrefix       string
}

type ObjectStore interface {
	PutObject(ctx context.Context, bucket, key string, body []byte, contentType string) error
	GetObject(ctx context.Context, bucket, key string) ([]byte, string, error)
}

type PersistOptions struct {
	FetchID  string
	RawID    string
	SourceID string
	Bucket   string
	Policy   RetentionPolicy
	Now      time.Time
}

type FetchLogRow struct {
	FetchID      string  `json:"fetch_id"`
	SourceID     string  `json:"source_id"`
	URLHash      string  `json:"url_hash"`
	StatusCode   uint16  `json:"status_code"`
	Success      uint8   `json:"success"`
	FetchedAt    string  `json:"fetched_at"`
	LatencyMS    uint32  `json:"latency_ms"`
	AttemptCount uint16  `json:"attempt_count"`
	RetryCount   uint16  `json:"retry_count"`
	BodyBytes    uint64  `json:"body_bytes"`
	ErrorMessage *string `json:"error_message,omitempty"`
}

type RawDocumentRow struct {
	RawID         string  `json:"raw_id"`
	FetchID       string  `json:"fetch_id"`
	SourceID      string  `json:"source_id"`
	URL           string  `json:"url"`
	FinalURL      string  `json:"final_url"`
	FetchedAt     string  `json:"fetched_at"`
	StatusCode    uint16  `json:"status_code"`
	ContentType   string  `json:"content_type"`
	ContentHash   string  `json:"content_hash"`
	BodyBytes     uint64  `json:"body_bytes"`
	ObjectKey     *string `json:"object_key,omitempty"`
	ETag          *string `json:"etag,omitempty"`
	LastModified  *string `json:"last_modified,omitempty"`
	NotModified   uint8   `json:"not_modified"`
	StorageClass  string  `json:"storage_class"`
	FetchMetadata string  `json:"fetch_metadata"`
}

type StoredFetch struct {
	FetchLog    FetchLogRow     `json:"fetch_log"`
	RawDocument *RawDocumentRow `json:"raw_document,omitempty"`
	Metadata    Metadata        `json:"metadata"`
}

type Metadata struct {
	RetentionClass     string              `json:"retention_class"`
	ReplayClass        ReplayClass         `json:"replay_class"`
	RequestMethod      string              `json:"request_method"`
	FetchURL           string              `json:"fetch_url"`
	FinalURL           string              `json:"final_url,omitempty"`
	StatusCode         int                 `json:"status_code"`
	Conditional        ConditionalRequest  `json:"conditional"`
	HeaderContentType  string              `json:"header_content_type,omitempty"`
	SniffedContentType string              `json:"sniffed_content_type,omitempty"`
	ContentEncoding    string              `json:"content_encoding,omitempty"`
	ETag               string              `json:"etag,omitempty"`
	LastModified       string              `json:"last_modified,omitempty"`
	RequestHeaders     map[string][]string `json:"request_headers,omitempty"`
	ResponseHeaders    map[string][]string `json:"response_headers,omitempty"`
	RetryCount         int                 `json:"retry_count"`
	RetryReasons       []string            `json:"retry_reasons,omitempty"`
	StorageClass       string              `json:"storage_class"`
	ObjectBucket       string              `json:"object_bucket,omitempty"`
	ObjectKey          string              `json:"object_key,omitempty"`
	InlineBodyBase64   string              `json:"inline_body_base64,omitempty"`
	InlineBodyType     string              `json:"inline_body_type,omitempty"`
	NotModified        bool                `json:"not_modified,omitempty"`
	ErrorMessage       string              `json:"error_message,omitempty"`
	StoredAt           string              `json:"stored_at"`
	BodyBytes          uint64              `json:"body_bytes"`
	ContentHash        string              `json:"content_hash,omitempty"`
	ContentType        string              `json:"content_type,omitempty"`
	SourceID           string              `json:"source_id"`
	RawID              string              `json:"raw_id,omitempty"`
	FetchID            string              `json:"fetch_id,omitempty"`
	URLHash            string              `json:"url_hash,omitempty"`
	Replayable         bool                `json:"replayable"`
	ReplaySource       string              `json:"replay_source,omitempty"`
	Provenance         map[string]string   `json:"provenance,omitempty"`
}

type ReplayResult struct {
	Body        []byte
	ContentType string
	Metadata    Metadata
	ObjectKey   string
}

func ResolveRetentionPolicy(class string) RetentionPolicy {
	switch strings.ToLower(strings.TrimSpace(class)) {
	case "hot":
		return RetentionPolicy{Name: "hot", InlineBodyMaxBytes: 256 << 10, ReplayClass: ReplayClassCached, ObjectPrefix: "hot"}
	case "cold":
		return RetentionPolicy{Name: "cold", ForceObjectStore: true, ReplayClass: ReplayClassCached, ObjectPrefix: "cold"}
	case "archive":
		return RetentionPolicy{Name: "archive", ForceObjectStore: true, ReplayClass: ReplayClassReplayOnly, ObjectPrefix: "archive"}
	case "live":
		return RetentionPolicy{Name: "live", InlineBodyMaxBytes: 32 << 10, ReplayClass: ReplayClassLive, ObjectPrefix: "live"}
	default:
		return RetentionPolicy{Name: "warm", InlineBodyMaxBytes: 64 << 10, ReplayClass: ReplayClassCached, ObjectPrefix: "warm"}
	}
}

func RetainResponse(ctx context.Context, opts PersistOptions, req Request, resp Response, store ObjectStore) (StoredFetch, error) {
	policy := opts.Policy
	if policy.Name == "" {
		policy = ResolveRetentionPolicy(req.Source.RetentionClass)
	}
	if req.Source.ForceObjectStore {
		policy.ForceObjectStore = true
		policy.InlineBodyMaxBytes = 0
	}
	if opts.Now.IsZero() {
		opts.Now = time.Now().UTC()
	}
	urlHash := sha256Hex([]byte(firstNonEmpty(resp.FinalURL, resp.FetchURL, req.URL)))
	metadata := Metadata{
		RetentionClass:     policy.Name,
		ReplayClass:        policy.ReplayClass,
		RequestMethod:      resp.Method,
		FetchURL:           firstNonEmpty(resp.FetchURL, req.URL),
		FinalURL:           resp.FinalURL,
		StatusCode:         resp.StatusCode,
		Conditional:        req.Conditional,
		HeaderContentType:  resp.HeaderContentType,
		SniffedContentType: resp.SniffedContentType,
		ContentEncoding:    resp.ContentEncoding,
		ETag:               resp.ETag,
		LastModified:       resp.LastModified,
		RequestHeaders:     resp.RequestHeaders,
		ResponseHeaders:    resp.ResponseHeaders,
		RetryCount:         maxInt(resp.Attempts-1, 0),
		RetryReasons:       append([]string(nil), resp.RetryReasons...),
		StorageClass:       "metadata-only",
		NotModified:        resp.NotModified,
		ErrorMessage:       strings.TrimSpace(resp.ErrorMessage),
		StoredAt:           formatStoredTime(opts.Now),
		BodyBytes:          uint64(maxInt64(resp.BodyBytes, 0)),
		ContentHash:        resp.ContentHash,
		ContentType:        resp.ContentType,
		SourceID:           opts.SourceID,
		RawID:              opts.RawID,
		FetchID:            opts.FetchID,
		URLHash:            urlHash,
		Replayable:         policy.ReplayClass != ReplayClassLive,
		Provenance: map[string]string{
			"etag":          resp.ETag,
			"last_modified": resp.LastModified,
		},
	}

	var errorMessage *string
	if metadata.ErrorMessage != "" {
		errCopy := metadata.ErrorMessage
		errorMessage = &errCopy
	}
	stored := StoredFetch{
		FetchLog: FetchLogRow{
			FetchID:      opts.FetchID,
			SourceID:     opts.SourceID,
			URLHash:      urlHash,
			StatusCode:   uint16(maxInt(resp.StatusCode, 0)),
			Success:      boolToUint8(resp.Success),
			FetchedAt:    formatStoredTime(resp.FetchedAt),
			LatencyMS:    durationMillis(resp.Latency),
			AttemptCount: uint16(maxInt(resp.Attempts, 1)),
			RetryCount:   uint16(maxInt(resp.Attempts-1, 0)),
			BodyBytes:    metadata.BodyBytes,
			ErrorMessage: errorMessage,
		},
		Metadata: metadata,
	}

	if !resp.Success && !resp.NotModified {
		return stored, nil
	}

	raw := &RawDocumentRow{
		RawID:        opts.RawID,
		FetchID:      opts.FetchID,
		SourceID:     opts.SourceID,
		URL:          firstNonEmpty(resp.FetchURL, req.URL),
		FinalURL:     firstNonEmpty(resp.FinalURL, resp.FetchURL, req.URL),
		FetchedAt:    formatStoredTime(resp.FetchedAt),
		StatusCode:   uint16(maxInt(resp.StatusCode, 0)),
		ContentType:  firstNonEmpty(resp.ContentType, resp.HeaderContentType, "application/octet-stream"),
		ContentHash:  resp.ContentHash,
		BodyBytes:    metadata.BodyBytes,
		ETag:         stringPtr(resp.ETag),
		LastModified: stringPtr(resp.LastModified),
		NotModified:  boolToUint8(resp.NotModified),
		StorageClass: metadata.StorageClass,
	}

	if len(resp.Body) > 0 {
		if shouldStoreAsObject(policy, len(resp.Body)) {
			if store == nil {
				return stored, fmt.Errorf("retention policy %q requires object storage", policy.Name)
			}
			objectKey := buildObjectKey(policy, opts.SourceID, resp.FetchedAt, raw.ContentHash, raw.ContentType)
			if err := store.PutObject(ctx, opts.Bucket, objectKey, resp.Body, raw.ContentType); err != nil {
				return stored, fmt.Errorf("store object %s: %w", objectKey, err)
			}
			raw.ObjectKey = stringPtr(objectKey)
			metadata.ObjectBucket = opts.Bucket
			metadata.ObjectKey = objectKey
			metadata.StorageClass = "object-store"
			metadata.ReplaySource = "object-store"
		} else {
			metadata.InlineBodyBase64 = base64.StdEncoding.EncodeToString(resp.Body)
			metadata.InlineBodyType = "base64"
			metadata.StorageClass = "inline"
			metadata.ReplaySource = "inline"
		}
	}

	metaJSON, err := json.Marshal(metadata)
	if err != nil {
		return stored, fmt.Errorf("marshal fetch metadata: %w", err)
	}
	raw.FetchMetadata = string(metaJSON)
	raw.StorageClass = metadata.StorageClass
	stored.RawDocument = raw
	stored.Metadata = metadata
	return stored, nil
}

func Replay(ctx context.Context, doc RawDocumentRow, store ObjectStore) (ReplayResult, error) {
	var metadata Metadata
	if err := json.Unmarshal([]byte(doc.FetchMetadata), &metadata); err != nil {
		return ReplayResult{}, fmt.Errorf("decode fetch metadata: %w", err)
	}
	result := ReplayResult{
		Metadata:    metadata,
		ContentType: firstNonEmpty(doc.ContentType, metadata.ContentType, "application/octet-stream"),
	}
	if metadata.InlineBodyBase64 != "" {
		body, err := base64.StdEncoding.DecodeString(metadata.InlineBodyBase64)
		if err != nil {
			return ReplayResult{}, fmt.Errorf("decode inline body: %w", err)
		}
		result.Body = body
		result.Metadata.ReplaySource = "inline"
		return result, nil
	}
	if metadata.ObjectBucket == "" || metadata.ObjectKey == "" {
		return result, nil
	}
	if store == nil {
		return ReplayResult{}, fmt.Errorf("object store required to replay %s", metadata.ObjectKey)
	}
	body, contentType, err := store.GetObject(ctx, metadata.ObjectBucket, metadata.ObjectKey)
	if err != nil {
		return ReplayResult{}, err
	}
	result.Body = body
	result.ObjectKey = metadata.ObjectKey
	result.ContentType = firstNonEmpty(contentType, result.ContentType)
	result.Metadata.ReplaySource = "object-store"
	return result, nil
}

func shouldStoreAsObject(policy RetentionPolicy, bodyLen int) bool {
	if policy.ForceObjectStore {
		return true
	}
	if policy.InlineBodyMaxBytes <= 0 {
		return bodyLen > 0
	}
	return bodyLen > policy.InlineBodyMaxBytes
}

func buildObjectKey(policy RetentionPolicy, sourceID string, fetchedAt time.Time, contentHash, contentType string) string {
	ext := extensionForContentType(contentType)
	if fetchedAt.IsZero() {
		fetchedAt = time.Now().UTC()
	}
	cleanSource := sanitizePathSegment(sourceID)
	cleanHash := sanitizePathSegment(firstNonEmpty(contentHash, sha256Hex([]byte(sourceID+fetchedAt.String()))))
	return path.Join(
		"raw",
		sanitizePathSegment(firstNonEmpty(policy.ObjectPrefix, policy.Name, "warm")),
		cleanSource,
		fetchedAt.UTC().Format("2006/01/02/150405.000"),
		cleanHash+ext,
	)
}

func extensionForContentType(contentType string) string {
	switch strings.ToLower(strings.TrimSpace(contentType)) {
	case "text/html":
		return ".html"
	case "application/json", "text/json":
		return ".json"
	case "application/xml", "text/xml":
		return ".xml"
	case "text/plain":
		return ".txt"
	default:
		return ".bin"
	}
}

func sanitizePathSegment(value string) string {
	trimmed := strings.TrimSpace(strings.ToLower(value))
	if trimmed == "" {
		return "unknown"
	}
	replacer := strings.NewReplacer(
		":", "-",
		"/", "-",
		"\\", "-",
		" ", "-",
	)
	return replacer.Replace(trimmed)
}

func boolToUint8(value bool) uint8 {
	if value {
		return 1
	}
	return 0
}

func durationMillis(duration time.Duration) uint32 {
	if duration <= 0 {
		return 0
	}
	ms := duration.Milliseconds()
	if ms > int64(^uint32(0)) {
		return ^uint32(0)
	}
	return uint32(ms)
}

func maxInt(current, floor int) int {
	if current < floor {
		return floor
	}
	return current
}

func maxInt64(current, floor int64) int64 {
	if current < floor {
		return floor
	}
	return current
}

func formatStoredTime(at time.Time) string {
	if at.IsZero() {
		at = time.Now().UTC()
	}
	return at.UTC().Format("2006-01-02 15:04:05.000")
}

func stringPtr(value string) *string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil
	}
	return &trimmed
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}
