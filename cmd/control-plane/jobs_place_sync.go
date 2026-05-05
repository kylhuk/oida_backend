package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"sort"
	"strings"
	"time"

	"global-osint-backend/internal/migrate"
)

const (
	geoBoundariesSyncJobName           = "geoboundaries-sync"
	geoNamesSyncJobName                = "geonames-sync"
	defaultPlaceStagePrefix            = "place-datasets"
	defaultGeoBoundariesGBOpenURL      = "https://www.geoboundaries.org/api/current/gbOpen/ALL/ALL/"
	defaultGeoNamesCountryInfoURL      = "https://download.geonames.org/export/dump/countryInfo.txt"
	defaultGeoNamesAdmin1CodesURL      = "https://download.geonames.org/export/dump/admin1CodesASCII.txt"
	defaultGeoNamesAdmin2CodesURL      = "https://download.geonames.org/export/dump/admin2Codes.txt"
	defaultGeoNamesHierarchyURL        = "https://download.geonames.org/export/dump/hierarchy.zip"
	defaultPlaceSyncHTTPTimeout        = 2 * time.Minute
	defaultPlaceSyncUserAgent          = "oida-control-plane/place-sync"
	defaultControlPlaneMinIOEndpoint   = "http://minio:9000"
	defaultControlPlaneMinIORegion     = "us-east-1"
	defaultGeoBoundariesStageObject    = "gbOpen_ALL_ALL.json"
)

type placeSyncAssetSpec struct {
	Name        string
	SourceURL   string
	StageObject string
	ContentType string
}

type placeSyncAssetResult struct {
	Name        string `json:"name"`
	SourceURL   string `json:"source_url"`
	StageObject string `json:"stage_object"`
	ContentType string `json:"content_type"`
	Bytes       int    `json:"bytes"`
	SHA256      string `json:"sha256"`
}

type controlPlaneS3Client struct {
	endpoint  *url.URL
	accessKey string
	secretKey string
	region    string
	client    *http.Client
}

func init() {
	jobRegistry[geoBoundariesSyncJobName] = jobRunner{
		description: "Download and stage geoBoundaries gbOpen metadata.",
		run:         runGeoBoundariesSync,
	}
	jobRegistry[geoNamesSyncJobName] = jobRunner{
		description: "Download and stage GeoNames admin datasets.",
		run:         runGeoNamesSync,
	}
}

func runGeoBoundariesSync(ctx context.Context) error {
	return runPlaceDatasetSync(ctx, geoBoundariesSyncJobName, "staged geoBoundaries gbOpen metadata", []placeSyncAssetSpec{{
		Name:        "gbOpen_ALL_ALL",
		SourceURL:   ctlGetenv("GEOBOUNDARIES_GBOPEN_URL", defaultGeoBoundariesGBOpenURL),
		StageObject: defaultGeoBoundariesStageObject,
		ContentType: "application/json",
	}})
}

func runGeoNamesSync(ctx context.Context) error {
	return runPlaceDatasetSync(ctx, geoNamesSyncJobName, "staged GeoNames admin datasets", []placeSyncAssetSpec{
		{
			Name:        "countryInfo",
			SourceURL:   ctlGetenv("GEONAMES_COUNTRY_INFO_URL", defaultGeoNamesCountryInfoURL),
			StageObject: "countryInfo.txt",
			ContentType: "text/plain; charset=utf-8",
		},
		{
			Name:        "admin1CodesASCII",
			SourceURL:   ctlGetenv("GEONAMES_ADMIN1_CODES_URL", defaultGeoNamesAdmin1CodesURL),
			StageObject: "admin1CodesASCII.txt",
			ContentType: "text/plain; charset=utf-8",
		},
		{
			Name:        "admin2Codes",
			SourceURL:   ctlGetenv("GEONAMES_ADMIN2_CODES_URL", defaultGeoNamesAdmin2CodesURL),
			StageObject: "admin2Codes.txt",
			ContentType: "text/plain; charset=utf-8",
		},
		{
			Name:        "hierarchy",
			SourceURL:   ctlGetenv("GEONAMES_HIERARCHY_URL", defaultGeoNamesHierarchyURL),
			StageObject: "hierarchy.zip",
			ContentType: "application/zip",
		},
	})
}

func runPlaceDatasetSync(ctx context.Context, jobName, successMessage string, assets []placeSyncAssetSpec) error {
	startedAt := time.Now().UTC().Truncate(time.Millisecond)
	runner := migrate.NewHTTPRunner(controlPlaneClickHouseURL())
	jobID := fmt.Sprintf("job:%s:%d", jobName, startedAt.UnixMilli())

	recordFailure := func(err error, message string, stats map[string]any) error {
		if recordErr := recordJobRun(ctx, runner, jobID, jobName, "failed", startedAt, time.Now().UTC().Truncate(time.Millisecond), message, stats); recordErr != nil {
			return fmt.Errorf("%w (job log failed: %v)", err, recordErr)
		}
		return err
	}

	stageBucket := ctlGetenv("STAGE_BUCKET", defaultStageBucketName)
	stagePrefix := strings.Trim(path.Clean(strings.TrimSpace(ctlGetenv("PLACE_STAGE_PREFIX", defaultPlaceStagePrefix))), "/.")
	if stagePrefix == "" {
		stagePrefix = defaultPlaceStagePrefix
	}
	objectStore, err := newControlPlaneS3Client()
	if err != nil {
		return recordFailure(err, "configure MinIO staging client", map[string]any{"stage_bucket": stageBucket, "stage_prefix": stagePrefix})
	}
	httpClient := &http.Client{Timeout: controlPlaneDurationEnv("PLACE_SYNC_HTTP_TIMEOUT", defaultPlaceSyncHTTPTimeout)}
	results := make([]placeSyncAssetResult, 0, len(assets))

	for _, asset := range assets {
		body, contentType, err := fetchPlaceSyncAsset(ctx, httpClient, asset)
		if err != nil {
			return recordFailure(err, "download place dataset asset", map[string]any{"asset": asset.Name, "source_url": asset.SourceURL})
		}
		stageObject := path.Join(stagePrefix, jobName, "current", strings.TrimLeft(asset.StageObject, "/"))
		if err := objectStore.PutObject(ctx, stageBucket, stageObject, body, contentType); err != nil {
			return recordFailure(err, "stage place dataset asset to MinIO", map[string]any{"asset": asset.Name, "source_url": asset.SourceURL, "stage_bucket": stageBucket, "stage_object": stageObject})
		}
		results = append(results, placeSyncAssetResult{
			Name:        asset.Name,
			SourceURL:   asset.SourceURL,
			StageObject: stageObject,
			ContentType: contentType,
			Bytes:       len(body),
			SHA256:      controlPlaneSHA256Hex(body),
		})
	}

	stats := map[string]any{
		"stage_bucket": stageBucket,
		"stage_prefix": stagePrefix,
		"asset_count":  len(results),
		"assets":       results,
	}
	if err := recordJobRun(ctx, runner, jobID, jobName, "success", startedAt, time.Now().UTC().Truncate(time.Millisecond), successMessage, stats); err != nil {
		return err
	}
	return nil
}

func fetchPlaceSyncAsset(ctx context.Context, client *http.Client, asset placeSyncAssetSpec) ([]byte, string, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimSpace(asset.SourceURL), nil)
	if err != nil {
		return nil, "", err
	}
	request.Header.Set("User-Agent", defaultPlaceSyncUserAgent)
	response, err := client.Do(request)
	if err != nil {
		return nil, "", err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 2048))
		return nil, "", fmt.Errorf("unexpected status %d from %s: %s", response.StatusCode, asset.SourceURL, strings.TrimSpace(string(body)))
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, "", err
	}
	contentType := strings.TrimSpace(response.Header.Get("Content-Type"))
	if contentType == "" {
		contentType = strings.TrimSpace(asset.ContentType)
	}
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	return body, contentType, nil
}

func newControlPlaneS3Client() (*controlPlaneS3Client, error) {
	endpoint, err := url.Parse(ctlGetenv("MINIO_ENDPOINT", defaultControlPlaneMinIOEndpoint))
	if err != nil {
		return nil, fmt.Errorf("parse MinIO endpoint: %w", err)
	}
	return &controlPlaneS3Client{
		endpoint:  endpoint,
		accessKey: ctlGetenv("MINIO_ACCESS_KEY", ctlGetenv("S3_ACCESS_KEY", ctlGetenv("MINIO_ROOT_USER", "minioadmin"))),
		secretKey: ctlGetenv("MINIO_SECRET_KEY", ctlGetenv("S3_SECRET_KEY", ctlGetenv("MINIO_ROOT_PASSWORD", "minioadmin"))),
		region:    ctlGetenv("MINIO_REGION", defaultControlPlaneMinIORegion),
		client:    &http.Client{Timeout: controlPlaneDurationEnv("PLACE_SYNC_MINIO_TIMEOUT", defaultPlaceSyncHTTPTimeout)},
	}, nil
}

func (c *controlPlaneS3Client) PutObject(ctx context.Context, bucket, key string, body []byte, contentType string) error {
	response, responseBody, err := c.do(ctx, http.MethodPut, "/"+bucket+"/"+key, body, contentType)
	if err != nil {
		return err
	}
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status %d: %s", response.StatusCode, strings.TrimSpace(string(responseBody)))
	}
	return nil
}

func (c *controlPlaneS3Client) do(ctx context.Context, method, rawPath string, body []byte, contentType string) (*http.Response, []byte, error) {
	canonicalPath := controlPlaneEscapePath(controlPlaneJoinPath(c.endpoint.Path, rawPath))
	requestURL := *c.endpoint
	requestURL.Path = canonicalPath
	requestURL.RawPath = canonicalPath
	requestURL.RawQuery = ""

	payloadHash := controlPlaneSHA256Hex(body)
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
	signedHeaders := controlPlaneSortedKeys(canonicalHeaders)
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
		controlPlaneSHA256Hex([]byte(canonicalRequest)),
	}, "\n")
	signature := hex.EncodeToString(controlPlaneSignV4(c.secretKey, dateStamp, c.region, "s3", stringToSign))
	authorization := fmt.Sprintf(
		"AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		c.accessKey,
		credentialScope,
		strings.Join(signedHeaders, ";"),
		signature,
	)
	req.Header.Set("Authorization", authorization)

	response, err := c.client.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, nil, err
	}
	return response, responseBody, nil
}

func controlPlaneSignV4(secret, dateStamp, region, service, stringToSign string) []byte {
	kDate := controlPlaneHMACSHA256([]byte("AWS4"+secret), dateStamp)
	kRegion := controlPlaneHMACSHA256(kDate, region)
	kService := controlPlaneHMACSHA256(kRegion, service)
	kSigning := controlPlaneHMACSHA256(kService, "aws4_request")
	return controlPlaneHMACSHA256(kSigning, stringToSign)
}

func controlPlaneHMACSHA256(key []byte, value string) []byte {
	h := hmac.New(sha256.New, key)
	_, _ = h.Write([]byte(value))
	return h.Sum(nil)
}

func controlPlaneSHA256Hex(body []byte) string {
	hash := sha256.Sum256(body)
	return hex.EncodeToString(hash[:])
}

func controlPlaneJoinPath(basePath, rawPath string) string {
	basePath = strings.TrimRight(basePath, "/")
	rawPath = "/" + strings.TrimLeft(rawPath, "/")
	if basePath == "" {
		return rawPath
	}
	return basePath + rawPath
}

func controlPlaneEscapePath(rawPath string) string {
	if rawPath == "" {
		return "/"
	}
	parts := strings.Split(rawPath, "/")
	for idx, part := range parts {
		parts[idx] = url.PathEscape(part)
	}
	escaped := strings.Join(parts, "/")
	if !strings.HasPrefix(escaped, "/") {
		escaped = "/" + escaped
	}
	if strings.HasSuffix(rawPath, "/") && !strings.HasSuffix(escaped, "/") {
		escaped += "/"
	}
	return escaped
}

func controlPlaneSortedKeys(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func controlPlaneDurationEnv(key string, fallback time.Duration) time.Duration {
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
