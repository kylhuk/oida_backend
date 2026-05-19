// Package objectstore provides an AWS4-HMAC-SHA256-signed S3/MinIO client.
// It is the single implementation shared by bootstrap, workers, and the API
// server so that the signing logic is not duplicated across binaries.
package objectstore

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
	"sort"
	"strings"
	"time"
)

// Client is an S3/MinIO HTTP client that signs requests with AWS4-HMAC-SHA256.
// The zero value is not usable; construct with New.
type Client struct {
	endpoint  *url.URL
	accessKey string
	secretKey string
	region    string
	http      *http.Client
}

// New constructs a Client. endpoint must include scheme and host
// (e.g. "http://minio:9000"). region is typically "us-east-1" for MinIO.
func New(endpoint, accessKey, secretKey, region string) (*Client, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, fmt.Errorf("objectstore: parse endpoint %q: %w", endpoint, err)
	}
	if u.Scheme == "" || u.Host == "" {
		return nil, fmt.Errorf("objectstore: endpoint %q must include scheme and host", endpoint)
	}
	return &Client{
		endpoint:  u,
		accessKey: accessKey,
		secretKey: secretKey,
		region:    region,
		http:      &http.Client{Timeout: 30 * time.Second},
	}, nil
}

// PutObject uploads body to bucket/key with the given content type.
func (c *Client) PutObject(ctx context.Context, bucket, key string, body []byte, contentType string) error {
	resp, respBody, err := c.do(ctx, http.MethodPut, "/"+bucket+"/"+key, body, contentType)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("objectstore: PUT %s/%s: status %d: %s", bucket, key, resp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	return nil
}

// GetObject fetches bucket/key and returns the body and content type.
// Returns a non-nil error (wrapping ErrNotFound) when the object does not exist.
func (c *Client) GetObject(ctx context.Context, bucket, key string) ([]byte, string, error) {
	resp, respBody, err := c.do(ctx, http.MethodGet, "/"+bucket+"/"+key, nil, "")
	if err != nil {
		return nil, "", err
	}
	if resp.StatusCode == http.StatusNotFound {
		return nil, "", fmt.Errorf("objectstore: %w: %s/%s", ErrNotFound, bucket, key)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf("objectstore: GET %s/%s: status %d: %s", bucket, key, resp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	return respBody, strings.TrimSpace(resp.Header.Get("Content-Type")), nil
}

// ObjectExists reports whether bucket/key exists without downloading its body.
func (c *Client) ObjectExists(ctx context.Context, bucket, key string) (bool, error) {
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
		return false, fmt.Errorf("objectstore: HEAD %s/%s: status %d: %s", bucket, key, resp.StatusCode, strings.TrimSpace(string(body)))
	}
}

// BucketExists reports whether bucket exists.
func (c *Client) BucketExists(ctx context.Context, bucket string) (bool, error) {
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
		return false, fmt.Errorf("objectstore: HEAD /%s: status %d: %s", bucket, resp.StatusCode, strings.TrimSpace(string(body)))
	}
}

// CreateBucket creates bucket.
func (c *Client) CreateBucket(ctx context.Context, bucket string) error {
	resp, body, err := c.do(ctx, http.MethodPut, "/"+bucket, nil, "")
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("objectstore: PUT /%s: status %d: %s", bucket, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

// Ping checks connectivity by listing the root path.
func (c *Client) Ping(ctx context.Context) error {
	resp, body, err := c.do(ctx, http.MethodGet, "/", nil, "")
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("objectstore: ping: status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

// ErrNotFound is returned by GetObject when the object does not exist.
var ErrNotFound = fmt.Errorf("not found")

// do performs a signed S3 request and returns the response, body bytes, and any transport error.
func (c *Client) do(ctx context.Context, method, rawPath string, body []byte, contentType string) (*http.Response, []byte, error) {
	canonicalPath := escapePath(joinPath(c.endpoint.Path, rawPath))
	reqURL := *c.endpoint
	reqURL.Path = canonicalPath
	reqURL.RawPath = canonicalPath
	reqURL.RawQuery = ""

	payloadHash := hashHex(body)
	now := time.Now().UTC()
	amzDate := now.Format("20060102T150405Z")
	dateStamp := now.Format("20060102")

	req, err := http.NewRequestWithContext(ctx, method, reqURL.String(), bytes.NewReader(body))
	if err != nil {
		return nil, nil, err
	}
	req.Host = c.endpoint.Host
	req.Header.Set("x-amz-content-sha256", payloadHash)
	req.Header.Set("x-amz-date", amzDate)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}

	headers := map[string]string{
		"host":                 req.Host,
		"x-amz-content-sha256": payloadHash,
		"x-amz-date":           amzDate,
	}
	if contentType != "" {
		headers["content-type"] = contentType
	}
	signedHeaderNames := sortedKeys(headers)
	var hb strings.Builder
	for _, name := range signedHeaderNames {
		hb.WriteString(name)
		hb.WriteByte(':')
		hb.WriteString(strings.TrimSpace(headers[name]))
		hb.WriteByte('\n')
	}
	signedHeaderList := strings.Join(signedHeaderNames, ";")
	credentialScope := strings.Join([]string{dateStamp, c.region, "s3", "aws4_request"}, "/")
	canonicalRequest := strings.Join([]string{
		method, canonicalPath, "",
		hb.String(), signedHeaderList, payloadHash,
	}, "\n")
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256", amzDate, credentialScope, hashHex([]byte(canonicalRequest)),
	}, "\n")
	sig := hex.EncodeToString(signV4(c.secretKey, dateStamp, c.region, "s3", stringToSign))
	req.Header.Set("Authorization", fmt.Sprintf(
		"AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		c.accessKey, credentialScope, signedHeaderList, sig,
	))

	resp, err := c.http.Do(req)
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

func hashHex(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
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
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
