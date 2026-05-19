package embeddings

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// Client calls the embedding service to convert text to float32 vectors.
type Client struct {
	baseURL string
	http    *http.Client
}

// New creates a client targeting the embedding service at baseURL.
func New(baseURL string) *Client {
	return &Client{
		baseURL: baseURL,
		http:    &http.Client{},
	}
}

// Embed converts texts to normalized float32 vectors.
// Returns one vector per input text in the same order.
func (c *Client) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	if len(texts) == 0 {
		return nil, nil
	}

	body, err := json.Marshal(map[string]any{"texts": texts})
	if err != nil {
		return nil, fmt.Errorf("embeddings: marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/embed", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("embeddings: new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("embeddings: do request: %w", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("embeddings: service error %d: %s", resp.StatusCode, string(respBody))
	}

	var result struct {
		Vectors [][]float32 `json:"vectors"`
	}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("embeddings: decode response: %w", err)
	}
	if len(result.Vectors) != len(texts) {
		return nil, fmt.Errorf("embeddings: got %d vectors for %d texts", len(result.Vectors), len(texts))
	}
	return result.Vectors, nil
}
