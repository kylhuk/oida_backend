package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// compile-time assertion that clickhouseClient implements clickhouseExecer.
var _ clickhouseExecer = (*clickhouseClient)(nil)

// ExecRequest is a ClickHouse query with optional parameters, settings, and format.
type ExecRequest struct {
	SQL      string
	Params   map[string]string // bound as param_<name>=value HTTP query params
	Settings map[string]string // bound as HTTP query params (e.g. max_execution_time=5)
	Format   string            // default "JSONEachRow"; use "JSON" to get envelope with rows_before_limit_at_least
}

// ExecResponse is the parsed response from a ClickHouse query.
type ExecResponse struct {
	Rows                   []map[string]any
	RowsBeforeLimitAtLeast *uint64
	RawBody                string // always populated; rows is nil when Format != JSON
}

// clickhouseExecer extends clickhouseQuerier with parameterized execution.
type clickhouseExecer interface {
	clickhouseQuerier
	Exec(ctx context.Context, req ExecRequest) (ExecResponse, error)
}

// Exec issues a parameterized ClickHouse HTTP query and returns the parsed response.
func (c *clickhouseClient) Exec(ctx context.Context, req ExecRequest) (ExecResponse, error) {
	sql := req.SQL

	// Append FORMAT JSON if requested and not already present.
	if req.Format == "JSON" {
		upper := strings.ToUpper(strings.TrimSpace(sql))
		if !strings.HasSuffix(upper, "FORMAT JSON") {
			sql = strings.TrimRight(sql, " \t\r\n") + " FORMAT JSON"
		}
	}

	// Build URL with query param.
	params := url.Values{}
	params.Set("query", sql)

	// Bind param_<name>=value for each entry in req.Params.
	for k, v := range req.Params {
		params.Set("param_"+k, v)
	}

	// Bind settings directly as URL query params.
	for k, v := range req.Settings {
		params.Set(k, v)
	}

	requestURL := c.baseURL + "/?" + params.Encode()

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, requestURL, nil)
	if err != nil {
		return ExecResponse{}, err
	}
	if c.username != "" {
		httpReq.SetBasicAuth(c.username, c.password)
	}

	resp, err := c.client.Do(httpReq)
	if err != nil {
		return ExecResponse{}, err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= http.StatusMultipleChoices {
		return ExecResponse{}, fmt.Errorf("clickhouse http %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	rawBody := string(body)

	if req.Format == "JSON" {
		var chResponse struct {
			Data                   []map[string]any `json:"data"`
			Rows                   uint64           `json:"rows"`
			RowsBeforeLimitAtLeast *uint64          `json:"rows_before_limit_at_least"`
		}
		if err := json.Unmarshal(body, &chResponse); err != nil {
			return ExecResponse{}, fmt.Errorf("clickhouse json decode: %w", err)
		}
		return ExecResponse{
			Rows:                   chResponse.Data,
			RowsBeforeLimitAtLeast: chResponse.RowsBeforeLimitAtLeast,
			RawBody:                rawBody,
		}, nil
	}

	return ExecResponse{RawBody: rawBody}, nil
}
