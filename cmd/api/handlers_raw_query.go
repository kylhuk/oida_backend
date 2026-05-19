package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"global-osint-backend/internal/oidaql"
)

const (
	rawQueryMaxBodyBytes = 64 * 1024
	rawQueryDefaultLimit = 1000
	rawQueryMaxLimit     = 10000
	rawQueryMaxTimeout   = 60000
)

type rawQueryRequest struct {
	Dialect     string            `json:"dialect"`
	QueryText   string            `json:"query_text"`
	Parameters  map[string]any    `json:"parameters"`
	ResultMode  string            `json:"result_mode"`
	SnapshotID  string            `json:"snapshot_id"`
	ResultLimit *int              `json:"result_limit"`
	TimeoutMs   *int              `json:"timeout_ms"`
}

func (s *apiServer) rawQueryHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Decode request body.
		r.Body = http.MaxBytesReader(w, r.Body, rawQueryMaxBodyBytes)
		var req rawQueryRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			respondError(w, s.version, http.StatusBadRequest, "invalid_request", "invalid JSON body: "+err.Error(), r.URL.Path)
			return
		}

		// Validate required fields.
		if req.Dialect == "" {
			respondError(w, s.version, http.StatusBadRequest, "invalid_request", "dialect is required", r.URL.Path)
			return
		}
		if req.QueryText == "" {
			respondError(w, s.version, http.StatusBadRequest, "invalid_request", "query_text is required", r.URL.Path)
			return
		}
		if req.ResultMode != "selection" && req.ResultMode != "tabular" {
			respondError(w, s.version, http.StatusBadRequest, "invalid_request", `result_mode must be "selection" or "tabular"`, r.URL.Path)
			return
		}

		// Validate and resolve result_limit.
		limit := rawQueryDefaultLimit
		if req.ResultLimit != nil {
			if *req.ResultLimit < 1 || *req.ResultLimit > rawQueryMaxLimit {
				respondError(w, s.version, http.StatusBadRequest, "invalid_request",
					fmt.Sprintf("result_limit must be between 1 and %d", rawQueryMaxLimit), r.URL.Path)
				return
			}
			limit = *req.ResultLimit
		}

		// Validate and resolve timeout.
		timeoutMs := int(s.queryTimeout / time.Millisecond)
		if req.TimeoutMs != nil {
			if *req.TimeoutMs < 1 || *req.TimeoutMs > rawQueryMaxTimeout {
				respondError(w, s.version, http.StatusBadRequest, "invalid_request",
					fmt.Sprintf("timeout_ms must be between 1 and %d", rawQueryMaxTimeout), r.URL.Path)
				return
			}
			timeoutMs = *req.TimeoutMs
		}

		// Validate snapshot_id.
		snapshotID := req.SnapshotID
		if snapshotID == "" {
			snapshotID = "live"
		}
		if err := s.validateSnapshotID(r.Context(), snapshotID); err != nil {
			respondError(w, s.version, http.StatusBadRequest, "invalid_snapshot", err.Error(), r.URL.Path)
			return
		}

		// Validate dialect is registered.
		if err := s.validateDialect(r.Context(), req.Dialect); err != nil {
			respondError(w, s.version, http.StatusBadRequest, "invalid_request", err.Error(), r.URL.Path)
			return
		}

		// Compile OIDA-QL to physical SQL.
		compiled, err := oidaql.Compile(req.QueryText)
		if err != nil {
			respondError(w, s.version, http.StatusBadRequest, "invalid_request", "query compile error: "+err.Error(), r.URL.Path)
			return
		}

		// Wrap compiled SQL with outer LIMIT (overrides any inner LIMIT from the emitter).
		var wrappedSQL string
		if req.ResultMode == "selection" {
			wrappedSQL = fmt.Sprintf("SELECT entity_id FROM (%s) AS oq LIMIT %d", compiled, limit)
		} else {
			wrappedSQL = fmt.Sprintf("SELECT * FROM (%s) AS oq LIMIT %d", compiled, limit)
		}

		// Build bound parameters from req.Parameters.
		params := make(map[string]string, len(req.Parameters))
		for k, v := range req.Parameters {
			params[k] = fmt.Sprintf("%v", v)
		}

		execTimeout := time.Duration(timeoutMs) * time.Millisecond
		ctx, cancel := context.WithTimeout(r.Context(), execTimeout)
		defer cancel()

		resp, execErr := s.exec.Exec(ctx, ExecRequest{
			SQL:    wrappedSQL,
			Params: params,
			Settings: map[string]string{
				"max_execution_time":            fmt.Sprintf("%d", timeoutMs/1000+1),
				"rows_before_limit_at_least":    "1",
			},
			Format: "JSON",
		})
		if execErr != nil {
			if ctx.Err() != nil {
				respondError(w, s.version, http.StatusRequestTimeout, "query_timeout", "query exceeded timeout", r.URL.Path)
				return
			}
			respondError(w, s.version, http.StatusBadGateway, "query_failed", execErr.Error(), r.URL.Path)
			return
		}

		if req.ResultMode == "selection" {
			entityIDs := make([]string, 0, len(resp.Rows))
			for _, row := range resp.Rows {
				if id, ok := row["entity_id"].(string); ok {
					entityIDs = append(entityIDs, id)
				}
			}
			var totalCount int64
			if resp.RowsBeforeLimitAtLeast != nil {
				totalCount = int64(*resp.RowsBeforeLimitAtLeast)
			} else {
				totalCount = int64(len(entityIDs))
			}
			respond(w, s.version, envelope{
				"kind":        "selection",
				"entity_ids":  entityIDs,
				"snapshot_id": snapshotID,
				"total_count": totalCount,
			})
			return
		}

		// Tabular mode: map ClickHouse column types to spec types.
		columns := make([]map[string]any, 0, len(resp.Meta))
		for _, m := range resp.Meta {
			specType, nullable := oidaql.MapColumnType(m.Type)
			col := map[string]any{
				"name":     m.Name,
				"type":     specType,
				"nullable": nullable,
			}
			columns = append(columns, col)
		}

		var totalRows int64
		if resp.RowsBeforeLimitAtLeast != nil {
			totalRows = int64(*resp.RowsBeforeLimitAtLeast)
		} else {
			totalRows = int64(len(resp.Rows))
		}

		// Normalize rows: use string keys as returned by ClickHouse JSON.
		rows := make([]map[string]any, len(resp.Rows))
		for i, r := range resp.Rows {
			rows[i] = r
		}

		respond(w, s.version, envelope{
			"kind":       "tabular",
			"columns":    columns,
			"rows":       rows,
			"total_rows": totalRows,
		})
	}
}

// validateSnapshotID checks that snapshot_id exists in meta.data_snapshot.
func (s *apiServer) validateSnapshotID(ctx context.Context, snapshotID string) error {
	queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	sql := fmt.Sprintf(
		"SELECT snapshot_id FROM meta.data_snapshot FINAL WHERE snapshot_id = %s LIMIT 1 FORMAT JSONEachRow",
		sqlLiteral(snapshotID),
	)
	out, err := s.clickhouse.Query(queryCtx, sql)
	if err != nil {
		return fmt.Errorf("snapshot validation failed: %w", err)
	}
	rows, err := decodeJSONEachRow(out)
	if err != nil {
		return fmt.Errorf("snapshot validation decode failed: %w", err)
	}
	if len(rows) == 0 {
		return fmt.Errorf("snapshot_id %q not found", snapshotID)
	}
	return nil
}

// validateDialect checks that dialect is registered and enabled in meta.query_dialect.
func (s *apiServer) validateDialect(ctx context.Context, dialect string) error {
	queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	sql := fmt.Sprintf(
		"SELECT dialect FROM meta.query_dialect FINAL WHERE dialect = %s AND enabled = 1 LIMIT 1 FORMAT JSONEachRow",
		sqlLiteral(dialect),
	)
	out, err := s.clickhouse.Query(queryCtx, sql)
	if err != nil {
		return fmt.Errorf("dialect validation failed: %w", err)
	}
	rows, err := decodeJSONEachRow(out)
	if err != nil {
		return fmt.Errorf("dialect validation decode failed: %w", err)
	}
	if len(rows) == 0 {
		return fmt.Errorf("dialect %q is not registered or is disabled", dialect)
	}
	return nil
}

