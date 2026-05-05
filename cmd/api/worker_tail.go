package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

const (
	defaultWorkerTailLimit = 50
	maxWorkerTailLimit     = 200
)

type workerTailCursor struct {
	OccurredAt string
	ActivityID string
}

func (s *apiServer) workerTailHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := rejectUnsupportedQueryParams(r, []string{"limit", "cursor", "source_id", "correlation_id"}); err != nil {
			respondError(w, s.version, http.StatusBadRequest, "invalid_request", err.Error(), r.URL.Path)
			return
		}
		limit, err := parseWorkerTailLimit(r.URL.Query().Get("limit"))
		if err != nil {
			respondError(w, s.version, http.StatusBadRequest, "invalid_request", err.Error(), r.URL.Path)
			return
		}
		cursor, err := decodeWorkerTailCursor(r.URL.Query().Get("cursor"))
		if err != nil {
			respondError(w, s.version, http.StatusBadRequest, "invalid_request", err.Error(), r.URL.Path)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), s.queryTimeout)
		defer cancel()
		output, err := s.clickhouse.Query(ctx, buildWorkerTailQuery(strings.TrimSpace(r.URL.Query().Get("source_id")), strings.TrimSpace(r.URL.Query().Get("correlation_id")), cursor, limit+1))
		if err != nil {
			respondError(w, s.version, http.StatusBadGateway, "query_failed", err.Error(), r.URL.Path)
			return
		}
		rows, err := decodeJSONEachRow(output)
		if err != nil {
			respondError(w, s.version, http.StatusInternalServerError, "decode_failed", err.Error(), r.URL.Path)
			return
		}
		hasNext := len(rows) > limit
		if hasNext {
			rows = rows[:limit]
		}
		data := envelope{"kind": "worker_tail", "items": rows, "limit": limit, "path": r.URL.Path}
		if cursor != nil {
			data["cursor"] = encodeWorkerTailCursor(*cursor)
		}
		if sourceID := strings.TrimSpace(r.URL.Query().Get("source_id")); sourceID != "" {
			data["source_id"] = sourceID
		}
		if correlationID := strings.TrimSpace(r.URL.Query().Get("correlation_id")); correlationID != "" {
			data["correlation_id"] = correlationID
		}
		if hasNext && len(rows) > 0 {
			last := rows[len(rows)-1]
			data["next_cursor"] = encodeWorkerTailCursor(workerTailCursor{OccurredAt: asString(last["occurred_at"]), ActivityID: asString(last["activity_id"])})
		}
		respond(w, s.version, data)
	}
}

func parseWorkerTailLimit(raw string) (int, error) {
	if strings.TrimSpace(raw) == "" {
		return defaultWorkerTailLimit, nil
	}
	value, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || value <= 0 || value > maxWorkerTailLimit {
		return 0, fmt.Errorf("limit must be between 1 and %d", maxWorkerTailLimit)
	}
	return value, nil
}

func encodeWorkerTailCursor(cursor workerTailCursor) string {
	raw := strings.TrimSpace(cursor.OccurredAt) + "|" + strings.TrimSpace(cursor.ActivityID)
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

func decodeWorkerTailCursor(raw string) (*workerTailCursor, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	decoded, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(raw))
	if err != nil {
		return nil, fmt.Errorf("invalid cursor")
	}
	parts := strings.SplitN(string(decoded), "|", 2)
	if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
		return nil, fmt.Errorf("invalid cursor")
	}
	return &workerTailCursor{OccurredAt: strings.TrimSpace(parts[0]), ActivityID: strings.TrimSpace(parts[1])}, nil
}

func buildWorkerTailQuery(sourceID, correlationID string, cursor *workerTailCursor, limit int) string {
	filters := make([]string, 0, 3)
	if sourceID != "" {
		filters = append(filters, fmt.Sprintf("source_id = %s", workerTailSQLString(sourceID)))
	}
	if correlationID != "" {
		filters = append(filters, fmt.Sprintf("correlation_id = %s", workerTailSQLString(correlationID)))
	}
	if cursor != nil {
		filters = append(filters, fmt.Sprintf("(occurred_at < parseDateTime64BestEffort(%s) OR (occurred_at = parseDateTime64BestEffort(%s) AND activity_id < %s))", workerTailSQLString(cursor.OccurredAt), workerTailSQLString(cursor.OccurredAt), workerTailSQLString(cursor.ActivityID)))
	}
	whereClause := ""
	if len(filters) > 0 {
		whereClause = "WHERE " + strings.Join(filters, " AND ")
	}
	return fmt.Sprintf(`SELECT activity_id, component, activity_kind, correlation_id, source_id, status, message, formatDateTime(occurred_at, '%%Y-%%m-%%dT%%H:%%i:%%S.%%3fZ', 'UTC') AS occurred_at, detail
FROM (
	SELECT job_id AS activity_id,
		'control-plane' AS component,
		'job_run' AS activity_kind,
		ifNull(correlation_id, '') AS correlation_id,
		JSONExtractString(stats, 'source_id') AS source_id,
		status,
		message,
		coalesce(finished_at, started_at) AS occurred_at,
		stats AS detail
	FROM ops.job_run
	UNION ALL
	SELECT fetch_id AS activity_id,
		'worker-fetch' AS component,
		'fetch' AS activity_kind,
		ifNull(correlation_id, '') AS correlation_id,
		source_id,
		if(success = 1, 'success', 'failed') AS status,
		ifNull(error_message, concat('status_code=', toString(status_code))) AS message,
		fetched_at AS occurred_at,
		concat('{"status_code":', toString(status_code), ',"body_bytes":', toString(body_bytes), ',"attempt_count":', toString(attempt_count), ',"retry_count":', toString(retry_count), '}') AS detail
	FROM ops.fetch_log
	UNION ALL
	SELECT parse_id AS activity_id,
		'worker-parse' AS component,
		'parse' AS activity_kind,
		ifNull(correlation_id, '') AS correlation_id,
		source_id,
		status,
		ifNull(error_message, concat('extracted_rows=', toString(extracted_rows))) AS message,
		coalesce(finished_at, started_at) AS occurred_at,
		concat('{"parser_id":', toJSONString(parser_id), ',"raw_id":', toJSONString(raw_id), ',"extracted_rows":', toString(extracted_rows), ',"error_class":', toJSONString(error_class), '}') AS detail
	FROM ops.parse_log
)
%s
ORDER BY occurred_at DESC, activity_id DESC
LIMIT %d
FORMAT JSONEachRow`, whereClause, limit)
}

func workerTailSQLString(value string) string {
	return "'" + strings.ReplaceAll(strings.TrimSpace(value), "'", "''") + "'"
}
