package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// ── POST /v1/vector/search ────────────────────────────────────────────────────

type vectorSearchRequest struct {
	VectorSpace       string    `json:"vector_space"`
	Version           string    `json:"version"`
	SnapshotID        string    `json:"snapshot_id"`
	QueryVector       []float64 `json:"query_vector"`
	Metric            string    `json:"metric"`
	K                 int       `json:"k"`
	MinSimilarity     *float64  `json:"min_similarity"`
	EntityTypeFilter  string    `json:"entity_type_filter"`
	ExcludeEntityRefs []string  `json:"exclude_entity_refs"`
	TieBreaker        string    `json:"tie_breaker"`
	TimeoutMs         *int      `json:"timeout_ms"`
}

func (s *apiServer) vectorSearchHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, 1*1024*1024) // 1 MiB; query_vector can be large
		var req vectorSearchRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			respondError(w, s.version, http.StatusBadRequest, "invalid_request", "invalid JSON body: "+err.Error(), r.URL.Path)
			return
		}

		if req.VectorSpace == "" {
			respondError(w, s.version, http.StatusBadRequest, "invalid_request", "vector_space is required", r.URL.Path)
			return
		}
		if req.Version == "" {
			respondError(w, s.version, http.StatusBadRequest, "invalid_request", "version is required", r.URL.Path)
			return
		}
		if len(req.QueryVector) == 0 {
			respondError(w, s.version, http.StatusBadRequest, "invalid_request", "query_vector is required", r.URL.Path)
			return
		}
		if req.Metric != "cosine" && req.Metric != "euclidean" && req.Metric != "dot" {
			respondError(w, s.version, http.StatusBadRequest, "invalid_request", `metric must be "cosine", "euclidean", or "dot"`, r.URL.Path)
			return
		}
		if req.K < 1 || req.K > 1000 {
			respondError(w, s.version, http.StatusBadRequest, "invalid_request", "k must be between 1 and 1000", r.URL.Path)
			return
		}

		snapshotID := req.SnapshotID
		if snapshotID == "" {
			snapshotID = "live"
		}

		timeoutMs := int(s.queryTimeout / time.Millisecond)
		if req.TimeoutMs != nil && *req.TimeoutMs > 0 && *req.TimeoutMs <= 60000 {
			timeoutMs = *req.TimeoutMs
		}

		// Fetch vector space metadata to verify it exists and get dimensions.
		ctx, cancel := context.WithTimeout(r.Context(), s.queryTimeout)
		defer cancel()

		spaceSQL := fmt.Sprintf(
			"SELECT name, dimensions, metric FROM meta.vector_space FINAL "+
				"WHERE name = %s AND version = %s AND enabled = 1 LIMIT 1 FORMAT JSONEachRow",
			sqlLiteral(req.VectorSpace), sqlLiteral(req.Version),
		)
		spaceOut, err := s.clickhouse.Query(ctx, spaceSQL)
		if err != nil {
			respondError(w, s.version, http.StatusBadGateway, "query_failed", err.Error(), r.URL.Path)
			return
		}
		spaceRows, err := decodeJSONEachRow(spaceOut)
		if err != nil || len(spaceRows) == 0 {
			respondError(w, s.version, http.StatusBadRequest, "invalid_request",
				fmt.Sprintf("vector space %q version %q not found", req.VectorSpace, req.Version), r.URL.Path)
			return
		}
		var spaceDims int
		if d, ok := spaceRows[0]["dimensions"]; ok {
			switch v := d.(type) {
			case float64:
				spaceDims = int(v)
			case string:
				fmt.Sscanf(v, "%d", &spaceDims)
			}
		}
		if spaceDims > 0 && len(req.QueryVector) != spaceDims {
			respondError(w, s.version, http.StatusBadRequest, "invalid_request",
				fmt.Sprintf("query_vector has %d dimensions, space expects %d", len(req.QueryVector), spaceDims),
				r.URL.Path)
			return
		}

		// Build the metric-specific distance / normalization expressions.
		qvJSON := float64SliceToClickHouseArray(req.QueryVector)
		distanceExpr, scoreExpr, orderDir := metricExpressions(req.Metric, qvJSON)

		// Build WHERE clause.
		var where []string
		where = append(where,
			fmt.Sprintf("vector_space = %s", sqlLiteral(req.VectorSpace)),
			fmt.Sprintf("version = %s", sqlLiteral(req.Version)),
		)
		if req.EntityTypeFilter != "" {
			where = append(where, fmt.Sprintf("entity_type = %s", sqlLiteral(req.EntityTypeFilter)))
		}
		if len(req.ExcludeEntityRefs) > 0 {
			quoted := make([]string, len(req.ExcludeEntityRefs))
			for i, ref := range req.ExcludeEntityRefs {
				quoted[i] = sqlLiteral(ref)
			}
			where = append(where, fmt.Sprintf("entity_id NOT IN (%s)", strings.Join(quoted, ",")))
		}

		var havingClauses []string
		if req.MinSimilarity != nil {
			havingClauses = append(havingClauses, fmt.Sprintf("normalized_score >= %g", *req.MinSimilarity))
		}

		having := ""
		if len(havingClauses) > 0 {
			having = " HAVING " + strings.Join(havingClauses, " AND ")
		}

		tieBreaker := ""
		if req.TieBreaker == "entity_id" {
			tieBreaker = ", entity_id ASC"
		}

		execCtx, execCancel := context.WithTimeout(r.Context(), time.Duration(timeoutMs)*time.Millisecond)
		defer execCancel()

		searchSQL := fmt.Sprintf(
			"SELECT entity_id, %s AS raw_metric_value, %s AS normalized_score "+
				"FROM silver.entity_embedding FINAL "+
				"WHERE %s"+
				"%s"+
				"ORDER BY raw_metric_value %s%s "+
				"LIMIT %d FORMAT JSON",
			distanceExpr, scoreExpr,
			strings.Join(where, " AND "),
			having,
			orderDir, tieBreaker,
			req.K,
		)

		resp, execErr := s.exec.Exec(execCtx, ExecRequest{
			SQL:    searchSQL,
			Format: "JSON",
			Settings: map[string]string{
				"max_execution_time": fmt.Sprintf("%d", timeoutMs/1000+1),
			},
		})
		if execErr != nil {
			if execCtx.Err() != nil {
				respondError(w, s.version, http.StatusRequestTimeout, "query_timeout", "vector search timed out", r.URL.Path)
				return
			}
			respondError(w, s.version, http.StatusBadGateway, "query_failed", execErr.Error(), r.URL.Path)
			return
		}

		hits := make([]map[string]any, 0, len(resp.Rows))
		for _, row := range resp.Rows {
			hits = append(hits, map[string]any{
				"entity_id":        row["entity_id"],
				"raw_metric_value": row["raw_metric_value"],
				"normalized_score": row["normalized_score"],
			})
		}

		respond(w, s.version, envelope{
			"kind":        "vector_search_result",
			"hits":        hits,
			"snapshot_id": snapshotID,
		})
	}
}

// ── POST /v1/embeddings/resolve ───────────────────────────────────────────────

type embeddingsResolveRequest struct {
	VectorSpace string   `json:"vector_space"`
	Version     string   `json:"version"`
	SnapshotID  string   `json:"snapshot_id"`
	SeedRefs    []string `json:"seed_refs"`
	Aggregation string   `json:"aggregation"`
}

func (s *apiServer) embeddingsResolveHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, 64*1024)
		var req embeddingsResolveRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			respondError(w, s.version, http.StatusBadRequest, "invalid_request", "invalid JSON body: "+err.Error(), r.URL.Path)
			return
		}

		if req.VectorSpace == "" {
			respondError(w, s.version, http.StatusBadRequest, "invalid_request", "vector_space is required", r.URL.Path)
			return
		}
		if req.Version == "" {
			respondError(w, s.version, http.StatusBadRequest, "invalid_request", "version is required", r.URL.Path)
			return
		}
		if len(req.SeedRefs) == 0 {
			respondError(w, s.version, http.StatusBadRequest, "invalid_request", "seed_refs is required", r.URL.Path)
			return
		}
		if req.Aggregation != "single" && req.Aggregation != "centroid" && req.Aggregation != "each" {
			respondError(w, s.version, http.StatusBadRequest, "invalid_request",
				`aggregation must be "single", "centroid", or "each"`, r.URL.Path)
			return
		}

		quoted := make([]string, len(req.SeedRefs))
		for i, ref := range req.SeedRefs {
			quoted[i] = sqlLiteral(ref)
		}

		ctx, cancel := context.WithTimeout(r.Context(), s.queryTimeout)
		defer cancel()

		sql := fmt.Sprintf(
			"SELECT entity_id, embedding FROM silver.entity_embedding FINAL "+
				"WHERE vector_space = %s AND version = %s AND entity_id IN (%s) FORMAT JSONEachRow",
			sqlLiteral(req.VectorSpace), sqlLiteral(req.Version),
			strings.Join(quoted, ","),
		)

		out, err := s.clickhouse.Query(ctx, sql)
		if err != nil {
			respondError(w, s.version, http.StatusBadGateway, "query_failed", err.Error(), r.URL.Path)
			return
		}
		rows, err := decodeJSONEachRow(out)
		if err != nil {
			respondError(w, s.version, http.StatusInternalServerError, "decode_failed", err.Error(), r.URL.Path)
			return
		}

		// Build entity_id → embedding map.
		found := make(map[string][]float64, len(rows))
		for _, row := range rows {
			id, _ := row["entity_id"].(string)
			if emb, ok := parseEmbedding(row["embedding"]); ok {
				found[id] = emb
			}
		}

		// Determine missing.
		var missingIDs []string
		for _, ref := range req.SeedRefs {
			if _, ok := found[ref]; !ok {
				missingIDs = append(missingIDs, ref)
			}
		}
		if missingIDs == nil {
			missingIDs = []string{}
		}

		// Apply aggregation.
		var vectors [][]float64
		switch req.Aggregation {
		case "each":
			for _, ref := range req.SeedRefs {
				if emb, ok := found[ref]; ok {
					vectors = append(vectors, emb)
				}
			}
		case "single":
			if len(rows) > 1 {
				respondError(w, s.version, http.StatusBadRequest, "invalid_request",
					"aggregation=single with more than one result is ambiguous", r.URL.Path)
				return
			}
			for _, emb := range found {
				vectors = append(vectors, emb)
				break
			}
		case "centroid":
			vectors = [][]float64{centroid(found)}
		}
		if vectors == nil {
			vectors = [][]float64{}
		}

		respond(w, s.version, envelope{
			"kind":               "embedding_result",
			"vectors":            vectors,
			"missing_entity_ids": missingIDs,
		})
	}
}

// ── GET /v1/vector-spaces/{name} ─────────────────────────────────────────────

func (s *apiServer) vectorSpaceDescribeHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimSpace(r.PathValue("name"))
		if name == "" {
			respondError(w, s.version, http.StatusBadRequest, "invalid_request", "missing vector space name", r.URL.Path)
			return
		}
		if err := rejectUnsupportedQueryParams(r, []string{"version"}); err != nil {
			respondError(w, s.version, http.StatusBadRequest, "invalid_request", err.Error(), r.URL.Path)
			return
		}
		version := strings.TrimSpace(r.URL.Query().Get("version"))
		if version == "" {
			respondError(w, s.version, http.StatusBadRequest, "invalid_request", "version query param is required", r.URL.Path)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), s.queryTimeout)
		defer cancel()

		// Main space row.
		spaceSQL := fmt.Sprintf(
			"SELECT name, version, dimensions, entity_types, metric, model_ref "+
				"FROM meta.vector_space FINAL "+
				"WHERE name = %s AND version = %s AND enabled = 1 LIMIT 1 FORMAT JSONEachRow",
			sqlLiteral(name), sqlLiteral(version),
		)
		spaceOut, err := s.clickhouse.Query(ctx, spaceSQL)
		if err != nil {
			respondError(w, s.version, http.StatusBadGateway, "query_failed", err.Error(), r.URL.Path)
			return
		}
		spaceRows, err := decodeJSONEachRow(spaceOut)
		if err != nil || len(spaceRows) == 0 {
			respondError(w, s.version, http.StatusNotFound, "not_found",
				fmt.Sprintf("vector space %q version %q not found", name, version), r.URL.Path)
			return
		}

		// Entity count from the embedding store.
		countSQL := fmt.Sprintf(
			"SELECT count() AS entity_count FROM silver.entity_embedding FINAL "+
				"WHERE vector_space = %s AND version = %s FORMAT JSONEachRow",
			sqlLiteral(name), sqlLiteral(version),
		)
		countOut, _ := s.clickhouse.Query(ctx, countSQL)
		var entityCount int64
		if countRows, err := decodeJSONEachRow(countOut); err == nil && len(countRows) > 0 {
			switch v := countRows[0]["entity_count"].(type) {
			case float64:
				entityCount = int64(v)
			case string:
				fmt.Sscanf(v, "%d", &entityCount)
			}
		}

		row := spaceRows[0]
		item := map[string]any{
			"kind":         "vector_space",
			"name":         row["name"],
			"version":      row["version"],
			"dimensions":   row["dimensions"],
			"entity_types": row["entity_types"],
			"metric":       row["metric"],
			"entity_count": entityCount,
		}
		if modelRef, ok := row["model_ref"].(string); ok && modelRef != "" {
			item["model_ref"] = modelRef
		}

		respond(w, s.version, item)
	}
}

// ── helpers ───────────────────────────────────────────────────────────────────

// metricExpressions returns (distanceExpr, scoreExpr, orderDir) for a given metric.
func metricExpressions(metric, qvJSON string) (distExpr, scoreExpr, orderDir string) {
	switch metric {
	case "euclidean":
		distExpr = fmt.Sprintf("L2Distance(embedding, %s)", qvJSON)
		scoreExpr = fmt.Sprintf("1.0 / (1.0 + L2Distance(embedding, %s))", qvJSON)
		orderDir = "ASC"
	case "dot":
		distExpr = fmt.Sprintf("arrayDotProduct(embedding, %s)", qvJSON)
		scoreExpr = fmt.Sprintf("greatest(0.0, least(1.0, arrayDotProduct(embedding, %s)))", qvJSON)
		orderDir = "DESC"
	default: // cosine
		distExpr = fmt.Sprintf("cosineDistance(embedding, %s)", qvJSON)
		scoreExpr = fmt.Sprintf("greatest(0.0, 1.0 - cosineDistance(embedding, %s))", qvJSON)
		orderDir = "ASC"
	}
	return
}

// float64SliceToClickHouseArray formats a []float64 as a ClickHouse array literal
// for use directly in SQL (not a bound parameter, since embeddings can be very large).
func float64SliceToClickHouseArray(v []float64) string {
	if len(v) == 0 {
		return "[]"
	}
	var b strings.Builder
	b.WriteString("[")
	for i, f := range v {
		if i > 0 {
			b.WriteString(",")
		}
		fmt.Fprintf(&b, "%g", f)
	}
	b.WriteString("]")
	return b.String()
}

// parseEmbedding decodes an embedding from a ClickHouse JSONEachRow value.
// ClickHouse returns Array(Float32) as a JSON array of numbers.
func parseEmbedding(raw any) ([]float64, bool) {
	arr, ok := raw.([]any)
	if !ok {
		return nil, false
	}
	out := make([]float64, len(arr))
	for i, v := range arr {
		switch n := v.(type) {
		case float64:
			out[i] = n
		default:
			return nil, false
		}
	}
	return out, true
}

// centroid computes the element-wise mean of all embeddings.
func centroid(found map[string][]float64) []float64 {
	if len(found) == 0 {
		return []float64{}
	}
	var dims int
	for _, emb := range found {
		dims = len(emb)
		break
	}
	result := make([]float64, dims)
	for _, emb := range found {
		for i, v := range emb {
			result[i] += v
		}
	}
	n := float64(len(found))
	for i := range result {
		result[i] /= n
	}
	return result
}
