package main

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"unicode/utf8"

	"global-osint-backend/internal/objectstore"
)

const (
	maxArtifactRefLen = 1024
	maxArtifactBytes  = 32 * 1024 * 1024 // 32 MiB inline limit
)

func (s *apiServer) artifactReadHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ref := strings.TrimSpace(r.PathValue("ref"))
		if ref == "" || utf8.RuneCountInString(ref) > maxArtifactRefLen {
			respondError(w, s.version, http.StatusBadRequest, "invalid_request", "artifact ref missing or too long", r.URL.Path)
			return
		}
		if !isValidArtifactRef(ref) {
			respondError(w, s.version, http.StatusBadRequest, "invalid_request", "artifact ref contains invalid characters", r.URL.Path)
			return
		}
		if err := rejectUnsupportedQueryParams(r, []string{"snapshot_id"}); err != nil {
			respondError(w, s.version, http.StatusBadRequest, "invalid_request", err.Error(), r.URL.Path)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), s.queryTimeout)
		defer cancel()

		sql := fmt.Sprintf(
			"SELECT artifact_ref, bucket, object_key, content_type, content_length, artifact_marking, created_at "+
				"FROM gold.api_v1_artifacts "+
				"WHERE artifact_ref = %s LIMIT 1 FORMAT JSONEachRow",
			sqlLiteral(ref),
		)
		output, err := s.clickhouse.Query(ctx, sql)
		if err != nil {
			respondError(w, s.version, http.StatusBadGateway, "query_failed", err.Error(), r.URL.Path)
			return
		}
		rows, err := decodeJSONEachRow(output)
		if err != nil {
			respondError(w, s.version, http.StatusInternalServerError, "decode_failed", err.Error(), r.URL.Path)
			return
		}
		if len(rows) == 0 {
			respondError(w, s.version, http.StatusNotFound, "not_found", fmt.Sprintf("artifact %q not found", ref), r.URL.Path)
			return
		}

		row := rows[0]
		bucket, _ := row["bucket"].(string)
		objectKey, _ := row["object_key"].(string)

		if s.objectStore == nil {
			respondError(w, s.version, http.StatusServiceUnavailable, "object_store_unavailable", "object store not configured", r.URL.Path)
			return
		}

		body, _, err := s.objectStore.GetObject(ctx, bucket, objectKey)
		if err != nil {
			if errors.Is(err, objectstore.ErrNotFound) {
				respondError(w, s.version, http.StatusNotFound, "not_found", fmt.Sprintf("artifact object %q not found in store", ref), r.URL.Path)
				return
			}
			respondError(w, s.version, http.StatusBadGateway, "object_store_error", err.Error(), r.URL.Path)
			return
		}
		if len(body) > maxArtifactBytes {
			respondError(w, s.version, http.StatusRequestEntityTooLarge, "payload_too_large",
				fmt.Sprintf("artifact exceeds %d MiB inline limit", maxArtifactBytes/1024/1024), r.URL.Path)
			return
		}

		item := map[string]any{
			"artifact_ref":     row["artifact_ref"],
			"content_length":   row["content_length"],
			"artifact_marking": row["artifact_marking"],
			"bytes":            base64.StdEncoding.EncodeToString(body),
		}
		if ct, ok := row["content_type"].(string); ok && ct != "" {
			item["content_type"] = ct
		}

		respond(w, s.version, envelope{"kind": "artifact", "item": item, "path": r.URL.Path})
	}
}

// isValidArtifactRef permits art: prefixed refs plus path-safe chars.
// Rejects whitespace, control chars, and null bytes.
func isValidArtifactRef(ref string) bool {
	for _, r := range ref {
		if r < 0x20 || r == 0x7f {
			return false
		}
	}
	return true
}
