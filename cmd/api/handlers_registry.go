package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

func (s *apiServer) registryLookupHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimSpace(r.PathValue("name"))
		if name == "" {
			respondError(w, s.version, http.StatusBadRequest, "invalid_request", "missing registry name", r.URL.Path)
			return
		}
		if err := rejectUnsupportedQueryParams(r, []string{"version"}); err != nil {
			respondError(w, s.version, http.StatusBadRequest, "invalid_request", err.Error(), r.URL.Path)
			return
		}

		version := strings.TrimSpace(r.URL.Query().Get("version"))
		ctx, cancel := context.WithTimeout(r.Context(), s.queryTimeout)
		defer cancel()

		var sql string
		if version == "" {
			sql = fmt.Sprintf(
				"SELECT name, version, criteria, result_limit, ordering, created_at "+
					"FROM gold.api_v1_saved_queries "+
					"WHERE name = %s "+
					"ORDER BY version DESC LIMIT 1 FORMAT JSONEachRow",
				sqlLiteral(name),
			)
		} else {
			sql = fmt.Sprintf(
				"SELECT name, version, criteria, result_limit, ordering, created_at "+
					"FROM gold.api_v1_saved_queries "+
					"WHERE name = %s AND version = %s "+
					"ORDER BY version DESC LIMIT 1 FORMAT JSONEachRow",
				sqlLiteral(name),
				sqlLiteral(version),
			)
		}

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
			respondError(w, s.version, http.StatusNotFound, "not_found", fmt.Sprintf("registry entry %q not found", name), r.URL.Path)
			return
		}

		row := rows[0]
		item := map[string]any{
			"name":       row["name"],
			"version":    row["version"],
			"created_at": row["created_at"],
		}
		// criteria is stored as a JSON string; decode it to an object for the response.
		if criteriaStr, ok := row["criteria"].(string); ok && criteriaStr != "" {
			var criteriaObj any
			if err := json.Unmarshal([]byte(criteriaStr), &criteriaObj); err == nil {
				item["criteria"] = criteriaObj
			} else {
				item["criteria"] = criteriaStr
			}
		}
		// result_limit may be null.
		if rl := row["result_limit"]; rl != nil {
			item["result_limit"] = rl
		}
		// ordering is stored as a JSON string; decode it to an array.
		if orderingStr, ok := row["ordering"].(string); ok && orderingStr != "" && orderingStr != "[]" {
			var orderingObj any
			if err := json.Unmarshal([]byte(orderingStr), &orderingObj); err == nil {
				item["ordering"] = orderingObj
			}
		}

		respond(w, s.version, envelope{"kind": "saved_query", "item": item, "path": r.URL.Path})
	}
}
