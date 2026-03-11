package main

import (
	"context"
	"net/http"
	"time"

	"global-osint-backend/internal/dashboardstats"
)

func (s *apiServer) internalStatsHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := rejectUnsupportedQueryParams(r, nil); err != nil {
			respondError(w, s.version, http.StatusBadRequest, "invalid_request", err.Error(), r.URL.Path)
			return
		}

		timeout := s.queryTimeout
		if timeout <= 0 {
			timeout = defaultAPIQueryTimeout
		}
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()

		report, err := dashboardstats.Collect(ctx, s.clickhouse, time.Now().UTC())
		if err != nil {
			respondError(w, s.version, http.StatusBadGateway, "query_failed", err.Error(), r.URL.Path)
			return
		}

		data := envelope{
			"kind":         "internal_stats",
			"summary":      report.Summary,
			"storage":      report.Storage,
			"quality":      report.Quality,
			"outputs":      report.Outputs,
			"generated_at": report.GeneratedAt,
			"warnings":     report.Warnings,
		}
		respond(w, s.version, data)
	}
}
