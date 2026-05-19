package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
)

type savedQuerySeed struct {
	Name        string          `json:"name"`
	Version     string          `json:"version"`
	Criteria    json.RawMessage `json:"criteria"`
	ResultLimit *uint32         `json:"result_limit"`
	Ordering    json.RawMessage `json:"ordering"`
	Description string          `json:"description"`
	Enabled     bool            `json:"enabled"`
}

func loadSavedQuerySeed(ctx context.Context, store sourceRegistryStore, path string) error {
	path = strings.TrimSpace(path)
	if path == "" {
		log.Println("bootstrap: saved query seed path empty, skipping")
		return nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		log.Printf("bootstrap: saved query seed not found at %s, skipping: %v", path, err)
		return nil
	}
	var seeds []savedQuerySeed
	if err := json.Unmarshal(data, &seeds); err != nil {
		return fmt.Errorf("decode saved query seed: %w", err)
	}
	if len(seeds) == 0 {
		log.Println("bootstrap: saved query seed is empty, skipping")
		return nil
	}
	values := make([]string, 0, len(seeds))
	for _, s := range seeds {
		criteriaJSON, err := json.Marshal(s.Criteria)
		if err != nil {
			return fmt.Errorf("saved query %s: marshal criteria: %w", s.Name, err)
		}
		orderingJSON := "[]"
		if len(s.Ordering) > 0 {
			b, err := json.Marshal(s.Ordering)
			if err != nil {
				return fmt.Errorf("saved query %s: marshal ordering: %w", s.Name, err)
			}
			orderingJSON = string(b)
		}
		resultLimit := "NULL"
		if s.ResultLimit != nil {
			resultLimit = fmt.Sprintf("%d", *s.ResultLimit)
		}
		enabled := 0
		if s.Enabled {
			enabled = 1
		}
		values = append(values, fmt.Sprintf("('%s','%s','%s',%s,'%s',now64(3),'','%s',%d,1,1,1,now64(3),'{}','[]')",
			esc(s.Name),
			esc(s.Version),
			esc(string(criteriaJSON)),
			resultLimit,
			esc(orderingJSON),
			esc(s.Description),
			enabled,
		))
	}
	sql := `INSERT INTO meta.saved_query
(name, version, criteria, result_limit, ordering, created_at, created_by, description, enabled, schema_version, record_version, api_contract_version, updated_at, attrs, evidence)
VALUES ` + strings.Join(values, ",")
	if err := store.ApplySQL(ctx, sql); err != nil {
		return fmt.Errorf("seed saved queries: %w", err)
	}
	log.Printf("bootstrap: seeded %d saved query record(s)", len(seeds))
	return nil
}
