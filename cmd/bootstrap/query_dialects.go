package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
)

type queryDialectSeed struct {
	Dialect               string `json:"dialect"`
	EntityProjectionRule  string `json:"entity_projection_rule"`
	ShapePolicy           string `json:"shape_policy"`
	CaseSensitivity       string `json:"case_sensitivity"`
	MaxTimeoutMS          uint32 `json:"max_timeout_ms"`
	CommentPrefix         string `json:"comment_prefix"`
	Enabled               bool   `json:"enabled"`
}

func loadQueryDialectSeed(ctx context.Context, store sourceRegistryStore, path string) error {
	path = strings.TrimSpace(path)
	if path == "" {
		log.Println("bootstrap: query dialect seed path empty, skipping")
		return nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		log.Printf("bootstrap: query dialect seed not found at %s, skipping: %v", path, err)
		return nil
	}
	var seeds []queryDialectSeed
	if err := json.Unmarshal(data, &seeds); err != nil {
		return fmt.Errorf("decode query dialect seed: %w", err)
	}
	if len(seeds) == 0 {
		log.Println("bootstrap: query dialect seed is empty, skipping")
		return nil
	}
	values := make([]string, 0, len(seeds))
	for _, s := range seeds {
		enabled := 0
		if s.Enabled {
			enabled = 1
		}
		values = append(values, fmt.Sprintf("('%s','%s','%s','%s',%d,'%s',%d,1,1,1,now64(3),'{}','[]')",
			esc(s.Dialect),
			esc(s.EntityProjectionRule),
			esc(s.ShapePolicy),
			esc(s.CaseSensitivity),
			s.MaxTimeoutMS,
			esc(s.CommentPrefix),
			enabled,
		))
	}
	sql := `INSERT INTO meta.query_dialect
(dialect, entity_projection_rule, shape_policy, case_sensitivity, max_timeout_ms, comment_prefix, enabled, schema_version, record_version, api_contract_version, updated_at, attrs, evidence)
VALUES ` + strings.Join(values, ",")
	if err := store.ApplySQL(ctx, sql); err != nil {
		return fmt.Errorf("seed query dialects: %w", err)
	}
	log.Printf("bootstrap: seeded %d query dialect(s)", len(seeds))
	return nil
}
