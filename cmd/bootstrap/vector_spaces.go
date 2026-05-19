package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
)

type vectorSpaceSeed struct {
	Name         string   `json:"name"`
	Version      string   `json:"version"`
	Dimensions   uint32   `json:"dimensions"`
	EntityTypes  []string `json:"entity_types"`
	Metric       string   `json:"metric"`
	StorageTable string   `json:"storage_table"`
	ModelRef     string   `json:"model_ref"`
}

func loadVectorSpacesSeed(ctx context.Context, store sourceRegistryStore, path string) error {
	path = strings.TrimSpace(path)
	if path == "" {
		log.Println("bootstrap: vector spaces seed path empty, skipping")
		return nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		log.Printf("bootstrap: vector spaces seed not found at %s, skipping: %v", path, err)
		return nil
	}
	var seeds []vectorSpaceSeed
	if err := json.Unmarshal(data, &seeds); err != nil {
		return fmt.Errorf("decode vector spaces seed: %w", err)
	}
	if len(seeds) == 0 {
		log.Println("bootstrap: vector spaces seed is empty, skipping")
		return nil
	}
	values := make([]string, 0, len(seeds))
	for _, s := range seeds {
		entityTypesJSON, _ := json.Marshal(s.EntityTypes)
		if s.Metric == "" {
			s.Metric = "cosine"
		}
		if s.StorageTable == "" {
			s.StorageTable = "silver.entity_embedding"
		}
		values = append(values, fmt.Sprintf("('%s','%s',%d,%s,'%s','%s','%s',1,0,1,1,now64(3),'{}','[]')",
			esc(s.Name),
			esc(s.Version),
			s.Dimensions,
			esc(string(entityTypesJSON)),
			esc(s.Metric),
			esc(s.StorageTable),
			esc(s.ModelRef),
		))
	}
	sql := `INSERT INTO meta.vector_space
(name, version, dimensions, entity_types, metric, storage_table, model_ref, enabled, schema_version, record_version, api_contract_version, updated_at, attrs, evidence)
VALUES ` + strings.Join(values, ",")
	if err := store.ApplySQL(ctx, sql); err != nil {
		return fmt.Errorf("seed vector spaces: %w", err)
	}
	log.Printf("bootstrap: seeded %d vector space record(s)", len(seeds))
	return nil
}
