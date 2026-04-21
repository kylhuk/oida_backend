package dashboardstats

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type compiledCatalogManifest struct {
	BronzeDDLManifest []struct {
		BronzeTable string `json:"bronze_table"`
	} `json:"bronze_ddl_manifest"`
}

func sourceBronzeTables() []string {
	paths := []string{
		filepath.Join("seed", "source_catalog_compiled.json"),
		filepath.Join("..", "..", "seed", "source_catalog_compiled.json"),
	}
	for _, path := range paths {
		rows, err := loadBronzeTablesFromCompiled(path)
		if err == nil && len(rows) > 0 {
			return rows
		}
	}
	return nil
}

func loadBronzeTablesFromCompiled(path string) ([]string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var compiled compiledCatalogManifest
	if err := json.Unmarshal(b, &compiled); err != nil {
		return nil, err
	}
	seen := map[string]struct{}{}
	tables := make([]string, 0, len(compiled.BronzeDDLManifest))
	for _, row := range compiled.BronzeDDLManifest {
		table := strings.TrimSpace(strings.TrimPrefix(row.BronzeTable, "bronze."))
		if table == "" {
			continue
		}
		if _, ok := seen[table]; ok {
			continue
		}
		seen[table] = struct{}{}
		tables = append(tables, table)
	}
	sort.Strings(tables)
	return tables, nil
}
