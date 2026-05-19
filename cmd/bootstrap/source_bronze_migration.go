package main

import (
	"fmt"
	"strings"
)

const (
	defaultBronzeMigrationPath    = "/app/migrations/clickhouse/0025_source_bronze_tables_expanded.sql"
	baseSourceBronzeTemplateTable = "bronze.src_seed_gdelt_v1"
)

var appendOnlySourceBronzeTables = map[string]struct{}{
	"bronze.src_catalog-auto-maritime-ocean-and-coastal-_f8f33fd7_v1": {},
}

func renderSourceBronzeMigration(compiled compiledSourceCatalog) (string, error) {
	if len(compiled.BronzeDDLManifest) == 0 {
		return "", fmt.Errorf("compiled source catalog has no bronze manifest rows")
	}
	var b strings.Builder
	for _, row := range compiled.BronzeDDLManifest {
		table := strings.TrimSpace(row.BronzeTable)
		if table == "" || table == baseSourceBronzeTemplateTable {
			continue
		}
		if _, ok := appendOnlySourceBronzeTables[table]; ok {
			continue
		}
		dbTable := strings.SplitN(table, ".", 2)
		if len(dbTable) != 2 || strings.TrimSpace(dbTable[0]) == "" || strings.TrimSpace(dbTable[1]) == "" {
			return "", fmt.Errorf("invalid bronze table %q", row.BronzeTable)
		}
		b.WriteString("CREATE TABLE IF NOT EXISTS `")
		b.WriteString(dbTable[0])
		b.WriteString("`.`")
		b.WriteString(dbTable[1])
		b.WriteString("` AS `bronze`.`src_seed_gdelt_v1`;\n\n")
	}
	out := b.String()
	out = strings.TrimSuffix(out, "\n")
	return out, nil
}
