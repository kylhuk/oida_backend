package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"global-osint-backend/internal/migrate"
	"global-osint-backend/internal/seed/unlocode"
)

const (
	unlocodeBatchSize      = 500
	unlocodePlaceType      = "port"
	unlocodeSourceSystem   = "unlocode"
	unlocodeSchemaVersion  = 1
	unlocodeContractVersion = 1
)

// loadUNLocodeSeed seeds silver.dim_place with UN/LOCODE port entries.
// It is idempotent: dim_place uses ReplacingMergeTree(record_version) so
// re-running just replaces rows with the same place_id.
func loadUNLocodeSeed(ctx context.Context, runner *migrate.HTTPRunner, csvPath string) error {
	if csvPath == "" {
		log.Println("UN/LOCODE seed path not configured, skipping")
		return nil
	}
	f, err := os.Open(csvPath)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("UN/LOCODE seed file not found at %s, skipping", csvPath)
			return nil
		}
		return fmt.Errorf("open unlocode csv: %w", err)
	}
	defer f.Close()

	ports, err := unlocode.Load(f)
	if err != nil {
		return fmt.Errorf("parse unlocode csv: %w", err)
	}
	if len(ports) == 0 {
		log.Println("UN/LOCODE seed file is empty, skipping")
		return nil
	}

	updatedAt := time.Now().UTC()
	recordVersion := uint64(updatedAt.Unix())

	total := 0
	for i := 0; i < len(ports); i += unlocodeBatchSize {
		end := i + unlocodeBatchSize
		if end > len(ports) {
			end = len(ports)
		}
		batch := ports[i:end]
		if err := insertUNLocodeBatch(ctx, runner, batch, updatedAt, recordVersion); err != nil {
			return fmt.Errorf("insert unlocode batch %d-%d: %w", i, end, err)
		}
		total += len(batch)
	}

	log.Printf("loaded %d UN/LOCODE port entries into silver.dim_place", total)
	return nil
}

func insertUNLocodeBatch(ctx context.Context, runner *migrate.HTTPRunner, ports []unlocode.Port, updatedAt time.Time, recordVersion uint64) error {
	validFrom := updatedAt.Format("2006-01-02 15:04:05.000")
	updatedAtStr := updatedAt.Format("2006-01-02 15:04:05.000")

	var sb strings.Builder
	sb.WriteString(`INSERT INTO silver.dim_place `)
	sb.WriteString(`(place_id, parent_place_id, canonical_name, place_type, admin_level, country_code, continent_code, `)
	sb.WriteString(`source_place_key, source_system, status, centroid_lat, centroid_lon, `)
	sb.WriteString(`bbox_min_lat, bbox_min_lon, bbox_max_lat, bbox_max_lon, `)
	sb.WriteString(`valid_from, schema_version, record_version, api_contract_version, updated_at, attrs, evidence) `)
	sb.WriteString("VALUES\n")

	for j, p := range ports {
		placeID := "plc:port:" + strings.ToLower(p.Code)
		sourcePlaceKey := "unlocode:" + p.Code
		countryCode := strings.ToLower(p.CountryCode)

		sb.WriteString(fmt.Sprintf(
			"(%s,%s,%s,%s,%d,%s,%s,%s,%s,%s,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%s,%d,%d,%d,%s,%s,%s)",
			chStr(placeID),
			"NULL",
			chStr(p.Name),
			chStr(unlocodePlaceType),
			0,
			chStr(countryCode),
			chStr(""),
			chStr(sourcePlaceKey),
			chStr(unlocodeSourceSystem),
			chStr("active"),
			p.Lat, p.Lon,
			p.Lat, p.Lon, p.Lat, p.Lon, // bbox = centroid for point locations
			chTS(validFrom),
			unlocodeSchemaVersion,
			recordVersion,
			unlocodeContractVersion,
			chTS(updatedAtStr),
			chStr("{}"),
			chStr("[]"),
		))
		if j < len(ports)-1 {
			sb.WriteString(",\n")
		}
	}

	_, err := runner.QueryBody(ctx, sb.String())
	return err
}

// chStr escapes a string value for ClickHouse VALUES.
func chStr(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `'`, `\'`)
	return "'" + s + "'"
}

// chTS wraps a timestamp string for ClickHouse.
func chTS(s string) string {
	return "'" + s + "'"
}
