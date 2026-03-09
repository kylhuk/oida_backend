package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"global-osint-backend/internal/migrate"
)

type sourceSeed struct {
	SourceID            string   `json:"source_id"`
	Domain              string   `json:"domain"`
	DomainFamily        string   `json:"domain_family"`
	SourceClass         string   `json:"source_class"`
	Entrypoints         []string `json:"entrypoints"`
	AuthMode            string   `json:"auth_mode"`
	FormatHint          string   `json:"format_hint"`
	RobotsPolicy        string   `json:"robots_policy"`
	RefreshStrategy     string   `json:"refresh_strategy"`
	License             string   `json:"license"`
	TermsURL            string   `json:"terms_url"`
	GeoScope            string   `json:"geo_scope"`
	Priority            int      `json:"priority"`
	ParserID            string   `json:"parser_id"`
	EntityTypes         []string `json:"entity_types"`
	ExpectedPlaceTypes  []string `json:"expected_place_types"`
	SupportsHistorical  bool     `json:"supports_historical"`
	SupportsDelta       bool     `json:"supports_delta"`
	ConfidenceBaseline  float64  `json:"confidence_baseline"`
}

func main() {
	ctx := context.Background()
	migrationDir := getenv("MIGRATIONS_DIR", "/app/migrations/clickhouse")
	clickhouseHTTP := getenv("CLICKHOUSE_HTTP_URL", "http://clickhouse:8123")
	seedPath := getenv("SOURCE_REGISTRY_SEED", "/app/seed/source_registry.json")

	runner := migrate.NewHTTPRunner(clickhouseHTTP)
	if err := runner.EnsureMigrationsTable(ctx); err != nil {
		log.Fatalf("ensure migration table: %v", err)
	}
	if err := applyMigrations(ctx, runner, migrationDir); err != nil {
		log.Fatal(err)
	}
	if err := loadSourceSeed(ctx, runner, seedPath); err != nil {
		log.Fatalf("load source seed: %v", err)
	}

	marker := getenv("BOOTSTRAP_READY_MARKER", "/tmp/bootstrap.ready")
	if err := os.WriteFile(marker, []byte(fmt.Sprintf("ready %s\n", time.Now().UTC().Format(time.RFC3339))), 0o644); err != nil {
		log.Fatal(err)
	}
	log.Println("bootstrap complete")
}

func applyMigrations(ctx context.Context, runner *migrate.HTTPRunner, migrationDir string) error {
	files, err := filepath.Glob(filepath.Join(migrationDir, "*.sql"))
	if err != nil {
		return err
	}
	sort.Strings(files)

	for _, f := range files {
		name := filepath.Base(f)
		b, err := os.ReadFile(f)
		if err != nil {
			return fmt.Errorf("read migration %s: %w", name, err)
		}
		checksum := sum(b)
		applied, err := runner.IsApplied(ctx, name, checksum)
		if err != nil {
			return fmt.Errorf("check applied %s: %w", name, err)
		}
		if applied {
			log.Printf("migration already applied: %s", name)
			continue
		}
		if err := runner.ApplySQL(ctx, string(b)); err != nil {
			_ = runner.Record(ctx, name, checksum, false, err.Error())
			return fmt.Errorf("apply migration %s: %w", name, err)
		}
		if err := runner.Record(ctx, name, checksum, true, "applied"); err != nil {
			return fmt.Errorf("record migration %s: %w", name, err)
		}
		log.Printf("applied migration: %s", name)
	}
	return nil
}

func loadSourceSeed(ctx context.Context, runner *migrate.HTTPRunner, path string) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var seeds []sourceSeed
	if err := json.Unmarshal(b, &seeds); err != nil {
		return err
	}
	for _, s := range seeds {
		check := fmt.Sprintf("SELECT count() FROM meta.source_registry WHERE source_id='%s' FORMAT TabSeparated", esc(s.SourceID))
		out, err := runner.Query(ctx, check)
		if err != nil {
			return err
		}
		if strings.TrimSpace(out) != "0" {
			continue
		}
		insert := fmt.Sprintf(`INSERT INTO meta.source_registry
(source_id, domain, domain_family, source_class, entrypoints, auth_mode, format_hint, robots_policy, refresh_strategy, license, terms_url, geo_scope, priority, parser_id, entity_types, expected_place_types, supports_historical, supports_delta, confidence_baseline, enabled, version, updated_at)
VALUES ('%s','%s','%s','%s',%s,'%s','%s','%s','%s','%s','%s','%s',%d,'%s',%s,%s,%d,%d,%f,1,1,now64(3))`,
			esc(s.SourceID), esc(s.Domain), esc(s.DomainFamily), esc(s.SourceClass), arr(s.Entrypoints), esc(s.AuthMode), esc(s.FormatHint), esc(s.RobotsPolicy), esc(s.RefreshStrategy), esc(s.License), esc(s.TermsURL), esc(s.GeoScope), s.Priority, esc(s.ParserID), arr(s.EntityTypes), arr(s.ExpectedPlaceTypes), btoi(s.SupportsHistorical), btoi(s.SupportsDelta), s.ConfidenceBaseline)
		if err := runner.ApplySQL(ctx, insert); err != nil {
			return err
		}
	}
	return nil
}

func btoi(b bool) int { if b { return 1 }; return 0 }

func arr(items []string) string {
	parts := make([]string, 0, len(items))
	for _, it := range items {
		parts = append(parts, fmt.Sprintf("'%s'", esc(it)))
	}
	return "[" + strings.Join(parts, ",") + "]"
}

func esc(s string) string { return strings.ReplaceAll(strings.TrimSpace(s), "'", "''") }

func sum(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func getenv(k, d string) string {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		return v
	}
	return d
}
