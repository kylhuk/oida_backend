package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSourceSilverCoverageRegistryDenominatorQueryUsesFrozenScope(t *testing.T) {
	query := sourceSilverCoverageRegistryDenominatorQuery()
	for _, fragment := range []string{
		"FROM meta.source_registry FINAL",
		"catalog_kind='concrete'",
		"transport_type='http'",
		"bronze_table IS NOT NULL",
		"countDistinct(source_id)",
	} {
		if !strings.Contains(query, fragment) {
			t.Fatalf("expected denominator query to include %q, got %q", fragment, query)
		}
	}
}

func TestSourceSilverCoverageUnexpectedStatesQueryAllowsOnlyFrozenStates(t *testing.T) {
	query := sourceSilverCoverageUnexpectedStatesQuery()
	for _, state := range sourceSilverCoverageRequiredStates() {
		if !strings.Contains(query, "'"+esc(state)+"'") {
			t.Fatalf("expected unexpected-state query to include %q, got %q", state, query)
		}
	}
	if !strings.Contains(query, "coverage_state NOT IN") {
		t.Fatalf("expected NOT IN state guard, got %q", query)
	}
}

func TestSourceSilverCoverageRequiredColumnsExactContract(t *testing.T) {
	got := sourceSilverCoverageRequiredColumns()
	want := []string{
		"source_id",
		"coverage_state",
		"routing_mode",
		"promote_profile",
		"terminal_kind",
		"terminal_destination",
		"last_bronze_at",
		"last_parse_at",
		"last_promote_at",
		"last_silver_at",
		"reason",
		"attrs",
		"updated_at",
	}
	if len(got) != len(want) {
		t.Fatalf("expected %d contract columns, got %d: %#v", len(want), len(got), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("expected contract column %d to be %q, got %q", i, want[i], got[i])
		}
	}
}

func TestSourceSilverCoverageMissingRoutingMetadataQueryChecksAllRequiredFields(t *testing.T) {
	query := sourceSilverCoverageMissingRoutingMetadataQuery()
	for _, fragment := range []string{
		"FROM meta.source_silver_coverage",
		"routing_mode = ''",
		"promote_profile = ''",
		"terminal_destination = ''",
	} {
		if !strings.Contains(query, fragment) {
			t.Fatalf("expected missing-routing query to include %q, got %q", fragment, query)
		}
	}
}

func TestSourceSilverCoverageContractUsesExplicitRoutingMatrix(t *testing.T) {
	path := filepath.Join("..", "..", "migrations", "clickhouse", "zzzzzz_source_silver_coverage_routing_matrix.sql")
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read routing matrix migration: %v", err)
	}
	sql := string(content)
	for _, fragment := range []string{
		"routing_matrix AS",
		"SELECT 'promote:geopolitical' AS promote_profile, 'event_records' AS source_shape",
		"SELECT 'promote:safety_security', 'observation_records', 'profile_specific', 'table', 'silver.fact_observation'",
		"SELECT 'promote:catalog', 'catalog_metadata', 'canonical', 'view', 'silver.v_source_terminal_catalog'",
		"source_shapes AS",
		"promote_profile = 'promote:catalog' AND source_class = 'catalog_source', 'catalog_metadata'",
		"promote_profile = 'promote:aviation' AND has(entity_types, 'aircraft'), 'track_points'",
		"promote_profile = 'promote:aviation' AND hasAny(entity_types, ['airport', 'airspace', 'navaid', 'reporting_point']), 'reference_entities'",
	} {
		if !strings.Contains(sql, fragment) {
			t.Fatalf("expected routing matrix migration to include %q", fragment)
		}
	}
}
