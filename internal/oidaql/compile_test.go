package oidaql_test

import (
	"strings"
	"testing"

	"global-osint-backend/internal/oidaql"
)

func TestCompileRejectsNonSelect(t *testing.T) {
	cases := []struct {
		name  string
		query string
	}{
		{"INSERT", "INSERT INTO foo VALUES (1)"},
		{"DROP", "DROP TABLE foo"},
		{"CREATE", "CREATE TABLE foo (id Int32) ENGINE=Memory"},
		{"ALTER", "ALTER TABLE foo ADD COLUMN x String"},
		{"UPDATE", "UPDATE foo SET x=1 WHERE id=1"},
		{"DELETE", "DELETE FROM foo WHERE id=1"},
		{"empty", ""},
		{"comment only", "-- this is a comment\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := oidaql.Compile(tc.query)
			if err == nil {
				t.Errorf("expected error for %q, got nil", tc.query)
			}
		})
	}
}

func TestCompileRejectsMultiStatement(t *testing.T) {
	_, err := oidaql.Compile("SELECT 1; DROP TABLE foo")
	if err == nil {
		t.Error("expected error for multi-statement query")
	}
}

func TestCompileRejectsSemicolonInString(t *testing.T) {
	// Semicolons inside string literals must not trigger rejection.
	sql := "SELECT ';' AS semicolon FROM entities"
	out, err := oidaql.Compile(sql)
	if err != nil {
		t.Errorf("unexpected error for semicolon in string: %v", err)
	}
	if !strings.Contains(out, "gold.api_v1_entities") {
		t.Errorf("expected table rewrite, got: %s", out)
	}
}

func TestCompileRejectsNetworkFunctions(t *testing.T) {
	cases := []string{
		"SELECT * FROM url('http://evil.com', 'CSV', 'x String')",
		"SELECT * FROM s3('s3://bucket/key', 'CSV')",
		"SELECT * FROM remote('host', 'db', 'table')",
		"SELECT * FROM mysql('host:port', 'db', 'table', 'user', 'pass')",
	}
	for _, q := range cases {
		_, err := oidaql.Compile(q)
		if err == nil {
			t.Errorf("expected error for network function in: %s", q)
		}
	}
}

func TestCompileRewritesLogicalTables(t *testing.T) {
	cases := []struct {
		logical  string
		physical string
	}{
		{"entities", "gold.api_v1_entities"},
		{"places", "gold.api_v1_places"},
		{"events", "gold.api_v1_events"},
		{"observations", "gold.api_v1_observations"},
		{"track_points", "silver.fact_track_point"},
		{"track_segments", "gold.api_v1_tracks"},
		{"artifacts", "gold.api_v1_artifacts"},
		{"metrics", "gold.api_v1_metrics"},
	}
	for _, tc := range cases {
		t.Run(tc.logical, func(t *testing.T) {
			out, err := oidaql.Compile("SELECT * FROM " + tc.logical + " LIMIT 10")
			if err != nil {
				t.Fatalf("unexpected compile error: %v", err)
			}
			if !strings.Contains(out, tc.physical) {
				t.Errorf("expected %q in output, got: %s", tc.physical, out)
			}
			if strings.Contains(out, " "+tc.logical+" ") || strings.HasSuffix(out, " "+tc.logical) {
				t.Errorf("logical table %q should be rewritten, still present in: %s", tc.logical, out)
			}
		})
	}
}

func TestCompileStripsCommentLines(t *testing.T) {
	// The frontend emitter prefixes queries with a comment line.
	query := "-- emitter-v0: confirm grammar with OIDA backend team\nSELECT entity_id FROM entities\nWHERE status = 'active'\nLIMIT 100"
	out, err := oidaql.Compile(query)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.Contains(out, "--") {
		t.Errorf("comment should be stripped, got: %s", out)
	}
	if !strings.Contains(out, "gold.api_v1_entities") {
		t.Errorf("expected entities rewrite, got: %s", out)
	}
}

func TestCompileFrontendEmitterOutput(t *testing.T) {
	// Simulate the query emitted by oida-ql-emitter.ts with aggregation.
	query := `-- emitter-v0: confirm grammar with OIDA backend team
SELECT entity_id FROM entities
WHERE (canonical_name LIKE '%Aurora%' OR status IN ('active', 'inactive'))
GROUP BY entity_id
HAVING (SELECT COUNT(*) FROM track_points WHERE entity_id = entities.entity_id) > 5
LIMIT 100`

	out, err := oidaql.Compile(query)
	if err != nil {
		t.Fatalf("unexpected compile error: %v", err)
	}
	if !strings.Contains(out, "gold.api_v1_entities") {
		t.Error("expected entities rewrite")
	}
	if !strings.Contains(out, "silver.fact_track_point") {
		t.Error("expected track_points rewrite")
	}
}

func TestMapColumnType(t *testing.T) {
	cases := []struct {
		chType   string
		specType string
		nullable bool
	}{
		{"String", "string", false},
		{"LowCardinality(String)", "string", false},
		{"Nullable(String)", "string", true},
		{"UInt64", "integer", false},
		{"Int32", "integer", false},
		{"Float64", "float", false},
		{"Decimal(10,2)", "float", false},
		{"Bool", "boolean", false},
		{"Date", "date", false},
		{"Date32", "date", false},
		{"DateTime64(3,'UTC')", "timestamp", false},
		{"DateTime", "timestamp", false},
		{"Array(String)", "json", false},
		{"Tuple(String, Int32)", "json", false},
		{"Map(String, UInt64)", "json", false},
		{"Nullable(UInt32)", "integer", true},
	}
	for _, tc := range cases {
		specType, nullable := oidaql.MapColumnType(tc.chType)
		if specType != tc.specType {
			t.Errorf("MapColumnType(%q) specType = %q, want %q", tc.chType, specType, tc.specType)
		}
		if nullable != tc.nullable {
			t.Errorf("MapColumnType(%q) nullable = %v, want %v", tc.chType, nullable, tc.nullable)
		}
	}
}
