package migrate

import "testing"

func TestSplitStatements(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want []string
	}{
		{
			name: "drops blank statements",
			in:   "CREATE DATABASE a;\n\nCREATE TABLE b(x Int32); ;",
			want: []string{"CREATE DATABASE a", "CREATE TABLE b(x Int32)"},
		},
		{
			name: "keeps semicolons in strings",
			in:   "INSERT INTO logs VALUES ('alpha;beta'), ('two'';three');\nSELECT ';';",
			want: []string{"INSERT INTO logs VALUES ('alpha;beta'), ('two'';three')", "SELECT ';'"},
		},
		{
			name: "keeps semicolons in comments",
			in:   "-- file preface; still comment\nCREATE TABLE t(x String);\n/* block; comment */\nINSERT INTO t VALUES ('ok');\n-- trailing; comment only",
			want: []string{"-- file preface; still comment\nCREATE TABLE t(x String)", "/* block; comment */\nINSERT INTO t VALUES ('ok')"},
		},
		{
			name: "keeps semicolons in quoted identifiers",
			in:   "CREATE TABLE \"semi;quoted\" (`value;col` String);",
			want: []string{"CREATE TABLE \"semi;quoted\" (`value;col` String)"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SplitStatements(tt.in)
			if len(got) != len(tt.want) {
				t.Fatalf("expected %d statements, got %d: %#v", len(tt.want), len(got), got)
			}
			for i := range tt.want {
				if got[i] != tt.want[i] {
					t.Fatalf("statement %d mismatch\nwant: %q\ngot:  %q", i, tt.want[i], got[i])
				}
			}
		})
	}
}
