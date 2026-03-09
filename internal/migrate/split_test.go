package migrate

import "testing"

func TestSplitStatements(t *testing.T) {
	in := "CREATE DATABASE a;\n\nCREATE TABLE b(x Int32); ;"
	got := SplitStatements(in)
	if len(got) != 2 {
		t.Fatalf("expected 2 statements, got %d", len(got))
	}
}
