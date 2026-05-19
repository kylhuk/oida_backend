package oidaql

import (
	"fmt"
	"regexp"
	"strings"
)

// forbiddenKeywords are SQL tokens that must never appear in an OIDA-QL query
// (case-insensitive word match). DDL, DML, and access-control statements.
var forbiddenKeywords = []string{
	"CREATE", "ALTER", "DROP", "TRUNCATE", "RENAME", "ATTACH", "DETACH",
	"INSERT", "UPDATE", "DELETE", "MERGE",
	"GRANT", "REVOKE",
	"SYSTEM", "KILL", "OPTIMIZE", "EXCHANGE",
}

// forbiddenFunctions are ClickHouse table/network function names that must
// not appear as function calls in an OIDA-QL query.
var forbiddenFunctions = []string{
	"url", "remote", "cluster", "s3", "mysql", "postgresql",
	"odbc", "jdbc", "sqlite", "mongodb", "redis", "rabbitmq",
	"kafka", "nats", "hdfs", "hive", "delta", "iceberg",
}

var (
	commentLineRe = regexp.MustCompile(`(?m)^[ \t]*--[^\n]*\n?`)
	inlineComment = regexp.MustCompile(`--[^\n]*`)
	wordBoundary  = func(word string) *regexp.Regexp {
		return regexp.MustCompile(`(?i)\b` + regexp.QuoteMeta(word) + `\b`)
	}
	logicalTableRe map[string]*regexp.Regexp
)

func init() {
	logicalTableRe = make(map[string]*regexp.Regexp, len(LogicalToPhysical))
	for logical := range LogicalToPhysical {
		logicalTableRe[logical] = wordBoundary(logical)
	}
}

// Compile validates and rewrites an OIDA-QL query into executable ClickHouse SQL.
// It strips comment lines, enforces single-statement SELECT-only, rejects DDL/DML
// and network table functions, and rewrites logical table names to physical views.
func Compile(queryText string) (string, error) {
	if len(queryText) > 64*1024 {
		return "", fmt.Errorf("query_text exceeds 64 KiB limit")
	}

	// Strip line comments.
	stripped := commentLineRe.ReplaceAllString(queryText, "")
	stripped = inlineComment.ReplaceAllString(stripped, "")
	stripped = strings.TrimSpace(stripped)

	if stripped == "" {
		return "", fmt.Errorf("query_text is empty after stripping comments")
	}

	// Multi-statement check: reject ';' outside single-quoted string literals.
	if hasSemicolonOutsideStrings(stripped) {
		return "", fmt.Errorf("multi-statement queries are not allowed")
	}

	// Must start with SELECT.
	firstToken := firstWord(stripped)
	if !strings.EqualFold(firstToken, "SELECT") {
		return "", fmt.Errorf("query must begin with SELECT; got %q", firstToken)
	}

	// Reject forbidden keywords (whole-word, case-insensitive).
	upperTokens := strings.ToUpper(stripped)
	for _, kw := range forbiddenKeywords {
		re := wordBoundary(kw)
		if re.MatchString(upperTokens) {
			return "", fmt.Errorf("forbidden keyword %q is not allowed in OIDA-QL queries", kw)
		}
	}

	// Reject forbidden function calls: name followed by '('.
	for _, fn := range forbiddenFunctions {
		pattern := regexp.MustCompile(`(?i)\b` + regexp.QuoteMeta(fn) + `\s*\(`)
		if pattern.MatchString(stripped) {
			return "", fmt.Errorf("forbidden function %q is not allowed in OIDA-QL queries", fn)
		}
	}

	// Rewrite logical table names to physical. Done on the original stripped
	// text to preserve user casing in non-table identifiers.
	compiled := stripped
	for logical, physical := range LogicalToPhysical {
		compiled = logicalTableRe[logical].ReplaceAllStringFunc(compiled, func(match string) string {
			return physical
		})
	}

	return compiled, nil
}

// hasSemicolonOutsideStrings returns true if a ';' character appears outside
// single-quoted string literals. Escape sequences inside strings ('') are
// handled by tracking adjacent single-quote pairs.
func hasSemicolonOutsideStrings(sql string) bool {
	inString := false
	for i := 0; i < len(sql); i++ {
		ch := sql[i]
		if ch == '\'' {
			if inString {
				// Escaped quote '' — check next char
				if i+1 < len(sql) && sql[i+1] == '\'' {
					i++ // skip escaped quote pair
					continue
				}
				inString = false
			} else {
				inString = true
			}
		} else if ch == ';' && !inString {
			return true
		}
	}
	return false
}

// firstWord returns the first whitespace-delimited token in s.
func firstWord(s string) string {
	s = strings.TrimSpace(s)
	for i, ch := range s {
		if ch == ' ' || ch == '\t' || ch == '\r' || ch == '\n' {
			return s[:i]
		}
	}
	return s
}
