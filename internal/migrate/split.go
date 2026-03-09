package migrate

import "strings"

func SplitStatements(sql string) []string {
	statements := make([]string, 0, strings.Count(sql, ";")+1)
	var current strings.Builder

	inSingleQuote := false
	inDoubleQuote := false
	inBacktick := false
	inLineComment := false
	inBlockComment := false

	flush := func() {
		stmt := strings.TrimSpace(current.String())
		if stmt != "" && hasSQLContent(stmt) {
			statements = append(statements, stmt)
		}
		current.Reset()
	}

	for i := 0; i < len(sql); i++ {
		ch := sql[i]

		switch {
		case inLineComment:
			current.WriteByte(ch)
			if ch == '\n' {
				inLineComment = false
			}
			continue
		case inBlockComment:
			current.WriteByte(ch)
			if ch == '*' && i+1 < len(sql) && sql[i+1] == '/' {
				current.WriteByte(sql[i+1])
				i++
				inBlockComment = false
			}
			continue
		case inSingleQuote:
			current.WriteByte(ch)
			if ch == '\\' && i+1 < len(sql) {
				current.WriteByte(sql[i+1])
				i++
				continue
			}
			if ch == '\'' {
				if i+1 < len(sql) && sql[i+1] == '\'' {
					current.WriteByte(sql[i+1])
					i++
					continue
				}
				inSingleQuote = false
			}
			continue
		case inDoubleQuote:
			current.WriteByte(ch)
			if ch == '\\' && i+1 < len(sql) {
				current.WriteByte(sql[i+1])
				i++
				continue
			}
			if ch == '"' {
				inDoubleQuote = false
			}
			continue
		case inBacktick:
			current.WriteByte(ch)
			if ch == '\\' && i+1 < len(sql) {
				current.WriteByte(sql[i+1])
				i++
				continue
			}
			if ch == '`' {
				inBacktick = false
			}
			continue
		}

		if ch == '-' && i+1 < len(sql) && sql[i+1] == '-' {
			current.WriteByte(ch)
			current.WriteByte(sql[i+1])
			i++
			inLineComment = true
			continue
		}
		if ch == '/' && i+1 < len(sql) && sql[i+1] == '*' {
			current.WriteByte(ch)
			current.WriteByte(sql[i+1])
			i++
			inBlockComment = true
			continue
		}

		switch ch {
		case '\'':
			inSingleQuote = true
		case '"':
			inDoubleQuote = true
		case '`':
			inBacktick = true
		case ';':
			flush()
			continue
		}

		current.WriteByte(ch)
	}

	flush()
	return statements
}

func hasSQLContent(sql string) bool {
	inLineComment := false
	inBlockComment := false

	for i := 0; i < len(sql); i++ {
		ch := sql[i]

		switch {
		case inLineComment:
			if ch == '\n' {
				inLineComment = false
			}
			continue
		case inBlockComment:
			if ch == '*' && i+1 < len(sql) && sql[i+1] == '/' {
				i++
				inBlockComment = false
			}
			continue
		}

		if ch == '-' && i+1 < len(sql) && sql[i+1] == '-' {
			i++
			inLineComment = true
			continue
		}
		if ch == '/' && i+1 < len(sql) && sql[i+1] == '*' {
			i++
			inBlockComment = true
			continue
		}
		if !isASCIISpace(ch) {
			return true
		}
	}

	return false
}

func isASCIISpace(ch byte) bool {
	switch ch {
	case ' ', '\t', '\n', '\r', '\f', '\v':
		return true
	default:
		return false
	}
}
