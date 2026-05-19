package oidaql

import "strings"

// MapColumnType converts a ClickHouse column type string to the spec ColumnType
// enum and reports whether the column is nullable.
// The spec enum is: string | integer | float | boolean | timestamp | date | geometry | json.
func MapColumnType(chType string) (specType string, nullable bool) {
	// Unwrap Nullable(X)
	if strings.HasPrefix(chType, "Nullable(") && strings.HasSuffix(chType, ")") {
		chType = chType[len("Nullable(") : len(chType)-1]
		nullable = true
	}
	// Unwrap LowCardinality(X)
	if strings.HasPrefix(chType, "LowCardinality(") && strings.HasSuffix(chType, ")") {
		chType = chType[len("LowCardinality(") : len(chType)-1]
	}

	switch {
	case chType == "Bool":
		return "boolean", nullable
	case chType == "Date" || chType == "Date32":
		return "date", nullable
	case strings.HasPrefix(chType, "DateTime"):
		return "timestamp", nullable
	case strings.HasPrefix(chType, "Float") || strings.HasPrefix(chType, "Decimal"):
		return "float", nullable
	case strings.HasPrefix(chType, "UInt") || strings.HasPrefix(chType, "Int"):
		return "integer", nullable
	case strings.HasPrefix(chType, "Array(") || strings.HasPrefix(chType, "Tuple(") || strings.HasPrefix(chType, "Map("):
		return "json", nullable
	case chType == "String" || strings.HasPrefix(chType, "FixedString("):
		return "string", nullable
	default:
		return "json", nullable
	}
}
