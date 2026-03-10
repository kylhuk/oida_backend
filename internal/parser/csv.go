package parser

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"strings"
)

type csvParser struct{}

func (csvParser) Descriptor() Descriptor {
	return Descriptor{
		ID:               "parser:csv",
		Family:           "structured",
		Version:          "1.0.0",
		RouteScope:       "raw_document",
		SourceClass:      "structured_document",
		HandlerRef:       "internal/parser.csvParser",
		SupportedFormats: []string{"csv", "tsv", "text/csv", "text/tab-separated-values"},
	}
}

func (p csvParser) Parse(_ context.Context, input Input) (Result, *ParseError) {
	if trimBody(input.Body) == "" {
		return Result{}, &ParseError{Code: CodeEmptyPayload, Message: "delimited payload is empty"}
	}

	delimiter := detectDelimiter(input)
	reader := csv.NewReader(strings.NewReader(string(input.Body)))
	reader.Comma = delimiter
	reader.TrimLeadingSpace = true

	headers, err := reader.Read()
	if err != nil {
		return Result{}, &ParseError{Code: CodeInvalidCSV, Message: "unable to read delimited header row", Details: map[string]any{"error": err.Error()}}
	}
	if len(headers) == 0 {
		return Result{}, &ParseError{Code: CodeSchemaDrift, Message: "delimited payload is missing header columns"}
	}
	for i := range headers {
		headers[i] = normalizeColumnName(headers[i], i)
	}
	reader.FieldsPerRecord = len(headers)

	desc := p.Descriptor()
	var candidates []Candidate
	rowNumber := 1
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			code := CodeInvalidCSV
			if strings.Contains(err.Error(), "wrong number of fields") {
				code = CodeSchemaDrift
			}
			return Result{}, &ParseError{
				Code:    code,
				Message: "delimited payload row could not be decoded",
				Details: map[string]any{"row_number": rowNumber + 1, "error": err.Error()},
			}
		}
		record := make(map[string]any, len(headers))
		for i, header := range headers {
			record[header] = row[i]
		}
		attrs := map[string]any{"row_number": rowNumber, "delimiter": string(delimiter)}
		candidates = append(candidates, newCandidate(input, desc, "structured_row", extractNativeID(record), record, attrs, nil))
		rowNumber++
	}

	if len(candidates) == 0 {
		return Result{}, &ParseError{Code: CodeInvalidCSV, Message: "delimited payload does not contain data rows"}
	}
	return newResult(desc, candidates), nil
}

func detectDelimiter(input Input) rune {
	for _, format := range candidateFormats(input) {
		if format == "tsv" {
			return '\t'
		}
	}
	trimmed := trimBody(input.Body)
	firstLine, _, _ := strings.Cut(trimmed, "\n")
	if strings.Count(firstLine, "\t") > strings.Count(firstLine, ",") {
		return '\t'
	}
	return ','
}

func normalizeColumnName(header string, index int) string {
	header = strings.TrimSpace(strings.ToLower(header))
	header = strings.ReplaceAll(header, " ", "_")
	if header == "" {
		return fmt.Sprintf("col_%d", index+1)
	}
	return header
}
