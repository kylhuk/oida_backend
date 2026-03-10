package parser

import (
	"context"
	"encoding/json"
	"strings"
)

type jsonParser struct{}

func (jsonParser) Descriptor() Descriptor {
	return Descriptor{
		ID:               "parser:json",
		Family:           "structured",
		Version:          "1.0.0",
		RouteScope:       "raw_document",
		SourceClass:      "structured_document",
		HandlerRef:       "internal/parser.jsonParser",
		SupportedFormats: []string{"json", "application/json"},
	}
}

func (p jsonParser) Parse(_ context.Context, input Input) (Result, *ParseError) {
	if trimBody(input.Body) == "" {
		return Result{}, &ParseError{Code: CodeEmptyPayload, Message: "json payload is empty"}
	}

	decoder := json.NewDecoder(strings.NewReader(string(input.Body)))
	decoder.UseNumber()

	var payload any
	if err := decoder.Decode(&payload); err != nil {
		return Result{}, &ParseError{
			Code:    CodeInvalidJSON,
			Message: "json payload could not be decoded",
			Details: map[string]any{"error": err.Error()},
		}
	}

	desc := p.Descriptor()
	normalized := normalizeMapValue(payload)
	var candidates []Candidate
	switch typed := normalized.(type) {
	case map[string]any:
		candidates = append(candidates, newCandidate(input, desc, "json_object", extractNativeID(typed), typed, nil, nil))
	case []any:
		for i, item := range typed {
			attrs := map[string]any{"row_number": i + 1}
			switch record := item.(type) {
			case map[string]any:
				candidates = append(candidates, newCandidate(input, desc, "json_row", extractNativeID(record), record, attrs, nil))
			default:
				candidates = append(candidates, newCandidate(input, desc, "json_value", "", map[string]any{"value": record}, attrs, nil))
			}
		}
	default:
		candidates = append(candidates, newCandidate(input, desc, "json_value", "", map[string]any{"value": typed}, nil, nil))
	}

	if len(candidates) == 0 {
		return Result{}, &ParseError{Code: CodeInvalidJSON, Message: "json payload did not emit candidates"}
	}
	return newResult(desc, candidates), nil
}
