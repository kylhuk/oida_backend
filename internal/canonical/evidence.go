package canonical

import "time"

type Evidence struct {
	Kind          string         `json:"kind"`
	SourceID      string         `json:"source_id,omitempty"`
	RawID         string         `json:"raw_id,omitempty"`
	Ref           string         `json:"ref,omitempty"`
	URL           string         `json:"url,omitempty"`
	Selector      string         `json:"selector,omitempty"`
	Pointer       string         `json:"pointer,omitempty"`
	Value         string         `json:"value,omitempty"`
	ParserID      string         `json:"parser_id,omitempty"`
	ParserVersion string         `json:"parser_version,omitempty"`
	CapturedAt    *time.Time     `json:"captured_at,omitempty"`
	Attrs         map[string]any `json:"attrs,omitempty"`
}

func NewRawDocumentEvidence(sourceID, rawID, url string) Evidence {
	return Evidence{
		Kind:     "raw_document",
		SourceID: sourceID,
		RawID:    rawID,
		Ref:      rawID,
		URL:      url,
	}
}

func NewParserVersionEvidence(parserID, parserVersion string) Evidence {
	return Evidence{
		Kind:          "parser_version",
		Ref:           parserID,
		Value:         parserVersion,
		ParserID:      parserID,
		ParserVersion: parserVersion,
	}
}

func NewSelectorEvidence(selector, value string) Evidence {
	return Evidence{
		Kind:     "selector",
		Selector: selector,
		Value:    value,
	}
}
