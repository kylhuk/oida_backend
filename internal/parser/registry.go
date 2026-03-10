package parser

import (
	"context"
	"encoding/json"
	"fmt"
	"mime"
	"sort"
	"strings"
	"time"

	"global-osint-backend/internal/canonical"
)

const (
	apiContractVersion = 1

	CodeParserNotRegistered = "parser_not_registered"
	CodeEmptyPayload        = "empty_payload"
	CodeInvalidJSON         = "invalid_json"
	CodeInvalidCSV          = "invalid_csv"
	CodeSchemaDrift         = "schema_drift"
	CodeInvalidXML          = "invalid_xml"
	CodeInvalidFeed         = "invalid_feed"
	CodeSelectorNotFound    = "selector_not_found"
	CodeInvalidProfile      = "invalid_profile"
)

type Input struct {
	ParserID    string
	SourceID    string
	RawID       string
	URL         string
	FormatHint  string
	ContentType string
	Body        []byte
	FetchedAt   time.Time
	Attrs       map[string]any
	Profile     *HTMLProfile
}

type HTMLProfile struct {
	Name   string      `json:"name"`
	Fields []HTMLField `json:"fields"`
}

type HTMLField struct {
	Name     string `json:"name"`
	Selector string `json:"selector,omitempty"`
	XPath    string `json:"xpath,omitempty"`
	Attr     string `json:"attr,omitempty"`
	All      bool   `json:"all,omitempty"`
	Required bool   `json:"required,omitempty"`
}

type Evidence = canonical.Evidence

type Candidate = canonical.RecordEnvelope

type Result struct {
	ParserID      string      `json:"parser_id"`
	ParserVersion string      `json:"parser_version"`
	Candidates    []Candidate `json:"candidates"`
}

type ParseError struct {
	Code      string         `json:"code"`
	Message   string         `json:"message"`
	Retryable bool           `json:"retryable"`
	Details   map[string]any `json:"details,omitempty"`
}

func (e *ParseError) Error() string {
	if e == nil {
		return ""
	}
	return e.Code + ": " + e.Message
}

type Descriptor struct {
	ID               string   `json:"parser_id"`
	Family           string   `json:"parser_family"`
	Version          string   `json:"parser_version"`
	RouteScope       string   `json:"route_scope"`
	SourceClass      string   `json:"source_class"`
	HandlerRef       string   `json:"handler_ref"`
	SupportedFormats []string `json:"supported_formats"`
}

type RegistryRecord struct {
	ParserID           string         `json:"parser_id"`
	ParserFamily       string         `json:"parser_family"`
	ParserVersion      string         `json:"parser_version"`
	RouteScope         string         `json:"route_scope"`
	InputFormat        string         `json:"input_format"`
	SourceClass        string         `json:"source_class"`
	HandlerRef         string         `json:"handler_ref"`
	Attrs              map[string]any `json:"attrs"`
	Evidence           []Evidence     `json:"evidence"`
	SchemaVersion      uint32         `json:"schema_version"`
	RecordVersion      uint64         `json:"record_version"`
	APIContractVersion uint32         `json:"api_contract_version"`
	Enabled            bool           `json:"enabled"`
	UpdatedAt          time.Time      `json:"updated_at"`
}

type Parser interface {
	Descriptor() Descriptor
	Parse(context.Context, Input) (Result, *ParseError)
}

type Registry struct {
	byID     map[string]Parser
	byFormat map[string][]Parser
	records  []RegistryRecord
}

func DefaultRegistry() *Registry {
	registry, err := NewRegistry(
		jsonParser{},
		csvParser{},
		xmlParser{},
		rssParser{},
		atomParser{},
		htmlProfileParser{},
	)
	if err != nil {
		panic(err)
	}
	return registry
}

func NewRegistry(parsers ...Parser) (*Registry, error) {
	r := &Registry{
		byID:     make(map[string]Parser, len(parsers)),
		byFormat: make(map[string][]Parser),
	}
	for _, parser := range parsers {
		if err := r.Register(parser); err != nil {
			return nil, err
		}
	}
	return r, nil
}

func (r *Registry) Register(p Parser) error {
	desc := p.Descriptor()
	if desc.ID == "" {
		return fmt.Errorf("parser missing id")
	}
	if _, exists := r.byID[desc.ID]; exists {
		return fmt.Errorf("duplicate parser id %q", desc.ID)
	}
	r.byID[desc.ID] = p
	now := time.Now().UTC().Round(time.Millisecond)
	for _, format := range desc.SupportedFormats {
		normalized := normalizeFormat(format)
		if normalized == "" {
			continue
		}
		r.byFormat[normalized] = append(r.byFormat[normalized], p)
		r.records = append(r.records, RegistryRecord{
			ParserID:           desc.ID,
			ParserFamily:       desc.Family,
			ParserVersion:      desc.Version,
			RouteScope:         desc.RouteScope,
			InputFormat:        normalized,
			SourceClass:        desc.SourceClass,
			HandlerRef:         desc.HandlerRef,
			Attrs:              map[string]any{"supported_formats": desc.SupportedFormats},
			Evidence:           []Evidence{canonical.NewParserVersionEvidence(desc.ID, desc.Version)},
			SchemaVersion:      canonical.SchemaVersion,
			RecordVersion:      canonical.InitialRecordVersion,
			APIContractVersion: apiContractVersion,
			Enabled:            true,
			UpdatedAt:          now,
		})
	}
	return nil
}

func (r *Registry) Lookup(parserID string) (Parser, bool) {
	p, ok := r.byID[strings.TrimSpace(parserID)]
	return p, ok
}

func (r *Registry) Resolve(input Input) (Parser, *ParseError) {
	if input.ParserID != "" {
		if parser, ok := r.Lookup(input.ParserID); ok {
			return parser, nil
		}
		return nil, &ParseError{
			Code:    CodeParserNotRegistered,
			Message: fmt.Sprintf("parser %q is not registered", input.ParserID),
			Details: map[string]any{"parser_id": input.ParserID},
		}
	}

	for _, format := range candidateFormats(input) {
		parsers := r.byFormat[format]
		if len(parsers) == 0 {
			continue
		}
		return parsers[0], nil
	}

	return nil, &ParseError{
		Code:    CodeParserNotRegistered,
		Message: "no parser matched the input contract",
		Details: map[string]any{
			"format_hint":  input.FormatHint,
			"content_type": input.ContentType,
			"candidates":   candidateFormats(input),
		},
	}
}

func (r *Registry) Parse(ctx context.Context, input Input) (Result, *ParseError) {
	parser, err := r.Resolve(input)
	if err != nil {
		return Result{}, err
	}
	return parser.Parse(ctx, input)
}

func (r *Registry) Records() []RegistryRecord {
	copyRecords := append([]RegistryRecord(nil), r.records...)
	sort.Slice(copyRecords, func(i, j int) bool {
		if copyRecords[i].InputFormat != copyRecords[j].InputFormat {
			return copyRecords[i].InputFormat < copyRecords[j].InputFormat
		}
		return copyRecords[i].ParserID < copyRecords[j].ParserID
	})
	return copyRecords
}

func newResult(desc Descriptor, candidates []Candidate) Result {
	return Result{
		ParserID:      desc.ID,
		ParserVersion: desc.Version,
		Candidates:    candidates,
	}
}

func newCandidate(input Input, desc Descriptor, kind, nativeID string, data map[string]any, attrs map[string]any, evidence []Evidence) Candidate {
	if data == nil {
		data = map[string]any{}
	}
	mergedEvidence := make([]Evidence, 0, len(evidence)+2)
	if input.RawID != "" || input.URL != "" {
		mergedEvidence = append(mergedEvidence, canonical.NewRawDocumentEvidence(input.SourceID, input.RawID, input.URL))
	}
	mergedEvidence = append(mergedEvidence, canonical.NewParserVersionEvidence(desc.ID, desc.Version))
	mergedEvidence = append(mergedEvidence, evidence...)
	return canonical.NewRecordEnvelope(kind, data, canonical.EnvelopeOptions{
		SourceID:      input.SourceID,
		RawID:         input.RawID,
		NativeID:      nativeID,
		ParserID:      desc.ID,
		ParserVersion: desc.Version,
		Attrs:         attrs,
		Evidence:      mergedEvidence,
	})
}

func candidateFormats(input Input) []string {
	formats := []string{}
	appendUniqueFormat(&formats, normalizeFormat(input.FormatHint))
	appendContentTypeFormats(&formats, input.ContentType)
	if input.Profile != nil {
		appendUniqueFormat(&formats, "html")
	}
	appendUniqueFormat(&formats, sniffFormat(input.Body))
	return formats
}

func appendContentTypeFormats(formats *[]string, contentType string) {
	mediaType, _, err := mime.ParseMediaType(strings.TrimSpace(contentType))
	if err != nil {
		appendUniqueFormat(formats, normalizeFormat(contentType))
		return
	}
	mediaType = normalizeFormat(mediaType)
	appendUniqueFormat(formats, mediaType)
	parts := strings.Split(mediaType, "/")
	if len(parts) == 2 {
		appendUniqueFormat(formats, parts[1])
		if strings.HasSuffix(parts[1], "+xml") {
			appendUniqueFormat(formats, strings.TrimSuffix(parts[1], "+xml"))
			appendUniqueFormat(formats, "xml")
		}
	}
	if strings.Contains(mediaType, "tab-separated") {
		appendUniqueFormat(formats, "tsv")
	}
}

func appendUniqueFormat(target *[]string, format string) {
	if format == "" {
		return
	}
	for _, existing := range *target {
		if existing == format {
			return
		}
	}
	*target = append(*target, format)
}

func normalizeFormat(v string) string {
	v = strings.TrimSpace(strings.ToLower(v))
	v = strings.TrimSuffix(v, "; charset=utf-8")
	switch v {
	case "application/json-seq", "jsonl", "ndjson":
		return "json"
	case "text/csv", "application/csv", "csv":
		return "csv"
	case "text/tab-separated-values", "application/tab-separated-values", "tsv":
		return "tsv"
	case "application/xml", "text/xml", "xml":
		return "xml"
	case "application/rss+xml", "rss":
		return "rss"
	case "application/atom+xml", "atom":
		return "atom"
	case "text/html", "application/xhtml+xml", "html":
		return "html"
	default:
		return v
	}
}

func sniffFormat(body []byte) string {
	trimmed := strings.TrimSpace(string(body))
	if trimmed == "" {
		return ""
	}
	if strings.HasPrefix(trimmed, "{") || strings.HasPrefix(trimmed, "[") {
		return "json"
	}
	if strings.Contains(trimmed, "\n") {
		firstLine, _, _ := strings.Cut(trimmed, "\n")
		switch {
		case strings.Count(firstLine, "\t") > 0:
			return "tsv"
		case strings.Count(firstLine, ",") > 0:
			return "csv"
		}
	}
	if strings.HasPrefix(trimmed, "<") {
		lower := strings.ToLower(trimmed)
		switch {
		case strings.Contains(lower, "<rss"):
			return "rss"
		case strings.Contains(lower, "<feed"):
			return "atom"
		case strings.Contains(lower, "<html"):
			return "html"
		default:
			return "xml"
		}
	}
	return ""
}

func trimBody(body []byte) string {
	return strings.TrimSpace(string(body))
}

func normalizeMapValue(v any) any {
	switch typed := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, value := range typed {
			out[key] = normalizeMapValue(value)
		}
		return out
	case []any:
		out := make([]any, len(typed))
		for i, value := range typed {
			out[i] = normalizeMapValue(value)
		}
		return out
	case json.Number:
		if integer, err := typed.Int64(); err == nil {
			return integer
		}
		if floatValue, err := typed.Float64(); err == nil {
			return floatValue
		}
		return typed.String()
	default:
		return v
	}
}

func extractNativeID(record map[string]any, fallbackKeys ...string) string {
	keys := append([]string{"id", "guid", "uuid", "code", "key"}, fallbackKeys...)
	for _, key := range keys {
		value, ok := record[key]
		if !ok {
			continue
		}
		if s := strings.TrimSpace(fmt.Sprint(value)); s != "" {
			return s
		}
	}
	return ""
}
