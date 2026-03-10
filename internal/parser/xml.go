package parser

import (
	"context"
	"encoding/xml"
	"fmt"
	"strconv"
	"strings"
)

type xmlParser struct{}

type rssParser struct{}

type atomParser struct{}

type htmlProfileParser struct{}

type treeNode struct {
	Name     string
	Attrs    map[string]string
	Text     string
	Children []*treeNode
	Parent   *treeNode
}

type rssDocument struct {
	Channel rssChannel `xml:"channel"`
}

type rssChannel struct {
	Title string    `xml:"title"`
	Link  string    `xml:"link"`
	Items []rssItem `xml:"item"`
}

type rssItem struct {
	Title       string   `xml:"title"`
	Link        string   `xml:"link"`
	Description string   `xml:"description"`
	GUID        string   `xml:"guid"`
	PubDate     string   `xml:"pubDate"`
	Categories  []string `xml:"category"`
}

type atomFeed struct {
	Title   string      `xml:"title"`
	ID      string      `xml:"id"`
	Updated string      `xml:"updated"`
	Entries []atomEntry `xml:"entry"`
}

type atomEntry struct {
	Title     string     `xml:"title"`
	ID        string     `xml:"id"`
	Updated   string     `xml:"updated"`
	Published string     `xml:"published"`
	Summary   string     `xml:"summary"`
	Links     []atomLink `xml:"link"`
}

type atomLink struct {
	Href string `xml:"href,attr"`
	Rel  string `xml:"rel,attr"`
}

func (xmlParser) Descriptor() Descriptor {
	return Descriptor{
		ID:               "parser:xml",
		Family:           "structured",
		Version:          "1.0.0",
		RouteScope:       "raw_document",
		SourceClass:      "structured_document",
		HandlerRef:       "internal/parser.xmlParser",
		SupportedFormats: []string{"xml", "application/xml", "text/xml"},
	}
}

func (rssParser) Descriptor() Descriptor {
	return Descriptor{
		ID:               "parser:rss",
		Family:           "feed",
		Version:          "1.0.0",
		RouteScope:       "raw_document",
		SourceClass:      "feed",
		HandlerRef:       "internal/parser.rssParser",
		SupportedFormats: []string{"rss", "application/rss+xml"},
	}
}

func (atomParser) Descriptor() Descriptor {
	return Descriptor{
		ID:               "parser:atom",
		Family:           "feed",
		Version:          "1.0.0",
		RouteScope:       "raw_document",
		SourceClass:      "feed",
		HandlerRef:       "internal/parser.atomParser",
		SupportedFormats: []string{"atom", "application/atom+xml"},
	}
}

func (htmlProfileParser) Descriptor() Descriptor {
	return Descriptor{
		ID:               "parser:html-profile",
		Family:           "html_profile",
		Version:          "1.0.0",
		RouteScope:       "raw_document",
		SourceClass:      "profile_page",
		HandlerRef:       "internal/parser.htmlProfileParser",
		SupportedFormats: []string{"html", "text/html", "application/xhtml+xml"},
	}
}

func (p xmlParser) Parse(_ context.Context, input Input) (Result, *ParseError) {
	root, err := parseTree(input.Body, false)
	if err != nil {
		return Result{}, err
	}
	desc := p.Descriptor()
	data := nodeToMap(root)
	candidate := newCandidate(input, desc, "xml_document", root.Attrs["id"], data, nil, nil)
	return newResult(desc, []Candidate{candidate}), nil
}

func (p rssParser) Parse(_ context.Context, input Input) (Result, *ParseError) {
	if trimBody(input.Body) == "" {
		return Result{}, &ParseError{Code: CodeEmptyPayload, Message: "rss payload is empty"}
	}
	var feed rssDocument
	if err := xml.Unmarshal(input.Body, &feed); err != nil {
		return Result{}, &ParseError{Code: CodeInvalidXML, Message: "rss payload could not be decoded", Details: map[string]any{"error": err.Error()}}
	}
	if len(feed.Channel.Items) == 0 {
		return Result{}, &ParseError{Code: CodeInvalidFeed, Message: "rss payload is missing channel items"}
	}
	desc := p.Descriptor()
	candidates := make([]Candidate, 0, len(feed.Channel.Items))
	for i, item := range feed.Channel.Items {
		data := map[string]any{
			"feed_type":    "rss",
			"feed_title":   strings.TrimSpace(feed.Channel.Title),
			"feed_link":    strings.TrimSpace(feed.Channel.Link),
			"title":        strings.TrimSpace(item.Title),
			"link":         strings.TrimSpace(item.Link),
			"guid":         strings.TrimSpace(item.GUID),
			"summary":      strings.TrimSpace(item.Description),
			"published_at": strings.TrimSpace(item.PubDate),
			"categories":   item.Categories,
		}
		attrs := map[string]any{"row_number": i + 1}
		nativeID := strings.TrimSpace(item.GUID)
		if nativeID == "" {
			nativeID = strings.TrimSpace(item.Link)
		}
		candidates = append(candidates, newCandidate(input, desc, "feed_item", nativeID, data, attrs, nil))
	}
	return newResult(desc, candidates), nil
}

func (p atomParser) Parse(_ context.Context, input Input) (Result, *ParseError) {
	if trimBody(input.Body) == "" {
		return Result{}, &ParseError{Code: CodeEmptyPayload, Message: "atom payload is empty"}
	}
	var feed atomFeed
	if err := xml.Unmarshal(input.Body, &feed); err != nil {
		return Result{}, &ParseError{Code: CodeInvalidXML, Message: "atom payload could not be decoded", Details: map[string]any{"error": err.Error()}}
	}
	if len(feed.Entries) == 0 {
		return Result{}, &ParseError{Code: CodeInvalidFeed, Message: "atom payload is missing feed entries"}
	}
	desc := p.Descriptor()
	candidates := make([]Candidate, 0, len(feed.Entries))
	for i, entry := range feed.Entries {
		data := map[string]any{
			"feed_type":    "atom",
			"feed_title":   strings.TrimSpace(feed.Title),
			"feed_id":      strings.TrimSpace(feed.ID),
			"feed_updated": strings.TrimSpace(feed.Updated),
			"title":        strings.TrimSpace(entry.Title),
			"id":           strings.TrimSpace(entry.ID),
			"link":         pickAtomLink(entry.Links),
			"summary":      strings.TrimSpace(entry.Summary),
			"updated_at":   strings.TrimSpace(entry.Updated),
			"published_at": strings.TrimSpace(entry.Published),
		}
		attrs := map[string]any{"row_number": i + 1}
		candidates = append(candidates, newCandidate(input, desc, "feed_item", strings.TrimSpace(entry.ID), data, attrs, nil))
	}
	return newResult(desc, candidates), nil
}

func (p htmlProfileParser) Parse(_ context.Context, input Input) (Result, *ParseError) {
	if input.Profile == nil {
		return Result{}, &ParseError{Code: CodeInvalidProfile, Message: "html profile parser requires a profile definition"}
	}
	if trimBody(input.Body) == "" {
		return Result{}, &ParseError{Code: CodeEmptyPayload, Message: "html payload is empty"}
	}
	root, err := parseTree(input.Body, true)
	if err != nil {
		return Result{}, err
	}
	desc := p.Descriptor()
	data := make(map[string]any, len(input.Profile.Fields))
	evidence := make([]Evidence, 0, len(input.Profile.Fields))
	for _, field := range input.Profile.Fields {
		if strings.TrimSpace(field.Name) == "" {
			return Result{}, &ParseError{Code: CodeInvalidProfile, Message: "html profile field is missing a name"}
		}
		matches, selector := selectNodes(root, field)
		if len(matches) == 0 {
			if field.Required {
				return Result{}, &ParseError{
					Code:    CodeSelectorNotFound,
					Message: fmt.Sprintf("profile field %q did not match any nodes", field.Name),
					Details: map[string]any{"field": field.Name, "selector": selector},
				}
			}
			continue
		}
		values := make([]string, 0, len(matches))
		for _, node := range matches {
			values = append(values, extractNodeValue(node, field.Attr))
		}
		if field.All {
			items := make([]any, len(values))
			for i := range values {
				items[i] = values[i]
			}
			data[field.Name] = items
		} else {
			data[field.Name] = values[0]
		}
		evidence = append(evidence, Evidence{Kind: "selector", Selector: selector, Value: strings.Join(values, " | ")})
	}
	nativeID := valueString(data["id"])
	if nativeID == "" {
		nativeID = valueString(data["data_id"])
	}
	attrs := map[string]any{"profile": input.Profile.Name}
	candidate := newCandidate(input, desc, "html_profile", nativeID, data, attrs, evidence)
	return newResult(desc, []Candidate{candidate}), nil
}

func parseTree(body []byte, htmlMode bool) (*treeNode, *ParseError) {
	if trimBody(body) == "" {
		return nil, &ParseError{Code: CodeEmptyPayload, Message: "xml payload is empty"}
	}
	decoder := xml.NewDecoder(strings.NewReader(string(body)))
	if htmlMode {
		decoder.Strict = false
		decoder.AutoClose = xml.HTMLAutoClose
		decoder.Entity = xml.HTMLEntity
	}
	var root *treeNode
	stack := []*treeNode{}
	for {
		token, err := decoder.Token()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return nil, &ParseError{Code: CodeInvalidXML, Message: "xml payload could not be tokenized", Details: map[string]any{"error": err.Error()}}
		}
		switch typed := token.(type) {
		case xml.StartElement:
			node := &treeNode{Name: strings.ToLower(typed.Name.Local), Attrs: map[string]string{}}
			for _, attr := range typed.Attr {
				node.Attrs[strings.ToLower(attr.Name.Local)] = attr.Value
			}
			if len(stack) > 0 {
				parent := stack[len(stack)-1]
				node.Parent = parent
				parent.Children = append(parent.Children, node)
			} else {
				root = node
			}
			stack = append(stack, node)
		case xml.EndElement:
			if len(stack) > 0 {
				stack = stack[:len(stack)-1]
			}
		case xml.CharData:
			if len(stack) == 0 {
				continue
			}
			text := strings.Join(strings.Fields(string(typed)), " ")
			if text == "" {
				continue
			}
			current := stack[len(stack)-1]
			if current.Text == "" {
				current.Text = text
			} else {
				current.Text += " " + text
			}
		}
	}
	if root == nil {
		return nil, &ParseError{Code: CodeInvalidXML, Message: "xml payload did not contain a document root"}
	}
	return root, nil
}

func nodeToMap(node *treeNode) map[string]any {
	childMaps := make([]any, len(node.Children))
	for i, child := range node.Children {
		childMaps[i] = nodeToMap(child)
	}
	return map[string]any{
		"name":     node.Name,
		"attrs":    node.Attrs,
		"text":     node.Text,
		"children": childMaps,
	}
}

func pickAtomLink(links []atomLink) string {
	for _, link := range links {
		if link.Rel == "alternate" && strings.TrimSpace(link.Href) != "" {
			return strings.TrimSpace(link.Href)
		}
	}
	for _, link := range links {
		if strings.TrimSpace(link.Href) != "" {
			return strings.TrimSpace(link.Href)
		}
	}
	return ""
}

func selectNodes(root *treeNode, field HTMLField) ([]*treeNode, string) {
	if selector := strings.TrimSpace(field.Selector); selector != "" {
		return selectCSS(root, selector), selector
	}
	if path := strings.TrimSpace(field.XPath); path != "" {
		return selectXPath(root, path), path
	}
	return nil, ""
}

type cssStep struct {
	Direct bool
	Query  cssQuery
}

type cssQuery struct {
	Tag       string
	ID        string
	Classes   []string
	AttrKey   string
	AttrValue string
}

func selectCSS(root *treeNode, selector string) []*treeNode {
	steps := parseCSSSelector(selector)
	if len(steps) == 0 {
		return nil
	}
	current := []*treeNode{root}
	for _, step := range steps {
		next := []*treeNode{}
		seen := map[*treeNode]struct{}{}
		for _, node := range current {
			var pool []*treeNode
			if step.Direct {
				pool = node.Children
			} else {
				pool = append([]*treeNode{node}, descendants(node)...)
			}
			for _, candidate := range pool {
				if !matchCSSQuery(candidate, step.Query) {
					continue
				}
				if _, exists := seen[candidate]; exists {
					continue
				}
				seen[candidate] = struct{}{}
				next = append(next, candidate)
			}
		}
		current = next
	}
	return current
}

func parseCSSSelector(selector string) []cssStep {
	parts := []string{}
	var current strings.Builder
	inAttr := false
	for _, r := range selector {
		switch {
		case r == '[':
			inAttr = true
			current.WriteRune(r)
		case r == ']':
			inAttr = false
			current.WriteRune(r)
		case !inAttr && (r == '>' || r == ' '):
			if current.Len() > 0 {
				parts = append(parts, current.String())
				current.Reset()
			}
			if r == '>' {
				parts = append(parts, ">")
			}
		case !inAttr && (r == '\n' || r == '\t'):
			if current.Len() > 0 {
				parts = append(parts, current.String())
				current.Reset()
			}
		default:
			current.WriteRune(r)
		}
	}
	if current.Len() > 0 {
		parts = append(parts, current.String())
	}
	steps := []cssStep{}
	direct := false
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if part == ">" {
			direct = true
			continue
		}
		steps = append(steps, cssStep{Direct: direct, Query: parseCSSQuery(part)})
		direct = false
	}
	return steps
}

func parseCSSQuery(token string) cssQuery {
	query := cssQuery{}
	attrKey := ""
	attrValue := ""
	if open := strings.Index(token, "["); open >= 0 {
		if close := strings.Index(token[open:], "]"); close > 0 {
			fragment := token[open+1 : open+close]
			token = token[:open]
			parts := strings.SplitN(fragment, "=", 2)
			attrKey = strings.ToLower(strings.TrimSpace(parts[0]))
			if len(parts) == 2 {
				attrValue = strings.Trim(parts[1], "\"'")
			}
		}
	}
	remaining := token
	for remaining != "" {
		switch remaining[0] {
		case '#':
			remaining = remaining[1:]
			query.ID, remaining = readUntilSpecial(remaining)
		case '.':
			remaining = remaining[1:]
			className, tail := readUntilSpecial(remaining)
			if className != "" {
				query.Classes = append(query.Classes, className)
			}
			remaining = tail
		default:
			query.Tag, remaining = readUntilSpecial(remaining)
		}
	}
	query.Tag = strings.ToLower(strings.TrimSpace(query.Tag))
	query.ID = strings.TrimSpace(query.ID)
	query.AttrKey = attrKey
	query.AttrValue = attrValue
	return query
}

func readUntilSpecial(s string) (string, string) {
	for i, r := range s {
		if r == '#' || r == '.' || r == '[' {
			return s[:i], s[i:]
		}
	}
	return s, ""
}

func matchCSSQuery(node *treeNode, query cssQuery) bool {
	if query.Tag != "" && node.Name != query.Tag {
		return false
	}
	if query.ID != "" && node.Attrs["id"] != query.ID {
		return false
	}
	if query.AttrKey != "" {
		value, ok := node.Attrs[query.AttrKey]
		if !ok {
			return false
		}
		if query.AttrValue != "" && value != query.AttrValue {
			return false
		}
	}
	if len(query.Classes) > 0 {
		classes := strings.Fields(node.Attrs["class"])
		for _, className := range query.Classes {
			if !containsString(classes, className) {
				return false
			}
		}
	}
	return true
}

type xpathStep struct {
	Descendant bool
	Name       string
	AttrKey    string
	AttrValue  string
	Index      int
}

func selectXPath(root *treeNode, path string) []*treeNode {
	steps := parseXPath(path)
	if len(steps) == 0 {
		return nil
	}
	current := []*treeNode{root}
	for i, step := range steps {
		next := []*treeNode{}
		for _, node := range current {
			var pool []*treeNode
			if i == 0 && !step.Descendant {
				pool = []*treeNode{node}
			} else if step.Descendant {
				pool = descendants(node)
			} else {
				pool = node.Children
			}
			for _, candidate := range pool {
				if candidate.Name != step.Name {
					continue
				}
				if step.AttrKey != "" && candidate.Attrs[step.AttrKey] != step.AttrValue {
					continue
				}
				next = append(next, candidate)
			}
		}
		if step.Index > 0 {
			if step.Index > len(next) {
				return nil
			}
			next = []*treeNode{next[step.Index-1]}
		}
		current = next
	}
	return current
}

func parseXPath(path string) []xpathStep {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil
	}
	steps := []xpathStep{}
	for len(path) > 0 {
		step := xpathStep{}
		switch {
		case strings.HasPrefix(path, "//"):
			step.Descendant = true
			path = path[2:]
		case strings.HasPrefix(path, "/"):
			path = path[1:]
		}
		segment := path
		if next := strings.Index(path, "/"); next >= 0 {
			segment = path[:next]
			path = path[next:]
		} else {
			path = ""
		}
		segment = strings.TrimSpace(segment)
		if segment == "" {
			continue
		}
		if open := strings.Index(segment, "["); open >= 0 {
			predicate := strings.TrimSuffix(segment[open+1:], "]")
			segment = segment[:open]
			if strings.HasPrefix(predicate, "@") {
				parts := strings.SplitN(strings.TrimPrefix(predicate, "@"), "=", 2)
				step.AttrKey = strings.ToLower(strings.TrimSpace(parts[0]))
				if len(parts) == 2 {
					step.AttrValue = strings.Trim(parts[1], "\"'")
				}
			} else if index, err := strconv.Atoi(predicate); err == nil {
				step.Index = index
			}
		}
		step.Name = strings.ToLower(strings.TrimSpace(segment))
		steps = append(steps, step)
	}
	return steps
}

func descendants(node *treeNode) []*treeNode {
	out := []*treeNode{}
	for _, child := range node.Children {
		out = append(out, child)
		out = append(out, descendants(child)...)
	}
	return out
}

func extractNodeValue(node *treeNode, attr string) string {
	if attr != "" {
		return strings.TrimSpace(node.Attrs[strings.ToLower(attr)])
	}
	if node.Text != "" {
		return strings.TrimSpace(node.Text)
	}
	parts := []string{}
	for _, child := range descendants(node) {
		if child.Text != "" {
			parts = append(parts, child.Text)
		}
	}
	return strings.TrimSpace(strings.Join(parts, " "))
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func valueString(v any) string {
	if v == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(v))
}
