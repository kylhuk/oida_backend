package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"global-osint-backend/internal/parser"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}

func run(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		serve()
		return 0
	}

	switch args[0] {
	case "help", "-h", "--help":
		printRootUsage(stdout)
		return 0
	case "list-parsers":
		return listParsers(stdout)
	case "parse":
		return parseOnce(args[1:], stdin, stdout, stderr)
	default:
		fmt.Fprintf(stderr, "unknown command %q\n\n", args[0])
		printRootUsage(stderr)
		return 2
	}
}

func serve() {
	registry := parser.DefaultRegistry()
	log.Printf("worker-parse started with %d parser registry routes", len(registry.Records()))
	for {
		time.Sleep(30 * time.Second)
		log.Println("worker-parse idle")
	}
}

func listParsers(stdout io.Writer) int {
	registry := parser.DefaultRegistry()
	return writeJSON(stdout, registry.Records())
}

func parseOnce(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("parse", flag.ContinueOnError)
	fs.SetOutput(stderr)
	parserID := fs.String("parser-id", "", "Explicit parser ID to route to.")
	formatHint := fs.String("format", "", "Optional input format hint such as json, csv, rss, atom, or html.")
	contentType := fs.String("content-type", "", "Optional content type for parser routing.")
	sourceID := fs.String("source-id", "", "Source ID carried into candidate output.")
	rawID := fs.String("raw-id", "", "Raw document ID carried into candidate output.")
	url := fs.String("url", "", "Optional source URL for evidence payloads.")
	profilePath := fs.String("profile", "", "Optional JSON file path for parser:html-profile selector definitions.")
	fs.Usage = func() {
		fmt.Fprint(fs.Output(), parseUsage())
	}
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if fs.NArg() != 0 {
		fmt.Fprintf(stderr, "unexpected arguments: %s\n\n", strings.Join(fs.Args(), " "))
		fs.Usage()
		return 2
	}

	body, err := io.ReadAll(stdin)
	if err != nil {
		fmt.Fprintf(stderr, "read stdin: %v\n", err)
		return 1
	}
	input := parser.Input{
		ParserID:    strings.TrimSpace(*parserID),
		SourceID:    strings.TrimSpace(*sourceID),
		RawID:       strings.TrimSpace(*rawID),
		URL:         strings.TrimSpace(*url),
		FormatHint:  strings.TrimSpace(*formatHint),
		ContentType: strings.TrimSpace(*contentType),
		Body:        body,
		FetchedAt:   time.Now().UTC(),
	}
	if strings.TrimSpace(*profilePath) != "" {
		profile, err := loadProfile(*profilePath)
		if err != nil {
			fmt.Fprintf(stderr, "load profile: %v\n", err)
			return 1
		}
		input.Profile = profile
	}

	result, parseErr := parser.DefaultRegistry().Parse(context.Background(), input)
	if parseErr != nil {
		_ = writeJSON(stdout, map[string]any{"error": parseErr})
		return 1
	}
	return writeJSON(stdout, result)
}

func loadProfile(path string) (*parser.HTMLProfile, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var profile parser.HTMLProfile
	if err := json.Unmarshal(b, &profile); err != nil {
		return nil, err
	}
	return &profile, nil
}

func writeJSON(w io.Writer, value any) int {
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(value); err != nil {
		return 1
	}
	return 0
}

func printRootUsage(w io.Writer) {
	fmt.Fprintln(w, "Usage:")
	fmt.Fprintln(w, "  worker-parse [command]")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Commands:")
	fmt.Fprintln(w, "  list-parsers    Print built-in parser registry routes as JSON")
	fmt.Fprintln(w, "  parse           Parse stdin and emit canonical candidates as JSON")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Run `worker-parse parse --help` for the parser runtime contract.")
}

func parseUsage() string {
	var b strings.Builder
	b.WriteString("Usage:\n")
	b.WriteString("  worker-parse parse [options] < input\n\n")
	b.WriteString("Contract:\n")
	b.WriteString("  - Resolves the requested or inferred parser via the built-in parser registry.\n")
	b.WriteString("  - Reads the raw payload from stdin and emits structured canonical candidates as JSON.\n")
	b.WriteString("  - Emits machine-readable parser errors on stdout and exits non-zero on failure.\n\n")
	b.WriteString("Options:\n")
	b.WriteString("  --parser-id string\n")
	b.WriteString("        Explicit parser ID to route to.\n")
	b.WriteString("  --format string\n")
	b.WriteString("        Optional input format hint such as json, csv, rss, atom, or html.\n")
	b.WriteString("  --content-type string\n")
	b.WriteString("        Optional content type for parser routing.\n")
	b.WriteString("  --source-id string\n")
	b.WriteString("        Source ID carried into candidate output.\n")
	b.WriteString("  --raw-id string\n")
	b.WriteString("        Raw document ID carried into candidate output.\n")
	b.WriteString("  --url string\n")
	b.WriteString("        Optional source URL for evidence payloads.\n")
	b.WriteString("  --profile string\n")
	b.WriteString("        Optional JSON file path for parser:html-profile selector definitions.\n")
	return b.String()
}
