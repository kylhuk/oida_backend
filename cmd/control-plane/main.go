package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strings"
	"time"
)

type jobRunner struct {
	description string
	run         func(context.Context) error
}

type jobOptions struct {
	SourceID string
}

type jobOptionsContextKey struct{}

var jobRegistry = map[string]jobRunner{
	"noop": {
		description: "No-op contract check that exits successfully.",
		run: func(ctx context.Context) error {
			log.Println("run-once job noop completed")
			return nil
		},
	},
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		serve()
		return 0
	}

	switch args[0] {
	case "help", "-h", "--help":
		printRootUsage(stdout)
		return 0
	case "run-once":
		return runOnce(args[1:], stdout, stderr)
	default:
		fmt.Fprintf(stderr, "unknown command %q\n\n", args[0])
		printRootUsage(stderr)
		return 2
	}
}

func serve() {
	log.Println("control-plane started")
	for {
		time.Sleep(30 * time.Second)
		log.Println("control-plane tick")
	}
}

func runOnce(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("run-once", flag.ContinueOnError)
	fs.SetOutput(stderr)
	jobName := fs.String("job", "", "Registered internal job name to execute exactly once.")
	sourceID := fs.String("source-id", "", "Optional source identifier for source-scoped internal jobs.")
	fs.Usage = func() {
		fmt.Fprint(fs.Output(), runOnceUsage())
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
	if *jobName == "" {
		fmt.Fprint(stderr, "missing required --job value\n\n")
		fs.Usage()
		return 2
	}

	job, ok := jobRegistry[*jobName]
	if !ok {
		fmt.Fprintf(stderr, "unknown job %q\n\n", *jobName)
		fs.Usage()
		return 2
	}

	ctx := context.WithValue(context.Background(), jobOptionsContextKey{}, jobOptions{SourceID: strings.TrimSpace(*sourceID)})
	log.Printf("run-once job starting: %s", *jobName)
	if err := job.run(ctx); err != nil {
		fmt.Fprintf(stderr, "job %q failed: %v\n", *jobName, err)
		return 1
	}
	fmt.Fprintf(stdout, "run-once job completed: %s\n", *jobName)
	return 0
}

func runOnceUsage() string {
	var b strings.Builder
	b.WriteString("Usage:\n")
	b.WriteString("  control-plane run-once --job <job-name>\n\n")
	b.WriteString("Contract:\n")
	b.WriteString("  - Executes exactly one registered internal job and exits.\n")
	b.WriteString("  - Job names are validated against the built-in registry.\n")
	b.WriteString("  - Unknown jobs and missing --job values exit non-zero.\n\n")
	b.WriteString("Options:\n")
	b.WriteString("  --job string\n")
	b.WriteString("        Registered internal job name to execute exactly once.\n\n")
	b.WriteString("  --source-id string\n")
	b.WriteString("        Optional source identifier for source-scoped jobs.\n\n")
	b.WriteString("Registered jobs:\n")
	for _, name := range sortedJobNames() {
		fmt.Fprintf(&b, "  - %s: %s\n", name, jobRegistry[name].description)
	}
	return b.String()
}

func printRootUsage(w io.Writer) {
	fmt.Fprintln(w, "Usage:")
	fmt.Fprintln(w, "  control-plane [command]")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Commands:")
	fmt.Fprintln(w, "  run-once    Execute one registered internal job and exit")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Run `control-plane run-once --help` for the deterministic job contract.")
}

func sortedJobNames() []string {
	names := make([]string, 0, len(jobRegistry))
	for name := range jobRegistry {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func currentJobOptions(ctx context.Context) jobOptions {
	if ctx == nil {
		return jobOptions{}
	}
	options, _ := ctx.Value(jobOptionsContextKey{}).(jobOptions)
	return options
}
