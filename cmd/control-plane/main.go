package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"global-osint-backend/internal/observability"
)

type jobRunner struct {
	description string
	run         func(context.Context) error
}

type jobOptions struct {
	SourceID    string
	PipelineID  string
	WindowStart string
	WindowEnd   string
	DeltaOnly   bool
}

type jobOptionsContextKey struct{}

var jobRegistry = map[string]jobRunner{
	"noop": {
		description: "No-op contract check that exits successfully.",
		run: func(ctx context.Context) error {
			observability.LogEvent("control-plane", "run_once_job_completed", observability.CorrelationID(ctx), map[string]any{"job": "noop", "status": "success"})
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
	serviceCorrelationID := observability.NewCorrelationID("control-plane")
	observability.LogEvent("control-plane", "service_started", serviceCorrelationID, nil)
	maxTicks := getenvInt("CONTROL_PLANE_MAX_TICKS", 0)
	tickCount := 0
	for {
		tickCorrelationID := observability.NewCorrelationID("control-plane-tick")
		tickCtx := observability.WithCorrelationID(context.Background(), tickCorrelationID)
		if err := runAutomaticSyncTick(tickCtx); err != nil {
			observability.LogEvent("control-plane", "automatic_sync_tick_failed", tickCorrelationID, map[string]any{"error": err.Error(), "tick": tickCount + 1})
		} else {
			observability.LogEvent("control-plane", "automatic_sync_tick_completed", tickCorrelationID, map[string]any{"tick": tickCount + 1})
		}
		tickCount++
		if maxTicks > 0 && tickCount >= maxTicks {
			observability.LogEvent("control-plane", "service_stopping", serviceCorrelationID, map[string]any{"ticks": tickCount})
			return
		}
		time.Sleep(30 * time.Second)
		observability.LogEvent("control-plane", "service_tick_wait_complete", serviceCorrelationID, map[string]any{"tick": tickCount})
	}
}

func getenvInt(key string, defaultValue int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return defaultValue
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return defaultValue
	}
	return parsed
}

func runOnce(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("run-once", flag.ContinueOnError)
	fs.SetOutput(stderr)
	jobName := fs.String("job", "", "Registered internal job name to execute exactly once.")
	sourceID := fs.String("source-id", "", "Optional source identifier for source-scoped internal jobs.")
	pipelineID := fs.String("pipeline-id", "", "Optional stored pipeline identifier for pipeline execution jobs.")
	windowStart := fs.String("window-start", "", "Optional inclusive UTC lower bound for replay/backfill windows.")
	windowEnd := fs.String("window-end", "", "Optional exclusive UTC upper bound for replay/backfill windows.")
	deltaOnly := fs.Bool("delta-only", false, "Force delta-only selection from the durable promote watermark.")
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

	correlationID := observability.NewCorrelationID("control-plane")
	ctx := observability.WithCorrelationID(context.Background(), correlationID)
	ctx = context.WithValue(ctx, jobOptionsContextKey{}, jobOptions{SourceID: strings.TrimSpace(*sourceID), PipelineID: strings.TrimSpace(*pipelineID), WindowStart: strings.TrimSpace(*windowStart), WindowEnd: strings.TrimSpace(*windowEnd), DeltaOnly: *deltaOnly})
	observability.LogEvent("control-plane", "run_once_job_started", correlationID, map[string]any{"job": *jobName})
	if err := job.run(ctx); err != nil {
		observability.LogEvent("control-plane", "run_once_job_failed", correlationID, map[string]any{"job": *jobName, "error": err.Error()})
		fmt.Fprintf(stderr, "job %q failed: %v\n", *jobName, err)
		return 1
	}
	observability.LogEvent("control-plane", "run_once_job_completed", correlationID, map[string]any{"job": *jobName, "status": "success"})
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
	b.WriteString("  --pipeline-id string\n")
	b.WriteString("        Optional stored pipeline identifier for pipeline execution jobs.\n\n")
	b.WriteString("  --window-start string\n")
	b.WriteString("        Optional inclusive UTC lower bound for replay/backfill windows.\n\n")
	b.WriteString("  --window-end string\n")
	b.WriteString("        Optional exclusive UTC upper bound for replay/backfill windows.\n\n")
	b.WriteString("  --delta-only\n")
	b.WriteString("        Force delta-only selection from the durable promote watermark.\n\n")
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

func runAutomaticSyncTick(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := runFingerprintProbeGenerationTick(ctx); err != nil {
		return err
	}
	if err := runFamilyTemplateGenerationTick(ctx); err != nil {
		return err
	}
	if err := runAutomaticHTTPSync(ctx); err != nil {
		return err
	}
	return nil
}
