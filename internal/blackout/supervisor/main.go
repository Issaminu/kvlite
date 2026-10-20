package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"runtime"
	"syscall"
	"time"
)

// options are the command-line flags.
type options struct {
	repo           string
	out            string
	goTool         string
	workers        int
	timeout        time.Duration
	memoryLimit    string
	maxRSS         int64 // zero means no limit
	lanes          string
	duration       time.Duration
	reports        bool
	alertCommand   string
	follow         bool
	followInterval time.Duration
	remote         string
	branch         string
}

// memoryLimitPattern matches the values of GOMEMLIMIT.
var memoryLimitPattern = regexp.MustCompile(`^(off|[0-9]+(B|KiB|MiB|GiB|TiB)?)$`)

// errUsage is the error of a flag that the flag package already reported.
var errUsage = errors.New("usage error")

// parseOptions parses the command-line flags and the lanes.
func parseOptions(args []string, output io.Writer) (options, []lane, error) {
	var opts options
	flags := flag.NewFlagSet("supervisor", flag.ContinueOnError)
	flags.SetOutput(output)
	flags.Usage = func() {
		fmt.Fprint(flags.Output(), "Usage: go run ./internal/blackout/supervisor [flags]\n\n")
		fmt.Fprint(flags.Output(), "The supervisor runs random Blackout seeds in worker processes and logs each failure.\n")
		fmt.Fprint(flags.Output(), "\"go doc ./internal/blackout/supervisor\" describes the output layout.\n\nFlags:\n")
		flags.PrintDefaults()
	}
	flags.StringVar(&opts.repo, "repo", ".", "KVLite `checkout` to compile; follow mode changes it, so give the runner its own checkout")
	flags.StringVar(&opts.out, "out", "blackout-results", "output `directory` for the status and the reports")
	flags.StringVar(&opts.goTool, "go", "go", "Go `command` that compiles the test binary")
	flags.IntVar(&opts.workers, "workers", 0, "number of worker processes that run at the same time (default: half the cores, and at most half the RAM divided by -max-rss)")
	flags.DurationVar(&opts.timeout, "timeout", 5*time.Minute, "time limit of one worker")
	flags.StringVar(&opts.memoryLimit, "memlimit", "2GiB", "soft memory `limit` of one worker (GOMEMLIMIT)")
	maxRSS := flags.String("max-rss", fmt.Sprintf("%dGiB", defaultMaxRSS>>30), "hard memory `limit` of one worker: the supervisor kills a worker whose RSS is larger; off removes the limit")
	flags.StringVar(&opts.lanes, "lanes", "default=80:steps=300,long=20:steps=3000", "lanes as name=weight:steps=N, with a comma between two lanes")
	flags.DurationVar(&opts.duration, "duration", 0, "stop after this time; zero runs until a signal")
	flags.BoolVar(&opts.reports, "reports", false, "write the full report of each failed seed to reports/SEED.txt; a replay of the seed gives the same report")
	flags.StringVar(&opts.alertCommand, "alert-command", "", "`command` to run for a new error, with the seed and the first line of the report as arguments")
	flags.BoolVar(&opts.follow, "follow", false, "fetch the remote and check out each new commit of the branch in -repo")
	flags.DurationVar(&opts.followInterval, "follow-interval", 5*time.Minute, "time between two fetches in follow mode")
	flags.StringVar(&opts.remote, "remote", "origin", "git remote for follow mode")
	flags.StringVar(&opts.branch, "branch", "main", "git branch for follow mode")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return opts, nil, err
		}
		return opts, nil, errUsage
	}
	var err error
	if opts.maxRSS, err = parseBytes(*maxRSS); err != nil {
		return opts, nil, fmt.Errorf("-max-rss: %w", err)
	}
	workersSet := false
	flags.Visit(func(set *flag.Flag) { workersSet = workersSet || set.Name == "workers" })
	if !workersSet {
		opts.workers = defaultWorkers(runtime.NumCPU(), physicalMemory(), opts.maxRSS)
	}
	switch {
	case flags.NArg() > 0:
		return opts, nil, fmt.Errorf("unexpected arguments: %q", flags.Args())
	case opts.workers <= 0:
		return opts, nil, errors.New("-workers must be positive")
	case opts.timeout <= 0:
		return opts, nil, errors.New("-timeout must be positive")
	case opts.followInterval <= 0:
		return opts, nil, errors.New("-follow-interval must be positive")
	case !memoryLimitPattern.MatchString(opts.memoryLimit):
		return opts, nil, fmt.Errorf("-memlimit %q is not a GOMEMLIMIT value, such as 2GiB", opts.memoryLimit)
	}
	lanes, err := parseLanes(opts.lanes)
	if err != nil {
		return opts, nil, err
	}
	if opts.repo, err = filepath.Abs(opts.repo); err != nil {
		return opts, nil, err
	}
	if opts.out, err = filepath.Abs(opts.out); err != nil {
		return opts, nil, err
	}
	return opts, lanes, nil
}

// stopContexts returns the stop context, which SIGINT or SIGTERM cancels, and the hard context, which a second signal cancels.
func stopContexts(logger *log.Logger) (stop context.Context, stopCancel context.CancelFunc, hard context.Context) {
	stop, stopCancel = context.WithCancel(context.Background())
	hard, hardCancel := context.WithCancel(context.Background())
	signals := make(chan os.Signal, 2)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-signals
		logger.Print("signal: start no new seeds and wait for the running workers; a second signal kills them")
		stopCancel()
		<-signals
		logger.Print("second signal: kill the running workers")
		hardCancel()
	}()
	return stop, stopCancel, hard
}

func main() {
	opts, lanes, err := parseOptions(os.Args[1:], os.Stderr)
	switch {
	case errors.Is(err, flag.ErrHelp):
		return
	case errors.Is(err, errUsage):
		os.Exit(2)
	case err != nil:
		fmt.Fprintln(os.Stderr, "supervisor:", err)
		os.Exit(2)
	}
	stop, stopCancel, hard := stopContexts(log.New(os.Stderr, "blackout ", log.LstdFlags))
	if err := newSupervisor(opts, lanes, stop, stopCancel, hard).run(); err != nil {
		fmt.Fprintln(os.Stderr, "supervisor:", err)
		os.Exit(1)
	}
}
