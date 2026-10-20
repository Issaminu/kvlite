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
	"strconv"
	"strings"
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
	memoryBudget   int64 // memory of all workers together; zero means unknown
	steps          int   // safety-phase steps of every seed; zero means defaultLanes
	duration       time.Duration
	reports        bool
	issues         string // owner/name of the GitHub repository for automatic issues; empty means none
	follow         bool
	followInterval time.Duration
	remote         string
	branch         string
}

// memoryLimitPattern matches the values of GOMEMLIMIT.
var memoryLimitPattern = regexp.MustCompile(`^(off|[0-9]+(B|KiB|MiB|GiB|TiB)?)$`)

// errUsage is the error of a flag that the flag package already reported.
var errUsage = errors.New("usage error")

// parseOptions parses the command-line flags and returns the lanes of the new seeds.
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
	flags.IntVar(&opts.workers, "workers", 0, "number of worker processes that run at the same time (default: one for each CPU that the Go runtime uses, as -memory allows)")
	flags.DurationVar(&opts.timeout, "timeout", 2*time.Hour, "time limit of one worker; slow CPUs make long seeds slow, so the limit finds only runs that make no progress")
	flags.StringVar(&opts.memoryLimit, "memlimit", "", "soft memory `limit` of one worker (GOMEMLIMIT) (default: half of -max-rss)")
	maxRSS := flags.String("max-rss", "", "hard memory `limit` of one worker: the supervisor kills a worker whose RSS is larger; off removes the limit (default: from -memory and the workers, at most 4GiB)")
	memory := flags.String("memory", "", "memory `budget` of all workers together (default: half of the RAM, or of the cgroup memory limit when it is smaller)")
	flags.IntVar(&opts.steps, "steps", 0, "safety-phase steps of every seed, for example for a quick check (default: 80% of the seeds with 300 steps and 20% with 3000 steps)")
	flags.DurationVar(&opts.duration, "duration", 0, "stop after this time; zero runs until a signal")
	flags.BoolVar(&opts.reports, "reports", false, "write the full report of each failed seed to reports/SEED.txt; a replay of the seed gives the same report")
	flags.StringVar(&opts.issues, "issues", "", "GitHub `repository` (owner/name) that gets an automatic issue for each new error; the token is in the environment variable GITHUB_TOKEN")
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
	set := make(map[string]bool)
	flags.Visit(func(given *flag.Flag) { set[given.Name] = true })
	total, _ := systemMemory()
	opts.memoryBudget = total / 2
	var err error
	if set["memory"] {
		if opts.memoryBudget, err = parseBytes(*memory); err != nil || opts.memoryBudget <= 0 {
			return opts, nil, fmt.Errorf("-memory %q is not a positive size, such as 1GiB", *memory)
		}
	}
	opts.maxRSS = -1 // plan chooses it
	if set["max-rss"] {
		if opts.maxRSS, err = parseBytes(*maxRSS); err != nil {
			return opts, nil, fmt.Errorf("-max-rss: %w", err)
		}
		if opts.maxRSS != 0 && opts.maxRSS < minMaxRSS {
			return opts, nil, fmt.Errorf("-max-rss must be off or at least %d MiB", minMaxRSS>>20)
		}
	}
	if set["workers"] && opts.workers <= 0 {
		return opts, nil, errors.New("-workers must be positive")
	}
	// GOMAXPROCS follows the cgroup CPU limit, and the kernel enforces that limit, so the supervisor reads no CPU limit itself.
	opts.workers, opts.maxRSS = plan(opts.memoryBudget, runtime.GOMAXPROCS(0), opts.workers, opts.maxRSS)
	if !set["memlimit"] {
		perWorker := opts.maxRSS
		if perWorker == 0 {
			perWorker = targetMaxRSS
		}
		opts.memoryLimit = strconv.FormatInt(perWorker/2, 10)
	}
	switch {
	case flags.NArg() > 0:
		return opts, nil, fmt.Errorf("unexpected arguments: %q", flags.Args())
	case opts.steps < 0:
		return opts, nil, errors.New("-steps must not be negative")
	case opts.timeout <= 0:
		return opts, nil, errors.New("-timeout must be positive")
	case opts.issues != "" && strings.Count(opts.issues, "/") != 1:
		return opts, nil, fmt.Errorf("-issues %q is not a repository of the form owner/name", opts.issues)
	case opts.issues != "" && os.Getenv("GITHUB_TOKEN") == "":
		return opts, nil, errors.New("-issues needs a GitHub token in the environment variable GITHUB_TOKEN")
	case opts.followInterval <= 0:
		return opts, nil, errors.New("-follow-interval must be positive")
	case !memoryLimitPattern.MatchString(opts.memoryLimit):
		return opts, nil, fmt.Errorf("-memlimit %q is not a GOMEMLIMIT value, such as 2GiB", opts.memoryLimit)
	}
	lanes := defaultLanes
	if opts.steps > 0 {
		lanes = []lane{{"steps", 1, opts.steps}}
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
