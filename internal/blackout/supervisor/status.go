package main

import (
	"fmt"
	"strings"
	"time"
)

const (
	// statusInterval is the time between two writes of status.json.
	statusInterval = 5 * time.Second
	// summaryInterval is the time between two summary lines on stderr.
	summaryInterval = 30 * time.Second
)

// counters counts the results of the new seeds of the current commit.
// The runs of a shrink do not count.
type counters struct {
	Runs     int
	Failures map[string]int // failed seeds for each kind
	// MemoryKills counts the runs that passed -max-rss; they are not failures.
	MemoryKills int
}

// add counts one run.
func (counts *counters) add(outcome runOutcome) {
	counts.Runs++
	switch outcome.Kind {
	case kindPass:
	case kindMemory:
		counts.MemoryKills++
	default:
		counts.Failures[outcome.Kind]++
	}
}

// status is the status.json file: the result of the runs of the current commit, with every seed that failed.
// Replay a seed at the commit with: go test -run 'TestBlackout$' -blackout.seed=SEED -blackout.steps=STEPS -blackout.trace=200 -v .
type status struct {
	Commit     string         `json:"commit"`   // with "-dirty" when the checkout had changes
	Platform   string         `json:"platform"` // a replay is exact only with the same operating-system page size
	Time       string         `json:"time"`     // the time of this write; an old time means that the runner stopped
	Uptime     string         `json:"uptime"`   // since the first run of this commit
	Runs       int            `json:"runs"`     // runs of new seeds; shrink runs are not counted
	Failures   map[string]int `json:"failures"`
	BuildError string         `json:"build_error,omitempty"`
	Errors     []errorRecord  `json:"errors"` // the most frequent first; a shrink run that fails with a different error adds a seed here too
	// MemoryKills counts the runs that the supervisor killed at -max-rss.
	// They are not failures of KVLite, and the log names their seeds.
	MemoryKills int `json:"memory_kills"`
	// Totals counts all testing in -out over every restart and commit, as totals.json does.
	Totals *totals `json:"totals,omitempty"`
}

// newStatus makes a status from the counters and the errors.
func newStatus(current *build, platform string, counts counters, errors []errorRecord, start, now time.Time) status {
	result := status{
		Commit:   current.Commit,
		Platform: platform,
		Time:     now.UTC().Format(time.RFC3339),
		Uptime:   now.Sub(start).Round(time.Second).String(),
		Runs:     counts.Runs,
		Failures: make(map[string]int),
		Errors:   errors,
	}
	result.MemoryKills = counts.MemoryKills
	if current.Dirty {
		result.Commit += "-dirty"
	}
	for _, kind := range []string{kindFail, kindPanic, kindTimeout, kindError} {
		result.Failures[kind] = counts.Failures[kind]
	}
	return result
}

// summary returns a status as one line of text.
func (current status) summary() string {
	var failures strings.Builder
	for _, kind := range []string{kindFail, kindPanic, kindTimeout, kindError} {
		fmt.Fprintf(&failures, " %s %d", kind, current.Failures[kind])
	}
	line := fmt.Sprintf("%s up %s: runs %d,%s, memory kills %d, errors %d", current.Commit, current.Uptime, current.Runs, failures.String(), current.MemoryKills, len(current.Errors))
	if current.Totals != nil {
		line += fmt.Sprintf("; totals since %s: %.1f worker-hours, %d runs, %d crashes", current.Totals.Since, current.Totals.WorkerHours, current.Totals.Runs, current.Totals.Crashes)
	}
	return line
}
