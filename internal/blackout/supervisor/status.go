package main

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
	"time"
)

const (
	// statusInterval is the time between two writes of status.json.
	statusInterval = 5 * time.Second
	// summaryInterval is the time between two summary lines on stderr.
	summaryInterval = 30 * time.Second
)

// counters counts the results of the new seeds.
// The runs of a shrink do not count.
type counters struct {
	Seeds      int
	Passes     int
	Failures   map[string]int // failed seeds for each kind
	Recoveries int            // sum of the recoveries that the workers checked
	Crashes    int            // sum of the crashes that the workers simulated
}

// add counts one run.
func (counts *counters) add(outcome runOutcome) {
	counts.Seeds++
	if outcome.Kind == kindPass {
		counts.Passes++
	} else {
		counts.Failures[outcome.Kind]++
	}
	if outcome.Result != nil {
		counts.Recoveries += outcome.Result.Recoveries
		counts.Crashes += outcome.Result.Crashes
	}
}

// status is the status.json file.
type status struct {
	Commit            string            `json:"commit"`
	Dirty             bool              `json:"dirty"`
	GoVersion         string            `json:"go_version"`
	Machine           string            `json:"machine"`
	StartTime         string            `json:"start_time"`
	Time              string            `json:"time"`
	Uptime            string            `json:"uptime"`
	UptimeSeconds     float64           `json:"uptime_seconds"`
	Workers           int               `json:"workers"`
	Running           int               `json:"running"`
	Stopping          bool              `json:"stopping"`
	SeedsRun          int               `json:"seeds_run"`
	SeedsPerSecond    float64           `json:"seeds_per_second"`
	Passes            int               `json:"passes"`
	Failures          map[string]int    `json:"failures"`
	RecoveriesChecked int               `json:"recoveries_checked"`
	CrashesSimulated  int               `json:"crashes_simulated"`
	ShrinksQueued     int               `json:"shrinks_queued"`
	Shrinking         string            `json:"shrinking,omitempty"` // signature ID of the running shrink
	BuildError        string            `json:"build_error,omitempty"`
	Signatures        []signatureStatus `json:"signatures"`
}

// signatureStatus is one signature in status.json.
type signatureStatus struct {
	ID            string `json:"id"`
	Kind          string `json:"kind"`
	Count         int    `json:"count"`
	ShortestSteps int    `json:"shortest_steps"`
	Text          string `json:"text"`
}

// newStatus makes a status from the counters and the signatures.
// The signature with the largest count comes first.
func newStatus(current *build, counts counters, records []signatureRecord, start, now time.Time) status {
	uptime := now.Sub(start)
	result := status{
		Commit:            current.Commit,
		Dirty:             current.Dirty,
		GoVersion:         current.GoVersion,
		StartTime:         start.UTC().Format(time.RFC3339),
		Time:              now.UTC().Format(time.RFC3339),
		Uptime:            uptime.Round(time.Second).String(),
		UptimeSeconds:     uptime.Seconds(),
		SeedsRun:          counts.Seeds,
		Passes:            counts.Passes,
		Failures:          make(map[string]int),
		RecoveriesChecked: counts.Recoveries,
		CrashesSimulated:  counts.Crashes,
		Signatures:        []signatureStatus{},
	}
	if seconds := uptime.Seconds(); seconds > 0 {
		result.SeedsPerSecond = float64(counts.Seeds) / seconds
	}
	for _, kind := range []string{kindFail, kindPanic, kindTimeout, kindError} {
		result.Failures[kind] = counts.Failures[kind]
	}
	slices.SortFunc(records, func(a, b signatureRecord) int {
		return cmp.Or(cmp.Compare(b.Count, a.Count), cmp.Compare(a.ID, b.ID))
	})
	for _, record := range records {
		result.Signatures = append(result.Signatures, signatureStatus{
			ID: record.ID, Kind: record.Kind, Count: record.Count, ShortestSteps: record.Shortest.Steps, Text: record.Text,
		})
	}
	return result
}

// summary returns a status as one line of text.
func (current status) summary() string {
	commit := current.Commit
	if current.Dirty {
		commit += "-dirty"
	}
	var failures strings.Builder
	for _, kind := range []string{kindFail, kindPanic, kindTimeout, kindError} {
		fmt.Fprintf(&failures, " %s %d", kind, current.Failures[kind])
	}
	return fmt.Sprintf("%s up %s: seeds %d (%.2f/s) pass %d%s, signatures %d, recoveries %d, crashes %d",
		commit, current.Uptime, current.SeedsRun, current.SeedsPerSecond, current.Passes, failures.String(),
		len(current.Signatures), current.RecoveriesChecked, current.CrashesSimulated)
}
