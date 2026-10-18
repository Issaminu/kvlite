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
	Seeds    int
	Failures map[string]int // failed seeds for each kind
}

// add counts one run.
func (counts *counters) add(outcome runOutcome) {
	counts.Seeds++
	if outcome.Kind != kindPass {
		counts.Failures[outcome.Kind]++
	}
}

// status is the status.json file: what a person needs to see at a glance.
// The passes and the rate follow from the other fields, and the failure log keeps the details of each failure.
type status struct {
	Commit     string            `json:"commit"` // with "-dirty" when the checkout had changes
	Time       string            `json:"time"`   // the time of this write; an old time means that the runner stopped
	Uptime     string            `json:"uptime"`
	SeedsRun   int               `json:"seeds_run"`
	Failures   map[string]int    `json:"failures"`
	BuildError string            `json:"build_error,omitempty"`
	Signatures []signatureStatus `json:"signatures"`
}

// signatureStatus is one signature in status.json. The text starts with the kind of the failure.
type signatureStatus struct {
	ID            string `json:"id"`
	Count         int    `json:"count"`
	ShortestSteps int    `json:"shortest_steps"`
	Text          string `json:"text"`
}

// newStatus makes a status from the counters and the signatures.
// The signature with the largest count comes first.
func newStatus(current *build, counts counters, records []signatureRecord, start, now time.Time) status {
	result := status{
		Commit:     current.Commit,
		Time:       now.UTC().Format(time.RFC3339),
		Uptime:     now.Sub(start).Round(time.Second).String(),
		SeedsRun:   counts.Seeds,
		Failures:   make(map[string]int),
		Signatures: []signatureStatus{},
	}
	if current.Dirty {
		result.Commit += "-dirty"
	}
	for _, kind := range []string{kindFail, kindPanic, kindTimeout, kindError} {
		result.Failures[kind] = counts.Failures[kind]
	}
	slices.SortFunc(records, func(a, b signatureRecord) int {
		return cmp.Or(cmp.Compare(b.Count, a.Count), cmp.Compare(a.ID, b.ID))
	})
	for _, record := range records {
		result.Signatures = append(result.Signatures, signatureStatus{
			ID: record.ID, Count: record.Count, ShortestSteps: record.Shortest.Steps, Text: record.Text,
		})
	}
	return result
}

// summary returns a status as one line of text.
func (current status) summary() string {
	var failures strings.Builder
	for _, kind := range []string{kindFail, kindPanic, kindTimeout, kindError} {
		fmt.Fprintf(&failures, " %s %d", kind, current.Failures[kind])
	}
	return fmt.Sprintf("%s up %s: seeds %d,%s, signatures %d", current.Commit, current.Uptime, current.SeedsRun, failures.String(), len(current.Signatures))
}
