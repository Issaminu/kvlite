package main

import (
	"context"
	"errors"
	"io"
	"log"
	"testing"
)

func TestClassify(t *testing.T) {
	exitErr := errors.New("exit status 1")
	passed := &workerResult{Passed: true}
	failed := &workerResult{Passed: false, Failure: "blackout seed 1 failed at step 2: x"}
	for _, test := range []struct {
		name     string
		result   *workerResult
		waitErr  error
		output   string
		stopKind string
		want     string
	}{
		{"passed", passed, nil, "PASS\n", "", kindPass},
		{"failed check", failed, exitErr, "--- FAIL: TestBlackout\n", "", kindFail},
		{"failed check with a panic line in the report", failed, exitErr, "    panic: boom\n", "", kindFail},
		{"panic in another goroutine", nil, exitErr, "panic: boom\n\ngoroutine 5 [running]:\n", "", kindPanic},
		{"fatal error", nil, exitErr, "fatal error: concurrent map writes\n", "", kindPanic},
		{"panic after a passed result", passed, exitErr, "panic: late\n", "", kindPanic},
		{"timeout", failed, exitErr, "SIGQUIT: quit\n", kindTimeout, kindTimeout},
		{"memory limit", nil, exitErr, "", kindMemory, kindMemory},
		{"exit without a result", nil, exitErr, "something odd\n", "", kindError},
		{"no test ran", nil, nil, "testing: warning: no tests to run\nPASS\n", "", kindError},
		{"non-zero exit after a passed result", passed, exitErr, "", "", kindError},
	} {
		if got := classify(test.result, test.waitErr, test.output, test.stopKind); got != test.want {
			t.Errorf("%s: got %s, want %s", test.name, got, test.want)
		}
	}
}

// TestMemoryKillIsNotAFailure requires that a run killed at -max-rss counts as a run and a memory kill, with no error, no shrink, and no failure count.
func TestMemoryKillIsNotAFailure(t *testing.T) {
	s := newSupervisor(options{workers: 1}, nil, context.Background(), func() {}, context.Background())
	s.logger = log.New(io.Discard, "", 0)
	current := &build{Commit: "test123"}
	s.setBuild(current, nil)
	s.handleSeed(current, runOutcome{Kind: kindMemory, Seed: 7, Steps: 3000, FirstLine: "memory: the worker used 5000 MiB, more than the limit of 4096 MiB"}, nil)
	got := s.status()
	if got.Runs != 1 || got.MemoryKills != 1 || len(got.Errors) != 0 || len(s.shrinks) != 0 {
		t.Fatalf("status: %+v, %d shrink jobs", got, len(s.shrinks))
	}
	for kind, count := range got.Failures {
		if count != 0 {
			t.Fatalf("the memory kill counts as %d failures of kind %s", count, kind)
		}
	}
}
