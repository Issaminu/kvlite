//go:build unix

package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// testWriter sends the log of the supervisor to the test log.
type testWriter struct{ t *testing.T }

func (writer testWriter) Write(data []byte) (int, error) {
	writer.t.Log(strings.TrimSuffix(string(data), "\n"))
	return len(data), nil
}

// TestSupervisorRecordsAndShrinks runs the loop of the supervisor with the test binary as the worker.
// Every run of 5 or more steps fails at step 4, so the status must count every failure, retain at most maxSeeds seeds, and shrink one seed to 5 steps.
// The error must also get one GitHub issue, and the shrink must add the 5-step run to it.
func TestSupervisorRecordsAndShrinks(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	out := t.TempDir()
	t.Setenv(helperEnv, "fail-from-5")
	opts := options{
		out:         out,
		workers:     2,
		timeout:     30 * time.Second,
		memoryLimit: "256MiB",
		reports:     true,
	}
	lanes := []lane{{"default", 1, 20}}
	// A status of an earlier supervisor must be kept, not overwritten.
	if err := os.WriteFile(filepath.Join(out, "status.json"), []byte(`{"commit": "old1234"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	stop, stopCancel := context.WithCancel(context.Background())
	defer stopCancel()
	s := newSupervisor(opts, lanes, stop, stopCancel, context.Background())
	s.logger = log.New(testWriter{t}, "", log.Lmicroseconds)
	github := &fakeGitHub{open: make(map[string]int), comments: make(map[int][]string)}
	server := httptest.NewServer(github)
	defer server.Close()
	s.issues = newIssueTracker("owner/name", "test-token")
	s.issues.api = server.URL
	if err := s.prepare(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(out, "status-old1234.json")); err != nil {
		t.Fatalf("the status of the earlier supervisor: %v", err)
	}
	served := make(chan struct{})
	go func() {
		defer close(served)
		s.serve(&build{Commit: "test123", Binary: executable, GOOS: "testos", GOARCH: "testarch"}, nil, nil)
	}()

	deadline := time.Now().Add(20 * time.Second)
	for {
		errors := s.errors.snapshot()
		if len(errors) == 1 && errors[0].Count > maxSeeds && errors[0].Seeds[0].Steps == 5 && len(github.commentsOf(101)) == 1 {
			break
		}
		if time.Now().After(deadline) {
			stopCancel()
			<-served
			t.Fatalf("the shrink did not complete: %+v", errors)
		}
		time.Sleep(20 * time.Millisecond)
	}
	stopCancel()
	<-served

	var current status
	data, err := os.ReadFile(filepath.Join(out, "status.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &current); err != nil {
		t.Fatal(err)
	}
	if current.Runs == 0 || current.Failures[kindFail] != current.Runs || current.Commit != "test123" || current.Platform != "testos/testarch" {
		t.Fatalf("status: %s", data)
	}
	// Shrink runs with the same error do not count; retained seeds are capped at maxSeeds.
	if len(current.Errors) != 1 || current.Errors[0].Error != "fail: Open: page N is bad" || current.Errors[0].Count != current.Runs || len(current.Errors[0].Seeds) != min(current.Runs, maxSeeds) {
		t.Fatalf("status errors: %s", data)
	}
	shortest := current.Errors[0].Seeds[0]
	if shortest.Steps != 5 || shortest.FailedAt == nil || *shortest.FailedAt != 4 || !strings.HasPrefix(shortest.Message, "Open: page ") {
		t.Fatalf("the shrunk seed: %+v", shortest)
	}
	reports, err := filepath.Glob(filepath.Join(out, "reports", "*.txt"))
	if err != nil || len(reports) != current.Runs {
		t.Fatalf("reports: %d files for %d failed runs, %v", len(reports), current.Runs, err)
	}
	if current.Totals == nil || current.Totals.Runs < int64(current.Runs) || current.Totals.Failures < int64(current.Runs) || !(current.Totals.WorkerHours > 0) || !(current.Totals.WallHours > 0) {
		t.Fatalf("totals: %+v for %d runs", current.Totals, current.Runs)
	}
	if saved, err := loadTotals(out, time.Now()); err != nil || *saved != *current.Totals {
		t.Fatalf("totals.json %+v, %v; status.json has %+v", saved, err, current.Totals)
	}
	if len(github.issues) != 1 || !strings.Contains(github.commentsOf(101)[0], "-blackout.steps=5") {
		t.Fatalf("GitHub: %d issues, comments %q", len(github.issues), github.commentsOf(101))
	}
	if left, _ := os.ReadDir(filepath.Join(out, "tmp")); len(left) != 0 {
		t.Fatalf("the runs left %d working directories", len(left))
	}
}

// commentsOf returns a copy of the comments of an issue.
func (fake *fakeGitHub) commentsOf(number int) []string {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	return slices.Clone(fake.comments[number])
}
