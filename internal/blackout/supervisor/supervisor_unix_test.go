//go:build unix

package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"path/filepath"
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

// TestSupervisorRecordsShrinksAndAlerts runs the loop of the supervisor with the test binary as the worker.
// Every run of 5 or more steps fails at step 4, so the status must list each failed seed, the shrink must make one seed 5 steps long, and the alert must run once.
func TestSupervisorRecordsShrinksAndAlerts(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	out := t.TempDir()
	alerts := filepath.Join(t.TempDir(), "alerts.txt")
	t.Setenv(helperEnv, "fail-from-5")
	t.Setenv(alertFileEnv, alerts)
	opts := options{
		out:          out,
		workers:      2,
		timeout:      30 * time.Second,
		memoryLimit:  "256MiB",
		lanes:        "default=1:steps=20",
		reports:      true,
		alertCommand: executable,
	}
	lanes, err := parseLanes(opts.lanes)
	if err != nil {
		t.Fatal(err)
	}
	// A status of an earlier supervisor must be kept, not overwritten.
	if err := os.WriteFile(filepath.Join(out, "status.json"), []byte(`{"commit": "old1234"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	stop, stopCancel := context.WithCancel(context.Background())
	defer stopCancel()
	s := newSupervisor(opts, lanes, stop, stopCancel, context.Background())
	s.logger = log.New(testWriter{t}, "", log.Lmicroseconds)
	if err := s.prepare(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(out, "status-old1234.json")); err != nil {
		t.Fatalf("the status of the earlier supervisor: %v", err)
	}
	served := make(chan struct{})
	go func() {
		defer close(served)
		s.serve(&build{Commit: "test123", Binary: executable, GoVersion: "go-test", GOOS: "testos", GOARCH: "testarch"}, nil, nil)
	}()

	deadline := time.Now().Add(20 * time.Second)
	for {
		errors := s.errors.snapshot()
		if len(errors) == 1 && errors[0].Seeds[0].Steps == 5 {
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
	if current.Runs == 0 || current.Failures[kindFail] != current.Runs || current.Commit != "test123" || !strings.HasPrefix(current.Platform, "testos/testarch, ") {
		t.Fatalf("status: %s", data)
	}
	// The shrink runs with the same error do not count, so the error has one seed for each run.
	if len(current.Errors) != 1 || current.Errors[0].Error != "fail: Open: page N is bad" || current.Errors[0].Count != current.Runs || len(current.Errors[0].Seeds) != current.Runs {
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

	alerted, err := os.ReadFile(alerts)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(alerted)), "\n")
	if len(lines) != 1 || !strings.Contains(lines[0], " | blackout seed ") {
		t.Fatalf("alerts: %q", lines)
	}
	if left, _ := os.ReadDir(filepath.Join(out, "tmp")); len(left) != 0 {
		t.Fatalf("the runs left %d working directories", len(left))
	}
}
